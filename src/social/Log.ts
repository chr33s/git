/** The append-only social history in one principal's identity repository. */
import { Context, Effect, Layer, Option } from "effect";

import { NAMESPACE, type Fingerprint, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import { type Genesis } from "../trust/Genesis.ts";
import * as TrustLog from "../trust/Log.ts";
import { principalId, type PrincipalId } from "../trust/Principal.ts";
import type { Projection as TrustProjection } from "../trust/Projection.ts";
import * as AppendOnly from "../trust/AppendOnly.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";
import * as Statement from "./Statement.ts";

export const newId = TrustLog.newId;

export const LOG_REF = "refs/social/log";
export const RECORD = "statement";

export interface Entry {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly payload: Statement.SocialStatement;
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

export interface VerifiedStatement extends Entry {
  readonly signer: Fingerprint;
}

export interface Rejected {
  readonly commit: Oid;
  readonly reason: string;
}

/** A verified log is a safe input to the cross-principal projection. */
export interface VerifiedLog {
  readonly principal: PrincipalId;
  readonly head: Oid | null;
  /** Whole social DAG, joins included, for causal revoke checks. */
  readonly parents?: ReadonlyMap<Oid, ReadonlyArray<Oid>>;
  readonly statements: ReadonlyArray<VerifiedStatement>;
  readonly rejected: ReadonlyArray<Rejected>;
}

export const MAX_RECORDS = 16_384;

export class Ceiling extends Context.Service<Ceiling, number>()("social/Log/Ceiling") {}

export const ceiling = (records: number): Layer.Layer<Ceiling> => Layer.succeed(Ceiling)(records);

export const ceilingOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(Ceiling), () => MAX_RECORDS);
});

/** What this log is, for the machinery every append-only log shares. */
const LOG: AppendOnly.LogDefinition<Statement.SocialStatement> = {
  name: "social.Log",
  ref: LOG_REF,
  record: RECORD,
  field: "social",
  noGenesis: "this identity repository has no genesis",
  decode: (bytes) => Statement.decode(bytes),
  ceiling: ceilingOf(),
};

const base = () => AppendOnly.base(LOG);

export const append = Effect.fn("social.Log.append")(
  function* (
    payload: Statement.SocialStatement,
    bytes: Uint8Array,
    signatures: ReadonlyArray<string>,
  ) {
    const repository = yield* Repository;
    const { expected, parent } = yield* base();
    const commit = yield* Record.write({
      name: RECORD,
      payload: bytes,
      signatures,
      parents: [parent],
      message: `${payload.type} ${payload.id}\n`,
    });
    yield* repository.setRef({ name: LOG_REF, to: commit, expected });
    return commit;
  },
  Effect.retry({ times: 3, while: (error) => error._tag === "RefConflict" }),
);

export const issue = Effect.fn("social.Log.issue")(function* (
  payload: Statement.SocialStatement,
  key: PrivateKey | ReadonlyArray<PrivateKey>,
) {
  const bytes = Statement.encode(payload);
  const keys = Array.isArray(key) ? key : [key];
  const signatures = yield* Effect.forEach(keys, (signer) => sign(signer, bytes, NAMESPACE));
  return yield* append(payload, bytes, signatures);
});

export const join = Effect.fn("social.Log.join")(function* (heads: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  if (heads.length < 2) {
    return yield* new Invalid({ field: "heads", reason: "a join needs two heads or more" });
  }
  const tree = yield* repository.writeTree([]);
  return yield* repository.commitTree({
    tree,
    parents: heads,
    message: "join social logs\n",
    author: Record.identityAt(new Date()),
  });
});

export const isSocialCommit = AppendOnly.isCommitOf(LOG);

export const withinCeiling = AppendOnly.withinCeilingOf(LOG);

/** Raw, structurally decodable records in deterministic topological order. */
export const entries = AppendOnly.entriesOf(LOG);

/**
 * One commit's ancestry, memoised per fold.
 *
 * The memo is the caller's rather than this function's, and it is keyed only
 * by commit: a `Parents` map is fixed for the fold that built it, so within
 * one fold a commit's ancestry cannot change, and between folds nothing is
 * shared. Without one, every question about ancestry re-walked the whole DAG
 * from scratch — and the two callers below ask on the order of one question
 * per commit, which made a fold of a log near its 16,384-record ceiling walk
 * that DAG tens of thousands of times. The host re-folds every sibling's log
 * after every mutating request.
 */
type Ancestry = Map<Oid, ReadonlySet<Oid>>;

const ancestorsOf = (commit: Oid, parents: Dag.Parents, memo: Ancestry): ReadonlySet<Oid> => {
  const cached = memo.get(commit);
  if (cached !== undefined) return cached;
  const seen = new Set<Oid>();
  const pending = [...(parents.get(commit) ?? [])];
  while (pending.length > 0) {
    const current = pending.pop();
    if (current === undefined) break;
    if (seen.has(current)) continue;
    seen.add(current);
    pending.push(...(parents.get(current) ?? []));
  }
  memo.set(commit, seen);
  return seen;
};

/** How strongly one candidate is embedded in the rest of the log. */
const rankOf = (
  commit: Oid,
  parents: Dag.Parents,
  memo: Ancestry,
): readonly [number, number, Oid] => {
  let descendants = 0;
  for (const candidate of parents.keys()) {
    if (ancestorsOf(candidate, parents, memo).has(commit)) descendants++;
  }
  return [descendants, ancestorsOf(commit, parents, memo).size, commit];
};

const winners = (
  records: ReadonlyArray<VerifiedStatement>,
  parents: Dag.Parents,
  memo: Ancestry,
): ReadonlySet<Oid> => {
  const byId = new Map<string, VerifiedStatement[]>();
  for (const record of records) {
    byId.set(record.payload.id, [...(byId.get(record.payload.id) ?? []), record]);
  }

  const selected = new Set<Oid>();
  for (const candidates of byId.values()) {
    const first = candidates[0];
    if (first === undefined) continue;
    // Nothing to rank against. Ranking is what makes this walk the DAG, and an
    // id claimed once — which is nearly every id — has one candidate that wins
    // by being the only one.
    if (candidates.length === 1) {
      selected.add(first.commit);
      continue;
    }
    let best = first;
    let rank = rankOf(best.commit, parents, memo);
    for (const candidate of candidates.slice(1)) {
      const next = rankOf(candidate.commit, parents, memo);
      const better =
        next[0] !== rank[0]
          ? next[0] > rank[0]
          : next[1] !== rank[1]
            ? next[1] > rank[1]
            : next[2] < rank[2];
      if (better) {
        best = candidate;
        rank = next;
      }
    }
    selected.add(best.commit);
  }
  return selected;
};

/**
 * Verify authorship and `social.write` against this identity's trust log.
 * Invalid records stay observable in `rejected` and contribute no graph edge.
 */
export const verified = Effect.fn("social.Log.verified")(function* (
  genesis: Genesis,
  trust: TrustProjection,
) {
  const repository = yield* Repository;
  const { parents, records } = yield* entries();
  // One ancestry memo for the whole fold: `parents` does not change under it,
  // and both the declared-head check below and `winners` at the end ask about
  // the same commits.
  const ancestry: Ancestry = new Map();
  const accepted: VerifiedStatement[] = [];
  const rejected: Rejected[] = [];
  const identity = principalId(genesis.repoId);
  const recordCommits = new Set(records.map((record) => record.commit));
  const hasEarlierRecord = new Map<Oid, boolean>();
  for (const commit of Dag.topological(parents)) {
    hasEarlierRecord.set(
      commit,
      (parents.get(commit) ?? []).some(
        (parent) => recordCommits.has(parent) || hasEarlierRecord.get(parent) === true,
      ),
    );
  }

  /** Trust ancestry is immutable for this fold, so each declared head is walked once. */
  const trustAncestry = new Map<Oid, ReadonlySet<Oid> | null>();
  const seenTrust: Verify.Ancestry = Effect.fnUntraced(function* (head: Oid) {
    if (trustAncestry.has(head)) {
      const known = trustAncestry.get(head);
      if (known === null || known === undefined) {
        return yield* new Invalid({ field: "trustHead", reason: `${head} cannot be walked` });
      }
      return known;
    }
    const walked = yield* TrustLog.ancestry(head).pipe(Effect.orElseSucceed(() => null));
    trustAncestry.set(head, walked);
    if (walked === null) {
      return yield* new Invalid({ field: "trustHead", reason: `${head} cannot be walked` });
    }
    return walked;
  });
  const trustMembership = new Map<Oid, boolean>();
  const containsTrust: Verify.Membership = Effect.fnUntraced(function* (commit: Oid) {
    const known = trustMembership.get(commit);
    if (known !== undefined) return known;
    const contained = yield* TrustLog.contains(commit).pipe(Effect.orElseSucceed(() => false));
    trustMembership.set(commit, contained);
    return contained;
  });
  const reachesTrust = Effect.fnUntraced(function* (head: Oid | null, target: Oid) {
    if (head === null || head === target) return true;
    return yield* seenTrust(head).pipe(
      Effect.map((seen) => seen.has(target)),
      Effect.orElseSucceed(() => false),
    );
  });

  /** Newest accepted trust head carried through social statements and joins. */
  const trustFloorAt = new Map<Oid, Oid | null>();
  const floorThrough: (
    commit: Oid,
  ) => Effect.Effect<Oid | null, Invalid | ObjectNotFound | StorageFailure, Repository> =
    Effect.fnUntraced(function* (commit: Oid) {
      if (trustFloorAt.has(commit)) return trustFloorAt.get(commit) ?? null;
      let floor: Oid | null = null;
      for (const parent of parents.get(commit) ?? []) {
        const candidate = yield* floorThrough(parent);
        if (candidate === null || candidate === floor) continue;
        if (floor === null || (yield* reachesTrust(candidate, floor))) floor = candidate;
      }
      trustFloorAt.set(commit, floor);
      return floor;
    });

  for (const entry of records) {
    let floor: Oid | null = null;
    for (const parent of entry.parents) {
      const candidate = yield* floorThrough(parent);
      if (candidate === null || candidate === floor) continue;
      if (floor === null || (yield* reachesTrust(candidate, floor))) floor = candidate;
    }
    // Rejected statements carry their accepted ancestors' floor forward.
    trustFloorAt.set(entry.commit, floor);

    const invalid = yield* Statement.validate(entry.payload, identity).pipe(
      Effect.as(null),
      Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
    );
    if (invalid !== null) {
      rejected.push({ commit: entry.commit, reason: invalid });
      continue;
    }

    const declared = entry.payload.socialHead;
    if (declared !== null && !isOid(declared)) {
      rejected.push({
        commit: entry.commit,
        reason: `declared social head ${declared} is invalid`,
      });
      continue;
    }
    if (declared !== null && !ancestorsOf(entry.commit, parents, ancestry).has(declared)) {
      rejected.push({
        commit: entry.commit,
        reason: `declared social head ${declared} is not an ancestor of this statement`,
      });
      continue;
    }
    if (declared === null && hasEarlierRecord.get(entry.commit) === true) {
      rejected.push({
        commit: entry.commit,
        reason: "only a first social statement may declare an empty social head",
      });
      continue;
    }

    const trustHead = entry.payload.trustHead;
    if (trustHead !== null && !isOid(trustHead)) {
      rejected.push({
        commit: entry.commit,
        reason: `declared trust head ${trustHead} is invalid`,
      });
      continue;
    }
    if (trustHead !== null && !(yield* containsTrust(trustHead))) {
      rejected.push({
        commit: entry.commit,
        reason: `declared trust head ${trustHead} is not in this identity's trust log`,
      });
      continue;
    }
    const behind = floor !== null && !(yield* reachesTrust(trustHead, floor));
    const effectiveTrustHead = behind ? floor : trustHead;
    const authorization = yield* Verify.authorize({
      projection: trust,
      bytes: entry.bytes,
      signatures: entry.signatures,
      capability: "social.write",
      made: { at: new Date(entry.payload.issuedAt), trustHead: effectiveTrustHead },
      seen: seenTrust,
      contains: containsTrust,
    });
    if (!authorization.ok) {
      rejected.push({ commit: entry.commit, reason: authorization.reason });
      continue;
    }
    accepted.push({ ...entry, signer: authorization.principal.fingerprint });
    if (effectiveTrustHead !== null) trustFloorAt.set(entry.commit, effectiveTrustHead);
  }

  const selected = winners(accepted, parents, ancestry);
  for (const entry of accepted) {
    if (!selected.has(entry.commit)) {
      rejected.push({
        commit: entry.commit,
        reason: `${entry.payload.id} has already been applied`,
      });
    }
  }

  return {
    principal: identity,
    head: yield* repository.resolve(LOG_REF),
    parents,
    statements: accepted.filter((entry) => selected.has(entry.commit)),
    rejected,
  } satisfies VerifiedLog;
});

export const contains = AppendOnly.containsOf(LOG);

export const ancestry = AppendOnly.ancestryOf(LOG);

export type LogError = Invalid | ObjectNotFound | StorageFailure;
