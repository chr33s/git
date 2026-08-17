/**
 * The trust log: membership as a hash-linked history, not a bag of refs.
 *
 * An earlier design gave every grant and revocation its own immutable ref and
 * synchronized the set by union. That converges, but it has a failure nobody
 * can see: *omission is invisible*. A replica that withholds one revocation
 * ref presents a trust state in which the revoked key is still a member, and
 * no verifier holding only a set of refs can tell the set is incomplete.
 *
 * A log fixes exactly that. Every record names its predecessors, so the state
 * has a frontier, and two replicas can tell whether one has seen everything
 * the other has. Withholding stops being silent — it becomes a stale head,
 * which `Checkpoint.ts` puts a bound on.
 *
 * ```text
 * refs/meta/trust/genesis ── the identity, written once
 *              ▲
 *              │ first record's parent
 * refs/meta/trust/log ────── grant → revoke → root-change → …
 * ```
 *
 * What this does *not* claim: freshness. A replica can serve a consistent but
 * old view, up to the age a verifier is willing to accept. That is inherent to
 * anything verifiable offline, and it is written down rather than papered over.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "./Certificate.ts";
import { GENESIS_REF } from "./Genesis.ts";
import * as Record from "./Record.ts";

/** The log's head. Append-only: it never moves to something not descended from it. */
export const LOG_REF = "refs/meta/trust/log";

/** The record name inside each commit — `entry.json` and `entry.sig`. */
export const RECORD = "entry";

export interface Entry {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly payload: Certificate.TrustPayload;
  /**
   * The payload's stored bytes.
   *
   * Carried beside the decoded form because signatures are over these, and
   * verifying against a re-encoding would check that this version can
   * reproduce the bytes rather than that the signer signed them.
   */
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

/**
 * A UUIDv7: milliseconds big-endian, then randomness.
 *
 * Time-ordered so that ids sort roughly by creation, which is what makes them
 * usable as a deterministic tie-break between records that are causally
 * concurrent — and it is a *tie-break*, never a clock anyone trusts.
 */
export const newId = (at: Date = new Date()): string => {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);

  const milliseconds = at.getTime();
  // 48 bits of timestamp; `>>>` would truncate at 32, so the high half is
  // divided out rather than shifted.
  const high = Math.floor(milliseconds / 2 ** 32);
  const low = milliseconds >>> 0;
  bytes[0] = (high >>> 8) & 0xff;
  bytes[1] = high & 0xff;
  bytes[2] = (low >>> 24) & 0xff;
  bytes[3] = (low >>> 16) & 0xff;
  bytes[4] = (low >>> 8) & 0xff;
  bytes[5] = low & 0xff;
  // Version 7, variant 10.
  bytes[6] = 0x70 | (bytes[6]! & 0x0f);
  bytes[8] = 0x80 | (bytes[8]! & 0x3f);

  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
};

/**
 * Where the next record hangs from, and what the ref must currently be.
 *
 * These are the same value except on the very first append, and the exception
 * is the whole reason they are two fields: the first record's *parent* is the
 * genesis commit, so the chain reaches identity, but the *ref* does not exist
 * yet, so the compare-and-swap has to be "must not exist" rather than "must be
 * the genesis". Conflating them makes the first append of every repository
 * fail with a conflict against a ref nobody has written.
 */
const base = Effect.fn("trust.Log.base")(function* () {
  const repository = yield* Repository;

  const head = yield* repository.resolve(LOG_REF);
  if (head !== null) return { parent: head, expected: head };

  const genesis = yield* repository.resolve(GENESIS_REF);
  if (genesis === null) {
    return yield* new Invalid({
      field: "trust",
      reason: "this repository has no genesis; run `hub init` before granting membership",
    });
  }
  return { parent: genesis, expected: null };
});

/**
 * Append a record and move the log head.
 *
 * The compare-and-swap is on the head this record was built against, so a
 * concurrent append loses and retries rather than overwriting. Retrying is
 * safe here in a way it is not everywhere: adding a record to a log does not
 * change what the record says, and the projection is what resolves two
 * records that arrived at once.
 */
export const append = Effect.fn("trust.Log.append")(
  function* (
    payload: Certificate.TrustPayload,
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

/**
 * Sign a payload and append it in one step.
 *
 * The bytes are encoded once and both signed and stored, rather than encoded
 * for signing and again for writing. Two encodings that agree today are two
 * that can drift, and the failure would be signatures that verify nowhere.
 *
 * Several keys because a quorum operation needs several: a root change is
 * signed by the threshold, not by whoever typed the command.
 */
export const issue = Effect.fn("trust.Log.issue")(function* (
  payload: Certificate.TrustPayload,
  keys: ReadonlyArray<PrivateKey>,
) {
  const bytes = Certificate.encode(payload);
  const signatures = yield* Effect.forEach(keys, (key) => sign(key, bytes, NAMESPACE));
  return yield* append(payload, bytes, signatures);
});

/**
 * Join two divergent heads.
 *
 * The result carries no payload — it is a synchronization artifact, and
 * `entries` walks through it without folding anything. Recording the join as a
 * commit rather than picking a winner is what keeps both sides' records in the
 * history: a log that resolved divergence by choosing one head would drop the
 * revocations on the other.
 */
export const join = Effect.fn("trust.Log.join")(function* (heads: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  if (heads.length < 2) {
    return yield* new Invalid({ field: "heads", reason: "a join needs two heads or more" });
  }

  const tree = yield* repository.writeTree([]);
  return yield* repository.commitTree({
    tree,
    parents: heads,
    message: "join\n",
    author: Record.identityAt(new Date()),
  });
});

/**
 * Every record in the log, oldest first, parents before children.
 *
 * Concurrent records — neither an ancestor of the other — are ordered by their
 * commit oid. Any total order over a partial one is arbitrary; what matters is
 * that every replica picks the *same* arbitrary one, so two hosts folding the
 * same history reach the same state.
 *
 * The whole log is read into memory. Membership changes are the rarest events
 * a repository has, and the alternative — a projection that streams — buys
 * nothing until a repository has more grants than commits.
 */
/**
 * Whether a commit belongs to the trust log.
 *
 * The genesis bounds the chain where it is supposed to end; this bounds one
 * that is not. A log head reaching into the source history would otherwise
 * make every authorization check walk the whole repository — the same trap
 * `hub/Event.ts` guards against.
 */
export const isTrustCommit = Effect.fn("trust.Log.isTrustCommit")(function* (commit: Oid) {
  const repository = yield* Repository;
  const info = yield* repository
    .readCommit(commit)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (info === null) return false;
  // The tree, not only the commit. `fetchRepository` applies refs without a
  // connectivity check, so a replica can hold a commit whose tree object never
  // arrived — and this walk is what every protected-branch push, collection
  // and deepening fetch runs first. Read as a failure, one missing tree took
  // all of them out; read as "not part of this history", it is one commit the
  // walk steps over.
  const found = yield* repository
    .findPath(info.tree, `${RECORD}.json`)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (found !== null) return true;
  // A join carries an empty tree; anything else is not part of this history.
  return (
    (yield* repository.readTree(info.tree).pipe(Effect.orElseSucceed(() => null)))?.length === 0
  );
});

/**
 * How many commits one trust log fold will walk.
 *
 * Larger than a pull request's ceiling because a repository's membership
 * history is meant to outlive its pull requests: at a checkpoint an hour this
 * is a couple of years, and at the daily cadence most repositories will want,
 * decades. It is a ceiling all the same, because the log is append-only and
 * nothing can shorten it — a host expecting to outgrow this raises it rather
 * than discovering it.
 */
export const MAX_RECORDS = 16_384;

/** The ceiling in force, when a host wants a different one; see `MAX_RECORDS`. */
export class Ceiling extends Context.Service<Ceiling, number>()("trust/Log/Ceiling") {}

export const ceiling = (records: number): Layer.Layer<Ceiling> => Layer.succeed(Ceiling)(records);

const ceilingOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(Ceiling), () => MAX_RECORDS);
});

/**
 * Whether a value stays inside that ceiling.
 *
 * Asked by the policy boundary, and *only* there. A pull request past its
 * ceiling is one candidate missing, which every caller can carry on without;
 * a membership log past its ceiling has no such reading — refusing to fold it
 * leaves a repository nothing can be authorized against, on a ref nothing can
 * shorten, and replication writes refs without passing this boundary at all.
 * So the fold walks whatever it is given, and the bound is applied where it
 * can be applied without turning an unreadable log into an unusable
 * repository: the push that would grow one.
 */
export const withinCeiling = Effect.fn("trust.Log.withinCeiling")(function* (head: Oid) {
  const repository = yield* Repository;
  const genesis = yield* repository.resolve(GENESIS_REF);
  // Bounded as the walk runs. Bounding the result means reading the whole log
  // before refusing it, which is the cost the ceiling exists to refuse.
  return yield* Dag.reachable(
    head,
    genesis,
    (commit) => isTrustCommit(commit),
    yield* ceilingOf(),
  ).pipe(
    Effect.as(true),
    Effect.catchTag("Invalid", () => Effect.succeed(false)),
  );
});

export const entries = Effect.fn("trust.Log.entries")(function* () {
  const repository = yield* Repository;

  const head = yield* repository.resolve(LOG_REF);
  if (head === null) return { records: [], parents: new Map<Oid, ReadonlyArray<Oid>>() };

  // The genesis is the chain's anchor, not a record: bounding the walk there
  // is what stops it reading the whole source history if anything ever points
  // the log at a branch.
  const genesis = yield* repository.resolve(GENESIS_REF);
  const parents = yield* Dag.reachable(head, genesis, (commit) => isTrustCommit(commit));
  const ordered = Dag.topological(parents);

  const records: Entry[] = [];
  for (const oid of ordered) {
    const carries = yield* Record.carries(oid, RECORD);
    // Join commits carry nothing; they are structure, not statements.
    if (!carries) continue;

    // A record this version cannot read is skipped, exactly as one that fails
    // its authority check is. Propagating here would be permanent: the log is
    // append-only, so a single malformed record pushed by any member would
    // fail every projection, and with it every request, with no way to rewind
    // the ref.
    const record = yield* Record.read(oid, RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) continue;

    const payload = yield* Certificate.decode(record.payload).pipe(
      Effect.orElseSucceed(() => null),
    );
    if (payload === null) continue;

    records.push({
      commit: oid,
      parents: parents.get(oid) ?? [],
      payload,
      bytes: record.payload,
      signatures: record.signatures,
    });
  }
  // The whole walked DAG rides along, joins included. A caller deciding
  // between two records that claim one id needs to know which of them the rest
  // of the log descends from, and a parent map built from record-carrying
  // commits alone would break at every join.
  return { records, parents };
});

/**
 * Whether a commit is part of this replica's trust log.
 *
 * Asked before an event's self-declared trust head is believed. Ancestry alone
 * cannot answer it: a walk from a fabricated oid, or from the tip of `main`,
 * reaches no trust record and so matches no revocation — which looks exactly
 * like an event that genuinely predates one. Naming any oid at all would
 * otherwise be a way out of every forward-only revocation.
 */
export const contains = Effect.fn("trust.Log.contains")(function* (commit: Oid) {
  const repository = yield* Repository;

  const head = yield* repository.resolve(LOG_REF);
  if (head === null) return false;

  const genesis = yield* repository.resolve(GENESIS_REF);
  const reachable = yield* Dag.reachable(head, genesis, (oid) => isTrustCommit(oid));
  return reachable.has(commit);
});

/**
 * Every trust record an entry can reach, itself included.
 *
 * What makes revocation ordering deterministic instead of wall-clock-dependent:
 * "had the author already seen this revocation?" is answered by ancestry, which
 * every replica computes the same way, rather than by comparing timestamps
 * anybody can write.
 *
 * Bounded by the genesis and by whether each commit is a trust record, for the
 * same reason `entries` is: an unbounded walk from an attacker-supplied oid
 * reads the repository's entire source history on every authorization check.
 */
export const ancestry = Effect.fn("trust.Log.ancestry")(function* (from: Oid) {
  const repository = yield* Repository;

  const genesis = yield* repository.resolve(GENESIS_REF);
  const reachable = yield* Dag.reachable(from, genesis, (oid) => isTrustCommit(oid));
  return new Set(reachable.keys());
});

export const RecordName = Schema.Literal(RECORD);

export type LogError = Invalid | ObjectNotFound | StorageFailure;
