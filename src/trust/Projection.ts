/**
 * The trust log folded into "who may do what, now".
 *
 * The fold is also where authority is *checked*. A record only takes effect if
 * the state that precedes it already authorized its signer to write it — which
 * is what makes this a chain reaching the genesis rather than a list of claims.
 * Doing the check anywhere else would mean a projection that could be built
 * from unverified records and then trusted by something that assumed otherwise.
 *
 * Records that fail are skipped, not fatal. A log is replicated, and anybody
 * who can push can append to it; one unauthorized record must not make a
 * repository's entire membership unreadable. What it must do is not count —
 * and `rejected` keeps them, because "why is Bob not a member?" is a question
 * an operator will ask.
 */
import { Effect, Result } from "effect";

import {
  type Fingerprint,
  fingerprint,
  NAMESPACE,
  parsePublicKey,
  verify,
} from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "./Certificate.ts";
import { MAX_SIGNATURES } from "./Certificate.ts";
import { type Genesis, type RepoId, type RootKey } from "./Genesis.ts";
import * as Log from "./Log.ts";

const decoder = new TextDecoder();

export interface Member {
  readonly fingerprint: Fingerprint;
  /** The `authorized_keys` line the grant carried. */
  readonly publicKey: string;
  readonly capabilities: ReadonlyArray<string>;
  readonly grantedAt: Date;
  readonly expiresAt: Date | null;
  /** The log commit that granted this, for an operator tracing authority. */
  readonly grant: Oid;
  /**
   * Every grant this key has been given, oldest first.
   *
   * Kept because `grant` alone answers "where do their capabilities come from
   * *now*", and judging a stored event needs "where did they come from *then*".
   * Collapsing the two meant an ordinary renewal — a second `hub grant` to the
   * same key — retroactively un-authorized every event that member had ever
   * signed, since the only grant on record post-dated all of them.
   */
  readonly history: ReadonlyArray<GrantRecord>;
}

/** One grant, as the fold recorded it. */
export interface GrantRecord {
  readonly commit: Oid;
  readonly capabilities: ReadonlyArray<string>;
}

export interface Revocation {
  readonly subject: Fingerprint;
  readonly reason: Certificate.Revoke["reason"];
  /**
   * When a compromise is taken to have begun, for the retroactive class.
   *
   * `null` for every other reason — those are forward-only, and a date on
   * them would invite the reading that they reach backwards too.
   */
  readonly compromisedFrom: Date | null;
  readonly commit: Oid;
  /**
   * The grant that put this key back, if one has.
   *
   * The revocation is *not* removed when a key is re-granted, because it is
   * still true about the window it covers: deleting it made every signature
   * the key made while revoked authorized again — at the old grant's
   * capabilities, not the narrower ones it was let back in with, and
   * including under a `compromised` revocation, the one class meant to reach
   * backwards. What a re-grant ends is the window, not the record.
   */
  readonly supersededBy: Oid | null;
}

/**
 * The window a key is currently out on, if it is out.
 *
 * A key can be revoked, let back in, and revoked again, so what it has is a
 * *list* of disjoint windows rather than one. Only the last of them can still
 * be open — a grant closes the open one before another revocation can follow —
 * so this is the whole of "is this key revoked right now", and every earlier
 * window stays on the record because it is still true about its own interval.
 */
export const openWindow = (
  revocations: ReadonlyArray<Revocation> | undefined,
): Revocation | null => {
  const last = revocations?.at(-1);
  return last !== undefined && last.supersededBy === null ? last : null;
};

export interface Attestation {
  readonly commit: Oid;
  readonly at: Date;
  readonly frontier: ReadonlyArray<string>;
}

export interface Rejected {
  readonly commit: Oid;
  readonly reason: string;
}

export interface Projection {
  readonly repoId: RepoId;
  /** The log head this state was folded from; `null` when the log is empty. */
  readonly head: Oid | null;
  readonly members: ReadonlyMap<Fingerprint, Member>;
  /**
   * What revoked members held before they were revoked.
   *
   * Kept because a forward-only revocation does not unmake history: a review
   * signed before its author could see their own revocation is still a review
   * this repository authorized, and judging it needs the capabilities they had
   * at the time. Dropping them from `members` alone would silently rewrite
   * every past event by anyone who ever left.
   */
  readonly former: ReadonlyMap<Fingerprint, Member>;
  /**
   * Every window each key has been out on, oldest first.
   *
   * A list rather than a record, because a key can be revoked, let back in and
   * revoked again — and each window is separately true. Kept as one, a second
   * revocation overwrote the first, and with it the retroactive reach of a
   * `compromised` one: an attacker who had a key revoked for compromise could
   * have it revoked again for any ordinary reason and then re-granted, and
   * every signature they made during the compromise was authorized again.
   */
  readonly revoked: ReadonlyMap<Fingerprint, ReadonlyArray<Revocation>>;
  readonly roots: ReadonlyArray<RootKey>;
  readonly threshold: number;
  /** The most recent checkpoint, for callers that bound how stale a view may be. */
  readonly checkpoint: Attestation | null;
  readonly rejected: ReadonlyArray<Rejected>;
}

/** Which distinct keys signed these bytes, out of a set we care about. */
const signersOf = Effect.fn("trust.Projection.signersOf")(function* (
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
) {
  const signers = new Set<Fingerprint>();
  for (const armored of signatures.slice(0, MAX_SIGNATURES)) {
    const key = yield* verify(armored, bytes, NAMESPACE).pipe(
      // A signature that does not parse cannot add authority, and refusing the
      // whole record for it would let anyone break a grant by appending junk.
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (key === null) continue;
    signers.add(yield* fingerprint(key));
  }
  return signers;
});

const rootQuorum = (
  signers: ReadonlySet<Fingerprint>,
  roots: ReadonlyArray<RootKey>,
  threshold: number,
): boolean => {
  let met = 0;
  for (const root of roots) if (signers.has(root.fingerprint)) met++;
  return met >= threshold;
};

/**
 * Whether a member's grant had expired by the time a record claims to have
 * been issued.
 *
 * The fold still has no *clock* — the comparison is against the record's own
 * `issuedAt`, so the answer is a pure function of the log and two hosts folding
 * the same history reach the same state. Ignoring expiry here entirely meant an
 * expired `member.invite` holder's pre-signed grants took effect on every
 * replica forever, which is an expiry that expires nothing.
 *
 * `issuedAt` is written by the issuer, so a determined one can backdate it —
 * but only into a window in which they genuinely held the capability, which is
 * the same bound `Verify` accepts for every other stored statement.
 */
const expired = (member: Member, at: Date): boolean =>
  member.expiresAt !== null && member.expiresAt.getTime() <= at.getTime();

/** Every signer who may invite members — all of them, not the first found. */
const inviters = (
  members: ReadonlyMap<Fingerprint, Member>,
  revoked: ReadonlyMap<Fingerprint, ReadonlyArray<Revocation>>,
  signers: ReadonlySet<Fingerprint>,
  at: Date,
): ReadonlyArray<Fingerprint> => {
  const found: Fingerprint[] = [];
  for (const signer of signers) {
    if (openWindow(revoked.get(signer)) !== null) continue;
    const member = members.get(signer);
    if (member === undefined || expired(member, at)) continue;
    if (Certificate.permits(member.capabilities, "member.invite")) found.push(signer);
  }
  return found;
};

/** Whether any signer holds a capability *within the fold*. */
const holds = (
  members: ReadonlyMap<Fingerprint, Member>,
  revoked: ReadonlyMap<Fingerprint, ReadonlyArray<Revocation>>,
  signers: ReadonlySet<Fingerprint>,
  capability: string,
  at: Date,
): Fingerprint | null => {
  for (const signer of signers) {
    if (openWindow(revoked.get(signer)) !== null) continue;
    const member = members.get(signer);
    if (member === undefined || expired(member, at)) continue;
    if (Certificate.permits(member.capabilities, capability)) return signer;
  }
  return null;
};

/**
 * Fold the log.
 *
 * The genesis supplies the initial roots and threshold; every later
 * `trust.root-change` replaces them, so a repository can rotate its roots
 * without its identity — which is over the genesis bytes — ever changing.
 */
export const project = Effect.fn("trust.Projection.project")(function* (genesis: Genesis) {
  const { entries, keyOf, winner } = yield* readLog();

  const members = new Map<Fingerprint, Member>();
  const former = new Map<Fingerprint, Member>();
  const revoked = new Map<Fingerprint, ReadonlyArray<Revocation>>();
  const rejected: Rejected[] = [];

  /**
   * The *statements* already applied, so none is applied twice.
   *
   * The log ref is writable by anybody holding `source.push` — append-only
   * containment is the only thing the policy boundary checks about it — so
   * re-committing an existing record's bytes at the head is a push anyone can
   * make. Without this, a replay of a revoked member's original grant passed
   * the re-instatement check below (its signer *is* the original admin) and
   * cleared the revocation; the same trick re-revoked a re-instated member,
   * restored a narrowed grant and reinstalled a stale checkpoint.
   *
   * Keyed on the id *and the bytes*, not the id alone. `winner` already
   * separates two commits of one record, but a copy carrying an extra junk
   * signature is a different key there and reaches this check, which is what
   * this catches. Keyed on the bare id it caught much more than that: any
   * member holding a trust capability could publish an authorized record —
   * a grant to a key of their own — re-using the id of a revocation naming
   * them, grind it below the real one, and burn the id, so the revocation was
   * discarded as a duplicate on a ref that can never be rewound. Two records
   * with one id but different content are two different claims, and each is
   * now answered on its own authority.
   */
  const applied = new Set<string>();
  const statementOf = (entry: (typeof entries)[number]) =>
    `${entry.payload.id}\u0000${decoder.decode(entry.bytes)}`;
  let roots: ReadonlyArray<RootKey> = genesis.roots;
  let threshold = genesis.document.threshold;
  let checkpoint: Attestation | null = null;
  let head: Oid | null = null;

  for (const entry of entries) {
    head = entry.commit;

    const invalid = yield* Certificate.validate(entry.payload, genesis.repoId).pipe(
      Effect.as(null),
      Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
    );
    if (invalid !== null) {
      rejected.push({ commit: entry.commit, reason: invalid });
      continue;
    }

    const signers = yield* signersOf(entry.bytes, entry.signatures);
    const quorum = rootQuorum(signers, roots, threshold);
    const payload = entry.payload;
    // What the record says about when it was made, which is what every
    // expiry below is judged against.
    const claimedAt = new Date(payload.issuedAt);

    // Asked here, recorded only where a record actually takes effect. Marking
    // the id before the authority check let an unauthorized record *burn* a
    // legitimate one's id: build a same-id record off the target's parent,
    // grind its oid below the real one, push a join, and the forgery folds
    // first, claims the id, is refused for want of authority — and the genuine
    // revocation behind it is then discarded as a duplicate.
    // Which of several records claiming one id is *the* one is decided by
    // descent, not by fold order. The ids are inside the signed bytes, so a
    // byte-identical replay carries the same one — and fold order breaks ties
    // by raw oid, which anybody who may write the ref can grind. Losing that
    // race moved `grant`, `history[].commit` and `Revocation.commit` onto a
    // commit honest trust heads cannot reach, which stops every stored event
    // by that member counting and stops a revocation applying at all.
    if (winner.get(keyOf(entry)) !== entry.commit) {
      rejected.push({ commit: entry.commit, reason: `${payload.id} has already been applied` });
      continue;
    }
    const statement = statementOf(entry);
    if (applied.has(statement)) {
      rejected.push({ commit: entry.commit, reason: `${payload.id} has already been applied` });
      continue;
    }

    if (payload.type === "trust.root-change") {
      // Only the current roots may replace themselves. A member with
      // `repo.admin` must not be able to rewrite the set of keys that granted
      // them `repo.admin` — that is the one loop authority cannot survive.
      if (!quorum) {
        rejected.push({ commit: entry.commit, reason: "root change needs the root quorum" });
        continue;
      }
      const changed = yield* rootsOf(payload.rootKeys);
      roots = changed;
      threshold = payload.threshold;
      applied.add(statement);
      continue;
    }

    if (payload.type === "trust.grant") {
      // Every signer who may invite, not merely the first one found: a record
      // co-signed by an inviter and an admin must not be accepted or refused
      // on the order the signatures happen to be in.
      const issuers = quorum ? [] : inviters(members, revoked, signers, claimedAt);
      if (!quorum && issuers.length === 0) {
        rejected.push({ commit: entry.commit, reason: "issuer may not invite members" });
        continue;
      }
      // Nobody may grant what they do not hold. Without this, one member with
      // `member.invite` could mint themselves an admin by granting it to a key
      // they also control. Held capabilities are pooled across the signers,
      // because they all signed the same record.
      if (!quorum) {
        const held = issuers.flatMap((issuer) => members.get(issuer)?.capabilities ?? []);
        const excess = payload.capabilities.filter(
          (capability) => !Certificate.permits(held, capability),
        );
        if (excess.length > 0) {
          rejected.push({
            commit: entry.commit,
            reason: `issuer cannot grant ${excess.join(", ")}`,
          });
          continue;
        }
      }

      // SAFETY: `Certificate.validate` has checked that `subject` is the
      // fingerprint of `publicKey`, which is what a `Fingerprint` names.
      const subject = payload.subject as Fingerprint;

      // A grant over a revocation re-instates — rotating a key back in is an
      // ordinary thing to do, and requiring a new fingerprint for it would
      // mean a compromised-then-recovered key could never be used again. But
      // it takes the authority that *made* the revocation, not merely the
      // authority to add members: `member.invite` clearing a revocation made
      // `revoke` undoable by anybody who could `grant`, retroactive compromise
      // revocations included, and left nothing in `revoked` to say so.
      if (
        openWindow(revoked.get(subject)) !== null &&
        !quorum &&
        holds(members, revoked, signers, "member.revoke", claimedAt) === null
      ) {
        rejected.push({
          commit: entry.commit,
          reason: `issuer may not re-instate ${subject}; that needs member.revoke`,
        });
        continue;
      }

      // Appended to whatever this key already had — including what it held
      // before a revocation, since re-instating is a continuation rather than
      // a fresh start, and the events signed under the old grant were
      // authorized when they were made.
      const previous = members.get(subject) ?? former.get(subject);
      members.set(subject, {
        fingerprint: subject,
        publicKey: payload.publicKey,
        capabilities: payload.capabilities,
        grantedAt: new Date(payload.issuedAt),
        expiresAt: payload.expiresAt === null ? null : new Date(payload.expiresAt),
        grant: entry.commit,
        history: [
          ...(previous?.history ?? []),
          { commit: entry.commit, capabilities: payload.capabilities },
        ],
      });
      // Closed, not erased: everything the key signed between the revocation
      // and this grant stays refused.
      // Only a revocation that is still *open* is closed by this grant. An
      // ordinary renewal would otherwise move an already-closed window's end
      // forward, and every event signed after the first re-instatement — which
      // named the old end as its trust head — would stop reaching it and be
      // refused: approvals vanishing and protected-branch merges failing for a
      // key that had been a member the whole time.
      const windows = revoked.get(subject) ?? [];
      const ending = openWindow(windows);
      if (ending !== null) {
        revoked.set(subject, [...windows.slice(0, -1), { ...ending, supersededBy: entry.commit }]);
      }
      // `former` is what a forward-only revocation is judged against, and this
      // key is a current member again: leaving a stale entry there would let a
      // later revocation be measured against capabilities they no longer hold.
      former.delete(subject);
      applied.add(statement);
      continue;
    }

    if (payload.type === "trust.revoke") {
      if (!quorum && holds(members, revoked, signers, "member.revoke", claimedAt) === null) {
        rejected.push({ commit: entry.commit, reason: "issuer may not revoke members" });
        continue;
      }
      // SAFETY: `Certificate.validate` has checked the subject is a fingerprint.
      const subject = payload.subject as Fingerprint;
      const compromised = payload.reason === "compromised";
      // Appended, never replacing: a key revoked, re-instated and revoked
      // again was out for two separate intervals, and the events it signed in
      // the first are not made authorized by the second ending.
      revoked.set(subject, [
        ...(revoked.get(subject) ?? []),
        {
          subject,
          supersededBy: null,
          reason: payload.reason,
          compromisedFrom: !compromised
            ? null
            : payload.compromisedAt !== null
              ? new Date(payload.compromisedAt)
              : // A compromise of unknown age is not a compromise of no age:
                // fall back to the grant, so everything the key signed is suspect.
                (members.get(subject)?.grantedAt ?? new Date(0)),
          commit: entry.commit,
        },
      ]);
      const held = members.get(subject);
      if (held !== undefined) former.set(subject, held);
      members.delete(subject);
      applied.add(statement);
      continue;
    }

    if (!quorum && holds(members, revoked, signers, "repo.admin", claimedAt) === null) {
      rejected.push({ commit: entry.commit, reason: "issuer may not checkpoint" });
      continue;
    }
    applied.add(statement);

    // The *newest*, not the last folded. Fold order is topological with an oid
    // tie-break, so two checkpoints made concurrently on two replicas and then
    // joined could leave the older one in force — and a repository that had
    // set `maxTrustAgeSeconds` would then refuse every push against an
    // attestation it already had a fresher replacement for. Ties go to the
    // greater oid, so every replica still agrees.
    const attested = {
      commit: entry.commit,
      at: new Date(payload.issuedAt),
      frontier: payload.frontier,
    };
    if (
      checkpoint === null ||
      attested.at > checkpoint.at ||
      (attested.at.getTime() === checkpoint.at.getTime() && attested.commit > checkpoint.commit)
    ) {
      checkpoint = attested;
    }
  }

  return {
    repoId: genesis.repoId,
    head,
    members,
    former,
    revoked,
    roots,
    threshold,
    checkpoint,
    rejected,
  };
});

/**
 * The log, plus which commit owns each record id.
 *
 * The rest of the log descends from a genuine record; a replay grafted on
 * later is descended from by nothing, however its oid sorts. Ties — where
 * descent cannot separate them — go to the lower oid, so every replica still
 * agrees.
 */
const readLog = Effect.fn("trust.Projection.readLog")(function* () {
  const { parents, records } = yield* Log.entries();

  const ancestors = new Map<Oid, Set<Oid>>();
  for (const oid of Dag.topological(parents)) {
    const set = new Set<Oid>();
    for (const parent of parents.get(oid) ?? []) {
      set.add(parent);
      for (const older of ancestors.get(parent) ?? []) set.add(older);
    }
    ancestors.set(oid, set);
  }

  // Grouped by id *and* by the exact bytes. Two records with one id but
  // different content are two different claims, and each is answered on its
  // own authority rather than by whichever reached the fold first. What this
  // decides is the other case: the same record committed twice, where nothing
  // distinguishes them but the commit, and the commit is what `grant` and
  // `Revocation.commit` are read from later.
  const claimed = new Map<string, Oid[]>();
  // The signatures are part of the record's identity, not decoration. Keyed on
  // the payload alone, a replay that kept the bytes and *dropped* the
  // signatures counted as the same record — so grinding a lower oid onto an
  // unsigned copy of a revocation won the tie-break and dropped the signed
  // original as its duplicate.
  const keyOf = (entry: (typeof records)[number]) =>
    [entry.payload.id, decoder.decode(entry.bytes), ...entry.signatures].join("\u0000");
  for (const entry of records) {
    const key = keyOf(entry);
    claimed.set(key, [...(claimed.get(key) ?? []), entry.commit]);
  }

  const winner = new Map<string, Oid>();
  for (const [id, commits] of claimed) {
    if (commits.length === 1) {
      winner.set(id, commits[0]!);
      continue;
    }
    const descendants = (commit: Oid) => {
      let count = 0;
      for (const seen of ancestors.values()) if (seen.has(commit)) count++;
      return count;
    };
    let best = commits[0]!;
    let reach = descendants(best);
    for (const commit of commits.slice(1)) {
      const count = descendants(commit);
      if (count > reach || (count === reach && commit < best)) {
        best = commit;
        reach = count;
      }
    }
    winner.set(id, best);
  }

  return { entries: records, winner, keyOf };
});

/**
 * Key lines to root keys.
 *
 * A line that does not parse is dropped rather than failing the fold:
 * `Certificate.validate` has already refused any root change whose keys are
 * unreadable, so reaching here with one means the record was accepted by an
 * older version, and losing the whole projection over it would be worse than
 * losing the key.
 */
const rootsOf = Effect.fn("trust.Projection.rootsOf")(function* (lines: ReadonlyArray<string>) {
  const roots: RootKey[] = [];
  for (const line of lines) {
    const parsed = parsePublicKey(line);
    if (Result.isFailure(parsed)) continue;
    roots.push({ key: parsed.success, fingerprint: yield* fingerprint(parsed.success) });
  }
  return roots;
});

export type ProjectionError = Invalid | ObjectNotFound | StorageFailure;
