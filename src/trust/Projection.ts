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
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "./Certificate.ts";
import { type Genesis, type RepoId, type RootKey } from "./Genesis.ts";
import * as Log from "./Log.ts";

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
}

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
  readonly revoked: ReadonlyMap<Fingerprint, Revocation>;
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
  for (const armored of signatures) {
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
  revoked: ReadonlyMap<Fingerprint, Revocation>,
  signers: ReadonlySet<Fingerprint>,
  at: Date,
): ReadonlyArray<Fingerprint> => {
  const found: Fingerprint[] = [];
  for (const signer of signers) {
    if (revoked.has(signer)) continue;
    const member = members.get(signer);
    if (member === undefined || expired(member, at)) continue;
    if (Certificate.permits(member.capabilities, "member.invite")) found.push(signer);
  }
  return found;
};

/** Whether any signer holds a capability *within the fold*. */
const holds = (
  members: ReadonlyMap<Fingerprint, Member>,
  revoked: ReadonlyMap<Fingerprint, Revocation>,
  signers: ReadonlySet<Fingerprint>,
  capability: string,
  at: Date,
): Fingerprint | null => {
  for (const signer of signers) {
    if (revoked.has(signer)) continue;
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
  const entries = yield* Log.entries();

  const members = new Map<Fingerprint, Member>();
  const former = new Map<Fingerprint, Member>();
  const revoked = new Map<Fingerprint, Revocation>();
  const rejected: Rejected[] = [];

  /**
   * The record ids already applied, so none is applied twice.
   *
   * The log ref is writable by anybody holding `source.push` — append-only
   * containment is the only thing the policy boundary checks about it — so
   * re-committing an existing record's bytes at the head is a push anyone can
   * make. Without this, a replay of a revoked member's original grant passed
   * the re-instatement check below (its signer *is* the original admin) and
   * cleared the revocation; the same trick re-revoked a re-instated member,
   * restored a narrowed grant and reinstalled a stale checkpoint. The id is
   * inside the signed bytes, so a replay cannot dodge this by changing it.
   */
  const applied = new Set<string>();
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

    if (applied.has(payload.id)) {
      rejected.push({ commit: entry.commit, reason: `${payload.id} has already been applied` });
      continue;
    }
    applied.add(payload.id);

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
        revoked.has(subject) &&
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
      revoked.delete(subject);
      // `former` is what a forward-only revocation is judged against, and this
      // key is a current member again: leaving a stale entry there would let a
      // later revocation be measured against capabilities they no longer hold.
      former.delete(subject);
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
      revoked.set(subject, {
        subject,
        reason: payload.reason,
        compromisedFrom: !compromised
          ? null
          : payload.compromisedAt !== null
            ? new Date(payload.compromisedAt)
            : // A compromise of unknown age is not a compromise of no age:
              // fall back to the grant, so everything the key signed is suspect.
              (members.get(subject)?.grantedAt ?? new Date(0)),
        commit: entry.commit,
      });
      const held = members.get(subject);
      if (held !== undefined) former.set(subject, held);
      members.delete(subject);
      continue;
    }

    if (!quorum && holds(members, revoked, signers, "repo.admin", claimedAt) === null) {
      rejected.push({ commit: entry.commit, reason: "issuer may not checkpoint" });
      continue;
    }
    checkpoint = {
      commit: entry.commit,
      at: new Date(payload.issuedAt),
      frontier: payload.frontier,
    };
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
