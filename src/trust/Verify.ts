/**
 * "Is this signer allowed to do this?" — the question every surface asks.
 *
 * Authentication proves possession of a private key. Membership proves the
 * repository authorized that key. They are separate checks and this module is
 * the second one, so that the smart-HTTP guard, the JSON API and the merge
 * policy cannot disagree about what a capability means.
 *
 * Two callers with genuinely different questions:
 *
 *   - a *live request* — "may this key push right now?" — where any revocation
 *     denies, and an expired grant denies.
 *   - a *stored event* — "was this review authorized when it was made?" — where
 *     a forward-only revocation must not retroactively unmake history, but a
 *     compromise must.
 *
 * The difference is `when`. Given it, the forward-only case is decided by
 * ancestry rather than by clocks: if the trust head the author was writing
 * against already contained the revocation, they had seen it, and the event is
 * refused. Every replica computes that the same way, which a timestamp
 * comparison could never promise.
 */
import { Effect } from "effect";

import { type Fingerprint, fingerprint, NAMESPACE, verify } from "../crypto/SshSignature.ts";
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import type { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { MAX_SIGNATURES, permits } from "./Certificate.ts";
import * as Log from "./Log.ts";
import { type Member, openWindow, type Projection } from "./Projection.ts";

/**
 * When an event was made, for judging a revocation against it.
 *
 * Absent means "now": a live request, held to the current state. Present means
 * a stored event, and carries the trust head its author was writing against —
 * `null` when the event does not record one, which is treated as "the author
 * had seen everything", the safe reading.
 */
export interface Made {
  readonly at: Date;
  readonly trustHead: Oid | null;
}

/**
 * "Everything this trust-log commit reaches", however the caller wants it
 * computed — `Log.ancestry`, or a memo over it.
 */
export type Ancestry = (
  head: Oid,
) => Effect.Effect<ReadonlySet<Oid>, Invalid | ObjectNotFound | StorageFailure, Repository>;

/**
 * "Is this a trust-log commit at all?", however the caller computes it.
 *
 * `Log.contains` walks the log, and a fold asks it once per event per revoked
 * signer on the protected-branch push path — about a head that cannot move
 * while the fold runs. The caller that has a memo hands it in.
 */
export type Membership = (
  commit: Oid,
) => Effect.Effect<boolean, Invalid | ObjectNotFound | StorageFailure, Repository>;

export type Authorization =
  | { readonly ok: true; readonly principal: Member }
  | { readonly ok: false; readonly reason: string };

const denied = (reason: string): Authorization => ({ ok: false, reason });

/**
 * Every distinct key that signed these bytes.
 *
 * Signatures that do not parse are skipped: they cannot add authority, and
 * failing the whole check would let anybody deny a member by appending junk to
 * a record they can write to.
 */
export const signers = Effect.fn("trust.Verify.signers")(function* (
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
) {
  const found: Fingerprint[] = [];
  for (const armored of signatures.slice(0, MAX_SIGNATURES)) {
    const key = yield* verify(armored, bytes, NAMESPACE).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (key !== null) found.push(yield* fingerprint(key));
  }
  return found;
});

/**
 * Whether a revocation reaches an event that was made at a given point.
 *
 * A compromise reaches backwards to when the compromise began, because the
 * premise is that those signatures were never the subject's. Every other
 * reason is forward-only and reaches only what the author made *after* they
 * could see it.
 */
const reaches = Effect.fn("trust.Verify.reaches")(function* (
  projection: Projection,
  subject: Fingerprint,
  made: Made | null,
  seen: Ancestry,
  held: Membership,
  permanent = false,
) {
  const revocations = projection.revoked.get(subject);
  if (revocations === undefined || revocations.length === 0) return false;

  // A live request is held to the current state: revoked is revoked, and a
  // key that has since been granted again is not.
  if (made === null) return openWindow(revocations) !== null;

  // Forward-only. An event that cannot *show* it predates the revocation does
  // not get the benefit of the doubt — and that covers a trust head this
  // replica cannot resolve as well as one that was never recorded. Ancestry
  // treats an unknown commit as unreachable, so without this check writing
  // junk into the field would opt out of revocation entirely, which is worse
  // than leaving it null.
  if (made.trustHead === null) return true;

  // The head has to be a commit in *this repository's trust log*. Ancestry
  // alone proves nothing about an oid nobody vouched for: a fabricated one, or
  // the tip of `main`, reaches no trust record and so matches no revocation —
  // indistinguishable from an event that genuinely predates it. Naming any oid
  // would otherwise be a way out of every forward-only revocation.
  if (!(yield* held(made.trustHead))) return true;

  // Every window, not merely the latest: a key revoked, let back in and
  // revoked again was out for two separate intervals, and an event made in the
  // first is not rescued by the second. It is enough to fall inside one.
  // A head whose ancestry this host will not walk is a head it cannot show
  // anything about — including that the event predates a revocation. Read as
  // "reaches nothing", the answer is not conservative but the opposite: every
  // forward-only revocation becomes invisible and the event counts. So an
  // ancestry it cannot compute is an event this refuses.
  const visible = yield* seen(made.trustHead).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  if (visible === null) return true;
  for (const revocation of revocations) {
    // A revocation that a later grant ended still covers its own window. An
    // event that can show it was made after the key came back is outside it;
    // one that cannot is inside, which is where every signature made *while*
    // revoked lives.
    if (revocation.supersededBy !== null && visible.has(revocation.supersededBy)) continue;

    // A compromise reaches everything the key signed, without consulting the
    // event's own `issuedAt`. That field is written by whoever holds the key,
    // and under a compromise that is the attacker — so comparing against it
    // would let them backdate their way out of the one revocation class meant
    // to reach backwards. `compromisedFrom` stays on the record as the
    // operator's account of when it began; it is not a verification input.
    // Except where the verdict has to be permanent. Reaching backwards moves
    // an answer that has already been acted on irreversibly: the host that
    // honoured a tombstone deleted the payload, and a later `compromised`
    // revocation that un-honours it leaves that host folding an event every
    // replica still holding the blob folds in full — a disagreement about the
    // same protected-branch push, arrived at from the same log. Forward reach
    // still applies below, so the key stops being able to make new ones.
    if (revocation.compromisedFrom !== null && !permanent) return true;

    // Past the window's end the check above has already answered; what is left
    // is an event that cannot show it, and it is judged against the revocation
    // as if the grant that ended it had not happened.
    if (visible.has(revocation.commit)) return true;
  }
  return false;
});

/**
 * Whether the grant a member's capabilities come from was already visible.
 *
 * The mirror of `reaches`, and for the same reason: "had the author already
 * seen this?" is a question about ancestry, which every replica computes the
 * same way, rather than about clocks anybody can write. A live request is
 * judged against the current state, so it needs no check; an event recording
 * no trust head is treated as having seen everything, which is the reading
 * that field gets everywhere else.
 */
const held = Effect.fn("trust.Verify.held")(function* (
  member: Member,
  capability: string,
  made: Made,
  seen: Ancestry,
) {
  // No trust head recorded is "they had seen everything", the reading that
  // field gets everywhere else — so the question falls back to what they hold.
  if (made.trustHead === null) return permits(member.capabilities, capability);

  // The *latest* grant visible from that head, which is what they held then.
  //
  // Not "any grant that ever conferred it": that would make capabilities
  // impossible to narrow, since the superseded grant stays in the history
  // forever. And not `member.capabilities`, which is the latest grant full
  // stop — that made a renewal un-authorize everything signed before it, and
  // a downgrade stricter than a revocation. `history` is in log order, so the
  // last reachable entry is the one in force.
  // As in `reaches`, and to the same end: an ancestry this host will not walk
  // shows no grant, and a grant it cannot show is one the event does not get.
  const ancestors = yield* seen(made.trustHead).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  if (ancestors === null) return false;
  for (let at = member.history.length - 1; at >= 0; at--) {
    const record = member.history[at]!;
    if (record.commit !== made.trustHead && !ancestors.has(record.commit)) continue;
    return permits(record.capabilities, capability);
  }
  return false;
});

/**
 * Authorize a signed payload for one capability.
 *
 * Returns the member rather than a boolean: the caller's next step is almost
 * always to record *who* — a review's author, a push's principal — and
 * recovering that from a second lookup is how the two come apart.
 */
export const authorize = Effect.fn("trust.Verify.authorize")(function* (input: {
  readonly projection: Projection;
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
  readonly capability: string;
  /** Absent for a live request; present when judging a stored event. */
  readonly made?: Made;
  /**
   * Trust-log ancestry, memoised by the caller.
   *
   * `Log.ancestry` walks the log — a read per commit — and a fold judging a
   * hundred events asks about the same handful of heads every time. The log
   * does not move while a projection is being built, so a caller folding one
   * hands its own memo in; a one-off caller gets the plain walk.
   */
  readonly seen?: Ancestry;
  /** As `seen`, for "is this a trust-log commit?"; see `Membership`. */
  readonly contains?: Membership;
  /**
   * Who signed, when the caller has already worked it out.
   *
   * Signature verification is the expensive half of this, and it is over
   * attacker-supplied input on a synchronous path. A caller asking twice about
   * one event — "may they comment?", then "do they also hold `hub.merge`?" —
   * was paying for every signature again to answer a question about the same
   * bytes, up to `MAX_SIGNATURES` of them per extra ask.
   */
  readonly signed?: ReadonlyArray<Fingerprint>;
  /**
   * Whether this verdict has to hold for good.
   *
   * An answer that moves is fine for a statement that is only ever re-read —
   * an approval that stops counting when its author's grant expires is the
   * conservative reading, and nothing was destroyed to reach it. It is not
   * fine for one the repository *acts* on irreversibly. A redaction tombstone
   * is the only such statement: honouring it deletes bytes, and a verdict that
   * can be withdrawn afterwards leaves the host that acted folding a history
   * no replica agrees with, and `gc` re-protecting a payload the operator was
   * told was gone.
   *
   * So a permanent verdict is judged against facts that only ever accumulate:
   * the capabilities the signer held at the trust head the event names, and
   * the revocations reachable from it. Wall-clock expiry is not consulted, and
   * a `compromised` revocation does not reach backwards past it. The live gate
   * still refuses an expired or revoked member a *new* tombstone; what this
   * fixes is the reading of one already on the record.
   */
  readonly permanent?: boolean;
}) {
  const made = input.made ?? null;
  const ancestry = input.seen ?? Log.ancestry;
  const membership = input.contains ?? Log.contains;
  const found = input.signed ?? (yield* signers(input.bytes, input.signatures));
  if (found.length === 0) return denied("no valid signature");

  // A verdict that has to hold for good has to say what it was judged against.
  // `trustHead` is nullable and means "had seen everything", which every other
  // reading treats as the conservative one — but here it makes the answer move:
  // `reaches` refuses such an event the moment its signer is revoked at all,
  // and `held` falls back to the *latest* grant, which a narrowing re-grant
  // shrinks. Either turns a tombstone this repository already acted on into
  // one it no longer honours, which is the divergence `permanent` exists to
  // remove. Refused instead, and refused the same way on every replica for
  // ever.
  if (input.permanent === true && made !== null && made.trustHead === null) {
    return denied("a redaction must record the trust head it was signed against");
  }

  let closest = "signer is not a member of this repository";
  for (const signer of found) {
    if (
      yield* reaches(input.projection, signer, made, ancestry, membership, input.permanent === true)
    ) {
      closest = `${signer} has been revoked`;
      continue;
    }

    // `former` covers the signer whose revocation exists but does not reach
    // this event: they were a member when they made it, and that is the
    // membership the event has to be judged against.
    const member = input.projection.members.get(signer) ?? input.projection.former.get(signer);
    if (member === undefined) continue;

    // Expiry is judged against the clock, never against `made.at`: that field
    // is the signer's own `issuedAt`, and backdating it would revive an
    // expired grant. The cost is that an expired member's past events stop
    // counting, which is the conservative reading and the one an attacker
    // cannot arrange.
    if (
      input.permanent !== true &&
      member.expiresAt !== null &&
      member.expiresAt.getTime() <= Date.now()
    ) {
      closest = `${signer}'s membership expired on ${member.expiresAt.toISOString()}`;
      continue;
    }

    // A live request is judged against what they hold now; a stored event
    // against what they held *then*, and only `held` can answer that. Asking
    // the current capabilities first — as this did — meant a *narrowing* grant
    // retroactively un-authorized every event its subject had already signed,
    // which made a downgrade stricter than a revocation: a full revocation
    // preserves those events through `former` and ancestry, and losing one
    // capability erased them.
    if (made === null) {
      if (!permits(member.capabilities, input.capability)) {
        closest = `${signer} does not hold ${input.capability}`;
        continue;
      }
    } else if (!(yield* held(member, input.capability, made, ancestry))) {
      closest = `${signer} did not hold ${input.capability} when this was signed`;
      continue;
    }

    return { ok: true, principal: member } as const;
  }

  return denied(closest);
});

/**
 * The same question for a key that has already been identified — the
 * authentication path, where possession was proved by a challenge rather than
 * by a signature over a payload.
 */
export const authorizeKey = Effect.fn("trust.Verify.authorizeKey")(function* (input: {
  readonly projection: Projection;
  readonly signer: Fingerprint;
  readonly capability: string;
  readonly at?: Date;
}) {
  if (yield* reaches(input.projection, input.signer, null, Log.ancestry, Log.contains)) {
    return denied(`${input.signer} has been revoked`);
  }

  const member = input.projection.members.get(input.signer);
  if (member === undefined) return denied(`${input.signer} is not a member of this repository`);

  const when = input.at ?? new Date();
  if (member.expiresAt !== null && member.expiresAt.getTime() <= when.getTime()) {
    return denied(`${input.signer}'s membership expired on ${member.expiresAt.toISOString()}`);
  }
  if (!permits(member.capabilities, input.capability)) {
    return denied(`${input.signer} does not hold ${input.capability}`);
  }
  return { ok: true, principal: member } as const;
});

/**
 * Whether the view is fresh enough to act on.
 *
 * A hash-linked log makes withholding visible but not impossible: a replica
 * can still serve a consistent view that stops short of a revocation. A
 * checkpoint is a signed statement that somebody with authority had seen a
 * given frontier at a given time, so requiring a recent one bounds how far
 * behind a served view may be. Callers that do not care pass no age.
 */
export type Freshness = { readonly ok: true } | { readonly ok: false; readonly reason: string };

/**
 * How far ahead of us a checkpoint's clock may be and still be believed.
 *
 * `at` is written by whoever signed the checkpoint, and `project` keeps the one
 * with the greatest `at` — so an age allowed to go negative made a single
 * forward-dated attestation satisfy `maxTrustAgeSeconds` for as long as it was
 * dated ahead, which is precisely the withheld-view bound this exists to
 * enforce, defeated by typing a date. Ordinary clock skew between hosts is
 * seconds, not hours; past this, a checkpoint is not evidence of anything.
 */
const SKEW = 300_000;

export const fresh = (
  projection: Projection,
  maxAge: number,
  now: Date = new Date(),
): Freshness => {
  if (projection.checkpoints.length === 0) {
    return {
      ok: false,
      reason: "this repository has no trust checkpoint; its membership view may be stale",
    };
  }
  // The newest *credible* one, not simply the newest. A single attestation
  // dated ahead — a malicious admin, or a CI box with a fast clock — would
  // otherwise be the only one this ever looked at, and refusing it would then
  // refuse every write on a repository with `maxTrustAgeSeconds` set, the write
  // to `refs/meta/policy` that lifts the bound included. Skipping past it costs
  // nothing: a checkpoint nobody can believe is not evidence either way.
  let ahead = 0;
  for (const checkpoint of projection.checkpoints) {
    const age = now.getTime() - checkpoint.at.getTime();
    if (age < -SKEW) {
      ahead++;
      continue;
    }
    return age <= maxAge
      ? { ok: true }
      : { ok: false, reason: `the newest trust checkpoint is ${Math.floor(age / 1000)}s old` };
  }
  return {
    ok: false,
    reason: `every trust checkpoint on record (${ahead}) is dated in the future`,
  };
};

export type VerifyError = Invalid | ObjectNotFound | StorageFailure;
