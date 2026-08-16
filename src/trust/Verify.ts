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
import type { Oid } from "../git/Store.ts";
import { permits } from "./Certificate.ts";
import * as Log from "./Log.ts";
import type { Member, Projection } from "./Projection.ts";

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
  for (const armored of signatures) {
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
) {
  const revocation = projection.revoked.get(subject);
  if (revocation === undefined) return false;

  // A live request is held to the current state: revoked is revoked.
  if (made === null) return true;

  // A compromise reaches everything the key signed, without consulting the
  // event's own `issuedAt`. That field is written by whoever holds the key,
  // and under a compromise that is the attacker — so comparing against it
  // would let them backdate their way out of the one revocation class meant
  // to reach backwards. `compromisedFrom` stays on the record as the
  // operator's account of when it began; it is not a verification input.
  if (revocation.compromisedFrom !== null) return true;

  // Forward-only. An event that records no trust head cannot show it predates
  // the revocation, so it does not get the benefit of the doubt.
  if (made.trustHead === null) return true;

  const seen = yield* Log.ancestry(made.trustHead);
  return seen.has(revocation.commit);
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
}) {
  const made = input.made ?? null;
  const found = yield* signers(input.bytes, input.signatures);
  if (found.length === 0) return denied("no valid signature");

  let closest = "signer is not a member of this repository";
  for (const signer of found) {
    if (yield* reaches(input.projection, signer, made)) {
      closest = `${signer} has been revoked`;
      continue;
    }

    // `former` covers the signer whose revocation exists but does not reach
    // this event: they were a member when they made it, and that is the
    // membership the event has to be judged against.
    const member = input.projection.members.get(signer) ?? input.projection.former.get(signer);
    if (member === undefined) continue;

    const when = made?.at ?? new Date();
    if (member.expiresAt !== null && member.expiresAt.getTime() <= when.getTime()) {
      closest = `${signer}'s membership expired on ${member.expiresAt.toISOString()}`;
      continue;
    }

    if (!permits(member.capabilities, input.capability)) {
      closest = `${signer} does not hold ${input.capability}`;
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
  if (yield* reaches(input.projection, input.signer, null)) {
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

export const fresh = (
  projection: Projection,
  maxAge: number,
  now: Date = new Date(),
): Freshness => {
  if (projection.checkpoint === null) {
    return {
      ok: false,
      reason: "this repository has no trust checkpoint; its membership view may be stale",
    };
  }
  const age = now.getTime() - projection.checkpoint.at.getTime();
  return age <= maxAge
    ? { ok: true }
    : { ok: false, reason: `the newest trust checkpoint is ${Math.floor(age / 1000)}s old` };
};

export type VerifyError = Invalid | ObjectNotFound | StorageFailure;
