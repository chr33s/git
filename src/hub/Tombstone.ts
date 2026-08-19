/**
 * Removing a record from a namespace that has no fold.
 *
 * A pull request's tombstone is judged by `Projection.ts`, which verifies
 * signatures, tracks a trust floor and settles concurrency — machinery a pull
 * request needs for other reasons and which redaction rides along on. Sessions
 * and tasks have none of it: their projections read what is there and say so,
 * because nothing downstream authorizes anything on the strength of what an
 * agent said about its own work.
 *
 * Redaction is the exception, and the reason is that it is the one operation
 * whose effect is irreversible and reaches other people's records. So the two
 * questions a tombstone raises are answered here, once, for both namespaces:
 * may this key write one, and does a written one count. Both are the trust
 * graph's answer rather than the namespace's, which is why neither lives in
 * `Session.ts` or `Task.ts`.
 *
 * The bytes are not deleted here or by either caller. A tombstone is a signed
 * statement that replicates; the payload goes at the next `gc`, which is the
 * only place that can tell whether the blob is also reachable from somewhere
 * else — git dedupes by content, and a prompt that matches a file in a tree is
 * the same object. See `Redaction.ts`.
 */
import { Effect, Schema } from "effect";

import { fingerprint, type PrivateKey } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { permits } from "../trust/Certificate.ts";
import type { Fingerprint } from "../crypto/SshSignature.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { openWindow, project as projectTrust } from "../trust/Projection.ts";
import type { Projection as TrustProjection } from "../trust/Projection.ts";

/** The one tag all three record namespaces spell the same way. */
export const TAG = "event.redacted";

const Tagged = Schema.Struct({ type: Schema.String });
const decodeTagged = Schema.decodeUnknownEffect(Tagged);

/**
 * Whether these record bytes claim to be a tombstone, whoever wrote them.
 *
 * The boundary asks this of every record on every `refs/hub/**` ref, and the
 * three namespaces wrap one tag in three different envelopes — so reading the
 * tag alone is what lets one gate cover all of them, rather than three
 * decoders and a list of which ref prefixes have been remembered.
 *
 * It fails closed: bytes that merely claim the tag are charged the capability.
 * For a check whose subject is an irreversible deletion that is the safe
 * direction, and the cost is that a record which is not really a tombstone
 * needs `hub.redact` to push — which is a record nobody writes by accident.
 */
export const claims = Effect.fn("hub.Tombstone.claims")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(new TextDecoder().decode(bytes)),
    catch: () => new Invalid({ field: "record", reason: "not JSON" }),
  }).pipe(Effect.orElseSucceed(() => null));
  if (json === null) return false;

  const tagged = yield* decodeTagged(json).pipe(Effect.orElseSucceed(() => null));
  return tagged?.type === TAG;
});

/**
 * The fields a tombstone carries, whatever namespace writes it.
 *
 * Spread into each namespace's own envelope rather than shared as a schema:
 * the envelope is what says which session or task the record belongs to, and a
 * tombstone that could be read without one would be a record with no ref.
 *
 * `targetCommit` is what resolves the removal and `target` is what a person
 * reads. An id alone stops resolving the moment the removal happens — the
 * payload it was read from is the payload that was deleted — so a projection
 * rebuilt afterwards could no longer tell which commit was meant, and the blob
 * would quietly stop being excluded from collection.
 */
export const fields = {
  /** The commit carrying the record being removed, hash-qualified. */
  targetCommit: Schema.String,
  /** The record's own event id, for a reader. */
  target: Schema.String,
  reason: Schema.String,
};

/**
 * Whether this key may write a tombstone here, as of now.
 *
 * Asked before the event is written rather than after. A record namespace is
 * append-only, so a tombstone that turns out not to count is one every future
 * reader pays to walk past and no reader can remove — and the signer learns
 * nothing from it either way.
 *
 * Expiry is part of the question, and that is not belt-and-braces: `counts`
 * below deliberately does not consult a clock, so an expired holder's
 * tombstone would count forever once it existed. The boundary refuses such an
 * event over the wire; without this, the local command would be the one door
 * that let a lapsed member drive an irreversible deletion.
 */
export const permitted = Effect.fn("hub.Tombstone.permitted")(function* (key: PrivateKey) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({ field: "repo", reason: "this repository has no genesis" });
  }

  const trust = yield* projectTrust(stored.genesis);
  const signer = yield* fingerprint(key.publicKey);
  const member = trust.members.get(signer);
  const expired = member?.expiresAt !== null && (member?.expiresAt?.getTime() ?? 0) <= Date.now();
  if (
    member === undefined ||
    expired ||
    openWindow(trust.revoked.get(signer)) !== null ||
    !permits(member.capabilities, "hub.redact")
  ) {
    return yield* new Invalid({
      field: "key",
      reason: `${signer} may not redact records here; that needs hub.redact`,
    });
  }
  return trust;
});

/**
 * Whether a tombstone that exists counts — the question `gc` asks.
 *
 * "Ever held `hub.redact`", not "holds it now", and the difference is
 * deliberate. This answer decides what is deleted, so it has to be monotone:
 * a host that has already dropped a payload must not later decide the
 * tombstone was invalid, because it cannot get the bytes back and would be
 * folding a history no replica agrees with. A narrowed grant is handled where
 * "now" is knowable — the boundary refuses the push — and expiry the same way.
 */
export const counts = (trust: TrustProjection, signers: ReadonlyArray<Fingerprint>): boolean =>
  signers.some((signer) => {
    const member = trust.members.get(signer);
    if (member === undefined) return false;
    return member.history.some((grant) => permits(grant.capabilities, "hub.redact"));
  });
