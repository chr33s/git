/** Identity-verified reviews whose authors are outside project membership. */
import { Effect } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import type { PullRequest, Review } from "../hub/Projection.ts";
import type { Genesis } from "../trust/Genesis.ts";
import {
  containsIdentityKey,
  identifyKey,
  isPrincipalId,
  type PrincipalId,
} from "../trust/Principal.ts";
import * as Verify from "../trust/Verify.ts";

export interface ExternalReview extends Review {
  readonly principal: PrincipalId;
}

export interface RejectedExternalReview {
  readonly commit: Oid;
  readonly reason: string;
  readonly quarantined: boolean;
}

export interface ExternalReviews {
  readonly reviews: ReadonlyArray<ExternalReview>;
  readonly rejected: ReadonlyArray<RejectedExternalReview>;
}

/**
 * Read only explicitly attributed external review events.
 *
 * This is separate from the ordinary hub projection on purpose: resolving a
 * key through an identity repository proves who spoke, but grants no project
 * capability. Policy may opt in to counting the result later.
 */
export const externalReviews = Effect.fn("social.Review.externalReviews")(function* (
  genesis: Genesis,
  pullRequest: PullRequest,
  pr: string,
  options?: { readonly maxIdentityAgeMs?: number; readonly now?: Date },
) {
  const walked = yield* Event.entries(pr);
  const latest = new Map<PrincipalId, ExternalReview>();
  const rejected: RejectedExternalReview[] = [];

  entries: for (const entry of walked.events) {
    const payload = entry.payload;
    if (
      payload?.type !== "review.submitted" ||
      payload.principal === undefined ||
      !isPrincipalId(payload.principal)
    ) {
      continue;
    }

    const invalid = yield* Event.validate(payload, genesis.repoId).pipe(
      Effect.as(null),
      Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
    );
    if (invalid !== null) {
      rejected.push({ commit: entry.commit, reason: invalid, quarantined: false });
      continue;
    }

    const head = Event.unqualify(payload.head);
    if (
      head === null ||
      pullRequest.head === null ||
      head !== pullRequest.head ||
      payload.base !== pullRequest.base
    ) {
      continue;
    }

    // Self-review is about the stable person, not one device fingerprint.
    // Without this, opening from a laptop and approving from a phone counted
    // as two people as soon as both keys belonged to one identity repository.
    const localAuthors = new Set([
      ...pullRequest.openers,
      ...pullRequest.reviews.map((review) => review.author),
    ]);
    const localPrincipals = new Set([
      ...pullRequest.openerPrincipals,
      ...pullRequest.reviews.flatMap((review) =>
        review.principal === null ? [] : [review.principal],
      ),
    ]);
    if (localPrincipals.has(payload.principal)) {
      rejected.push({
        commit: entry.commit,
        reason: `${payload.principal} already authored this proposal or a local review`,
        quarantined: false,
      });
      continue;
    }
    for (const local of localAuthors) {
      const relation = yield* containsIdentityKey(payload.principal, local);
      if (relation.belongs) {
        rejected.push({
          commit: entry.commit,
          reason: `${payload.principal} already authored this proposal or a local review`,
          quarantined: false,
        });
        continue entries;
      }
    }

    const signed = [...(yield* Verify.signers(entry.bytes, entry.signatures))].sort();
    let author: Fingerprint | null = null;
    let quarantine: string | null = null;
    let moved: string | null = null;
    for (const signer of signed) {
      if (pullRequest.openers.has(signer)) continue;
      const identity = yield* identifyKey({
        principal: payload.principal,
        signer,
      });
      if (identity.ok) {
        if ((options?.maxIdentityAgeMs ?? 0) > 0) {
          const freshness = Verify.fresh(
            identity.identity.projection,
            options?.maxIdentityAgeMs ?? 0,
            options?.now ?? new Date(),
          );
          if (!freshness.ok) {
            moved = `${payload.principal}'s identity view is stale: ${freshness.reason}`;
            continue;
          }
        }
        if (identity.identity.head !== payload.identityHead) {
          moved = `${payload.principal} is now at ${identity.identity.head ?? "an empty log"}, not the pinned identity head ${payload.identityHead ?? "none"}`;
          continue;
        }
        author = signer;
        break;
      }
      if (identity.quarantined) quarantine ??= identity.reason;
    }
    if (author === null) {
      rejected.push({
        commit: entry.commit,
        reason:
          moved ?? quarantine ?? `${payload.principal} did not sign with a current identity key`,
        quarantined: quarantine !== null,
      });
      continue;
    }

    latest.set(payload.principal, {
      principal: payload.principal,
      // An external review speaks for exactly the principal it names — the
      // whole route here is identity, never a direct key of this repository.
      claims: [payload.principal],
      id: payload.id,
      author,
      head,
      base: payload.base,
      commit: entry.commit,
      decision: payload.decision,
      body: payload.body,
      at: new Date(payload.issuedAt),
      dismissed: false,
      stale: false,
    });
  }

  return {
    reviews: [...latest.values()].filter((review) => review.decision === "approve"),
    rejected,
  } satisfies ExternalReviews;
});
