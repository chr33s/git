/**
 * What one key may do here, and what its push will be judged by.
 *
 * The answer a client otherwise learns by being refused: push, read the
 * refusal, adjust, push again. That is an expensive protocol for anything that
 * pays for discovery in tokens, and it is avoidable — the repository already
 * knows the answer, in the two places the boundary itself reads it from. This
 * joins them and says so.
 *
 * Shared rather than written twice because the CLI answers about a repository
 * on this disk and the JSON verb answers about the credential presented to a
 * host, and those must not drift: an answer that disagreed with the
 * enforcement would be worse than no answer, since a caller would act on it.
 */
import { Effect } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { openWindow, type Projection } from "../trust/Projection.ts";
import { permits } from "../trust/Certificate.ts";
import * as Verify from "../trust/Verify.ts";
import * as Session from "../hub/Session.ts";
import * as Policy from "./Policy.ts";
import { WhoamiAnswer, WhoamiVerdict } from "./ApiContract.ts";

/** Whether a push to one ref would get through, and what it answers to. */
export const Verdict = WhoamiVerdict;

/**
 * Every field is present on every answer, `null` where it does not apply.
 *
 * The reader is a program deciding what to do next, and a field that vanishes
 * is one it has to guess the meaning of.
 */
export const Answer = WhoamiAnswer;

export type Answer = (typeof Answer)["Type"];
export type Verdict = (typeof Verdict)["Type"];

/** What one key holds here, and why it may not write at all. */
export interface Standing {
  readonly capabilities: ReadonlyArray<string>;
  readonly expiresAt: Date | null;
  readonly refusal: string | null;
}

/** The standing of a key this repository has never heard of, or cannot use. */
export const withoutMembership = (refusal: string): Standing => ({
  capabilities: [],
  expiresAt: null,
  refusal,
});

/**
 * What the trust fold says about one key, now.
 *
 * Expiry is judged here rather than read off the projection, because the
 * projection keeps expired members on purpose — a record signed while a grant
 * was live stays authorized by it — and this asks the other question: what may
 * this key do at this moment.
 */
export const standingOf = (
  projection: Projection,
  subject: Fingerprint,
  now: Date,
  capabilities?: ReadonlyArray<string>,
): Standing => {
  const member = projection.members.get(subject);
  if (member === undefined) {
    const windows = projection.revoked.get(subject);
    const out = windows === undefined ? null : openWindow(windows);
    return withoutMembership(
      out === null
        ? "this key is not a member of this repository"
        : `this key is revoked (${out.reason})`,
    );
  }
  return {
    // A delegated credential narrows what the member holds, so where the
    // caller knows the narrowed set it is the true answer to "what may this
    // *request* do" — which is the question a credential holder is asking.
    capabilities: capabilities ?? member.capabilities,
    expiresAt: member.expiresAt,
    refusal:
      member.expiresAt !== null && member.expiresAt.getTime() <= now.getTime()
        ? `this grant expired ${member.expiresAt.toISOString()}; ask for a fresh one`
        : null,
  };
};

/** What a protected branch asks of a push, in the order a caller meets them. */
const requirementsOf = (rules: Policy.Rules): ReadonlyArray<string> => {
  const why: Array<string> = [];
  if (rules.requirePullRequest) why.push("requirePullRequest");
  if (rules.requiredApprovals > 0) why.push(`requiredApprovals: ${rules.requiredApprovals}`);
  if (rules.requiredChecks.length > 0) {
    why.push(`requiredChecks: [${rules.requiredChecks.join(", ")}]`);
  }
  if (rules.requireResolvedThreads) why.push("requireResolvedThreads");
  return why;
};

/**
 * What a branch offers *besides* a direct push of an approved revision.
 *
 * Kept apart from `requirementsOf` deliberately. `verdictFor` reads a non-empty
 * requirement list as "a direct push meets none of these", which is exactly
 * what `Policy.protectedBranch` does — it returns early when no requirement is
 * set. Listed among them, `queueCandidates` made a branch with no review
 * requirements at all report itself as refusing pushes the boundary allows: a
 * setting that widens what may land, reported as though it narrowed it.
 */
const alternativesOf = (rules: Policy.Rules): ReadonlyArray<string> =>
  // And not while provenance is required, where the boundary refuses every
  // candidate for want of a session trailer and `queue run` refuses the target
  // by name — nor at a depth of zero, where `candidateChain` short-circuits
  // before it looks at anything. Advertising a route nothing can take is worse
  // than advertising none: it is the answer an agent would act on.
  rules.queueCandidates && rules.queueDepth > 0 && !rules.requireProvenance
    ? [`or a queue candidate, up to ${String(rules.queueDepth)} deep`]
    : [];

/**
 * The standing verdict for one ref, before any particular push exists.
 *
 * A protected branch carrying requirements refuses a direct push whatever the
 * revision is, and one carrying none still refuses a delete or a force. What
 * this cannot say is whether some future revision satisfies an approval or a
 * check — that is a question about a push nobody has made yet.
 */
const verdictFor = (input: {
  readonly rules: Policy.Rules;
  readonly protectedRef: boolean;
  readonly standing: Standing;
  readonly stale: string | null;
}): Verdict => {
  if (input.standing.refusal !== null) return { push: "refused", why: [input.standing.refusal] };
  if (input.stale !== null) return { push: "refused", why: [input.stale] };
  if (!permits(input.standing.capabilities, "source.push")) {
    return { push: "refused", why: ["source.push is not granted to this key"] };
  }
  if (!input.protectedRef) return { push: "allowed", why: [] };

  const requirements = requirementsOf(input.rules);
  return requirements.length === 0
    ? { push: "allowed", why: ["protected: no force-push, no deletion"] }
    : {
        push: "refused",
        why: [
          ...requirements,
          "a direct push meets none of these; open a pull request",
          ...alternativesOf(input.rules),
        ],
      };
};

/** The name the answer gives to every ref no protection rule covers. */
export const OTHERWISE = "(any other ref)";

/**
 * The answer, from a projection the caller already holds.
 *
 * Rules are read whether or not there is a genesis: a repository may publish
 * branch rules before it has an identity, and the boundary honours them there,
 * so an answer that skipped them would understate what a push has to satisfy.
 */
export const answer = Effect.fn("Whoami.answer")(function* (input: {
  readonly subject: Fingerprint | null;
  readonly repoId: RepoId | null;
  readonly projection: Projection | null;
  readonly held: Standing;
  readonly now?: Date;
}) {
  const rules = yield* Policy.rulesOf();
  const now = input.now ?? new Date();

  // Read only where it is set: it is a walk of every session, which a
  // repository that bounds nothing should not pay for to be told its standing.
  // Surfaced here rather than enforced at the boundary, because what it counts
  // is self-reported — an agent that can see the line coming stops at it,
  // which is all an advisory bound can honestly ask for.
  const budget =
    rules.maxUsageTokens <= 0
      ? null
      : yield* Effect.gen(function* () {
          const window =
            rules.usageWindowSeconds > 0
              ? new Date(now.getTime() - rules.usageWindowSeconds * 1000)
              : new Date(0);
          const spent = yield* Session.usageSince(window);
          return {
            maxUsageTokens: rules.maxUsageTokens,
            windowSeconds: rules.usageWindowSeconds,
            usedTokens: spent.total,
            remainingTokens: Math.max(0, rules.maxUsageTokens - spent.total),
          };
        });

  const freshness =
    input.projection === null || rules.maxTrustAgeSeconds <= 0
      ? null
      : Verify.fresh(input.projection, rules.maxTrustAgeSeconds * 1000, now);
  const stale = freshness === null || freshness.ok ? null : freshness.reason;

  const branches: Record<string, Verdict> = {};
  for (const pattern of rules.protected) {
    branches[pattern] = verdictFor({ rules, protectedRef: true, standing: input.held, stale });
  }
  // Asked as its own case rather than by matching a wildcard: the boundary
  // counts two overlapping prefixes as a match, which is the conservative
  // reading for a write that names a whole namespace and the wrong one here —
  // it would report every unprotected branch as protected the moment one
  // branch under it was.
  branches[OTHERWISE] = verdictFor({ rules, protectedRef: false, standing: input.held, stale });

  return {
    repo: input.repoId,
    subject: input.subject,
    member: input.held.refusal === null,
    why: input.held.refusal,
    capabilities: input.held.capabilities,
    expiresAt: input.held.expiresAt === null ? null : input.held.expiresAt.toISOString(),
    trust:
      freshness === null
        ? null
        : {
            maxTrustAgeSeconds: rules.maxTrustAgeSeconds,
            fresh: freshness.ok,
            reason: freshness.ok ? null : freshness.reason,
          },
    budget,
    branches,
  } satisfies Answer;
});
