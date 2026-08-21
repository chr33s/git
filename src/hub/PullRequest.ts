/**
 * The operations a person performs on a pull request.
 *
 * `Event.ts` is the format and the history; this is the verb layer over it, so
 * that a caller says "approve this revision" rather than assembling an
 * envelope. Every operation is the same three steps — build a payload, sign
 * it, append it — and each returns the commit it wrote, which is what a caller
 * needs to report or to compare-and-swap against.
 *
 * Nothing here checks authority. That is deliberate: a signature by a key with
 * no capability produces an event the projection rejects, and the projection
 * is where the rule lives so that a replica which never ran this code reaches
 * the same verdict. Refusing early here as well would be a second copy of the
 * rule, and the two would eventually disagree.
 */
import { DateTime, Effect } from "effect";

import { fingerprint, type PrivateKey } from "../crypto/SshSignature.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { readGenesis, type RepoId } from "../trust/Genesis.ts";
import type { PrincipalId } from "../trust/Principal.ts";
import { permits } from "../trust/Certificate.ts";
import { openWindow, project as projectTrust } from "../trust/Projection.ts";
import { LOG_REF } from "../trust/Log.ts";
import * as Event from "./Event.ts";
import { project } from "./Projection.ts";

/**
 * What every operation needs to know about where it is being made.
 *
 * `trustHead` is read rather than passed: the caller cannot be expected to
 * know it, and getting it wrong is how an event ends up claiming its author
 * had not seen a revocation they had.
 */
const context = Effect.fn("hub.PullRequest.context")(function* (repo: RepoId, pr: string) {
  const repository = yield* Repository;
  const trustHead = yield* repository.resolve(LOG_REF);
  return {
    version: 1,
    repo,
    pr,
    id: Event.newId(),
    issuedAt: DateTime.formatIso(yield* DateTime.now),
    trustHead,
  } as const;
});

export interface OpenInput {
  readonly repo: RepoId;
  readonly title: string;
  readonly description?: string;
  /** The branch this asks to change. */
  readonly base: string;
  readonly head: Oid;
  readonly key: PrivateKey;
  /** Supplied only when a caller is reproducing a known pull request. */
  readonly id?: string;
}

/** Open a pull request, and return the identifier the ref is named for. */
export const open = Effect.fn("hub.PullRequest.open")(function* (input: OpenInput) {
  const pr = input.id ?? Event.newId();
  if (!Event.isPullRequestId(pr)) {
    return yield* new Invalid({
      field: "id",
      reason: `'${pr}' cannot name a pull request; it must be one ref path component`,
    });
  }
  const base = yield* context(input.repo, pr);

  const commit = yield* Event.issue(
    {
      ...base,
      type: "pr.opened",
      title: input.title,
      description: input.description ?? "",
      base: input.base,
      head: Event.qualify(input.head),
    },
    input.key,
  );
  return { pr, commit };
});

export const update = Effect.fn("hub.PullRequest.update")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly head: Oid;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    { ...base, type: "pr.updated", head: Event.qualify(input.head) },
    input.key,
  );
});

export const close = Effect.fn("hub.PullRequest.close")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue({ ...base, type: "pr.closed" }, input.key);
});

export const reopen = Effect.fn("hub.PullRequest.reopen")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue({ ...base, type: "pr.reopened" }, input.key);
});

/**
 * Record that a pull request was merged.
 *
 * The event does not perform the merge — moving the branch is the policy
 * boundary's job, and it happens under a compare-and-swap this cannot see.
 * This is the statement that it happened, naming both what was merged and what
 * it became.
 */
export const merged = Effect.fn("hub.PullRequest.merged")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly head: Oid;
  readonly mergeCommit: Oid;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    {
      ...base,
      type: "pr.merged",
      head: Event.qualify(input.head),
      mergeCommit: Event.qualify(input.mergeCommit),
    },
    input.key,
  );
});

export const review = Effect.fn("hub.PullRequest.review")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  /** The exact revision reviewed — an approval is never of a pull request. */
  readonly head: Oid;
  readonly decision: "approve" | "reject" | "comment";
  readonly body?: string;
  /** Present only when this is a federated review outside project membership. */
  readonly principal?: PrincipalId;
  /** Required with `principal`; external reviews pin their destination. */
  readonly base?: string;
  /** Required with `principal`; pins the identity view authorizing its device key. */
  readonly identityHead?: Oid;
  readonly key: PrivateKey;
}) {
  const externalBase = input.base;
  if (
    input.principal !== undefined &&
    (externalBase === undefined || input.identityHead === undefined)
  ) {
    return yield* new Invalid({
      field: externalBase === undefined ? "base" : "identityHead",
      reason:
        externalBase === undefined
          ? "an external review must name the branch it reviews"
          : "an external review must pin the identity trust-log head it was signed against",
    });
  }
  const base = yield* context(input.repo, input.pr);
  const payload = {
    ...base,
    type: "review.submitted" as const,
    head: Event.qualify(input.head),
    decision: input.decision,
    body: input.body ?? "",
  };
  if (input.principal === undefined) {
    if (input.identityHead !== undefined) {
      return yield* new Invalid({
        field: "identityHead",
        reason: "only an external review may pin an identity trust-log head",
      });
    }
    return yield* Event.issue(payload, input.key);
  }
  if (externalBase === undefined || input.identityHead === undefined) {
    return yield* new Invalid({
      field: externalBase === undefined ? "base" : "identityHead",
      reason: "an external review must pin its branch and identity trust-log head",
    });
  }
  return yield* Event.issue(
    {
      ...payload,
      principal: input.principal,
      base: Event.branchRef(externalBase),
      identityHead: input.identityHead,
    },
    input.key,
  );
});

export const dismissReview = Effect.fn("hub.PullRequest.dismissReview")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly review: string;
  readonly reason?: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    { ...base, type: "review.dismissed", review: input.review, reason: input.reason ?? "" },
    input.key,
  );
});

export interface CommentInput {
  readonly repo: RepoId;
  readonly pr: string;
  readonly body: string;
  readonly key: PrivateKey;
  /** An inline comment records where it was made, and against what. */
  readonly head?: Oid;
  readonly path?: string;
  readonly side?: "old" | "new";
  readonly line?: number;
  readonly contextHash?: string;
}

export const comment = Effect.fn("hub.PullRequest.comment")(function* (input: CommentInput) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    {
      ...base,
      type: "comment.created",
      body: input.body,
      head: input.head === undefined ? null : Event.qualify(input.head),
      path: input.path ?? null,
      side: input.side ?? null,
      line: input.line ?? null,
      contextHash: input.contextHash ?? null,
    },
    input.key,
  );
});

export const reply = Effect.fn("hub.PullRequest.reply")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly thread: string;
  readonly body: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    { ...base, type: "comment.replied", thread: input.thread, body: input.body },
    input.key,
  );
});

export const resolve = Effect.fn("hub.PullRequest.resolve")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly thread: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue({ ...base, type: "comment.resolved", thread: input.thread }, input.key);
});

export const reopenThread = Effect.fn("hub.PullRequest.reopenThread")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly thread: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue({ ...base, type: "comment.reopened", thread: input.thread }, input.key);
});

export const checkStarted = Effect.fn("hub.PullRequest.checkStarted")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly head: Oid;
  readonly name: string;
  readonly provider: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    {
      ...base,
      type: "check.started",
      head: Event.qualify(input.head),
      name: input.name,
      provider: input.provider,
    },
    input.key,
  );
});

export const checkCompleted = Effect.fn("hub.PullRequest.checkCompleted")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  readonly head: Oid;
  readonly name: string;
  readonly provider: string;
  readonly status: "success" | "failure" | "neutral";
  readonly url?: string;
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    {
      ...base,
      type: "check.completed",
      head: Event.qualify(input.head),
      name: input.name,
      provider: input.provider,
      status: input.status,
      url: input.url ?? null,
    },
    input.key,
  );
});

/**
 * Redact an event's content.
 *
 * What happens here is that a signed tombstone is appended, because that is
 * what replicates — a deletion that is only a deletion comes back from the
 * first replica that still has the object. The commit, the tree and the
 * event's place in the chain all stay. Content goes; structure does not,
 * because every later event's hash depends on it.
 *
 * The bytes go in `gc`, which takes `Redaction.excluded()` and stops
 * protecting what a tombstone covers. Not here, and deliberately: a packed
 * object cannot be dropped without rewriting its pack, and the loose copy
 * needs a question answered that only a reachability walk can answer — git
 * dedupes by content, so a redacted payload can be the very object a branch
 * names, or one a reflog still leads back to. Deleting it here left the source
 * history dangling; answering it here would mean reproducing the walk `gc`
 * *is*. So an operator's removal is complete at the next collection, and this
 * says so rather than reporting bytes gone that are still on disk.
 */
export const redact = Effect.fn("hub.PullRequest.redact")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  /** The event id to remove — what the tombstone names. */
  readonly target: string;
  readonly reason: string;
  readonly key: PrivateKey;
}) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({ field: "repo", reason: "this repository has no genesis" });
  }
  const trust = yield* projectTrust(stored.genesis);

  // The target is resolved through the projection, not over the raw walk.
  // Those two disagree: the walk includes events the fold rejected, so a
  // pre-planted unsigned event re-using an id could be picked here while the
  // fold redacted a different one — leaving `redact` reporting failure forever
  // over a payload `gc` had already been told to prune.
  const before = yield* project(stored.genesis, trust, input.pr);
  const claimants = before.claims.get(input.target) ?? [];
  if (claimants.length === 0) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.pr} has no event ${input.target}`,
    });
  }
  if (claimants.length > 1) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.pr} has ${claimants.length} events claiming ${input.target}`,
    });
  }
  const targetCommit = claimants[0];
  if (targetCommit === undefined) {
    return yield* new Invalid({ field: "target", reason: `${input.target} has no event commit` });
  }

  const { events } = yield* Event.entries(input.pr);
  const target = events.find((entry) => entry.commit === targetCommit);
  if (target?.payload?.type === "event.redacted") {
    return yield* new Invalid({
      field: "target",
      reason: "a tombstone is the record of a removal and is not itself removable",
    });
  }

  // Asked *before* the tombstone is written, not only afterwards. The check
  // below rebuilds the projection and refuses a tombstone that did not count —
  // but by then the event is on an append-only ref forever, and `Redaction`
  // has to fold this pull request on every `gc` and every retried fetch the
  // moment any `event.redacted` payload is present. So a signer holding
  // nothing could make every future collection pay for a tombstone that was
  // never going to work. What they can still do is push the event directly;
  // what they cannot do is have this command do it for them.
  // Expiry among them, and that is not belt-and-braces: the fold judges a
  // tombstone `permanent`, which deliberately does not consult the clock, so
  // an expired holder's tombstone counts for ever once it exists. The boundary
  // refuses that very event over the wire — "a redaction needs an unexpired
  // hub.redact" — so without this the local command was the one door that let
  // an expired member drive an irreversible deletion.
  const signer = yield* fingerprint(input.key.publicKey);
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
      reason: `${signer} may not redact events here; that needs hub.redact`,
    });
  }

  const base = yield* context(input.repo, input.pr);
  const commit = yield* Event.issue(
    {
      ...base,
      type: "event.redacted",
      target: input.target,
      targetCommit: Event.qualify(targetCommit),
      reason: input.reason,
    },
    input.key,
  );

  // Nothing is deleted until the tombstone is known to count.
  //
  // Writing the event and deleting the blob are two different authorities, and
  // treating the first as implying the second meant a signer holding only
  // `hub.comment` could blank another member's approval: the projection
  // refused their tombstone, so `redacted` stayed empty and `Redaction.blobs`
  // never listed it — but the payload was already gone, and the approval had
  // become an unreadable event that stopped counting toward a merge. So the
  // projection is rebuilt and asked, and a refused tombstone is reported as
  // the failure it is rather than performed anyway.
  const state = yield* project(stored.genesis, trust, input.pr);
  if (!state.redacted.has(targetCommit)) {
    const refused = state.rejected.find((entry) => entry.commit === commit);
    return yield* new Invalid({
      field: "target",
      reason: refused?.reason ?? `the tombstone over ${input.target} did not take effect`,
    });
  }

  // The tombstone is written; the bytes go where every other removal happens.
  //
  // Not here, and that is the whole point of `gc` taking `Redaction.excluded()`.
  // A packed object needed `gc` anyway — a pack cannot give up one object
  // without being rewritten — and the loose copy needs the same question
  // answered before it goes: git dedupes by content, so a redacted payload can
  // be the very object a branch names, or one a reflog still leads back to.
  // Deleting it here left the source history dangling, and answering it here
  // would mean reproducing the reachability walk `gc` *is*. One place decides,
  // and it is the one that can see the whole repository.
  return commit;
});

export type PullRequestError = Invalid | ObjectNotFound | StorageFailure;
