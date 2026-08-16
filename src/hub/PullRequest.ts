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
import { Effect } from "effect";

import type { PrivateKey } from "../crypto/SshSignature.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import * as Record from "../trust/Record.ts";
import * as Event from "./Event.ts";

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
    issuedAt: new Date().toISOString(),
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
  readonly key: PrivateKey;
}) {
  const base = yield* context(input.repo, input.pr);
  return yield* Event.issue(
    {
      ...base,
      type: "review.submitted",
      head: Event.qualify(input.head),
      decision: input.decision,
      body: input.body ?? "",
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
 * Two things happen, and both are necessary. A signed tombstone is appended,
 * because that is what replicates — a deletion that is only a deletion comes
 * back from the first replica that still has the object. And the payload blob
 * is dropped from this repository, because a tombstone alone removes nothing.
 *
 * The commit, the tree and the event's place in the chain all stay. Content
 * goes; structure does not, because every later event's hash depends on it.
 */
export const redact = Effect.fn("hub.PullRequest.redact")(function* (input: {
  readonly repo: RepoId;
  readonly pr: string;
  /** The event id to remove — what the tombstone names. */
  readonly target: string;
  readonly reason: string;
  readonly key: PrivateKey;
}) {
  const repository = yield* Repository;

  const events = yield* Event.entries(input.pr);
  const target = events.find((entry) => entry.payload?.id === input.target);
  if (target === undefined) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.pr} has no event ${input.target}`,
    });
  }
  if (target.payload?.type === "event.redacted") {
    return yield* new Invalid({
      field: "target",
      reason: "a tombstone is the record of a removal and is not itself removable",
    });
  }

  const base = yield* context(input.repo, input.pr);
  const commit = yield* Event.issue(
    { ...base, type: "event.redacted", target: input.target, reason: input.reason },
    input.key,
  );

  // The blob, by the name its own tree entry gives it.
  const info = yield* repository.readCommit(target.commit);
  const entry = yield* repository.findPath(info.tree, `${Event.RECORD}.json`);
  if (entry !== null) yield* repository.deleteObject(entry.oid);

  return commit;
});

export type PullRequestError = Invalid | ObjectNotFound | StorageFailure;

export { Record };
