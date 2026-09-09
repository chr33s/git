/**
 * The Change Request workflows: the diff, the discussion, the review, the merge.
 *
 * Each one is split on provenance, and the split is the point. A *hub* Change
 * Request settles through the hub's own endpoints: signed events, read back
 * from the projection, so what the screen shows is the repository's answer
 * rather than an optimistic guess. A *fixture* Change Request keeps the
 * design's tab-local story, which is clearly sample behaviour and can never be
 * reached by a hub entity.
 *
 * The consequence worth stating: there is no tab-local "merged" for something
 * the repository still holds open. A refused merge is a `Failed…` Message
 * carrying the reason, and the Change Request stays open beneath it.
 */
import { Command } from "foldkit";
import { Effect, Schema } from "effect";

import { LoadedDiff } from "./app.model.ts";
import { AppMessage } from "./app.message.ts";
import { absent, reasonOf } from "./thrown.ts";

/**
 * Ask the server what changed, then read both sides of each file.
 *
 * Interruptible and keyed by the task: a reader clicking through Change
 * Requests supersedes the previous read rather than racing it, which is what
 * the old generation counter was doing by hand and paying for twice over.
 */
export const LoadDiff = Command.define("LoadDiff", {
  args: { id: Schema.String, sourceRef: Schema.String, targetRef: Schema.String },
  messages: [AppMessage.SucceededLoadDiff, AppMessage.FellBackLoadDiff],
  interrupt: { keyFields: ["id"], toKey: ({ id }) => id },
  execute: ({ id, sourceRef, targetRef }) =>
    Effect.gen(function* () {
      const api = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = api.clientFromDocument();
      const files = yield* Effect.tryPromise(async () => await client.diff(targetRef, sourceRef));
      const loaded = yield* Effect.tryPromise(
        async () =>
          await Promise.all(
            files
              .filter((file) => !file.binary)
              .map(async (file): Promise<LoadedDiff> => {
                // `added` has no old side and `removed` has no new one; asking
                // for the missing side would be a guaranteed 404. Existing
                // sides and separate files are independent, so all in parallel.
                const [oldContents, newContents] = await Promise.all([
                  file.status === "added" ? null : client.file(targetRef, file.path),
                  file.status === "removed" ? null : client.file(sourceRef, file.path),
                ]);
                return { path: file.path, status: file.status, oldContents, newContents };
              }),
          ),
      );
      return AppMessage.SucceededLoadDiff({ id, files: loaded });
    }).pipe(
      // Refs the repository does not have — the fixture case — leave the
      // design's own diff showing, labelled as the sample it is.
      Effect.orElseSucceed(() =>
        AppMessage.FellBackLoadDiff({
          id,
          reason: `${id} names refs that are not in this repository`,
        }),
      ),
    ),
});

/**
 * Comment on a Change Request.
 *
 * A hub comment is a signed event, read back from the projection — a refusal
 * shows *as* a refusal, never as a tab-local comment pretending to be
 * repository state.
 */
export const CommentRemote = Command.define("CommentRemote", {
  args: { id: Schema.String, body: Schema.String },
  messages: [AppMessage.SucceededComment, AppMessage.FailedComment],
  execute: ({ id, body }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const sent = yield* Effect.tryPromise(async () => await hub.commentOn(id, body));
      return sent
        ? AppMessage.SucceededComment()
        : AppMessage.FailedComment({
            reason: "the hub refused the comment — is this key a member?",
          });
    }).pipe(
      Effect.orElseSucceed(() =>
        AppMessage.FailedComment({ reason: "the hub could not be reached" }),
      ),
    ),
});

/**
 * Settle a hub Change Request through the hub's own merge endpoint.
 *
 * One server-side transition that advances the base to the approved head and
 * appends the signed `pr.merged` beside it, judged together — and what shows
 * next is the projection, re-read.
 */
export const MergeRemote = Command.define("MergeRemote", {
  args: { id: Schema.String, head: Schema.String, base: Schema.String },
  messages: [AppMessage.SucceededMerge, AppMessage.FailedMerge],
  execute: ({ id, head, base }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const refused = yield* Effect.tryPromise(async () => await hub.merge(id, head, base));
      return refused === null
        ? AppMessage.SucceededMerge()
        : AppMessage.FailedMerge({ reason: refused });
    }).pipe(
      Effect.orElseSucceed(() =>
        AppMessage.FailedMerge({
          reason: "the hub could not be reached — the Change Request stays open",
        }),
      ),
    ),
});

/**
 * Merge a fixture Change Request.
 *
 * The server's generic endpoint is offered the refs — which usually do not
 * exist outside the design — and a refusal it *could* have honoured (a policy
 * `Invalid`, a real conflict) is worth showing and blocks the sample
 * projection. Refs it simply does not have fall through to that projection.
 */
export const MergeFixture = Command.define("MergeFixture", {
  args: {
    id: Schema.String,
    title: Schema.String,
    sourceRef: Schema.String,
    targetRef: Schema.String,
  },
  messages: [AppMessage.SucceededMerge, AppMessage.FailedMerge],
  execute: ({ id, title, sourceRef, targetRef }) =>
    Effect.gen(function* () {
      const outcome = yield* Effect.tryPromise(
        async () => await mergeFixture({ id, title, sourceRef, targetRef }),
      );
      return outcome === null
        ? AppMessage.SucceededMerge()
        : AppMessage.FailedMerge({ reason: outcome });
    }).pipe(
      // `mergeFixture` rethrows what it does not recognise. Recovered rather
      // than left as a defect: Foldkit's runner treats a defect as terminal,
      // and a Change Request that could not be merged is not a reason to end
      // the session.
      Effect.catch((cause) => Effect.succeed(AppMessage.FailedMerge({ reason: reasonOf(cause) }))),
    ),
});

/**
 * Offer the refs to the server's generic merge, and read the answer.
 *
 * Classified here rather than in the Effect: `tryPromise` widens the thrown
 * value to an unknown cause, and which class it came from is exactly what
 * separates "this repository never had these refs" — the fixture case, where
 * the sample projection records the merge — from a refusal it could have
 * honoured, which is worth showing and blocks that projection.
 */
const mergeFixture = async (input: {
  readonly id: string;
  readonly title: string;
  readonly sourceRef: string;
  readonly targetRef: string;
}): Promise<string | null> => {
  const api = await import("./api.ts");
  try {
    const result = await api.clientFromDocument().merge({
      ours: input.targetRef,
      theirs: input.sourceRef,
      into: input.targetRef,
      message: `merge ${input.id}: ${input.title}`,
    });
    if (result.kind !== "conflicted") return null;
    const paths = result.conflicts.map((conflict) => conflict.path).join(", ");
    return `merge conflicted on ${paths} — resolve on the branch first`;
  } catch (error) {
    if (!(error instanceof api.ApiError) && !(error instanceof TypeError)) throw error;
    // Refs this repository never had are the fixture case, where the sample
    // projection records the merge; anything else is an answer worth showing.
    return absent(error) ? null : api.describe(error);
  }
};

export const ReviewRemote = Command.define("ReviewRemote", {
  args: {
    id: Schema.String,
    decision: Schema.Literals(["approve", "reject"]),
    /** The revision this review judges — the one that was on screen. */
    head: Schema.String,
  },
  messages: [AppMessage.SucceededMerge, AppMessage.FailedMerge],
  execute: ({ id, decision, head }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const sent = yield* Effect.tryPromise(async () => await hub.review(id, decision, head));
      return sent
        ? AppMessage.SucceededMerge()
        : AppMessage.FailedMerge({ reason: "the hub refused the review — is this key a member?" });
    }).pipe(
      Effect.orElseSucceed(() =>
        AppMessage.FailedMerge({ reason: "the hub could not be reached" }),
      ),
    ),
});

/** Answer a review thread, or settle it. */
export const ThreadAction = Command.define("ThreadAction", {
  args: {
    id: Schema.String,
    thread: Schema.String,
    action: Schema.Literals(["resolve", "reopen", "reply"]),
    body: Schema.String,
  },
  messages: [AppMessage.SucceededThread, AppMessage.FailedThread],
  execute: ({ id, thread, action, body }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const sent = yield* Effect.tryPromise(async () =>
        action === "reply"
          ? await hub.reply(id, thread, body)
          : await hub.resolveThread(id, thread, action === "resolve"),
      );
      return sent
        ? AppMessage.SucceededThread()
        : AppMessage.FailedThread({ reason: "the hub refused the thread update" });
    }).pipe(
      Effect.orElseSucceed(() =>
        AppMessage.FailedThread({ reason: "the hub could not be reached" }),
      ),
    ),
});

/** A hub task's lease, and its end: claim it, let it go, close it. */
export const TaskAction = Command.define("TaskAction", {
  args: {
    id: Schema.String,
    action: Schema.Literals(["claim", "release", "complete", "abandon"]),
  },
  messages: [AppMessage.SucceededTaskAction, AppMessage.FailedTaskAction],
  execute: ({ id, action }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const sent = yield* Effect.tryPromise(async () => await hub.taskAction(id, action));
      return sent
        ? AppMessage.SucceededTaskAction()
        : AppMessage.FailedTaskAction({ reason: "the hub refused the task update" });
    }).pipe(
      Effect.orElseSucceed(() =>
        AppMessage.FailedTaskAction({ reason: "the hub could not be reached" }),
      ),
    ),
});

/**
 * Fill a hub Change Request's discussion, checks and review.
 *
 * A hub-sourced task carries only its listing row until someone looks at it.
 * A no-op for fixture ids: the design's data is complete.
 */
export const HydrateDetail = Command.define("HydrateDetail", {
  args: { id: Schema.String, head: Schema.NullOr(Schema.String) },
  messages: [AppMessage.SucceededHydrate, AppMessage.CompletedNavigate],
  execute: ({ id, head }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const task = yield* Effect.tryPromise(async () => await hub.hydrated(id, head));
      // `null` is a fixture id, or a detail the listing has already moved
      // past. Neither is a failure, and neither has anything to fold in.
      return task === null ? AppMessage.CompletedNavigate() : AppMessage.SucceededHydrate({ task });
    }).pipe(Effect.orElseSucceed(() => AppMessage.CompletedNavigate())),
});
