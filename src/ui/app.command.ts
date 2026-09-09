/**
 * Everything the application does to the world outside its Model.
 *
 * Each Command runs an Effect and answers with a Message, so a reader can
 * follow any effect from the click that started it to the state it produced
 * without leaving the Message union. The rule that makes that hold: no
 * Command's error channel escapes — every one of them turns a failure into a
 * `Failed…` Message, because a failure the Model cannot hold is a failure the
 * reader never sees.
 *
 * The API clients and the hub module are reached through dynamic `import()`
 * rather than held anywhere. They carry the `HttpApi` declaration and the
 * Effect runtime with them, and first paint should not wait on either — the
 * same argument `highlight.ts` makes for Shiki. That is also why no client
 * instance appears in the Model: it is a handle, and the Model holds facts.
 */
import { Command } from "foldkit";
import { Effect, Schema } from "effect";

import { SessionRow, Task } from "./model.ts";
import { AppMessage } from "./app.message.ts";
import { tasks as fixtures } from "./fixtures.ts";
import { reasonOf } from "./thrown.ts";

/**
 * Ask the server who is asking.
 *
 * A refusal is not an error here: an anonymous reader is a reader, and the
 * screens show a repository they cannot write to rather than nothing at all.
 */
export const FetchIdentity = Command.define("FetchIdentity", {
  messages: [AppMessage.SucceededFetchIdentity, AppMessage.FailedFetchIdentity],
  execute: Effect.gen(function* () {
    const api = yield* Effect.tryPromise(async () => await import("./api.ts"));
    const answer = yield* Effect.tryPromise(async () => await api.clientFromDocument().whoami());
    return AppMessage.SucceededFetchIdentity({ viewer: answer });
  }).pipe(
    // `orElseSucceed` cannot see the cause, and the cause is the message: a
    // reader told only that identity failed learns nothing they can act on.
    Effect.catch((cause) =>
      Effect.succeed(AppMessage.FailedFetchIdentity({ reason: reasonOf(cause) })),
    ),
  ),
});

/**
 * Ask the hub what this repository holds.
 *
 * Three answers, and they are not interchangeable — see `hub.listings`. A
 * refusal empties the list and says so; unreachable or empty keeps the
 * fixtures, which are the UI's documented offline state.
 */
export const LoadTasks = Command.define("LoadTasks", {
  messages: [AppMessage.SucceededLoadTasks, AppMessage.DeniedLoadTasks, AppMessage.FailedLoadTasks],
  execute: Effect.gen(function* () {
    const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
    const answer = yield* Effect.tryPromise(async () => await hub.listings());
    switch (answer._tag) {
      case "Loaded":
        return AppMessage.SucceededLoadTasks({ tasks: answer.tasks, sessions: answer.sessions });
      case "Denied":
        return AppMessage.DeniedLoadTasks({ reason: answer.reason });
      case "Unreachable":
        return AppMessage.FailedLoadTasks({ reason: answer.reason });
    }
  }).pipe(
    // As above: the reason is what the notice shows, so the cause is carried.
    Effect.catch((cause) =>
      Effect.succeed(AppMessage.FailedLoadTasks({ reason: reasonOf(cause) })),
    ),
  ),
});

/**
 * Open a Task.
 *
 * The hub first: signed with this browser's key and appended for real. Only
 * when the event cannot land — offline, or a key the repository does not yet
 * trust — does the task stay in this tab, which is what the dialog warns. The
 * two outcomes are different Messages because they are different facts, and
 * the reader is told which one happened.
 */
export const CreateTask = Command.define("CreateTask", {
  args: {
    title: Schema.String,
    desc: Schema.String,
    parent: Schema.String,
    fallbackId: Schema.String,
  },
  messages: [AppMessage.SucceededCreateTask, AppMessage.FellBackCreateTask],
  execute: ({ title, desc, parent, fallbackId }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const id = yield* Effect.tryPromise(
        async () => await hub.createTask({ title, description: desc, parent }),
      );
      return id === null
        ? AppMessage.FellBackCreateTask({ id: fallbackId })
        : AppMessage.SucceededCreateTask({ id });
    }).pipe(Effect.orElseSucceed(() => AppMessage.FellBackCreateTask({ id: fallbackId }))),
});

/**
 * File a task under another, or out from under one.
 *
 * A fixture task has no ref to append to, so it moves in this tab only; a live
 * task the hub refused stays where it was, and the reason says so rather than
 * the list quietly disagreeing with the repository.
 */
export const MoveTask = Command.define("MoveTask", {
  args: { id: Schema.String, parent: Schema.String, live: Schema.Boolean },
  messages: [AppMessage.SucceededMoveTask, AppMessage.FailedMoveTask],
  execute: ({ id, parent, live }) =>
    Effect.gen(function* () {
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const landed = yield* Effect.tryPromise(async () => await hub.moveTask(id, parent));
      if (landed) return AppMessage.SucceededMoveTask({ id, parent });
      return live
        ? AppMessage.FailedMoveTask({ id, reason: "the hub did not accept the move" })
        : AppMessage.SucceededMoveTask({ id, parent });
    }).pipe(
      Effect.orElseSucceed(() =>
        live
          ? AppMessage.FailedMoveTask({ id, reason: "the hub could not be reached" })
          : AppMessage.SucceededMoveTask({ id, parent }),
      ),
    ),
});

/** The design's sample data, which is what an offline page shows. */
export const seedTasks: readonly Task[] = fixtures;

/** No sessions until the hub answers; the Activity screen says so itself. */
export const seedSessions: readonly SessionRow[] = [];

/**
 * Search file contents.
 *
 * Interruptible, keyed by the Command's own name: a reader typing turns one
 * query into several, and only the last one's answer is wanted. The old
 * implementation counted generations by hand and discarded late replies; this
 * interrupts the fiber instead, so a superseded search stops before it can
 * dispatch. The request already in flight still finishes at the server — the
 * client takes no `AbortSignal` — but nothing downstream waits on it.
 *
 * Literal and case-insensitive, because a reader types text rather than a
 * regular expression — the same contract `POST /grep` has always had.
 */
export const Grep = Command.define("Grep", {
  args: { pattern: Schema.String },
  messages: [AppMessage.SucceededGrep, AppMessage.FailedGrep],
  interrupt: true,
  execute: ({ pattern }) =>
    Effect.gen(function* () {
      const api = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = api.clientFromDocument();
      const state = yield* Effect.tryPromise(async () => await client.refState());
      const oid = yield* Effect.tryPromise(async () => await import("../git/Oid.ts"));
      const tip = oid.isOid(state.head)
        ? state.head
        : state.refs.find((ref) => ref.name === state.head)?.oid;
      // A repository with no commits has nothing to search, which is an empty
      // answer rather than a failure — the screen says "no matches", not
      // "unavailable", because the server answered perfectly well.
      if (tip === undefined) return AppMessage.SucceededGrep({ matches: [], truncated: false });
      const found = yield* Effect.tryPromise(
        async () => await client.grep(pattern, tip, undefined),
      );
      return AppMessage.SucceededGrep({
        matches: found.matches.map((match) => ({
          path: match.path,
          line: match.line,
          text: match.text,
        })),
        truncated: found.truncated,
      });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(
          AppMessage.FailedGrep({
            reason:
              cause instanceof Error && cause.message !== ""
                ? cause.message
                : "it is not reachable",
          }),
        ),
      ),
    ),
});

/**
 * Read recent history for the Activity timeline.
 *
 * One fetch covers paging too: ‹ walks back through what is already loaded
 * rather than repeating the per-commit header reads for each window. A hundred
 * commits is the bound, and a window past it reads as empty.
 */
export const LoadCommits = Command.define("LoadCommits", {
  messages: [AppMessage.SucceededLoadCommits, AppMessage.FailedLoadCommits],
  interrupt: true,
  execute: Effect.gen(function* () {
    const api = yield* Effect.tryPromise(async () => await import("./api.ts"));
    const client = api.clientFromDocument();
    const state = yield* Effect.tryPromise(async () => await client.refState());
    const oid = yield* Effect.tryPromise(async () => await import("../git/Oid.ts"));
    const tip = oid.isOid(state.head)
      ? state.head
      : state.refs.find((ref) => ref.name === state.head)?.oid;
    // A repository with no commits has history the server can describe: it is
    // empty. That is a success, and the grid says "no commits in this window"
    // rather than falling back to the design's sample.
    if (tip === undefined) return AppMessage.SucceededLoadCommits({ commits: [] });
    const commits = yield* Effect.tryPromise(async () => await client.recentCommits(tip, 100));
    return AppMessage.SucceededLoadCommits({
      commits: commits.map((commit) => ({
        oid: commit.oid,
        subject: commit.subject,
        author: commit.author,
        at: commit.at,
      })),
    });
  }).pipe(
    Effect.catch((cause) =>
      Effect.succeed(
        AppMessage.FailedLoadCommits({ reason: reasonOf(cause, "it is not reachable") }),
      ),
    ),
  ),
});
