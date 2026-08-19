/**
 * `chr33s-git task …` — what needs doing, and who is on it.
 *
 * The fleet's working rhythm, in four verbs: a task is opened, hooks wake
 * whoever watches for one, agents race to claim it, and the lease one of them
 * takes is what tells the rest to look elsewhere. `list` is what an agent woken
 * by a task ref actually reads — the tasks nobody currently holds.
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import * as Task from "../hub/Task.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { readPrivateKey, repoArgument, rootFlag, withRepo } from "./shared.ts";

const listOf = (value: string): ReadonlyArray<string> =>
  value === ""
    ? []
    : value
        .split(",")
        .map((entry) => entry.trim())
        .filter((entry) => entry !== "");

const identityOf = Effect.fn("task.identityOf")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis; run \`chr33s-git hub init ${repo} --key <key>\` first`,
    });
  }
  return stored.genesis.repoId;
});

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);

const taskArgument = Argument.string("task");

const open = Command.make(
  "open",
  {
    root: rootFlag,
    key: keyFlag,
    title: Flag.string("title").pipe(Flag.withDescription("What needs doing, in one line")),
    description: Flag.string("description").pipe(Flag.withDefault("")),
    ref: Flag.string("ref").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Refs this task concerns, comma-separated"),
    ),
    pull: Flag.string("pull").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Pull requests it concerns, comma-separated"),
    ),
    parent: Flag.string("parent").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The task this one belongs to — a release, an epic, a parent story"),
    ),
    repo: repoArgument,
  },
  ({ description, key, parent, pull, ref, repo, root, title }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const task = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const opened = yield* Task.open({
            repo: yield* identityOf(repo),
            title,
            description,
            refs: listOf(ref),
            pulls: listOf(pull),
            parent,
            key: signer,
          });
          return opened.task;
        }),
      );
      // The id alone, so a hook can hand it to whatever it starts.
      yield* Console.log(task);
    }),
);

const claim = Command.make(
  "claim",
  {
    root: rootFlag,
    key: keyFlag,
    ttl: Flag.integer("ttl").pipe(
      Flag.withDefault(3600),
      Flag.withDescription("Seconds this lease lasts before it frees itself"),
    ),
    repo: repoArgument,
    task: taskArgument,
  },
  ({ key, repo, root, task, ttl }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const outcome = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          // Read before writing, so a claimant that lost the race says so
          // rather than appending a second claim nobody honours. Advisory, and
          // deliberately: two agents reading at once can both pass this, and
          // the projection still names one holder.
          const state = yield* Task.project(task);
          if (state.claim !== null) return { taken: true, until: state.claim.expiresAt };
          yield* Task.claim({
            repo: yield* identityOf(repo),
            task,
            key: signer,
            ttlSeconds: ttl,
          });
          return { taken: false, until: "" };
        }),
      );

      if (outcome.taken) {
        return yield* new Invalid({
          field: "task",
          reason: `${task} is already claimed until ${outcome.until}`,
        });
      }
      yield* Console.log(`Claimed ${task} for ${ttl}s`);
    }),
);

const release = Command.make(
  "release",
  { root: rootFlag, key: keyFlag, repo: repoArgument, task: taskArgument },
  ({ key, repo, root, task }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* Task.release({ repo: yield* identityOf(repo), task, key: signer });
        }),
      );
      yield* Console.log(`Released ${task}`);
    }),
);

const close = Command.make(
  "close",
  {
    root: rootFlag,
    key: keyFlag,
    outcome: Flag.choice("outcome", ["completed", "abandoned", "superseded"]).pipe(
      Flag.withDefault("completed" as const),
    ),
    pull: Flag.string("pull").pipe(Flag.withDefault("")),
    session: Flag.string("session").pipe(Flag.withDefault("")),
    repo: repoArgument,
    task: taskArgument,
  },
  ({ key, outcome, pull, repo, root, session, task }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* Task.close({
            repo: yield* identityOf(repo),
            task,
            key: signer,
            outcome,
            pulls: listOf(pull),
            sessions: listOf(session),
          });
        }),
      );
      yield* Console.log(`Closed ${task} (${outcome})`);
    }),
);

const reopen = Command.make(
  "reopen",
  { root: rootFlag, key: keyFlag, repo: repoArgument, task: taskArgument },
  ({ key, repo, root, task }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* Task.reopen({ repo: yield* identityOf(repo), task, key: signer });
        }),
      );
      yield* Console.log(`Reopened ${task}`);
    }),
);

/**
 * Move a task under another, or out from under one.
 *
 * Its own subcommand rather than a flag on `open`, because where work sits is
 * the thing that changes: this is what a slipped release or a split epic
 * looks like on an append-only ref.
 */
const reparent = Command.make(
  "reparent",
  {
    root: rootFlag,
    key: keyFlag,
    parent: Flag.string("parent").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The task it now belongs to; omit to detach it"),
    ),
    repo: repoArgument,
    task: taskArgument,
  },
  ({ key, parent, repo, root, task }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* Task.reparent({ repo: yield* identityOf(repo), task, parent, key: signer });
        }),
      );
      yield* Console.log(parent === "" ? `Detached ${task}` : `Filed ${task} under ${parent}`);
    }),
);

/** As `session redact`, on the namespace that shares its one way back. */
const redact = Command.make(
  "redact",
  {
    root: rootFlag,
    key: keyFlag,
    target: Flag.string("target").pipe(Flag.withDescription("The record's event id")),
    reason: Flag.string("reason").pipe(Flag.withDescription("Why it is being removed")),
    repo: repoArgument,
    task: taskArgument,
  },
  ({ key, reason, repo, root, target, task }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* Task.redact({
            repo: yield* identityOf(repo),
            task,
            target,
            reason,
            key: signer,
          });
        }),
      );
      yield* Console.log(`Redacted ${target}; the payload goes at the next gc`);
    }),
);

const list = Command.make(
  "list",
  {
    root: rootFlag,
    all: Flag.boolean("all").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Include claimed and closed tasks"),
    ),
    repo: repoArgument,
  },
  ({ all, repo, root }) =>
    Effect.gen(function* () {
      const found = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const projections = yield* Effect.forEach(yield* Task.tasks(), (task) =>
            Task.project(task),
          );
          return projections.filter((state) => all || state.available);
        }),
      );
      yield* Console.log(JSON.stringify(found, null, 2));
    }),
);

const show = Command.make(
  "show",
  { root: rootFlag, repo: repoArgument, task: taskArgument },
  ({ repo, root, task }) =>
    Effect.gen(function* () {
      const state = yield* withRepo(root, repo, Task.project(task));
      yield* Console.log(JSON.stringify(state, null, 2));
    }),
);

export const taskCommand = Command.make("task", {}, () =>
  Console.log(
    "chr33s-git task <open|claim|release|close|reopen|reparent|redact|list|show> — see --help",
  ),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Record what needs doing")),
    claim.pipe(Command.withDescription("Take a task, on a lease that frees itself")),
    release.pipe(Command.withDescription("Let go of a task before its lease ends")),
    close.pipe(Command.withDescription("Record how a task was resolved")),
    reopen.pipe(Command.withDescription("Undo a close, which a ref cannot be rewound to do")),
    reparent.pipe(Command.withDescription("File a task under another, or detach it")),
    redact.pipe(Command.withDescription("Remove one record's content, needing hub.redact")),
    list.pipe(Command.withDescription("Tasks nobody currently holds")),
    show.pipe(Command.withDescription("What one task amounts to now")),
  ]),
);
