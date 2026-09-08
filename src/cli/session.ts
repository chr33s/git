/**
 * `git+ session …` — recording what an agent was told, and what came
 * of it.
 *
 * Plumbing, by design: the expected caller is a harness hook — a Claude Code
 * `SessionStart` writing the opening, a `Stop` writing what was produced — not
 * a person typing ceremonies. `open` prints the session id alone, so a hook
 * can capture it and put it in a commit trailer without parsing prose.
 */
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { isSea } from "node:sea";

import { Config, Console, Effect, Schema } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { readGenesis } from "../trust/Genesis.ts";
import * as Memory from "../hub/Memory.ts";
import { Repository } from "../git/Repository.ts";
import * as Session from "../hub/Session.ts";
import { readPrivateKey, repoArgument, rootFlag, withRepo } from "./shared.ts";

/** Comma-separated list flags, which is how a hook passes several of a thing. */
const listOf = (value: string): ReadonlyArray<string> =>
  value === ""
    ? []
    : value
        .split(",")
        .map((entry) => entry.trim())
        .filter((entry) => entry !== "");

/**
 * The repository's own identity, which every record is bound to.
 *
 * Refused rather than defaulted: a session record names the repository inside
 * its signed bytes so it cannot be replayed into another one, and a repository
 * with no genesis has no identity to name.
 */
const identityOf = Effect.fn("session.identityOf")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis; run \`git+ hub init ${repo} --key <key>\` first`,
    });
  }
  return stored.genesis.repoId;
});

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);

/** A typo must not create an append-only ref for a session nobody opened. */
const existingSession = Effect.fn("session.existingSession")(function* (session: string) {
  const repository = yield* Repository;
  if ((yield* repository.resolve(Session.refOf(session))) === null) {
    return yield* new Invalid({
      field: "session",
      reason: `this repository has no session '${session}'`,
    });
  }
});

const open = Command.make(
  "open",
  {
    root: rootFlag,
    key: keyFlag,
    agent: Flag.string("agent").pipe(
      Flag.withDefault("unknown"),
      Flag.withDescription("What kind of agent this is, e.g. claude-code"),
    ),
    model: Flag.string("model").pipe(Flag.withDefault(""), Flag.withDescription("Model name")),
    harness: Flag.string("harness").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Harness and version"),
    ),
    prompt: Flag.string("prompt").pipe(Flag.withDescription("The instruction, as given")),
    role: Flag.choice("role", ["user", "system"]).pipe(Flag.withDefault("user" as const)),
    instructions: Flag.string("instructions").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Object id of the standing instructions in force"),
    ),
    repo: repoArgument,
  },
  ({ agent, harness, instructions, key, model, prompt, repo, role, root }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const session = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const identity = yield* identityOf(repo);
          const opened = yield* Session.open({
            repo: identity,
            agent: { kind: agent, model, harness },
            prompt,
            role,
            key: signer,
            instructions: instructions === "" ? null : instructions,
          });
          return opened.session;
        }),
      );
      // The id alone: a hook captures this and writes `Session: <id>` into the
      // commit it is about to make.
      yield* Console.log(session);
    }),
);

const produce = Command.make(
  "produce",
  {
    root: rootFlag,
    key: keyFlag,
    session: Flag.string("session").pipe(Flag.withDescription("The session this reports on")),
    commit: Flag.string("commit").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Commits this session produced, comma-separated"),
    ),
    ref: Flag.string("ref").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Refs it wrote, comma-separated"),
    ),
    pull: Flag.string("pull").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Pull requests it opened, comma-separated"),
    ),
    note: Flag.string("note").pipe(
      Flag.withDefault(""),
      Flag.withDescription("What was decided or learned, distilled"),
    ),
    inputTokens: Flag.integer("input-tokens").pipe(Flag.withDefault(0)),
    outputTokens: Flag.integer("output-tokens").pipe(Flag.withDefault(0)),
    repo: repoArgument,
  },
  ({ commit, inputTokens, key, note, outputTokens, pull, ref, repo, root, session }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const written = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const identity = yield* identityOf(repo);
          yield* existingSession(session);
          return yield* Session.produced({
            repo: identity,
            session,
            key: signer,
            commits: listOf(commit),
            refs: listOf(ref),
            pulls: listOf(pull),
            note: note === "" ? null : note,
            // Absent rather than zero: a harness that does not report usage
            // and one that used nothing are different facts.
            usage: inputTokens === 0 && outputTokens === 0 ? null : { inputTokens, outputTokens },
          });
        }),
      );
      yield* Console.log(written);
    }),
);

const show = Command.make(
  "show",
  {
    root: rootFlag,
    branch: Flag.string("branch").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Show the session that last produced this ref instead of an id"),
    ),
    repo: repoArgument,
    session: Argument.string("session").pipe(Argument.optional),
  },
  ({ branch, repo, root, session }) =>
    Effect.gen(function* () {
      const projection = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          // A branch, because that is the question an agent has on checkout:
          // "put me back in context for this" rather than "for this id", which
          // it holds only if it opened the session itself.
          const id =
            branch === ""
              ? session._tag === "Some"
                ? session.value
                : null
              : yield* Session.latestFor(branch);
          if (id === null) {
            return yield* new Invalid({
              field: "session",
              reason:
                branch === ""
                  ? "name a session, or pass --branch to look one up"
                  : `no session has produced ${branch}`,
            });
          }
          // A session this repository has never heard of is an error, not an
          // empty document. `Session.project` answers for any id — it walks a
          // ref that need not exist — so a typo, or a name that was never a
          // session, printed a projection with nothing in it and exited zero,
          // which reads as "this session did nothing" rather than "there is no
          // such session".
          yield* existingSession(id);
          return yield* Session.project(id);
        }),
      );
      yield* Console.log(JSON.stringify(projection, null, 2));
    }),
);

const ask = Command.make(
  "ask",
  {
    root: rootFlag,
    key: keyFlag,
    session: Flag.string("session").pipe(Flag.withDescription("The session that is blocked")),
    question: Flag.string("question").pipe(Flag.withDescription("What only a person can answer")),
    option: Flag.string("option").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The answers on offer, comma-separated"),
    ),
    repo: repoArgument,
  },
  ({ key, option, question, repo, root, session }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const decision = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* existingSession(session);
          return yield* Session.ask({
            repo: yield* identityOf(repo),
            session,
            key: signer,
            question,
            options: listOf(option),
          });
        }),
      );
      // The decision id alone, so whatever asked can wait on this one answer.
      yield* Console.log(decision);
    }),
);

const answer = Command.make(
  "answer",
  {
    root: rootFlag,
    key: keyFlag,
    session: Flag.string("session").pipe(Flag.withDescription("The session that asked")),
    decision: Flag.string("decision").pipe(Flag.withDescription("The question being answered")),
    chose: Flag.string("chose").pipe(Flag.withDescription("The answer")),
    note: Flag.string("note").pipe(Flag.withDefault("")),
    repo: repoArgument,
  },
  ({ chose, decision, key, note, repo, root, session }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* existingSession(session);
          const state = yield* Session.project(session);
          if (!state.decisions.some((asked) => asked.id === decision)) {
            return yield* new Invalid({
              field: "decision",
              reason: `${session} has no decision '${decision}'`,
            });
          }
          yield* Session.answer({
            repo: yield* identityOf(repo),
            session,
            key: signer,
            decision,
            chose,
            note: note === "" ? null : note,
          });
        }),
      );
      yield* Console.log(`Answered ${decision}: ${chose}`);
    }),
);

/** The harness sends its event on stdin; installed scripts only select the CLI. */
const hookFile = <A>(operation: () => A) =>
  Effect.try({
    try: operation,
    catch: (cause) => new Invalid({ field: "work", reason: `session hook: ${String(cause)}` }),
  });

const hook = Command.make(
  "hook",
  {
    root: rootFlag,
    key: keyFlag,
    work: Flag.string("work"),
    repo: repoArgument,
    phase: Argument.choice("phase", ["start", "stop"]),
  },
  ({ key, phase, repo, root, work }) =>
    Effect.gen(function* () {
      const input = yield* hookFile(() => fs.readFileSync(0, "utf8"));
      const event = yield* Schema.decodeEffect(
        Schema.fromJsonString(
          Schema.Struct({
            prompt: Schema.optional(Schema.String),
            session_id: Schema.optionalKey(Schema.String),
          }),
        ),
      )(input || "{}").pipe(Effect.orElseSucceed(() => ({ prompt: "", session_id: "" })));
      // Harnesses sharing a checkout must not consume each other's pending
      // report. Keep the legacy filename for callers without a harness id.
      const state = path.resolve(
        work,
        ".chr33s",
        event.session_id
          ? `session.${createHash("sha256").update(event.session_id).digest("hex")}.id`
          : "session.id",
      );
      const exists = yield* hookFile(() => fs.existsSync(state));
      if (phase === "start") {
        if (exists) return;
        const signer = yield* readPrivateKey(key);
        const model = yield* Config.string("CLAUDE_MODEL").pipe(Config.withDefault(""));
        const harness = yield* Config.string("CLAUDE_CODE_VERSION").pipe(Config.withDefault(""));
        const opened = yield* withRepo(
          root,
          repo,
          Effect.gen(function* () {
            return yield* Session.open({
              repo: yield* identityOf(repo),
              agent: { kind: "claude-code", model, harness },
              prompt: event.prompt ?? "",
              role: "user",
              key: signer,
              instructions: null,
            });
          }),
        );
        yield* hookFile(() => fs.writeFileSync(state, opened.session));
      } else if (exists) {
        yield* Effect.gen(function* () {
          const session = yield* hookFile(() => fs.readFileSync(state, "utf8").trim());
          const signer = yield* readPrivateKey(key);
          const branch = yield* Config.string("CHR33S_GIT_BRANCH").pipe(Config.withDefault(""));
          yield* withRepo(
            root,
            repo,
            Effect.gen(function* () {
              yield* existingSession(session);
              yield* Session.produced({
                repo: yield* identityOf(repo),
                session,
                key: signer,
                commits: [],
                refs: listOf(branch),
                pulls: [],
                note: null,
                usage: null,
              });
            }),
          );
        }).pipe(
          // A failed report must not attach the next prompt to this session.
          Effect.ensuring(hookFile(() => fs.rmSync(state, { force: true })).pipe(Effect.orDie)),
        );
      }
    }),
);

/** POSIX shell words, including paths containing quotes or shell metacharacters. */
const shellQuote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";

const hookScript = (input: {
  readonly root: string;
  readonly repo: string;
  readonly key: string;
  readonly work: string;
}) => {
  const command = [
    process.execPath,
    ...(isSea() ? [] : [path.resolve(import.meta.dirname, "bin.ts")]),
    "session",
    "hook",
    "--root",
    input.root,
    "--key",
    input.key,
    "--work",
    input.work,
    input.repo,
  ];
  return `#!/bin/sh
# Written by git+ session enable. Safe to edit; safe to delete.
exec ${command.map(shellQuote).join(" ")} "$1"
`;
};

/** The hook entries this writes, which is also how it recognises its own. */
const entryFor = (script: string, phase: "start" | "stop") => ({
  hooks: [{ type: "command", command: `/bin/sh ${shellQuote(script)} ${phase}` }],
});

/**
 * Remove one record's content, which is the only way back out of a prompt that
 * carried something it should not have.
 *
 * The scanner in front of `session open` is heuristic and says so, so the way
 * back has to exist. What this writes is a signed tombstone; the bytes go at
 * the next `gc`, which is the only pass that can tell whether the blob is
 * reachable from anywhere else — so the message says "at the next collection"
 * rather than reporting a removal that has not happened yet.
 */
const redact = Command.make(
  "redact",
  {
    root: rootFlag,
    key: keyFlag,
    session: Flag.string("session").pipe(Flag.withDescription("The session holding the record")),
    target: Flag.string("target").pipe(Flag.withDescription("The record's event id")),
    reason: Flag.string("reason").pipe(Flag.withDescription("Why it is being removed")),
    repo: repoArgument,
  },
  ({ key, reason, repo, root, session, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* Session.redact({
            repo: yield* identityOf(repo),
            session,
            target,
            reason,
            key: signer,
          });
        }),
      );
      yield* Console.log(`Redacted ${target}; the payload goes at the next gc`);
    }),
);

const enable = Command.make(
  "enable",
  {
    root: rootFlag,
    key: keyFlag,
    work: Flag.string("work").pipe(
      Flag.withDefault("."),
      Flag.withDescription("The checkout whose harness should record sessions"),
    ),
    repo: repoArgument,
  },
  ({ key, repo, root, work }) =>
    Effect.gen(function* () {
      const directory = path.resolve(work, ".chr33s");
      const script = path.join(directory, "session.sh");
      const settings = path.resolve(work, ".claude", "settings.json");

      yield* Effect.try({
        try: () => {
          fs.mkdirSync(directory, { recursive: true });
          fs.writeFileSync(
            script,
            hookScript({
              work: path.resolve(work),
              root: path.resolve(root),
              repo,
              key: path.resolve(key),
            }),
            { mode: 0o755 },
          );

          // Merged into whatever is already there, and matched by the command
          // it would write: an operator's other hooks are not this command's
          // to remove, and running it twice must not record everything twice.
          fs.mkdirSync(path.dirname(settings), { recursive: true });
          const existing: { hooks?: Record<string, Array<unknown>> } = fs.existsSync(settings)
            ? JSON.parse(fs.readFileSync(settings, "utf8"))
            : {};
          const hooks = existing.hooks ?? {};
          for (const [event, phase] of [
            ["UserPromptSubmit", "start"],
            ["Stop", "stop"],
          ] as const) {
            const entry = entryFor(script, phase);
            const legacy = {
              hooks: [
                {
                  type: "command",
                  command: `node ${JSON.stringify(path.join(directory, "session.mjs"))} ${phase}`,
                },
              ],
            };
            const already = (hooks[event] ?? []).filter(
              (value) =>
                JSON.stringify(value) !== JSON.stringify(entry) &&
                JSON.stringify(value) !== JSON.stringify(legacy),
            );
            hooks[event] = [...already, entry];
          }
          fs.writeFileSync(settings, `${JSON.stringify({ ...existing, hooks }, null, 2)}\n`);
          return { script, settings };
        },
        catch: (cause) =>
          new Invalid({ field: "work", reason: `cannot write hooks: ${String(cause)}` }),
      });

      yield* Console.log(`Recording sessions for ${repo}:`);
      yield* Console.log(`  ${script}`);
      yield* Console.log(`  ${settings}`);
    }),
);

const memoryShow = Command.make(
  "memory",
  {
    root: rootFlag,
    distill: Flag.boolean("distill").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Rebuild it from the sessions first"),
    ),
    repo: repoArgument,
  },
  ({ distill, repo, root }) =>
    Effect.gen(function* () {
      const note = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          if (!distill) return yield* Memory.read();
          // Rebuilt rather than merged: the sessions are the record, and a
          // view of them that could drift from what it cites would be worse
          // than no view at all.
          const fresh = yield* Memory.distill();
          const text = Memory.render(fresh.entries, fresh.sessions);
          yield* Memory.write(text);
          return text;
        }),
      );
      yield* Console.log(note ?? "no memory yet; run with --distill");
    }),
);

export const sessionCommand = Command.make("session", {}, () =>
  Console.log("git+ session <open|produce|show|ask|answer|redact|enable|hook|memory> — see --help"),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Record who was instructed, and what they were asked")),
    produce.pipe(Command.withDescription("Record what a session produced")),
    show.pipe(Command.withDescription("What a session amounts to, by id or by branch")),
    ask.pipe(Command.withDescription("Record a question only a person can answer")),
    answer.pipe(Command.withDescription("Answer one, which unblocks the session that asked")),
    redact.pipe(Command.withDescription("Remove one record's content, needing hub.redact")),
    enable.pipe(Command.withDescription("Install the harness hooks that record sessions")),
    hook.pipe(Command.withDescription("Run an installed harness hook")),
    memoryShow.pipe(
      Command.withDescription("What agents have learned here, distilled from their sessions"),
    ),
  ]),
);
