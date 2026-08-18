/**
 * `chr33s-git session …` — recording what an agent was told, and what came
 * of it.
 *
 * Plumbing, by design: the expected caller is a harness hook — a Claude Code
 * `SessionStart` writing the opening, a `Stop` writing what was produced — not
 * a person typing ceremonies. `open` prints the session id alone, so a hook
 * can capture it and put it in a commit trailer without parsing prose.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { readGenesis } from "../trust/Genesis.ts";
import * as Memory from "../hub/Memory.ts";
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
      reason: `${repo} has no genesis; run \`chr33s-git hub init ${repo} --key <key>\` first`,
    });
  }
  return stored.genesis.repoId;
});

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);

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

/**
 * The script the harness actually runs.
 *
 * Written into the work tree rather than generated inline in a settings file,
 * for two reasons: a hook an operator can read is one they can correct, and
 * the prompt arrives as JSON on the hook's stdin, which is more than a shell
 * one-liner should be asked to parse.
 *
 * It records at most one opening per session and, when the session ends, what
 * the branch it worked on came to. Everything it passes to the CLI it got from
 * the harness or from git — never from a hub event, which is somebody else's
 * text (docs/agents.md §8).
 */
const hookScript = (input: {
  readonly cli: string;
  readonly root: string;
  readonly repo: string;
  readonly key: string;
}) => `#!/usr/bin/env node
// Written by \`chr33s-git session enable\`. Safe to edit; safe to delete.
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

const CLI = ${JSON.stringify(input.cli)};
const ROOT = ${JSON.stringify(input.root)};
const REPO = ${JSON.stringify(input.repo)};
const KEY = ${JSON.stringify(input.key)};
const STATE = path.join(import.meta.dirname, "session.id");

const run = (args) =>
  execFileSync(process.execPath, [CLI, ...args], { encoding: "utf8" }).trim();

const read = async () => {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    return {};
  }
};

const event = await read();

if (process.argv[2] === "start") {
  // One opening per session: the harness may call this more than once, and a
  // second opening would be a second account of the same work.
  if (!fs.existsSync(STATE)) {
    const session = run([
      "session", "open",
      "--root", ROOT, "--key", KEY,
      "--agent", "claude-code",
      "--model", process.env.CLAUDE_MODEL ?? "",
      "--harness", process.env.CLAUDE_CODE_VERSION ?? "",
      "--prompt", event.prompt ?? "",
      REPO,
    ]);
    fs.writeFileSync(STATE, session);
  }
} else if (fs.existsSync(STATE)) {
  const session = fs.readFileSync(STATE, "utf8").trim();
  const branch = process.env.CHR33S_GIT_BRANCH ?? "";
  run([
    "session", "produce",
    "--root", ROOT, "--key", KEY,
    "--session", session,
    ...(branch === "" ? [] : ["--ref", branch]),
    REPO,
  ]);
  // The session is closed by being reported: the next start opens a new one.
  fs.rmSync(STATE, { force: true });
}
`;

/** The hook entries this writes, which is also how it recognises its own. */
const entryFor = (script: string, phase: "start" | "stop") => ({
  hooks: [{ type: "command", command: `node ${JSON.stringify(script)} ${phase}` }],
});

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
      const script = path.join(directory, "session.mjs");
      const settings = path.resolve(work, ".claude", "settings.json");

      yield* Effect.try({
        try: () => {
          fs.mkdirSync(directory, { recursive: true });
          fs.writeFileSync(
            script,
            hookScript({
              cli: path.resolve(import.meta.dirname, "bin.ts"),
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
            const already = (hooks[event] ?? []).filter(
              (value) => JSON.stringify(value) !== JSON.stringify(entry),
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
  Console.log("chr33s-git session <open|produce|show|ask|answer|enable|memory> — see --help"),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Record who was instructed, and what they were asked")),
    produce.pipe(Command.withDescription("Record what a session produced")),
    show.pipe(Command.withDescription("What a session amounts to, by id or by branch")),
    ask.pipe(Command.withDescription("Record a question only a person can answer")),
    answer.pipe(Command.withDescription("Answer one, which unblocks the session that asked")),
    enable.pipe(Command.withDescription("Install the harness hooks that record sessions")),
    memoryShow.pipe(
      Command.withDescription("What agents have learned here, distilled from their sessions"),
    ),
  ]),
);
