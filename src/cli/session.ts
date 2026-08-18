/**
 * `chr33s-git session …` — recording what an agent was told, and what came
 * of it.
 *
 * Plumbing, by design: the expected caller is a harness hook — a Claude Code
 * `SessionStart` writing the opening, a `Stop` writing what was produced — not
 * a person typing ceremonies. `open` prints the session id alone, so a hook
 * can capture it and put it in a commit trailer without parsing prose.
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { readGenesis } from "../trust/Genesis.ts";
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

export const sessionCommand = Command.make("session", {}, () =>
  Console.log("chr33s-git session <open|produce|show> — see --help"),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Record who was instructed, and what they were asked")),
    produce.pipe(Command.withDescription("Record what a session produced")),
    show.pipe(Command.withDescription("What a session amounts to, by id or by branch")),
  ]),
);
