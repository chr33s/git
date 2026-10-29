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
import { Argument, Command, Flag } from "effect/cli";

import { Invalid } from "../git/Error.ts";
import { readGenesis } from "../trust/Genesis.ts";
import * as Memory from "../hub/Memory.ts";
import * as Concept from "../knowledge/Concept.ts";
import * as Session from "../hub/Session.ts";
import * as Capture from "../hub/SessionCapture.ts";
import * as Handoff from "./SessionHandoff.node.ts";
import * as Briefing from "./Briefing.node.ts";
import * as Trace from "../hub/Trace.ts";
import { Repository } from "../git/Repository.ts";
import * as Invocation from "../telemetry/Invocation.ts";
import * as Audit from "./audit.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import {
  membershipOrNull,
  readPrivateKey,
  repoArgument,
  repoFlag,
  rootFlag,
  withDiscovered,
  withRepo,
  commaList,
} from "./shared.ts";

/** Comma-separated list flags, which is how a hook passes several of a thing. */
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

/**
 * The repository's identity and its membership, for an audit that judges both.
 *
 * Built once per command rather than per record: the projection audits every
 * exposure a session holds, and rebuilding the trust walk for each of them
 * would make a long run's audit quadratic in the trust log.
 */
const membership = Effect.fn("session.membership")(function* () {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: "this repository has no genesis, so its trace cannot be judged",
    });
  }
  return { repo: stored.genesis.repoId, trust: yield* projectTrust(stored.genesis) };
});

/**
 * A learning from a file, or from stdin.
 *
 * The same bounded validation the `--note` path gets — `Session.issue` scans
 * it for secrets and holds it to `MAX_PAYLOAD` — reached by a route that keeps
 * multiline or sensitive text out of process arguments (§10.2). Trailing
 * whitespace goes because an editor added it, not because this is rewriting
 * what somebody wrote.
 */
const readNote = Effect.fn("session.readNote")(function* (location: string) {
  const contents = yield* Effect.try({
    try: () => fs.readFileSync(location === "-" ? 0 : location, "utf8"),
    catch: () =>
      new Invalid({
        field: "note-file",
        reason:
          location === "-" ? "cannot read the learning from stdin" : `cannot read ${location}`,
      }),
  });
  return contents.trim();
});

const keyFlag = Flag.String("key").pipe(
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
    agent: Flag.String("agent").pipe(
      Flag.withDefault("unknown"),
      Flag.withDescription("What kind of agent this is, e.g. claude-code"),
    ),
    model: Flag.String("model").pipe(Flag.withDefault(""), Flag.withDescription("Model name")),
    harness: Flag.String("harness").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Harness and version"),
    ),
    prompt: Flag.String("prompt").pipe(Flag.withDescription("The instruction, as given")),
    role: Flag.Literals("role", ["user", "system"]).pipe(Flag.withDefault("user" as const)),
    instructions: Flag.String("instructions").pipe(
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
    session: Flag.String("session").pipe(Flag.withDescription("The session this reports on")),
    commit: Flag.String("commit").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Commits this session produced, comma-separated"),
    ),
    ref: Flag.String("ref").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Refs it wrote, comma-separated"),
    ),
    pull: Flag.String("pull").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Pull requests it opened, comma-separated"),
    ),
    note: Flag.String("note").pipe(
      Flag.withDefault(""),
      Flag.withDescription("What was decided or learned, distilled"),
    ),
    noteFile: Flag.String("note-file").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Read the learning from a file, or from - for stdin"),
    ),
    inputTokens: Flag.Int("input-tokens").pipe(Flag.withDefault(0)),
    outputTokens: Flag.Int("output-tokens").pipe(Flag.withDefault(0)),
    repo: repoArgument,
  },
  ({ commit, inputTokens, key, note, noteFile, outputTokens, pull, ref, repo, root, session }) =>
    Effect.gen(function* () {
      // Two ways to say one thing is two ways to say two different things.
      // Refused rather than resolved by precedence, so a hook that passes both
      // learns which one this command would have dropped (§10.2).
      if (note !== "" && noteFile !== "") {
        return yield* new Invalid({
          field: "note",
          reason: "--note and --note-file both say what was learned; pass one",
        });
      }
      // A learning is prose somebody wrote, and prose on a command line is
      // prose in a shell history and in `ps`. The file path is a local adapter
      // input — never a path a Concept or a hub event supplied (§10.2).
      const learned = noteFile === "" ? note : yield* readNote(noteFile);
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
            commits: commaList(commit),
            refs: commaList(ref),
            pulls: commaList(pull),
            note: learned === "" ? null : learned,
            // Absent rather than zero: a harness that does not report usage
            // and one that used nothing are different facts.
            usage: inputTokens === 0 && outputTokens === 0 ? null : { inputTokens, outputTokens },
          });
        }),
      );
      yield* Console.log(written);
    }),
);

/**
 * `git+ session show [<session>] [--audit]` — the audit read path.
 *
 * The one verb in this group whose repository is a flag rather than a
 * positional, and the reason is the shape docs/telemetry.md §15 and §17 ask
 * for: `git+ session show <session> --audit` from inside a checkout. Two
 * optional positionals cannot express that — one value would fill the first —
 * so the session keeps the positional and `--repo` selects a bare repository
 * for server and administration use.
 *
 * `--audit` joins the policy-visible session projection with its
 * policy-invisible Invocation history. They stay two refs and one answer: a
 * reader should not have to know that a run's account and its runtime trace
 * live in different namespaces for different reasons.
 */
const show = Command.make(
  "show",
  {
    root: rootFlag,
    repo: repoFlag,
    branch: Flag.String("branch").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Show the session that last produced this ref instead of an id"),
    ),
    audit: Flag.Boolean("audit").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Join the run's Invocations, context exposures and capture health"),
    ),
    json: Flag.Boolean("json").pipe(
      Flag.withDefault(false),
      Flag.withDescription("The joined projection as JSON rather than the Invocation layout"),
    ),
    session: Argument.String("session").pipe(Argument.optional),
  },
  ({ audit, branch, json, repo, root, session }) =>
    Effect.gen(function* () {
      const projection = yield* withDiscovered(
        root,
        repo,
        Effect.gen(function* () {
          // A branch, because that is the question an agent has on checkout:
          // "put me back in context for this" rather than "for this id", which
          // it holds only if it opened the session itself.
          // Refused rather than resolved. This verb's repository moved from a
          // positional to `--repo`, so the old `session show <repo> --branch=x`
          // now parses `<repo>` as the session — which `--branch` then
          // discards — and `--repo ""` quietly reads the current checkout
          // instead. Wrong repository, no error. Naming both is ambiguous
          // whichever way it was meant, so it is an error either way.
          if (branch !== "" && session._tag === "Some") {
            return yield* new Invalid({
              field: "session",
              reason: `name a session or --branch, not both; --repo selects the repository`,
            });
          }

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
                  : (yield* Session.sessions()).length > 0
                    ? `no session has produced ${branch}`
                    : `this repository holds no session refs; fetch them with \`git+ hub enable --refs 'refs/hub/session/*'\``,
            });
          }

          // A session this repository has never heard of is an error, not an
          // empty document. `isSessionId` cannot help — it accepts any legal
          // ref component, which a repository name is — so the old spelling,
          // where this argument named the repository, went straight through
          // the `--branch` guard above: `git+ session show <repo>` resolved
          // whichever checkout the process was standing in, projected a
          // session named after the repository, printed `exists: false` and
          // exited zero. Existence is what actually separates the two.
          //
          // Either ref counts. `context for --session S` and `trace record`
          // write only the trace ref, so a run with a full runtime account and
          // no record of the work it was doing is an ordinary thing to ask
          // about — and `--audit` is how you ask.
          const repository = yield* Repository;
          const known =
            (yield* repository.resolve(Session.refOf(id))) !== null ||
            (yield* repository.resolve(Trace.refOf(id))) !== null;
          if (!known) {
            // And say which of the two it is. `hub enable` does not fetch
            // `refs/hub/session/*` — agents.md §10 makes it opt-in, so that a
            // replica can hold review state without the provenance firehose —
            // and a clone that took the default has no session refs at all,
            // where "no session 'X'" reads as a typo rather than as a
            // configuration.
            const any = (yield* Session.sessions()).length > 0;
            return yield* new Invalid({
              field: "session",
              reason: any
                ? `this repository has no session '${id}'; --repo selects the repository`
                : `this repository holds no session refs; fetch them with \`git+ hub enable --refs 'refs/hub/session/*'\``,
            });
          }

          const projected = yield* Session.project(id);
          if (!audit) return { session: projected, audit: null } as const;

          // The trace ref is read only when it is asked for. It is the large,
          // policy-invisible half, and a `session show` that folded it every
          // time would make the cheap question pay for the expensive one.
          const { repo: identity, trust } = yield* membership();
          return {
            session: projected,
            audit: yield* Invocation.project({ session: id, repo: identity, trust }),
          } as const;
        }),
      );

      // The session projection stays JSON whether or not `--audit` was asked
      // for: its reader is a harness hook, and a hook that had to parse prose
      // is a hook that breaks on the next wording change. The audit view is
      // the human-facing one, so it renders unless a machine asks otherwise.
      if (projection.audit === null || json) {
        return yield* Console.log(
          JSON.stringify(
            projection.audit === null
              ? projection.session
              : { ...projection.session, audit: projection.audit },
            null,
            2,
          ),
        );
      }
      for (const line of Audit.renderAll(projection.audit, projection.session)) {
        yield* Console.log(line);
      }
    }),
);

const ask = Command.make(
  "ask",
  {
    root: rootFlag,
    key: keyFlag,
    session: Flag.String("session").pipe(Flag.withDescription("The session that is blocked")),
    question: Flag.String("question").pipe(Flag.withDescription("What only a person can answer")),
    option: Flag.String("option").pipe(
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
            options: commaList(option),
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
    session: Flag.String("session").pipe(Flag.withDescription("The session that asked")),
    decision: Flag.String("decision").pipe(Flag.withDescription("The question being answered")),
    chose: Flag.String("chose").pipe(Flag.withDescription("The answer")),
    note: Flag.String("note").pipe(Flag.withDefault("")),
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

/**
 * Where a harness session's pending report and its learning live.
 *
 * Keyed by the harness's own session, hashed: two agents in one checkout must
 * not consume each other's pending report, or hand over each other's learning
 * (docs/context-pack.knowledge.md §10.3). The legacy names stay for callers
 * without a harness id.
 */
const hookPaths = (work: string, harness: string | undefined) => {
  const key = harness ? `.${createHash("sha256").update(harness).digest("hex")}` : "";
  const directory = path.resolve(work, ".chr33s");
  return {
    state: path.join(directory, `session${key}.id`),
    learning: path.join(directory, `learning${key}.txt`),
    pending: path.join(directory, `output${key}.json`),
    briefing: path.join(directory, `briefing${key}.json`),
  };
};

/**
 * The memory derived over the committed view HEAD names, where there is one.
 *
 * Concepts are ordinary source files, and a repository with no commits has no
 * Concepts — only session learnings.
 */
const deriveMemory = Effect.fn("session.deriveMemory")(function* (bundle: string) {
  const repository = yield* Repository;
  const sourceRef = yield* repository.head;
  return yield* Memory.derive({ sourceRef, bundle, ...(yield* membershipOrNull()) });
});

const hook = Command.make(
  "hook",
  {
    root: rootFlag,
    key: keyFlag,
    work: Flag.String("work"),
    repo: repoArgument,
    phase: Argument.Literals("phase", ["start", "stop"]),
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
      const { state, learning, pending, briefing } = hookPaths(work, event.session_id);
      const exists = yield* hookFile(() => fs.existsSync(state));
      const saved = yield* Handoff.read(pending);
      if (phase === "start") {
        // One opening per session: the harness calls this on every prompt,
        // and a second opening would be a second account of the same work.
        const recall = yield* Config.Boolean("CHR33S_GIT_STARTUP_MEMORY").pipe(
          Config.withDefault(true),
        );
        if (exists) {
          if (recall)
            yield* Effect.gen(function* () {
              const session = yield* hookFile(() => fs.readFileSync(state, "utf8").trim());
              const built = yield* withRepo(root, repo, deriveMemory(Concept.BUNDLE));
              const boundary = yield* Briefing.record({
                location: briefing,
                session,
                built,
                delivered: false,
              });
              if (boundary.invalidated)
                yield* Console.log(
                  "Repository memory: some earlier recalled support is no longer validated by the current evidence view. Earlier context cannot be withdrawn; revalidate it against current sources.",
                );
            }).pipe(
              Effect.catch(() =>
                Console.error(
                  "git+: current memory validation is unavailable; earlier recalled data has not been revalidated",
                ),
              ),
            );
          return;
        }
        if (saved !== null) {
          const output = yield* Session.decode(new TextEncoder().encode(saved.payload));
          const identity = yield* withRepo(root, repo, identityOf(repo));
          if (output.repo !== identity)
            return yield* new Invalid({
              field: "handoff",
              reason: "pending output belongs to another repository",
            });
          yield* Handoff.writeState(state, output.session);
          return;
        }
        const signer = yield* readPrivateKey(key);
        const model = yield* Config.String("CLAUDE_MODEL").pipe(Config.withDefault(""));
        const harness = yield* Config.String("CLAUDE_CODE_VERSION").pipe(Config.withDefault(""));
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
        yield* Handoff.writeState(state, opened.session);

        if (recall)
          yield* Effect.gen(function* () {
            const built = yield* withRepo(root, repo, deriveMemory(Concept.BUNDLE));
            yield* Briefing.record({
              location: briefing,
              session: opened.session,
              built,
              delivered: built.entries.length > 0,
            });
            if (built.entries.length > 0) yield* Console.log(built.text);
          }).pipe(
            Effect.catch(() =>
              Console.error("git+: startup memory unavailable; continuing without it"),
            ),
          );
        // Where the *agent* leaves what it learned. A stop hook cannot infer a
        // useful discovery from a branch name, so the learning is handed over
        // explicitly or there is none — and no learning is a valid outcome
        // (§10.2). Said here because the path is keyed by a hash the agent
        // cannot guess.
        yield* Console.log(
          `To record what this session learned, write it to ${learning} before stopping.`,
        );
      } else if (exists || saved !== null) {
        yield* Handoff.withLock(
          state,
          Effect.gen(function* () {
            const output =
              saved === null
                ? null
                : yield* Session.decode(new TextEncoder().encode(saved.payload));
            const session = exists
              ? yield* hookFile(() => fs.readFileSync(state, "utf8").trim())
              : output?.session;
            if (session === undefined)
              return yield* new Invalid({
                field: "handoff",
                reason: "pending output has no session identity",
              });
            const learned = yield* hookFile(() => fs.existsSync(learning));
            const retained = saved;
            const branch = yield* Config.String("CHR33S_GIT_BRANCH").pipe(Config.withDefault(""));
            // Explicit harness output only. A branch hint never guesses authored commits.
            const commits = yield* Config.String("CHR33S_GIT_COMMITS").pipe(Config.withDefault(""));
            const pulls = yield* Config.String("CHR33S_GIT_PULLS").pipe(Config.withDefault(""));
            const usageJson = yield* Config.String("CHR33S_GIT_USAGE").pipe(Config.withDefault(""));
            const usage =
              usageJson === "" || retained !== null
                ? null
                : yield* Schema.decodeEffect(
                    Schema.fromJsonString(
                      Schema.Struct({ inputTokens: Schema.Int, outputTokens: Schema.Int }),
                    ),
                  )(usageJson).pipe(
                    Effect.mapError(
                      (cause) => new Invalid({ field: "usage", reason: cause.message }),
                    ),
                  );
            const prepared =
              retained ??
              (yield* withRepo(
                root,
                repo,
                Effect.gen(function* () {
                  yield* existingSession(session);
                  const signer = yield* readPrivateKey(key);
                  return yield* Capture.prepare({
                    repo: yield* identityOf(repo),
                    session,
                    key: signer,
                    commits: commaList(commits),
                    refs: commaList(branch),
                    pulls: commaList(pulls),
                    note: learned ? (yield* readNote(learning)) || null : null,
                    usage,
                  });
                }),
              ));
            // The identity and exact signed bytes survive a crash before or after append.
            if (retained === null) yield* Handoff.write(pending, prepared);
            const recorded = yield* withRepo(
              root,
              repo,
              Effect.gen(function* () {
                return yield* Capture.append(prepared, { repo: yield* identityOf(repo), session });
              }),
            );
            yield* hookFile(() => fs.rmSync(learning, { force: true }));
            const persisted = yield* withRepo(
              root,
              repo,
              Effect.gen(function* () {
                return yield* Memory.write(yield* deriveMemory(Concept.BUNDLE));
              }),
            ).pipe(Effect.orElseSucceed(() => null));
            const available =
              persisted !== null &&
              (persisted.state === "written" || persisted.state === "unchanged");
            if (available) {
              yield* hookFile(() => fs.rmSync(state, { force: true }));
              yield* hookFile(() => fs.rmSync(pending, { force: true }));
            } else {
              yield* Console.error(
                "git+: the learning was recorded; memory is unavailable; retry stop to rebuild it",
              );
            }
            yield* Console.log(
              JSON.stringify({
                version: 1,
                state: available ? "recorded" : "recorded-projection-unavailable",
                commit: recorded.commit,
                recovered: recorded.recovered,
              }),
            );
          }),
        ).pipe(
          Effect.tapError(() =>
            Console.error(
              "git+: outcome not acknowledged; this session's handoff is retained for retry",
            ).pipe(
              Effect.andThen(Console.log(JSON.stringify({ version: 1, state: "not-recorded" }))),
            ),
          ),
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
 * Remove one record's content — the way back out of a prompt that carried
 * something it should not have.
 *
 * The scanner in front of `session open` is heuristic and says so, so this
 * way back has to exist. It writes a signed tombstone; the bytes go at the
 * next `gc`, the only pass that can tell whether the blob is reachable from
 * anywhere else — hence "at the next collection" rather than claiming a
 * removal that hasn't happened yet.
 */
const redact = Command.make(
  "redact",
  {
    root: rootFlag,
    key: keyFlag,
    session: Flag.String("session").pipe(Flag.withDescription("The session holding the record")),
    target: Flag.String("target").pipe(Flag.withDescription("The record's event id")),
    reason: Flag.String("reason").pipe(Flag.withDescription("Why it is being removed")),
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
    work: Flag.String("work").pipe(
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
      // The learning handoff is explicit by design: the component that can
      // judge the work is the one that supplies it (§10.2).
      yield* Console.log("");
      yield* Console.log(
        `To record a durable learning, write one line of it to ${path.join(directory, "learning.<session>.txt")} before the session ends.`,
      );
    }),
);

/**
 * `git+ session memory [--distill]` — what this repository has learned.
 *
 * Two read paths on purpose (§12.4). A plain read shows the note as it stands,
 * labelled as the historical cache it is; `--distill` re-derives from the
 * Concepts and session records that are eligible *now* and persists the
 * result. Automatic harness injection uses the derivation, never the note's
 * raw text — a note can come from another branch, another host, or from before
 * a redaction, and its stamp is unsigned (§8.4).
 */
const memoryShow = Command.make(
  "memory",
  {
    root: rootFlag,
    distill: Flag.Boolean("distill").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Rebuild it from the current Concepts and session records, and persist"),
    ),
    derive: Flag.Boolean("derive").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Print the validated projection without persisting it"),
    ),
    bundle: Flag.String("bundle").pipe(
      Flag.withDefault(Concept.BUNDLE),
      Flag.withDescription("Repository-relative Concept bundle root"),
    ),
    repo: repoArgument,
  },
  ({ bundle, derive, distill, repo, root }) =>
    Effect.gen(function* () {
      const result = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          if (!distill && !derive) {
            const note = yield* Memory.read();
            return { note, derived: null, persisted: null } as const;
          }

          const built = yield* deriveMemory(bundle);
          if (!distill) return { note: built.text, derived: built, persisted: null } as const;
          return {
            note: built.text,
            derived: built,
            persisted: yield* Memory.write(built),
          } as const;
        }),
      );

      if (result.note === null) {
        return yield* Console.log("no memory yet; run with --distill");
      }
      // An empty projection is no memory. Printed as the note, its heading and
      // stamp would be framed as cited data by the hook that pipes stdout into
      // a prompt, on every session of a repository with nothing to recall.
      yield* Console.log(
        result.derived !== null && result.derived.entries.length === 0
          ? "no memory yet; nothing eligible to recall"
          : result.note,
      );

      // Everything below is the account of the derivation, on stderr so the
      // note itself stays the whole of stdout for a hook that pipes it.
      const derived = result.derived;
      if (derived === null) {
        return yield* Console.error(
          "! this is the stored note, which is a historical cache; --derive revalidates it",
        );
      }
      if (!derived.complete) {
        yield* Console.error("! partial: not every candidate could be collected");
      }
      if (derived.omitted > 0) {
        yield* Console.error(`! ${derived.omitted} eligible entr(ies) did not fit the budget`);
      }
      for (const candidate of derived.excluded) {
        yield* Console.error(
          `! excluded ${candidate.entry.source?.path ?? candidate.entry.text}: ${candidate.reasons.join(", ")}`,
        );
      }
      if (result.persisted !== null) {
        // Which of the three, because "written", "identical, so nothing was
        // written" and "somebody else moved the ref" are three outcomes a
        // caller acts on differently (§8.3).
        yield* Console.error(
          result.persisted.state === "written"
            ? `note ${result.persisted.commit}`
            : result.persisted.state === "unchanged"
              ? "unchanged; the derivation matches the stored note"
              : result.persisted.state === "no-anchor"
                ? "! not persisted: this repository has no genesis to anchor the note to"
                : result.persisted.state === "superseded-input"
                  ? "! not persisted: source inputs changed; derive again"
                  : `! not persisted: ${result.persisted.reason}`,
        );
      }
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
