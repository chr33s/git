/**
 * Sessions, driven the way a harness hook drives them: a real process, a real
 * key on disk, a real repository under a root.
 *
 * The properties worth checking are the ones the whole thing exists for — that
 * what an agent was told survives the sandbox it was told in, that a second
 * agent can read it, and that the record is bound to the repository and the
 * key that wrote it.
 */
import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import * as fsSync from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { enableHubUnder, opensshPrivateKey } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>, cwd?: string): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { cwd, encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

/**
 * The note itself, without the derivation's account of itself.
 *
 * `cli` concatenates stdout and stderr; the note is stdout, and every notice
 * about budgets, exclusions and persistence goes to stderr so a hook can pipe
 * one without parsing the other.
 */
const noteIn = (output: string): string =>
  output
    .split("\n")
    .filter(
      (line) =>
        !line.startsWith("!") &&
        !line.startsWith("note ") &&
        line !== "unchanged; the derivation matches the stored note",
    )
    .join("\n")
    .trim();

const failing = (args: ReadonlyArray<string>, cwd?: string): Promise<string> =>
  cli(args, cwd).then(
    () => "",
    (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
  );

const inRepository = <A, E>(
  directory: string,
  effect: Effect.Effect<A, E, GitRepository.Repository>,
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(nodeStores(directory)),
        ),
      ),
    ),
  );

describe("cli session", () => {
  let root = "";
  let project = "";
  let key = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-session-"));
    project = path.join(root, "project");
    key = path.join(root, "agent");
    await cli(["init", "--root", root, "project"]);
    const fixture = await enableHubUnder(root, "project", [
      "repo.read",
      "source.push",
      "hub.session",
    ]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "agent@example.com"), {
      mode: 0o600,
    });
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const openSession = async (prompt: string) =>
    (
      await cli([
        "session",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--agent",
        "claude-code",
        "--model",
        "claude-fable-5",
        "--prompt",
        prompt,
        "project",
      ])
    ).trim();

  it.live("refuses session reports and decisions with absent targets", () =>
    Effect.promise(async () => {
      for (const [verb, flags] of [
        ["produce", []],
        ["ask", ["--question", "What should happen?"]],
        ["answer", ["--decision", "missing", "--chose", "continue"]],
      ] as const) {
        const session = `missing-${verb}`;
        const output = await failing([
          "session",
          verb,
          "--root",
          root,
          "--key",
          key,
          "--session",
          session,
          ...flags,
          "project",
        ]);
        assert.match(output, /no session/, verb);
        await assert.rejects(fs.readFile(path.join(project, "refs", "hub", "session", session)), {
          code: "ENOENT",
        });
      }
      const session = await openSession("a real session with no question");
      const ref = path.join(project, "refs", "hub", "session", session);
      const before = await fs.readFile(ref, "utf8");
      const output = await failing([
        "session",
        "answer",
        "--root",
        root,
        "--key",
        key,
        "--session",
        session,
        "--decision",
        "missing",
        "--chose",
        "continue",
        "project",
      ]);
      assert.match(output, /no decision/);
      assert.equal(await fs.readFile(ref, "utf8"), before);
    }),
  );

  it.effect("records what was asked, and reads it back after the sandbox is gone", () =>
    Effect.promise(async () => {
      const session = await openSession("document how to set up agents with their own ssh key");
      assert.match(session, /^[0-9a-f-]{36}$/, `open prints the id alone: ${session}`);

      const commit = await inRepository(
        project,
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          return yield* repository.commit({
            branch: "refs/heads/claude/agent-keys",
            tree: EMPTY_TREE_OID,
            message: `Document per-agent SSH keys\n\nSession: ${session}\n`,
            author: {
              name: "Claude",
              email: "claude@agents.example.com",
              at: new Date(1_700_000_000_000),
              offset: 0,
            },
          });
        }),
      );

      await cli([
        "session",
        "produce",
        "--root",
        root,
        "--key",
        key,
        "--session",
        session,
        "--commit",
        commit,
        "--ref",
        "refs/heads/claude/agent-keys",
        "--note",
        "wrote the doc; the CLI needed a --key that takes either half",
        "--input-tokens",
        "1200",
        "--output-tokens",
        "800",
        "project",
      ]);

      // What a second agent reads. The sandbox that held the transcript is gone;
      // this is what the repository kept.
      const shown = JSON.parse(
        await cli(["session", "show", "--root", root, "--repo", "project", session]),
      );
      assert.equal(shown.session, session);

      // And the old spelling, where this argument named the repository. The
      // `--branch` guard catches the two-argument form; this one went through,
      // resolving whichever checkout the process was standing in, projecting a
      // session named after the repository, printing `exists: false` and
      // exiting zero — the silent misread the move to `--repo` was supposed to
      // prevent.
      const misread = await failing(["session", "show", "project"], path.join(root, "project"));
      assert.match(misread, /has no session 'project'/);

      assert.equal(shown.agent.kind, "claude-code");
      assert.equal(shown.agent.model, "claude-fable-5");
      assert.deepEqual(
        shown.prompts.map((entry: { prompt: string }) => entry.prompt),
        ["document how to set up agents with their own ssh key"],
      );
      assert.deepEqual(shown.commits, [commit]);
      assert.deepEqual(shown.refs, ["refs/heads/claude/agent-keys"]);
      assert.equal(shown.notes.length, 1);
      assert.deepEqual(shown.usage, { inputTokens: 1200, outputTokens: 800 });
      assert.deepEqual(shown.unreadable, []);

      // And a session this repository has never heard of is an error rather
      // than an empty document. `Session.project` answers for any id — it
      // walks a ref that need not exist — so a typo printed a projection with
      // nothing in it and exited zero, which reads as "this session did
      // nothing" rather than "there is no such session".
      const missing = await failing([
        "session",
        "show",
        "--root",
        root,
        "--repo",
        "project",
        "0192f000-0000-7000-8000-0000000000ff",
      ]);
      assert.match(missing, /has no session '0192f000-0000-7000-8000-0000000000ff'/);
    }),
  );

  it.effect("answers by branch, which is the question an agent has on checkout", () =>
    Effect.promise(async () => {
      const first = await openSession("start the thing");
      await cli([
        "session",
        "produce",
        "--root",
        root,
        "--key",
        key,
        "--session",
        first,
        "--ref",
        "refs/heads/topic",
        "project",
      ]);

      const shown = JSON.parse(
        await cli([
          "session",
          "show",
          "--root",
          root,
          "--repo",
          "project",
          "--branch",
          "refs/heads/topic",
        ]),
      );
      assert.equal(shown.session, first);
      assert.deepEqual(
        shown.prompts.map((entry: { prompt: string }) => entry.prompt),
        ["start the thing"],
      );

      const missing = await failing([
        "session",
        "show",
        "--root",
        root,
        "--repo",
        "project",
        "--branch",
        "refs/heads/nowhere",
      ]);
      assert.match(missing, /no session has produced/);

      // The old shape, where the repository was a positional. It now parses as
      // a session, `--branch` discards it, and `--root` on its own names no
      // repository — so without a check this read whatever checkout the
      // process was standing in and answered for the wrong repository.
      const positional = await failing([
        "session",
        "show",
        "--root",
        root,
        "project",
        "--branch",
        "refs/heads/topic",
      ]);
      assert.match(positional, /--root names a directory of repositories/);

      // And a session and a branch together are ambiguous whichever way they
      // were meant, so that is refused on its own terms.
      const ambiguous = await failing([
        "session",
        "show",
        "--root",
        root,
        "--repo",
        "project",
        "0192f000-0000-7000-8000-000000000001",
        "--branch",
        "refs/heads/topic",
      ]);
      assert.match(ambiguous, /name a session or --branch, not both/);
    }),
  );

  it.effect("finds the latest production even when an older session resumes", () =>
    Effect.promise(async () => {
      const first = await openSession("older session");
      const second = await openSession("newer session");
      const produce = (session: string) =>
        cli([
          "session",
          "produce",
          "--root",
          root,
          "--key",
          key,
          "--session",
          session,
          "--ref",
          "refs/heads/topic",
          "project",
        ]);
      const show = async (branch: string) =>
        JSON.parse(
          await cli(["session", "show", "--root", root, "--branch", branch, "--repo", "project"]),
        );
      await produce(first);
      await produce(second);
      assert.equal((await show("refs/heads/topic")).session, second);
      await produce(first);
      assert.equal((await show("refs/heads/topic")).session, first);
      assert.equal(
        (await show("topic")).session,
        first,
        "the documented short branch spelling works",
      );
    }),
  );

  it.effect("keeps one session's events on one ref, and hides them from a source clone", () =>
    Effect.promise(async () => {
      const session = await openSession("first");
      const other = await openSession("second");
      assert.notEqual(session, other);

      const refs = await inRepository(
        project,
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          return (yield* repository.refs).map(([name]) => name);
        }),
      );
      assert.ok(refs.includes(`refs/hub/session/${session}`), refs.join("\n"));
      assert.ok(refs.includes(`refs/hub/session/${other}`), refs.join("\n"));

      // A session ref is a hub ref, so everything hub refs already get applies
      // to it: append-only, undeletable, and out of a source-only clone's
      // advertisement. The listing a stock client sees is `refs/heads/*`.
      const advertised = refs.filter((name) => name.startsWith("refs/hub/"));
      assert.equal(advertised.length, 2, "two sessions, two refs");
    }),
  );

  it.effect("carries a question a person answers, and says what they chose", () =>
    Effect.promise(async () => {
      const session = await openSession("rename the thing or keep the alias?");

      const decision = (
        await cli([
          "session",
          "ask",
          "--root",
          root,
          "--key",
          key,
          "--session",
          session,
          "--question",
          "rename, or keep an alias?",
          "--option",
          "rename,alias",
          "project",
        ])
      ).trim();

      const blocked = JSON.parse(
        await cli(["session", "show", "--root", root, "--repo", "project", session]),
      );
      assert.equal(blocked.decisions.length, 1);
      assert.equal(blocked.decisions[0].id, decision);
      assert.equal(blocked.decisions[0].chose, null, "unanswered until somebody answers");
      assert.deepEqual(blocked.decisions[0].options, ["rename", "alias"]);

      await cli([
        "session",
        "answer",
        "--root",
        root,
        "--key",
        key,
        "--session",
        session,
        "--decision",
        decision,
        "--chose",
        "alias",
        "project",
      ]);

      const answered = JSON.parse(
        await cli(["session", "show", "--root", root, "--repo", "project", session]),
      );
      assert.equal(answered.decisions[0].chose, "alias");
    }),
  );

  it.effect("installs hooks that record a session, and installs them once", () =>
    Effect.promise(async () => {
      const work = path.join(root, "work ' tree");
      await fs.mkdir(work, { recursive: true });
      const legacyScript = path.join(work, ".chr33s", "session.mjs");
      const custom = { hooks: [{ type: "command", command: "echo custom" }] };
      await fs.mkdir(path.join(work, ".claude"));
      await fs.writeFile(
        path.join(work, ".claude", "settings.json"),
        JSON.stringify({
          hooks: Object.fromEntries(
            [
              ["UserPromptSubmit", "start"],
              ["Stop", "stop"],
            ].map(([event, phase]) => [
              event,
              [
                custom,
                {
                  hooks: [
                    { type: "command", command: `node ${JSON.stringify(legacyScript)} ${phase}` },
                  ],
                },
              ],
            ]),
          ),
        }),
      );

      await cli(["session", "enable", "--root", root, "--key", key, "--work", work, "project"]);
      await cli(["session", "enable", "--root", root, "--key", key, "--work", work, "project"]);

      // Merged rather than appended blindly: running it twice must not record
      // everything twice, and an operator's other hooks are not this command's
      // to remove.
      const settings = JSON.parse(
        await fs.readFile(path.join(work, ".claude", "settings.json"), "utf8"),
      );
      assert.equal(settings.hooks.UserPromptSubmit.length, 2);
      assert.equal(settings.hooks.Stop.length, 2);
      assert.deepEqual(settings.hooks.UserPromptSubmit[0], custom);
      assert.deepEqual(settings.hooks.Stop[0], custom);

      // And the hook actually records, driven the way the harness drives it:
      // the prompt arrives as JSON on stdin.
      const script = path.join(work, ".chr33s", "session.sh");
      // `execFileSync`, because the hook reads its event from stdin and only the
      // synchronous form takes `input` — the async one leaves the pipe open and
      // the script waits on it forever.
      execFileSync("/bin/sh", [script, "start"], {
        input: JSON.stringify({ session_id: "harness-a", prompt: "fix the flaky test" }),
        encoding: "utf8",
      });

      // Keyed by the harness's own session: two agents in one checkout must
      // not share a state file, or the second reports against the first's id.
      const keyed = (name: string, harness: string, extension: string) =>
        path.join(
          work,
          ".chr33s",
          `${name}.${createHash("sha256").update(harness).digest("hex")}.${extension}`,
        );
      const state = keyed("session", "harness-a", "id");
      const id = (await fs.readFile(state, "utf8")).trim();
      const shown = JSON.parse(
        await cli(["session", "show", "--root", root, "--repo", "project", id]),
      );
      assert.deepEqual(
        shown.prompts.map((entry: { prompt: string }) => entry.prompt),
        ["fix the flaky test"],
      );

      // A second start is the same session, not a second account of it.
      execFileSync("/bin/sh", [script, "start"], {
        input: JSON.stringify({ session_id: "harness-a", prompt: "and again" }),
        encoding: "utf8",
      });
      assert.equal((await fs.readFile(state, "utf8")).trim(), id, "one opening per session");

      // A concurrent harness session opens its own, and reports its own.
      execFileSync("/bin/sh", [script, "start"], {
        input: JSON.stringify({ session_id: "harness-b", prompt: "something else" }),
        encoding: "utf8",
      });
      const other = (await fs.readFile(keyed("session", "harness-b", "id"), "utf8")).trim();
      assert.notEqual(other, id, "concurrent harness sessions do not share an id");

      // Stopping reports what it produced and clears the state, so the next
      // prompt opens a new session rather than appending to a finished one.
      // A learning the agent left behind is handed over by file rather than on
      // a command line, and a session that left none records none.
      await fs.writeFile(
        keyed("learning", "harness-a", "txt"),
        "gotcha: the flaky test needs the production fixture\n",
      );
      execFileSync("/bin/sh", [script, "stop"], {
        input: JSON.stringify({ session_id: "harness-a" }),
        encoding: "utf8",
        env: { ...process.env, CHR33S_GIT_BRANCH: "refs/heads/topic" },
      });
      const after = JSON.parse(
        await cli(["session", "show", "--root", root, "--repo", "project", id]),
      );
      assert.deepEqual(after.refs, ["refs/heads/topic"]);
      assert.deepEqual(after.notes, ["gotcha: the flaky test needs the production fixture"]);
      assert.equal(fsSync.existsSync(state), false);
      // The learning goes with the state: redelivering it would count one
      // observation twice.
      const learning = keyed("learning", "harness-a", "txt");
      assert.equal(fsSync.existsSync(learning), false);

      // A report that does not land keeps the learning for the next stop of
      // the same harness session: discarded, it would have been counted never
      // (§10.3). The state still clears, so the next prompt opens afresh.
      await fs.writeFile(state, `${id}\n`);
      // Over the record cap, which `produce` refuses before writing anything.
      await fs.writeFile(learning, `gotcha: ${"kept until it is recorded ".repeat(12_000)}\n`);
      assert.throws(() =>
        execFileSync("/bin/sh", [script, "stop"], {
          input: JSON.stringify({ session_id: "harness-a" }),
          encoding: "utf8",
          stdio: ["pipe", "pipe", "pipe"],
        }),
      );
      assert.equal(fsSync.existsSync(state), false);
      assert.equal(fsSync.existsSync(learning), true, "an unrecorded learning is not discarded");
    }),
  );

  it.live("keeps overlapping harness sessions separate in one checkout", () =>
    Effect.promise(async () => {
      const work = path.join(root, "shared-work");
      await cli(["session", "enable", "--root", root, "--key", key, "--work", work, "project"]);
      const script = path.join(work, ".chr33s", "session.sh");
      const invoke = (phase: string, session: string, prompt: string, branch = "") =>
        execFileSync("/bin/sh", [script, phase], {
          input: JSON.stringify({ session_id: session, prompt }),
          encoding: "utf8",
          env: { ...process.env, CHR33S_GIT_BRANCH: branch },
        });

      invoke("start", "harness-a", "first session's work");
      invoke("start", "harness-b", "second session's work");
      invoke("start", "harness-a", "duplicate start");
      const ids = await inRepository(
        project,
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          return (yield* repository.refs)
            .map(([name]) => name)
            .filter((name) => name.startsWith("refs/hub/session/"))
            .map((name) => name.slice("refs/hub/session/".length));
        }),
      );
      assert.equal(ids.length, 2, "each harness opens its own session; duplicate starts reuse it");

      invoke("stop", "never-started", "", "refs/heads/unrelated");
      invoke("stop", "harness-b", "", "refs/heads/second");
      invoke("stop", "harness-a", "", "refs/heads/first");
      for (const id of ids) {
        const shown = JSON.parse(
          await cli(["session", "show", "--root", root, "--repo", "project", id]),
        );
        assert.equal(shown.prompts.length, 1);
        const prompt = shown.prompts[0].prompt;
        assert.ok(["first session's work", "second session's work"].includes(prompt));
        assert.deepEqual(shown.refs, [
          prompt === "first session's work" ? "refs/heads/first" : "refs/heads/second",
        ]);
      }
      assert.deepEqual(
        (await fs.readdir(path.join(work, ".chr33s"))).filter((name) => name.endsWith(".id")),
        [],
        "stopping both harnesses clears both pending records",
      );
    }),
  );

  it.effect("compounds what sessions learned, and forgets what their source lost", () =>
    Effect.promise(async () => {
      const note = async (text: string) => {
        const session = await openSession("some work");
        await cli([
          "session",
          "produce",
          "--root",
          root,
          "--key",
          key,
          "--session",
          session,
          "--note",
          text,
          "project",
        ]);
        return session;
      };

      const first = await note(
        "gotcha: run npm install before typecheck; postinstall applies patches",
      );
      await note("gotcha: run npm install before typecheck; postinstall applies patches");
      const only = await note("convention: tests colocate as *.test.ts beside sources");

      const memory = await cli(["session", "memory", "--root", root, "--distill", "project"]);

      // Ordered by how often a thing was observed, which is also the eviction
      // rule: the twice-seen gotcha outranks the once-seen convention.
      assert.match(memory, /gotcha: run npm install/);
      assert.match(memory, /2 observation\(s\)/);
      assert.match(memory, /convention: tests colocate/);
      assert.ok(
        memory.indexOf("gotcha:") < memory.indexOf("convention:"),
        `what was seen more often comes first: ${memory}`,
      );

      // Cited, so a reader can check it against the record rather than trust
      // it — in the entries themselves, not only in the stamp line, which
      // names the newest session whatever the entries cite.
      const entries = noteIn(memory);
      assert.ok(entries.includes(first), `entries name the sessions they came from: ${memory}`);
      assert.ok(entries.includes(only), `entries name the sessions they came from: ${memory}`);

      // Read back without distilling: it is a note now, not a computation —
      // and it says so, because a stored note can come from another branch or
      // from before a redaction (§8.4).
      const stored = await cli(["session", "memory", "--root", root, "project"]);
      assert.match(stored, /historical cache/);
      assert.equal(noteIn(stored), noteIn(memory));
    }),
  );

  it.effect("refuses a prompt that carries a credential, and not one that mentions an oid", () =>
    Effect.promise(async () => {
      // The scan is over what somebody typed, not over the record. A RepoID is
      // base64 of a digest and a trust head is forty hex characters, so a scan
      // of the envelope refused every session on every hub-enabled repository —
      // which is how a scanner teaches an operator to turn it off.
      const ordinary = await openSession(
        "the fold at 90c7f2e1b4a8d3f5c6e7a0b1c2d3e4f5a6b7c8d9 walks refs/hub/pr/*",
      );
      assert.match(ordinary, /^[0-9a-f-]{36}$/);

      const refused = await failing([
        "session",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--prompt",
        "deploy with ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789",
        "project",
      ]);
      assert.match(refused, /provider token/);
      // And the refusal does not reprint what it refused.
      assert.ok(!refused.includes("VwXyZ0123456789"), refused);
    }),
  );

  it.effect("refuses to record a session against a repository that has no identity", () =>
    Effect.promise(async () => {
      await cli(["init", "--root", root, "plain"]);
      const refused = await failing([
        "session",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--prompt",
        "anything",
        "plain",
      ]);
      assert.match(refused, /has no genesis/);
    }),
  );
});
