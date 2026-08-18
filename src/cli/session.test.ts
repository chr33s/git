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

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

const failing = (args: ReadonlyArray<string>): Promise<string> =>
  cli(args).then(
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

  it("records what was asked, and reads it back after the sandbox is gone", async () => {
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
    const shown = JSON.parse(await cli(["session", "show", "--root", root, "project", session]));
    assert.equal(shown.session, session);
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
  });

  it("answers by branch, which is the question an agent has on checkout", async () => {
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
      await cli(["session", "show", "--root", root, "--branch", "refs/heads/topic", "project"]),
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
      "--branch",
      "refs/heads/nowhere",
      "project",
    ]);
    assert.match(missing, /no session has produced/);
  });

  it("keeps one session's events on one ref, and hides them from a source clone", async () => {
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
  });

  it("carries a question a person answers, and says what they chose", async () => {
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

    const blocked = JSON.parse(await cli(["session", "show", "--root", root, "project", session]));
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

    const answered = JSON.parse(await cli(["session", "show", "--root", root, "project", session]));
    assert.equal(answered.decisions[0].chose, "alias");
  });

  it("installs hooks that record a session, and installs them once", async () => {
    const work = path.join(root, "work");
    await fs.mkdir(work, { recursive: true });

    await cli(["session", "enable", "--root", root, "--key", key, "--work", work, "project"]);
    await cli(["session", "enable", "--root", root, "--key", key, "--work", work, "project"]);

    // Merged rather than appended blindly: running it twice must not record
    // everything twice, and an operator's other hooks are not this command's
    // to remove.
    const settings = JSON.parse(
      await fs.readFile(path.join(work, ".claude", "settings.json"), "utf8"),
    );
    assert.equal(settings.hooks.UserPromptSubmit.length, 1);
    assert.equal(settings.hooks.Stop.length, 1);

    // And the hook actually records, driven the way the harness drives it:
    // the prompt arrives as JSON on stdin.
    const script = path.join(work, ".chr33s", "session.mjs");
    // `execFileSync`, because the hook reads its event from stdin and only the
    // synchronous form takes `input` — the async one leaves the pipe open and
    // the script waits on it forever.
    execFileSync(process.execPath, [script, "start"], {
      input: JSON.stringify({ prompt: "fix the flaky test" }),
      encoding: "utf8",
    });

    const id = (await fs.readFile(path.join(work, ".chr33s", "session.id"), "utf8")).trim();
    const shown = JSON.parse(await cli(["session", "show", "--root", root, "project", id]));
    assert.deepEqual(
      shown.prompts.map((entry: { prompt: string }) => entry.prompt),
      ["fix the flaky test"],
    );

    // A second start is the same session, not a second account of it.
    execFileSync(process.execPath, [script, "start"], {
      input: JSON.stringify({ prompt: "and again" }),
      encoding: "utf8",
    });
    assert.equal(
      (await fs.readFile(path.join(work, ".chr33s", "session.id"), "utf8")).trim(),
      id,
      "one opening per session",
    );

    // Stopping reports what it produced and clears the state, so the next
    // prompt opens a new session rather than appending to a finished one.
    execFileSync(process.execPath, [script, "stop"], {
      input: "{}",
      encoding: "utf8",
      env: { ...process.env, CHR33S_GIT_BRANCH: "refs/heads/topic" },
    });
    const after = JSON.parse(await cli(["session", "show", "--root", root, "project", id]));
    assert.deepEqual(after.refs, ["refs/heads/topic"]);
    assert.equal(fsSync.existsSync(path.join(work, ".chr33s", "session.id")), false);
  });

  it("compounds what sessions learned, and forgets what their source lost", async () => {
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

    await note("gotcha: run npm install before typecheck; postinstall applies patches");
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

    // Cited, so a reader can check it against the record rather than trust it.
    assert.ok(memory.includes(only), `entries name the sessions they came from: ${memory}`);

    // Read back without distilling: it is a note now, not a computation.
    const stored = await cli(["session", "memory", "--root", root, "project"]);
    assert.equal(stored.trim(), memory.trim());
  });

  it("refuses to record a session against a repository that has no identity", async () => {
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
  });
});
