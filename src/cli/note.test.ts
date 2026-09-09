/**
 * `git+ note` and `git+ why`, driven as an agent drives them.
 *
 * End to end rather than unit: the parts are tested in `src/hub`, and what is
 * left to check is that they line up — that a note `note add` writes is one
 * `why` reads back, that the exit code a merge check reads means what §22 says
 * it means, and above all that the answer to "did I invalidate anything" does
 * not change just because it was asked three times.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { hasGit } from "../testing/Git.ts";
import { writeKeyPair } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const VERIFY = `export function verify(token: string): boolean {
  return token.length > 0
}
`;

let root = "";

interface Run {
  readonly stdout: string;
  readonly code: number;
}

/**
 * One command, with its exit code kept.
 *
 * §22 gives `note check` three exit codes and makes 2 mean "unresolved drift",
 * which is the whole interface a merge check consumes — so a helper that threw
 * on a non-zero exit would be unable to test the thing that matters most.
 */
const cli = async (...args: ReadonlyArray<string>): Promise<Run> => {
  try {
    const done = await execFileAsync(process.execPath, [entry, ...args], {
      encoding: "utf8",
      cwd: root,
      env: { ...process.env, XDG_CONFIG_HOME: path.join(root, "config") },
    });
    return { stdout: `${done.stdout}${done.stderr}`, code: 0 };
  } catch (failure) {
    // SAFETY: `execFile`'s rejection is always its own error object, which
    // carries the child's captured streams and exit code; only those three
    // fields are read, each guarded for absence.
    const held = failure as { stdout?: string; stderr?: string; code?: number };
    return { stdout: `${held.stdout ?? ""}${held.stderr ?? ""}`, code: held.code ?? 1 };
  }
};

const git = async (...args: ReadonlyArray<string>): Promise<void> => {
  await execFileAsync("git", ["-c", "user.name=T", "-c", "user.email=t@e.com", ...args], {
    cwd: root,
    env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" },
  });
};

const write = async (file: string, content: string): Promise<void> => {
  await fs.mkdir(path.join(root, path.dirname(file)), { recursive: true });
  await fs.writeFile(path.join(root, file), content);
};

/** One revision as an oid, since this CLI resolves refs and oids and no suffixes. */
const revision = async (rev: string): Promise<string> =>
  (await execFileAsync("git", ["rev-parse", rev], { cwd: root, encoding: "utf8" })).stdout.trim();

/** The id `note add` printed, which every later command addresses it by. */
const added = async (...args: ReadonlyArray<string>): Promise<string> => {
  const done = await cli("note", "add", "--key", "key", ...args);
  assert.equal(done.code, 0, done.stdout);
  return done.stdout.trim();
};

const statuses = async (...args: ReadonlyArray<string>) => {
  const done = await cli("note", "check", "--json", ...args);
  // SAFETY: `--json` prints exactly `HubNoteCheck`-shaped output, and a
  // non-JSON stdout means the command failed — which throws here, loudly, and
  // is what a test wants it to do.
  const parsed = JSON.parse(done.stdout) as {
    notes: ReadonlyArray<{
      id: string;
      status: string;
      path: string;
      pathMovedFrom: string | null;
    }>;
  };
  const notes = [...parsed.notes].sort((left, right) => left.path.localeCompare(right.path));
  return { notes, code: done.code };
};

describe.skipIf(!hasGit)("git+ note, end to end", () => {
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-note-"));
    await git("init", "--quiet", "--initial-branch=main", ".");
    await write("src/auth.ts", VERIFY);
    await write("docs/readme.md", "# Readme\n\nintro\n");
    await git("add", "-A");
    await git("commit", "--quiet", "-m", "first");
    await writeKeyPair(path.join(root, "key"), "author@example.com");
    // The work tree's own repository is the one the note commands sign for.
    const identity = await cli("hub", "init", "--root", ".", "--key", "key", ".git");
    assert.equal(identity.code, 0, identity.stdout);
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it("writes a constraint and reads it back where it applies", async () => {
    const note = await added(
      "--anchor",
      "function verify",
      "src/auth.ts",
      "must stay constant-time",
    );

    const why = await cli("why", "src/auth.ts");
    assert.equal(why.code, 0);
    assert.match(why.stdout, /function verify/);
    assert.match(why.stdout, /must stay constant-time/);
    assert.match(why.stdout, /status: fresh/);
    assert.match(why.stdout, new RegExp(note));

    // A different subtree says nothing about it.
    const elsewhere = await cli("why", "docs/");
    assert.doesNotMatch(elsewhere.stdout, /must stay constant-time/);
  });

  it("keeps reporting drift until a signed judgment resolves it", async () => {
    const note = await added(
      "--anchor",
      "function verify",
      "src/auth.ts",
      "must stay constant-time",
    );
    await write("src/auth.ts", VERIFY.replace("length > 0", "length > 2"));

    // §15, which is the invariant the whole feature exists for: looking at the
    // changed code, repeatedly, is never the same as agreeing with it.
    for (let round = 0; round < 3; round++) {
      const audited = await statuses();
      assert.deepEqual(
        audited.notes.map((entry) => entry.status),
        ["content-changed"],
        `round ${round}`,
      );
      assert.equal(audited.code, 2, "unresolved drift is exit 2");
    }

    const confirmed = await cli("note", "confirm", "--key", "key", note);
    assert.equal(confirmed.code, 0, confirmed.stdout);

    const after = await statuses();
    assert.deepEqual(
      after.notes.map((entry) => entry.status),
      ["fresh"],
    );
    assert.equal(after.code, 0);
  });

  it("separates formatting from a body change and a body from a declaration", async () => {
    await added("--anchor", "function verify", "src/auth.ts", "must stay constant-time");

    await write(
      "src/auth.ts",
      "export function verify( token: string ): boolean {\n\n  // unchanged\n  return token.length > 0\n}\n",
    );
    assert.deepEqual(
      (await statuses()).notes.map((entry) => entry.status),
      ["fresh"],
    );

    await write("src/auth.ts", VERIFY.replace("length > 0", "length > 2"));
    assert.deepEqual(
      (await statuses()).notes.map((entry) => entry.status),
      ["content-changed"],
    );

    await write("src/auth.ts", VERIFY.replace("token: string", "token: Uint8Array"));
    assert.deepEqual(
      (await statuses()).notes.map((entry) => entry.status),
      ["contract-changed"],
    );
  });

  it("checks only what this work tree touched when asked to", async () => {
    await added("--anchor", "function verify", "src/auth.ts", "must stay constant-time");
    await added("docs/readme.md", "the synopsis is generated");

    await write("docs/readme.md", "# Readme\n\nrewritten\n");

    const touched = await statuses("--touched");
    assert.deepEqual(
      touched.notes.map((entry) => entry.path),
      ["docs/readme.md"],
      "the untouched note is not this change's problem",
    );

    // The whole-repository audit still sees both, and both are fresh apart
    // from the one that moved.
    const everything = await statuses();
    assert.deepEqual(
      everything.notes.map((entry) => [entry.path, entry.status]),
      [
        ["docs/readme.md", "content-changed"],
        ["src/auth.ts", "fresh"],
      ],
    );
  });

  it("follows a rename rather than reporting the source gone", async () => {
    await added("--anchor", "function verify", "src/auth.ts", "must stay constant-time");

    await fs.mkdir(path.join(root, "src/security"), { recursive: true });
    await git("mv", "src/auth.ts", "src/security/auth.ts");
    await git("commit", "--quiet", "-m", "move");

    const audited = await statuses();
    const only = audited.notes[0];
    assert.equal(only?.status, "fresh");
    assert.equal(only?.path, "src/security/auth.ts");
    assert.equal(only?.pathMovedFrom, "src/auth.ts");
    assert.equal(audited.code, 0);
  });

  it("scopes a merge check to the paths a range changed", async () => {
    await added("--anchor", "function verify", "src/auth.ts", "must stay constant-time");
    await added("docs/readme.md", "the synopsis is generated");

    const base = await revision("HEAD");
    await write("docs/readme.md", "# Readme\n\nrewritten\n");
    await git("add", "-A");
    await git("commit", "--quiet", "-m", "docs only");
    const head = await revision("HEAD");

    // The range touched the docs and nothing else, so the constraint on
    // `src/auth.ts` is not what this change has to answer for.
    const ranged = await statuses("--base", base, "--head", head);
    assert.deepEqual(
      ranged.notes.map((entry) => [entry.path, entry.status]),
      [["docs/readme.md", "content-changed"]],
    );
    assert.equal(ranged.code, 2);
  });

  it("refuses a bare anchor two declarations answer to, and names them", async () => {
    await write("src/auth.ts", `${VERIFY}\nexport class verify {}\n`);
    const refused = await cli(
      "note",
      "add",
      "--key",
      "key",
      "--anchor",
      "verify",
      "src/auth.ts",
      "x",
    );
    assert.notEqual(refused.code, 0);
    assert.match(refused.stdout, /ambiguous/i);
    assert.match(refused.stdout, /function verify/);
    assert.match(refused.stdout, /class verify/);
  });

  it("retires a constraint without letting it block a merge", async () => {
    const note = await added(
      "--anchor",
      "function verify",
      "src/auth.ts",
      "must stay constant-time",
    );
    await write("src/auth.ts", VERIFY.replace("length > 0", "length > 2"));
    assert.equal((await statuses()).code, 2);

    const retired = await cli("note", "retire", "--key", "key", "--reason", "helper deleted", note);
    assert.equal(retired.code, 0, retired.stdout);

    // §7.4: a retired note preserves its history and stops blocking.
    const after = await statuses();
    assert.deepEqual(after.notes, []);
    assert.equal(after.code, 0);

    const restored = await cli("note", "restore", "--key", "key", note);
    assert.equal(restored.code, 0, restored.stdout);
    // Restored against the source as it stands, so it is fresh rather than
    // drifted — the restore recorded a baseline, which is a signed judgment.
    assert.deepEqual(
      (await statuses()).notes.map((entry) => entry.status),
      ["fresh"],
    );
  });

  it("answers a bare `why` with the pinned constraints", async () => {
    await added("--anchor", "function verify", "src/auth.ts", "must stay constant-time");
    await added("--pinned", "docs/readme.md", "the synopsis is generated");

    const opening = await cli("why");
    assert.equal(opening.code, 0);
    assert.match(opening.stdout, /the synopsis is generated/);
    assert.doesNotMatch(opening.stdout, /must stay constant-time/);
    assert.match(opening.stdout, /docs\/readme\.md#@file/);
  });

  it("suggests constraints from sessions and records none of them", async () => {
    const before = await cli("note", "candidates", "--json");
    assert.deepEqual(JSON.parse(before.stdout), { candidates: [] });

    const head = await revision("HEAD");
    const opened = await cli(
      "session",
      "open",
      "--root",
      ".",
      "--key",
      "key",
      "--agent",
      "claude-code",
      "--model",
      "opus",
      "--harness",
      "cli",
      "--prompt",
      "fix the path handling",
      ".git",
    );
    assert.equal(opened.code, 0, opened.stdout);
    const session = opened.stdout.trim().split("\n")[0] ?? "";

    const produced = await cli(
      "session",
      "produce",
      "--root",
      ".",
      "--key",
      "key",
      "--session",
      session,
      "--commit",
      head,
      "--note",
      "gotcha: drive-letter comparison must stay case-insensitive",
      ".git",
    );
    assert.equal(produced.code, 0, produced.stdout);

    const suggested = await cli("note", "candidates");
    assert.equal(suggested.code, 0, suggested.stdout);
    assert.match(suggested.stdout, /drive-letter comparison/);
    assert.match(suggested.stdout, /git\+ note add src\/auth\.ts/);

    // §27: suggested, and not recorded. Nothing is anchored until somebody
    // signs a `note.created` for it.
    assert.deepEqual((await statuses()).notes, []);
  });
});
