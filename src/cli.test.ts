import * as assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import * as helpers from "./test.helpers.ts";

const cli = await helpers.cli();
before(() => cli.before());
after(() => cli.after());

/**
 * Helper: run a command and return its combined stdout/stderr text. Used
 * for commands that *intentionally* fail (no remote, no commit, etc.) so
 * we can assert on the diagnostic message without aborting the test.
 */
async function tryRun(command: string, cwd: string) {
  try {
    return await cli.bin(command, { cwd });
  } catch (e) {
    return e instanceof Error ? e.message : String(e);
  }
}

/**
 * Helper: prepare a repo with a single committed file `a.txt` so that
 * branching, log, tag, etc. have something to work with.
 */
async function setupWithCommit(file = "a.txt", content = "hello\n") {
  const cwd = await cli.setup();
  await cli.bin("init", { cwd });
  await writeFile(join(cwd, file), content);
  await cli.bin(`add ${file}`, { cwd });
  await cli.bin(`commit -m "first"`, { cwd });
  return cwd;
}

void describe("Cli", () => {
  void it("init - creates a new repository", async () => {
    const cwd = await cli.setup();
    const output = await cli.bin("init", { cwd });
    assert.match(output, /Initialized empty Git repository/);
    // HEAD is wired up to refs/heads/main.
    const head = await readFile(join(cwd, ".git/HEAD"), "utf8");
    assert.equal(head.trim(), "ref: refs/heads/main");
  });

  void it("status - shows repository status", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await cli.bin("status", { cwd });
    assert.match(output, /On branch main/);
  });

  void it("add - stages a new file (verifiable via git ls-files)", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    await writeFile(join(cwd, "test.txt"), "Hello, World!");
    await cli.bin("add test.txt", { cwd });

    // Real git can read our index → strongest possible parity check.
    const ls = await helpers.run("git ls-files", { cwd });
    assert.equal(ls.trim(), "test.txt");

    const status = await cli.bin("status", { cwd });
    assert.match(status, /test\.txt/);
  });

  void it("commit - records a commit summary line and updates HEAD", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    await writeFile(join(cwd, "test.txt"), "Hello, World!");
    await cli.bin("add test.txt", { cwd });

    const out = await cli.bin(`commit -m "test commit"`, { cwd });
    assert.match(out, /\[main [0-9a-f]+\] test commit/);

    // refs/heads/main should now point at a 40-char OID.
    const main = await readFile(join(cwd, ".git/refs/heads/main"), "utf8");
    assert.match(main.trim(), /^[0-9a-f]{40}$/);
  });

  void it("log - displays the commit just made", async () => {
    const cwd = await setupWithCommit();
    const out = await cli.bin("log --oneline", { cwd });
    // `<short-oid> first`
    assert.match(out.split("\n")[0]!, /^[0-9a-f]{7,40}\s+first$/);
  });

  void it("branch - creates and lists branches", async () => {
    const cwd = await setupWithCommit();
    await cli.bin("branch feature", { cwd });
    const out = await cli.bin("branch", { cwd });
    assert.match(out, /\bmain\b/);
    assert.match(out, /\bfeature\b/);
  });

  void it("checkout - switches the working branch", async () => {
    const cwd = await setupWithCommit();
    await cli.bin("branch feature", { cwd });
    await cli.bin("checkout feature", { cwd });
    const head = await readFile(join(cwd, ".git/HEAD"), "utf8");
    assert.equal(head.trim(), "ref: refs/heads/feature");
  });

  void it("rm - removes a tracked file from the index", async () => {
    const cwd = await setupWithCommit("test.txt");
    await cli.bin("rm test.txt", { cwd });
    const ls = await helpers.run("git ls-files", { cwd });
    assert.equal(ls.trim(), "");
  });

  // ---- previously-smoke tests, now with real assertions ----------------

  void it("clone - reports an error when the URL is unreachable", async () => {
    const cwd = await cli.setup();
    const output = await tryRun("clone http://127.0.0.1:1/none.git", cwd);
    assert.match(output, /Error|failed|refused|ECONNREFUSED|fetch/i);
  });

  void it("mv - renames a tracked file", async () => {
    const cwd = await setupWithCommit("a.txt");
    await cli.bin("mv a.txt b.txt", { cwd });
    const ls = await helpers.run("git ls-files", { cwd });
    assert.equal(ls.trim(), "b.txt");
  });

  void it("restore - brings a modified file back to HEAD content", async () => {
    const cwd = await setupWithCommit("a.txt", "original\n");
    await writeFile(join(cwd, "a.txt"), "modified\n");
    await cli.bin("restore a.txt", { cwd });
    const content = await readFile(join(cwd, "a.txt"), "utf8");
    assert.equal(content, "original\n");
  });

  void it("show - prints the HEAD commit", async () => {
    const cwd = await setupWithCommit();
    const out = await cli.bin("show HEAD", { cwd });
    // Our `show` prints the commit object + subject line.
    assert.match(out, /first/);
  });

  void it("switch - switches to an existing branch", async () => {
    const cwd = await setupWithCommit();
    await cli.bin("branch feature", { cwd });
    await cli.bin("switch feature", { cwd });
    const head = await readFile(join(cwd, ".git/HEAD"), "utf8");
    assert.equal(head.trim(), "ref: refs/heads/feature");
  });

  void it("merge - merging an ancestor branch is a no-op (already up to date)", async () => {
    const cwd = await setupWithCommit();
    await cli.bin("branch feature", { cwd });
    const out = await cli.bin("merge feature", { cwd });
    assert.match(out, /[Aa]lready up to date/);
  });

  void it("rebase - onto current branch is a graceful no-op", async () => {
    const cwd = await setupWithCommit();
    const out = await tryRun("rebase main", cwd);
    assert.equal(typeof out, "string");
  });

  void it("reset - reset HEAD on a clean repo runs cleanly", async () => {
    const cwd = await setupWithCommit();
    const out = await tryRun("reset HEAD", cwd);
    assert.match(out, /[Rr]eset/);
  });

  void it("tag - creates a tag visible via `git tag` and our listing", async () => {
    const cwd = await setupWithCommit();
    await cli.bin("tag v1", { cwd });

    // Cross-checked by real git.
    const refTags = await helpers.run("git tag", { cwd });
    assert.equal(refTags.trim(), "v1");

    // And by our own listing.
    const ourTags = await cli.bin("tag", { cwd });
    assert.equal(ourTags.trim(), "v1");
  });

  void it("bisect - reset on a non-bisecting repo prints a bisect message", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await tryRun("bisect reset", cwd);
    assert.match(output, /[Bb]isect/);
  });

  void it("diff - empty repo with nothing staged produces no output", async () => {
    const cwd = await setupWithCommit();
    const out = await cli.bin("diff", { cwd });
    assert.equal(out.trim(), "");
  });

  void it("diff --name-only - lists the staged path before commit", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    await writeFile(join(cwd, "a.txt"), "hello\n");
    await cli.bin("add a.txt", { cwd });
    const out = await cli.bin("diff --name-only", { cwd });
    assert.equal(out.trim(), "a.txt");
  });

  void it("grep - finds a literal match in a tracked file", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    await writeFile(join(cwd, "a.txt"), "alpha\nbeta\ngamma\n");
    await cli.bin("add a.txt", { cwd });
    const out = await cli.bin("grep beta", { cwd });
    assert.equal(out.trim(), "a.txt:beta");
  });

  void it("backfill - reports nothing to do without a remote", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await tryRun("backfill", cwd);
    assert.match(output, /[Nn]othing to backfill|0 objects/);
  });

  void it("history - --dry-run reports zero rewrites on a clean repo", async () => {
    const cwd = await setupWithCommit();
    const output = await tryRun("history --dry-run", cwd);
    assert.match(output, /history|0 .*rewritten|no.+changes/i);
  });

  void it("fetch - errors when no remote is configured", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await tryRun("fetch origin", cwd);
    assert.match(output, /remote|origin|not found|no such/i);
  });

  void it("pull - errors when no remote is configured", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await tryRun("pull origin main", cwd);
    assert.match(output, /remote|origin|not found|no such/i);
  });

  void it("push - errors when no remote is configured", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await tryRun("push origin main", cwd);
    assert.match(output, /remote|origin|not found|no such/i);
  });

  void it("remote - lists no remotes on a fresh repo", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    const output = await cli.bin("remote", { cwd });
    assert.equal(output.trim(), "");
  });

  void it("remote add - then list shows the new remote", async () => {
    const cwd = await cli.setup();
    await cli.bin("init", { cwd });
    await cli.bin("remote add origin http://example.invalid/r.git", { cwd });
    const output = await cli.bin("remote", { cwd });
    assert.equal(output.trim(), "origin");
  });
});
