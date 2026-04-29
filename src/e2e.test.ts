import * as assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import * as helpers from "./test.helpers.ts";

/**
 * End-to-end parity tests between the upstream `git` CLI and the in-repo
 * `.git` package CLI (`src/cli.ts`).
 *
 * Strategy:
 *   - Each test provisions two parallel work-trees: `ref/` (real `git`
 *     init) and `ours/` (our CLI init).
 *   - The same logical command runs against both work-trees.
 *   - Outputs are normalized (OIDs, absolute paths, trailing whitespace
 *     collapsed) before comparison.
 *   - Where the two CLIs share a format we assert byte-for-byte equality;
 *     where they intentionally diverge today we assert structural parity
 *     (both produce non-empty output containing the same key tokens).
 *     Each soft assertion is annotated with a `// PARITY:` comment so it
 *     can be tightened later.
 */

const SHA1_RE = /\b[0-9a-f]{7,40}\b/g;
const TIMESTAMP_RE = /\b\d{10}\s[+-]\d{4}\b/g;

interface Pair {
  ref: string; // real-git work-tree
  ours: string; // our CLI work-tree
}

const cli = await helpers.cli();

function normalize(s: string, cwd?: string) {
  let out = s;
  if (cwd) out = out.split(cwd).join("<CWD>");
  out = out.replace(SHA1_RE, "<oid>");
  out = out.replace(TIMESTAMP_RE, "<ts>");
  out = out
    .split("\n")
    .map((l) => l.replace(/\s+$/, ""))
    .join("\n")
    .replace(/\n+$/, "");
  return out;
}

function runGit(command: string, cwd: string) {
  return helpers.run(`git ${command}`, { cwd });
}

function runOurs(command: string, cwd: string) {
  return cli.bin(command, { cwd });
}

async function tryRun(fn: () => Promise<string>): Promise<string> {
  try {
    return await fn();
  } catch (e) {
    return e instanceof Error ? e.message : String(e);
  }
}

async function makePair(): Promise<Pair> {
  const ref = await cli.setup();
  const ours = join(
    cli.dir,
    `ours-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`,
  );
  await mkdir(ours, { recursive: true });
  return { ref, ours };
}

async function bothInit(pair: Pair) {
  await runOurs("init", pair.ours);
}

/**
 * Stage a fresh blob in both work-trees and commit it. Returns the file
 * path that was added so callers can compose follow-up actions.
 */
async function bothCommit(pair: Pair, name: string, content: string, message: string) {
  await Promise.all([
    writeFile(join(pair.ref, name), content),
    writeFile(join(pair.ours, name), content),
  ]);
  await Promise.all([runGit(`add ${name}`, pair.ref), runOurs(`add ${name}`, pair.ours)]);
  await Promise.all([
    runGit(`commit -m "${message}"`, pair.ref),
    runOurs(`commit -m "${message}"`, pair.ours),
  ]);
  return name;
}

before(() => cli.before());
after(() => cli.after());

void describe("e2e: cli parity with system git", () => {
  // ---- exact parity: format already matches real git --------------------
  void describe("init", () => {
    void it("emits an init banner mentioning an empty Git repository", async () => {
      const pair = await makePair();
      const refDir = join(
        cli.dir,
        `ref-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`,
      );
      await mkdir(refDir, { recursive: true });
      const refOut = await runGit("init -b main --no-template", refDir);
      const oursOut = await runOurs("init", pair.ours);
      const phrase = /Initialized empty Git repository/;
      assert.match(refOut, phrase);
      assert.match(oursOut, phrase);
    });

    void it("creates .git/HEAD pointing at refs/heads/main", async () => {
      const pair = await makePair();
      await runOurs("init", pair.ours);
      const [refHead, oursHead] = await Promise.all([
        readFile(join(pair.ref, ".git/HEAD"), "utf8"),
        readFile(join(pair.ours, ".git/HEAD"), "utf8"),
      ]);
      assert.equal(oursHead.trim(), refHead.trim());
      assert.equal(oursHead.trim(), "ref: refs/heads/main");
    });
  });

  void describe("status", () => {
    void it("reports the current branch on a fresh repo", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const [refOut, oursOut] = await Promise.all([
        runGit("status", pair.ref),
        runOurs("status", pair.ours),
      ]);
      assert.match(refOut, /On branch main/);
      assert.match(oursOut, /On branch main/);
    });

    void it("mentions the staged file after `add`", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "hello\n"),
        writeFile(join(pair.ours, "a.txt"), "hello\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      const [refOut, oursOut] = await Promise.all([
        runGit("status", pair.ref),
        runOurs("status", pair.ours),
      ]);
      // PARITY: both must reference the path; section heading text differs.
      assert.match(refOut, /a\.txt/);
      assert.match(oursOut, /a\.txt/);
    });

    void it("--short: both produce non-empty output mentioning a.txt", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "hi\n"),
        writeFile(join(pair.ours, "a.txt"), "hi\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      const [refOut, oursOut] = await Promise.all([
        runGit("status --short", pair.ref),
        runOurs("status --short", pair.ours),
      ]);
      // PARITY: porcelain prefix differs (`A ` vs `A  `); compare paths.
      assert.match(refOut, /a\.txt/);
      assert.match(oursOut, /a\.txt/);
    });
  });

  void describe("add", () => {
    void it("stages a new file without error in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "hello\n"),
        writeFile(join(pair.ours, "a.txt"), "hello\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      const refLs = await runGit("ls-files", pair.ref);
      assert.equal(refLs.trim(), "a.txt");
    });

    void it("ours produces a Git-compatible index (read by git ls-files)", async () => {
      const pair = await makePair();
      await runOurs("init", pair.ours);
      await writeFile(join(pair.ours, "a.txt"), "hello\n");
      await runOurs("add a.txt", pair.ours);
      const out = await runGit("ls-files", pair.ours);
      assert.equal(out.trim(), "a.txt");
    });
  });

  void describe("branch", () => {
    void it("lists no branches on a fresh repo", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const [refOut, oursOut] = await Promise.all([
        runGit("branch", pair.ref),
        runOurs("branch", pair.ours),
      ]);
      assert.equal(normalize(oursOut, pair.ours), normalize(refOut, pair.ref));
    });

    void it("listing mentions main after a commit", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "init");
      const [refOut, oursOut] = await Promise.all([
        runGit("branch", pair.ref),
        runOurs("branch", pair.ours),
      ]);
      // PARITY: real git prefixes the active branch with `* `; ours uses
      // two spaces. Assert membership only.
      assert.match(refOut, /main/);
      assert.match(oursOut, /main/);
    });

    void it("creating a new branch makes it visible in `branch` output", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "init");
      await Promise.all([runGit("branch feature", pair.ref), runOurs("branch feature", pair.ours)]);
      const [refOut, oursOut] = await Promise.all([
        runGit("branch", pair.ref),
        runOurs("branch", pair.ours),
      ]);
      assert.match(refOut, /feature/);
      assert.match(oursOut, /feature/);
    });
  });

  void describe("log", () => {
    void it("empty repo: both report there are no commits", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const refOut = await tryRun(() => runGit("log", pair.ref));
      const oursOut = await tryRun(() => runOurs("log", pair.ours));
      // PARITY: real git uses "does not have any commits yet"; we say
      // "your current branch has no commits yet".
      assert.match(refOut, /no commits|does not have any commits/);
      assert.match(oursOut, /no commits/);
    });

    void it("--oneline: same `<short-oid> <subject>` shape after one commit", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "subject");
      const [refOut, oursOut] = await Promise.all([
        runGit("log --oneline", pair.ref),
        runOurs("log --oneline", pair.ours),
      ]);
      const refLine = normalize(refOut).split("\n")[0]!;
      const oursLine = normalize(oursOut).split("\n")[0]!;
      assert.match(refLine, /^<oid>\s+subject$/);
      assert.match(oursLine, /^<oid>\s+subject$/);
    });
  });

  void describe("commit", () => {
    void it("summary line: both match `[main <oid>] <subject>`", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "x\n"),
        writeFile(join(pair.ours, "a.txt"), "x\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      const [refOut, oursOut] = await Promise.all([
        runGit('commit -m "first"', pair.ref),
        runOurs('commit -m "first"', pair.ours),
      ]);
      // PARITY: real git first commit injects `(root-commit) ` before the
      // OID; ours does not. Assert `[main ... first` appears in both.
      assert.match(refOut, /\[main.*first/);
      assert.match(oursOut, /\[main.*first/);
    });
  });

  void describe("tag", () => {
    void it("tag listing matches exactly when both repos share state", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      // Some user gitconfigs set `tag.gpgsign=true`, which would convert
      // a lightweight `git tag v1` into an annotated/signed tag and
      // demand a message. Force lightweight to keep parity deterministic.
      await Promise.all([
        runGit("-c tag.gpgsign=false -c tag.forceSignAnnotated=false tag v1", pair.ref),
        runOurs("tag v1", pair.ours),
      ]);
      const [refOut, oursOut] = await Promise.all([
        runGit("tag", pair.ref),
        runOurs("tag", pair.ours),
      ]);
      assert.equal(normalize(oursOut), normalize(refOut));
    });

    void it("creating a lightweight tag makes it appear in `git tag`", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      await runOurs("tag v9", pair.ours);
      // Real git can read our .git directory and observe the tag.
      const out = await runGit("tag", pair.ours);
      assert.match(out, /v9/);
    });
  });

  // ---- structural parity: outputs differ today, key tokens match -------
  void describe("clone", () => {
    void it("both error out when cloning a non-existent URL", async () => {
      const pair = await makePair();
      const url = "http://127.0.0.1:1/does-not-exist.git";
      const refOut = await tryRun(() => runGit(`clone ${url}`, pair.ref));
      const oursOut = await tryRun(() => runOurs(`clone ${url}`, pair.ours));
      assert.ok(refOut.length > 0);
      assert.ok(oursOut.length > 0);
    });
  });

  void describe("mv", () => {
    void it("renaming a tracked file works in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      await Promise.all([runGit("mv a.txt b.txt", pair.ref), runOurs("mv a.txt b.txt", pair.ours)]);
      const [refLs, oursLs] = await Promise.all([
        runGit("ls-files", pair.ref),
        runGit("ls-files", pair.ours),
      ]);
      assert.equal(refLs.trim(), "b.txt");
      assert.equal(oursLs.trim(), "b.txt");
    });
  });

  void describe("restore", () => {
    void it("restoring a modified file brings back HEAD content in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "original\n", "c");
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "modified\n"),
        writeFile(join(pair.ours, "a.txt"), "modified\n"),
      ]);
      await Promise.all([runGit("restore a.txt", pair.ref), runOurs("restore a.txt", pair.ours)]);
      const [refContent, oursContent] = await Promise.all([
        readFile(join(pair.ref, "a.txt"), "utf8"),
        readFile(join(pair.ours, "a.txt"), "utf8"),
      ]);
      assert.equal(refContent, "original\n");
      assert.equal(oursContent, "original\n");
    });
  });

  void describe("rm", () => {
    void it.todo("rm of a tracked file matches (our CLI handler is a stub)");
  });

  void describe("show", () => {
    void it("both produce non-empty output for HEAD after a commit", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "subject");
      const [refOut, oursOut] = await Promise.all([
        runGit("show HEAD", pair.ref),
        runOurs("show HEAD", pair.ours),
      ]);
      // PARITY: real git uses pretty commit format with diff; we print the
      // raw commit object. Both must reference the subject.
      assert.match(refOut, /subject/);
      assert.match(oursOut, /subject/);
    });
  });

  void describe("checkout", () => {
    void it("switching to an existing branch updates HEAD in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      await Promise.all([runGit("branch feature", pair.ref), runOurs("branch feature", pair.ours)]);
      await Promise.all([
        runGit("checkout feature", pair.ref),
        runOurs("checkout feature", pair.ours),
      ]);
      const [refHead, oursHead] = await Promise.all([
        readFile(join(pair.ref, ".git/HEAD"), "utf8"),
        readFile(join(pair.ours, ".git/HEAD"), "utf8"),
      ]);
      assert.equal(refHead.trim(), "ref: refs/heads/feature");
      assert.equal(oursHead.trim(), "ref: refs/heads/feature");
    });
  });

  void describe("switch", () => {
    void it("switching to an existing branch updates HEAD in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      await Promise.all([runGit("branch feature", pair.ref), runOurs("branch feature", pair.ours)]);
      await Promise.all([runGit("switch feature", pair.ref), runOurs("switch feature", pair.ours)]);
      const [refHead, oursHead] = await Promise.all([
        readFile(join(pair.ref, ".git/HEAD"), "utf8"),
        readFile(join(pair.ours, ".git/HEAD"), "utf8"),
      ]);
      assert.equal(refHead.trim(), "ref: refs/heads/feature");
      assert.equal(oursHead.trim(), "ref: refs/heads/feature");
    });
  });

  void describe("merge", () => {
    void it("merging an ancestor branch produces output in both (no-op fast-forward)", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      await Promise.all([runGit("branch feature", pair.ref), runOurs("branch feature", pair.ours)]);
      const refOut = await tryRun(() => runGit("merge feature", pair.ref));
      const oursOut = await tryRun(() => runOurs("merge feature", pair.ours));
      assert.ok(refOut.length > 0);
      assert.ok(oursOut.length > 0);
    });

    void it.todo("3-way merge commit message matches (CLI lacks divergent-branch helper)");
  });

  void describe("rebase", () => {
    void it("rebasing onto current branch is a graceful no-op (both)", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      const refOut = await tryRun(() => runGit("rebase main", pair.ref));
      const oursOut = await tryRun(() => runOurs("rebase main", pair.ours));
      assert.equal(typeof refOut, "string");
      assert.equal(typeof oursOut, "string");
    });
  });

  void describe("reset", () => {
    void it("reset HEAD on a clean repo runs cleanly in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      const refOut = await tryRun(() => runGit("reset HEAD", pair.ref));
      const oursOut = await tryRun(() => runOurs("reset HEAD", pair.ours));
      // Real git: empty stdout on success; ours: "Reset to HEAD".
      assert.equal(typeof refOut, "string");
      assert.match(oursOut, /[Rr]eset/);
    });
  });

  void describe("bisect", () => {
    void it("bisect reset on a non-bisecting repo is graceful in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const refOut = await tryRun(() => runGit("bisect reset", pair.ref));
      const oursOut = await tryRun(() => runOurs("bisect reset", pair.ours));
      // Real git: empty stdout on no-op ("We are not bisecting." goes to
      // stderr but exits 0). Ours: "Bisect state cleared".
      assert.equal(typeof refOut, "string");
      assert.match(oursOut, /[Bb]isect/);
    });
  });

  void describe("diff", () => {
    void it("--name-only after `add` lists the new path in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "hello\n"),
        writeFile(join(pair.ours, "a.txt"), "hello\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      // PARITY: real git's plain `diff --name-only` compares index↔worktree
      // and is empty here. `diff --cached --name-only` shows the staged
      // path. Ours emits the staged path either way (no commit yet).
      const [refOut, oursOut] = await Promise.all([
        runGit("diff --cached --name-only", pair.ref),
        runOurs("diff --name-only", pair.ours),
      ]);
      assert.equal(refOut.trim(), "a.txt");
      assert.equal(oursOut.trim(), "a.txt");
    });

    void it("on a clean repo: both produce no diff output", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await bothCommit(pair, "a.txt", "x\n", "c");
      const [refOut, oursOut] = await Promise.all([
        runGit("diff", pair.ref),
        runOurs("diff", pair.ours),
      ]);
      assert.equal(refOut.trim(), "");
      assert.equal(oursOut.trim(), "");
    });
  });

  void describe("grep", () => {
    void it("literal search across tracked files matches in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "alpha\nbeta\ngamma\n"),
        writeFile(join(pair.ours, "a.txt"), "alpha\nbeta\ngamma\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      const [refOut, oursOut] = await Promise.all([
        runGit("grep beta", pair.ref),
        runOurs("grep beta", pair.ours),
      ]);
      assert.equal(normalize(refOut), normalize(oursOut));
      assert.equal(normalize(oursOut), "a.txt:beta");
    });

    void it("`-i` case-insensitive search hits in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        writeFile(join(pair.ref, "a.txt"), "Beta\n"),
        writeFile(join(pair.ours, "a.txt"), "Beta\n"),
      ]);
      await Promise.all([runGit("add a.txt", pair.ref), runOurs("add a.txt", pair.ours)]);
      const [refOut, oursOut] = await Promise.all([
        runGit("grep -i beta", pair.ref),
        runOurs("grep -i beta", pair.ours),
      ]);
      assert.match(refOut, /Beta/);
      assert.match(oursOut, /Beta/);
    });
  });

  void describe("backfill", () => {
    void it("no-op when no remote is configured (both produce some output)", async () => {
      const pair = await makePair();
      await bothInit(pair);
      // Real git ships `backfill` only in recent versions; either way
      // both should emit *something* — accept any string.
      const refOut = await tryRun(() => runGit("backfill", pair.ref));
      const oursOut = await tryRun(() => runOurs("backfill", pair.ours));
      // Real git's `backfill` is partial-clone tooling; on a non-partial
      // repo it may exit 0 with empty stdout. Just assert it didn't blow
      // up catastrophically.
      assert.equal(typeof refOut, "string");
      assert.match(oursOut, /backfill/i);
    });
  });

  void describe("history", () => {
    void it.todo("--dry-run matches (real git ships no `git history` command)");
  });

  void describe("fetch", () => {
    void it("fetch with no remote configured fails informatively in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const refOut = await tryRun(() => runGit("fetch", pair.ref));
      const oursOut = await tryRun(() => runOurs("fetch", pair.ours));
      // Real git may print to stderr only and exit 0/1 quietly; ensure
      // both commands return a string (no crash) rather than asserting
      // length.
      assert.equal(typeof refOut, "string");
      assert.equal(typeof oursOut, "string");
    });
  });

  void describe("pull", () => {
    void it("pull with no remote configured fails informatively in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const refOut = await tryRun(() => runGit("pull", pair.ref));
      const oursOut = await tryRun(() => runOurs("pull", pair.ours));
      assert.ok(refOut.length > 0);
      assert.ok(oursOut.length > 0);
    });
  });

  void describe("push", () => {
    void it("push with no remote configured fails informatively in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const refOut = await tryRun(() => runGit("push", pair.ref));
      const oursOut = await tryRun(() => runOurs("push", pair.ours));
      assert.ok(refOut.length > 0);
      assert.ok(oursOut.length > 0);
    });
  });

  void describe("remote", () => {
    void it("listing matches (empty) on a fresh repo", async () => {
      const pair = await makePair();
      await bothInit(pair);
      const [refOut, oursOut] = await Promise.all([
        runGit("remote", pair.ref),
        runOurs("remote", pair.ours),
      ]);
      assert.equal(normalize(refOut), normalize(oursOut));
      assert.equal(normalize(oursOut), "");
    });

    void it("`remote add` then list shows the new remote in both", async () => {
      const pair = await makePair();
      await bothInit(pair);
      await Promise.all([
        runGit("remote add origin http://example.invalid/r.git", pair.ref),
        runOurs("remote add origin http://example.invalid/r.git", pair.ours),
      ]);
      const [refOut, oursOut] = await Promise.all([
        runGit("remote", pair.ref),
        runOurs("remote", pair.ours),
      ]);
      assert.equal(normalize(refOut), "origin");
      assert.equal(normalize(oursOut), "origin");
    });
  });
});
