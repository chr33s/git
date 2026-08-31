/** Differential cases for Git-shaped work-tree commands. */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { gitCompatibilityBaseline } from "./GitCompat.ts";
import { hasGit } from "../testing/Git.ts";
import { runProcess, sameProcessResult } from "../testing/Process.ts";

const source = path.resolve("src", "cli", "main.ts");

const environment: NodeJS.ProcessEnv = {
  ...process.env,
  GIT_AUTHOR_DATE: "1700000000 +0000",
  GIT_AUTHOR_EMAIL: "compat@example.invalid",
  GIT_AUTHOR_NAME: "Compat",
  GIT_COMMITTER_DATE: "1700000000 +0000",
  GIT_COMMITTER_EMAIL: "compat@example.invalid",
  GIT_COMMITTER_NAME: "Compat",
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_SYSTEM: "/dev/null",
  GIT_PAGER: "cat",
  LANG: "C",
  LC_ALL: "C",
  NO_COLOR: "1",
  // Pinned together, because node refuses to honour one while the other is
  // set: inheriting a `FORCE_COLOR` from the surrounding shell made every
  // `git+` process print "the 'NO_COLOR' env is ignored…" to stderr, and these
  // suites compare stderr byte for byte — so a developer whose terminal or CI
  // sets it saw thirty-four failures that had nothing to do with the code.
  FORCE_COLOR: "0",
  PAGER: "cat",
  TERM: "dumb",
  TZ: "UTC",
};

const git = (cwd: string, args: ReadonlyArray<string>) =>
  runProcess({ command: "git", args, cwd, env: environment });

const gitPlus = (cwd: string, args: ReadonlyArray<string>) =>
  runProcess({ command: process.execPath, args: [source, ...args], cwd, env: environment });

interface Snapshot {
  readonly head: Awaited<ReturnType<typeof git>>;
  readonly index: Awaited<ReturnType<typeof git>>;
  readonly refs: Awaited<ReturnType<typeof git>>;
  readonly status: Awaited<ReturnType<typeof git>>;
}

const snapshot = async (cwd: string): Promise<Snapshot> => ({
  head: await git(cwd, ["rev-parse", "--verify", "HEAD"]),
  index: await git(cwd, ["ls-files", "--stage", "-z"]),
  refs: await git(cwd, ["show-ref", "--head"]),
  status: await git(cwd, ["status", "--porcelain=v1", "-z"]),
});

const sameSnapshot = (left: Snapshot, right: Snapshot) =>
  sameProcessResult(left.head, right.head) &&
  sameProcessResult(left.index, right.index) &&
  sameProcessResult(left.refs, right.refs) &&
  sameProcessResult(left.status, right.status);

const initialize = async (directory: string) => {
  const initialized = await git(directory, ["init", "--quiet", "--initial-branch=main"]);
  assert.equal(initialized.code, 0);
  await fs.writeFile(path.join(directory, "tracked.txt"), "one\n");
  const added = await git(directory, ["add", "--", "tracked.txt"]);
  assert.equal(added.code, 0);
  const committed = await git(directory, ["commit", "--quiet", "-m", "initial"]);
  assert.equal(committed.code, 0);
};

const prepare = async (directory: string, command: string) => {
  await initialize(directory);
  if (command === "add" || command === "commit") {
    await fs.writeFile(path.join(directory, "new.txt"), "new\n");
  }
  if (command === "status" || command === "restore") {
    await fs.writeFile(path.join(directory, "tracked.txt"), "changed\n");
  }
  if (command === "reset") {
    await fs.writeFile(path.join(directory, "tracked.txt"), "second\n");
    const added = await git(directory, ["add", "--", "tracked.txt"]);
    assert.equal(added.code, 0);
    const committed = await git(directory, ["commit", "--quiet", "-m", "second"]);
    assert.equal(committed.code, 0);
  }
};

const commandCases = [
  { command: "add", args: ["add", "--", "new.txt"] },
  { command: "status", args: ["status", "--porcelain=v1", "-z"] },
  { command: "commit", args: ["commit", "-m", "new"] },
  { command: "rm", args: ["rm", "--", "tracked.txt"] },
  { command: "mv", args: ["mv", "tracked.txt", "moved.txt"] },
  { command: "restore", args: ["restore", "--", "tracked.txt"] },
  { command: "reset", args: ["reset", "--hard", "HEAD~1"] },
  { command: "switch", args: ["switch", "-c", "topic"] },
] as const;

describe.skipIf(!hasGit)("work-tree CLI compatibility", () => {
  it("uses the pinned stock Git baseline", async () => {
    const version = await git(process.cwd(), ["--version"]);
    assert.equal(
      new TextDecoder().decode(version.stdout).trim(),
      `git version ${gitCompatibilityBaseline}`,
    );
  });

  it("matches quiet init state", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-compat-init-"));
    try {
      const stock = path.join(root, "stock");
      const plus = path.join(root, "plus");
      await fs.mkdir(stock);
      await fs.mkdir(plus);
      const args = ["init", "--quiet", "--initial-branch=main"];
      const [fromGit, fromGitPlus] = await Promise.all([git(stock, args), gitPlus(plus, args)]);
      assert.equal(sameProcessResult(fromGit, fromGitPlus), true);
      assert.equal(sameSnapshot(await snapshot(stock), await snapshot(plus)), true);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  for (const testCase of commandCases) {
    it(`matches git for ${testCase.command}`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), `git-compat-${testCase.command}-`));
      try {
        const stock = path.join(root, "stock");
        const plus = path.join(root, "plus");
        await fs.mkdir(stock);
        await fs.mkdir(plus);
        await Promise.all([prepare(stock, testCase.command), prepare(plus, testCase.command)]);
        const [fromGit, fromGitPlus] = await Promise.all([
          git(stock, testCase.args),
          gitPlus(plus, testCase.args),
        ]);
        assert.equal(sameProcessResult(fromGit, fromGitPlus), true);
        assert.equal(sameSnapshot(await snapshot(stock), await snapshot(plus)), true);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
