/** Differential cases for Git-shaped inspection and ref commands. */
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

const commit = async (directory: string, message: string) => {
  const committed = await git(directory, ["commit", "--quiet", "-m", message]);
  assert.equal(committed.code, 0);
};

const initialize = async (directory: string) => {
  const initialized = await git(directory, ["init", "--quiet", "--initial-branch=main"]);
  assert.equal(initialized.code, 0);
  await fs.writeFile(path.join(directory, "tracked.txt"), "alpha\nneedle\n");
  const added = await git(directory, ["add", "--", "tracked.txt"]);
  assert.equal(added.code, 0);
  await commit(directory, "initial");

  await fs.writeFile(path.join(directory, "tracked.txt"), "alpha\nNeedle\nsecond\n");
  const updated = await git(directory, ["add", "--", "tracked.txt"]);
  assert.equal(updated.code, 0);
  await commit(directory, "second");
};

const commandCases = [
  { command: "log", args: ["log", "--oneline"] },
  { command: "show", args: ["show", "--stat", "HEAD"] },
  { command: "diff", args: ["diff", "HEAD~1", "HEAD"] },
  { command: "grep", args: ["grep", "-i", "-n", "needle"] },
  { command: "reflog", args: ["reflog", "show", "--date=iso", "HEAD"] },
  { command: "branch", args: ["branch", "topic"] },
  { command: "tag", args: ["tag", "-a", "v1", "-m", "release"] },
] as const;

describe.skipIf(!hasGit)("inspection and ref CLI compatibility", () => {
  it("uses the pinned stock Git baseline", async () => {
    const version = await git(process.cwd(), ["--version"]);
    assert.equal(
      new TextDecoder().decode(version.stdout).trim(),
      `git version ${gitCompatibilityBaseline}`,
    );
  });

  for (const testCase of commandCases) {
    it(`matches git for ${testCase.command}`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), `git-compat-${testCase.command}-`));
      try {
        const stock = path.join(root, "stock");
        const plus = path.join(root, "plus");
        await fs.mkdir(stock);
        await fs.mkdir(plus);
        await Promise.all([initialize(stock), initialize(plus)]);
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
