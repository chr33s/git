/** Differential cases for Git-shaped maintenance commands. */
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
  readonly bisect: Awaited<ReturnType<typeof git>>;
  readonly head: Awaited<ReturnType<typeof git>>;
  readonly index: Awaited<ReturnType<typeof git>>;
  readonly refs: Awaited<ReturnType<typeof git>>;
  readonly status: Awaited<ReturnType<typeof git>>;
  readonly fsck: Awaited<ReturnType<typeof git>>;
}

const snapshot = async (cwd: string): Promise<Snapshot> => ({
  bisect: await git(cwd, ["bisect", "log"]),
  head: await git(cwd, ["rev-parse", "--verify", "HEAD"]),
  index: await git(cwd, ["ls-files", "--stage", "-z"]),
  refs: await git(cwd, ["show-ref", "--head"]),
  status: await git(cwd, ["status", "--porcelain=v1", "-z"]),
  fsck: await git(cwd, ["fsck"]),
});

const sameSnapshot = (left: Snapshot, right: Snapshot) =>
  sameProcessResult(left.bisect, right.bisect) &&
  sameProcessResult(left.head, right.head) &&
  sameProcessResult(left.index, right.index) &&
  sameProcessResult(left.refs, right.refs) &&
  sameProcessResult(left.status, right.status) &&
  sameProcessResult(left.fsck, right.fsck);

const successful = async (cwd: string, args: ReadonlyArray<string>) => {
  const result = await git(cwd, args);
  assert.equal(result.code, 0, new TextDecoder().decode(result.stderr));
};

const commit = async (directory: string, message: string) => {
  await successful(directory, ["commit", "--quiet", "-m", message]);
};

const initialize = async (directory: string) => {
  await successful(directory, ["init", "--quiet", "--initial-branch=main"]);
  for (const [name, contents] of [
    ["one.txt", "one\n"],
    ["two.txt", "two\n"],
    ["three.txt", "three\n"],
  ] as const) {
    await fs.writeFile(path.join(directory, name), contents);
    await successful(directory, ["add", "--", name]);
    await commit(directory, name);
  }
};

const commandCases = [
  { command: "archive", args: ["archive", "--format=tar", "HEAD"] },
  { command: "bisect", args: ["bisect", "start", "HEAD", "HEAD~2"] },
  { command: "fsck", args: ["fsck", "--full", "--no-reflogs"] },
  { command: "gc", args: ["gc", "--prune=now"] },
] as const;

describe.skipIf(!hasGit)("maintenance CLI compatibility", () => {
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
        await Promise.all([fs.mkdir(stock), fs.mkdir(plus)]);
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
