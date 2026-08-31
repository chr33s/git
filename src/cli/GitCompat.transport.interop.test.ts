/** Differential cases for Git-shaped remote and transport commands. */
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
  readonly config: Awaited<ReturnType<typeof git>>;
  readonly head: Awaited<ReturnType<typeof git>>;
  readonly index: Awaited<ReturnType<typeof git>>;
  readonly refs: Awaited<ReturnType<typeof git>>;
  readonly status: Awaited<ReturnType<typeof git>>;
}

const snapshot = async (cwd: string): Promise<Snapshot> => ({
  config: await git(cwd, ["config", "--local", "--null", "--list"]),
  head: await git(cwd, ["rev-parse", "--verify", "HEAD"]),
  index: await git(cwd, ["ls-files", "--stage", "-z"]),
  refs: await git(cwd, ["show-ref", "--head"]),
  status: await git(cwd, ["status", "--porcelain=v1", "-z"]),
});

const sameSnapshot = (left: Snapshot, right: Snapshot) =>
  sameProcessResult(left.config, right.config) &&
  sameProcessResult(left.head, right.head) &&
  sameProcessResult(left.index, right.index) &&
  sameProcessResult(left.refs, right.refs) &&
  sameProcessResult(left.status, right.status);

const successful = async (cwd: string, args: ReadonlyArray<string>) => {
  const result = await git(cwd, args);
  assert.equal(result.code, 0, new TextDecoder().decode(result.stderr));
};

const commit = async (cwd: string, message: string) => {
  await successful(cwd, ["commit", "--quiet", "-m", message]);
};

/** A bare remote and a local clone whose remote URL stays relative and reproducible. */
const initializeTransport = async (root: string) => {
  await fs.mkdir(root, { recursive: true });
  const remote = path.join(root, "remote");
  const seed = path.join(root, "seed");
  const local = path.join(root, "local");
  await Promise.all([fs.mkdir(remote), fs.mkdir(seed), fs.mkdir(local)]);
  await successful(remote, ["init", "--bare", "--quiet", "--initial-branch=main"]);
  await successful(seed, ["init", "--quiet", "--initial-branch=main"]);
  await fs.writeFile(path.join(seed, "base.txt"), "base\n");
  await successful(seed, ["add", "--", "base.txt"]);
  await commit(seed, "base");
  await successful(seed, ["remote", "add", "origin", "../remote"]);
  await successful(seed, ["push", "--quiet", "-u", "origin", "main"]);
  await successful(local, ["clone", "--quiet", "../remote", "."]);
  // Git canonicalizes a local clone URL to an absolute path. Keep the command
  // fixture's subsequent transport diagnostics independent of its sandbox root.
  await successful(local, ["remote", "set-url", "origin", "../remote"]);
  return { local, remote, seed };
};

const advance = async (seed: string) => {
  await fs.writeFile(path.join(seed, "remote.txt"), "advanced\n");
  await successful(seed, ["add", "--", "remote.txt"]);
  await commit(seed, "advance");
  await successful(seed, ["push", "--quiet", "origin", "main"]);
};

const remoteCases = [
  { command: "remote add", args: ["remote", "add", "origin", "../remote"], configured: false },
  { command: "remote -v", args: ["remote", "-v"], configured: true },
  { command: "remote get-url", args: ["remote", "get-url", "origin"], configured: true },
  {
    command: "remote set-url",
    args: ["remote", "set-url", "origin", "../other"],
    configured: true,
  },
  { command: "remote rename", args: ["remote", "rename", "origin", "upstream"], configured: true },
  { command: "remote remove", args: ["remote", "remove", "origin"], configured: true },
] as const;

describe.skipIf(!hasGit)("remote and transport CLI compatibility", () => {
  it("uses the pinned stock Git baseline", async () => {
    const version = await git(process.cwd(), ["--version"]);
    assert.equal(
      new TextDecoder().decode(version.stdout).trim(),
      `git version ${gitCompatibilityBaseline}`,
    );
  });

  for (const testCase of remoteCases) {
    it(`matches git for ${testCase.command}`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-compat-remote-"));
      try {
        const stock = path.join(root, "stock");
        const plus = path.join(root, "plus");
        await Promise.all([fs.mkdir(stock), fs.mkdir(plus)]);
        await Promise.all([
          successful(stock, ["init", "--quiet", "--initial-branch=main"]),
          successful(plus, ["init", "--quiet", "--initial-branch=main"]),
        ]);
        if (testCase.configured) {
          await Promise.all([
            successful(stock, ["remote", "add", "origin", "../remote"]),
            successful(plus, ["remote", "add", "origin", "../remote"]),
          ]);
        }
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

  it("matches git for a local clone", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-compat-clone-"));
    try {
      const stock = path.join(root, "stock");
      const plus = path.join(root, "plus");
      await Promise.all([fs.mkdir(stock), fs.mkdir(plus)]);
      for (const directory of [stock, plus]) {
        const remote = path.join(directory, "remote");
        const seed = path.join(directory, "seed");
        await Promise.all([fs.mkdir(remote), fs.mkdir(seed)]);
        await successful(remote, ["init", "--bare", "--quiet", "--initial-branch=main"]);
        await successful(seed, ["init", "--quiet", "--initial-branch=main"]);
        await fs.writeFile(path.join(seed, "base.txt"), "base\n");
        await successful(seed, ["add", "--", "base.txt"]);
        await commit(seed, "base");
        await successful(seed, ["remote", "add", "origin", "../remote"]);
        await successful(seed, ["push", "--quiet", "-u", "origin", "main"]);
        await fs.mkdir(path.join(directory, "clone"));
      }
      const [fromGit, fromGitPlus] = await Promise.all([
        git(path.join(stock, "clone"), ["clone", "../remote", "."]),
        gitPlus(path.join(plus, "clone"), ["clone", "../remote", "."]),
      ]);
      assert.equal(sameProcessResult(fromGit, fromGitPlus), true);
      const stockSnapshot = await snapshot(path.join(stock, "clone"));
      const plusSnapshot = await snapshot(path.join(plus, "clone"));
      const normalizedConfig = (value: Uint8Array) =>
        new TextDecoder()
          .decode(value)
          .replace(/\/(?:stock|plus)\/clone\/\.\.\/remote/g, "/<fixture>/remote");
      assert.equal(
        normalizedConfig(stockSnapshot.config.stdout),
        normalizedConfig(plusSnapshot.config.stdout),
      );
      assert.equal(
        sameProcessResult(stockSnapshot.head, plusSnapshot.head) &&
          sameProcessResult(stockSnapshot.index, plusSnapshot.index) &&
          sameProcessResult(stockSnapshot.refs, plusSnapshot.refs) &&
          sameProcessResult(stockSnapshot.status, plusSnapshot.status),
        true,
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  for (const command of ["fetch", "pull", "push"] as const) {
    it(`matches git for ${command}`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), `git-compat-${command}-`));
      try {
        const stock = await initializeTransport(path.join(root, "stock"));
        const plus = await initializeTransport(path.join(root, "plus"));
        if (command === "push") {
          for (const local of [stock.local, plus.local]) {
            await fs.writeFile(path.join(local, "local.txt"), "local\n");
            await successful(local, ["add", "--", "local.txt"]);
            await commit(local, "local");
          }
        } else {
          await Promise.all([advance(stock.seed), advance(plus.seed)]);
        }
        const args =
          command === "fetch"
            ? ["fetch"]
            : command === "pull"
              ? ["pull", "--ff-only"]
              : ["push", "origin", "main"];
        const [fromGit, fromGitPlus] = await Promise.all([
          git(stock.local, args),
          gitPlus(plus.local, args),
        ]);
        assert.equal(sameProcessResult(fromGit, fromGitPlus), true);
        assert.equal(sameSnapshot(await snapshot(stock.local), await snapshot(plus.local)), true);
        assert.equal(
          sameProcessResult(
            await git(stock.remote, ["show-ref", "--head"]),
            await git(plus.remote, ["show-ref", "--head"]),
          ),
          true,
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
