/**
 * `Repository.log` against `git log`, order included.
 *
 * This exists because the walk used to follow first parents only, which is
 * indistinguishable from correct on a linear history and silently drops
 * everything that arrived by a merge on any other. A test that built its own
 * history and checked its own expectations would have agreed with the bug —
 * git's output is the only thing that would not have.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { gitIn, hasGit } from "../testing/Git.ts";
import { stores as nodeStores } from "./Node.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";

describe.skipIf(!hasGit)("log, against git", () => {
  let root: string;

  const git = (...args: string[]) => gitIn(root)(...args);

  /**
   * Each commit is a second later than the last.
   *
   * Without this every commit in a test lands in the same second, and among
   * commits that share a timestamp and are on different branches there is no
   * right order — git falls back to its queue discipline and this falls back
   * to the oid. Distinct dates are also what a real repository has, so this
   * is the case worth pinning; the tie is called out where `log` is written.
   */
  let clock = 1_700_000_000;
  const commit = (message: string, file = "n.txt") => {
    execFileSync("sh", ["-c", `echo ${message} > ${file}`], { cwd: root });
    git("add", file);
    clock += 1;
    execFileSync(
      "git",
      ["-c", "user.name=T", "-c", "user.email=t@e.com", "commit", "-q", "-m", message],
      {
        cwd: root,
        env: {
          ...process.env,
          GIT_AUTHOR_DATE: `${clock} +0000`,
          GIT_COMMITTER_DATE: `${clock} +0000`,
        },
      },
    );
    return git("rev-parse", "HEAD").trim();
  };

  /** A merge commit, dated like the rest so the order is well defined. */
  const merge = (branch: string) => {
    clock += 1;
    execFileSync(
      "git",
      [
        "-c",
        "user.name=T",
        "-c",
        "user.email=t@e.com",
        "merge",
        "-q",
        "--no-ff",
        "-m",
        `merge ${branch}`,
        branch,
      ],
      {
        cwd: root,
        env: {
          ...process.env,
          GIT_AUTHOR_DATE: `${clock} +0000`,
          GIT_COMMITTER_DATE: `${clock} +0000`,
        },
      },
    );
    return git("rev-parse", "HEAD").trim();
  };

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "log-"));
    clock = 1_700_000_000;
    git("init", "-q", "-b", "main", ".");
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const ours = (from: string, options?: { readonly firstParent?: boolean }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commits = yield* Stream.runCollect(repository.log(from as Oid, options));
        return commits.map((entry) => entry.oid);
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(nodeStores(path.join(root, ".git"))),
          ),
        ),
        Effect.orDie,
      ),
    );

  const theirs = (...args: string[]) =>
    git("log", "--format=%H", ...args)
      .split("\n")
      .filter((line) => line !== "");

  /** main and side both move, then main merges side. */
  const forked = () => {
    commit("root");
    commit("main-1");
    git("checkout", "-q", "-b", "side");
    commit("side-1", "s.txt");
    commit("side-2", "s.txt");
    git("checkout", "-q", "main");
    commit("main-2");
    return merge("side");
  };

  it("reports every commit a merge brought in, in git's order", async () => {
    const tip = forked();

    const expected = theirs(tip);
    assert.equal(expected.length, 6, "root, two on main, two on side, and the merge");
    assert.deepEqual(await ours(tip), expected);
  });

  it("flattens to the first parent when asked, as `git log --first-parent` does", async () => {
    const tip = forked();

    const expected = theirs("--first-parent", tip);
    assert.equal(expected.length, 4, "the side branch's own commits are not listed");
    assert.deepEqual(await ours(tip, { firstParent: true }), expected);
  });

  it("keeps git's order across nested merges", async () => {
    commit("root");

    // Two side branches merged at different points, so the walk has to hold
    // more than one pending side at once and still agree with git.
    git("checkout", "-q", "-b", "a");
    commit("a-1", "a.txt");
    git("checkout", "-q", "main");
    git("checkout", "-q", "-b", "b");
    commit("b-1", "b.txt");
    commit("b-2", "b.txt");

    git("checkout", "-q", "main");
    commit("main-1");
    merge("a");
    commit("main-2");
    const tip = merge("b");

    assert.deepEqual(await ours(tip), theirs(tip));
  });

  it("reports a commit reachable by two routes exactly once", async () => {
    const tip = forked();

    const listed = await ours(tip);
    assert.equal(new Set(listed).size, listed.length);
  });
});
