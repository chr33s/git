/**
 * Path history, against `git log -- <path>`.
 *
 * The interesting cases are all merges: a filter that merely asked "did this
 * commit touch the path" agrees with git on a straight line and diverges the
 * moment history forks, so the linear test proves almost nothing on its own
 * and the merge tests are the ones that matter.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { gitIn, hasGit } from "../testing/Git.ts";
import { forPath } from "./History.ts";
import { stores as nodeStores } from "./Node.ts";
import * as GitRepository from "./Repository.ts";
import type { Oid } from "./Store.ts";

describe.skipIf(!hasGit)("path history", () => {
  let root: string;

  const git = (...args: string[]) => gitIn(root)(...args);

  const write = (file: string, content: string) =>
    execFileSync(
      "sh",
      ["-c", `mkdir -p "$(dirname ${file})" && printf %s '${content}' > ${file}`],
      {
        cwd: root,
      },
    );

  const commit = (message: string) => {
    git("add", "-A");
    git("commit", "-q", "-m", message);
    return git("rev-parse", "HEAD").trim();
  };

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "history-"));
    git("init", "-q", "-b", "main", ".");
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  /** Ours, as a list of oids newest-first. */
  const ours = (from: string, file: string) =>
    Effect.runPromise(
      Stream.runCollect(forPath(from as Oid, file)).pipe(
        Effect.map((changes) => changes.map((change) => change.oid)),
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(nodeStores(path.join(root, ".git"))),
          ),
        ),
        Effect.orDie,
      ),
    );

  /** git's, the same way. */
  const theirs = (from: string, file: string) =>
    git("log", "--format=%H", from, "--", file)
      .split("\n")
      .filter((line) => line !== "");

  it("reports only the commits that changed the path", async () => {
    write("a.txt", "1");
    write("b.txt", "1");
    commit("both");

    write("a.txt", "2");
    commit("a again");

    write("b.txt", "2");
    commit("b again");

    write("a.txt", "3");
    const tip = commit("a once more");

    assert.deepEqual(await ours(tip, "a.txt"), theirs(tip, "a.txt"));
    assert.deepEqual(await ours(tip, "b.txt"), theirs(tip, "b.txt"));
    // Three of the four commits touched a.txt, so this is not vacuous.
    assert.equal(theirs(tip, "a.txt").length, 3);
  });

  it("simplifies a merge whose sides did not both touch the path", async () => {
    write("shared.txt", "1");
    write("side-only.txt", "1");
    commit("root");

    git("checkout", "-q", "-b", "side");
    write("side-only.txt", "2");
    commit("side changes its own file");

    git("checkout", "-q", "main");
    write("shared.txt", "2");
    commit("main changes shared");

    git("merge", "-q", "--no-ff", "-m", "merge side", "side");
    const tip = git("rev-parse", "HEAD").trim();

    // The merge is treesame to one parent for each path, so git reports it
    // for neither — this is the case a naive "touched it?" filter gets wrong.
    assert.deepEqual(await ours(tip, "shared.txt"), theirs(tip, "shared.txt"));
    assert.deepEqual(await ours(tip, "side-only.txt"), theirs(tip, "side-only.txt"));
    assert.ok(!theirs(tip, "shared.txt").includes(tip), "the merge itself is simplified away");
  });

  it("reports a merge that resolved the path to something neither side had", async () => {
    write("c.txt", "base");
    commit("root");

    git("checkout", "-q", "-b", "side");
    write("c.txt", "side");
    commit("side edits c");

    git("checkout", "-q", "main");
    write("c.txt", "main");
    commit("main edits c");

    // Conflict, resolved to a third value: the merge is treesame to neither
    // parent, so it is a real change to the path and is reported.
    try {
      git("merge", "--no-ff", "-m", "merge side", "side");
    } catch {
      // expected: the merge conflicts
    }
    write("c.txt", "resolved");
    git("add", "c.txt");
    git("commit", "-q", "--no-edit");
    const tip = git("rev-parse", "HEAD").trim();

    assert.deepEqual(await ours(tip, "c.txt"), theirs(tip, "c.txt"));
    assert.ok(theirs(tip, "c.txt").includes(tip), "a real merge resolution is reported");
  });

  it("handles a path added, deleted and re-added", async () => {
    write("x.txt", "1");
    commit("add x");

    execFileSync("sh", ["-c", "rm x.txt"], { cwd: root });
    commit("delete x");

    write("x.txt", "2");
    const tip = commit("re-add x");

    assert.deepEqual(await ours(tip, "x.txt"), theirs(tip, "x.txt"));
    assert.equal(theirs(tip, "x.txt").length, 3);
  });

  it("finds a path inside a directory", async () => {
    write("src/deep/file.ts", "1");
    commit("add nested");

    write("other.txt", "1");
    commit("unrelated");

    write("src/deep/file.ts", "2");
    const tip = commit("edit nested");

    assert.deepEqual(await ours(tip, "src/deep/file.ts"), theirs(tip, "src/deep/file.ts"));
    assert.equal(theirs(tip, "src/deep/file.ts").length, 2);
  });
});
