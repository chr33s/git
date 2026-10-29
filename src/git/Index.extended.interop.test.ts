/**
 * Version 3 index entries in a real checkout: `git add -N` and sparse
 * checkout's skip-worktree bit, read the way git reads them.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { gitIn, hasGit } from "../testing/Git.ts";
import * as Checkout from "./Checkout.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { workspace } from "./Work.node.ts";

const layerFor = (root: string) =>
  Repository.layer.pipe(
    Layer.provide(Repository.hooksNoop),
    Layer.provide(stores(path.join(root, ".git"))),
    Layer.provideMerge(workspace(root)),
  );

describe.skipIf(!hasGit)("extended index entries against git", () => {
  let root: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "index-v3-"));
    const git = gitIn(root);
    git("init", "-q", "-b", "main");
    await fs.writeFile(path.join(root, "kept.txt"), "kept\n");
    await fs.writeFile(path.join(root, "sparse.txt"), "sparse\n");
    git("add", ".");
    git("commit", "-qm", "initial");

    // Planned but not staged, and outside the sparse cone: edited on disk,
    // then removed, neither of which git counts as a change.
    await fs.writeFile(path.join(root, "planned.txt"), "planned\n");
    git("add", "-N", "planned.txt");
    git("update-index", "--skip-worktree", "sparse.txt");
    await fs.rm(path.join(root, "sparse.txt"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("reports status as git does", () =>
    Effect.gen(function* () {
      const status = yield* Checkout.status().pipe(Effect.provide(layerFor(root)));
      assert.deepEqual(status.staged, []);
      assert.deepEqual(status.unstaged, [{ path: "planned.txt", change: "added" }]);
      assert.deepEqual(status.untracked, []);
      assert.equal(gitIn(root)("status", "--porcelain"), " A planned.txt\n");
    }),
  );

  it.effect("leaves sparse paths out of add, and keeps both flags through it", () =>
    Effect.gen(function* () {
      yield* Checkout.add(["."]).pipe(Effect.provide(layerFor(root)));
      // `planned.txt` is now staged for real; `sparse.txt` is still indexed,
      // still skip-worktree, and not staged as a deletion.
      assert.equal(gitIn(root)("ls-files", "-t", "-v", "sparse.txt"), "S sparse.txt\n");
      assert.equal(gitIn(root)("diff", "--cached", "--name-status"), "A\tplanned.txt\n");
    }),
  );

  it.effect("commits without the intent-to-add entry, as git does", () =>
    Effect.gen(function* () {
      const nothing = yield* Checkout.commit({
        message: "only intent\n",
        author: { name: "A", email: "a@example.com", at: new Date(0), offset: 0 },
      }).pipe(Effect.provide(layerFor(root)), Effect.flip);
      assert.equal(nothing._tag, "Invalid");

      yield* Effect.promise(() => fs.writeFile(path.join(root, "kept.txt"), "changed\n"));
      gitIn(root)("add", "kept.txt");
      yield* Checkout.commit({
        message: "with intent beside it\n",
        author: { name: "A", email: "a@example.com", at: new Date(0), offset: 0 },
      }).pipe(Effect.provide(layerFor(root)));
      assert.equal(gitIn(root)("ls-tree", "--name-only", "HEAD"), "kept.txt\nsparse.txt\n");
      assert.equal(gitIn(root)("ls-files", "-t", "-v", "sparse.txt"), "S sparse.txt\n");
      assert.equal(gitIn(root)("fsck", "--strict"), "");
    }),
  );

  it.effect("refuses to switch a sparse checkout rather than fill it in", () =>
    Effect.gen(function* () {
      gitIn(root)("branch", "other");
      const refused = yield* Checkout.checkout("other", { force: true }).pipe(
        Effect.provide(layerFor(root)),
        Effect.flip,
      );
      assert.equal(refused._tag, "Invalid");
      yield* Effect.promise(async () => {
        await assert.rejects(fs.stat(path.join(root, "sparse.txt")));
      });
    }),
  );
});
