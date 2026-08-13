/**
 * The working tree, against the real `git` binary.
 *
 * A staging area that only this implementation understands would be a private
 * format wearing git's filename, so the interop tests do the thing that
 * actually settles it: stage with ours and let `git status` describe the
 * result, stage with git and let ours describe it.
 */
import assert from "node:assert/strict";
import * as fsSync from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { gitIn, hasGit } from "../testing/Git.ts";
import * as Checkout from "./Checkout.ts";
import { stores as memoryStores } from "./Memory.ts";
import { stores as nodeStores } from "./Node.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { encodeTree } from "./Format.ts";
import { ObjectStore, RefStore } from "./Store.ts";
import { indexMemory, workTreeMemory, WorkTree } from "./Work.ts";
import { workspace } from "./Work.node.ts";

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const encode = (text: string) => new TextEncoder().encode(text);

/** Everything a work tree needs, in memory. */
const inMemory = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(memoryStores),
  Layer.provideMerge(indexMemory),
  Layer.provideMerge(workTreeMemory),
);

describe("working tree", () => {
  it.live("tracks the three states a path can be in", () =>
    Effect.gen(function* () {
      const work = yield* WorkTree;

      yield* work.write("a.txt", encode("one\n"), 0o100644);
      yield* work.write("dir/b.txt", encode("two\n"), 0o100644);

      // Nothing staged: both files are untracked, and that is the only
      // thing status should say about them.
      const fresh = yield* Checkout.status();
      assert.deepEqual(fresh.untracked, ["a.txt", "dir/b.txt"]);
      assert.deepEqual(fresh.staged, []);
      assert.deepEqual(fresh.unstaged, []);

      yield* Checkout.add(["."]);
      const staged = yield* Checkout.status();
      assert.deepEqual(
        staged.staged.map((entry) => [entry.path, entry.change]),
        [
          ["a.txt", "added"],
          ["dir/b.txt", "added"],
        ],
      );
      assert.deepEqual(staged.untracked, []);

      const committed = yield* Checkout.commit({ message: "first", author });
      assert.equal(committed.files, 2);

      // Committed and clean.
      const clean = yield* Checkout.status();
      assert.deepEqual(clean.staged, []);
      assert.deepEqual(clean.unstaged, []);

      // Edit on disk: unstaged, until it is added.
      yield* work.write("a.txt", encode("one changed\n"), 0o100644);
      const dirty = yield* Checkout.status();
      assert.deepEqual(
        dirty.unstaged.map((entry) => [entry.path, entry.change]),
        [["a.txt", "modified"]],
      );

      yield* Checkout.add(["a.txt"]);
      const restaged = yield* Checkout.status();
      assert.deepEqual(
        restaged.staged.map((entry) => [entry.path, entry.change]),
        [["a.txt", "modified"]],
      );
      assert.deepEqual(restaged.unstaged, []);

      // Deleted on disk shows as unstaged deletion.
      yield* work.remove("dir/b.txt");
      const deleted = yield* Checkout.status();
      assert.deepEqual(
        deleted.unstaged.map((entry) => [entry.path, entry.change]),
        [["dir/b.txt", "deleted"]],
      );
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("restores from the index and from a commit", () =>
    Effect.gen(function* () {
      const work = yield* WorkTree;

      yield* work.write("a.txt", encode("original\n"), 0o100644);
      yield* Checkout.add(["a.txt"]);
      yield* Checkout.commit({ message: "first", author });

      yield* work.write("a.txt", encode("scribbled over\n"), 0o100644);
      yield* Checkout.restore(["a.txt"]);
      assert.equal(new TextDecoder().decode(yield* work.read("a.txt")), "original\n");

      // Staging a change then restoring the index from HEAD unstages it.
      yield* work.write("a.txt", encode("staged change\n"), 0o100644);
      yield* Checkout.add(["a.txt"]);
      yield* Checkout.restore(["a.txt"], { staged: true, worktree: false, source: "HEAD" });
      const status = yield* Checkout.status();
      assert.deepEqual(status.staged, []);
      // …and leaves the disk alone, which is the half `--staged` promises.
      assert.equal(new TextDecoder().decode(yield* work.read("a.txt")), "staged change\n");
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("unstages a newly added file with --staged", () =>
    Effect.gen(function* () {
      const work = yield* WorkTree;

      yield* work.write("new.txt", encode("fresh\n"), 0o100644);
      yield* Checkout.add(["new.txt"]);
      assert.equal((yield* Checkout.status()).staged.length, 1);

      // HEAD does not hold the path, so restoring the index from it means
      // taking the entry out — which is what unstaging is.
      yield* Checkout.restore(["new.txt"], { staged: true, worktree: false });
      const after = yield* Checkout.status();
      assert.deepEqual(after.staged, []);
      assert.deepEqual(after.untracked, ["new.txt"]);
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("unstages a deletion, whose file is not on disk to stat", () =>
    Effect.gen(function* () {
      const work = yield* WorkTree;

      yield* work.write("a.txt", encode("kept\n"), 0o100644);
      yield* Checkout.add(["a.txt"]);
      yield* Checkout.commit({ message: "first", author });

      yield* Checkout.remove(["a.txt"]);
      assert.equal((yield* Checkout.status()).staged.length, 1);

      // The file is gone from disk, which is exactly when the index still has
      // to be written: HEAD holds the path, so restoring the index re-adds it.
      yield* Checkout.restore(["a.txt"], { staged: true, worktree: false });
      assert.deepEqual((yield* Checkout.status()).staged, []);
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("removes, moves, and refuses what is not tracked", () =>
    Effect.gen(function* () {
      const work = yield* WorkTree;

      yield* work.write("keep.txt", encode("keep\n"), 0o100644);
      yield* work.write("gone.txt", encode("gone\n"), 0o100644);
      yield* Checkout.add(["."]);
      yield* Checkout.commit({ message: "first", author });

      yield* Checkout.remove(["gone.txt"]);
      assert.equal(yield* work.stat("gone.txt"), null);

      yield* Checkout.move("keep.txt", "moved.txt");
      assert.equal(yield* work.stat("keep.txt"), null);
      assert.equal(new TextDecoder().decode(yield* work.read("moved.txt")), "keep\n");

      const untracked = yield* Checkout.remove(["nothing.txt"]).pipe(Effect.flip);
      assert.equal(untracked._tag, "Invalid");
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("refuses to overwrite an untracked file or delete a staged one", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const work = yield* WorkTree;

      yield* work.write("shared.txt", encode("main\n"), 0o100644);
      yield* Checkout.add(["."]);
      yield* Checkout.commit({ message: "on main", author });
      yield* repository.branch({ name: "side", base: "refs/heads/main" });
      yield* Checkout.checkout("side");
      yield* work.write("only-on-side.txt", encode("side\n"), 0o100644);
      yield* Checkout.add(["."]);
      yield* Checkout.commit({ message: "on side", author });
      yield* Checkout.checkout("main");

      // An untracked file the target branch also has: overwriting it loses
      // content the object store never saw.
      yield* work.write("only-on-side.txt", encode("mine, unsaved\n"), 0o100644);
      const clobber = yield* Effect.flip(Checkout.checkout("side"));
      assert.equal(clobber._tag, "Invalid");
      assert.equal(
        new TextDecoder().decode(yield* work.read("only-on-side.txt")),
        "mine, unsaved\n",
      );

      // And a staged addition the target does not have.
      yield* work.remove("only-on-side.txt");
      yield* work.write("fresh.txt", encode("staged\n"), 0o100644);
      yield* Checkout.add(["fresh.txt"]);
      const staged = yield* Effect.flip(Checkout.checkout("side"));
      assert.equal(staged._tag, "Invalid");
      assert.equal(new TextDecoder().decode(yield* work.read("fresh.txt")), "staged\n");
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("checks out a branch, and refuses to overwrite unstaged work", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const work = yield* WorkTree;

      yield* work.write("shared.txt", encode("main\n"), 0o100644);
      yield* Checkout.add(["."]);
      yield* Checkout.commit({ message: "on main", author });

      yield* repository.branch({ name: "side", base: "refs/heads/main" });
      yield* Checkout.checkout("side");

      yield* work.write("shared.txt", encode("side\n"), 0o100644);
      yield* work.write("only-on-side.txt", encode("side only\n"), 0o100644);
      yield* Checkout.add(["."]);
      yield* Checkout.commit({ message: "on side", author });

      // Switching back removes what the target tree does not have.
      yield* Checkout.checkout("main");
      assert.equal(new TextDecoder().decode(yield* work.read("shared.txt")), "main\n");
      assert.equal(yield* work.stat("only-on-side.txt"), null);

      // An unstaged edit is not silently discarded.
      yield* work.write("shared.txt", encode("uncommitted\n"), 0o100644);
      const refused = yield* Checkout.checkout("side").pipe(Effect.flip);
      assert.equal(refused._tag, "Invalid");

      // …unless it is asked for.
      yield* Checkout.checkout("side", { force: true });
      assert.equal(new TextDecoder().decode(yield* work.read("shared.txt")), "side\n");
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("leaves the work tree alone when -b names a branch that exists", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const work = yield* WorkTree;

      yield* work.write("kept.txt", encode("committed\n"), 0o100644);
      yield* Checkout.add(["."]);
      yield* Checkout.commit({ message: "first", author });
      yield* repository.branch({ name: "taken", base: "refs/heads/main" });

      // Staged but never committed: `checkout -b --force` deletes exactly
      // these from disk on its way to the new branch, and creating the branch
      // afterwards meant a name that was already taken aborted the checkout
      // with the files already gone and the index never saved.
      yield* work.write("staged.txt", encode("not committed\n"), 0o100644);
      yield* Checkout.add(["staged.txt"]);

      const refused = yield* Effect.flip(Checkout.checkout("taken", { create: true, force: true }));
      assert.equal(refused._tag, "RefConflict");

      assert.equal(new TextDecoder().decode(yield* work.read("staged.txt")), "not committed\n");
      assert.equal(new TextDecoder().decode(yield* work.read("kept.txt")), "committed\n");
      assert.equal(yield* repository.head, "refs/heads/main");

      // And a name that is free still works. `checkout -b` branches from the
      // oid HEAD resolves to, so a `branch` that only understood ref names
      // failed on every one of these with "unknown ref '<40 hex>'".
      const created = yield* Checkout.checkout("fresh", { create: true, force: true });
      assert.equal(created.ref, "refs/heads/fresh");
      assert.equal(yield* repository.head, "refs/heads/fresh");
      assert.equal(yield* repository.resolve("refs/heads/fresh"), created.oid);
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("keeps a submodule through a checkout and the commit that follows", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const work = yield* WorkTree;

      yield* work.write("readme.md", encode("top\n"), 0o100644);
      yield* Checkout.add(["."]);
      const first = yield* Checkout.commit({ message: "first", author });

      // A gitlink names a commit in another repository — an oid nothing here
      // will ever hold. Left out of the index by `checkout`, it is left out
      // of the tree the next `commit` writes, and the submodule disappears
      // from history with no error and no conflict.
      const submodule = "1".repeat(40);
      const withModule = yield* repository.writeTree([
        ...(yield* repository.readTree(first.tree)),
        { mode: "160000", name: "vendor", oid: submodule as never },
      ]);
      const tip = yield* repository.commit({
        branch: "main",
        tree: withModule,
        message: "add submodule",
        author,
        expected: first.oid,
      });

      yield* Checkout.checkout("main", { force: true });
      // Nothing was written to disk for it: there are no bytes to write.
      assert.equal(yield* work.stat("vendor"), null);

      yield* work.write("readme.md", encode("edited\n"), 0o100644);
      yield* Checkout.add(["readme.md"]);
      const next = yield* Checkout.commit({ message: "edit", author });

      const entries = yield* repository.readTree(next.tree);
      assert.deepEqual(entries.map((entry) => `${entry.mode} ${entry.name}`).sort(), [
        "100644 readme.md",
        "160000 vendor",
      ]);
      assert.equal(entries.find((entry) => entry.name === "vendor")?.oid, submodule);
      assert.notEqual(next.oid, tip);
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );
});

describe("a checkout of a tree it did not write", () => {
  let root: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "hostile-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it("creates the checkout directory on its first write", async () => {
    // `--work ./new` materialises the tree as it writes; a containment check
    // that resolved the root before it existed would fail with ENOENT here.
    const checkout = path.join(root, "not-yet");
    const layer = GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provideMerge(nodeStores(path.join(root, "bare"))),
      Layer.provideMerge(workspace(checkout)),
    );

    await Effect.runPromise(
      Effect.gen(function* () {
        const work = yield* WorkTree;
        yield* work.write("a/b.txt", encode("made\n"), 0o100644);
      }).pipe(Effect.provide(layer)) as unknown as Effect.Effect<void>,
    );

    assert.equal(fsSync.existsSync(path.join(checkout, "a", "b.txt")), true);
  });

  it("refuses a symlink that points at an ancestor of the checkout", async () => {
    const checkout = path.join(root, "work");
    await fs.mkdir(checkout, { recursive: true });
    // The escape the ancestor-tolerant check let through: `link -> ..` puts
    // the parent of the checkout inside it, and a write under `link` lands
    // beside the checkout rather than in it.
    await fs.symlink("..", path.join(checkout, "link"));

    const layer = GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provideMerge(nodeStores(path.join(checkout, ".git"))),
      Layer.provideMerge(workspace(checkout)),
    );

    await Effect.runPromise(
      Effect.gen(function* () {
        const work = yield* WorkTree;
        yield* Effect.flip(work.write("link/pwned.txt", encode("owned\n"), 0o100644));
      }).pipe(Effect.provide(layer)) as unknown as Effect.Effect<void>,
    );

    assert.equal(fsSync.existsSync(path.join(root, "pwned.txt")), false);
  });

  it("refuses to remove through a symlink that leaves the checkout", async () => {
    const checkout = path.join(root, "work");
    const outside = path.join(root, "outside");
    await fs.mkdir(checkout, { recursive: true });
    await fs.mkdir(outside, { recursive: true });
    await fs.writeFile(path.join(outside, "keep.txt"), "not yours\n");
    await fs.symlink(outside, path.join(checkout, "link"));

    const layer = GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provideMerge(nodeStores(path.join(checkout, ".git"))),
      Layer.provideMerge(workspace(checkout)),
    );

    await Effect.runPromise(
      Effect.gen(function* () {
        const work = yield* WorkTree;
        // `link/keep.txt` names nothing inside the checkout: the name is
        // innocent and the path still lands in another directory.
        yield* Effect.flip(work.remove("link/keep.txt"));
      }).pipe(Effect.provide(layer)) as unknown as Effect.Effect<void>,
    );

    assert.equal(fsSync.existsSync(path.join(outside, "keep.txt")), true);
  });

  it("refuses a tree entry that would write outside the checkout", async () => {
    const checkout = path.join(root, "work");
    await fs.mkdir(checkout, { recursive: true });

    const layer = GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provideMerge(nodeStores(path.join(checkout, ".git"))),
      Layer.provideMerge(workspace(checkout)),
    );

    const failure = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;

        // A tree from a remote can name anything: `parseTree` validates the
        // bytes, not the meaning, so `..` reaches the work tree as a path.
        // Written as raw bytes, because `writeTree` now refuses to mint one —
        // this is the other half of the defence, for a tree that arrived in a
        // pack rather than through this API.
        const objects = yield* ObjectStore;
        const blob = yield* repository.writeBlob(encode("owned\n"));
        const inner = yield* repository.writeTree([{ mode: "100644", name: "escaped", oid: blob }]);
        const tree = yield* objects.write({
          type: "tree",
          data: encodeTree([{ mode: "40000", name: "..", oid: inner }]),
        });
        yield* repository.commit({ branch: "main", tree, message: "hostile", author });

        return yield* Effect.flip(Checkout.checkout("main"));
      }).pipe(Effect.provide(layer)) as unknown as Effect.Effect<{ _tag: string }>,
    );

    assert.equal(failure._tag, "Invalid");
    // And nothing landed beside the checkout.
    assert.equal(fsSync.existsSync(path.join(root, "escaped")), false);
  });
});

describe.skipIf(!hasGit)("working tree, against git", () => {
  let root: string;

  const layerFor = (checkout: string) =>
    GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provideMerge(nodeStores(path.join(checkout, ".git"))),
      Layer.provideMerge(workspace(checkout)),
    );

  const git = (cwd: string, ...args: string[]) => gitIn(cwd)(...args);

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "worktree-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it("stages files that git status then agrees are staged", async () => {
    git(root, "init", "-q", "-b", "main", ".");

    await fs.writeFile(path.join(root, "a.txt"), "one\n");
    await fs.mkdir(path.join(root, "dir"), { recursive: true });
    await fs.writeFile(path.join(root, "dir", "b.txt"), "two\n");
    await fs.writeFile(path.join(root, "run.sh"), "#!/bin/sh\n");
    await fs.chmod(path.join(root, "run.sh"), 0o755);

    await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layerFor(root)), Effect.orDie));

    // git's own reader on the index we wrote: paths, modes and oids.
    const listed = git(root, "ls-files", "--stage");
    assert.match(listed, /^100644 [0-9a-f]{40} 0\ta\.txt$/m);
    assert.match(listed, /^100644 [0-9a-f]{40} 0\tdir\/b\.txt$/m);
    // The execute bit survived, which is the mode handling under test.
    assert.match(listed, /^100755 [0-9a-f]{40} 0\trun\.sh$/m);

    // And git agrees these are staged additions, not untracked files.
    const status = git(root, "status", "--porcelain");
    assert.match(status, /^A {2}a\.txt$/m);
    assert.match(status, /^A {2}run\.sh$/m);

    // git can complete the commit from our index.
    git(root, "commit", "-q", "-m", "staged by us");
    assert.equal(git(root, "log", "--format=%s").trim(), "staged by us");
  });

  it("reads an index git wrote, and describes it the same way", async () => {
    git(root, "init", "-q", "-b", "main", ".");

    await fs.writeFile(path.join(root, "tracked.txt"), "tracked\n");
    await fs.writeFile(path.join(root, "untracked.txt"), "untracked\n");
    git(root, "add", "tracked.txt");

    const status = await Effect.runPromise(
      Checkout.status().pipe(Effect.provide(layerFor(root)), Effect.orDie),
    );

    assert.deepEqual(
      status.staged.map((entry) => [entry.path, entry.change]),
      [["tracked.txt", "added"]],
    );
    assert.deepEqual(status.untracked, ["untracked.txt"]);
    assert.deepEqual(status.unstaged, []);
  });

  it("commits from its own index into a repository git then reads", async () => {
    const checkout = path.join(root, "work");
    await fs.mkdir(checkout, { recursive: true });

    // A bare-style setup written by us: `.git` with HEAD, then a checkout.
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* (yield* RefStore).setHead("refs/heads/main");
      }).pipe(Effect.provide(nodeStores(path.join(checkout, ".git"))), Effect.orDie),
    );

    await fs.writeFile(path.join(checkout, "readme.md"), "# hello\n");
    await fs.mkdir(path.join(checkout, "src"), { recursive: true });
    await fs.writeFile(path.join(checkout, "src", "main.ts"), "export const x = 1;\n");

    const committed = await Effect.runPromise(
      Effect.gen(function* () {
        yield* Checkout.add(["."]);
        return yield* Checkout.commit({ message: "from our index", author });
      }).pipe(Effect.provide(layerFor(checkout)), Effect.orDie),
    );
    assert.equal(committed.files, 2);

    // git reads the commit, its tree, and finds the work tree clean.
    assert.equal(git(checkout, "log", "--format=%s").trim(), "from our index");
    assert.match(git(checkout, "ls-tree", "-r", "--name-only", "HEAD"), /src\/main\.ts/);
    assert.equal(git(checkout, "status", "--porcelain").trim(), "");
    git(checkout, "fsck", "--strict");
  });
});
