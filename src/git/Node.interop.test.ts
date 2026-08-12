/**
 * Interop: a repository written through the ports is a real git repository.
 *
 * The unit tests prove the backend satisfies our own contract. This proves the
 * contract is the right one — `git` itself reads what we wrote, so the object
 * framing, the SHA-1s, the tree encoding and the ref layout are all correct
 * rather than merely self-consistent.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";

import { Effect } from "effect";

import { encodeCommit, encodeTree } from "./Format.ts";
import { stores } from "./Node.ts";
import { ObjectStore, type Oid, RefStore } from "./Store.ts";

const hasGit = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

describe("Node backend interop with git", { skip: hasGit ? false : "git not installed" }, () => {
  const build = async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-interop-"));

    const commit = await Effect.runPromise(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const refs = yield* RefStore;

        const blob = yield* objects.write({
          type: "blob",
          data: new TextEncoder().encode("hello\n"),
        });
        const tree = yield* objects.write({
          type: "tree",
          data: encodeTree([{ mode: "100644", name: "a.txt", oid: blob }]),
        });
        const commit = yield* objects.write({
          type: "commit",
          data: encodeCommit({
            tree,
            parents: [],
            author,
            committer: author,
            message: "from our store\n",
          }),
        });

        yield* refs.setHead("refs/heads/main");
        yield* refs.apply([{ name: "refs/heads/main", value: commit, expected: null }]);
        return commit;
      }).pipe(Effect.provide(stores(root))) as Effect.Effect<Oid>,
    );

    return { commit, root };
  };

  const git = (root: string, ...args: string[]) =>
    execFileSync("git", [`--git-dir=${root}`, ...args], { encoding: "utf8" }).trim();

  it("passes git fsck", async () => {
    const { root } = await build();
    try {
      // fsck writes complaints to stdout/stderr and exits non-zero on a broken
      // object, so reaching the assertion at all is most of the test.
      assert.equal(git(root, "fsck", "--strict"), "");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("is readable by git log, cat-file and ls-tree", async () => {
    const { commit, root } = await build();
    try {
      assert.equal(git(root, "rev-parse", "HEAD"), commit);
      assert.match(git(root, "log", "--oneline"), /from our store/);
      assert.match(
        git(root, "cat-file", "-p", "HEAD"),
        /author Alice <alice@example\.com> 1700000000 \+0000/,
      );
      assert.match(
        git(root, "ls-tree", "HEAD"),
        /100644 blob ce013625030ba8dba906f756967f9e9ca394464a\ta\.txt/,
      );
      assert.equal(git(root, "show", "HEAD:a.txt"), "hello");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });
});
