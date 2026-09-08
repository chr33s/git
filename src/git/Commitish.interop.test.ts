import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Stream } from "effect";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { earliestUnique } from "../social/Lineage.ts";
import { next as bisectNext } from "./Bisect.ts";
import { stores } from "./Node.ts";
import { forPath } from "./History.ts";
import { cherryPick, rebase } from "./Rebase.ts";
import * as Repository from "./Repository.ts";
import { isOid } from "./Store.ts";

describe.skipIf(!hasGit)("commit revisions against Git", () => {
  for (const operation of [
    "branch",
    "log",
    "first-parent",
    "path-history",
    "lineage",
    "bisect",
    "merge-base",
    "merge-tree",
    "merge",
    "is-ancestor",
    "cherry-pick",
    "rebase",
  ] as const) {
    it.live(`${operation} follows annotated and nested tags to their commits`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "commitish-"));
        try {
          const git = gitIn(root);
          git("init", "-q", "-b", "main");
          fastImport(
            root,
            importCommit({
              branch: "refs/heads/main",
              mark: 1,
              message: "base",
              files: [{ path: "file.txt", content: "base\n" }],
            }) +
              importCommit({
                branch: "refs/heads/main",
                mark: 2,
                message: "tip",
                files: [{ path: "file.txt", content: "tip\n" }],
                from: 1,
              }),
          );
          git("tag", "-am", "release", "release");
          git("-c", "advice.nestedTag=false", "tag", "-am", "nested", "nested", "release");
          const base = git("rev-parse", "main^").trim();
          assert.ok(isOid(base));
          git("tag", "-am", "base", "base-tag", base);
          const baseTag = git("rev-parse", "base-tag").trim();
          assert.ok(isOid(baseTag));
          const layer = Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provide(stores(path.join(root, ".git"))),
          );
          for (const ref of ["release", "nested"]) {
            const oid = git("rev-parse", ref).trim();
            assert.ok(isOid(oid));
            if (operation === "branch") {
              git("branch", `stock-${ref}`, ref);
              const native = await Effect.runPromise(
                Effect.gen(function* () {
                  return yield* (yield* Repository.Repository).branch({
                    name: `native-${ref}`,
                    base: `refs/tags/${ref}`,
                  });
                }).pipe(Effect.provide(layer)),
              );
              const expected = git("rev-parse", `stock-${ref}`).trim();
              assert.equal(native, expected);
              assert.equal(git("rev-parse", `native-${ref}`).trim(), expected);
            } else if (
              operation === "log" ||
              operation === "first-parent" ||
              operation === "path-history"
            ) {
              const options = { firstParent: operation === "first-parent" };
              await Effect.runPromise(
                Effect.gen(function* () {
                  const stream =
                    operation === "path-history"
                      ? forPath(oid, "file.txt").pipe(Stream.map((commit) => commit.oid))
                      : (yield* Repository.Repository)
                          .log(oid, options)
                          .pipe(Stream.map((commit) => commit.oid));
                  const expected = git(
                    "log",
                    "--format=%H",
                    ...(options.firstParent ? ["--first-parent"] : []),
                    ref,
                    ...(operation === "path-history" ? ["--", "file.txt"] : []),
                  )
                    .trim()
                    .split("\n");
                  for (let execution = 0; execution < 2; execution++) {
                    assert.deepEqual(yield* Stream.runCollect(stream), expected);
                  }
                }).pipe(Effect.provide(layer)),
              );
            } else {
              await Effect.runPromise(
                Effect.gen(function* () {
                  const repository = yield* Repository.Repository;
                  if (operation === "bisect") {
                    const tip = git("rev-parse", `${ref}^{}`).trim();
                    assert.ok(isOid(tip));
                    const found = yield* bisectNext({ bad: oid, good: [baseTag] });
                    assert.equal(found.kind, "found");
                    assert.equal(found.commit, git("rev-list", ref, "^base-tag").trim());
                    assert.deepEqual(
                      yield* bisectNext({ bad: oid, good: [] }),
                      yield* bisectNext({ bad: tip, good: [] }),
                    );
                    for (const [bad, good] of [
                      [oid, tip],
                      [tip, oid],
                    ] as const) {
                      const refused = yield* Effect.result(bisectNext({ bad, good: [good] }));
                      assert.equal(refused._tag, "Failure");
                      if (refused._tag === "Failure") assert.equal(refused.failure._tag, "Invalid");
                    }
                    const output = execFileSync(
                      process.execPath,
                      [
                        path.resolve("src/cli/main.ts"),
                        "bisect",
                        "--root",
                        root,
                        "--bad",
                        ref,
                        "--good",
                        "base-tag",
                        ".git",
                      ],
                      { env: gitEnv, encoding: "utf8" },
                    );
                    assert.equal(output.trim(), `${tip} is the first bad commit`);
                  } else if (operation === "lineage") {
                    const origin = `sha1:${git("rev-list", "--max-parents=0", ref).trim()}`;
                    const fork = `sha1:${git("rev-list", ref, "^base-tag").trim()}`;
                    assert.equal(yield* earliestUnique({ head: oid }), origin);
                    assert.equal(yield* earliestUnique({ head: oid, upstream: baseTag }), fork);
                    for (const [flags, expected] of [
                      [[], origin],
                      [["--from", "base-tag"], fork],
                    ] as const) {
                      const output = execFileSync(
                        process.execPath,
                        [
                          path.resolve("src/cli/main.ts"),
                          "social",
                          "lineage",
                          "--root",
                          root,
                          "--revision",
                          ref,
                          ...flags,
                          ".git",
                        ],
                        { env: gitEnv, encoding: "utf8" },
                      );
                      assert.equal(output.trim(), expected);
                    }
                  } else if (operation === "merge-base") {
                    assert.equal(
                      yield* repository.mergeBase(baseTag, oid),
                      git("merge-base", "base-tag", ref).trim(),
                    );
                  } else if (operation === "is-ancestor") {
                    git("merge-base", "--is-ancestor", "base-tag", ref);
                    assert.equal(yield* repository.isAncestor(baseTag, oid), true);
                  } else if (operation === "merge-tree") {
                    assert.equal(
                      (yield* repository.mergeTree({ ours: baseTag, theirs: oid })).tree,
                      git("merge-tree", "--write-tree", "base-tag", ref).trim(),
                    );
                  } else {
                    git("-c", "advice.detachedHead=false", "checkout", "--quiet", "--detach", base);
                    let head;
                    if (operation === "merge") {
                      git("merge", "--ff-only", "--quiet", ref);
                      head = (yield* repository.merge({
                        ours: "refs/tags/base-tag",
                        theirs: `refs/tags/${ref}`,
                        author: (yield* repository.readCommit(base)).author,
                      })).commit;
                    } else if (operation === "cherry-pick") {
                      git("cherry-pick", ref);
                      head = (yield* cherryPick({
                        commit: `refs/tags/${ref}`,
                        onto: "refs/tags/base-tag",
                      })).head;
                    } else {
                      git("-c", "advice.detachedHead=false", "rebase", "base-tag", ref);
                      head = (yield* rebase({
                        branch: `refs/tags/${ref}`,
                        onto: "refs/tags/base-tag",
                      })).head;
                    }
                    assert.ok(head !== null);
                    const commit = yield* repository.readCommit(head);
                    assert.equal(commit.tree, git("rev-parse", "HEAD^{tree}").trim());
                    assert.deepEqual(
                      commit.parents,
                      git("show", "-s", "--format=%P", "HEAD").trim().split(" "),
                    );
                  }
                }).pipe(Effect.provide(layer)),
              );
            }
          }
          if (operation === "branch") {
            git("tag", "-am", "tree", "tree", "HEAD^{tree}");
            assert.throws(() => git("branch", "stock-tree", "tree"));
            const rejected = await Effect.runPromise(
              Effect.gen(function* () {
                const repository = yield* Repository.Repository;
                const failure = yield* repository
                  .branch({ name: "native-tree", base: "refs/tags/tree" })
                  .pipe(Effect.flip);
                assert.equal(yield* repository.resolve("refs/heads/native-tree"), null);
                return failure;
              }).pipe(Effect.provide(layer)),
            );
            assert.equal(rejected._tag, "Invalid");
          }
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
