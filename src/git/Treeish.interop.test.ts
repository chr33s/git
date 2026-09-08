import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { isOid } from "./Store.ts";

describe.skipIf(!hasGit)("tree revisions against Git", () => {
  it.live("lists, searches and shallow-fetches nested tags and tags targeting trees", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "treeish-"));
      try {
        const git = gitIn(root);
        git("init", "--bare", "-q", "-b", "main");
        fastImport(
          root,
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: "base",
            files: [{ path: "readme.md", content: "needle in a tagged tree\n" }],
          }) +
            importCommit({
              branch: "refs/heads/main",
              mark: 2,
              message: "second",
              files: [],
              from: 1,
            }) +
            importCommit({
              branch: "refs/heads/main",
              mark: 3,
              message: "third",
              files: [],
              from: 2,
            }),
        );
        git("tag", "-am", "release", "release");
        git("-c", "advice.nestedTag=false", "tag", "-am", "nested", "nested", "release");
        git("tag", "-am", "tree", "tree", "HEAD^{tree}");
        git("-c", "advice.nestedTag=false", "tag", "-am", "nested-tree", "nested-tree", "tree");
        let previous = "nested";
        for (let index = 0; index < 20; index++) {
          const name = `chain-${index}`;
          git("-c", "advice.nestedTag=false", "tag", "-am", name, name, previous);
          previous = name;
        }
        const layer = Repository.layer.pipe(
          Layer.provide(Repository.hooksNoop),
          Layer.provide(stores(root)),
        );
        for (const ref of ["release", "nested", "tree", "nested-tree", previous]) {
          const oid = git("rev-parse", ref).trim();
          assert.ok(isOid(oid));
          const expected = git("rev-parse", `${ref}^{tree}`).trim();
          const native = await Effect.runPromise(
            Effect.gen(function* () {
              const repository = yield* Repository.Repository;
              const tree = yield* Repository.treeAt(repository, oid);
              const files = yield* repository.listFiles(tree);
              const search = yield* repository.search({
                ref: `refs/tags/${ref}`,
                pattern: "needle",
                fixed: true,
              });
              return { tree, files: files.map((file) => file.path), search };
            }).pipe(Effect.provide(layer)),
          );
          assert.equal(native.tree, expected, ref);
          assert.deepEqual(
            native.files,
            git("ls-tree", "-r", "--name-only", ref).trim().split("\n"),
            ref,
          );
          assert.deepEqual(
            native.search.matches,
            [{ path: "readme.md", line: 1, text: "needle in a tagged tree" }],
            ref,
          );
          assert.equal(
            git("grep", "-F", "-n", "needle", ref).trim(),
            `${ref}:readme.md:1:needle in a tagged tree`,
          );
        }
        const clone = path.join(root, "shallow-clone");
        git(
          "-c",
          "advice.detachedHead=false",
          "clone",
          "--quiet",
          "--depth=1",
          "--branch",
          previous,
          pathToFileURL(root).href,
          clone,
        );
        const want = git("rev-parse", previous).trim();
        const parent = git("rev-parse", "HEAD^").trim();
        assert.ok(isOid(want) && isOid(parent));
        const plan = await Effect.runPromise(
          Effect.gen(function* () {
            return yield* (yield* Repository.Repository).fetch({
              wants: [want],
              haves: [],
              depth: 1,
            });
          }).pipe(Effect.provide(layer)),
        );
        assert.deepEqual(
          plan.shallow,
          (await fs.readFile(path.join(clone, ".git", "shallow"), "utf8")).trim().split("\n"),
        );
        assert.equal(plan.oids.includes(parent), false);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
