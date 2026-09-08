import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";

describe.skipIf(!hasGit)("merge conflict preferences against Git", () => {
  for (const strategy of ["ours", "theirs"] as const) {
    it.live(`${strategy} preserves clean edits within files changed on both sides`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "merge-preference-"));
        const git = gitIn(root);
        try {
          git("init", "-q", "-b", "main");
          const base = "one\ntwo\nthree\nfour\nfive\nsix\nseven\n";
          fastImport(
            root,
            [
              importCommit({
                branch: "refs/heads/main",
                mark: 1,
                message: "base",
                files: [
                  { path: "clean.txt", content: base },
                  { path: "conflict.txt", content: base },
                  { path: "mode.sh", content: base },
                  { path: "binary", content: "base\0bytes" },
                ],
              }),
              importCommit({
                branch: "refs/heads/ours",
                mark: 2,
                from: 1,
                message: "our edits",
                files: [
                  { path: "clean.txt", content: base.replace("one", "our one") },
                  {
                    path: "conflict.txt",
                    content: base.replace("one", "our one").replace("four", "our four"),
                  },
                  { path: "mode.sh", content: base.replace("one", "our one") },
                  { path: "binary", content: "ours\0bytes" },
                ],
              }),
              importCommit({
                branch: "refs/heads/theirs",
                mark: 3,
                from: 1,
                message: "their edits",
                files: [
                  { path: "clean.txt", content: base.replace("seven", "their seven") },
                  {
                    path: "conflict.txt",
                    content: base.replace("four", "their four").replace("seven", "their seven"),
                  },
                  { path: "binary", content: "theirs\0bytes" },
                ],
              }),
            ].join(""),
          );
          git("checkout", "-q", "theirs");
          await fs.chmod(path.join(root, "mode.sh"), 0o755);
          git("update-index", "--chmod=+x", "mode.sh");
          git("commit", "-qm", "make script executable");
          git("branch", "native", "ours");
          git("checkout", "-q", "ours");
          git("merge", "--no-edit", `-X${strategy}`, "theirs");

          const outcome = await Effect.runPromise(
            Effect.gen(function* () {
              const repository = yield* Repository.Repository;
              return yield* repository.merge({
                ours: "refs/heads/native",
                theirs: "refs/heads/theirs",
                into: "refs/heads/native",
                strategy,
                author: {
                  name: "T",
                  email: "t@e.com",
                  at: new Date(1_700_000_000_000),
                  offset: 0,
                },
              });
            }).pipe(
              Effect.provide(
                Repository.layer.pipe(
                  Layer.provide(Repository.hooksNoop),
                  Layer.provide(stores(path.join(root, ".git"))),
                ),
              ),
            ),
          );
          assert.equal(outcome.kind, "merged");
          for (const name of ["clean.txt", "conflict.txt", "mode.sh", "binary"]) {
            assert.equal(git("show", `native:${name}`), git("show", `HEAD:${name}`), name);
          }
          assert.equal(outcome.tree, git("rev-parse", "HEAD^{tree}").trim());
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
