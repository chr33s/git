import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { retirePacksUnder, stores } from "./Node.ts";
import { isOid, ObjectStore } from "./Store.ts";

describe.skipIf(!hasGit)("live transitive alternates", () => {
  for (const packed of [false, true]) {
    it.live(`refreshes an intermediate repository's object source (packed=${packed})`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "alternates-refresh-"));
        try {
          const a = path.join(root, "a");
          const b = path.join(root, "b");
          const middle = path.join(root, "middle");
          const child = path.join(root, "child");
          const git = gitIn(root);
          git("init", "--bare", "-q", "-b", "main", a);
          fastImport(
            a,
            importCommit({
              branch: "refs/heads/main",
              mark: 1,
              message: "base",
              files: [{ path: "base.txt", content: "base\n" }],
            }),
          );
          git("clone", "--bare", "--quiet", a, b);
          fastImport(
            b,
            importCommit({
              branch: "refs/heads/extra",
              mark: 1,
              message: "extra",
              files: [{ path: "extra.txt", content: "extra\n" }],
            }),
          );
          if (packed) {
            gitIn(a)("repack", "-ad");
            gitIn(b)("repack", "-ad");
          }
          git("clone", "--bare", "--shared", "--quiet", a, middle);
          git("clone", "--bare", "--shared", "--quiet", middle, child);
          const old = gitIn(a)("rev-parse", "HEAD:base.txt").trim();
          const added = gitIn(b)("rev-parse", "extra:extra.txt").trim();
          assert.ok(isOid(old) && isOid(added));
          if (packed) {
            await assert.rejects(
              fs.stat(path.join(b, "objects", added.slice(0, 2), added.slice(2))),
              { code: "ENOENT" },
            );
          }
          const childFile = path.join(child, "objects", "info", "alternates");
          const before = await fs.readFile(childFile, "utf8");
          await Effect.runPromise(
            Effect.gen(function* () {
              const objects = yield* ObjectStore;
              assert.equal(new TextDecoder().decode((yield* objects.read(old)).data), "base\n");
              assert.equal(yield* objects.has(added), false);
              // B contains every original object plus a new branch, so changing
              // the middle repository's source preserves all existing history.
              yield* Effect.promise(() =>
                fs.writeFile(
                  path.join(middle, "objects", "info", "alternates"),
                  `${path.join(b, "objects")}\n`,
                ),
              );
              assert.equal(gitIn(child)("cat-file", "-p", added), "extra\n");
              assert.equal(yield* objects.has(added), true);
              assert.equal(new TextDecoder().decode((yield* objects.read(added)).data), "extra\n");
              assert.equal(new TextDecoder().decode((yield* objects.read(old)).data), "base\n");
              // Return to A, then add and remove a previously absent dependency
              // one level farther down the chain. The child's file never moves.
              yield* Effect.promise(() =>
                fs.writeFile(
                  path.join(middle, "objects", "info", "alternates"),
                  `${path.join(a, "objects")}\n`,
                ),
              );
              assert.equal(yield* objects.has(added), false);
              const indirect = path.join(a, "objects", "info", "alternates");
              yield* Effect.promise(() => fs.writeFile(indirect, `${path.join(b, "objects")}\n`));
              assert.equal(gitIn(child)("cat-file", "-p", added), "extra\n");
              assert.equal(new TextDecoder().decode((yield* objects.read(added)).data), "extra\n");
              yield* Effect.promise(() => fs.unlink(indirect));
              assert.equal(yield* objects.has(added), false);
            }).pipe(Effect.provide(stores(child))),
          );
          assert.equal(await fs.readFile(childFile, "utf8"), before);
        } finally {
          await retirePacksUnder(root);
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
