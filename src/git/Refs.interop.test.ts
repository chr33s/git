import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { initializeBare, refStore } from "./Node.ts";
import { isOid, RefStore, type RefUpdate } from "./Store.ts";

describe.skipIf(!hasGit)("filesystem ref writers", () => {
  it.live("preserves a loose tip when deleting its older packed entry fails", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "ref-delete-failure-"));
      try {
        const git = gitIn(root);
        git("init", "--bare", "-q", "-b", "main");
        fastImport(
          root,
          importCommit({ branch: "refs/heads/main", mark: 1, message: "base", files: [] }) +
            importCommit({
              branch: "refs/heads/main",
              mark: 2,
              message: "tip",
              files: [],
              from: 1,
            }),
        );
        const tip = git("rev-parse", "main").trim();
        const base = git("rev-parse", "main^").trim();
        git("update-ref", "refs/heads/main", base);
        git("pack-refs", "--all", "--prune");
        git("update-ref", "refs/heads/main", tip);
        assert.notEqual(tip, base);
        const packed = await fs.readFile(path.join(root, "packed-refs"), "utf8");
        const script = path.join(root, "fail-delete.mjs");
        await fs.writeFile(
          script,
          `
          import assert from "node:assert/strict";
          import fs from "node:fs/promises";
          import { syncBuiltinESMExports } from "node:module";
          import { Effect } from ${JSON.stringify(import.meta.resolve("effect"))};
          import { refStore } from ${JSON.stringify(import.meta.resolve("./Node.ts"))};
          import { RefStore } from ${JSON.stringify(import.meta.resolve("./Store.ts"))};
          const [root, tip, base, stage, atomic] = process.argv.slice(2);
          const original = fs[stage];
          // Fault injection stays in this child process and only affects the
          // packed-ref rewrite, after reservation and expectation checks.
          fs[stage] = async (...args) => {
            const target = stage === "rename" ? args[1] : args[0];
            if (target === root + "/packed-refs" || target.startsWith(root + "/packed-refs."))
              throw Object.assign(new Error("simulated filesystem I/O failure"), { code: "EIO" });
            return original(...args);
          };
          syncBuiltinESMExports();
          try {
            const result = await Effect.runPromise(Effect.gen(function* () {
              const refs = yield* RefStore;
              return yield* refs.apply([
                ...(atomic === "true" ? [{ name: "refs/heads/other", value: tip, expected: base }] : []),
                { name: "refs/heads/main", value: null, expected: tip },
              ], { atomic: atomic === "true" });
            }).pipe(Effect.provide(refStore(root))));
            assert.equal(result.at(-1).applied, false);
            assert.equal(result.at(-1).current, tip, "a refused deletion must preserve the tip");
          } finally {
            fs[stage] = original;
            syncBuiltinESMExports();
          }
        `,
        );
        for (const stage of ["writeFile", "rename"]) {
          for (const atomic of [false, true]) {
            git("update-ref", "refs/heads/other", base);
            await promisify(execFile)(process.execPath, [
              script,
              root,
              tip,
              base,
              stage,
              String(atomic),
            ]);
            assert.equal(git("rev-parse", "main").trim(), tip);
            assert.equal(git("rev-parse", "other").trim(), base);
            assert.equal(await fs.readFile(path.join(root, "packed-refs"), "utf8"), packed);
            assert.deepEqual(
              (await fs.readdir(root)).filter((name) => name.startsWith("packed-refs.")),
              [],
            );
          }
        }
        await Effect.runPromise(
          Effect.gen(function* () {
            const result = yield* (yield* RefStore).apply([
              { name: "refs/heads/main", value: null },
            ]);
            assert.equal(result[0]?.applied, true);
          }).pipe(Effect.provide(refStore(root))),
        );
        assert.equal(git("for-each-ref", "--format=%(refname)", "refs/heads/main").trim(), "");
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  it.live(
    "initializes an empty bare repository and preserves Git's reinitialization behavior",
    () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "bare-init-"));
        const native = path.join(root, "native");
        const stock = path.join(root, "stock");
        await fs.mkdir(native);
        await fs.mkdir(stock);
        try {
          for (const directory of [native, stock])
            await fs.writeFile(path.join(directory, "HEAD.lock"), "", { flag: "wx" });
          await assert.rejects(Effect.runPromise(initializeBare(native, "trunk")));
          await assert.rejects(
            promisify(execFile)("git", ["init", "--bare", "-b", "trunk", stock], { env: gitEnv }),
          );
          for (const directory of [native, stock])
            await fs.unlink(path.join(directory, "HEAD.lock"));
          await Effect.runPromise(initializeBare(native, "trunk"));
          gitIn(stock)("init", "--bare", "-q", "-b", "trunk");
          for (const directory of [native, stock]) {
            assert.equal(gitIn(directory)("rev-parse", "--is-bare-repository").trim(), "true");
            assert.equal(gitIn(directory)("for-each-ref", "--format=%(refname)").trim(), "");
            await fs.writeFile(path.join(directory, "HEAD.lock"), "", { flag: "wx" });
          }
          await Effect.runPromise(initializeBare(native, "side"));
          gitIn(stock)("init", "--bare", "-q", "-b", "side");
          for (const directory of [native, stock]) {
            assert.equal(gitIn(directory)("symbolic-ref", "HEAD").trim(), "refs/heads/trunk");
            await fs.unlink(path.join(directory, "HEAD.lock"));
          }
          // A detached HEAD is preserved as well.
          for (const directory of [native, stock]) {
            fastImport(
              directory,
              importCommit({ branch: "refs/heads/trunk", mark: 1, message: "base", files: [] }),
            );
            gitIn(directory)(
              "update-ref",
              "--no-deref",
              "HEAD",
              gitIn(directory)("rev-parse", "trunk").trim(),
            );
          }
          await Effect.runPromise(initializeBare(native, "side"));
          gitIn(stock)("init", "--bare", "-q", "-b", "side");
          assert.equal(
            await fs.readFile(path.join(native, "HEAD"), "utf8"),
            await fs.readFile(path.join(stock, "HEAD"), "utf8"),
          );
          const blocked = path.join(root, "blocked");
          await fs.mkdir(path.join(blocked, "HEAD"), { recursive: true });
          await assert.rejects(Effect.runPromise(initializeBare(blocked)));
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
  );

  it.live("reserves a large atomic batch within a normal Unix descriptor limit", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "ref-lock-limit-"));
      try {
        const git = gitIn(root);
        git("init", "--bare", "-q", "-b", "main");
        fastImport(
          root,
          importCommit({ branch: "refs/heads/main", mark: 1, message: "base", files: [] }),
        );
        const oid = git("rev-parse", "main").trim();
        const script = path.join(root, "write.mjs");
        await fs.writeFile(
          script,
          `
          import assert from 'node:assert/strict';
          import { Effect } from ${JSON.stringify(import.meta.resolve("effect"))};
          import { refStore } from ${JSON.stringify(import.meta.resolve("./Node.ts"))};
          import { RefStore } from ${JSON.stringify(import.meta.resolve("./Store.ts"))};
          const result = await Effect.runPromise(Effect.gen(function* () {
            const refs = yield* RefStore;
            return yield* refs.apply(Array.from({length: 256}, (_, i) => ({
              name: 'refs/heads/batch-' + i, value: ${JSON.stringify(oid)}, expected: null
            })), {atomic: true});
          }).pipe(Effect.provide(refStore(${JSON.stringify(root)}))));
          assert.equal(result.filter(r => r.applied).length, 256);
        `,
        );
        await promisify(execFile)("sh", [
          "-c",
          'ulimit -n 128\nexec "$@"',
          "ref-limit",
          process.execPath,
          script,
        ]);
        assert.equal(
          git("for-each-ref", "--format=%(refname)", "refs/heads/batch-*").trim().split("\n")
            .length,
          256,
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  it.live("respects a ref lock held by a prepared stock-Git transaction", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "ref-lock-"));
      try {
        const git = gitIn(root);
        git("init", "--bare", "-q", "-b", "main");
        fastImport(
          root,
          [
            importCommit({ branch: "refs/heads/main", mark: 1, message: "base", files: [] }),
            importCommit({
              branch: "refs/heads/theirs",
              mark: 2,
              from: 1,
              message: "theirs",
              files: [],
            }),
            importCommit({
              branch: "refs/heads/ours",
              mark: 3,
              from: 1,
              message: "ours",
              files: [],
            }),
          ].join(""),
        );
        const base = git("rev-parse", "main").trim();
        const theirs = git("rev-parse", "theirs").trim();
        const ours = git("rev-parse", "ours").trim();
        assert.ok(isOid(base) && isOid(ours));
        const child = spawn("git", ["update-ref", "--stdin"], { cwd: root, env: gitEnv });
        let stdout = "";
        let stderr = "";
        const prepared = Promise.withResolvers<void>();
        const finished = new Promise<number | null>((resolve, reject) => {
          child.once("error", (error) => {
            prepared.reject(error);
            reject(error);
          });
          child.once("close", (code) => {
            if (!stdout.includes("prepare: ok")) prepared.reject(new Error(stderr));
            resolve(code);
          });
        });
        child.stdout.on("data", (bytes: Buffer) => {
          stdout += bytes.toString();
          if (stdout.includes("prepare: ok")) prepared.resolve();
        });
        child.stderr.on("data", (bytes: Buffer) => {
          stderr += bytes.toString();
        });
        child.stdin.write(`start\nupdate refs/heads/main ${theirs} ${base}\nprepare\n`);
        try {
          await prepared.promise;
          const result = await Effect.runPromise(
            Effect.gen(function* () {
              const refs = yield* RefStore;
              return yield* refs.apply([{ name: "refs/heads/main", value: ours, expected: base }]);
            }).pipe(Effect.provide(refStore(root))),
          );
          assert.equal(result[0]?.applied, false, "a native writer must not bypass Git's lock");
          assert.equal(git("rev-parse", "main").trim(), base);
        } finally {
          child.stdin.end("commit\n");
          assert.equal(await finished, 0, stderr);
        }
        assert.equal(git("rev-parse", "main").trim(), theirs);

        // After Git releases the lock, eight independent store instances
        // compete for one expectation. Exactly one may successfully replace it.
        assert.ok(isOid(theirs));
        const update: RefUpdate = { name: "refs/heads/main", value: ours, expected: theirs };
        const results = await Promise.all(
          Array.from({ length: 8 }, () =>
            Effect.runPromise(
              Effect.gen(function* () {
                return yield* (yield* RefStore).apply([update]);
              }).pipe(Effect.provide(refStore(root))),
            ),
          ),
        );
        assert.equal(results.filter((result) => result[0]?.applied).length, 1);
        assert.equal(git("rev-parse", "main").trim(), ours);
        assert.equal(
          (await fs.readdir(path.join(root, "refs/heads"))).some((name) => name.endsWith(".lock")),
          false,
        );

        git("pack-refs", "--all", "--prune");
        const packedBefore = await fs.readFile(path.join(root, "packed-refs"), "utf8");
        const packedLock = path.join(root, "packed-refs.lock");
        await fs.writeFile(packedLock, "", { flag: "wx" });
        try {
          const refused = await Effect.runPromise(
            Effect.gen(function* () {
              return yield* (yield* RefStore).apply([
                { name: "refs/heads/main", value: null, expected: ours },
              ]);
            }).pipe(Effect.provide(refStore(root))),
          );
          assert.equal(refused[0]?.applied, false);
          assert.equal(await fs.readFile(path.join(root, "packed-refs"), "utf8"), packedBefore);
          assert.equal(git("rev-parse", "main").trim(), ours);
        } finally {
          await fs.unlink(packedLock);
        }
        await Effect.runPromise(
          Effect.gen(function* () {
            const refs = yield* RefStore;
            const removed = yield* refs.apply([
              { name: "refs/heads/main", value: null, expected: ours },
            ]);
            assert.equal(removed[0]?.applied, true);
            assert.equal(yield* refs.resolve("refs/heads/main"), null);
          }).pipe(Effect.provide(refStore(root))),
        );

        const headBefore = await fs.readFile(path.join(root, "HEAD"), "utf8");
        const headLock = path.join(root, "HEAD.lock");
        await fs.writeFile(headLock, "", { flag: "wx" });
        try {
          await Effect.runPromise(
            Effect.gen(function* () {
              const failed = yield* (yield* RefStore).setHead("refs/heads/ours").pipe(Effect.flip);
              assert.equal(failed._tag, "StorageFailure");
            }).pipe(Effect.provide(refStore(root))),
          );
          assert.equal(await fs.readFile(path.join(root, "HEAD"), "utf8"), headBefore);
        } finally {
          await fs.unlink(headLock);
        }
        await Effect.runPromise(
          Effect.gen(function* () {
            yield* (yield* RefStore).setHead("refs/heads/ours");
          }).pipe(Effect.provide(refStore(root))),
        );
        assert.equal(git("symbolic-ref", "HEAD").trim(), "refs/heads/ours");
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
