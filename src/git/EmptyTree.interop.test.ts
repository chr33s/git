import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { hasGit, gitIn } from "../testing/Git.ts";
import { EMPTY_TREE_OID } from "./Format.ts";
import { initializeBare, stores } from "./Node.ts";
import * as Repository from "./Repository.ts";

describe.skipIf(!hasGit)("empty-tree commit integrity", () => {
  it("materializes the canonical tree before acknowledging a commit", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "empty-tree-commit-"));
    try {
      await Effect.runPromise(initializeBare(root));
      const oid = await Effect.runPromise(
        Repository.Repository.use((repository) =>
          repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "empty",
            author: { name: "Test", email: "test@example.com", at: new Date(0), offset: 0 },
          }),
        ).pipe(
          Effect.provide(
            Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(root))),
          ),
        ),
      );
      assert.equal(gitIn(root)("rev-parse", "main").trim(), oid);
      gitIn(root)("fsck", "--strict");
      assert.equal(gitIn(root)("cat-file", "-t", EMPTY_TREE_OID).trim(), "tree");
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("does not publish a ref when the empty tree cannot be synced, and a retry recovers", async () => {
    const script = `
      import assert from "node:assert/strict";
      import fs from "node:fs/promises";
      import { syncBuiltinESMExports } from "node:module";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { Effect, Layer } from "effect";
      import { initializeBare, stores } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
      import * as Repository from ${JSON.stringify(new URL("./Repository.ts", import.meta.url).href)};
      import { EMPTY_TREE_OID } from ${JSON.stringify(new URL("./Format.ts", import.meta.url).href)};
      import { gitIn } from ${JSON.stringify(new URL("../testing/Git.ts", import.meta.url).href)};
      const root = await fs.realpath(await fs.mkdtemp(join(tmpdir(), "empty-tree-sync-")));
      const original = fs.open;
      let injected = false;
      let broken = true;
      const prefix = join(root, "objects", EMPTY_TREE_OID.slice(0, 2), EMPTY_TREE_OID.slice(2));
      fs.open = async (...args) => {
        const file = await original(...args);
        if (broken && (args[0] === prefix || (args[0].startsWith(prefix + ".") && args[0].endsWith(".tmp")))) {
          file.sync = async () => {
            injected = true;
            throw Object.assign(new Error("fixture sync failure"), { code: "EIO" });
          };
        }
        return file;
      };
      syncBuiltinESMExports();
      try {
        await Effect.runPromise(initializeBare(root));
        const commit = Repository.Repository.use(repository => repository.commit({ branch: "main", tree: EMPTY_TREE_OID, message: "empty", author: { name: "Test", email: "test@example.com", at: new Date(0), offset: 0 } })).pipe(Effect.provide(Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(root)))));
        const result = await Effect.runPromise(Effect.result(commit));
        assert.equal(injected, true);
        assert.equal(result._tag, "Failure");
        assert.equal(result.failure._tag, "StorageFailure");
        assert.equal(gitIn(root)("for-each-ref", "--format=%(refname)").trim(), "");
        assert.equal((await fs.readdir(root, { recursive: true })).some(name => name.endsWith(".tmp") || name.endsWith(".lock")), false);
        broken = false;
        await Effect.runPromise(commit);
        const first = gitIn(root)("rev-parse", "main").trim();
        injected = false;
        broken = true;
        const repeated = await Effect.runPromise(Effect.result(commit));
        assert.equal(injected, true, "existing tree writes must also complete their sync");
        assert.equal(repeated._tag, "Failure");
        assert.equal(gitIn(root)("rev-parse", "main").trim(), first);
        broken = false;
        await Effect.runPromise(commit);
        assert.notEqual(gitIn(root)("rev-parse", "main").trim(), first);
        gitIn(root)("fsck", "--strict");
        console.log("empty tree sync verified");
      } finally {
        fs.open = original;
        syncBuiltinESMExports();
        await fs.rm(root, { recursive: true, force: true });
      }
    `;
    const result = await promisify(execFile)(
      process.execPath,
      ["--input-type=module", "-e", script],
      { timeout: 20_000 },
    );
    assert.match(result.stdout, /empty tree sync verified/);
  });
});
