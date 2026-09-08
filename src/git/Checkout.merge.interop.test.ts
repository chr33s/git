import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer } from "effect";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";
import * as Checkout from "./Checkout.ts";
import * as Repository from "./Repository.ts";
import { stores } from "./Node.ts";
import { workspace } from "./Work.node.ts";

const source = path.resolve("src/cli/main.ts");
const standalone = path.resolve("dist/sea/git+");
const drivers = ["library", "source", ...(existsSync(standalone) ? ["standalone"] : [])];

describe.skipIf(!hasGit)("checkout abandons Git merge state", () => {
  for (const driver of drivers)
    for (const conflict of ["forced", "refused", "unknown-target"]) {
      it(`${driver} clears merge state only after a successful ${conflict} checkout`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "checkout-merge-"));
        try {
          const git = gitIn(root);
          git("init", "-q", "-b", "main");
          await fs.writeFile(path.join(root, "a"), "base\n");
          git("add", "a");
          git("commit", "--allow-empty", "-qm", "base");
          git("checkout", "-qb", "side");
          await fs.writeFile(path.join(root, "a"), "theirs\n");
          git("add", "a");
          git("commit", "-qm", "theirs");
          git("checkout", "-q", "main");
          await fs.writeFile(path.join(root, "a"), "ours\n");
          git("add", "a");
          git("commit", "-qm", "ours");
          const merge = spawnSync("git", ["merge", "side"], {
            cwd: root,
            env: gitEnv,
            encoding: "utf8",
          });
          assert.equal(merge.status, 1, merge.stdout + merge.stderr);
          const mergeHead = await fs.readFile(path.join(root, ".git/MERGE_HEAD"));
          const index = await fs.readFile(path.join(root, ".git/index"));
          const contents = await fs.readFile(path.join(root, "a"));
          const layer = Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provide(stores(path.join(root, ".git"))),
            Layer.provideMerge(workspace(root)),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(gitEnv))),
          );
          const target = conflict === "unknown-target" ? "missing" : "main";
          if (driver === "library") {
            const result = await Effect.runPromise(
              Checkout.checkout(target, { force: conflict !== "refused" }).pipe(
                Effect.result,
                Effect.provide(layer),
              ),
            );
            assert.equal(result._tag, conflict === "forced" ? "Success" : "Failure");
          } else {
            const result = spawnSync(
              driver === "source" ? process.execPath : standalone,
              [
                ...(driver === "source" ? [source] : []),
                "switch",
                target,
                "--work",
                root,
                ...(conflict === "refused" ? [] : ["--force"]),
              ],
              { env: gitEnv, encoding: "utf8", timeout: 30000 },
            );
            if (conflict === "forced")
              assert.equal(result.status, 0, result.stdout + result.stderr);
            else assert.notEqual(result.status, 0);
          }
          if (conflict !== "forced") {
            assert.deepEqual(await fs.readFile(path.join(root, ".git/MERGE_HEAD")), mergeHead);
            assert.deepEqual(await fs.readFile(path.join(root, ".git/index")), index);
            assert.deepEqual(await fs.readFile(path.join(root, "a")), contents);
          } else {
            for (const name of ["MERGE_HEAD", "MERGE_MSG", "MERGE_MODE", "AUTO_MERGE"])
              assert.equal(existsSync(path.join(root, ".git", name)), false, name);
            assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "ours\n");
            await fs.writeFile(path.join(root, "a"), "next edit\n");
            await Effect.runPromise(Checkout.add(["a"]).pipe(Effect.provide(layer)));
            await Effect.runPromise(
              Checkout.commit({
                message: "next",
                author: { name: "T", email: "t@e.com", at: new Date(1700000000000), offset: 0 },
              }).pipe(Effect.provide(layer)),
            );
            assert.equal(git("show", "-s", "--format=%P", "HEAD").trim().split(" ").length, 1);
          }
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }
});
