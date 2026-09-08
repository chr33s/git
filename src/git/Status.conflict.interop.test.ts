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

describe.skipIf(!hasGit)("native conflict status against Git", () => {
  it("matches Git for all seven combinations of unresolved index stages", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "status-stages-"));
    try {
      const git = gitIn(root);
      git("init", "-q", "-b", "main");
      await fs.writeFile(path.join(root, "base"), "base\n");
      git("add", "base");
      git("commit", "-qm", "base");
      const oid = git("rev-parse", "HEAD:base").trim();
      let input = "";
      for (let mask = 1; mask <= 7; mask += 1) {
        await fs.writeFile(path.join(root, `a${mask}`), "base\n");
        for (let stage = 1; stage <= 3; stage += 1) {
          if ((mask & (1 << (stage - 1))) !== 0) input += `100644 ${oid} ${stage}\ta${mask}\n`;
        }
      }
      const update = spawnSync("git", ["update-index", "--index-info"], {
        cwd: root,
        env: gitEnv,
        encoding: "utf8",
        input,
      });
      assert.equal(update.status, 0, update.stderr);
      const layer = Repository.layer.pipe(
        Layer.provide(Repository.hooksNoop),
        Layer.provide(stores(path.join(root, ".git"))),
        Layer.provideMerge(workspace(root)),
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(gitEnv))),
      );
      const current = await Effect.runPromise(Checkout.status().pipe(Effect.provide(layer)));
      assert.equal(current.unmerged.length, 7);
      assert.equal(
        current.unmerged.map((entry) => `${entry.status} ${entry.path}`).join("\n"),
        git("status", "--porcelain").trim(),
      );
      assert.deepEqual(current.staged, []);
      assert.deepEqual(current.unstaged, []);
      assert.deepEqual(current.untracked, []);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  for (const driver of drivers)
    for (const conflict of ["content", "add/add", "ours-deleted", "theirs-deleted"]) {
      it(`${driver} reports ${conflict} until the resolution is staged`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "status-conflict-"));
        try {
          const git = gitIn(root);
          git("init", "-q", "-b", "main");
          if (conflict !== "add/add") {
            await fs.writeFile(path.join(root, "a"), "base\n");
            git("add", "a");
          }
          git("commit", "--allow-empty", "-qm", "base");
          git("checkout", "-qb", "side");
          if (conflict === "theirs-deleted") git("rm", "a");
          else {
            await fs.writeFile(path.join(root, "a"), "theirs\n");
            git("add", "a");
          }
          git("commit", "-qm", "theirs");
          git("checkout", "-q", "main");
          if (conflict === "ours-deleted") git("rm", "a");
          else {
            await fs.writeFile(path.join(root, "a"), "ours\n");
            git("add", "a");
          }
          git("commit", "-qm", "ours");
          const merge = spawnSync("git", ["merge", "side"], {
            cwd: root,
            env: gitEnv,
            encoding: "utf8",
          });
          assert.equal(merge.status, 1, merge.stdout + merge.stderr);
          const expected = git("status", "--porcelain").trim();
          const index = await fs.readFile(path.join(root, ".git/index"));
          const layer = Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provide(stores(path.join(root, ".git"))),
            Layer.provideMerge(workspace(root)),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(gitEnv))),
          );
          const status = async () => {
            if (driver === "library") {
              const current = await Effect.runPromise(
                Checkout.status().pipe(Effect.provide(layer)),
              );
              assert.deepEqual(current.staged, []);
              assert.deepEqual(current.unstaged, []);
              assert.deepEqual(current.untracked, []);
              assert.deepEqual(current.unmerged, [{ path: "a", status: expected.slice(0, 2) }]);
            } else {
              const result = spawnSync(
                driver === "source" ? process.execPath : standalone,
                [...(driver === "source" ? [source] : []), "status", "--work", root],
                { env: gitEnv, encoding: "utf8", timeout: 30000 },
              );
              assert.equal(result.status, 0, result.stderr);
              assert.equal(
                result.stdout
                  .trim()
                  .split("\n")
                  .filter((line) => !line.startsWith("## "))
                  .join("\n"),
                expected,
              );
            }
          };
          await status();
          await fs.writeFile(path.join(root, "a"), "resolved\n");
          await status();
          assert.deepEqual(await fs.readFile(path.join(root, ".git/index")), index);
          const refused = await Effect.runPromise(
            Checkout.checkout("side").pipe(Effect.result, Effect.provide(layer)),
          );
          assert.equal(refused._tag, "Failure");
          assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "resolved\n");
          assert.deepEqual(await fs.readFile(path.join(root, ".git/index")), index);
          git("add", "a");
          assert.deepEqual(
            (await Effect.runPromise(Checkout.status().pipe(Effect.provide(layer)))).unmerged,
            [],
          );
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }
});
