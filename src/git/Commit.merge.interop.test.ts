import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";
import * as Checkout from "./Checkout.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { isOid } from "./Store.ts";
import { workspace } from "./Work.node.ts";

const source = path.resolve("src/cli/main.ts");
const standalone = path.resolve("dist/sea/git+");
const drivers = ["library", "source", ...(existsSync(standalone) ? ["standalone"] : [])];
const author = { name: "T", email: "t@e.com", at: new Date(1_700_000_000_000), offset: 0 };
const mergeFiles = ["MERGE_HEAD", "MERGE_MSG", "MERGE_MODE", "AUTO_MERGE"];

describe.skipIf(!hasGit)("committing a merge started by Git", () => {
  for (const driver of drivers) {
    for (const scenario of ["resolved", "unchanged", "octopus", "linked", "tagged"]) {
      it(`${driver} completes a ${scenario} merge with its parents and state intact`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "commit-merge-"));
        try {
          const common = path.join(root, "common");
          const reference = path.join(root, "reference");
          await fs.mkdir(common);
          const git = gitIn(common);
          git("init", "-q", "-b", "main");
          await fs.writeFile(path.join(common, "a"), "base\n");
          git("add", ".");
          git("commit", "-qm", "base");
          git("branch", "base");
          git("checkout", "-qb", "side");
          await fs.writeFile(
            path.join(common, scenario === "octopus" ? "side.txt" : "a"),
            "side\n",
          );
          git("add", ".");
          git("commit", "-qm", "side");
          if (scenario === "tagged") git("tag", "-am", "release", "release");
          if (scenario === "octopus") {
            git("checkout", "-qb", "second", "base");
            await fs.writeFile(path.join(common, "second.txt"), "second\n");
            git("add", ".");
            git("commit", "-qm", "second");
          }
          git("checkout", "-q", "main");
          await fs.writeFile(path.join(common, "a"), "main\n");
          git("commit", "-qam", "main");
          await fs.cp(common, reference, { recursive: true });
          const native = scenario === "linked" ? path.join(root, "linked") : common;
          if (scenario === "linked") git("worktree", "add", "-qb", "linked", native);
          const main = git("rev-parse", "main");

          for (const directory of [reference, native]) {
            const merge = spawnSync(
              "git",
              [
                "merge",
                "--no-ff",
                "--no-commit",
                scenario === "tagged" ? "release" : "side",
                ...(scenario === "octopus" ? ["second"] : []),
              ],
              { cwd: directory, env: gitEnv, encoding: "utf8" },
            );
            assert.equal(merge.status, scenario === "octopus" ? 0 : 1, merge.stdout + merge.stderr);
            if (scenario !== "octopus") {
              assert.match(merge.stdout, /CONFLICT/);
              await fs.writeFile(
                path.join(directory, "a"),
                scenario === "unchanged" ? "main\n" : "resolved\n",
              );
              gitIn(directory)("add", "a");
            }
          }
          gitIn(reference)("commit", "-qm", "merge completed");
          const nativeGit = gitIn(native);
          const gitDirectory = nativeGit("rev-parse", "--absolute-git-dir").trim();
          if (driver === "library") {
            const layer = Repository.layer.pipe(
              Layer.provide(Repository.hooksNoop),
              Layer.provide(stores(gitDirectory)),
              Layer.provideMerge(workspace(native, gitDirectory)),
            );
            const pendingHeads = await fs.readFile(path.join(gitDirectory, "MERGE_HEAD"));
            const pendingMessage = await fs.readFile(path.join(gitDirectory, "MERGE_MSG"));
            const head = nativeGit("rev-parse", "HEAD");
            const wrong = nativeGit("rev-parse", "base").trim();
            assert.ok(isOid(wrong));
            const refused = await Effect.runPromise(
              Checkout.commit({ message: "stale expectation", author, expected: wrong }).pipe(
                Effect.result,
                Effect.provide(layer),
              ),
            );
            assert.equal(refused._tag, "Failure");
            if (refused._tag === "Failure") assert.equal(refused.failure._tag, "RefConflict");
            assert.equal(nativeGit("rev-parse", "HEAD"), head);
            assert.deepEqual(
              await fs.readFile(path.join(gitDirectory, "MERGE_HEAD")),
              pendingHeads,
            );
            assert.deepEqual(
              await fs.readFile(path.join(gitDirectory, "MERGE_MSG")),
              pendingMessage,
            );
            await Effect.runPromise(
              Checkout.commit({ message: "merge completed", author }).pipe(Effect.provide(layer)),
            );
          } else {
            const committed = spawnSync(
              driver === "source" ? process.execPath : standalone,
              [
                ...(driver === "source" ? [source] : []),
                "commit",
                "--work",
                ".",
                "--message",
                "merge completed",
              ],
              { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
            );
            assert.equal(committed.status, 0, committed.stdout + committed.stderr);
          }
          assert.equal(
            nativeGit("show", "-s", "--format=%P", "HEAD"),
            gitIn(reference)("show", "-s", "--format=%P", "HEAD"),
          );
          assert.equal(
            nativeGit("rev-parse", "HEAD^{tree}"),
            gitIn(reference)("rev-parse", "HEAD^{tree}"),
          );
          assert.equal(nativeGit("status", "--porcelain").trim(), "");
          for (const name of mergeFiles) {
            assert.equal(existsSync(path.join(reference, ".git", name)), false, `Git kept ${name}`);
            assert.equal(
              existsSync(path.join(gitDirectory, name)),
              false,
              `native commit kept ${name}`,
            );
          }
          if (scenario === "linked") assert.equal(git("rev-parse", "main"), main);
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }
  }
});
