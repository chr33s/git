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
import { workspace } from "./Work.node.ts";

const standalone = path.resolve("dist/sea/git+");
const source = path.resolve("src/cli/main.ts");
const drivers = ["library", "source", ...(existsSync(standalone) ? ["standalone"] : [])];
const layerFor = (root: string) =>
  Repository.layer.pipe(
    Layer.provide(Repository.hooksNoop),
    Layer.provide(stores(path.join(root, ".git"))),
    Layer.provideMerge(workspace(root, path.join(root, ".git"))),
  );

describe.skipIf(!hasGit)("native gitlink porcelain against Git", () => {
  for (const driver of drivers) {
    for (const state of ["clean", "advanced", "detached", "staged"]) {
      it(`${driver} status handles ${state} submodule HEAD`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "gitlink-status-"));
        try {
          const native = path.join(root, "native");
          const origin = path.join(root, "origin");
          await fs.mkdir(native);
          await fs.mkdir(origin);
          gitIn(native)("init", "-q", "-b", "main");
          gitIn(origin)("init", "-q", "-b", "main");
          await fs.writeFile(path.join(origin, "file"), "first\n");
          gitIn(origin)("add", ".");
          gitIn(origin)("commit", "-qm", "first");
          gitIn(native)(
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            origin,
            "child",
          );
          gitIn(native)("commit", "-qm", "submodule");
          const child = path.join(native, "child");
          if (state !== "clean") {
            await fs.writeFile(path.join(child, "file"), "second\n");
            gitIn(child)("add", ".");
            gitIn(child)("commit", "-qm", "second");
          }
          if (state === "detached") gitIn(child)("checkout", "-q", "--detach");
          if (state === "staged") gitIn(native)("add", "child");
          const expected = gitIn(native)(
            "status",
            "--porcelain",
            "--untracked-files=all",
          ).trimEnd();
          assert.equal(
            expected,
            state === "clean" ? "" : state === "staged" ? "M  child" : " M child",
          );
          const before = await fs.readFile(path.join(native, ".git/index"));
          if (driver === "library") {
            const status = await Effect.runPromise(
              Checkout.status().pipe(Effect.provide(layerFor(native))),
            );
            assert.deepEqual(status.untracked, []);
            assert.deepEqual(
              status.staged,
              state === "staged" ? [{ path: "child", change: "modified" }] : [],
            );
            assert.deepEqual(
              status.unstaged,
              state === "advanced" || state === "detached"
                ? [{ path: "child", change: "modified" }]
                : [],
            );
          } else {
            const result = spawnSync(
              driver === "source" ? process.execPath : standalone,
              [...(driver === "source" ? [source] : []), "status", "--work", "."],
              { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
            );
            assert.equal(result.status, 0, result.stdout + result.stderr);
            assert.equal(
              result.stdout
                .split("\n")
                .filter((line) => !line.startsWith("## "))
                .join("\n")
                .trimEnd(),
              expected,
            );
          }
          assert.deepEqual(await fs.readFile(path.join(native, ".git/index")), before);
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }

    for (const kind of [
      "embedded",
      "submodule",
      "linked",
      "unborn",
      "tracked-files",
      "deinitialized",
    ]) {
      it(`${driver} add handles ${kind} repositories`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "gitlink-interop-"));
        try {
          const native = path.join(root, "native");
          const reference = path.join(root, "reference");
          const origin = path.join(root, "origin");
          await fs.mkdir(native);
          await fs.mkdir(origin);
          gitIn(native)("init", "-q", "-b", "main");
          gitIn(origin)("init", "-q", "-b", "main");
          await fs.writeFile(path.join(origin, "file"), "initial\n");
          gitIn(origin)("add", ".");
          gitIn(origin)("commit", "-qm", "initial");
          const child = path.join(native, "child");
          if (kind === "submodule" || kind === "deinitialized") {
            gitIn(native)(
              "-c",
              "protocol.file.allow=always",
              "submodule",
              "add",
              "-q",
              origin,
              "child",
            );
            gitIn(native)("commit", "-qm", "submodule");
            await fs.writeFile(path.join(child, "file"), "next\n");
            gitIn(child)("add", ".");
            gitIn(child)("commit", "-qm", "next");
            if (kind === "deinitialized") {
              gitIn(native)("submodule", "deinit", "-f", "child");
              await fs.writeFile(path.join(child, "file"), "next\n");
            }
          } else if (kind === "linked") {
            gitIn(origin)("worktree", "add", "-qb", "linked", child);
            gitIn(origin)("pack-refs", "--all");
          } else {
            await fs.mkdir(child);
            await fs.writeFile(path.join(child, "file"), "nested\n");
            if (kind === "tracked-files") {
              gitIn(native)("add", ".");
              gitIn(native)("commit", "-qm", "ordinary directory");
              await fs.writeFile(path.join(child, "new"), "new nested file\n");
            }
            gitIn(child)("init", "-q", "-b", "main");
            if (kind !== "unborn") {
              gitIn(child)("add", ".");
              gitIn(child)("commit", "-qm", "nested");
            }
          }
          await fs.writeFile(path.join(native, "ordinary"), "outer file\n");
          await fs.cp(native, reference, { recursive: true });
          const expected = spawnSync("git", ["-C", reference, "add", "."], {
            env: gitEnv,
            encoding: "utf8",
          });
          assert.equal(expected.status === 0, kind !== "unborn", expected.stderr);
          let succeeded: boolean;
          if (driver === "library") {
            const result = await Effect.runPromiseExit(
              Checkout.add(["."]).pipe(Effect.provide(layerFor(native))),
            );
            succeeded = result._tag === "Success";
          } else {
            const result = spawnSync(
              driver === "source" ? process.execPath : standalone,
              [...(driver === "source" ? [source] : []), "add", "--work", ".", "."],
              { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
            );
            succeeded = result.status === 0;
          }
          assert.equal(succeeded, expected.status === 0);
          assert.equal(
            gitIn(native)("ls-files", "--stage"),
            gitIn(reference)("ls-files", "--stage"),
          );
          if (succeeded) assert.equal(gitIn(native)("write-tree"), gitIn(reference)("write-tree"));
          assert.equal(existsSync(path.join(native, ".git/index.lock")), false);
          assert.equal(
            await fs.readFile(path.join(child, "file"), "utf8"),
            kind === "submodule" || kind === "deinitialized"
              ? "next\n"
              : kind === "linked"
                ? "initial\n"
                : "nested\n",
          );
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }
  }
});
