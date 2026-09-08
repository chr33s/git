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

describe.skipIf(!hasGit)("checkout across gitlink removal against Git", () => {
  for (const driver of drivers) {
    for (const kind of ["embedded", "submodule"]) {
      for (const force of [false, true]) {
        it(`${driver} switches away and back with ${kind}, force=${force}`, async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "gitlink-checkout-"));
          try {
            const native = path.join(root, "native");
            const reference = path.join(root, "reference");
            const origin = path.join(root, "origin");
            await fs.mkdir(native);
            await fs.mkdir(origin);
            gitIn(native)("init", "-q", "-b", "main");
            gitIn(origin)("init", "-q", "-b", "main");
            await fs.writeFile(path.join(origin, "file"), "nested content\n");
            gitIn(origin)("add", ".");
            gitIn(origin)("commit", "-qm", "nested");
            if (kind === "submodule")
              gitIn(native)(
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "-q",
                origin,
                "child",
              );
            else gitIn(native)("clone", "-q", origin, "child");
            await fs.writeFile(path.join(native, "a"), "outer content\n");
            gitIn(native)("add", ".");
            gitIn(native)("commit", "-qm", "outer");
            const tree = spawnSync("git", ["-C", native, "mktree"], {
              env: gitEnv,
              input: "",
              encoding: "utf8",
            });
            assert.equal(tree.status, 0, tree.stderr);
            const empty = gitIn(native)("commit-tree", tree.stdout.trim(), "-m", "empty").trim();
            gitIn(native)("branch", "empty", empty);
            await fs.cp(native, reference, { recursive: true });
            const nestedHead = gitIn(path.join(native, "child"))("rev-parse", "HEAD");
            for (const branch of ["empty", "main"]) {
              gitIn(reference)("checkout", "-q", ...(force ? ["--force"] : []), branch);
              if (driver === "library") {
                await Effect.runPromise(
                  Checkout.checkout(branch, { force }).pipe(Effect.provide(layerFor(native))),
                );
              } else {
                const result = spawnSync(
                  driver === "source" ? process.execPath : standalone,
                  [
                    ...(driver === "source" ? [source] : []),
                    "switch",
                    "--work",
                    ".",
                    ...(force ? ["--force"] : []),
                    branch,
                  ],
                  { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
                );
                assert.equal(result.status, 0, result.stdout + result.stderr);
              }
              assert.equal(
                gitIn(native)("symbolic-ref", "HEAD"),
                gitIn(reference)("symbolic-ref", "HEAD"),
              );
              assert.equal(
                gitIn(native)("ls-files", "--stage"),
                gitIn(reference)("ls-files", "--stage"),
              );
              assert.equal(
                gitIn(native)("status", "--porcelain"),
                gitIn(reference)("status", "--porcelain"),
              );
              assert.equal(existsSync(path.join(native, "a")), branch === "main");
              assert.equal(
                await fs.readFile(path.join(native, "child/file"), "utf8"),
                "nested content\n",
              );
              assert.equal(gitIn(path.join(native, "child"))("rev-parse", "HEAD"), nestedHead);
              assert.equal(existsSync(path.join(native, ".git/index.lock")), false);
            }
          } finally {
            await fs.rm(root, { recursive: true, force: true });
          }
        });
      }
    }
  }
});
