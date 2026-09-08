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

describe.skipIf(!hasGit)("native filemode behavior against Git", () => {
  for (const driver of drivers)
    for (const filemode of [false, true, "included"])
      for (const change of ["mode", "content", "type", "conflict"]) {
        it(`${driver} honors filemode=${filemode} for ${change} changes`, async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "filemode-interop-"));
          try {
            const native = path.join(root, "native"),
              reference = path.join(root, "reference");
            await fs.mkdir(native);
            const git = gitIn(native);
            git("init", "-q", "-b", "main");
            await fs.writeFile(path.join(native, "regular"), "base\n", { mode: 0o644 });
            await fs.writeFile(path.join(native, "executable"), "base\n", { mode: 0o755 });
            await fs.symlink("regular", path.join(native, "link"));
            git("add", ".");
            git("commit", "-qm", "base");
            if (filemode === "included") {
              git("config", "--unset", "core.filemode");
              await fs.writeFile(
                path.join(native, ".git/filemode.config"),
                "[core]\n filemode = false\n",
              );
              git("config", "include.path", "filemode.config");
            } else git("config", "core.filemode", String(filemode));
            await fs.chmod(path.join(native, "regular"), 0o755);
            await fs.chmod(path.join(native, "executable"), 0o644);
            if (change === "content")
              for (const file of ["regular", "executable"])
                await fs.writeFile(path.join(native, file), "changed\n");
            if (change === "type") {
              await fs.unlink(path.join(native, "regular"));
              await fs.symlink("executable", path.join(native, "regular"));
              await fs.unlink(path.join(native, "link"));
              await fs.writeFile(path.join(native, "link"), "now regular\n", { mode: 0o755 });
            }
            await fs.writeFile(path.join(native, "new-executable"), "new\n", { mode: 0o755 });
            if (change === "conflict") {
              const oid = git("rev-parse", "HEAD:regular").trim();
              const result = spawnSync("git", ["update-index", "--index-info"], {
                cwd: native,
                env: gitEnv,
                encoding: "utf8",
                input: `0 ${"0".repeat(40)}\tregular\n100644 ${oid} 1\tregular\n100755 ${oid} 2\tregular\n100644 ${oid} 3\tregular\n`,
              });
              assert.equal(result.status, 0, result.stderr);
              await fs.writeFile(path.join(native, "regular"), "resolved\n");
            }
            await fs.cp(native, reference, { recursive: true, verbatimSymlinks: true });
            const layer = Repository.layer.pipe(
              Layer.provide(Repository.hooksNoop),
              Layer.provide(stores(path.join(native, ".git"))),
              Layer.provideMerge(workspace(native)),
              Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(gitEnv))),
            );
            const status = await Effect.runPromise(Checkout.status().pipe(Effect.provide(layer)));
            assert.deepEqual(
              [...status.unstaged, ...status.unmerged].map((entry) => entry.path).sort(),
              [
                ...new Set(
                  gitIn(reference)("diff", "--name-only").trim().split("\n").filter(Boolean),
                ),
              ].sort(),
            );
            gitIn(reference)("add", ".");
            if (driver === "library") {
              await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layer)));
              await Effect.runPromise(
                Checkout.commit({
                  message: "changed",
                  author: { name: "T", email: "t@e.com", at: new Date(1700000000000), offset: 0 },
                }).pipe(Effect.provide(layer)),
              );
            } else {
              for (const args of [
                ["add", "."],
                ["commit", "--message", "changed"],
              ]) {
                const result = spawnSync(
                  driver === "source" ? process.execPath : standalone,
                  [...(driver === "source" ? [source] : []), ...args, "--work", "."],
                  { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30000 },
                );
                assert.equal(result.status, 0, result.stdout + result.stderr);
              }
            }
            gitIn(reference)("commit", "-qm", "changed");
            assert.equal(git("ls-files", "--stage"), gitIn(reference)("ls-files", "--stage"));
            assert.equal(
              git("rev-parse", "HEAD^{tree}"),
              gitIn(reference)("rev-parse", "HEAD^{tree}"),
            );
            if (driver === "library" && change === "mode") {
              gitIn(reference)("rm", "new-executable");
              await Effect.runPromise(
                Checkout.remove(["new-executable"]).pipe(Effect.provide(layer)),
              );
              assert.equal(git("ls-files", "--stage"), gitIn(reference)("ls-files", "--stage"));
            }
          } finally {
            await fs.rm(root, { recursive: true, force: true });
          }
        });
      }
});
