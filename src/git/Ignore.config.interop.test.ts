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
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { workspace } from "./Work.node.ts";

const source = path.resolve("src/cli/main.ts");
const standalone = path.resolve("dist/sea/git+");
const drivers = ["library", "source", ...(existsSync(standalone) ? ["standalone"] : [])];

describe.skipIf(!hasGit)("native ignore configuration sources against Git", () => {
  for (const driver of drivers) {
    for (const mode of [
      "global",
      "include",
      "xdg",
      "xdg-config",
      "home-config",
      "system",
      "worktree-config",
      "include-override",
      "local-override",
    ]) {
      it(`${driver} reads ${mode} configuration`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "ignore-config-"));
        try {
          const directory = path.join(root, "work");
          const home = path.join(root, "home");
          const xdg = path.join(root, "xdg");
          await fs.mkdir(directory);
          await fs.mkdir(home);
          await fs.mkdir(path.join(xdg, "git"), { recursive: true });
          gitIn(directory)("init", "-q", "-b", "main");
          const excludes = path.join(root, "configured.ignore");
          await fs.writeFile(excludes, "*.generated\n");
          const config = path.join(root, "global.config");
          await fs.writeFile(config, `[core]\n excludesFile = ${excludes}\n`);
          if (mode === "include" || mode === "include-override")
            gitIn(directory)("config", "include.path", "../../global.config");
          if (mode === "include-override") gitIn(directory)("config", "core.excludesFile", "");
          if (mode === "home-config") await fs.copyFile(config, path.join(home, ".gitconfig"));
          if (mode === "worktree-config") {
            gitIn(directory)("config", "extensions.worktreeConfig", "true");
            await fs.copyFile(config, path.join(directory, ".git/config.worktree"));
          }
          if (mode === "xdg-config") await fs.copyFile(config, path.join(xdg, "git/config"));
          if (mode === "xdg" || mode === "local-override")
            await fs.writeFile(path.join(xdg, "git/ignore"), "*.generated\n");
          if (mode === "local-override") gitIn(directory)("config", "core.excludesFile", "");
          await fs.writeFile(path.join(directory, "bundle.generated"), "output\n");
          await fs.writeFile(path.join(directory, "readme.txt"), "source\n");
          const settings = {
            HOME: home,
            XDG_CONFIG_HOME: xdg,
            GIT_CONFIG_GLOBAL:
              mode === "global" || mode === "local-override"
                ? config
                : mode === "xdg-config" || mode === "home-config"
                  ? undefined
                  : "/dev/null",
            GIT_CONFIG_SYSTEM: mode === "system" ? config : "/dev/null",
            GIT_CONFIG_NOSYSTEM: mode === "system" ? "0" : "1",
          };
          const env = { ...gitEnv, ...settings };
          const reference = spawnSync("git", ["ls-files", "--others", "--exclude-standard"], {
            cwd: directory,
            env,
            encoding: "utf8",
          });
          assert.equal(reference.status, 0, reference.stderr);
          const layer = Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provide(stores(path.join(directory, ".git"))),
            Layer.provideMerge(workspace(directory)),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(settings))),
          );
          if (driver === "library") {
            const status = await Effect.runPromise(Checkout.status().pipe(Effect.provide(layer)));
            assert.deepEqual(
              [...status.untracked].sort(),
              reference.stdout.trimEnd().split("\n").sort(),
            );
            await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layer)));
          } else {
            const result = spawnSync(
              driver === "source" ? process.execPath : standalone,
              [...(driver === "source" ? [source] : []), "add", "--work", ".", "."],
              { cwd: directory, env, encoding: "utf8", timeout: 30_000 },
            );
            assert.equal(result.status, 0, result.stdout + result.stderr);
          }
          assert.equal(gitIn(directory)("ls-files"), reference.stdout);
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }
  }
});
