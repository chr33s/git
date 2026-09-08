import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";

const sea = resolve("dist/sea", process.platform === "win32" ? "git+.exe" : "git+");

for (const driver of ["source", "standalone"] as const) {
  describe.skipIf(!hasGit || (driver === "standalone" && !existsSync(sea)))(
    `native CLI diff (${driver})`,
    () => {
      it.effect("applies mode changes and file creation/deletion in both directions", () =>
        Effect.sync(() => {
          const root = mkdtempSync(join(tmpdir(), "cli-diff-metadata-"));
          try {
            const directory = join(root, "repo");
            gitIn(root)("init", "--initial-branch=main", directory);
            const git = gitIn(directory);
            writeFileSync(join(directory, "mode"), "same content\n");
            writeFileSync(join(directory, "binary-mode"), "binary\0content");
            writeFileSync(join(directory, "type-change"), "target");
            writeFileSync(join(directory, "deleted empty"), "");
            writeFileSync(join(directory, "deleted.txt"), "removed\n");
            git("add", ".");
            git("commit", "-m", "before");
            const before = git("rev-parse", "HEAD").trim();
            git("rm", "deleted empty", "deleted.txt");
            writeFileSync(join(directory, "new\tempty"), "");
            writeFileSync(join(directory, "new.txt"), "created\n");
            git("add", ".");
            git("update-index", "--chmod=+x", "mode");
            git("update-index", "--chmod=+x", "binary-mode");
            const target = git("rev-parse", "HEAD:type-change").trim();
            git("update-index", "--cacheinfo", `120000,${target},type-change`);
            git("commit", "-m", "after");
            const after = git("rev-parse", "HEAD").trim();
            gitIn(root)("clone", "--bare", directory, join(root, "stored"));
            for (const [from, to] of [
              [before, after],
              [after, before],
            ] as const) {
              const args = ["diff", "--root", root, "stored", from, to];
              const patch = execFileSync(
                driver === "source" ? process.execPath : sea,
                driver === "source" ? [resolve("src/cli/main.ts"), ...args] : args,
                { env: gitEnv, encoding: "utf8" },
              );
              git("checkout", "--force", "--detach", from);
              execFileSync("git", ["apply", "--index", "-"], {
                cwd: directory,
                env: gitEnv,
                input: patch,
              });
              assert.equal(git("write-tree").trim(), git("rev-parse", `${to}^{tree}`).trim());
            }
          } finally {
            rmSync(root, { recursive: true, force: true });
          }
        }),
      );

      it.effect("preserves trailing patch whitespace when Git applies its output", () =>
        Effect.sync(() => {
          const root = mkdtempSync(join(tmpdir(), "cli-diff-"));
          try {
            const directory = join(root, "repo");
            gitIn(root)("init", "--initial-branch=main", directory);
            const git = gitIn(directory);
            const endings = ["\n\n", "\nshared  \n", "\r\nshared\r\n"];
            for (const [index, ending] of endings.entries()) {
              writeFileSync(join(directory, `${index}.txt`), `before${ending}`);
            }
            git("add", ".");
            git("commit", "-m", "before");
            const before = git("rev-parse", "HEAD").trim();
            for (const [index, ending] of endings.entries()) {
              writeFileSync(join(directory, `${index}.txt`), `after${ending}`);
            }
            git("add", ".");
            git("commit", "-m", "after");
            const after = git("rev-parse", "HEAD").trim();
            gitIn(root)("clone", "--bare", directory, join(root, "stored"));
            const args = ["diff", "--root", root, "stored", before, after];
            const patch = execFileSync(
              driver === "source" ? process.execPath : sea,
              driver === "source" ? [resolve("src/cli/main.ts"), ...args] : args,
              { env: gitEnv, encoding: "utf8" },
            );
            git("checkout", "--detach", before);
            execFileSync("git", ["apply", "--whitespace=nowarn", "-"], {
              cwd: directory,
              env: gitEnv,
              input: patch,
            });
            for (const [index, ending] of endings.entries()) {
              assert.equal(readFileSync(join(directory, `${index}.txt`), "utf8"), `after${ending}`);
            }
          } finally {
            rmSync(root, { recursive: true, force: true });
          }
        }),
      );
    },
  );
}
