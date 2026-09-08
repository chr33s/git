import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import { existsSync } from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { describe, it } from "@effect/vitest";

import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { runProcess, sameProcessResult } from "../testing/Process.ts";

const source = path.resolve("src/cli/main.ts");
const sea = path.resolve("dist/sea", process.platform === "win32" ? "git+.exe" : "git+");

describe.skipIf(!hasGit)("global Git path options", () => {
  it("resolves repository and work-tree selectors at the same directory as Git", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-global-paths-"));
    try {
      for (const name of [".", "meta", "sub", "sub/meta"]) {
        const location = path.join(root, name);
        await fs.mkdir(location, { recursive: true });
        gitIn(location)("init", "--bare", "-q", "-b", "main");
        fastImport(
          location,
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: name,
            files: [{ path: "tracked", content: "base\n" }],
          }),
        );
      }
      for (const base of [root, path.join(root, "sub")]) {
        await fs.mkdir(path.join(base, "tree"));
        await fs.writeFile(path.join(base, "tree", "tracked"), "base\n");
        await fs.writeFile(
          path.join(base, "tree", base === root ? "outer" : "inner"),
          "untracked\n",
        );
      }
      await fs.mkdir(path.join(root, "sub", "child"));
      await fs.symlink(path.join(root, "sub", "child"), path.join(root, "link"), "dir");
      for (const selectors of [
        ["--git-dir=meta", "-C", "sub"],
        ["--git-dir", "meta", "-C", "sub"],
        ["--git-dir=meta", "--work-tree=tree", "-C", "sub"],
        ["--git-dir", "meta", "--work-tree", "tree", "-C", "sub"],
        ["--bare", "-C", "sub"],
        ["-C", "sub", "--bare", "-C", ".."],
        ["--bare", "--git-dir=meta", "-C", "sub"],
        ["--git-dir=meta", "--bare", "-C", "sub"],
        ["-C", "link", "-C", ".."],
        ["-C", "link/.."],
        ["-C", "link", "-C", "", "-C", ".."],
        ["--git-dir=link/../meta"],
        ["--git-dir=meta", "--work-tree=link/../tree"],
      ]) {
        const command = selectors.some((option) => option.startsWith("--work-tree"))
          ? ["status", "--porcelain"]
          : ["log", "-1", "--format=%s"];
        const args = [...selectors, ...command];
        const expected = await runProcess({ command: "git", args, cwd: root, env: gitEnv });
        assert.equal(expected.code, 0, expected.stderr.toString());
        for (const executable of [
          { command: process.execPath, prefix: [source] },
          ...(existsSync(sea) ? [{ command: sea, prefix: [] }] : []),
        ]) {
          const actual = await runProcess({
            command: executable.command,
            args: [...executable.prefix, ...args],
            cwd: root,
            env: gitEnv,
          });
          assert.equal(
            sameProcessResult(actual, expected),
            true,
            `${executable.command} ${args.join(" ")}: expected ${expected.stdout.toString()}, got ${actual.stdout.toString()} ${actual.stderr.toString()}`,
          );
        }
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("refuses an invalid intermediate directory before a later directory or mutation", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-invalid-directory-"));
    try {
      const git = gitIn(root);
      git("init", "--bare", "-q", "-b", "main");
      fastImport(
        root,
        importCommit({ branch: "refs/heads/main", mark: 1, message: "main", files: [] }),
      );
      await fs.writeFile(path.join(root, "file"), "a file is not a directory\n");
      for (const directories of [
        ["-C", "missing/.."],
        ["-C", "missing", "-C", root],
        ["-C", "file", "-C", root],
      ]) {
        const args = [...directories, "branch", "unintended"];
        const expected = await runProcess({ command: "git", args, cwd: root, env: gitEnv });
        assert.equal(expected.code, 128);
        const actual = await runProcess({
          command: process.execPath,
          args: [source, ...args],
          cwd: root,
          env: gitEnv,
        });
        assert.equal(actual.code, expected.code, `${args.join(" ")}: ${actual.stderr.toString()}`);
        assert.match(actual.stderr.toString(), /cannot change to/);
        assert.equal(actual.stdout.length, 0);
        assert.equal(git("branch", "--list", "unintended"), "");
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("allows init to create a Git directory that does not exist yet", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-new-selector-"));
    try {
      for (const [name, executable] of [
        ["git", { command: "git", prefix: [] }],
        ["source", { command: process.execPath, prefix: [source] }],
        ...(existsSync(sea) ? [["sea", { command: sea, prefix: [] }] as const] : []),
      ] as const) {
        const cwd = path.join(root, name);
        await fs.mkdir(path.join(cwd, "new"), { recursive: true });
        const result = await runProcess({
          command: executable.command,
          args: [...executable.prefix, "--git-dir=new/metadata", "init", "--bare", "--quiet"],
          cwd,
          env: gitEnv,
        });
        assert.equal(result.code, 0, result.stderr.toString());
        assert.equal(
          gitIn(cwd)("--git-dir=new/metadata", "rev-parse", "--is-bare-repository").trim(),
          "true",
        );
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
