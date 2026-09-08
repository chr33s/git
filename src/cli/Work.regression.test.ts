import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { setTimeout } from "node:timers/promises";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";

const entry = path.join(import.meta.dirname, "main.ts");
const standalone = path.resolve("dist/sea/git+");
const executables = [
  { command: process.execPath, args: [entry] },
  ...(existsSync(standalone) ? [{ command: standalone, args: [] }] : []),
];
describe.skipIf(!hasGit)("native CLI review regressions", () => {
  let root: string;
  const git = (...args: string[]) => gitIn(root)(...args).trim();
  const cli = (cwd: string, ...args: string[]) =>
    spawnSync(process.execPath, [entry, ...args], { cwd, encoding: "utf8", timeout: 30_000 });
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "native-cli-review-"));
    git("init", "-q", "-b", "main");
    await fs.writeFile(path.join(root, "a"), "base\n");
    git("add", "a");
    git("commit", "-qm", "base");
  });
  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  for (const [number, executable] of executables.entries()) {
    for (const command of ["restore", "mv"]) {
      it(`${number === 0 ? "source" : "standalone"} ${command} refuses unresolved index stages`, async () => {
        git("checkout", "-qb", "side");
        await fs.writeFile(path.join(root, "a"), "side\n");
        git("commit", "-qam", "side");
        git("checkout", "-q", "main");
        await fs.writeFile(path.join(root, "a"), "main\n");
        git("commit", "-qam", "main");
        const merge = spawnSync("git", ["merge", "--no-commit", "side"], {
          cwd: root,
          env: gitEnv,
          encoding: "utf8",
        });
        assert.equal(merge.status, 1, merge.stdout + merge.stderr);
        assert.match(merge.stdout, /CONFLICT/);
        await fs.writeFile(path.join(root, "a"), "partial resolution\n");
        const index = await fs.readFile(path.join(root, ".git/index"));
        const restored = spawnSync(
          executable.command,
          [
            ...executable.args,
            command,
            "--work",
            ".",
            "a",
            ...(command === "mv" ? ["renamed"] : []),
          ],
          { cwd: root, env: gitEnv, encoding: "utf8", timeout: 30_000 },
        );
        assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "partial resolution\n");
        assert.deepEqual(await fs.readFile(path.join(root, ".git/index")), index);
        assert.equal(existsSync(path.join(root, "renamed")), false);
        assert.notEqual(restored.status, 0);
        assert.match(restored.stdout + restored.stderr, /unmerged/);
      });
    }

    it(`${number === 0 ? "source" : "standalone"} restore resolves named and object sources`, async () => {
      git("branch", "source");
      git("tag", "-am", "release", "release");
      await fs.writeFile(path.join(root, "a"), "current\n");
      await fs.writeFile(path.join(root, "new"), "new current\n");
      git("add", ".");
      git("commit", "-qm", "current");
      for (const source of [
        "source",
        "release",
        git("rev-parse", "source"),
        git("rev-parse", "source^{tree}"),
      ]) {
        git("reset", "--hard", "HEAD");
        const index = git("ls-files", "--stage");
        await fs.writeFile(path.join(root, "a"), "local edit\n");
        const restored = spawnSync(
          executable.command,
          [...executable.args, "restore", "--work", ".", "--source", source, "a", "new"],
          { cwd: root, env: gitEnv, encoding: "utf8", timeout: 30_000 },
        );
        assert.equal(restored.status, 0, restored.stdout + restored.stderr);
        assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "base\n");
        assert.equal(existsSync(path.join(root, "new")), false);
        assert.equal(git("ls-files", "--stage"), index);
      }
    });

    it(`${number === 0 ? "source" : "standalone"} restore rejects unmatched paths without overwriting earlier files`, async () => {
      await fs.writeFile(path.join(root, "a"), "local edit\n");
      const restored = spawnSync(
        executable.command,
        [...executable.args, "restore", "--work", ".", "a", "missing"],
        { cwd: root, env: gitEnv, encoding: "utf8", timeout: 30_000 },
      );
      assert.notEqual(restored.status, 0);
      assert.match(restored.stdout + restored.stderr, /not tracked/);
      assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "local edit\n");
    });

    for (const operation of ["status", "switch"] as const) {
      it(`${number === 0 ? "source" : "standalone"} ${operation} preserves edits with the indexed size and mtime`, async () => {
        const file = path.join(root, "a");
        git("checkout", "-qb", "side");
        await fs.writeFile(file, "side\n");
        git("commit", "-qam", "side");
        git("checkout", "-q", "main");

        const native = (...args: string[]) =>
          spawnSync(executable.command, [...executable.args, ...args], {
            cwd: root,
            env: gitEnv,
            encoding: "utf8",
            timeout: 30_000,
          });
        const timestamp = 1_600_000_000;
        await fs.utimes(file, timestamp, timestamp);
        const staged = native("add", "--work", ".", "a");
        assert.equal(staged.status, 0, staged.stdout + staged.stderr);
        const cachedIndex = await fs.readFile(path.join(root, ".git/index"));
        const before = await fs.stat(file, { bigint: true });
        // Git builds that compare whole-second ctimes must also see this
        // change. Wait for that filesystem timestamp boundary, not a fiber.
        await setTimeout(1020 - (Date.now() % 1000));
        await fs.writeFile(file, "edit\n");
        await fs.utimes(file, timestamp, timestamp);
        const after = await fs.stat(file, { bigint: true });
        assert.equal(after.size, before.size);
        assert.equal(after.mtimeNs, before.mtimeNs);
        assert.notEqual(after.ctimeNs, before.ctimeNs);

        assert.equal(git("status", "--porcelain"), "M a");
        const refused = spawnSync("git", ["switch", "side"], {
          cwd: root,
          env: gitEnv,
          encoding: "utf8",
        });
        assert.notEqual(refused.status, 0);
        assert.match(refused.stderr, /would be overwritten/);
        // Git can refresh index metadata while answering status; give the
        // native command exactly the snapshot from before the edit.
        await fs.writeFile(path.join(root, ".git/index"), cachedIndex);

        const result = native(
          operation,
          "--work",
          ".",
          ...(operation === "switch" ? ["side"] : []),
        );
        if (operation === "status") {
          assert.equal(result.status, 0, result.stdout + result.stderr);
          assert.match(result.stdout, /^ M a$/m);
        } else {
          assert.notEqual(result.status, 0, result.stdout + result.stderr);
          assert.match(result.stdout + result.stderr, /unstaged change/);
        }
        assert.equal(await fs.readFile(file, "utf8"), "edit\n");
        assert.equal(git("branch", "--show-current"), "main");
        assert.deepEqual(await fs.readFile(path.join(root, ".git/index")), cachedIndex);
      });
    }
  }

  it("stages and restores paths relative to the invocation directory", async () => {
    await fs.mkdir(path.join(root, "sub"));
    await fs.writeFile(path.join(root, "sub/a"), "base\n");
    git("add", ".");
    git("commit", "-qm", "sub");
    await fs.writeFile(path.join(root, "a"), "root change\n");
    await fs.writeFile(path.join(root, "sub/a"), "sub change\n");
    const added = cli(root, "-C", "sub", "-C", ".", "add", "--work", ".", "a");
    assert.equal(added.status, 0, added.stderr);
    assert.equal(git("diff", "--cached", "--name-only"), "sub/a");
    const restored = cli(path.join(root, "sub"), "restore", "--work", ".", "--staged", "a");
    assert.equal(restored.status, 0, restored.stderr);
    assert.equal(git("diff", "--cached", "--name-only"), "");
    assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "root change\n");
  });

  it("uses the linked index and shared objects and refs when committing", async () => {
    const linked = path.join(root, "linked");
    git("worktree", "add", "-qb", "linked", linked);
    const main = git("rev-parse", "main");
    const status = cli(root, "status", "--work", linked);
    assert.equal(status.status, 0, status.stderr);
    assert.equal(status.stdout.trim(), "## linked");
    await fs.writeFile(path.join(linked, "a"), "linked change\n");
    assert.equal(cli(linked, "add", "--work", ".", "a").status, 0);
    const committed = cli(linked, "commit", "--work", ".", "--message", "linked change");
    assert.equal(committed.status, 0, committed.stderr + committed.stdout);
    assert.equal(git("rev-parse", "main"), main);
    assert.equal(git("show", "linked:a"), "linked change");
    assert.equal(gitIn(linked)("status", "--porcelain").trim(), "");
  });

  for (const selector of ["--work-tree", "GIT_WORK_TREE"] as const) {
    for (const nestedRepository of [false, true]) {
      it(`uses the current repository index with ${selector} (${nestedRepository ? "with" : "without"} its own repository)`, async () => {
        const alternate = path.join(root, "alternate");
        await fs.mkdir(alternate);
        await fs.writeFile(path.join(alternate, "a"), "alternate base\n");
        const alternateGit = gitIn(alternate);
        if (nestedRepository) {
          alternateGit("init", "-q", "-b", "alternate");
          alternateGit("add", "a");
          alternateGit("commit", "-qm", "alternate base");
        }
        await fs.writeFile(path.join(alternate, "a"), "alternate change\n");

        git("--work-tree", alternate, "add", "a");
        const expected = git("show", ":a");
        assert.equal(expected, "alternate change");
        for (const executable of executables) {
          git("reset", "-q", "HEAD", "--", "a");
          const added = spawnSync(
            executable.command,
            [
              ...executable.args,
              ...(selector === "--work-tree" ? [selector, alternate] : []),
              "add",
              "--work",
              ".",
              "a",
            ],
            {
              cwd: root,
              env: selector === "GIT_WORK_TREE" ? { ...gitEnv, GIT_WORK_TREE: alternate } : gitEnv,
              encoding: "utf8",
              timeout: 30_000,
            },
          );
          assert.equal(added.status, 0, added.stderr + added.stdout);
          assert.equal(git("show", ":a"), expected);
          if (nestedRepository) assert.equal(alternateGit("show", ":a").trim(), "alternate base");
          assert.equal(await fs.readFile(path.join(root, "a"), "utf8"), "base\n");
        }
      });
    }
  }

  for (const [number, executable] of executables.entries()) {
    it(`${number === 0 ? "source" : "standalone"} status quotes paths the way git's porcelain does`, async () => {
      // Every class git's own `quote_path` covers: a space, the C escapes, a
      // byte above ASCII, and the newline that ends a porcelain record.
      const names = [
        "plain.txt",
        "with space.txt",
        'quote".txt',
        "back\\slash.txt",
        "tab\there.txt",
        "new\nline.txt",
        "ünï.txt",
      ];
      for (const name of names) await fs.writeFile(path.join(root, name), "content\n");
      git("add", "with space.txt", 'quote".txt');
      await fs.writeFile(path.join(root, "with space.txt"), "changed\n");

      const expected = git("status", "--porcelain", "-uall")
        .split("\n")
        .filter((line) => line !== "")
        .sort();
      const status = spawnSync(executable.command, [...executable.args, "status", "--work", "."], {
        cwd: root,
        env: gitEnv,
        encoding: "utf8",
        timeout: 30_000,
      });
      assert.equal(status.status, 0, status.stderr + status.stdout);
      const actual = status.stdout
        .split("\n")
        .filter((line) => line !== "" && !line.startsWith("## "))
        .sort();
      assert.deepEqual(actual, expected);
    });
  }

  it("returns failure for both replay commands on conflict", async () => {
    git("checkout", "-qb", "side");
    await fs.writeFile(path.join(root, "a"), "side\n");
    git("commit", "-qam", "side");
    git("checkout", "-q", "main");
    await fs.writeFile(path.join(root, "a"), "main\n");
    git("commit", "-qam", "main");
    const main = git("rev-parse", "main");
    for (const command of ["cherry-pick", "rebase"]) {
      const result = cli(root, command, "--root", root, ".git", "side", "--onto", "main");
      assert.equal(result.status, 1, result.stdout + result.stderr);
      assert.match(result.stdout + result.stderr, /conflict/);
      assert.doesNotMatch(result.stdout, /skipped/);
      assert.equal(git("rev-parse", "main"), main);
    }
  });
});
