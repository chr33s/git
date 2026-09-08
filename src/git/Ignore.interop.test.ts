import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";
import * as Checkout from "./Checkout.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { workspace } from "./Work.node.ts";

const standalone = path.resolve("dist/sea/git+");
const source = path.resolve("src/cli/main.ts");
const drivers = ["library", "source", ...(existsSync(standalone) ? ["standalone"] : [])];
const layerFor = (root: string, gitDirectory = path.join(root, ".git")) =>
  Repository.layer.pipe(
    Layer.provide(Repository.hooksNoop),
    Layer.provide(stores(gitDirectory)),
    Layer.provideMerge(workspace(root, gitDirectory)),
  );

describe.skipIf(!hasGit)("native ignored paths against Git", () => {
  let root: string;
  let native: string;
  let reference: string;
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "ignore-interop-"));
    native = path.join(root, "native");
    reference = path.join(root, "reference");
    await fs.mkdir(native);
    const git = gitIn(native);
    git("init", "-q", "-b", "main");
    const write = async (name: string, content: string) => {
      await fs.mkdir(path.dirname(path.join(native, name)), { recursive: true });
      await fs.writeFile(path.join(native, name), content);
    };
    await write("build/kept.txt", "tracked\n");
    await write("sub/kept.tmp", "tracked\n");
    git("add", ".");
    git("commit", "-qm", "tracked files");
    await write(
      ".gitignore",
      "build/\n*.tmp\n!/keep.tmp\n/root-only.txt\n**/cache/**\n\\#literal\n\\!literal\nspace\\ \n!info-reincluded\n",
    );
    await write("sub/.gitignore", "!wanted.tmp\nnested-only.txt\n");
    await write(".git/info/exclude", "local-output\ninfo-reincluded\n");
    for (const name of [
      "build/kept.txt",
      "sub/kept.tmp",
      "build/output.js",
      "sub/wanted.tmp",
      "sub/other.tmp",
      "keep.tmp",
      "sub/keep.tmp",
      "root-only.txt",
      "sub/root-only.txt",
      "deep/cache/generated.js",
      "#literal",
      "!literal",
      "space ",
      "local-output",
      "info-reincluded",
      "sub/nested-only.txt",
      "plain.txt",
    ]) {
      await write(name, "changed\n");
    }
    await fs.cp(native, reference, { recursive: true });
  });
  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  for (const insensitive of [false, true]) {
    it(`uses repository ignorecase=${insensitive}`, async () => {
      gitIn(native)("config", "core.ignorecase", String(insensitive));
      await fs.appendFile(path.join(native, ".gitignore"), "UPPER.txt\n");
      await fs.writeFile(path.join(native, "upper.txt"), "case fixture\n");
      const status = await Effect.runPromise(
        Checkout.status().pipe(Effect.provide(layerFor(native))),
      );
      assert.equal(status.untracked.includes("upper.txt"), !insensitive);
      assert.deepEqual(
        status.untracked,
        gitIn(native)("ls-files", "--others", "--exclude-standard").trimEnd().split("\n"),
      );
    });
  }

  it("combines parent negation across exclude files and scopes nested basename rules", async () => {
    await fs.appendFile(path.join(native, ".git/info/exclude"), "shared/\n");
    await fs.appendFile(path.join(native, ".gitignore"), "!shared/\n");
    await fs.appendFile(path.join(native, "sub/.gitignore"), "deep-only.txt/ \n!other.tmp\n");
    for (const name of [
      "shared/kept.txt",
      "sub/deep/deep-only.txt/output",
      "deep-only.txt/output",
    ]) {
      await fs.mkdir(path.dirname(path.join(native, name)), { recursive: true });
      await fs.writeFile(path.join(native, name), "fixture\n");
    }
    const status = await Effect.runPromise(
      Checkout.status().pipe(Effect.provide(layerFor(native))),
    );
    assert.deepEqual(
      status.untracked,
      gitIn(native)("ls-files", "--others", "--exclude-standard").trimEnd().split("\n"),
    );
    assert.ok(status.untracked.includes("shared/kept.txt"));
    assert.ok(status.untracked.includes("deep-only.txt/output"));
    assert.ok(!status.untracked.includes("sub/deep/deep-only.txt/output"));
  });

  it("uses the common repository excludes in a linked worktree", async () => {
    const linked = path.join(root, "linked");
    gitIn(native)("worktree", "add", "-qb", "linked", linked);
    await fs.writeFile(path.join(linked, "local-output"), "ignored\n");
    await fs.writeFile(path.join(linked, "plain.txt"), "visible\n");
    const git = gitIn(linked);
    const status = await Effect.runPromise(
      Checkout.status().pipe(
        Effect.provide(layerFor(linked, git("rev-parse", "--absolute-git-dir").trim())),
      ),
    );
    assert.deepEqual(status.untracked, ["plain.txt"]);
    assert.deepEqual(
      status.untracked,
      git("ls-files", "--others", "--exclude-standard").trimEnd().split("\n"),
    );
  });

  it("stages a tracked file's deletion when it becomes an ignored directory", async () => {
    for (const directory of [native, reference]) {
      const file = path.join(directory, "build/kept.txt");
      await fs.rm(file);
      await fs.mkdir(file);
      await fs.writeFile(path.join(file, "output"), "generated\n");
    }
    const status = await Effect.runPromise(
      Checkout.status().pipe(Effect.provide(layerFor(native))),
    );
    assert.ok(
      status.unstaged.some(
        (change) => change.path === "build/kept.txt" && change.change === "deleted",
      ),
    );
    gitIn(reference)("add", ".");
    await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layerFor(native))));
    assert.equal(gitIn(native)("ls-files", "--stage"), gitIn(reference)("ls-files", "--stage"));
  });

  it("accepts add-dot when every untracked file is ignored", async () => {
    const empty = path.join(root, "empty");
    await fs.mkdir(empty);
    gitIn(empty)("init", "-q", "-b", "main");
    await fs.writeFile(path.join(empty, ".git/info/exclude"), "*\n");
    await fs.writeFile(path.join(empty, "output"), "generated\n");
    gitIn(empty)("add", ".");
    assert.deepEqual(
      await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layerFor(empty)))),
      [],
    );
    assert.equal(gitIn(empty)("ls-files"), "");
  });

  for (const driver of drivers) {
    for (const location of ["absolute", "relative", "quoted", "tilde", "missing"]) {
      it(`${driver} honors ${location} configured excludes and higher-precedence rules`, async () => {
        for (const directory of [native, reference]) {
          const file = location === "quoted" ? 'rules #; "quoted" \\file' : "configured-excludes";
          const absolute = path.join(directory, ".git", file);
          if (location !== "missing") await fs.writeFile(absolute, "*.generated\nsub/kept.tmp\n");
          gitIn(directory)(
            "config",
            "core.excludesFile",
            location === "relative"
              ? `.git/${file}`
              : location === "tilde"
                ? `~/${path.relative(os.homedir(), absolute)}`
                : absolute,
          );
          await fs.appendFile(
            path.join(directory, ".git/info/exclude"),
            "!info-visible.generated\n",
          );
          await fs.appendFile(path.join(directory, ".gitignore"), "!root-visible.generated\n");
          for (const name of [
            "hidden.generated",
            "info-visible.generated",
            "root-visible.generated",
          ])
            await fs.writeFile(path.join(directory, name), "generated output\n");
        }
        const status = await Effect.runPromise(
          Checkout.status().pipe(Effect.provide(layerFor(native))),
        );
        assert.deepEqual(
          [...status.untracked].sort(),
          gitIn(reference)("ls-files", "--others", "--exclude-standard")
            .trimEnd()
            .split("\n")
            .sort(),
        );
        gitIn(reference)("add", ".");
        if (driver === "library")
          await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layerFor(native))));
        else {
          const result = spawnSync(
            driver === "source" ? process.execPath : standalone,
            [...(driver === "source" ? [source] : []), "add", "--work", ".", "."],
            { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
          );
          assert.equal(result.status, 0, result.stdout + result.stderr);
        }
        assert.equal(gitIn(native)("ls-files", "--stage"), gitIn(reference)("ls-files", "--stage"));
      });
    }

    it(`${driver} add preserves literal directory names in nested ignore rules`, async () => {
      for (const directory of [native, reference]) {
        for (const prefix of ["[id]", "[...slug]/child", "a*b", "a?b", "back\\slash"]) {
          const nested = path.join(directory, prefix);
          await fs.mkdir(nested, { recursive: true });
          await fs.writeFile(path.join(nested, ".gitignore"), "output\n*.log\n!keep.log\n");
          for (const name of ["output", "generated.log", "keep.log", "visible.txt"]) {
            await fs.writeFile(path.join(nested, name), "fixture\n");
          }
        }
      }
      gitIn(reference)("add", ".");
      if (driver === "library") {
        await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layerFor(native))));
      } else {
        const result = spawnSync(
          driver === "source" ? process.execPath : standalone,
          [...(driver === "source" ? [source] : []), "add", "--work", ".", "."],
          { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
        );
        assert.equal(result.status, 0, result.stdout + result.stderr);
      }
      assert.equal(gitIn(native)("ls-files", "--stage"), gitIn(reference)("ls-files", "--stage"));
    });

    for (const operation of ["status", "add"] as const) {
      it(`${driver} ${operation} respects ignored paths while retaining tracked modifications`, async () => {
        const git = gitIn(reference);
        const expectedStatus = git("status", "--porcelain", "--untracked-files=all")
          .trimEnd()
          .split("\n")
          .sort();
        if (operation === "add") git("add", ".");
        if (driver === "library") {
          if (operation === "add")
            await Effect.runPromise(Checkout.add(["."]).pipe(Effect.provide(layerFor(native))));
          else {
            const status = await Effect.runPromise(
              Checkout.status().pipe(Effect.provide(layerFor(native))),
            );
            assert.deepEqual(status.staged, []);
            assert.deepEqual(status.unstaged.map(({ path }) => path).sort(), [
              "build/kept.txt",
              "sub/kept.tmp",
            ]);
            assert.deepEqual(
              status.untracked,
              git("ls-files", "--others", "--exclude-standard").trimEnd().split("\n"),
            );
          }
        } else {
          const result = spawnSync(
            driver === "source" ? process.execPath : standalone,
            [
              ...(driver === "source" ? [source] : []),
              operation,
              "--work",
              ".",
              ...(operation === "add" ? ["."] : []),
            ],
            { cwd: native, env: gitEnv, encoding: "utf8", timeout: 30_000 },
          );
          assert.equal(result.status, 0, result.stdout + result.stderr);
          if (operation === "status")
            assert.deepEqual(
              result.stdout
                .trimEnd()
                .split("\n")
                .filter((line) => !line.startsWith("## "))
                .sort(),
              expectedStatus,
            );
        }
        if (operation === "add")
          assert.equal(gitIn(native)("ls-files", "--stage"), git("ls-files", "--stage"));
      });
    }
  }
});
