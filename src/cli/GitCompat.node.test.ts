import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { coreCompatibility, manifestProblems } from "./GitCompat.ts";
import {
  discoverRepository,
  isCoreCompatibilityInvocation,
  parseInvocation,
} from "./GitCompat.node.ts";

const input = (argv: ReadonlyArray<string>, cwd = "/workspace") => ({
  argv,
  cwd,
  environment: {},
});

describe("Git-compatible invocation", () => {
  it("keeps the manifest free of unimplemented compatibility claims", () => {
    assert.deepEqual(manifestProblems(coreCompatibility), []);
  });

  it("consumes repeated -C and all declared global options before the command", () => {
    const parsed = parseInvocation(
      input([
        "-C",
        "project",
        "-C",
        "nested",
        "--git-dir=meta",
        "--work-tree",
        "tree",
        "-c",
        "core.abbrev=12",
        "-ccolor.ui=false",
        "--bare",
        "--no-pager",
        "status",
        "--short",
      ]),
    );
    if (parsed._tag === "InvalidInvocation") assert.fail(parsed.message);
    assert.deepEqual(parsed.invocation, {
      argv: ["status", "--short"],
      cwd: "/workspace/project/nested",
      gitDir: "/workspace/project/nested/meta",
      workTree: "/workspace/project/nested/tree",
      config: ["core.abbrev=12", "color.ui=false"],
      bare: true,
      noPager: true,
    });
  });

  describe("which implementation a shared command name reaches", () => {
    const dispatch = (argv: ReadonlyArray<string>) => {
      const parsed = parseInvocation(input(argv));
      if (parsed._tag === "InvalidInvocation") assert.fail(parsed.message);
      return isCoreCompatibilityInvocation(parsed.invocation) ? "git" : "extension";
    };

    it("routes a flag only the extension's fetch accepts to the extension", () => {
      // `--root` has a default, so the extension's own `fetch` is a complete
      // invocation with no selector in it, and every flag-bearing spelling was
      // handed to stock git — which reads `<repo> <url>` as a remote and a
      // refspec rather than as the two arguments the extension declares.
      assert.equal(
        dispatch(["fetch", "--token", "abc", "myrepo", "https://example.com/r"]),
        "extension",
      );
      assert.equal(
        dispatch(["fetch", "--branch=main", "myrepo", "https://example.com/r"]),
        "extension",
      );
      assert.equal(
        dispatch(["pull", "--token=abc", "myrepo", "https://example.com/r", "main"]),
        "extension",
      );
    });

    it("leaves a per-command selector out of every other command", () => {
      // `--branch` is stock git's own flag on `clone`; selecting the extension
      // on it globally would take a plain clone away from the git that owns it.
      assert.equal(dispatch(["clone", "--branch", "main", "https://example.com/r"]), "git");
      assert.equal(dispatch(["log", "--branch", "main"]), "git");
    });

    it("still answers the global selectors, spaced or attached", () => {
      assert.equal(dispatch(["log", "--root", "/srv"]), "extension");
      assert.equal(dispatch(["log", "--root=/srv"]), "extension");
      assert.equal(dispatch(["merge", "--strategy=ours"]), "extension");
    });

    it("does not read a selector out of a flag's value", () => {
      assert.equal(dispatch(["commit", "-m", "--root cause"]), "git");
      assert.equal(dispatch(["status", "--", "--root"]), "git");
    });

    it("hands an unadorned core command to git", () => {
      assert.equal(dispatch(["status", "--short"]), "git");
      assert.equal(dispatch(["fetch", "origin", "main"]), "git");
    });
  });

  it("uses explicit Git selectors before environment selectors", () => {
    const fromEnvironment = parseInvocation({
      argv: ["status"],
      cwd: "/workspace",
      environment: { GIT_DIR: "metadata", GIT_WORK_TREE: "tree" },
    });
    if (fromEnvironment._tag === "InvalidInvocation") assert.fail(fromEnvironment.message);
    assert.equal(fromEnvironment.invocation.gitDir, "/workspace/metadata");
    assert.equal(fromEnvironment.invocation.workTree, "/workspace/tree");

    const explicit = parseInvocation({
      argv: ["--git-dir", "other", "--work-tree", "other-tree", "status"],
      cwd: "/workspace",
      environment: { GIT_DIR: "metadata", GIT_WORK_TREE: "tree" },
    });
    if (explicit._tag === "InvalidInvocation") assert.fail(explicit.message);
    assert.equal(explicit.invocation.gitDir, "/workspace/other");
    assert.equal(explicit.invocation.workTree, "/workspace/other-tree");
  });

  it("keeps command-local flags untouched and reports incomplete global options", () => {
    const commandFlag = parseInvocation(input(["status", "--short"]));
    assert.equal(commandFlag._tag, "Invocation");
    if (commandFlag._tag === "Invocation") {
      assert.deepEqual(commandFlag.invocation.argv, ["status", "--short"]);
    }

    const missing = parseInvocation(input(["--git-dir"]));
    assert.deepEqual(missing, {
      _tag: "InvalidInvocation",
      message: "--git-dir requires a value",
    });
  });

  it.effect("discovers a .git directory, gitdir file and bare repository", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-compat-"));
      try {
        const workTree = path.join(root, "work");
        const common = path.join(root, "common");
        const bare = path.join(root, "bare.git");
        await fs.mkdir(path.join(workTree, "child"), { recursive: true });
        await fs.mkdir(path.join(common, "objects"), { recursive: true });
        await fs.mkdir(path.join(common, "refs"), { recursive: true });
        await fs.writeFile(path.join(workTree, ".git"), "gitdir: ../common\n");
        await fs.mkdir(path.join(bare, "objects"), { recursive: true });
        await fs.mkdir(path.join(bare, "refs"), { recursive: true });
        await fs.writeFile(path.join(bare, "HEAD"), "ref: refs/heads/main\n");

        const fromWorkTree = parseInvocation(input(["status"], path.join(workTree, "child")));
        if (fromWorkTree._tag === "InvalidInvocation") assert.fail(fromWorkTree.message);
        assert.deepEqual(await Effect.runPromise(discoverRepository(fromWorkTree.invocation)), {
          gitDir: common,
          workTree,
        });

        const fromBare = parseInvocation(input(["log"], bare));
        if (fromBare._tag === "InvalidInvocation") assert.fail(fromBare.message);
        assert.deepEqual(await Effect.runPromise(discoverRepository(fromBare.invocation)), {
          gitDir: bare,
          workTree: null,
        });
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
