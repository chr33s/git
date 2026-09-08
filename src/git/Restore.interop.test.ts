import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
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

const destinations = [
  { name: "worktree", flags: ["--worktree"], options: { worktree: true } },
  { name: "index", flags: ["--staged"], options: { staged: true } },
  {
    name: "both",
    flags: ["--staged", "--worktree"],
    options: { staged: true, worktree: true },
  },
];

const layerFor = (root: string) =>
  Repository.layer.pipe(
    Layer.provide(Repository.hooksNoop),
    Layer.provide(stores(path.join(root, ".git"))),
    Layer.provideMerge(workspace(root)),
  );

const snapshot = async (root: string) => ({
  index: gitIn(root)("ls-files", "--stage"),
  files: await Promise.all(
    ["a", "new", "old"].map(async (name) => {
      try {
        return await fs.readFile(path.join(root, name), "utf8");
      } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null;
        throw cause;
      }
    }),
  ),
  head: gitIn(root)("rev-parse", "HEAD"),
});

describe.skipIf(!hasGit)("restore against Git", () => {
  let root: string;
  let native: string;
  let reference: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "restore-interop-"));
    native = path.join(root, "native");
    reference = path.join(root, "reference");
    await fs.mkdir(native);
    const git = gitIn(native);
    git("init", "-q", "-b", "main");
    await fs.writeFile(path.join(native, "a"), "source\n");
    await fs.writeFile(path.join(native, "old"), "old source\n");
    git("add", ".");
    git("commit", "-qm", "source");
    git("branch", "source");
    git("tag", "-am", "source tag", "source-tag");
    git("tag", "-am", "source tree", "source-tree", "HEAD^{tree}");
    await fs.writeFile(path.join(native, "a"), "current\n");
    await fs.writeFile(path.join(native, "new"), "new current\n");
    git("rm", "old");
    git("add", ".");
    git("commit", "-qm", "current");
    await fs.writeFile(path.join(native, "a"), "local edit\n");
    await fs.writeFile(path.join(native, "new"), "new local edit\n");
    await fs.cp(native, reference, { recursive: true });
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const conflict = async (kind: "modify/modify" | "add/add" = "modify/modify") => {
    const git = gitIn(native);
    const file = kind === "add/add" ? "old" : "a";
    git("reset", "--hard", "main");
    git("checkout", "-qb", "side", kind === "add/add" ? "main" : "source");
    await fs.writeFile(path.join(native, file), "side\n");
    git("add", file);
    git("commit", "-qm", "side");
    git("checkout", "-q", "main");
    if (kind === "add/add") {
      await fs.writeFile(path.join(native, file), "main\n");
      git("add", file);
      git("commit", "-qm", "main addition");
    }
    const merge = spawnSync("git", ["merge", "--no-commit", "side"], {
      cwd: native,
      env: gitEnv,
      encoding: "utf8",
    });
    assert.equal(merge.status, 1, merge.stdout + merge.stderr);
    assert.match(merge.stdout, /CONFLICT/);
    assert.equal(
      git("ls-files", "--unmerged").trim().split("\n").length,
      kind === "add/add" ? 2 : 3,
    );
    await fs.writeFile(path.join(native, file), "partial conflict resolution\n");
    await fs.writeFile(path.join(native, "new"), "unrelated local edit\n");
    await fs.cp(native, reference, { recursive: true });
    return file;
  };

  for (const kind of ["modify/modify", "add/add"] as const) {
    it.effect(
      `refuses an unresolved ${kind} index source before replacing any requested file`,
      () =>
        Effect.promise(async () => {
          const file = await conflict(kind);
          const before = await snapshot(native);
          const index = await fs.readFile(path.join(native, ".git/index"));
          const refused = spawnSync("git", ["restore", "--", "new", file], {
            cwd: reference,
            env: gitEnv,
            encoding: "utf8",
          });
          assert.equal(refused.status, 1);
          assert.match(refused.stderr, /unmerged/);
          assert.deepEqual(await snapshot(reference), before);
          const result = await Effect.runPromise(
            Checkout.restore(["new", file]).pipe(Effect.result, Effect.provide(layerFor(native))),
          );
          assert.deepEqual(await snapshot(native), before);
          assert.deepEqual(await fs.readFile(path.join(native, ".git/index")), index);
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") {
            assert.equal(result.failure._tag, "Invalid");
            if (result.failure._tag === "Invalid") assert.match(result.failure.reason, /unmerged/);
          }
        }),
    );
  }

  for (const destination of destinations) {
    it.effect(`restores an unresolved path into ${destination.name} from a commit`, () =>
      Effect.promise(async () => {
        await conflict();
        const source = destination.name === "worktree" ? { source: "HEAD" } : {};
        gitIn(reference)(
          "restore",
          ...destination.flags,
          ...(destination.name === "worktree" ? ["--source=HEAD"] : []),
          "--",
          "a",
        );
        await Effect.runPromise(
          Checkout.restore(["a"], { ...destination.options, ...source }).pipe(
            Effect.provide(layerFor(native)),
          ),
        );
        assert.deepEqual(await snapshot(native), await snapshot(reference));
      }),
    );
  }

  it.effect("refuses moving an unresolved path without discarding its index stages", () =>
    Effect.promise(async () => {
      await conflict();
      const before = await snapshot(native);
      const refused = spawnSync("git", ["mv", "a", "renamed"], {
        cwd: reference,
        env: gitEnv,
        encoding: "utf8",
      });
      assert.notEqual(refused.status, 0);
      assert.match(refused.stderr, /conflict/);
      assert.deepEqual(await snapshot(reference), before);
      const result = await Effect.runPromise(
        Checkout.move("a", "renamed").pipe(Effect.result, Effect.provide(layerFor(native))),
      );
      assert.deepEqual(await snapshot(native), before);
      await assert.rejects(fs.stat(path.join(native, "renamed")), { code: "ENOENT" });
      assert.equal(result._tag, "Failure");
    }),
  );

  for (const destination of destinations) {
    for (const source of ["refs/heads/source", "refs/tags/source-tag", "refs/tags/source-tree"]) {
      it.effect(`restores ${destination.name} from ${source}, including paths absent there`, () =>
        Effect.promise(async () => {
          gitIn(reference)(
            "restore",
            `--source=${source}`,
            ...destination.flags,
            "--",
            "a",
            "new",
            "old",
          );
          const restored = await Effect.runPromise(
            Checkout.restore(["a", "new", "old"], { ...destination.options, source }).pipe(
              Effect.provide(layerFor(native)),
            ),
          );
          assert.deepEqual(restored, ["a", "new", "old"]);
          assert.deepEqual(await snapshot(native), await snapshot(reference));
        }),
      );
    }

    it.effect(`refuses an unmatched path before changing ${destination.name}`, () =>
      Effect.promise(async () => {
        const before = await snapshot(native);
        const index = await fs.readFile(path.join(native, ".git/index"));
        const refused = spawnSync(
          "git",
          ["restore", "--source=refs/heads/source", ...destination.flags, "--", "a", "missing"],
          { cwd: reference, env: gitEnv, encoding: "utf8" },
        );
        assert.notEqual(refused.status, 0);
        assert.match(refused.stderr, /did not match/);
        assert.deepEqual(await snapshot(reference), before);

        const result = await Effect.runPromise(
          Checkout.restore(["a", "missing"], {
            ...destination.options,
            source: "refs/heads/source",
          }).pipe(Effect.result, Effect.provide(layerFor(native))),
        );
        assert.equal(result._tag, "Failure");
        assert.deepEqual(await snapshot(native), before);
        assert.deepEqual(await fs.readFile(path.join(native, ".git/index")), index);
      }),
    );
  }
});
