/**
 * Packs as storage, end to end.
 *
 * The claim being tested is not "we can write a pack" but "a repository whose
 * objects are only in a pack still works" — reads, reachability, fetches and
 * `fsck` all go through the same store, and before this they all depended on
 * every object having its own loose entry.
 *
 * The pack is handed to the real `git` binary as well, because a pack only
 * this codebase can read would be a private format wearing git's extension.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { hasGit } from "../testing/Git.ts";
import * as Maintenance from "./Maintenance.ts";
import { stores as memoryStores } from "./Memory.ts";
import { PackStore } from "./Packed.ts";
import { stores as nodeStores } from "./Node.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { ObjectStore, type Oid, RefStore } from "./Store.ts";

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** A repository with `count` commits, each touching two files. */
const seed = (count: number) =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    for (let index = 0; index < count; index++) {
      // Read the tip rather than carrying it: each commit builds on the
      // previous tree, and threading it through the loop makes the inference
      // circular for no benefit.
      const tip = yield* repository.resolve("refs/heads/main");
      const changes = [
        { path: "stable.txt", content: new TextEncoder().encode("unchanging\n") },
        {
          path: `nested/file-${index}.txt`,
          content: new TextEncoder().encode(`revision ${index}\n`),
        },
      ];
      const tree =
        tip === null
          ? yield* repository.writeFiles({ changes })
          : yield* repository.writeFiles({
              base: (yield* repository.readCommit(tip)).tree,
              changes,
            });
      yield* repository.commit({ branch: "main", tree, message: `commit ${index}`, author });
    }
    return (yield* repository.resolve("refs/heads/main"))!;
  });

describe("packs at rest", () => {
  it.live("serves a repository whose objects live only in a pack", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const objects = yield* ObjectStore;

      const head = yield* seed(6);
      const before = yield* Stream.runCollect(objects.list);
      assert.ok(before.length > 10);

      const report = yield* repository.gc({ repack: true });
      assert.notEqual(report.packed, undefined);
      assert.equal(report.packed!.objects, before.length);

      // Nothing loose is left, and the same objects are still listed —
      // through the pack index this time.
      const after = yield* Stream.runCollect(objects.list);
      assert.deepEqual([...after].sort(), [...before].sort());

      // Everything still reads: the commit, its history, and the content.
      const commit = yield* repository.readCommit(head);
      assert.equal(commit.message, "commit 5");
      const log = yield* Stream.runCollect(repository.log(head));
      assert.equal(log.length, 6);

      const files = yield* repository.listFiles(commit.tree);
      assert.deepEqual(files.map((file) => file.path).sort(), [
        "nested/file-0.txt",
        "nested/file-1.txt",
        "nested/file-2.txt",
        "nested/file-3.txt",
        "nested/file-4.txt",
        "nested/file-5.txt",
        "stable.txt",
      ]);
      const blob = yield* repository.readBlob(
        files.find((file) => file.path === "stable.txt")!.oid,
      );
      assert.equal(new TextDecoder().decode(blob), "unchanging\n");

      // Integrity holds when every object is read back out of the pack.
      const fsck = yield* repository.fsck;
      assert.deepEqual(fsck.problems, []);
      assert.deepEqual(fsck.danglingRefs, []);

      // And a fetch can still be served, which walks and re-packs them.
      const plan = yield* repository.fetch({ wants: [head], haves: [] });
      assert.equal(plan.oids.length, before.length);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );

  it.live("collects garbage before packing, so the pack holds only live objects", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const objects = yield* ObjectStore;

      yield* seed(2);
      const orphan = yield* repository.writeBlob(new TextEncoder().encode("unreferenced\n"));

      const report = yield* repository.gc({ repack: true });
      assert.deepEqual(report.removed, [orphan]);

      const remaining = yield* Stream.runCollect(objects.list);
      assert.equal(remaining.includes(orphan), false);
      assert.equal(report.packed!.objects, remaining.length);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );

  it.live("reports a packed object as retained rather than removed, and drops it on a repack", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const objects = yield* ObjectStore;

      yield* seed(1);
      const tree = yield* repository.writeFiles({
        changes: [{ path: "secret.txt", content: new TextEncoder().encode("secret\n") }],
      });
      yield* repository.commit({ branch: "tmp", tree, message: "secret", author });
      yield* repository.gc({ repack: true, reflogGrace: 0 });

      const secret = (yield* repository.listFiles(tree)).find(
        (file) => file.path === "secret.txt",
      )!.oid;
      yield* repository.deleteRef("refs/heads/tmp");

      // Unreachable, but a pack holds it — and deleting one object from a
      // pack is a rewrite of the pack, so a plain gc cannot claim it gone.
      const swept = yield* repository.gc({ reflogGrace: 0 });
      assert.equal(swept.removed.includes(secret), false);
      assert.equal(swept.retained.includes(secret), true);
      assert.equal(yield* repository.contains(secret), true);

      // The repack is what collects it: the superseded pack is deleted.
      const repacked = yield* repository.gc({ repack: true, reflogGrace: 0 });
      assert.equal(repacked.removed.includes(secret), true);
      assert.equal(yield* repository.contains(secret), false);
      assert.equal((yield* Stream.runCollect(objects.list)).includes(secret), false);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );

  it.live("refuses to collect when its roots name objects the store cannot read", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const objects = yield* ObjectStore;
      const refs = yield* RefStore;
      const packs = yield* PackStore;

      const head = yield* seed(2);
      const before = (yield* Stream.runCollect(objects.list)).length;

      // Every ref names something unreadable — a store pointed at the wrong
      // prefix, a half-restored backup. Every object then looks unreachable,
      // so a gc that trusted the walk would delete the entire repository.
      // SAFETY: forty lowercase hex characters by construction, which is exactly
      // what the Oid brand stands for.
      const missing = "9".repeat(40) as Oid;
      const broken: RefStore["Service"] = {
        ...refs,
        list: () => Effect.succeed([["refs/heads/main", missing] as const]),
        logged: Effect.succeed([]),
        resolve: () => Effect.succeed(null),
      };

      const failure = yield* Effect.flip(
        Maintenance.gc({ objects, packs, refs: broken }, { reflogGrace: 0 }),
      );

      assert.equal(failure._tag, "StorageFailure");
      assert.equal((yield* Stream.runCollect(objects.list)).length, before);
      assert.equal(yield* repository.contains(head), true);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );

  it.live("never deletes a pack on the strength of a walk that found no roots", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const objects = yield* ObjectStore;
      const refs = yield* RefStore;
      const packs = yield* PackStore;

      yield* seed(2);
      yield* repository.gc({ repack: true, reflogGrace: 0 });
      const before = (yield* packs.list).length;
      assert.ok(before > 0);

      // A ref store that reports nothing — a `packed-refs` file a backend
      // does not parse, a registry that failed to load, a prefix typo. An
      // empty walk is an absence of evidence, not evidence that a repository
      // is garbage, and deleting its packs is the one step that cannot be
      // taken back.
      const blind: RefStore["Service"] = {
        ...refs,
        list: () => Effect.succeed([]),
        logged: Effect.succeed([]),
        resolve: () => Effect.succeed(null),
      };
      yield* Maintenance.gc({ objects, packs, refs: blind }, { repack: true, reflogGrace: 0 });

      assert.equal((yield* packs.list).length, before);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );

  it.live("keeps the history of a branch that was deleted, which is what the reflog is for", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;

      yield* seed(1);
      const tree = yield* repository.writeFiles({
        changes: [{ path: "wip.txt", content: new TextEncoder().encode("work\n") }],
      });
      const doomed = yield* repository.commit({
        branch: "wip",
        tree,
        message: "wip",
        author,
      });

      // `git push origin :wip`, then the next scheduled collection. The ref
      // is gone from `list` — its log is the only thing that still leads
      // back to the commit, and that is precisely when it must be consulted.
      yield* repository.deleteRef("refs/heads/wip");
      const kept = yield* repository.gc({});
      assert.equal(kept.removed.includes(doomed), false);
      assert.equal(yield* repository.contains(doomed), true);

      const purged = yield* repository.gc({ reflogGrace: 0 });
      assert.equal(purged.removed.includes(doomed), true);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );

  it.live("keeps a commit the reflog still names until the grace period is waived", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;

      const first = yield* seed(1);
      const second = yield* seed(1);
      // A reset, or a force-push: the branch no longer reaches `second`, and
      // the reflog entry that leads back to it is the only record left.
      yield* repository.setRef({ name: "refs/heads/main", to: first });

      const kept = yield* repository.gc({});
      assert.equal(kept.removed.includes(second), false);
      assert.equal(yield* repository.contains(second), true);

      const purged = yield* repository.gc({ reflogGrace: 0 });
      assert.equal(purged.removed.includes(second), true);
      assert.equal(yield* repository.contains(second), false);
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(memoryStores),
        ),
      ),
    ),
  );
});

describe.skipIf(!hasGit)("packs at rest, on disk", () => {
  let root: string;

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "packed-node-"));
  });

  afterAll(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it("writes a pack the git binary accepts, and reads its own repository back", async () => {
    const directory = path.join(root, "repo");
    const layer = GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(nodeStores(directory)),
    );

    // git identifies a repository by its HEAD file; without it `--git-dir`
    // refuses the directory however complete the objects are.
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* (yield* RefStore).setHead("refs/heads/main");
      }).pipe(Effect.provide(nodeStores(directory))),
    );

    const head = await Effect.runPromise(seed(8).pipe(Effect.provide(layer)));

    const report = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* Repository).gc({ repack: true });
      }).pipe(Effect.provide(layer)),
    );
    assert.notEqual(report.packed, undefined);

    // Loose objects are gone from disk; the pack pair is there instead.
    const objectsDir = path.join(directory, "objects");
    const shards = (await fs.readdir(objectsDir)).filter((name) => /^[0-9a-f]{2}$/.test(name));
    const loose = (
      await Promise.all(shards.map((shard) => fs.readdir(path.join(objectsDir, shard))))
    ).flat();
    assert.deepEqual(loose, [], "repack left loose objects behind");

    const packDir = path.join(objectsDir, "pack");
    const packFiles = await fs.readdir(packDir);
    assert.ok(packFiles.some((name) => name.endsWith(".pack")));
    assert.ok(packFiles.some((name) => name.endsWith(".idx")));

    // git's own verifier on our pack — the assertion that makes the format
    // claim real rather than self-referential.
    const idx = packFiles.find((name) => name.endsWith(".idx"))!;
    execFileSync("git", ["verify-pack", "-v", path.join(packDir, idx)], { encoding: "utf8" });

    // And git reads the repository as a whole, with no loose objects at all.
    const log = execFileSync("git", [`--git-dir=${directory}`, "log", "--format=%s"], {
      encoding: "utf8",
    });
    assert.equal(log.trim().split("\n")[0], "commit 7");
    execFileSync("git", [`--git-dir=${directory}`, "fsck", "--strict"], { encoding: "utf8" });

    // Our own reader, on a repository that is now nothing but a pack.
    const readBack = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository.readCommit(head);
        return { message: commit.message, files: yield* repository.listFiles(commit.tree) };
      }).pipe(Effect.provide(layer)),
    );
    assert.equal(readBack.message, "commit 7");
    assert.equal(readBack.files.length, 9);
  });
});
