import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { LfsStore } from "./Lfs.ts";
import { file } from "./Lfs.node.ts";
import { configuration } from "./LfsCapacity.node.ts";

const oid = (content: string) => createHash("sha256").update(content).digest("hex");
const body = (content: string) => Stream.succeed(new TextEncoder().encode(content));
const limits = (bytes = 10, objects = 100) =>
  ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      GIT_LFS_RETAINED_BYTES: String(bytes),
      GIT_LFS_RETAINED_OBJECTS: String(objects),
    }),
  );
const write = (root: string, content: string, bytes = 10, objects = 100) =>
  Effect.gen(function* () {
    const store = yield* LfsStore;
    return yield* store.write(oid(content), body(content));
  }).pipe(Effect.provide(Layer.provideMerge(file(root), limits(bytes, objects))));

for (const mode of ["bytes", "objects", "orphan"] as const)
  it.live(`counts retained ${mode} across backend reopen and preserves existing content`, () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-quota-"));
      try {
        await Effect.runPromise(write(root, "123456"));
        if (mode === "orphan") await fs.writeFile(path.join(root, "abandoned.tmp"), "12");
        const rejected = await Effect.runPromise(
          write(root, "abcdef", 10, mode === "objects" ? 1 : 100).pipe(Effect.result),
        );
        assert.equal(rejected._tag, "Failure");
        if (rejected._tag === "Failure") assert.equal(rejected.failure._tag, "LfsQuotaExceeded");
        const existing = path.join(
          root,
          oid("123456").slice(0, 2),
          oid("123456").slice(2, 4),
          oid("123456"),
        );
        assert.equal(await fs.readFile(existing, "utf8"), "123456");
        await assert.rejects(fs.stat(path.join(root, ".quota.lock")), { code: "ENOENT" });
        // Quota failure cleans its temporary object and returns the lock.
        await Effect.runPromise(write(root, "ab", 10, 100));
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

it.live("holds the exclusive lock through a pending upload, then releases it on cancellation", () =>
  Effect.gen(function* () {
    const root = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "lfs-quota-race-")));
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => fs.rm(root, { recursive: true, force: true })),
    );
    const entered = yield* Deferred.make<void>();
    const held = yield* Effect.gen(function* () {
      const store = yield* LfsStore;
      return yield* store.write(
        oid("abc"),
        Stream.fromEffect(Deferred.succeed(entered, undefined)).pipe(
          Stream.flatMap(() => Stream.never),
        ),
      );
    }).pipe(Effect.provide(Layer.provideMerge(file(root), limits())), Effect.forkScoped);
    yield* Deferred.await(entered);
    const busy = yield* write(root, "abc").pipe(Effect.result);
    assert.equal(busy._tag, "Failure");
    if (busy._tag === "Failure") assert.equal(busy.failure._tag, "LfsQuotaBusy");
    yield* Fiber.interrupt(held);
    yield* write(root, "abc");
  }),
);

for (const mode of ["stale-lock", "symlink"] as const)
  it.live(`fails closed on ${mode}`, () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-quota-state-"));
      try {
        if (mode === "stale-lock") await fs.writeFile(path.join(root, ".quota.lock"), "");
        else await fs.symlink(os.tmpdir(), path.join(root, "aa"));
        const result = await Effect.runPromise(write(root, "abc").pipe(Effect.result));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure")
          assert.equal(
            result.failure._tag,
            mode === "stale-lock" ? "LfsQuotaBusy" : "StorageFailure",
          );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

for (const name of ["GIT_LFS_RETAINED_BYTES", "GIT_LFS_RETAINED_OBJECTS"])
  for (const value of ["0", "-1", "1.5", "9007199254740992", "Infinity"])
    it.effect(`rejects ${name}=${value}`, () =>
      Effect.gen(function* () {
        assert.equal((yield* configuration().pipe(Effect.result))._tag, "Failure");
      }).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ [name]: value })))),
    );
