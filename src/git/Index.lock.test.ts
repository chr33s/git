import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";
import { Invalid } from "./Error.ts";
import { IndexStore } from "./Work.ts";
import { indexFile } from "./Work.node.ts";

describe("index reservation lifetime", () => {
  let root: string;
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "index-lifetime-"));
  });
  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("releases a reservation after an interrupted operation", () =>
    Effect.gen(function* () {
      const index = yield* IndexStore;
      const entered = yield* Deferred.make<void>();
      const owner = yield* index
        .withLock(() => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)))
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      assert.equal(existsSync(path.join(root, "index.lock")), true);
      yield* Fiber.interrupt(owner);
      assert.equal(existsSync(path.join(root, "index.lock")), false);
      yield* index.save([]);
      assert.deepEqual(yield* index.load, []);
    }).pipe(Effect.provide(indexFile(root))),
  );

  it.effect("releases a reservation after a rejected operation", () =>
    Effect.gen(function* () {
      const index = yield* IndexStore;
      const result = yield* index
        .withLock(() => new Invalid({ field: "fixture", reason: "rejected edit" }))
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.equal(existsSync(path.join(root, "index.lock")), false);
      yield* index.save([]);
      assert.deepEqual(yield* index.load, []);
    }).pipe(Effect.provide(indexFile(root))),
  );

  it.effect("removes temporary output and its reservation when publication fails", () =>
    Effect.gen(function* () {
      const index = yield* IndexStore;
      yield* Effect.promise(() => fs.mkdir(path.join(root, "index")));
      const result = yield* index.save([]).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure._tag, "StorageFailure");
      assert.deepEqual(yield* Effect.promise(() => fs.readdir(root)), ["index"]);
      yield* Effect.promise(() => fs.rmdir(path.join(root, "index")));
      yield* index.save([]);
      assert.deepEqual(yield* index.load, []);
    }).pipe(Effect.provide(indexFile(root))),
  );
});
