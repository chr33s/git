import assert from "node:assert/strict";
import { Effect, Result } from "effect";
import { refStore } from "./Cloudflare.ts";
import { RefStore } from "./Store.ts";
import type { Runner } from "./Store.contract.ts";

export const headCapacityContract = (
  storage: DurableObjectStorage,
  repo: string,
  runner: Runner,
): void => {
  for (const mode of ["boundary", "lowered", "corrupt", "namespaces"] as const)
    runner.it(`Git HEAD quota: ${mode}`, async () => {
      const namespace = `${repo}/head-${crypto.randomUUID()}`;
      const key = `HEAD:${namespace}`;
      const target = "refs/heads/東京";
      const bytes = new TextEncoder().encode(target).byteLength;
      const open = (limit: number, name = namespace) =>
        Effect.runPromise(RefStore.pipe(Effect.provide(refStore(storage, name, undefined, limit))));
      try {
        const exact = await open(bytes);
        if (mode === "boundary") {
          await Effect.runPromise(exact.setHead(target));
          assert.equal(await Effect.runPromise(exact.head), target);
          const refused = await Effect.runPromise(Effect.result(exact.setHead(`${target}a`)));
          assert.ok(Result.isFailure(refused));
          assert.equal(refused.failure._tag, "StorageFailure");
          assert.equal(await storage.get(key), target);
          const below = await open(bytes - 1);
          assert.ok(
            Result.isFailure(await Effect.runPromise(Effect.result(below.setHead(target)))),
          );
          assert.equal(await storage.get(key), target);
        } else if (mode === "lowered") {
          await Effect.runPromise(exact.setHead(target));
          const low = await open("refs/x".length);
          const read = await Effect.runPromise(Effect.result(low.head));
          assert.ok(Result.isFailure(read));
          assert.equal(read.failure._tag, "StorageFailure");
          assert.equal(await storage.get(key), target, "refused reads preserve legacy data");
          await Effect.runPromise(low.setHead("refs/x"));
          assert.equal(await Effect.runPromise(low.head), "refs/x");
          await Effect.runPromise((await open(bytes)).setHead(target));
          assert.equal(await storage.get(key), target);
        } else if (mode === "corrupt") {
          await storage.put(key, { private: "invalid-head" });
          assert.ok(Result.isFailure(await Effect.runPromise(Effect.result(exact.head))));
          assert.ok(
            Result.isFailure(await Effect.runPromise(Effect.result(exact.setHead(target)))),
          );
          assert.deepEqual(await storage.get(key), { private: "invalid-head" });
        } else {
          const other = await open(bytes, `${namespace}-other`);
          await Effect.runPromise(exact.setHead(target));
          await Effect.runPromise(other.setHead("refs/x"));
          assert.ok(
            Result.isFailure(await Effect.runPromise(Effect.result(exact.setHead(`${target}a`)))),
          );
          assert.equal(await Effect.runPromise(other.head), "refs/x");
        }
      } finally {
        await storage.delete(key);
        await storage.delete(`HEAD:${namespace}-other`);
      }
    });
};
