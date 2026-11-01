import assert from "node:assert/strict";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { objectStore, packStore, r2Packs } from "./Cloudflare.ts";
import { hashObject } from "./Format.ts";
import { ObjectStore } from "./Store.ts";
import type { Runner } from "./Store.contract.ts";

export const cloudflareMutationContract = (
  bucket: R2Bucket,
  repo: string,
  runner: Runner,
): void => {
  for (const mode of ["loose-write", "loose-delete", "pack-write", "pack-delete"] as const)
    runner.it(`R2 Git mutation settlement: ${mode}`, async () => {
      const namespace = `${repo}/mutation-${crypto.randomUUID()}`;
      const data = new Uint8Array([1, 2, 3]);
      const object = { type: "blob", data } as const;
      const oid = await Effect.runPromise(hashObject(object));
      const looseKey = `${namespace}/objects/${oid}`;
      const packKey = `${namespace}/pack/fixture.pack`;
      const indexKey = `${namespace}/pack/fixture.idx`;
      const entered = await Effect.runPromise(Deferred.make<void>());
      const release = await Effect.runPromise(Deferred.make<void>());
      const settled = await Effect.runPromise(Deferred.make<void>());
      let mutations = 0;
      const afterMutation = async () => {
        mutations++;
        if (mutations === 1) {
          Effect.runSync(Deferred.succeed(entered, undefined));
          await Effect.runPromise(Deferred.await(release));
        }
        if (mutations === (mode.startsWith("pack") ? 2 : 1))
          Effect.runSync(Deferred.succeed(settled, undefined));
      };
      const wrapped: R2Bucket = {
        head: bucket.head.bind(bucket),
        get: bucket.get.bind(bucket),
        list: bucket.list.bind(bucket),
        createMultipartUpload: bucket.createMultipartUpload.bind(bucket),
        resumeMultipartUpload: bucket.resumeMultipartUpload.bind(bucket),
        put: async (key, value, options) => {
          const result = await bucket.put(key, value, options);
          await afterMutation();
          return result;
        },
        delete: async (key) => {
          await bucket.delete(key);
          await afterMutation();
        },
      };
      if (mode === "loose-delete")
        await bucket.put(looseKey, data, { customMetadata: { type: "blob" } });
      if (mode === "pack-delete") {
        await bucket.put(packKey, data);
        await bucket.put(indexKey, data);
      }
      const packs = r2Packs(wrapped, namespace);
      const loose = objectStore(wrapped, namespace).pipe(
        Layer.provide(packStore(wrapped, namespace)),
      );
      const operation =
        mode === "loose-write"
          ? ObjectStore.use((store) => store.write(object)).pipe(
              Effect.asVoid,
              Effect.provide(loose),
            )
          : mode === "loose-delete"
            ? ObjectStore.use((store) => store.delete(oid)).pipe(Effect.provide(loose))
            : mode === "pack-write"
              ? packs.write({ name: "fixture", pack: data, index: data })
              : packs.delete("fixture");
      const writing = Effect.runFork(operation);
      await Effect.runPromise(Deferred.await(entered));
      let finished = false;
      const interrupted = Effect.runPromise(Fiber.interrupt(writing)).then(() => {
        finished = true;
      });
      try {
        try {
          // A real native read yields to the interruption while the mutation's
          // response is still held. No module or filesystem API is mocked.
          await bucket.head(looseKey);
          assert.equal(finished, false, "interruption returned before the R2 mutation settled");
        } finally {
          Effect.runSync(Deferred.succeed(release, undefined));
          await Effect.runPromise(Deferred.await(settled));
          await interrupted;
        }
        if (mode === "loose-write") {
          const stored = await bucket.get(looseKey);
          assert.ok(stored);
          assert.deepEqual(new Uint8Array(await stored.arrayBuffer()), data);
        } else if (mode === "pack-write") {
          for (const key of [packKey, indexKey]) {
            const stored = await bucket.get(key);
            assert.ok(stored, "a started pack publication must settle both objects");
            assert.deepEqual(new Uint8Array(await stored.arrayBuffer()), data);
          }
        } else {
          for (const key of mode === "loose-delete" ? [looseKey] : [packKey, indexKey])
            assert.equal(await bucket.head(key), null);
        }
      } finally {
        await bucket.delete([looseKey, packKey, indexKey]);
      }
    });
};
