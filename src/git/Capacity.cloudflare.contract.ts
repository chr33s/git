import assert from "node:assert/strict";
import { Deferred, Effect, Layer, Logger } from "effect";
import { bounded } from "./Capacity.cloudflare.ts";
import { objectStore, packStore } from "./Cloudflare.ts";
import { StorageFailure } from "./Error.ts";
import { hashObject } from "./Format.ts";
import { PackStore } from "./Packed.ts";
import { ObjectStore } from "./Store.ts";
import type { Runner } from "./Store.contract.ts";

const failureEvents = {
  "quota.reservation": "quota_reserved",
  "quota.bytes": "quota_exceeded",
  "quota.keys": "quota_exceeded",
  "quota.inspect": "quota_reconciliation_required",
  write: "mutation_failed",
  delete: "mutation_failed",
};

export const gitCapacityContract = (
  bucket: R2Bucket,
  storage: DurableObjectStorage,
  repo: string,
  runner: Runner,
): void => {
  for (const mode of [
    "bytes",
    "keys",
    "legacy",
    "legacy-over-limit",
    "ledger-mismatch",
    "packs",
    "uncertain-put",
    "uncertain-delete",
    "concurrent",
    "sync-failure",
  ] as const)
    runner.it(`Git retained quota: ${mode}`, async () => {
      const namespace = `${repo}/git-capacity-${crypto.randomUUID()}`;
      const entered = await Effect.runPromise(Deferred.make<void>());
      const release = await Effect.runPromise(Deferred.make<void>());
      let nativePuts = 0;
      let listings = 0;
      let syncs = 0;
      const wrapped: R2Bucket = {
        head: bucket.head.bind(bucket),
        get: bucket.get.bind(bucket),
        createMultipartUpload: bucket.createMultipartUpload.bind(bucket),
        resumeMultipartUpload: bucket.resumeMultipartUpload.bind(bucket),
        list: (options) => {
          listings++;
          return bucket.list(mode === "legacy" ? { ...options, limit: 1 } : options);
        },
        put: async (key, value, options) => {
          nativePuts++;
          const result = await bucket.put(key, value, options);
          if (mode === "uncertain-put") throw new Error("private-lost-put-response");
          if (mode === "concurrent" && nativePuts === 1) {
            Effect.runSync(Deferred.succeed(entered, undefined));
            await Effect.runPromise(Deferred.await(release));
          }
          return result;
        },
        delete: async (key) => {
          await bucket.delete(key);
          if (mode === "uncertain-delete") throw new Error("private-lost-delete-response");
        },
      };
      const disk = {
        sql: storage.sql,
        transactionSync: storage.transactionSync.bind(storage),
        sync: async () => {
          await storage.sync();
          if (++syncs === 3 && mode === "sync-failure") throw new Error("private-sync-failure");
        },
      };
      const capacity = { bytes: mode === "packs" ? 20 : 10, keys: mode === "keys" ? 1 : 100 };
      const backend = (source = wrapped) => {
        const limited = bounded(source, disk, namespace, capacity);
        return objectStore(limited, namespace).pipe(
          Layer.provideMerge(packStore(limited, namespace)),
        );
      };
      const data = (content: string) => new TextEncoder().encode(content);
      const oidOf = (content: string) =>
        Effect.runPromise(hashObject({ type: "blob", data: data(content) }));
      const write = (content: string) =>
        ObjectStore.use((store) => store.write({ type: "blob", data: data(content) })).pipe(
          Effect.provide(backend()),
        );
      const remove = (content: string) =>
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          const oid = yield* hashObject({ type: "blob", data: data(content) });
          yield* store.delete(oid);
        }).pipe(Effect.provide(backend()));
      const refusal = async <A>(
        effect: Effect.Effect<A, StorageFailure>,
        operation: keyof typeof failureEvents,
      ) => {
        const messages: string[] = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          messages.push(JSON.stringify(message));
        });
        const result = await Effect.runPromise(
          effect.pipe(Effect.result, Effect.provide(Logger.layer([logger]))),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.operation, operation);
        assert.equal(messages.length, 1, "each refused mutation must emit one storage signal");
        const message = messages[0] ?? "";
        assert.match(message, /GitStorage/);
        assert.match(message, new RegExp(failureEvents[operation]));
        assert.ok(!message.includes(namespace), "signals must omit repository paths");
        assert.doesNotMatch(message, /private-lost|private-sync|123456|orphan\.pack/);
      };
      try {
        if (mode === "legacy") {
          await bucket.put(`${namespace}/objects/${await oidOf("123456")}`, data("123456"), {
            customMetadata: { type: "blob" },
          });
          await bucket.put(`${namespace}/objects/${await oidOf("a")}`, data("a"), {
            customMetadata: { type: "blob" },
          });
          await bucket.put(`${namespace}/pack/orphan.pack`, data("b"));
          await refusal(write("xyz"), "quota.bytes");
          assert.ok(listings >= 3, "inventory must consume multiple native R2 pages");
          await Effect.runPromise(write("cd"));
        } else if (mode === "legacy-over-limit") {
          const oid = await oidOf("12345678901");
          const key = `${namespace}/objects/${oid}`;
          await bucket.put(key, data("12345678901"), { customMetadata: { type: "blob" } });
          await refusal(write("ab"), "quota.bytes");
          assert.equal(nativePuts, 0);
          const stored = await Effect.runPromise(
            ObjectStore.use((store) => store.read(oid)).pipe(Effect.provide(backend())),
          );
          assert.deepEqual(stored.data, data("12345678901"));
          // Even after offline removal of excess data, an incomplete inventory
          // is fenced until explicitly reconciled; a new backend cannot steal it.
          await bucket.delete(key);
          await refusal(write("ab"), "quota.reservation");
          assert.equal(
            storage.sql
              .exec<{ state: number }>("SELECT state FROM git_capacity WHERE repo = ?", namespace)
              .one().state,
            0,
          );
          storage.sql.exec("DELETE FROM git_capacity WHERE repo = ?", namespace);
          await Effect.runPromise(write("ab"));
        } else if (mode === "ledger-mismatch") {
          await Effect.runPromise(write("123456"));
          const key = `${namespace}/objects/${await oidOf("123456")}`;
          // An external writer invalidated accounting. Refuse deletion before
          // mutating R2, preserve the fence, and identify reconciliation in logs.
          await bucket.put(key, data("1234567"), { customMetadata: { type: "blob" } });
          await refusal(remove("123456"), "quota.inspect");
          assert.equal((await bucket.head(key))?.size, 7);
          await refusal(write("ab"), "quota.reservation");
        } else if (mode === "packs") {
          await Effect.runPromise(
            PackStore.use((packs) =>
              packs.write({ name: "fixture", pack: data("12345678"), index: data("abcd") }),
            ).pipe(Effect.provide(backend())),
          );
          await refusal(write("123456789"), "quota.bytes");
          await refusal(
            PackStore.use((packs) =>
              packs.write({ name: "too-large", pack: data("123456789"), index: data("abcd") }),
            ).pipe(Effect.provide(backend())),
            "quota.bytes",
          );
          await Effect.runPromise(
            PackStore.use((packs) => packs.delete("fixture")).pipe(Effect.provide(backend())),
          );
          await Effect.runPromise(write("123456789"));
        } else if (mode === "concurrent") {
          const first = Effect.runPromise(write("123456"));
          await Effect.runPromise(Deferred.await(entered));
          try {
            await refusal(write("ab"), "quota.reservation");
          } finally {
            Effect.runSync(Deferred.succeed(release, undefined));
            await first;
          }
          await Effect.runPromise(write("ab"));
        } else if (
          mode === "uncertain-put" ||
          mode === "uncertain-delete" ||
          mode === "sync-failure"
        ) {
          if (mode === "uncertain-delete") {
            await Effect.runPromise(write("123456"));
            await refusal(remove("123456"), "delete");
          } else await refusal(write("123456"), "write");
          await refusal(write("ab"), "quota.reservation");
          const held = storage.sql
            .exec<{ bytes: number; keys: number; state: number }>(
              "SELECT bytes, keys, state FROM git_capacity WHERE repo = ?",
              namespace,
            )
            .one();
          assert.equal(held.state, 2);
          assert.equal(held.bytes, 6);
          assert.equal(held.keys, 1);
          if (mode === "sync-failure") assert.equal(nativePuts, 0);
          if (mode === "uncertain-put") {
            const oid = await oidOf("123456");
            const stored = await Effect.runPromise(
              ObjectStore.use((store) => store.read(oid)).pipe(Effect.provide(backend())),
            );
            assert.deepEqual(stored.data, data("123456"));
          }
          // All native fixture mutations have settled. Offline reconciliation
          // discards only accounting, then inventories the retained R2 objects.
          storage.sql.exec("DELETE FROM git_capacity WHERE repo = ?", namespace);
          await Effect.runPromise(
            ObjectStore.use((store) => store.write({ type: "blob", data: data("ab") })).pipe(
              Effect.provide(backend(bucket)),
            ),
          );
          const recovered = storage.sql
            .exec<{ bytes: number; keys: number; state: number }>(
              "SELECT bytes, keys, state FROM git_capacity WHERE repo = ?",
              namespace,
            )
            .one();
          assert.equal(recovered.state, 1);
          assert.equal(recovered.bytes, mode === "uncertain-put" ? 8 : 2);
          assert.equal(recovered.keys, mode === "uncertain-put" ? 2 : 1);
        } else {
          await Effect.runPromise(write("123456"));
          assert.equal(listings, 2, "inventory scans only the two Git prefixes once");
          await refusal(
            write(mode === "keys" ? "a" : "abcdef"),
            mode === "keys" ? "quota.keys" : "quota.bytes",
          );
          assert.equal(listings, 2, "reopened backends must use durable accounting");
          await Effect.runPromise(remove("123456"));
          await Effect.runPromise(write("abcdef"));
        }
      } finally {
        // Native fixture calls have settled; this prefix and row are test-owned.
        Effect.runSync(Deferred.succeed(release, undefined));
        const page = await bucket.list({ prefix: `${namespace}/` });
        await bucket.delete(page.objects.map((object) => object.key));
        storage.sql.exec("DELETE FROM git_capacity WHERE repo = ?", namespace);
      }
    });
};
