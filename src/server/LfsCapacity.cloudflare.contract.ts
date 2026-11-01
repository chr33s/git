import assert from "node:assert/strict";
import { ConfigProvider, Deferred, Effect, Fiber, Stream } from "effect";
import { bytesToHex } from "../git/Format.ts";
import type { Runner } from "../git/Store.contract.ts";
import { LfsStore } from "./Lfs.ts";
import { r2 } from "./Lfs.cloudflare.ts";
import { lockKey, reserve } from "./LfsCapacity.cloudflare.ts";

export const lfsCapacityContract = (bucket: R2Bucket, repo: string, runner: Runner): void => {
  for (const mode of [
    "bytes",
    "objects",
    "pagination",
    "competing-writer",
    "uncertain-lock",
    "uncertain-object",
    "cancelled-scan",
  ] as const)
    runner.it(`LFS retained quota: ${mode}`, async () => {
      const namespace = `${repo}/lfs-capacity-${crypto.randomUUID()}`;
      const entered = await Effect.runPromise(Deferred.make<void>());
      const release = await Effect.runPromise(Deferred.make<void>());
      let listCalls = 0;
      const wrapped: R2Bucket = {
        head: bucket.head.bind(bucket),
        get: bucket.get.bind(bucket),
        put: async (key, value, options) => {
          const result = await bucket.put(key, value, options);
          if (
            mode === "uncertain-lock" ||
            (mode === "uncertain-object" && key !== lockKey(namespace))
          )
            throw new Error("private-lost-write-response");
          return result;
        },
        delete: bucket.delete.bind(bucket),
        // Real listing with a deliberately small page, not an invented result.
        list: async (options) => {
          listCalls++;
          const page = await bucket.list({
            ...options,
            limit: mode === "pagination" || mode === "cancelled-scan" ? 1 : 1000,
          });
          if (mode === "cancelled-scan" && listCalls === 1) {
            assert.ok(page.truncated, "cancellation must prevent a subsequent page");
            Effect.runSync(Deferred.succeed(entered, undefined));
            await Effect.runPromise(Deferred.await(release));
          }
          return page;
        },
        createMultipartUpload: bucket.createMultipartUpload.bind(bucket),
        resumeMultipartUpload: bucket.resumeMultipartUpload.bind(bucket),
      };
      const write = async (content: string) => {
        const bytes = new TextEncoder().encode(content);
        const oid = bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
        const result = await Effect.runPromise(
          LfsStore.use((store) => store.write(oid, Stream.succeed(bytes))).pipe(
            Effect.provide(r2({ bucket: wrapped, repo: namespace })),
            Effect.provide(
              ConfigProvider.layer(
                ConfigProvider.fromUnknown({
                  GIT_LFS_RETAINED_BYTES: "10",
                  GIT_LFS_RETAINED_OBJECTS: mode === "objects" ? "1" : "100",
                }),
              ),
            ),
            Effect.result,
          ),
        );
        return { oid, result };
      };
      try {
        if (mode === "cancelled-scan") {
          await bucket.put(`${namespace}/lfs/a`, "a");
          await bucket.put(`${namespace}/lfs/b`, "b");
          const scanning = Effect.runFork(
            reserve(wrapped, namespace, { bytes: 10, objects: 100 }).pipe(Effect.scoped),
          );
          await Effect.runPromise(Deferred.await(entered));
          let finished = false;
          const interrupted = Effect.runPromise(Fiber.interrupt(scanning)).then(() => {
            finished = true;
          });
          try {
            assert.ok(
              await bucket.head(lockKey(namespace)),
              "pending listing must retain its quota lock",
            );
            assert.equal(finished, false, "cancellation must await the pending native listing");
          } finally {
            Effect.runSync(Deferred.succeed(release, undefined));
            await interrupted;
          }
          assert.equal(listCalls, 1, "cancelled scan must not request another page");
          assert.equal(await bucket.head(lockKey(namespace)), null);
        } else if (mode === "competing-writer") {
          await Effect.runPromise(
            reserve(bucket, namespace, { bytes: 10, objects: 100 }).pipe(
              Effect.andThen(
                Effect.promise(async () => {
                  const { result } = await write("abc");
                  assert.equal(result._tag, "Failure");
                  if (result._tag === "Failure") assert.equal(result.failure._tag, "LfsQuotaBusy");
                }),
              ),
              Effect.scoped,
            ),
          );
          assert.equal((await write("abc")).result._tag, "Success");
        } else if (mode === "uncertain-lock" || mode === "uncertain-object") {
          const { result } = await write("abc");
          assert.equal(result._tag, "Failure");
          assert.ok(await bucket.head(lockKey(namespace)));
          if (mode === "uncertain-object") {
            const key = bytesToHex(
              new Uint8Array(
                await crypto.subtle.digest("SHA-256", new TextEncoder().encode("abc")),
              ),
            );
            const object = await bucket.get(`${namespace}/lfs/${key}`);
            assert.ok(object, "a lost put response can still leave a complete object");
            assert.equal(await object.text(), "abc");
          }
          const retry = await Effect.runPromise(
            reserve(bucket, namespace, { bytes: 10, objects: 100 }).pipe(
              Effect.scoped,
              Effect.result,
            ),
          );
          assert.equal(retry._tag, "Failure");
          if (retry._tag === "Failure") assert.equal(retry.failure._tag, "LfsQuotaBusy");
        } else {
          const first = await write("123456");
          assert.equal(first.result._tag, "Success");
          if (mode === "pagination") {
            assert.equal((await write("a")).result._tag, "Success");
            assert.equal((await write("b")).result._tag, "Success");
          }
          const rejected = await write("abcdef");
          assert.equal(rejected.result._tag, "Failure");
          if (rejected.result._tag === "Failure")
            assert.equal(rejected.result.failure._tag, "LfsQuotaExceeded");
          assert.equal(await bucket.head(`${namespace}/lfs/${rejected.oid}`), null);
          const retained = await bucket.get(`${namespace}/lfs/${first.oid}`);
          assert.ok(retained);
          assert.equal(await retained.text(), "123456");
          assert.equal(await bucket.head(lockKey(namespace)), null);
          if (mode !== "objects") assert.equal((await write("cd")).result._tag, "Success");
        }
      } finally {
        // All fixture writers have settled; only this fixture owns its prefix.
        const objects = await bucket.list({ prefix: `${namespace}/lfs/` });
        for (const object of objects.objects) await bucket.delete(object.key);
      }
    });
};
