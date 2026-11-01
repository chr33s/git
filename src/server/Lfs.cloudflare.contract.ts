import assert from "node:assert/strict";
import { Effect, Fiber, Layer, Logger, Stream } from "effect";
import { StorageFailure } from "../git/Error.ts";
import type { Runner } from "../git/Store.contract.ts";
import { LfsStore } from "./Lfs.ts";
import { r2 } from "./Lfs.cloudflare.ts";
import { lockKey } from "./LfsCapacity.cloudflare.ts";

/** Real R2 handles: reject a first part or interrupt after one has been stored. */
export const lfsCancellationContract = (bucket: R2Bucket, repo: string, runner: Runner): void => {
  for (const mode of ["first-part-failure", "interrupt", "abort-failure"] as const)
    runner.it(`LFS multipart cleanup after ${mode}`, async () => {
      let notify: () => void = () => undefined;
      const uploaded = new Promise<void>((resolve) => {
        notify = resolve;
      });
      let created: (upload: R2MultipartUpload) => void = () => undefined;
      const opened = new Promise<R2MultipartUpload>((resolve) => {
        created = resolve;
      });
      let cleanup = async () => {};
      let aborted = false;
      const secret = "private-cleanup-cause";
      const sourceFailure = new StorageFailure({ operation: "fixture", path: secret });
      const messages: string[] = [];
      const logger = Logger.make<unknown, void>(({ message }) => {
        messages.push(JSON.stringify(message));
      });
      const wrapped: R2Bucket = {
        head: bucket.head.bind(bucket),
        get: bucket.get.bind(bucket),
        put: bucket.put.bind(bucket),
        delete: bucket.delete.bind(bucket),
        list: bucket.list.bind(bucket),
        resumeMultipartUpload: bucket.resumeMultipartUpload.bind(bucket),
        createMultipartUpload: async (key, options) => {
          const native = await bucket.createMultipartUpload(key, options);
          created(native);
          cleanup = () => native.abort().catch(() => undefined);
          return {
            key: native.key,
            uploadId: native.uploadId,
            uploadPart: async (number, bytes, options) => {
              // Part zero is rejected by real R2 validation, before any bytes
              // become a stored part. No module or storage implementation mock.
              const part = await native.uploadPart(
                mode === "first-part-failure" ? 0 : number,
                bytes,
                options,
              );
              notify();
              return part;
            },
            complete: native.complete.bind(native),
            abort: async () => {
              if (mode === "abort-failure") throw new Error(secret);
              await native.abort();
              aborted = true;
            },
          };
        },
      };
      const namespace = `${repo}/lfs-cancellation-${crypto.randomUUID()}`;
      const oid = "0".repeat(64);
      try {
        await Effect.runPromise(
          Effect.gen(function* () {
            const store = yield* LfsStore;
            const body = Stream.make(new Uint8Array(8 * 1024 * 1024)).pipe(
              Stream.concat(mode === "abort-failure" ? Stream.fail(sourceFailure) : Stream.never),
            );
            if (mode === "interrupt") {
              const writing = yield* store.write(oid, body).pipe(Effect.forkScoped);
              yield* Effect.promise(() => uploaded);
              yield* Fiber.interrupt(writing);
            } else {
              const result = yield* store.write(oid, body).pipe(Effect.result);
              assert.equal(result._tag, "Failure");
              if (result._tag === "Failure") assert.equal(result.failure._tag, "StorageFailure");
              if (mode === "abort-failure" && result._tag === "Failure")
                assert.equal(
                  result.failure,
                  sourceFailure,
                  "cleanup must preserve the primary failure",
                );
            }
          }).pipe(
            Effect.provide(
              Layer.mergeAll(r2({ bucket: wrapped, repo: namespace }), Logger.layer([logger])),
            ),
            Effect.scoped,
          ),
        );
        assert.equal(
          aborted,
          mode !== "abort-failure",
          "write settlement must await multipart abort",
        );
        const native = await opened;
        assert.equal(await bucket.head(native.key), null);
        if (mode === "abort-failure") {
          assert.ok(
            await bucket.head(lockKey(namespace)),
            "failed abort must keep the quota fence",
          );
          const retry = await Effect.runPromise(
            LfsStore.use((store) => store.write(oid, Stream.empty)).pipe(
              Effect.provide(r2({ bucket, repo: namespace })),
              Effect.result,
            ),
          );
          assert.equal(retry._tag, "Failure");
          if (retry._tag === "Failure") assert.equal(retry.failure._tag, "LfsQuotaBusy");
          assert.match(messages.join(""), /multipart_cleanup_failed/);
          assert.equal(messages.join("").includes(secret), false);
          assert.equal(messages.join("").includes(native.key), false);
          // Failed cleanup can leave billed parts; the operator must reconcile.
          await bucket
            .resumeMultipartUpload(native.key, native.uploadId)
            .uploadPart(2, new Uint8Array([1]));
        } else {
          assert.equal(await bucket.head(lockKey(namespace)), null);
          await assert.rejects(() =>
            bucket
              .resumeMultipartUpload(native.key, native.uploadId)
              .uploadPart(1, new Uint8Array([1])),
          );
        }
      } finally {
        await cleanup();
        // The isolated fixture has no remaining writers or multipart parts.
        await bucket.delete(lockKey(namespace));
      }
    });
};
