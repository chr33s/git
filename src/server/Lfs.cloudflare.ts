/**
 * LFS objects in R2, beside the git objects.
 *
 * An LFS object is large by definition and a Durable Object has 128 MiB, so
 * the upload is never held whole. It cannot stream into `bucket.put` either:
 * R2 refuses a stream whose length it is not told up front, and a chunked
 * upload has none to tell. So it goes to R2 as a multipart upload, one
 * `PART_BYTES` part resident at a time.
 *
 * Verification uses `crypto.DigestStream`, which is the Workers primitive for
 * hashing something you are not holding: every chunk is fed to the digest as
 * it goes to R2. A multipart upload is not an object until it is completed,
 * so one whose content does not match the name it was given is aborted and
 * was never servable, and an already-verified object under that name is
 * never touched.
 */
import { bytesToHex, concatBytes } from "../git/Format.ts";
import { Effect, Layer, Stream } from "effect";

import { Invalid, LfsQuotaExceeded, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import * as Capacity from "./LfsCapacity.cloudflare.ts";
import { configuration, type Limits } from "./LfsCapacity.ts";
import { LfsStore } from "./Lfs.ts";

export interface CloudflareLfsOptions {
  readonly bucket: R2Bucket;
  readonly repo: string;
  readonly capacity?: Limits;
}

/**
 * One multipart part. R2 needs every part but the last to be the same size
 * and at least 5 MiB; this is the memory one upload holds at a time.
 */
const PART_BYTES = 8 * 1024 * 1024;

const hex = (buffer: ArrayBuffer): string => bytesToHex(new Uint8Array(buffer));

interface DigestStream extends WritableStream<Uint8Array> {
  readonly digest: Promise<ArrayBuffer>;
}

/**
 * `crypto.DigestStream` is a workerd extension of the Web Crypto API. The
 * ambient `Crypto` in scope here is the standard one, which has no such
 * member, so the extension is declared locally — the same seam the R2
 * binding cast crosses in `host/Cloudflare.ts`.
 */
interface WorkerdCrypto extends Crypto {
  readonly DigestStream: new (algorithm: string) => DigestStream;
}

// SAFETY: this layer only ever runs on workerd, where the runtime `crypto`
// carries the `DigestStream` constructor the standard declaration omits.
const digestStream = (algorithm: string): DigestStream =>
  new (crypto as WorkerdCrypto).DigestStream(algorithm);

export const r2 = (options: CloudflareLfsOptions): Layer.Layer<LfsStore> =>
  Layer.effect(
    LfsStore,
    Effect.gen(function* () {
      const limits = options.capacity ?? (yield* configuration().pipe(Effect.orDie));
      const key = (oid: string) => `${options.repo}/lfs/${oid}`;
      const failed = (operation: string, oid: string) => (cause: unknown) =>
        new StorageFailure({ operation, path: key(oid), cause });

      return LfsStore.of({
        head: (oid) =>
          Effect.tryPromise({
            try: async () => {
              const found = await options.bucket.head(key(oid));
              return found === null ? null : { oid, size: found.size };
            },
            catch: failed("lfs.head", oid),
          }),

        read: (oid) =>
          Effect.tryPromise({
            try: () => options.bucket.get(key(oid)),
            catch: failed("lfs.read", oid),
          }).pipe(
            Effect.flatMap((object) =>
              object === null || object.body === null
                ? Effect.fail(new ObjectNotFound({ oid }))
                : Effect.succeed(
                    Stream.fromReadableStream({
                      // SAFETY: R2 serves an object's bytes; the runtime types
                      // declare `body` without a chunk type.
                      evaluate: () => object.body as ReadableStream<Uint8Array>,
                      onError: (cause) =>
                        new StorageFailure({ operation: "lfs.read", path: key(oid), cause }),
                    }),
                  ),
            ),
          ),

        write: Effect.fn("Lfs.r2.write")(function* (
          oid: string,
          body: Stream.Stream<Uint8Array, StorageFailure>,
        ) {
          const quota = yield* Capacity.reserve(options.bucket, options.repo, limits);
          let upload: R2MultipartUpload | null = null;
          let completed = false;
          let digestClosed = false;
          const parts: R2UploadedPart[] = [];
          let pending: Uint8Array[] = [];
          let buffered = 0;
          let size = 0;

          const send = async (bytes: Uint8Array): Promise<void> => {
            // Retain the handle before uploadPart: even failure of the first
            // part must leave cleanup able to abort the multipart upload.
            quota.storagePending();
            upload ??= await options.bucket.createMultipartUpload(key(oid));
            parts.push(await upload.uploadPart(parts.length + 1, bytes));
          };
          return yield* Effect.acquireUseRelease(
            Effect.sync(() => {
              const digest = digestStream("SHA-256");
              // Abort rejects this promise. Observe it immediately, while still
              // awaiting the original promise when validating completed content.
              void digest.digest.catch(() => undefined);
              return { digest, hashing: digest.getWriter() };
            }),
            ({ digest, hashing }) =>
              Effect.gen(function* () {
                yield* body.pipe(
                  Stream.runForEach((chunk) =>
                    Effect.tryPromise({
                      try: async () => {
                        quota.consume(chunk.length);
                        await hashing.write(chunk);
                        size += chunk.length;
                        pending.push(chunk);
                        buffered += chunk.length;
                        while (buffered >= PART_BYTES) {
                          const joined = concatBytes(pending);
                          await send(joined.subarray(0, PART_BYTES));
                          pending = [joined.subarray(PART_BYTES)];
                          buffered -= PART_BYTES;
                        }
                      },
                      catch: (cause) =>
                        cause instanceof LfsQuotaExceeded ? cause : failed("lfs.write", oid)(cause),
                    }).pipe(Effect.uninterruptible),
                  ),
                );
                const actual = yield* Effect.tryPromise({
                  try: async () => {
                    await hashing.close();
                    digestClosed = true;
                    return hex(await digest.digest);
                  },
                  catch: failed("lfs.hash", oid),
                }).pipe(Effect.uninterruptible);
                if (actual !== oid)
                  return yield* new Invalid({
                    field: "oid",
                    reason: `content hashes to ${actual}`,
                  });
                yield* Effect.tryPromise({
                  try: async () => {
                    const rest = concatBytes(pending);
                    quota.storagePending();
                    if (upload === null) await options.bucket.put(key(oid), rest);
                    else {
                      if (rest.length > 0) await send(rest);
                      await upload.complete(parts);
                    }
                    completed = true;
                    quota.storageSettled();
                  },
                  catch: failed("lfs.write", oid),
                }).pipe(Effect.uninterruptible);
                return { oid, size };
              }),
            ({ hashing }) =>
              Effect.gen(function* () {
                // Each in-flight R2 call is uninterruptible, so these handles cannot
                // appear or mutate after the finalizer has inspected them.
                const unfinished = upload;
                if (!completed && unfinished !== null)
                  yield* Effect.tryPromise({
                    try: async () => {
                      await unfinished.abort();
                      quota.storageSettled();
                    },
                    catch: failed("lfs.abort", oid),
                  }).pipe(
                    Effect.catch(() =>
                      Effect.logError(
                        "LFS multipart cleanup failed; reconcile incomplete uploads",
                        {
                          component: "Lfs",
                          event: "multipart_cleanup_failed",
                          operation: "abort",
                        },
                      ),
                    ),
                  );
                if (!digestClosed)
                  yield* Effect.promise(() => hashing.abort().catch(() => undefined));
                hashing.releaseLock();
              }),
          );
        }, Effect.scoped),
      });
    }),
  );
