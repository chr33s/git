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

import { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { LfsStore } from "./Lfs.ts";

export interface CloudflareLfsOptions {
  readonly bucket: R2Bucket;
  readonly repo: string;
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
  Layer.sync(LfsStore, () => {
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

      write: (oid, body) =>
        Effect.gen(function* () {
          const written = yield* Effect.tryPromise({
            try: async () => {
              const digest = digestStream("SHA-256");
              const hashing = digest.getWriter();
              let upload: R2MultipartUpload | null = null;
              const parts: R2UploadedPart[] = [];
              let pending: Uint8Array[] = [];
              let buffered = 0;
              let size = 0;

              /** One part, opening the upload on the first; the upload is returned. */
              const send = async (
                open: R2MultipartUpload | null,
                bytes: Uint8Array,
              ): Promise<R2MultipartUpload> => {
                const started = open ?? (await options.bucket.createMultipartUpload(key(oid)));
                parts.push(await started.uploadPart(parts.length + 1, bytes));
                return started;
              };

              try {
                for await (const chunk of Stream.toAsyncIterable(body)) {
                  await hashing.write(chunk);
                  size += chunk.length;
                  pending.push(chunk);
                  buffered += chunk.length;
                  while (buffered >= PART_BYTES) {
                    const joined = concatBytes(pending);
                    upload = await send(upload, joined.subarray(0, PART_BYTES));
                    pending = [joined.subarray(PART_BYTES)];
                    buffered -= PART_BYTES;
                  }
                }
                await hashing.close();
                const actual = hex(await digest.digest);

                if (actual !== oid) {
                  // Nothing was ever visible under the key: a multipart
                  // upload is not an object until it completes.
                  await upload?.abort();
                  return { actual, size };
                }

                const rest = concatBytes(pending);
                if (upload === null) {
                  await options.bucket.put(key(oid), rest);
                } else {
                  if (rest.length > 0) upload = await send(upload, rest);
                  await upload.complete(parts);
                }
                return { actual, size };
              } catch (error) {
                // The parts already sent are billed storage until aborted,
                // and the original failure is the one worth reporting.
                await upload?.abort().catch(() => undefined);
                throw error;
              }
            },
            catch: failed("lfs.write", oid),
          });

          if (written.actual !== oid) {
            return yield* new Invalid({
              field: "oid",
              reason: `content hashes to ${written.actual}`,
            });
          }
          return { oid, size: written.size };
        }),
    });
  });
