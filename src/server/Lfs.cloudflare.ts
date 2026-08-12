/**
 * LFS objects in R2, beside the git objects.
 *
 * The upload streams straight into `bucket.put` — an LFS object is large by
 * definition and a Durable Object has 128 MiB, so buffering it is the failure
 * mode this whole design exists to avoid.
 *
 * Verification uses `crypto.DigestStream`, which is the Workers primitive for
 * hashing something you are not holding: the body is teed, one branch goes to
 * R2 and the other to the digest, and an object whose content does not match
 * the name it was given is deleted again rather than left to be served.
 */
import { Effect, Layer, Stream } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { LfsStore } from "./Lfs.ts";

export interface CloudflareLfsOptions {
  readonly bucket: R2Bucket;
  readonly repo: string;
}

const hex = (buffer: ArrayBuffer): string =>
  [...new Uint8Array(buffer)].map((byte) => byte.toString(16).padStart(2, "0")).join("");

/**
 * `crypto.DigestStream` is a workerd global. The ambient `Crypto` in scope
 * here is the standard one, which has no such member, so the shape is named
 * locally — the same seam the R2 binding cast crosses in `host/Cloudflare.ts`.
 */
interface DigestStream extends WritableStream<Uint8Array> {
  readonly digest: Promise<ArrayBuffer>;
}
const digestStream = (algorithm: string): DigestStream =>
  new (crypto as unknown as { DigestStream: new (name: string) => DigestStream }).DigestStream(
    algorithm,
  );

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
              const source = Stream.toReadableStream(body) as ReadableStream<Uint8Array>;
              const [toBucket, toDigest] = source.tee();

              const digest = digestStream("SHA-256");
              const hashing = toDigest.pipeTo(digest);

              const stored = await options.bucket.put(key(oid), toBucket);
              await hashing;

              return { actual: hex(await digest.digest), size: stored?.size ?? 0 };
            },
            catch: failed("lfs.write", oid),
          });

          if (written.actual !== oid) {
            // Never leave a mis-named object behind: the next download would
            // serve it as though the hash had been checked.
            yield* Effect.promise(() => options.bucket.delete(key(oid)));
            return yield* new Invalid({
              field: "oid",
              reason: `content hashes to ${written.actual}`,
            });
          }

          return { oid, size: written.size };
        }),
    });
  });
