/**
 * LFS objects as files, beside the repository they belong to.
 *
 * Its own module for `node:fs`, like `Subscribers.node.ts`.
 *
 * The upload is written to a temp file while `node:crypto` hashes the same
 * bytes, and only renamed into place once the digest matches the name the
 * client claimed. A rejected upload therefore leaves nothing behind, and a
 * stored object is one whose content has been checked at least once.
 */
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

import { Effect, Layer, Stream } from "effect";

import { Invalid, LfsQuotaExceeded, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import * as Capacity from "./LfsCapacity.node.ts";
import { LfsStore } from "./Lfs.ts";

/** Fanned out two levels, the way git shards its own object directories. */
const locate = (root: string, oid: string): string =>
  path.join(root, oid.slice(0, 2), oid.slice(2, 4), oid);

export const file = (root: string): Layer.Layer<LfsStore> =>
  Layer.effect(
    LfsStore,
    Effect.gen(function* () {
      const capacity = yield* Capacity.configuration().pipe(Effect.orDie);
      const failed = (operation: string, target: string) => (cause: unknown) =>
        new StorageFailure({ operation, path: target, cause });

      return LfsStore.of({
        head: (oid) =>
          Effect.tryPromise({
            try: async () => {
              const target = locate(root, oid);
              try {
                const stat = await fsp.stat(target);
                if (!stat.isFile()) throw new Error("LFS object is not a regular file");
                return { oid, size: stat.size };
              } catch (error) {
                if (error instanceof Error && "code" in error && error.code === "ENOENT")
                  return null;
                throw error;
              }
            },
            catch: failed("lfs.head", oid),
          }),

        read: (oid) =>
          Effect.gen(function* () {
            const target = locate(root, oid);
            const exists = yield* Effect.tryPromise({
              try: async () => {
                try {
                  const stat = await fsp.stat(target);
                  if (!stat.isFile()) throw new Error("LFS object is not a regular file");
                  return true;
                } catch (error) {
                  if (error instanceof Error && "code" in error && error.code === "ENOENT")
                    return false;
                  throw error;
                }
              },
              catch: failed("lfs.read", oid),
            });
            if (!exists) return yield* new ObjectNotFound({ oid });
            return Stream.fromAsyncIterable(
              // SAFETY: a read stream opened without an encoding yields
              // Buffer chunks, which are Uint8Arrays; node types the async
              // iteration as `any`.
              fs.createReadStream(target) as AsyncIterable<Uint8Array>,
              (cause) => new StorageFailure({ operation: "lfs.read", path: oid, cause }),
            );
          }),

        write: (oid, body) => {
          const target = locate(root, oid);
          const temporary = `${target}.${crypto.randomUUID()}.tmp`;
          return Effect.gen(function* () {
            const reserve = yield* Capacity.reserve(root, capacity);
            const written = yield* Effect.callback<
              { readonly actual: string; readonly size: number },
              StorageFailure | LfsQuotaExceeded
            >((resume, signal) => {
              const source = Stream.toAsyncIterable(body)[Symbol.asyncIterator]();
              const finished = (async () => {
                await fsp.mkdir(path.dirname(target), {
                  recursive: true,
                  mode: 0o700,
                });
                const digest = createHash("sha256");
                let size = 0;

                // One pass: the bytes go to disk and through the digest
                // together, so nothing is buffered and nothing is read twice.
                // The file stream is *part* of the pipeline rather than driven
                // from inside a `Writable`, so a write that fails — a full disk,
                // most of all — fails the upload. Swallowing it would let the
                // digest, which is computed over what was fed rather than over
                // what landed, promote a truncated file under a verified hash.
                await pipeline(
                  Readable.from({ [Symbol.asyncIterator]: () => source }),
                  new Transform({
                    transform(chunk: Buffer, _encoding, done) {
                      try {
                        reserve(chunk.length);
                      } catch (error) {
                        done(error instanceof Error ? error : new Error("LFS capacity exceeded"));
                        return;
                      }
                      digest.update(chunk);
                      size += chunk.length;
                      done(null, chunk);
                    },
                  }),
                  // flush waits for fsync before close; pipeline propagates sync
                  // errors, so a failed sync cannot promote the temporary file.
                  fs.createWriteStream(temporary, { flags: "wx", mode: 0o600, flush: true }),
                  { signal },
                );

                return {
                  actual: digest.digest("hex"),
                  size,
                };
              })().then(
                (value) => resume(Effect.succeed(value)),
                (cause) =>
                  resume(
                    Effect.fail(
                      cause instanceof LfsQuotaExceeded ? cause : failed("lfs.write", oid)(cause),
                    ),
                  ),
              );
              // Abort alone cannot unblock a source waiting for its next chunk.
              // Close its Effect iterator explicitly, then wait for the disk
              // pipeline to close before the outer finalizer removes its file.
              return Effect.promise(async () => {
                try {
                  await source.return?.();
                } finally {
                  await finished;
                }
              });
            });

            if (written.actual !== oid) {
              return yield* new Invalid({
                field: "oid",
                reason: `content hashes to ${written.actual}`,
              });
            }

            yield* Effect.tryPromise({
              try: async () => {
                await fsp.rename(temporary, target);
                // Persist the object name, then each directory entry leading to
                // it. A directory-sync failure is an unknown upload outcome: the
                // complete object may be visible, but success is not acknowledged.
                // A previous failed upload may have left any ancestor unsynced.
                // Existence on retry (including in a fresh process) proves no
                // durability boundary, so repeat the entire physical chain.
                let directory = await fsp.realpath(path.dirname(target));
                for (;;) {
                  const handle = await fsp.open(directory, "r");
                  try {
                    await handle.sync();
                  } finally {
                    await handle.close();
                  }
                  const parent = path.dirname(directory);
                  if (parent === directory) break;
                  directory = parent;
                }
              },
              catch: failed("lfs.write", oid),
            }).pipe(Effect.uninterruptible);
            return { oid, size: written.size };
          }).pipe(
            // Whatever stopped the upload short of the rename — a mismatch, a
            // client that hung up, a full disk, an interruption — its partial
            // file is removed when finalizers can run. Process death can leave
            // an ignored private temporary file for offline operator cleanup.
            // After a rename there is nothing here, and `force` makes that a no-op.
            Effect.ensuring(
              Effect.tryPromise(() => fsp.rm(temporary, { force: true })).pipe(
                Effect.catch(() =>
                  Effect.logError("LFS upload cleanup failed; reconcile temporary objects", {
                    component: "Lfs",
                    event: "upload_cleanup_failed",
                  }),
                ),
              ),
            ),
            Effect.scoped,
          );
        },
      });
    }),
  );
