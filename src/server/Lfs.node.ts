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

import { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { LfsStore } from "./Lfs.ts";

/** Fanned out two levels, the way git shards its own object directories. */
const locate = (root: string, oid: string): string =>
  path.join(root, oid.slice(0, 2), oid.slice(2, 4), oid);

export const file = (root: string): Layer.Layer<LfsStore> =>
  Layer.sync(LfsStore, () => {
    const failed = (operation: string, target: string) => (cause: unknown) =>
      new StorageFailure({ operation, path: target, cause });

    return LfsStore.of({
      head: (oid) =>
        Effect.tryPromise({
          try: async () => {
            const target = locate(root, oid);
            try {
              const stat = await fsp.stat(target);
              return { oid, size: stat.size };
            } catch {
              return null;
            }
          },
          catch: failed("lfs.head", oid),
        }),

      read: (oid) =>
        Effect.suspend(() => {
          const target = locate(root, oid);
          if (!fs.existsSync(target)) return Effect.fail(new ObjectNotFound({ oid }));
          return Effect.succeed(
            Stream.fromAsyncIterable(
              // SAFETY: a read stream opened without an encoding yields
              // Buffer chunks, which are Uint8Arrays; node types the async
              // iteration as `any`.
              fs.createReadStream(target) as AsyncIterable<Uint8Array>,
              (cause) => new StorageFailure({ operation: "lfs.read", path: oid, cause }),
            ),
          );
        }),

      write: (oid, body) =>
        Effect.gen(function* () {
          const target = locate(root, oid);
          const temporary = `${target}.${crypto.randomUUID()}.tmp`;

          const written = yield* Effect.tryPromise({
            try: async () => {
              await fsp.mkdir(path.dirname(target), { recursive: true });
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
                Readable.from(Stream.toAsyncIterable(body)),
                new Transform({
                  transform(chunk: Buffer, _encoding, done) {
                    digest.update(chunk);
                    size += chunk.length;
                    done(null, chunk);
                  },
                }),
                fs.createWriteStream(temporary),
              );

              return { actual: digest.digest("hex"), size };
            },
            catch: failed("lfs.write", oid),
          });

          if (written.actual !== oid) {
            yield* Effect.promise(() => fsp.rm(temporary, { force: true }));
            return yield* new Invalid({
              field: "oid",
              reason: `content hashes to ${written.actual}`,
            });
          }

          yield* Effect.tryPromise({
            try: () => fsp.rename(temporary, target),
            catch: failed("lfs.write", oid),
          });
          return { oid, size: written.size };
        }),
    });
  });
