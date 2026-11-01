import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Effect } from "effect";
import { LfsQuotaBusy, LfsQuotaExceeded, StorageFailure } from "../git/Error.ts";

import type { Limits } from "./LfsCapacity.ts";
export { configuration } from "./LfsCapacity.ts";

/** Exclusive across processes and path aliases; stale locks fail closed. */
export const reserve = Effect.fn("LfsCapacity.reserve")(function* (root: string, limits: Limits) {
  const lock = path.join(root, ".quota.lock");
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        await fs.mkdir(root, { recursive: true, mode: 0o700 });
        const handle = await fs.open(lock, "wx", 0o600);
        await handle.close();
      },
      catch: (cause) =>
        cause instanceof Error && "code" in cause && cause.code === "EEXIST"
          ? new LfsQuotaBusy({})
          : new StorageFailure({ operation: "lfs.quota.lock", path: root, cause }),
    }),
    () =>
      Effect.tryPromise(() => fs.unlink(lock)).pipe(
        Effect.catch(() =>
          Effect.logError("LFS quota lock cleanup failed; stop writers and reconcile storage", {
            component: "Lfs",
            event: "quota_lock_cleanup_failed",
          }),
        ),
      ),
  );
  const used = yield* Effect.callback<number, LfsQuotaExceeded | StorageFailure>(
    (resume, signal) => {
      const scan = async () => {
        let bytes = 0;
        let objects = 0;
        let entries = 0;
        const walk = async (directory: string, depth: number): Promise<void> => {
          signal.throwIfAborted();
          for await (const entry of await fs.opendir(directory)) {
            signal.throwIfAborted();
            if (depth === 0 && entry.name === ".quota.lock") continue;
            entries += 1;
            // Bound even a tree containing only empty directories.
            if (entries > limits.objects * 3 + 256)
              throw new LfsQuotaExceeded({ resource: "objects" });
            const target = path.join(directory, entry.name);
            const stat = await fs.lstat(target);
            signal.throwIfAborted();
            if (stat.isDirectory() && depth < 2 && /^[a-f0-9]{2}$/.test(entry.name)) {
              await walk(target, depth + 1);
            } else if (stat.isFile()) {
              objects += 1;
              bytes += stat.size;
              if (objects >= limits.objects) throw new LfsQuotaExceeded({ resource: "objects" });
              if (!Number.isSafeInteger(bytes) || bytes >= limits.bytes)
                throw new LfsQuotaExceeded({ resource: "bytes" });
            } else throw new Error("unsupported entry in LFS storage");
          }
        };
        await walk(root, 0);
        return bytes;
      };
      const settled = scan().then(
        (bytes) => resume(Effect.succeed(bytes)),
        (cause) =>
          resume(
            Effect.fail(
              cause instanceof LfsQuotaExceeded
                ? cause
                : new StorageFailure({ operation: "lfs.quota.scan", path: root, cause }),
            ),
          ),
      );
      // Cancellation closes active directory iterators and waits for the last
      // native read before the enclosing scope releases the persistent lock.
      return Effect.promise(() => settled);
    },
  );
  let remaining = limits.bytes - used;
  return (bytes: number): void => {
    if (bytes > remaining) throw new LfsQuotaExceeded({ resource: "bytes" });
    remaining -= bytes;
  };
});
