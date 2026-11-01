import { Effect } from "effect";
import { LfsQuotaBusy, LfsQuotaExceeded, StorageFailure } from "../git/Error.ts";
import type { Limits } from "./LfsCapacity.ts";

export const lockKey = (repo: string): string => `${repo}/lfs/.quota.lock`;

/** Persistent exclusion: uncertain writes or aborts require offline reconciliation. */
export const reserve = Effect.fn("LfsCapacity.r2.reserve")(function* (
  bucket: R2Bucket,
  repo: string,
  limits: Limits,
) {
  const lock = lockKey(repo);
  let safeToRelease = true;
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const created = await bucket.put(lock, "", {
          onlyIf: new Headers({ "if-none-match": "*" }),
        });
        if (created === null) throw new LfsQuotaBusy({});
      },
      catch: (cause) =>
        cause instanceof LfsQuotaBusy
          ? cause
          : new StorageFailure({ operation: "lfs.quota.lock", path: repo, cause }),
    }),
    () =>
      safeToRelease
        ? Effect.tryPromise(() => bucket.delete(lock)).pipe(
            Effect.catch(() =>
              Effect.logError("LFS quota lock cleanup failed; stop writers and reconcile storage", {
                component: "Lfs",
                event: "quota_lock_cleanup_failed",
              }),
            ),
          )
        : Effect.logError(
            "LFS quota lock retained; reconcile uncertain uploads before removing it",
            {
              component: "Lfs",
              event: "quota_reconciliation_required",
            },
          ),
  );
  const used = yield* Effect.callback<number, LfsQuotaExceeded | StorageFailure>(
    (resume, signal) => {
      const scan = async () => {
        let bytes = 0;
        let objects = 0;
        let pages = 0;
        let cursor: string | undefined;
        for (;;) {
          signal.throwIfAborted();
          const page = await bucket.list({ prefix: `${repo}/lfs/`, limit: 1000, cursor });
          signal.throwIfAborted();
          pages += 1;
          if (pages > limits.objects + 2)
            throw new Error("LFS listing did not make bounded progress");
          for (const object of page.objects) {
            if (object.key === lock) continue;
            objects += 1;
            bytes += object.size;
            if (objects >= limits.objects) throw new LfsQuotaExceeded({ resource: "objects" });
            if (!Number.isSafeInteger(bytes) || bytes >= limits.bytes)
              throw new LfsQuotaExceeded({ resource: "bytes" });
          }
          if (!page.truncated) return bytes;
          if (page.cursor === cursor) throw new Error("LFS listing cursor did not advance");
          cursor = page.cursor;
        }
      };
      const settled = scan().then(
        (bytes) => resume(Effect.succeed(bytes)),
        (cause) =>
          resume(
            Effect.fail(
              cause instanceof LfsQuotaExceeded
                ? cause
                : new StorageFailure({ operation: "lfs.quota.scan", path: repo, cause }),
            ),
          ),
      );
      // R2 listing cannot be aborted; retain admission and the lock until its
      // pending call settles, then stop before requesting another page.
      return Effect.promise(() => settled);
    },
  );
  let remaining = limits.bytes - used;
  return {
    consume: (bytes: number): void => {
      if (bytes > remaining) throw new LfsQuotaExceeded({ resource: "bytes" });
      remaining -= bytes;
    },
    storagePending: () => {
      safeToRelease = false;
    },
    storageSettled: () => {
      safeToRelease = true;
    },
  };
});
