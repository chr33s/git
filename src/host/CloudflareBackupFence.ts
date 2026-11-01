/** Per-object backup admission. Administrative operations require a private trusted caller. */
import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Effect, Exit, Result, Schema } from "effect";
import { StorageFailure } from "../git/Error.ts";
import * as ResponseDelivery from "./ResponseDelivery.ts";

export const key = "git-plus:backup-fence:v1";
export const Marker = Schema.Struct({
  version: Schema.Literal(1),
  snapshotId: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  phase: Schema.Literals(["draining", "held"]),
});
type Storage = Pick<DurableObjectStorage, "kv" | "sync">;
const failed = () => new StorageFailure({ operation: "backup.fence", path: "backup" });

export const make = (storage: Storage) => {
  let active = 0;
  let administrative = false;
  let uncertainRelease = false;
  let drained: (() => void) | undefined;
  const marker = () => {
    const value = storage.kv.get(key);
    return value === undefined ? undefined : Schema.decodeUnknownSync(Marker)(value);
  };
  const lease = () => {
    active += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      active -= 1;
      if (active === 0) {
        drained?.();
        drained = undefined;
      }
    };
  };
  const enter = () => {
    try {
      if (administrative || uncertainRelease || marker() !== undefined) throw failed();
      return lease();
    } catch {
      throw failed();
    }
  };
  /** Only register descendants of work that already holds a lease. */
  const followup = () => {
    if (active === 0) throw failed();
    return lease();
  };
  const control = async (operation: () => Promise<void>) => {
    if (administrative) throw failed();
    administrative = true;
    try {
      await operation();
    } catch {
      throw failed();
    } finally {
      administrative = false;
    }
  };
  const hold = (snapshotId: string) =>
    control(async () => {
      const next = Schema.decodeUnknownSync(Marker)({ version: 1, snapshotId, phase: "draining" });
      const existing = marker();
      if (existing !== undefined && existing.snapshotId !== snapshotId) throw failed();
      storage.kv.put(key, next);
      await storage.sync();
      if (active !== 0)
        await new Promise<void>((resolve) => {
          drained = resolve;
        });
      storage.kv.put(key, { ...next, phase: "held" });
      await storage.sync();
    });
  const finishRelease = (snapshotId: string, abandon: boolean) =>
    control(async () => {
      const existing = marker();
      // Release is a postcondition: an absent fence needs no mutation. This
      // makes a lost successful acknowledgement retryable after eviction.
      if (existing === undefined) {
        if (uncertainRelease) throw failed();
        await storage.sync();
        return;
      }
      if (existing.snapshotId !== snapshotId) throw failed();
      if (abandon) {
        // Cancel only this existing capture, never create a new hold. Existing
        // requests and their followups must finish before admission can reopen.
        if (active !== 0)
          await new Promise<void>((resolve) => {
            drained = resolve;
          });
        await storage.sync();
      } else if (active !== 0 || existing.phase !== "held") throw failed();
      storage.kv.delete(key);
      try {
        await storage.sync();
        uncertainRelease = false;
      } catch {
        // An uncertain release must not reopen admission in this activation.
        uncertainRelease = true;
        storage.kv.put(key, existing);
        throw failed();
      }
    });
  const respond = <E, R>(work: Effect.Effect<Response, E, R>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const acquired = yield* Effect.try({ try: enter, catch: failed }).pipe(Effect.result);
        if (Result.isFailure(acquired))
          return new Response("repository backup in progress", {
            status: 503,
            headers: { "cache-control": "no-store", "retry-after": "1" },
          });
        const done = acquired.success;
        return yield* restore(work).pipe(
          Effect.map((response) => ResponseDelivery.track(response, done)),
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Effect.sync(done) : Effect.void)),
        );
      }),
    );
  return {
    enter,
    followup,
    hold,
    release: (snapshotId: string) => finishRelease(snapshotId, false),
    abandon: (snapshotId: string) => finishRelease(snapshotId, true),
    respond,
  };
};
