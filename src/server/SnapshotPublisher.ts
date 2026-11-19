/** Publication ordering for the Cloudflare read snapshot and recovery journal. */
import { Clock, Context, Effect, Layer, Result, Schema, Semaphore } from "effect";

import { type GitError, StorageFailure } from "../git/Error.ts";
import * as Telemetry from "./SnapshotTelemetry.ts";
import * as Capacity from "./SnapshotCapacity.ts";
import * as Snapshot from "./Snapshot.ts";

export class Store extends Context.Service<
  Store,
  {
    readonly read: (key: string) => Effect.Effect<Uint8Array | null, StorageFailure>;
    readonly write: (key: string, bytes: Uint8Array) => Effect.Effect<void, StorageFailure>;
    readonly remove: (key: string) => Effect.Effect<void, StorageFailure>;
    readonly sequence: (repo: string) => Effect.Effect<number, StorageFailure>;
    readonly setSequence: (repo: string, value: number) => Effect.Effect<void, StorageFailure>;
  }
>()("SnapshotPublisher/Store") {}

export const cloudflare = (
  bucket: R2Bucket,
  storage: Pick<DurableObjectStorage, "get" | "put">,
  bytesLimit = Capacity.defaults,
): Layer.Layer<Store> =>
  Layer.effect(
    Store,
    Effect.sync(() => {
      const attempt = <A>(key: string, operation: () => Promise<A>) =>
        Effect.tryPromise({
          try: operation,
          catch: (cause) => new StorageFailure({ operation: "snapshot", path: key, cause }),
        });
      return Store.of({
        read: Effect.fn("SnapshotPublisher.read")((key: string) =>
          attempt(key, async () => {
            const held = await bucket.get(key);
            return held === null ? null : await Capacity.read(held, bytesLimit);
          }),
        ),
        write: Effect.fn("SnapshotPublisher.write")((key: string, bytes: Uint8Array) =>
          attempt(key, async () => {
            Capacity.check(bytes.byteLength, bytesLimit);
            await bucket.put(key, bytes);
          }),
        ),
        remove: Effect.fn("SnapshotPublisher.remove")((key: string) =>
          attempt(key, () => bucket.delete(key)),
        ),
        sequence: Effect.fn("SnapshotPublisher.sequence")((repo: string) =>
          attempt(
            `snapshot-seq:${repo}`,
            async () => (await storage.get<number>(`snapshot-seq:${repo}`)) ?? 0,
          ),
        ),
        setSequence: Effect.fn("SnapshotPublisher.setSequence")((repo: string, value: number) =>
          attempt(`snapshot-seq:${repo}`, () => storage.put(`snapshot-seq:${repo}`, value)),
        ),
      });
    }),
  );

/** Construct once per writer, retaining its last successfully published views. */
export const make = Effect.fn("SnapshotPublisher.make")(function* (
  capture: (
    repo: string,
    previous?: Snapshot.Published,
  ) => Effect.Effect<Snapshot.Published, GitError>,
) {
  const store = yield* Store;
  const snapshots = new Map<string, Snapshot.Published>();
  const publishing = yield* Semaphore.make(1);

  const invalidate = Effect.fn("SnapshotPublisher.invalidate")(function* (repo: string) {
    let previous = snapshots.get(repo);
    if (previous === undefined) {
      const held = yield* store.read(Snapshot.keyOf(repo));
      if (held !== null) previous = Snapshot.decode(held) ?? undefined;
    }
    // Refuse the mutation if the old authority decision cannot be removed.
    // Deleting only after a failed publication would leave a public snapshot
    // readable during a write/delete outage after the repo became private.
    yield* store.remove(Snapshot.keyOf(repo));
    snapshots.delete(repo);
    return previous;
  });

  const publish = Effect.fn("SnapshotPublisher.publish")(function* (
    repo: string,
    previous: Snapshot.Published | undefined,
  ) {
    const started = yield* Clock.currentTimeMillis;
    const captured = yield* capture(repo, previous);
    const changed = previous === undefined || !Snapshot.same(previous, captured);
    if (changed) {
      const previousSequence = yield* Schema.decodeEffect(
        Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThan(10 ** 10 - 1)),
      )(yield* store.sequence(repo)).pipe(
        Effect.mapError(
          () =>
            new StorageFailure({
              operation: "snapshot.sequence",
              path: "snapshot-journal",
            }),
        ),
      );
      const seq = previousSequence + 1;
      // Make room before adding an entry. A failed delete must not advance
      // the sequence and permanently abandon the entry that should be retired.
      if (seq > Snapshot.RETAIN) {
        const retired = yield* Effect.result(
          store.remove(Snapshot.journalKeyOf(repo, seq - Snapshot.RETAIN)),
        );
        if (Result.isFailure(retired)) {
          yield* Effect.logWarning("snapshot journal retention failed", {
            component: "SnapshotPublisher",
            operation: "retention",
          });
          return;
        }
      }
      const entry = Snapshot.entryOf(seq, captured, previous);
      yield* store.write(Snapshot.journalKeyOf(repo, seq), Snapshot.encodeJournal(entry));
      yield* store.setSequence(repo, seq);
    }
    // Even an unchanged view needs replacement: invalidate removed it.
    yield* store.write(Snapshot.keyOf(repo), Snapshot.encode(captured));
    snapshots.set(repo, captured);
    yield* Telemetry.published(captured.publishedAt, started, changed);
  });

  // Hold the permit across invalidation, the mutation, and publication. A
  // delayed previous publication cannot reintroduce a stale public decision.
  // acquire/release finish uncancellable R2 calls before releasing the permit;
  // the request body and handler remain interruptible. Failed publication is
  // safe: the old snapshot was removed before anything could change authority.
  return Effect.fn("SnapshotPublisher.mutate")(function* <A, E, R>(
    repo: string,
    mutation: Effect.Effect<A, E, R>,
  ) {
    return yield* Effect.acquireUseRelease(
      invalidate(repo).pipe(
        Effect.tapError(() =>
          Effect.logError("snapshot invalidation failed; mutation refused", {
            component: "SnapshotPublisher",
            operation: "invalidate",
          }),
        ),
      ),
      () => mutation,
      (previous) =>
        publish(repo, previous).pipe(
          Effect.catchCause(() =>
            Effect.logError("snapshot publication failed; authoritative reads required", {
              component: "SnapshotPublisher",
              operation: "publish",
            }),
          ),
        ),
    );
  }, Semaphore.withPermit(publishing));
});
