/** Publication ordering for the Cloudflare read snapshot and recovery journal. */
import { Context, Effect, Layer, Semaphore } from "effect";

import { type GitError, StorageFailure } from "../git/Error.ts";
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
            return held === null ? null : new Uint8Array(await held.arrayBuffer());
          }),
        ),
        write: Effect.fn("SnapshotPublisher.write")((key: string, bytes: Uint8Array) =>
          attempt(key, () => bucket.put(key, bytes)).pipe(Effect.asVoid),
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

  // R2 operations yield to other requests. Keep capture, sequence allocation,
  // journal/latest writes and failure cleanup in one ordered publication.
  // Once acquired, finish even if the requester leaves: an uncancellable R2
  // write must not outlive this permit and overwrite a later publication.
  return Effect.fn("SnapshotPublisher.publish")(
    function* (repo: string) {
      yield* Effect.gen(function* () {
        let previous = snapshots.get(repo);
        if (previous === undefined) {
          const held = yield* store
            .read(Snapshot.keyOf(repo))
            .pipe(Effect.orElseSucceed(() => null));
          if (held !== null) previous = Snapshot.decode(held) ?? undefined;
        }
        const captured = yield* capture(repo, previous);
        if (previous !== undefined && Snapshot.same(previous, captured)) return;

        // Publish the journal before latest, and retain only its bounded window.
        const seq = (yield* store.sequence(repo)) + 1;
        const entry = Snapshot.entryOf(seq, captured, previous);
        yield* store.write(Snapshot.journalKeyOf(repo, seq), Snapshot.encodeJournal(entry));
        yield* store.setSequence(repo, seq);
        if (seq > Snapshot.RETAIN) {
          yield* store
            .remove(Snapshot.journalKeyOf(repo, seq - Snapshot.RETAIN))
            .pipe(Effect.ignore);
        }
        yield* store.write(Snapshot.keyOf(repo), Snapshot.encode(captured));
        snapshots.set(repo, captured);
      }).pipe(
        Effect.catch(() =>
          Effect.gen(function* () {
            snapshots.delete(repo);
            yield* store.remove(Snapshot.keyOf(repo)).pipe(Effect.ignore);
          }),
        ),
        Effect.ignoreCause,
      );
    },
    Effect.uninterruptible,
    Semaphore.withPermit(publishing),
  );
});
