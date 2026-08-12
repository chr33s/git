/**
 * Packs as storage, not just as transport.
 *
 * Before this, every backend exploded an incoming pack into loose objects:
 * one filesystem entry — or one R2 key — per object, forever. A repository
 * cloned from a real git server also could not be read at all if its objects
 * arrived packed, because nothing knew how to look inside a `.pack`.
 *
 * The shape is a decorator rather than a fifth backend, because "look in the
 * loose objects, then in each pack" is the same sentence whatever the bytes
 * live on. Each backend supplies `PackStore` — how to list packs and read
 * their bytes — and gets the read path for free.
 *
 * Loose wins on a tie. A repack writes the pack first and deletes the loose
 * copies second, so during the window where both exist they hold identical
 * bytes and either answer is right; if a repack is interrupted, the loose
 * copy is the one that is certainly complete.
 */
import { Context, Effect, Layer, Stream } from "effect";

import { ObjectNotFound, StorageFailure } from "./Error.ts";
import { type PackSource, readAt } from "./PackFile.ts";
import { findInPackIndex, parsePackIndex } from "./PackIndex.ts";
import { ObjectStore, type Oid, type RawObject, tracedObjectStore } from "./Store.ts";

export interface PackHandle {
  /** `pack-<sha>`, without an extension — the pair share a stem. */
  readonly name: string;
  /**
   * The `.idx` bytes, held whole.
   *
   * An index is ~28 bytes per object where the pack is unbounded, and every
   * lookup needs the fanout table, so this is the one part worth keeping
   * resident. The pack itself is only ever read in ranges.
   */
  readonly index: Uint8Array;
  readonly source: PackSource;
}

export class PackStore extends Context.Service<
  PackStore,
  {
    readonly list: Effect.Effect<ReadonlyArray<PackHandle>, StorageFailure>;
    readonly write: (input: {
      readonly name: string;
      readonly pack: Uint8Array;
      readonly index: Uint8Array;
    }) => Effect.Effect<void, StorageFailure>;
    readonly delete: (name: string) => Effect.Effect<void, StorageFailure>;
  }
>()("git/PackStore") {}

/** No packs, and none can be written: the default a backend opts out with. */
export const noPacks = Layer.succeed(PackStore, {
  list: Effect.succeed([]),
  write: () =>
    Effect.fail(
      new StorageFailure({
        operation: "packs.write",
        path: "",
        cause: "this backend has no packs",
      }),
    ),
  delete: () => Effect.void,
});

const failed = (operation: string, path: string) => (cause: unknown) =>
  new StorageFailure({ operation, path, cause });

/**
 * An `ObjectStore` that reads loose objects first and packs second.
 *
 * Writes always go loose. A pack is produced by repacking, not by appending —
 * the format has a count and a trailing checksum over the whole file, so
 * "add one object to a pack" is a rewrite of the pack.
 */
export const packed = (
  loose: ObjectStore["Service"],
  packs: PackStore["Service"],
  backend: string,
): ObjectStore["Service"] => {
  /**
   * Where an oid lives, if it lives in a pack.
   *
   * The fanout table makes this a binary search per pack rather than a scan,
   * which is the entire reason the `.idx` format exists.
   */
  const locate = (oid: Oid) =>
    Effect.gen(function* () {
      for (const handle of yield* packs.list) {
        const found = findInPackIndex(handle.index, oid);
        if (found._tag === "Failure") {
          return yield* Effect.fail(
            new StorageFailure({
              operation: "packs.lookup",
              path: handle.name,
              cause: found.failure,
            }),
          );
        }
        if (found.success !== null) return { handle, entry: found.success };
      }
      return null;
    });

  const fromPack = (oid: Oid) =>
    Effect.gen(function* () {
      const located = yield* locate(oid);
      if (located === null) return null;

      return yield* Effect.tryPromise({
        try: () =>
          readAt(located.handle.source, located.entry.offset, async (base) => {
            // A ref-delta whose base is not in this pack: look everywhere
            // else this store can see, which is what makes a thin pack
            // readable once it has been stored.
            const elsewhere = await Effect.runPromise(
              read(base).pipe(
                Effect.map((object): RawObject | null => object),
                Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
                Effect.catchTag("StorageFailure", () => Effect.succeed(null)),
              ),
            );
            return elsewhere;
          }),
        catch: failed("packs.read", located.handle.name),
      });
    });

  const read = (oid: Oid): Effect.Effect<RawObject, ObjectNotFound | StorageFailure> =>
    loose
      .read(oid)
      .pipe(
        Effect.catchTag("ObjectNotFound", () =>
          fromPack(oid).pipe(
            Effect.flatMap((found) =>
              found === null ? Effect.fail(new ObjectNotFound({ oid })) : Effect.succeed(found),
            ),
          ),
        ),
      );

  return tracedObjectStore(backend, {
    read,

    // A packed object is stored deflated and possibly as a delta, so there is
    // no byte range that is "the object" — it has to be reconstructed before
    // it can be streamed. Loose reads keep their streaming path.
    readStream: (oid) =>
      loose
        .readStream(oid)
        .pipe(
          Effect.catchTag("ObjectNotFound", () =>
            read(oid).pipe(Effect.map((object) => Stream.make(object.data))),
          ),
        ),

    write: loose.write,
    has: (oid) =>
      loose
        .has(oid)
        .pipe(
          Effect.flatMap((held) =>
            held ? Effect.succeed(true) : locate(oid).pipe(Effect.map((found) => found !== null)),
          ),
        ),

    // Deleting from a pack is a repack; `gc` owns that, and silently doing it
    // here would turn one delete into rewriting a gigabyte.
    delete: loose.delete,

    /**
     * `Stream.suspend`, so the dedupe set below belongs to one run of the
     * stream rather than to the store. Hoisting it would make the first
     * listing correct and every later one empty.
     */
    list: Stream.suspend(() => {
      // A repack in flight can leave an object both loose and packed, and a
      // caller counting objects should not see it twice.
      const seen = new Set<Oid>();

      return Stream.concat(
        loose.list,
        Stream.unwrap(
          packs.list.pipe(
            Effect.map((handles) =>
              Stream.fromIterable(handles).pipe(
                Stream.flatMap((handle) => {
                  const parsed = parsePackIndex(handle.index);
                  return parsed._tag === "Failure"
                    ? Stream.fail(
                        new StorageFailure({
                          operation: "packs.list",
                          path: handle.name,
                          cause: parsed.failure,
                        }),
                      )
                    : Stream.fromIterable(parsed.success.map((entry) => entry.oid));
                }),
              ),
            ),
          ),
        ),
      ).pipe(
        Stream.filter((oid: Oid) => {
          if (seen.has(oid)) return false;
          seen.add(oid);
          return true;
        }),
      );
    }),
  });
};
