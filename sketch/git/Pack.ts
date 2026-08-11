/**
 * Packfile transport.
 *
 * Today `GitPackParser.parsePack` calls `#readFullStream` and concatenates the
 * whole packfile into one `Uint8Array` before it looks at the header
 * (`src/git.pack.ts:547`). A Durable Object gets 128 MiB; a push of a repo with
 * a few large blobs takes the isolate out, and there is no backpressure signal
 * to the client on the way down either.
 *
 * Sketch: the pack is a `Stream`. Chunks are pulled as the parser consumes
 * them, each object is written to `ObjectStore` as it resolves, and only the
 * delta base window stays resident. The same shape runs on the client, where
 * the constraint is the tab's memory instead of the isolate's.
 */
import { Effect, Stream } from "effect";
import { PackCorrupt } from "./Error.ts";
import { hashObject } from "./Format.ts";
import { ObjectStore, type ObjectType, type Oid, type RawObject } from "./Store.ts";

export interface PackEntry {
  readonly oid: Oid;
  readonly type: ObjectType;
  readonly offset: number;
  readonly size: number;
}

/**
 * Parse a packfile as it arrives, writing objects through to storage.
 *
 * Deltas are the only thing that has to be held: `REF_DELTA` bases are read
 * back from `ObjectStore` (they are already there — that is the invariant the
 * sender promises), `OFS_DELTA` bases are kept in a bounded LRU keyed by
 * offset, and anything that misses gets deferred to a second pass over the
 * objects already written rather than over the raw bytes.
 */
export const parse = (
  pack: Stream.Stream<Uint8Array, PackCorrupt>,
): Effect.Effect<ReadonlyArray<PackEntry>, PackCorrupt, ObjectStore> =>
  Effect.gen(function* () {
    const store = yield* ObjectStore;

    return yield* pack.pipe(
      // `decodeObjects` is a Channel: it owns the incremental inflate + header
      // state machine that `#parsePackObject` runs today, but yields objects
      // instead of indexing into a buffer it already holds in full.
      decodeObjects,
      Stream.mapEffect((object) =>
        Effect.gen(function* () {
          const oid = yield* hashObject(object);
          // Idempotent: re-pushing an object the server already has is a
          // no-op, which is what makes a resumed push cheap.
          const known = yield* store.has(oid).pipe(Effect.orDie);
          if (!known) yield* store.write(object).pipe(Effect.orDie);
          return {
            oid,
            type: object.type,
            offset: 0,
            size: object.data.length,
          } satisfies PackEntry;
        }),
      ),
      // The pack index is derived, so building it costs one pass and no
      // second copy of the data.
      Stream.runCollect,
    );
  });

declare const decodeObjects: (
  self: Stream.Stream<Uint8Array, PackCorrupt>,
) => Stream.Stream<RawObject, PackCorrupt, ObjectStore>;

/**
 * Build a packfile lazily for upload-pack.
 *
 * The response body is this stream handed straight to `HttpServerResponse`, so
 * the first bytes reach the client while the object walk is still running and
 * a client that hangs up cancels the walk — today the walk completes into an
 * array regardless.
 */
export const write = (
  oids: Stream.Stream<Oid, PackCorrupt>,
): Stream.Stream<Uint8Array, PackCorrupt, ObjectStore> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const store = yield* ObjectStore;
      const objects = oids.pipe(
        Stream.mapEffect((oid) => store.read(oid).pipe(Effect.orDie)),
        Stream.flatMap((object) => Stream.fromIterable(encodeEntry(object))),
      );
      return Stream.concat(header(0), objects);
    }),
  );

declare const header: (count: number) => Stream.Stream<Uint8Array, never>;
declare const encodeEntry: (object: RawObject) => ReadonlyArray<Uint8Array>;

/** pkt-line framing, unchanged in substance from `git.protocol.ts`. */
export const pktLine = {
  encode: (payload: Uint8Array): Uint8Array => {
    const length = payload.length + 4;
    const prefix = new TextEncoder().encode(length.toString(16).padStart(4, "0"));
    const out = new Uint8Array(length);
    out.set(prefix);
    out.set(payload, 4);
    return out;
  },
  flush: new TextEncoder().encode("0000"),
  /** Framing is a `Channel`, so a truncated frame is a typed failure, not a hang. */
  decode: (
    self: Stream.Stream<Uint8Array, PackCorrupt>,
  ): Stream.Stream<Uint8Array, PackCorrupt> => decodeFrames(self),
};

declare const decodeFrames: (
  self: Stream.Stream<Uint8Array, PackCorrupt>,
) => Stream.Stream<Uint8Array, PackCorrupt>;

export { PackCorrupt };
