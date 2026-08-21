/**
 * Packfile transport, platform-neutral.
 *
 * The pack is consumed as a stream: chunks are pulled as the parser needs
 * them, each object is written to `ObjectStore` as it resolves, and delta
 * bases are re-read from the store by oid rather than pinned in memory — so
 * only the object currently being decoded is ever resident. A Durable Object
 * gets 128 MiB, a browser tab less, and a transfer must not be bounded by
 * either.
 *
 * Object boundaries inside a pack are implicit: each object's data is its own
 * zlib stream, and the next object starts wherever that stream ends.
 * `git/Inflate.ts` is pull-based, so it consumes exactly the stream's bytes
 * and the boundary falls out by construction; `git/Sha1.ts` hashes the
 * trailer incrementally. Nothing here imports `node:*` — the same module
 * runs in node, workerd and browsers, which is what lets a browser clone.
 *
 * Writing emits full objects by default: valid by the format, larger on the
 * wire, and the right trade on the request path, where delta search would
 * spend CPU and latency ahead of the first byte. `PackOptions.deltify` turns
 * on an ofs-delta sliding window for the caller that can afford it — repack,
 * where the work is background and the bytes are storage. Per-object deflate
 * is `CompressionStream("deflate")`, available everywhere this runs.
 */
import { Effect, Result, Schema, Stream } from "effect";

import { type ObjectNotFound, PackCorrupt, type StorageFailure } from "./Error.ts";
import { bytesToHex, concatBytes as concat, hashObject } from "./Format.ts";
import { type ByteSource, inflate as zlibInflate, InflateError } from "./Inflate.ts";
import { bufferSource, readAt } from "./PackFile.ts";
import { buildPackIndex, crc32, type PackIndexEntry } from "./PackIndex.ts";
import { PackStore } from "./Packed.ts";
import { Sha1 } from "./Sha1.ts";
import { ObjectStore, type ObjectType, type Oid, type RawObject } from "./Store.ts";

const TYPE_CODES = { commit: 1, tree: 2, blob: 3, tag: 4 } as const satisfies Record<
  ObjectType,
  number
>;
/** Keyed by the wire's type code, which is why lookups can miss. */
const CODE_TYPES = new Map<number, ObjectType>([
  [1, "commit"],
  [2, "tree"],
  [3, "blob"],
  [4, "tag"],
]);
const OFS_DELTA = 6;
const REF_DELTA = 7;

const readU32 = (bytes: Uint8Array, at: number): number =>
  ((bytes[at]! << 24) | (bytes[at + 1]! << 16) | (bytes[at + 2]! << 8) | bytes[at + 3]!) >>> 0;

type ObjectHeader =
  | { readonly kind: "full"; readonly type: ObjectType; readonly size: number }
  | { readonly kind: "ofs"; readonly distance: number; readonly size: number }
  | { readonly kind: "ref"; readonly base: Oid; readonly size: number };

/**
 * Pull-based byte source over the incoming chunks.
 *
 * Every byte that belongs to the pack body is fed to a running SHA-1 as it is
 * consumed, so by the time the parser reaches the 20-byte trailer the digest
 * of everything before it is already known — no buffering, no second pass.
 */
class Source {
  readonly #iterator: AsyncIterator<Uint8Array>;
  #pending: Uint8Array[] = [];
  readonly #hash = new Sha1();
  /** Absolute offset of the next unconsumed byte; ofs-delta bases point here. */
  offset = 0;

  constructor(input: AsyncIterable<Uint8Array>) {
    this.#iterator = input[Symbol.asyncIterator]();
  }

  corrupt(reason: string): PackCorrupt {
    return new PackCorrupt({ reason, offset: this.offset });
  }

  /** Next raw chunk — pending first, then the upstream. `null` at end of input. */
  async #next(): Promise<Uint8Array | null> {
    const head = this.#pending.shift();
    if (head !== undefined) return head;
    const result = await this.#iterator.next();
    if (result.done === true) return null;
    return result.value.length === 0 ? this.#next() : result.value;
  }

  #pushBack(chunks: ReadonlyArray<Uint8Array>): void {
    this.#pending.unshift(...chunks.filter((chunk) => chunk.length > 0));
  }

  #consume(bytes: Uint8Array): void {
    this.#hash.update(bytes);
    this.offset += bytes.length;
  }

  async #gather(n: number): Promise<Uint8Array> {
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const chunk = await this.#next();
      if (chunk === null) throw this.corrupt(`truncated: needed ${n - filled} more bytes`);
      const use = Math.min(chunk.length, n - filled);
      out.set(chunk.subarray(0, use), filled);
      this.#pushBack([chunk.subarray(use)]);
      filled += use;
    }
    return out;
  }

  /** Exactly `n` bytes, consumed into the running digest. */
  async take(n: number): Promise<Uint8Array> {
    const bytes = await this.#gather(n);
    this.#consume(bytes);
    return bytes;
  }

  async byte(): Promise<number> {
    return (await this.take(1))[0]!;
  }

  async header(): Promise<number> {
    const magic = await this.take(4);
    if (magic[0] !== 0x50 || magic[1] !== 0x41 || magic[2] !== 0x43 || magic[3] !== 0x4b) {
      throw this.corrupt("bad magic: not a packfile");
    }
    const version = readU32(await this.take(4), 0);
    if (version !== 2) throw this.corrupt(`unsupported pack version ${version}`);
    return readU32(await this.take(4), 0);
  }

  async objectHeader(): Promise<ObjectHeader> {
    let byte = await this.byte();
    const code = (byte >> 4) & 0x7;
    let size = byte & 0x0f;
    let shift = 4;
    while (byte & 0x80) {
      byte = await this.byte();
      size += (byte & 0x7f) * 2 ** shift;
      shift += 7;
    }

    if (code === OFS_DELTA) {
      byte = await this.byte();
      let distance = byte & 0x7f;
      while (byte & 0x80) {
        byte = await this.byte();
        distance = (distance + 1) * 128 + (byte & 0x7f);
      }
      return { kind: "ofs", distance, size };
    }
    if (code === REF_DELTA) {
      // SAFETY: `take` either returns exactly 20 bytes or throws, and hex-encoding
      // them yields exactly the 40 lowercase hex characters an oid is.
      return { kind: "ref", base: bytesToHex(await this.take(20)) as Oid, size };
    }

    const type = CODE_TYPES.get(code);
    if (type === undefined) throw this.corrupt(`unknown object type code ${code}`);
    return { kind: "full", type, size };
  }

  /**
   * One zlib stream from the current position. The pull-based inflate
   * consumes exactly the stream's bytes — the tracking here only exists to
   * feed the consumed prefix into the pack digest afterwards.
   */
  async inflate(limit?: number): Promise<Uint8Array> {
    const fed: Uint8Array[] = [];
    let returned = 0;
    const adapter: ByteSource = {
      next: async () => {
        const chunk = await this.#next();
        if (chunk !== null) fed.push(chunk);
        return chunk;
      },
      pushBack: (bytes) => {
        returned += bytes.length;
        this.#pushBack([bytes]);
      },
    };

    try {
      const out = await zlibInflate(adapter, limit);
      const consumed = fed.reduce((total, chunk) => total + chunk.length, 0) - returned;
      let seen = 0;
      for (const chunk of fed) {
        if (seen + chunk.length <= consumed) this.#consume(chunk);
        else if (seen < consumed) this.#consume(chunk.subarray(0, consumed - seen));
        seen += chunk.length;
      }
      return out;
    } catch (error) {
      throw error instanceof InflateError ? this.corrupt(error.reason) : error;
    }
  }

  /** The 20-byte trailer: SHA-1 of everything before it. */
  async trailer(): Promise<void> {
    const expected = this.#hash.digestHex();
    const actual = bytesToHex(await this.#gather(20));
    if (actual !== expected) {
      throw new PackCorrupt({
        reason: `checksum mismatch: pack says ${actual}, content hashes to ${expected}`,
        offset: this.offset,
      });
    }
  }
}

/**
 * Apply a git delta (the payload of an ofs-delta or ref-delta object) to its
 * base. Pure byte work, so it returns a `Result` like the codecs in
 * `Format.ts`.
 */

export const applyDelta = (
  base: Uint8Array,
  delta: Uint8Array,
): Result.Result<Uint8Array, PackCorrupt> => {
  const corrupt = (reason: string) => Result.fail(new PackCorrupt({ reason }));
  let position = 0;

  const varint = (): number | null => {
    let value = 0;
    let shift = 0;
    for (;;) {
      const byte = delta[position];
      if (byte === undefined) return null;
      position++;
      value += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return value;
      shift += 7;
    }
  };

  const baseSize = varint();
  if (baseSize === null) return corrupt("delta truncated in base size");
  if (baseSize !== base.length) {
    return corrupt(`delta expects a ${baseSize}-byte base, got ${base.length}`);
  }
  const targetSize = varint();
  if (targetSize === null) return corrupt("delta truncated in target size");
  /**
   * What the instructions can actually produce.
   *
   * The declared size is a claim by whoever wrote the pack, and the buffer is
   * allocated before a byte of it is justified — so a sixty-byte delta could
   * ask for gigabytes. A fixed cap is the wrong answer: git's own threshold is
   * far larger than anything safe here, so capping rejects packs stock git
   * emits. Counting first is exact — a copy can produce no more than the base
   * holds, an insert no more than the delta carries — and the total has to be
   * what was declared.
   */
  const produced = (() => {
    let at = position;
    let total = 0;
    while (at < delta.length) {
      const command = delta[at++]!;
      if (command === 0) return null;
      if (command & 0x80) {
        let offset = 0;
        let size = 0;
        for (let bit = 0; bit < 4; bit++) {
          if (command & (1 << bit)) {
            const byte = delta[at++];
            if (byte === undefined) return null;
            offset |= byte << (bit * 8);
          }
        }
        for (let bit = 0; bit < 3; bit++) {
          if (command & (1 << (bit + 4))) {
            const byte = delta[at++];
            if (byte === undefined) return null;
            size |= byte << (bit * 8);
          }
        }
        const length = size === 0 ? 0x10000 : size;
        if (offset + length > base.length) return null;
        total += length;
      } else {
        at += command;
        if (at > delta.length) return null;
        total += command;
      }
    }
    return total;
  })();

  if (produced === null) return corrupt("delta instructions run past their input");
  if (produced !== targetSize) {
    return corrupt(`delta declares ${targetSize} bytes; its instructions produce ${produced}`);
  }

  const target = new Uint8Array(targetSize);
  let written = 0;
  while (position < delta.length) {
    const command = delta[position++]!;

    if (command & 0x80) {
      let offset = 0;
      let size = 0;
      for (let bit = 0; bit < 4; bit++) {
        if (command & (1 << bit)) {
          const byte = delta[position++];
          if (byte === undefined) return corrupt("copy instruction truncated");
          offset += byte * 2 ** (8 * bit);
        }
      }
      for (let bit = 0; bit < 3; bit++) {
        if (command & (0x10 << bit)) {
          const byte = delta[position++];
          if (byte === undefined) return corrupt("copy instruction truncated");
          size += byte * 2 ** (8 * bit);
        }
      }
      if (size === 0) size = 0x10000;
      if (offset + size > base.length) return corrupt("copy reaches past the base");
      if (written + size > targetSize) return corrupt("copy reaches past the target size");
      target.set(base.subarray(offset, offset + size), written);
      written += size;
    } else if (command !== 0) {
      if (position + command > delta.length) return corrupt("insert instruction truncated");
      if (written + command > targetSize) return corrupt("insert reaches past the target size");
      target.set(delta.subarray(position, position + command), written);
      position += command;
      written += command;
    } else {
      return corrupt("delta opcode 0 is reserved");
    }
  }

  if (written !== targetSize) {
    return corrupt(`delta produced ${written} bytes, header says ${targetSize}`);
  }
  return Result.succeed(target);
};

/** Blocks this long index the base for `createDelta`'s copy search. */
const DELTA_BLOCK = 16;
/** A copy instruction names at most this many bytes; size 0 encodes it. */
const MAX_COPY = 0x10000;
/** An insert instruction carries at most 127 literal bytes. */
const MAX_INSERT = 127;

/** The little-endian 7-bit varint `applyDelta` reads sizes with. */
export const sizeVarint = (value: number): number[] => {
  const bytes: number[] = [];
  let rest = value;
  for (;;) {
    const low = rest % 128;
    rest = Math.floor(rest / 128);
    if (rest === 0) {
      bytes.push(low);
      return bytes;
    }
    bytes.push(low | 0x80);
  }
};

/** FNV-1a over one block: cheap, and a collision only costs a byte compare. */
const blockHash = (bytes: Uint8Array, at: number): number => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < DELTA_BLOCK; index++) {
    hash ^= bytes[at + index]!;
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
};

/**
 * The block index `createDelta` searches. Computed once per base and reused
 * across every target that tries it — a window slot outlives roughly a
 * window's worth of targets, and indexing is the O(base) half of the search.
 */
export type DeltaIndex = ReadonlyMap<number, ReadonlyArray<number>>;

export const indexBase = (base: Uint8Array): DeltaIndex => {
  const index = new Map<number, number[]>();
  for (let at = 0; at + DELTA_BLOCK <= base.length; at += DELTA_BLOCK) {
    const key = blockHash(base, at);
    const slot = index.get(key);
    if (slot === undefined) index.set(key, [at]);
    // A base full of one repeated block would otherwise collect every
    // offset here and turn the scan quadratic; any handful of them is as
    // good as all of them.
    else if (slot.length < 64) slot.push(at);
  }
  return index;
};

/**
 * Produce a delta whose application to `base` yields `target` — the inverse
 * of `applyDelta`, in the same copy/insert vocabulary — or `null` when the
 * result is not worth having: a delta that saves less than a tenth of the
 * target costs a base read at every future access of the object and buys
 * almost nothing back, and `options.limit` lets a caller holding a better
 * candidate cap the size tighter still, so a losing search aborts early.
 *
 * The search is greedy: the base's 16-byte block index (`options.index`, or
 * computed here) is probed at each target position, and the longest
 * block-anchored match becomes a copy. Bytes no block matches accumulate
 * into inserts, written into a buffer pre-sized to the budget — exceeding
 * it is the bail-out, not a resize. This is the shape of git's own delta
 * search, without its heuristics for sliding the anchor — simplicity over
 * the last few percent.
 */
export const createDelta = (
  base: Uint8Array,
  target: Uint8Array,
  options?: { readonly index?: DeltaIndex; readonly limit?: number },
): Uint8Array | null => {
  if (base.length === 0 || target.length < DELTA_BLOCK) return null;

  const budget = Math.min(
    Math.floor(target.length * 0.9),
    options?.limit ?? Number.MAX_SAFE_INTEGER,
  );
  const header = [...sizeVarint(base.length), ...sizeVarint(target.length)];
  if (header.length >= budget) return null;

  const index = options?.index ?? indexBase(base);

  const out = new Uint8Array(budget);
  out.set(header, 0);
  let written = header.length;
  let pendingStart = 0;
  let position = 0;

  const flushInsert = (upTo: number): boolean => {
    let from = pendingStart;
    while (from < upTo) {
      const length = Math.min(MAX_INSERT, upTo - from);
      if (written + 1 + length > budget) return false;
      out[written++] = length;
      out.set(target.subarray(from, from + length), written);
      written += length;
      from += length;
    }
    pendingStart = upTo;
    return true;
  };

  const emitCopy = (offset: number, size: number): boolean => {
    let command = 0x80;
    const operands: number[] = [];
    for (let bit = 0; bit < 4; bit++) {
      const byte = Math.floor(offset / 2 ** (8 * bit)) % 256;
      if (byte !== 0) {
        command |= 1 << bit;
        operands.push(byte);
      }
    }
    // `MAX_COPY` travels as size zero — the one value the three size bytes
    // cannot spell, which is why `applyDelta` reads zero as 0x10000.
    const encoded = size === MAX_COPY ? 0 : size;
    for (let bit = 0; bit < 3; bit++) {
      const byte = Math.floor(encoded / 2 ** (8 * bit)) % 256;
      if (byte !== 0) {
        command |= 0x10 << bit;
        operands.push(byte);
      }
    }
    if (written + 1 + operands.length > budget) return false;
    out[written++] = command;
    for (const byte of operands) out[written++] = byte;
    return true;
  };

  while (position < target.length) {
    let bestAt = -1;
    let bestLength = 0;
    if (position + DELTA_BLOCK <= target.length) {
      for (const candidate of index.get(blockHash(target, position)) ?? []) {
        const limit = Math.min(base.length - candidate, target.length - position);
        let length = 0;
        while (length < limit && base[candidate + length] === target[position + length]) length++;
        if (length >= DELTA_BLOCK && length > bestLength) {
          bestLength = length;
          bestAt = candidate;
        }
      }
    }

    if (bestAt === -1) {
      position++;
      // A literal run that alone exceeds the budget cannot be saved by
      // anything that follows it.
      if (position - pendingStart > budget) return null;
      continue;
    }

    if (!flushInsert(position)) return null;
    let offset = bestAt;
    let remaining = bestLength;
    while (remaining > 0) {
      const size = Math.min(MAX_COPY, remaining);
      if (!emitCopy(offset, size)) return null;
      offset += size;
      remaining -= size;
    }
    position += bestLength;
    pendingStart = position;
  }

  if (!flushInsert(target.length)) return null;
  return out.slice(0, written);
};

/**
 * Ingest a version-2 packfile: every object — full, ofs-delta, ref-delta —
 * lands in `ObjectStore` as it is decoded, and the trailing checksum is
 * verified against the running digest. Returns the oids in pack order.
 *
 * Thin packs work for free: a ref-delta whose base is not in the pack reads
 * it from the store, and fails with `ObjectNotFound` if it is nowhere at all.
 */
export const unpack = <E>(
  input: Stream.Stream<Uint8Array, E>,
): Effect.Effect<ReadonlyArray<Oid>, PackCorrupt | ObjectNotFound | StorageFailure, ObjectStore> =>
  Effect.gen(function* () {
    const store = yield* ObjectStore;
    const source = new Source(Stream.toAsyncIterable(input));
    const step = <A>(run: () => Promise<A>) =>
      Effect.tryPromise({
        try: run,
        catch: (cause) =>
          Schema.is(PackCorrupt)(cause) ? cause : new PackCorrupt({ reason: String(cause) }),
      });

    const count = yield* step(() => source.header());
    /** Object boundaries seen so far, for resolving ofs-delta bases. */
    const oidAt = new Map<number, Oid>();
    const oids: Oid[] = [];

    for (let index = 0; index < count; index++) {
      const start = source.offset;
      const header = yield* step(() => source.objectHeader());
      // Bounded by what the header declared: the size check below happens
      // after the stream is inflated, so without this a few megabytes of pack
      // can expand to gigabytes before anything compares them.
      const data = yield* step(() => source.inflate(header.size + 1));
      if (data.length !== header.size) {
        return yield* new PackCorrupt({
          reason: `object ${index}: header says ${header.size} bytes, inflated to ${data.length}`,
          offset: start,
        });
      }

      let object: RawObject;
      if (header.kind === "full") {
        object = { type: header.type, data };
      } else {
        const baseOid = header.kind === "ref" ? header.base : oidAt.get(start - header.distance);
        if (baseOid === undefined) {
          return yield* new PackCorrupt({
            reason: `object ${index}: ofs-delta base is not an object boundary`,
            offset: start,
          });
        }
        const base = yield* store.read(baseOid);
        const resolved = yield* Effect.fromResult(applyDelta(base.data, data));
        object = { type: base.type, data: resolved };
      }

      const oid = yield* store.write(object);
      oidAt.set(start, oid);
      oids.push(oid);
    }

    yield* step(() => source.trailer());
    return oids;
  });

/** The two thresholds `ingest` decides retention by; see its doc. */
export interface IngestOptions {
  /** Packs with fewer objects than this are exploded loose. Default 8. */
  readonly retainAtLeast?: number;
  /** Packs larger than this many bytes stream loose. Default 64 MiB. */
  readonly retainUpTo?: number;
}

const RETAIN_AT_LEAST = 8;
const RETAIN_UP_TO = 64 * 1024 * 1024;

const asCorrupt = (cause: unknown): PackCorrupt =>
  Schema.is(PackCorrupt)(cause) ? cause : new PackCorrupt({ reason: String(cause) });

/**
 * Ingest a packfile, keeping it *as a pack* when that is the better shape.
 *
 * `unpack` above explodes every push into loose objects — one store write
 * per object, forever, which on an object store priced and paced per PUT is
 * the expensive half of a push. But the pack already is a storage format:
 * verified, indexed and registered with `PackStore`, its cost is two writes
 * however many objects it carries, and `Packed.ts` reads through it — thin
 * ref-deltas included, resolved against the rest of the store.
 *
 * Retention is a judgment, not a rule, and every refusal falls back to
 * `unpack` rather than failing the push:
 *
 *   - a pack below `retainAtLeast` objects goes loose — a one-commit push
 *     is a handful of writes either way, and a pack per tiny push grows the
 *     set every lookup consults;
 *   - a pack above `retainUpTo` streams loose — retention must buffer the
 *     bytes to index them, and a transfer must not be bounded by this
 *     optimization's memory;
 *   - a backend whose pack store cannot take the write (or has none) goes
 *     loose, which is exactly what it did before this existed.
 *
 * The cost of keeping the pack is one extra decode: the boundary walk that
 * indexes it must inflate every object (a zlib stream's end is only found
 * by reading it), and each object is then resolved once more to learn its
 * oid — with delta bases re-resolved along the chain rather than pinned,
 * for the same memory reason `unpack` gives. CPU is bought once at push
 * time; writes are saved forever after.
 */
export const ingest = <E>(
  input: Stream.Stream<Uint8Array, E>,
  options?: IngestOptions,
): Effect.Effect<
  ReadonlyArray<Oid>,
  PackCorrupt | ObjectNotFound | StorageFailure,
  ObjectStore | PackStore
> =>
  Effect.gen(function* () {
    const atLeast = options?.retainAtLeast ?? RETAIN_AT_LEAST;
    const upTo = options?.retainUpTo ?? RETAIN_UP_TO;

    // Buffer up to the cap. The iterator is held so that a pack too large
    // to retain continues, un-rewound, into the streaming path below.
    const iterator = Stream.toAsyncIterable(input)[Symbol.asyncIterator]();
    const buffered: Uint8Array[] = [];
    let total = 0;
    let ended = false;
    yield* Effect.tryPromise({
      try: async () => {
        while (total <= upTo) {
          const next = await iterator.next();
          if (next.done === true) {
            ended = true;
            return;
          }
          buffered.push(next.value);
          total += next.value.length;
        }
      },
      catch: asCorrupt,
    });

    if (!ended) {
      // Too large to hold: replay what was buffered, then the live tail.
      const replay = async function* (): AsyncIterable<Uint8Array> {
        yield* buffered;
        for (;;) {
          const next = await iterator.next();
          if (next.done === true) return;
          yield next.value;
        }
      };
      return yield* unpack(Stream.fromAsyncIterable(replay(), asCorrupt));
    }

    const bytes = concat(buffered);
    // The object count sits in the header; a pack too small to say so is
    // `unpack`'s to refuse with its own words.
    const count = bytes.length >= 12 ? readU32(bytes, 8) : 0;
    if (bytes.length < 32 || count < atLeast) {
      return yield* unpack(Stream.make(bytes));
    }

    return yield* retain(bytes).pipe(
      // The pack store said no — absent, full, or briefly unreachable. The
      // push still lands the way every push always has; only the shape of
      // the storage degrades.
      Effect.catchTag("StorageFailure", () => unpack(Stream.make(bytes))),
    );
  });

/**
 * Keep one fully-buffered pack: verify it, index it, register it.
 *
 * Two passes over bytes already in memory. The first walks the boundaries —
 * inflating each object to find where the next begins — and checks the
 * trailing checksum against the running digest. The second resolves each
 * entry back out of the pack (`readAt`, the same reader every later access
 * uses) to learn its oid, reaching into the store for a thin delta's base
 * exactly as the read path will. Only then does anything get written: a
 * pack whose index this function built is a pack it has already proven it
 * can read.
 */
const retain = (
  bytes: Uint8Array,
): Effect.Effect<
  ReadonlyArray<Oid>,
  PackCorrupt | ObjectNotFound | StorageFailure,
  ObjectStore | PackStore
> =>
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const packs = yield* PackStore;

    const source = new Source(
      (async function* () {
        yield bytes;
      })(),
    );
    const step = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: asCorrupt });

    const declared = yield* step(() => source.header());
    const starts: number[] = [];
    for (let index = 0; index < declared; index++) {
      const start = source.offset;
      starts.push(start);
      const header = yield* step(() => source.objectHeader());
      const data = yield* step(() => source.inflate(header.size + 1));
      if (data.length !== header.size) {
        return yield* new PackCorrupt({
          reason: `object ${index}: header says ${header.size} bytes, inflated to ${data.length}`,
          offset: start,
        });
      }
    }
    yield* step(() => source.trailer());
    const trailer = bytes.subarray(bytes.length - 20);

    const context = yield* Effect.context<never>();
    const pack = bufferSource(bytes);
    /** Boundaries whose oid is known, for in-pack ref-delta bases. */
    const known = new Map<Oid, number>();
    const readEntry = (offset: number, depth: number): Promise<RawObject> =>
      readAt(
        pack,
        offset,
        async (base, at) => {
          const inPack = known.get(base);
          if (inPack !== undefined) return await readEntry(inPack, at);
          // A thin delta: the base lives in the store — the same place the
          // read path will find it once this pack is registered.
          return await Effect.runPromiseWith(context)(
            objects.read(base).pipe(
              Effect.map((object): RawObject | null => object),
              Effect.catchTag("ObjectNotFound", () => Effect.succeed<RawObject | null>(null)),
            ),
          );
        },
        depth,
        packs.inflate,
      );

    const entries: Array<PackIndexEntry> = [];
    const oids: Array<Oid> = [];
    for (let index = 0; index < starts.length; index++) {
      const start = starts[index]!;
      const end = index + 1 < starts.length ? starts[index + 1]! : bytes.length - 20;
      const object = yield* Effect.tryPromise({
        try: () => readEntry(start, 0),
        catch: asCorrupt,
      });
      const oid = yield* hashObject(object);
      known.set(oid, start);
      oids.push(oid);
      entries.push({ oid, offset: start, crc32: crc32(bytes.subarray(start, end)) });
    }

    yield* packs.write({
      name: `pack-${bytesToHex(trailer)}`,
      pack: bytes,
      index: buildPackIndex(entries, trailer),
    });
    return oids;
  });

const encodeObjectHeader = (code: number, size: number): Uint8Array => {
  const bytes: number[] = [];
  let current = (code << 4) | (size & 0x0f);
  let rest = Math.floor(size / 16);
  while (rest > 0) {
    bytes.push(current | 0x80);
    current = rest & 0x7f;
    rest = Math.floor(rest / 128);
  }
  bytes.push(current);
  return Uint8Array.from(bytes);
};

/** One-shot per-object deflate; boundary detection is only a reading problem. */
// SAFETY: every byte source in this module is allocated with `new Uint8Array`
// or sliced from one, so the backing buffer is a plain `ArrayBuffer` — the
// `Blob` constructor's type merely cannot see that through the generic default.
const deflate = async (bytes: Uint8Array): Promise<Uint8Array> =>
  new Uint8Array(
    await new Response(
      new Blob([bytes as Uint8Array<ArrayBuffer>])
        .stream()
        .pipeThrough(new CompressionStream("deflate")),
    ).arrayBuffer(),
  );

/**
 * Emit a version-2 packfile for the given objects, already-walked and in
 * order. The stream is lazy: each object is read from the store and deflated
 * as the consumer pulls, so the first bytes leave before the last object is
 * read — and a consumer that hangs up stops the reads.
 */
export interface PackedEntry {
  readonly oid: Oid;
  /** Byte offset of this object's header within the pack. */
  readonly offset: number;
  /** CRC32 of the object's stored bytes, as the `.idx` records it. */
  readonly crc32: number;
}

export interface DeltaOptions {
  /** How many recent same-type objects to try as bases. Default 10. */
  readonly window?: number;
  /** Objects larger than this are stored whole and never window. Default 1 MiB. */
  readonly maxSize?: number;
  /** Longest allowed base chain; readers cap at 64. Default 50. */
  readonly maxDepth?: number;
}

export interface PackOptions {
  /**
   * Called as each object is written, in pack order. This is how a repack
   * collects what it needs for the `.idx` without a second pass.
   */
  readonly onObject?: (entry: PackedEntry) => void;
  /**
   * Store objects as ofs-deltas against a sliding window of recent
   * same-type objects when the delta wins by at least half. Off by default:
   * the window pins `window × maxSize` bytes and the search costs CPU per
   * object, both of which belong in a background repack rather than ahead
   * of a fetch response's first byte.
   */
  readonly deltify?: DeltaOptions;
}

/** The reverse of `objectHeader`'s ofs-delta distance read. */
export const encodeOfsDistance = (distance: number): Uint8Array => {
  const bytes = [distance % 128];
  let rest = Math.floor(distance / 128) - 1;
  while (rest >= 0) {
    bytes.unshift(0x80 | (rest % 128));
    rest = Math.floor(rest / 128) - 1;
  }
  return Uint8Array.from(bytes);
};

/** Below this a delta cannot beat its own header overhead. */
const MIN_DELTA_TARGET = 64;

export const pack = (
  oids: ReadonlyArray<Oid>,
  options?: PackOptions,
): Stream.Stream<Uint8Array, ObjectNotFound | StorageFailure, ObjectStore> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const store = yield* ObjectStore;
      const hash = new Sha1();
      const emit = (bytes: Uint8Array): Uint8Array => {
        hash.update(bytes);
        return bytes;
      };

      const header = new Uint8Array(12);
      header.set([0x50, 0x41, 0x43, 0x4b]); // "PACK"
      header[7] = 2; // version
      header[8] = (oids.length >>> 24) & 0xff;
      header[9] = (oids.length >>> 16) & 0xff;
      header[10] = (oids.length >>> 8) & 0xff;
      header[11] = oids.length & 0xff;

      // Where the next object starts, which only the writer knows — and
      // which is exactly what an `.idx` records. Reporting it as the pack is
      // produced is what lets a repack build the index without a second pass
      // over bytes it would otherwise have to keep.
      let offset = header.length;

      const deltify = options?.deltify;
      const windowSize = deltify?.window ?? 10;
      const maxSize = deltify?.maxSize ?? 1024 * 1024;
      const maxDepth = deltify?.maxDepth ?? 50;

      /**
       * The window: recent objects kept raw with their block index built
       * once on admission, the offset a delta against them will name, and
       * the chain depth one would inherit. Bases must already be in the
       * pack — an ofs-delta points backwards — so candidacy and emission
       * order agree by construction. Objects too small to ever be a delta
       * target are also kept out as bases: they would only evict candidates
       * that can actually win.
       */
      interface Candidate {
        readonly type: ObjectType;
        readonly data: Uint8Array;
        readonly index: DeltaIndex;
        readonly offset: number;
        readonly depth: number;
      }
      const window: Candidate[] = [];

      const spell = async (
        object: RawObject,
      ): Promise<{ readonly bytes: Uint8Array; readonly depth: number }> => {
        if (
          deltify !== undefined &&
          object.data.length >= MIN_DELTA_TARGET &&
          object.data.length <= maxSize
        ) {
          let best: { readonly delta: Uint8Array; readonly base: Candidate } | null = null;
          // Newest first, and each candidate is capped by the best delta
          // found so far: a later candidate that cannot beat it aborts its
          // scan early instead of completing a delta only to be discarded.
          // The half-of-target cap is the acceptance rule — a delta that
          // wins by less keeps a base resolution on every future read for
          // very little saved.
          for (let at = window.length - 1; at >= 0; at--) {
            const base = window[at]!;
            if (base.type !== object.type || base.depth >= maxDepth) continue;
            const delta = createDelta(base.data, object.data, {
              index: base.index,
              limit: Math.min(
                Math.floor(object.data.length / 2),
                (best?.delta.length ?? Number.MAX_SAFE_INTEGER) - 1,
              ),
            });
            if (delta === null) continue;
            best = { delta, base };
          }
          if (best !== null) {
            return {
              bytes: concat([
                encodeObjectHeader(OFS_DELTA, best.delta.length),
                encodeOfsDistance(offset - best.base.offset),
                await deflate(best.delta),
              ]),
              depth: best.base.depth + 1,
            };
          }
        }
        return {
          bytes: concat([
            encodeObjectHeader(TYPE_CODES[object.type], object.data.length),
            await deflate(object.data),
          ]),
          depth: 0,
        };
      };

      const objects = Stream.fromIterable(oids).pipe(
        Stream.mapEffect((oid) =>
          store.read(oid).pipe(
            Effect.flatMap((object) =>
              Effect.promise(async () => {
                const { bytes, depth } = await spell(object);
                options?.onObject?.({ oid, offset, crc32: crc32(bytes) });
                if (
                  deltify !== undefined &&
                  object.data.length >= MIN_DELTA_TARGET &&
                  object.data.length <= maxSize
                ) {
                  window.push({
                    type: object.type,
                    data: object.data,
                    index: indexBase(object.data),
                    offset,
                    depth,
                  });
                  if (window.length > windowSize) window.shift();
                }
                offset += bytes.length;
                return emit(bytes);
              }),
            ),
          ),
        ),
      );

      const trailer = Stream.fromEffect(Effect.sync(() => Uint8Array.from(hash.digest())));

      return Stream.fromIterable([emit(header)]).pipe(
        Stream.concat(objects),
        Stream.concat(trailer),
      );
    }),
  );
