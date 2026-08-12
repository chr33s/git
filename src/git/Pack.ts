/**
 * Packfile transport — phase 3.
 *
 * The pack is consumed as a stream: chunks are pulled as the parser needs
 * them, each object is written to `ObjectStore` as it resolves, and delta
 * bases are re-read from the store by oid rather than pinned in memory — so
 * only the object currently being decoded is ever resident. A Durable Object
 * gets 128 MiB, and a push must not be bounded by it.
 *
 * Object boundaries inside a pack are implicit: each object's data is its own
 * zlib stream, and the next object starts wherever that stream ends. The
 * parser leans on `node:zlib`'s `Inflate` (available under workerd's
 * `nodejs_compat` as well), which ends exactly at the deflate terminator and
 * reports the consumed byte count as `bytesWritten` — no second pass, no
 * hand-rolled inflate.
 *
 * Writing emits full objects only, no deltas: valid by the format, larger on
 * the wire, and enough for upload-pack until delta compression pays its way.
 */
import { createHash } from "node:crypto";
import { createInflate, deflateSync } from "node:zlib";

import { Effect, Result, Stream } from "effect";

import { type ObjectNotFound, PackCorrupt, type StorageFailure } from "./Error.ts";
import { bytesToHex } from "./Format.ts";
import { ObjectStore, type ObjectType, type Oid, type RawObject } from "./Store.ts";

const TYPE_CODES: Record<ObjectType, number> = { commit: 1, tree: 2, blob: 3, tag: 4 };
const CODE_TYPES: Record<number, ObjectType> = { 1: "commit", 2: "tree", 3: "blob", 4: "tag" };
const OFS_DELTA = 6;
const REF_DELTA = 7;

const concat = (parts: ReadonlyArray<Uint8Array>): Uint8Array => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

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
  readonly #hash = createHash("sha1");
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
      return { kind: "ref", base: bytesToHex(await this.take(20)) as Oid, size };
    }

    const type = CODE_TYPES[code];
    if (type === undefined) throw this.corrupt(`unknown object type code ${code}`);
    return { kind: "full", type, size };
  }

  /**
   * One zlib stream from the current position. `Inflate` ends itself at the
   * deflate terminator; whatever was fed past it is pushed back for the next
   * object, and exactly the consumed bytes go into the digest.
   */
  inflate(): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      const inflater = createInflate();
      const fed: Uint8Array[] = [];
      let inFlight: Uint8Array | null = null;
      const out: Uint8Array[] = [];
      let done = false;

      const finish = (error?: unknown) => {
        if (done) return;
        done = true;
        const consumed = inflater.bytesWritten;
        inflater.removeAllListeners();
        inflater.close();
        if (error !== undefined) {
          if (error instanceof PackCorrupt) reject(error);
          else reject(this.corrupt(error instanceof Error ? error.message : JSON.stringify(error)));
          return;
        }

        const leftovers: Uint8Array[] = [];
        let seen = 0;
        for (const chunk of fed) {
          if (seen + chunk.length <= consumed) this.#consume(chunk);
          else if (seen >= consumed) leftovers.push(chunk);
          else {
            this.#consume(chunk.subarray(0, consumed - seen));
            leftovers.push(chunk.subarray(consumed - seen));
          }
          seen += chunk.length;
        }
        if (inFlight !== null) leftovers.push(inFlight);
        this.#pushBack(leftovers);
        resolve(concat(out));
      };

      inflater.on("error", (error) => finish(error));
      inflater.on("data", (chunk: Uint8Array) => out.push(chunk));
      inflater.on("end", () => finish());

      const pump = async () => {
        while (!done) {
          const chunk = await this.#next();
          if (done) {
            // `end` fired while we were pulling; the chunk was never fed.
            inFlight = chunk;
            return;
          }
          if (chunk === null) {
            // Upstream is exhausted; let zlib decide whether the stream was
            // complete (`end`) or cut short (`error`).
            inflater.end();
            return;
          }
          fed.push(chunk);
          if (!inflater.write(chunk)) {
            await new Promise<void>((drained) => inflater.once("drain", drained));
          }
        }
      };
      void pump().catch((error: unknown) => finish(error));
    });
  }

  /** The 20-byte trailer: SHA-1 of everything before it. */
  async trailer(): Promise<void> {
    const expected = this.#hash.digest("hex");
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
          cause instanceof PackCorrupt ? cause : new PackCorrupt({ reason: String(cause) }),
      });

    const count = yield* step(() => source.header());
    /** Object boundaries seen so far, for resolving ofs-delta bases. */
    const oidAt = new Map<number, Oid>();
    const oids: Oid[] = [];

    for (let index = 0; index < count; index++) {
      const start = source.offset;
      const header = yield* step(() => source.objectHeader());
      const data = yield* step(() => source.inflate());
      if (data.length !== header.size) {
        return yield* Effect.fail(
          new PackCorrupt({
            reason: `object ${index}: header says ${header.size} bytes, inflated to ${data.length}`,
            offset: start,
          }),
        );
      }

      let object: RawObject;
      if (header.kind === "full") {
        object = { type: header.type, data };
      } else {
        const baseOid = header.kind === "ref" ? header.base : oidAt.get(start - header.distance);
        if (baseOid === undefined) {
          return yield* Effect.fail(
            new PackCorrupt({
              reason: `object ${index}: ofs-delta base is not an object boundary`,
              offset: start,
            }),
          );
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
    return oids as ReadonlyArray<Oid>;
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

/**
 * Emit a version-2 packfile for the given objects, already-walked and in
 * order. The stream is lazy: each object is read from the store and deflated
 * as the consumer pulls, so the first bytes leave before the last object is
 * read — and a consumer that hangs up stops the reads.
 */
export const pack = (
  oids: ReadonlyArray<Oid>,
): Stream.Stream<Uint8Array, ObjectNotFound | StorageFailure, ObjectStore> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const store = yield* ObjectStore;
      const hash = createHash("sha1");
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

      const objects = Stream.fromIterable(oids).pipe(
        Stream.mapEffect((oid) =>
          store
            .read(oid)
            .pipe(
              Effect.map((object) =>
                emit(
                  concat([
                    encodeObjectHeader(TYPE_CODES[object.type], object.data.length),
                    new Uint8Array(deflateSync(object.data)),
                  ]),
                ),
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
