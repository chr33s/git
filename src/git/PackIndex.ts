/**
 * The `.idx` v2 companion to a packfile.
 *
 * A packfile on its own can only be read front to back: object boundaries are
 * implicit in the zlib streams, so reaching the last object means inflating
 * every object before it. The index is what turns a pack from transport into
 * storage — a fanout table and a sorted oid table answer "where does this oid
 * live" in two reads and a binary search, with no scan of the pack at all.
 *
 * Pure synchronous byte work, like `Format.ts`: plain functions returning
 * `Result`, hashing through `Sha1.ts` rather than Web Crypto so nothing here
 * has to become async.
 */
import { Result } from "effect";

import { Invalid } from "./Error.ts";
import { bytesToHex, hexToBytes } from "./Format.ts";
import { Sha1 } from "./Sha1.ts";
import { isOid, type Oid } from "./Store.ts";

export interface PackIndexEntry {
  readonly oid: Oid;
  /** Byte offset of the object's header inside the `.pack`. */
  readonly offset: number;
  /** CRC32 of the object's compressed bytes in the pack, as git records it. */
  readonly crc32: number;
}

const invalid = (field: string, reason: string) => Result.fail(new Invalid({ field, reason }));

/** `\xfftOc` — chosen because no v1 index (which starts with a fanout) can begin with it. */
const MAGIC = Uint8Array.of(0xff, 0x74, 0x4f, 0x63);
const HEADER_SIZE = 4 + 4 + 256 * 4;
const TRAILER_SIZE = 40;
/** oid + crc + offset per object, before any large-offset table. */
const PER_OBJECT = 20 + 4 + 4;
/** Offsets at or above this cannot be stored inline: the top bit is the escape flag. */
const INLINE_LIMIT = 0x8000_0000;

const CRC_TABLE = (() => {
  const table = new Int32Array(256);
  for (let index = 0; index < 256; index++) {
    let value = index;
    for (let bit = 0; bit < 8; bit++) value = value & 1 ? (value >>> 1) ^ 0xedb8_8320 : value >>> 1;
    table[index] = value;
  }
  return table;
})();

/** CRC32 (IEEE, reflected) — the checksum git stores per object in the index. */
export const crc32 = (bytes: Uint8Array): number => {
  let crc = -1;
  for (const byte of bytes) crc = (crc >>> 8) ^ CRC_TABLE[(crc ^ byte) & 0xff]!;
  return (crc ^ -1) >>> 0;
};

const compareOids = (left: Uint8Array, right: Uint8Array): number => {
  for (let index = 0; index < 20; index++) {
    const difference = left[index]! - right[index]!;
    if (difference !== 0) return difference;
  }
  return 0;
};

/**
 * Build a version-2 index for `entries`, which need not arrive sorted — the
 * format's whole value is the ordering, so it is imposed here.
 */
export const buildPackIndex = (
  entries: ReadonlyArray<PackIndexEntry>,
  packChecksum: Uint8Array,
): Uint8Array => {
  const rows = entries
    .map((entry) => ({ ...entry, bytes: hexToBytes(entry.oid) }))
    .sort((left, right) => compareOids(left.bytes, right.bytes));

  const largeCount = rows.filter((row) => row.offset >= INLINE_LIMIT).length;
  const size = HEADER_SIZE + PER_OBJECT * rows.length + 8 * largeCount + TRAILER_SIZE;

  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  out.set(MAGIC);
  view.setUint32(4, 2);

  // fanout[i] counts the objects whose first byte is <= i, so the entries for a
  // given first byte are the half-open range [fanout[i - 1], fanout[i]).
  let seen = 0;
  for (let bucket = 0; bucket < 256; bucket++) {
    while (seen < rows.length && rows[seen]!.bytes[0]! <= bucket) seen++;
    view.setUint32(8 + bucket * 4, seen);
  }

  const oidsAt = HEADER_SIZE;
  const crcsAt = oidsAt + 20 * rows.length;
  const offsetsAt = crcsAt + 4 * rows.length;
  const largeAt = offsetsAt + 4 * rows.length;

  let largeIndex = 0;
  for (const [index, row] of rows.entries()) {
    out.set(row.bytes, oidsAt + index * 20);
    view.setUint32(crcsAt + index * 4, row.crc32 >>> 0);
    if (row.offset < INLINE_LIMIT) {
      view.setUint32(offsetsAt + index * 4, row.offset);
    } else {
      view.setUint32(offsetsAt + index * 4, (INLINE_LIMIT | largeIndex) >>> 0);
      const at = largeAt + largeIndex * 8;
      view.setUint32(at, Math.floor(row.offset / 2 ** 32));
      view.setUint32(at + 4, row.offset >>> 0);
      largeIndex++;
    }
  }

  out.set(packChecksum, size - TRAILER_SIZE);
  out.set(new Sha1().update(out.subarray(0, size - 20)).digest(), size - 20);
  return out;
};

interface Layout {
  readonly view: DataView;
  readonly count: number;
  readonly oidsAt: number;
  readonly crcsAt: number;
  readonly offsetsAt: number;
  readonly largeAt: number;
  readonly largeCount: number;
}

/**
 * Validate the framing and derive the table positions. Cheap enough that both
 * readers do it: the trailer hash is over raw bytes, so a full read of the
 * index is still not a full *parse* of it.
 */
const layout = (bytes: Uint8Array): Result.Result<Layout, Invalid> => {
  if (bytes.length < HEADER_SIZE + TRAILER_SIZE) return invalid("idx", "truncated: no header");

  for (let index = 0; index < 4; index++) {
    if (bytes[index] !== MAGIC[index]!) return invalid("idx", "bad magic: not a pack index");
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = view.getUint32(4);
  if (version !== 2) return invalid("idx", `unsupported index version ${version}`);

  const expected = bytesToHex(bytes.subarray(bytes.length - 20));
  const actual = new Sha1().update(bytes.subarray(0, bytes.length - 20)).digestHex();
  if (actual !== expected) {
    return invalid("idx", `checksum mismatch: index says ${expected}, content hashes to ${actual}`);
  }

  const count = view.getUint32(8 + 255 * 4);
  const fixed = HEADER_SIZE + PER_OBJECT * count + TRAILER_SIZE;
  const slack = bytes.length - fixed;
  if (slack < 0) return invalid("idx", `truncated: ${count} objects need ${fixed} bytes`);
  if (slack % 8 !== 0) return invalid("idx", "large-offset table is not a multiple of 8 bytes");

  const oidsAt = HEADER_SIZE;
  const crcsAt = oidsAt + 20 * count;
  const offsetsAt = crcsAt + 4 * count;
  return Result.succeed({
    view,
    count,
    oidsAt,
    crcsAt,
    offsetsAt,
    largeAt: offsetsAt + 4 * count,
    largeCount: slack / 8,
  });
};

/** Resolve the offset word at `index`, following the escape into the 64-bit table. */
const offsetAt = (self: Layout, index: number): Result.Result<number, Invalid> => {
  const raw = self.view.getUint32(self.offsetsAt + index * 4);
  if ((raw & INLINE_LIMIT) === 0) return Result.succeed(raw);

  const slot = raw & 0x7fff_ffff;
  if (slot >= self.largeCount) return invalid("idx", `large-offset index ${slot} is out of range`);
  const high = self.view.getUint32(self.largeAt + slot * 8);
  // Above 2^53 the offset stops being representable, and no real pack is there.
  if (high > 0x001f_ffff) return invalid("idx", "offset exceeds the safe integer range");
  return Result.succeed(high * 2 ** 32 + self.view.getUint32(self.largeAt + slot * 8 + 4));
};

const entryAt = (
  bytes: Uint8Array,
  self: Layout,
  index: number,
): Result.Result<PackIndexEntry, Invalid> => {
  const offset = offsetAt(self, index);
  if (Result.isFailure(offset)) return Result.fail(offset.failure);
  // SAFETY: `layout` verified the buffer is large enough for `count` rows, so
  // this 20-byte oid row is in bounds and hex-encodes to exactly the 40
  // lowercase hex characters an oid is.
  return Result.succeed({
    oid: bytesToHex(bytes.subarray(self.oidsAt + index * 20, self.oidsAt + index * 20 + 20)) as Oid,
    offset: offset.success,
    crc32: self.view.getUint32(self.crcsAt + index * 4),
  });
};

/** Every entry, in index order — which is oid order, ascending. */
export const parsePackIndex = (
  bytes: Uint8Array,
): Result.Result<ReadonlyArray<PackIndexEntry>, Invalid> => {
  const parsed = layout(bytes);
  if (Result.isFailure(parsed)) return Result.fail(parsed.failure);

  const entries: PackIndexEntry[] = [];
  for (let index = 0; index < parsed.success.count; index++) {
    const entry = entryAt(bytes, parsed.success, index);
    if (Result.isFailure(entry)) return Result.fail(entry.failure);
    entries.push(entry.success);
  }
  return Result.succeed(entries);
};

/**
 * Look one oid up without parsing the rest.
 *
 * This is the reason the format exists: the fanout table bounds the search to
 * the objects sharing the oid's first byte, and the oid table is sorted, so a
 * binary search over that slice touches ~log2(n / 256) rows. A linear scan or a
 * full `parsePackIndex` would make the index no better than reading the pack.
 */
export const findInPackIndex = (
  bytes: Uint8Array,
  oid: Oid,
): Result.Result<PackIndexEntry | null, Invalid> => {
  if (!isOid(oid)) return invalid("oid", `malformed oid '${String(oid)}'`);

  const parsed = layout(bytes);
  if (Result.isFailure(parsed)) return Result.fail(parsed.failure);
  const self = parsed.success;

  const target = hexToBytes(oid);
  const bucket = target[0]!;
  let low = bucket === 0 ? 0 : self.view.getUint32(8 + (bucket - 1) * 4);
  let high = self.view.getUint32(8 + bucket * 4);
  if (low > high || high > self.count) return invalid("idx", "fanout table is not monotonic");

  while (low < high) {
    const middle = (low + high) >>> 1;
    const at = self.oidsAt + middle * 20;
    const order = compareOids(bytes.subarray(at, at + 20), target);
    if (order === 0) return entryAt(bytes, self, middle);
    if (order < 0) low = middle + 1;
    else high = middle;
  }
  return Result.succeed(null);
};
