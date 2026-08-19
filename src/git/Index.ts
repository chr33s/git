/**
 * The on-disk index (`.git/index`), version 2.
 *
 * The real `DIRC` binary format rather than a private stand-in, because the
 * index is a handover point: `git status` in a checkout we wrote has to see the
 * same staged state we do. Same seam as `Format.ts` — synchronous byte work, so
 * plain functions returning `Result`.
 *
 * The trailer is a SHA-1 over everything before it. `Sha1.ts` is used instead
 * of Web Crypto so `encodeIndex` stays synchronous; nothing here needs Effect.
 *
 * Extensions (`TREE`, `REUC`, ...) sit between the last entry and the trailer.
 * They are cache, not state, so decoding stops at the entry count and drops
 * them; encoding writes none, which git treats as a cold cache-tree.
 */
import { Result } from "effect";
import { Invalid } from "./Error.ts";
import { bytesToHex, hexToBytes } from "./Format.ts";
import { Sha1 } from "./Sha1.ts";
import type { Oid } from "./Store.ts";

export interface IndexEntry {
  readonly path: string;
  readonly oid: Oid;
  /** Full stat mode, e.g. `0o100644`; git stores type and permission bits. */
  readonly mode: number;
  readonly size: number;
  readonly mtimeSeconds: number;
  readonly mtimeNanos: number;
  readonly ctimeSeconds: number;
  readonly ctimeNanos: number;
  readonly device: number;
  readonly inode: number;
  readonly uid: number;
  readonly gid: number;
  /** 0 for a normal entry; 1..3 are the merge stages. */
  readonly stage: number;
  readonly assumeValid: boolean;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const invalid = (field: string, reason: string) => Result.fail(new Invalid({ field, reason }));

/** "DIRC", as a big-endian word. */
const SIGNATURE = 0x44495243;
const VERSION = 2;
const HEADER_SIZE = 12;
const CHECKSUM_SIZE = 20;
/** Ten 32-bit stat fields, the 20-byte oid, the 16-bit flags. */
const ENTRY_HEADER_SIZE = 62;

const FLAG_ASSUME_VALID = 0x8000;
/** Version 3's "extended flags follow" bit; illegal in a v2 entry. */
const FLAG_EXTENDED = 0x4000;
const FLAG_STAGE = 0x3000;
const FLAG_NAME_LENGTH = 0x0fff;

/** Padded with 1..8 NULs, so every entry starts 8-byte aligned. */
const entrySize = (pathLength: number) => Math.ceil((ENTRY_HEADER_SIZE + pathLength + 1) / 8) * 8;

/** git orders by raw UTF-8 bytes; JS string order differs above the BMP. */
const comparePaths = (left: string, right: string): number => {
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  const shared = Math.min(a.length, b.length);
  for (let index = 0; index < shared; index++) {
    const difference = a[index]! - b[index]!;
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
};

/** Path, then stage — the order git requires and refuses to read without. */
const sortEntries = (entries: ReadonlyArray<IndexEntry>): IndexEntry[] =>
  [...entries].sort((a, b) => comparePaths(a.path, b.path) || a.stage - b.stage);

export const decodeIndex = (
  bytes: Uint8Array,
): Result.Result<ReadonlyArray<IndexEntry>, Invalid> => {
  if (bytes.length < HEADER_SIZE + CHECKSUM_SIZE) return invalid("index", "truncated header");

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0) !== SIGNATURE) return invalid("index", "bad signature, expected 'DIRC'");

  const version = view.getUint32(4);
  if (version !== VERSION) return invalid("index", `unsupported index version ${version}`);

  const body = bytes.subarray(0, bytes.length - CHECKSUM_SIZE);
  const expected = bytesToHex(bytes.subarray(bytes.length - CHECKSUM_SIZE));
  const actual = new Sha1().update(body).digestHex();
  if (actual !== expected) return invalid("index", `checksum ${actual} does not match ${expected}`);

  const count = view.getUint32(8);
  const entries: IndexEntry[] = [];
  let offset = HEADER_SIZE;

  for (let index = 0; index < count; index++) {
    if (offset + ENTRY_HEADER_SIZE > body.length)
      return invalid("index", `entry ${index} truncated`);

    const flags = view.getUint16(offset + 60);
    if ((flags & FLAG_EXTENDED) !== 0) {
      return invalid("index", `entry ${index} sets the extended flag, illegal in version 2`);
    }

    // A length of 0xFFF means "0xFFF or more"; the NUL is then the only bound.
    const start = offset + ENTRY_HEADER_SIZE;
    const nameLength = flags & FLAG_NAME_LENGTH;
    let end = start + nameLength;
    if (nameLength === FLAG_NAME_LENGTH) {
      const nul = body.indexOf(0, start);
      if (nul === -1) return invalid("index", `entry ${index} path is unterminated`);
      end = nul;
    }
    if (end >= body.length) return invalid("index", `entry ${index} truncated`);
    if (body[end] !== 0) return invalid("index", `entry ${index} path is unterminated`);

    // SAFETY: the entry-header bound check above guarantees the twenty oid
    // bytes are present, and twenty bytes hex-encode to the forty characters
    // the Oid brand names.
    entries.push({
      path: decoder.decode(body.subarray(start, end)),
      oid: bytesToHex(body.subarray(offset + 40, offset + 60)) as Oid,
      mode: view.getUint32(offset + 24),
      size: view.getUint32(offset + 36),
      mtimeSeconds: view.getUint32(offset + 8),
      mtimeNanos: view.getUint32(offset + 12),
      ctimeSeconds: view.getUint32(offset),
      ctimeNanos: view.getUint32(offset + 4),
      device: view.getUint32(offset + 16),
      inode: view.getUint32(offset + 20),
      uid: view.getUint32(offset + 28),
      gid: view.getUint32(offset + 32),
      stage: (flags & FLAG_STAGE) >>> 12,
      assumeValid: (flags & FLAG_ASSUME_VALID) !== 0,
    });

    offset += entrySize(end - start);
  }

  return Result.succeed(entries);
};

export const encodeIndex = (entries: ReadonlyArray<IndexEntry>): Uint8Array => {
  const sorted = sortEntries(entries);
  const paths = sorted.map((entry) => encoder.encode(entry.path));

  const total =
    HEADER_SIZE + paths.reduce((sum, path) => sum + entrySize(path.length), 0) + CHECKSUM_SIZE;
  const out = new Uint8Array(total);
  const view = new DataView(out.buffer);

  view.setUint32(0, SIGNATURE);
  view.setUint32(4, VERSION);
  view.setUint32(8, sorted.length);

  let offset = HEADER_SIZE;
  for (const [index, entry] of sorted.entries()) {
    const path = paths[index]!;

    view.setUint32(offset, entry.ctimeSeconds);
    view.setUint32(offset + 4, entry.ctimeNanos);
    view.setUint32(offset + 8, entry.mtimeSeconds);
    view.setUint32(offset + 12, entry.mtimeNanos);
    view.setUint32(offset + 16, entry.device);
    view.setUint32(offset + 20, entry.inode);
    view.setUint32(offset + 24, entry.mode);
    view.setUint32(offset + 28, entry.uid);
    view.setUint32(offset + 32, entry.gid);
    view.setUint32(offset + 36, entry.size);
    out.set(hexToBytes(entry.oid), offset + 40);

    const flags =
      (entry.assumeValid ? FLAG_ASSUME_VALID : 0) |
      ((entry.stage & 0x3) << 12) |
      Math.min(path.length, FLAG_NAME_LENGTH);
    view.setUint16(offset + 60, flags);

    out.set(path, offset + ENTRY_HEADER_SIZE);
    // The gap to the next entry is already zero: `out` starts zeroed.
    offset += entrySize(path.length);
  }

  out.set(new Sha1().update(out.subarray(0, offset)).digest(), offset);
  return out;
};

/** Replaces any entry with the same path and stage, keeping git's ordering. */
export const addEntry = (
  entries: ReadonlyArray<IndexEntry>,
  entry: IndexEntry,
): ReadonlyArray<IndexEntry> =>
  sortEntries([
    ...entries.filter((other) => other.path !== entry.path || other.stage !== entry.stage),
    entry,
  ]);

/** Drops every stage of `path`, which is what resolving a conflict means. */
export const removeEntry = (
  entries: ReadonlyArray<IndexEntry>,
  path: string,
): ReadonlyArray<IndexEntry> => entries.filter((entry) => entry.path !== path);

export const findEntry = (
  entries: ReadonlyArray<IndexEntry>,
  path: string,
): IndexEntry | undefined => entries.find((entry) => entry.path === path);
