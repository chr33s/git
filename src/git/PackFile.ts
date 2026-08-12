/**
 * One object out of a packfile, by byte offset.
 *
 * `Pack.ts` reads a pack front to back, which is what the wire needs. Storage
 * needs the other shape: given an offset from the `.idx`, produce that one
 * object and read nothing else. Without this a pack can only be transport, so
 * every object has to be exploded to a loose file on arrival — which is what
 * the backends did before, and why a repository was one filesystem entry (or
 * one R2 key) per object.
 *
 * Nothing here holds the pack. `Inflate.ts` is pull-based and pushes back what
 * it did not consume, so a zlib stream ends exactly where it ends and the
 * reader stops asking for windows — a 2 GiB pack costs a couple of reads to
 * pull a small blob out of the middle of it.
 */
import { Result } from "effect";

import { bytesToHex } from "./Format.ts";
import { type ByteSource, inflate, InflateError } from "./Inflate.ts";
import { PackCorrupt } from "./Error.ts";
import { applyDelta } from "./Pack.ts";
import type { ObjectType, Oid, RawObject } from "./Store.ts";

const CODE_TYPES: Record<number, ObjectType> = { 1: "commit", 2: "tree", 3: "blob", 4: "tag" };
const OFS_DELTA = 6;
const REF_DELTA = 7;

/**
 * 64 KiB: large enough that most objects arrive in one read, small enough
 * that pulling a small object out of a huge pack stays cheap. The reader
 * asks for more only when zlib says it needs it.
 */
const WINDOW = 64 * 1024;

/**
 * Random access to a pack's bytes. A file descriptor satisfies it with
 * `read(2)`; R2 satisfies it with a range GET, which is the reason the port
 * is offsets rather than a buffer.
 */
export interface PackSource {
  readonly size: number;
  /** Bytes `[offset, offset + length)`; may return fewer at the end. */
  readonly read: (offset: number, length: number) => Promise<Uint8Array>;
}

/** A pack in memory, for tests and for backends whose packs are small. */
export const bufferSource = (bytes: Uint8Array): PackSource => ({
  size: bytes.length,
  read: (offset, length) => Promise.resolve(bytes.subarray(offset, offset + length)),
});

const corrupt = (reason: string, offset: number) => new PackCorrupt({ reason, offset });

/**
 * A `ByteSource` over a window of the pack, extending only when asked.
 *
 * `primed` is the tail of the window the header was parsed from: re-reading
 * those bytes would work but would double the reads for every small object,
 * which is most of them.
 */
const windowed = (source: PackSource, start: number, primed: Uint8Array): ByteSource => {
  let position = start;
  const pending: Uint8Array[] = primed.length > 0 ? [primed] : [];

  return {
    next: async () => {
      const held = pending.shift();
      if (held !== undefined) return held;
      if (position >= source.size) return null;
      const chunk = await source.read(position, Math.min(WINDOW, source.size - position));
      if (chunk.length === 0) return null;
      position += chunk.length;
      return chunk;
    },
    pushBack: (bytes) => {
      if (bytes.length > 0) pending.unshift(bytes);
    },
  };
};

interface Header {
  readonly code: number;
  readonly size: number;
  /** How many bytes the header itself occupied. */
  readonly length: number;
  /** ofs-delta: how far back the base lies. ref-delta: its oid. */
  readonly baseOffset?: number;
  readonly baseOid?: Oid;
}

const parseHeader = (bytes: Uint8Array, at: number): Header => {
  let position = 0;
  const byte = () => {
    const value = bytes[position];
    if (value === undefined) throw corrupt("object header runs past the window", at);
    position++;
    return value;
  };

  let current = byte();
  const code = (current >> 4) & 0x7;
  let size = current & 0x0f;
  let shift = 4;
  while (current & 0x80) {
    current = byte();
    size += (current & 0x7f) * 2 ** shift;
    shift += 7;
  }

  if (code === OFS_DELTA) {
    current = byte();
    let distance = current & 0x7f;
    while (current & 0x80) {
      current = byte();
      distance = (distance + 1) * 128 + (current & 0x7f);
    }
    return { code, size, length: position, baseOffset: at - distance };
  }

  if (code === REF_DELTA) {
    const oid = bytesToHex(bytes.subarray(position, position + 20)) as Oid;
    if (oid.length !== 40) throw corrupt("ref-delta base oid runs past the window", at);
    position += 20;
    return { code, size, length: position, baseOid: oid };
  }

  return { code, size, length: position };
};

/**
 * The object at `offset`, deltas resolved.
 *
 * `resolveBase` is how a ref-delta finds a base this pack does not contain —
 * another pack, or a loose object. A pack git wrote is self-contained, but one
 * that arrived over the wire as a thin pack is not, and the caller is the only
 * one that knows where else to look.
 */
export const readAt = async (
  source: PackSource,
  offset: number,
  resolveBase: (oid: Oid) => Promise<RawObject | null>,
  depth = 0,
): Promise<RawObject> => {
  // A delta chain is bounded in every pack git produces; an unbounded one is
  // a cycle, and following it would hang rather than fail.
  if (depth > 64) throw corrupt("delta chain deeper than 64", offset);
  if (offset < 0 || offset >= source.size) throw corrupt("offset outside the pack", offset);

  const window = await source.read(offset, Math.min(WINDOW, source.size - offset));
  const header = parseHeader(window, offset);

  let data: Uint8Array;
  try {
    data = await inflate(windowed(source, offset + window.length, window.subarray(header.length)));
  } catch (error) {
    throw error instanceof InflateError ? corrupt(error.message, offset) : error;
  }

  if (header.code === OFS_DELTA || header.code === REF_DELTA) {
    const base =
      header.baseOffset !== undefined
        ? await readAt(source, header.baseOffset, resolveBase, depth + 1)
        : await resolveBase(header.baseOid!);
    if (base === null) throw corrupt(`delta base ${header.baseOid} is nowhere`, offset);

    const applied = applyDelta(base.data, data);
    if (Result.isFailure(applied)) throw applied.failure;
    return { type: base.type, data: applied.success };
  }

  const type = CODE_TYPES[header.code];
  if (type === undefined) throw corrupt(`unknown object type ${header.code}`, offset);
  if (data.length !== header.size) {
    throw corrupt(`header says ${header.size} bytes, inflated to ${data.length}`, offset);
  }
  return { type, data };
};
