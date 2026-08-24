/**
 * Repository snapshot → bundle bytes, then verification.
 *
 * The builder captures the ref OIDs it intends to bundle and walks only
 * those. A concurrent push can advance the repository; it cannot change the
 * meaning of an already captured snapshot. The pack is streamed — nothing
 * here collects a repository into one `Uint8Array` on the write path.
 */
import { Effect, Result, Schema, Stream } from "effect";

import { BundleCorrupt, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { bytesToHex, concatBytes } from "../git/Format.ts";
import { inflate, InflateError, type ByteSource } from "../git/Inflate.ts";
import { reachable } from "../git/Maintenance.ts";
import * as Pack from "../git/Pack.ts";
import { Sha1 } from "../git/Sha1.ts";
import { ObjectStore, type Oid } from "../git/Store.ts";
import {
  type BundleArtifact,
  type BundleFilter,
  type BundleHeader,
  type BundleKind,
  type BundleSnapshot,
  encodeHeader,
  parseHeader,
} from "./BundleFormat.ts";

const readU32 = (bytes: Uint8Array, at: number): number =>
  ((bytes[at]! << 24) | (bytes[at + 1]! << 16) | (bytes[at + 2]! << 8) | bytes[at + 3]!) >>> 0;

const OFS_DELTA = 6;
const REF_DELTA = 7;

class Cursor implements ByteSource {
  #pending: Uint8Array[] = [];
  #iterator: AsyncIterator<Uint8Array>;
  readonly hash = new Sha1();
  hashing = true;

  constructor(input: AsyncIterable<Uint8Array>) {
    this.#iterator = input[Symbol.asyncIterator]();
  }

  async next(): Promise<Uint8Array | null> {
    const head = this.#pending.shift();
    if (head !== undefined) return head;
    const result = await this.#iterator.next();
    if (result.done === true) return null;
    return result.value.length === 0 ? this.next() : result.value;
  }

  pushBack(bytes: Uint8Array): void {
    if (bytes.length > 0) this.#pending.unshift(bytes);
  }

  async take(n: number): Promise<Uint8Array> {
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const chunk = await this.next();
      if (chunk === null) {
        throw new BundleCorrupt({ reason: `truncated pack: needed ${n - filled} more bytes` });
      }
      const use = Math.min(chunk.length, n - filled);
      out.set(chunk.subarray(0, use), filled);
      this.pushBack(chunk.subarray(use));
      filled += use;
    }
    if (this.hashing) this.hash.update(out);
    return out;
  }

  async byte(): Promise<number> {
    return (await this.take(1))[0]!;
  }
}

const skipObject = async (source: Cursor): Promise<void> => {
  let byte = await source.byte();
  const code = (byte >> 4) & 0x7;
  let size = byte & 0x0f;
  let shift = 4;
  while (byte & 0x80) {
    byte = await source.byte();
    size += (byte & 0x7f) * 2 ** shift;
    shift += 7;
  }
  if (code === OFS_DELTA) {
    byte = await source.byte();
    while (byte & 0x80) byte = await source.byte();
  } else if (code === REF_DELTA) {
    await source.take(20);
  } else if (code < 1 || code > 4) {
    throw new BundleCorrupt({ reason: `unknown pack object type ${code}` });
  }
  try {
    const data = await inflate(source, size + 1);
    if (data.length !== size) {
      throw new BundleCorrupt({
        reason: `pack object header says ${size} bytes, inflated to ${data.length}`,
      });
    }
  } catch (cause) {
    if (Schema.is(BundleCorrupt)(cause)) throw cause;
    const reason = cause instanceof InflateError ? cause.reason : String(cause);
    throw new BundleCorrupt({ reason: `pack object inflate failed: ${reason}` });
  }
};

const verifyPack = async (bytes: Uint8Array): Promise<void> => {
  if (bytes.length < 32) {
    throw new BundleCorrupt({ reason: "pack is shorter than a header and trailer" });
  }
  if (bytes[0] !== 0x50 || bytes[1] !== 0x41 || bytes[2] !== 0x43 || bytes[3] !== 0x4b) {
    throw new BundleCorrupt({ reason: "bundle payload is not a packfile" });
  }
  const version = readU32(bytes, 4);
  if (version !== 2) {
    throw new BundleCorrupt({ reason: `unsupported pack version ${version}` });
  }
  const count = readU32(bytes, 8);
  const body = bytes.subarray(0, bytes.length - 20);
  const trailer = bytes.subarray(bytes.length - 20);
  const source = new Cursor(
    (async function* () {
      yield body.subarray(12);
    })(),
  );
  for (let index = 0; index < count; index++) await skipObject(source);
  const leftover = await source.next();
  if (leftover !== null && leftover.length > 0) {
    throw new BundleCorrupt({ reason: "pack has trailing bytes before its trailer" });
  }
  const expected = new Sha1().update(body).digest();
  if (bytesToHex(expected) !== bytesToHex(trailer)) {
    throw new BundleCorrupt({ reason: "pack trailer does not match the SHA-1 of its contents" });
  }
};

const sameRefs = (
  actual: Readonly<Record<string, Oid>>,
  expected: Readonly<Record<string, Oid>>,
): string | null => {
  const names = new Set([...Object.keys(actual), ...Object.keys(expected)]);
  for (const name of names) {
    if (actual[name] !== expected[name]) {
      return `ref '${name}' is ${actual[name] ?? "missing"}, expected ${expected[name] ?? "missing"}`;
    }
  }
  return null;
};

const sameOids = (actual: ReadonlyArray<Oid>, expected: ReadonlyArray<Oid>): string | null => {
  if (actual.length !== expected.length) {
    return `prerequisite count ${actual.length} != ${expected.length}`;
  }
  for (let index = 0; index < expected.length; index++) {
    if (actual[index] !== expected[index]) {
      return `prerequisite ${index} is ${actual[index]}, expected ${expected[index]}`;
    }
  }
  return null;
};

/** Parse and check a generated bundle against the snapshot it claims to hold. */
export const verifyBundle = (
  bytes: Uint8Array,
  expected: {
    readonly refs: Readonly<Record<string, Oid>>;
    readonly prerequisites: ReadonlyArray<Oid>;
    readonly filter: BundleFilter;
  },
): Effect.Effect<BundleHeader, BundleCorrupt> =>
  Effect.gen(function* () {
    const parsed = parseHeader(bytes);
    if (Result.isFailure(parsed)) return yield* parsed.failure;
    const { header, packOffset } = parsed.success;
    if (header.filter !== expected.filter) {
      return yield* new BundleCorrupt({
        reason: `filter is ${header.filter ?? "none"}, expected ${expected.filter ?? "none"}`,
      });
    }
    const refs = sameRefs(header.refs, expected.refs);
    if (refs !== null) return yield* new BundleCorrupt({ reason: refs });
    const prerequisites = sameOids(header.prerequisites, expected.prerequisites);
    if (prerequisites !== null) return yield* new BundleCorrupt({ reason: prerequisites });
    yield* Effect.tryPromise({
      try: () => verifyPack(bytes.subarray(packOffset)),
      catch: (cause) =>
        Schema.is(BundleCorrupt)(cause)
          ? cause
          : new BundleCorrupt({ reason: cause instanceof Error ? cause.message : String(cause) }),
    });
    return header;
  });

/**
 * Reachability for a snapshot. `blob:none` still walks trees so the client
 * can reconstruct structure; blobs are dropped from the pack.
 */
export const oidsFor = Effect.fn("BundleBuilder.oidsFor")(function* (input: {
  readonly snapshot: BundleSnapshot;
  readonly prerequisiteRefs?: Readonly<Record<string, Oid>>;
}) {
  const objects = yield* ObjectStore;
  const roots = Object.values(input.snapshot.refs);
  const skip =
    input.prerequisiteRefs === undefined
      ? undefined
      : (yield* reachable(objects, Object.values(input.prerequisiteRefs), {
          ignoreMissing: true,
        })).seen;

  const walked = yield* reachable(objects, roots, {
    ignoreMissing: true,
    skip,
    classify: input.snapshot.filter === "blob:none",
  });

  const oids: Oid[] = [];
  for (const oid of walked.order) {
    if (input.snapshot.filter === "blob:none") {
      const kind = walked.classified.kinds.get(oid);
      if (kind?.type === "blob") continue;
    }
    oids.push(oid);
  }
  return oids;
});

export const headerFor = (input: {
  readonly snapshot: BundleSnapshot;
  readonly prerequisites: ReadonlyArray<Oid>;
}): BundleHeader => ({
  version: input.snapshot.filter === null ? 2 : 3,
  filter: input.snapshot.filter,
  refs: input.snapshot.refs,
  prerequisites: input.prerequisites,
});

/** Header bytes followed by a lazily written pack. */
export const bundleStream = (
  oids: ReadonlyArray<Oid>,
  header: BundleHeader,
): Stream.Stream<Uint8Array, ObjectNotFound | StorageFailure, ObjectStore> =>
  Stream.fromIterable([encodeHeader(header)]).pipe(Stream.concat(Pack.pack(oids)));

export interface BuiltBundle {
  readonly header: BundleHeader;
  readonly oids: ReadonlyArray<Oid>;
  readonly stream: Stream.Stream<Uint8Array, ObjectNotFound | StorageFailure, ObjectStore>;
}

export const build = Effect.fn("BundleBuilder.build")(function* (input: {
  readonly snapshot: BundleSnapshot;
  readonly kind: BundleKind;
  readonly prerequisiteRefs?: Readonly<Record<string, Oid>>;
}) {
  const prerequisites =
    input.kind === "incremental" && input.prerequisiteRefs !== undefined
      ? uniqueOids(Object.values(input.prerequisiteRefs))
      : [];
  const oids =
    input.prerequisiteRefs === undefined
      ? yield* oidsFor({ snapshot: input.snapshot })
      : yield* oidsFor({ snapshot: input.snapshot, prerequisiteRefs: input.prerequisiteRefs });
  const header = headerFor({ snapshot: input.snapshot, prerequisites });
  return {
    header,
    oids,
    stream: bundleStream(oids, header),
  } satisfies BuiltBundle;
});

const uniqueOids = (oids: ReadonlyArray<Oid>): ReadonlyArray<Oid> => {
  const seen = new Set<Oid>();
  const out: Oid[] = [];
  for (const oid of oids) {
    if (seen.has(oid)) continue;
    seen.add(oid);
    out.push(oid);
  }
  return out;
};

/** Concatenate a just-written artifact for verification. Streaming write already happened. */
export const collect = (
  source: Stream.Stream<Uint8Array, StorageFailure>,
): Effect.Effect<Uint8Array, StorageFailure> =>
  Stream.runCollect(source).pipe(Effect.map((chunks) => concatBytes(chunks)));

export type { BundleArtifact };
