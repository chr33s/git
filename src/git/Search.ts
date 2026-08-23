/**
 * Literal blob search and its deliberately disposable in-memory prefilter.
 *
 * Blob ordinals belong only to one Repository instance. Object storage remains
 * authoritative: postings only decide which blobs merit exact verification.
 */
import { Context, Effect, Layer, Result, Schema } from "effect";

import { isBinary } from "./Diff.ts";
import { Invalid } from "./Error.ts";
import { Sha1 } from "./Sha1.ts";
import { isOid, type Oid } from "./Store.ts";

export const MAX_MATCHES = 2_000;
export const MAX_FILE_BYTES = 4 * 1024 * 1024;

export interface LineMatch {
  readonly line: number;
  readonly text: string;
}

export interface SearchMatch {
  readonly path: string;
  readonly line: number;
  readonly text: string;
}

export interface CharacterRange {
  /** UTF-16 offsets in `text`, suitable for browser highlighting. */
  readonly start: number;
  readonly end: number;
}

export interface FuzzyMatch extends SearchMatch {
  readonly ranges: ReadonlyArray<CharacterRange>;
  readonly score: number;
}

export interface SearchResult {
  readonly matches: ReadonlyArray<SearchMatch>;
  /** Approximate hits are deliberately separate from literal matches. */
  readonly suggestions?: ReadonlyArray<FuzzyMatch>;
  readonly truncated: boolean;
  /** Opaque cursor, present only when more exact work remains. */
  readonly continuation?: string;
  readonly skipped: ReadonlyArray<string>;
}

export type LineMatcher = (line: string) => boolean;

export const INDEX_VERSION = 1;

/**
 * The existing grep syntax, isolated from transport handling. The index is
 * used only for its fixed, ASCII, case-insensitive subset; every other form
 * still uses this verifier against every reachable blob.
 */
export const compileMatcher = (input: {
  readonly pattern: string;
  readonly fixed?: boolean;
  readonly ignoreCase?: boolean;
}): Result.Result<LineMatcher, Invalid> => {
  if (input.fixed === true) {
    const needle = input.ignoreCase === true ? input.pattern.toLowerCase() : input.pattern;
    return Result.succeed((line) =>
      (input.ignoreCase === true ? line.toLowerCase() : line).includes(needle),
    );
  }

  try {
    const unescaped = input.pattern.replace(/\\./g, "");
    const quantifiers = unescaped.match(/[*+?]|\{\d/g)?.length ?? 0;
    const grouped = /\((?!\?:)/.test(unescaped);
    if (quantifiers > 1 || grouped || input.pattern.length > 200) {
      return Result.fail(
        new Invalid({
          field: "pattern",
          reason:
            "this endpoint accepts at most one repetition and no groups, because more " +
            "can take unbounded time to match; use `regex: false` for a literal search",
        }),
      );
    }
    const expression = new RegExp(input.pattern, input.ignoreCase === true ? "i" : "");
    return Result.succeed((line) => expression.test(line));
  } catch (cause) {
    return Result.fail(
      new Invalid({ field: "pattern", reason: cause instanceof Error ? cause.message : "bad" }),
    );
  }
};

/** Verify one candidate with the same decoded-line semantics as unindexed grep. */
export const verify = (
  data: Uint8Array,
  test: LineMatcher,
  maximum: number,
): ReadonlyArray<LineMatch> => {
  const decoder = new TextDecoder();
  const matches: LineMatch[] = [];
  let start = 0;
  let line = 0;
  while (start <= data.length && matches.length < maximum) {
    const newline = data.indexOf(0x0a, start);
    const end = newline === -1 ? data.length : newline;
    line += 1;
    const text = decoder.decode(data.subarray(start, end));
    if (test(text)) matches.push({ line, text });
    if (newline === -1) break;
    start = newline + 1;
  }
  return matches;
};

export type BlobState = "searchable" | "unindexed" | "binary" | "too-large";

export interface IndexedBlob {
  readonly oid: Oid;
  readonly ordinal: number;
  readonly state: BlobState;
}

export interface BlobIndexStats {
  readonly blobs: number;
  readonly bigrams: number;
  readonly snapshotBytes: number;
}

const hex = (bytes: Uint8Array): string =>
  [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
const checksum = (bytes: Uint8Array): string => hex(new Sha1().update(bytes).digest());

const Snapshot = Schema.Struct({
  version: Schema.Literal(INDEX_VERSION),
  blobs: Schema.Array(
    Schema.Struct({
      oid: Schema.String,
      state: Schema.Literals(["searchable", "unindexed", "binary", "too-large"]),
      bigrams: Schema.Array(Schema.Finite),
    }),
  ),
});

const SnapshotFile = Schema.Struct({ payload: Schema.String, checksum: Schema.String });

const PersistedChunk = Schema.Struct({
  name: Schema.String,
  kind: Schema.Literals(["blobs", "postings"]),
  ordinalStart: Schema.Finite,
  ordinalEnd: Schema.Finite,
  /** Sorted bigram coverage, present on posting chunks so a query can load only the chunks it needs. */
  bigramStart: Schema.optional(Schema.Finite),
  bigramEnd: Schema.optional(Schema.Finite),
  /** SHA-1 of the raw chunk bytes, before the manifest codec is applied. */
  checksum: Schema.String,
  /** Raw chunk bytes; the size the checksum covers. */
  size: Schema.Finite,
  /** Bytes the host actually stored, after the codec. */
  compressedSize: Schema.Finite,
});
const PersistedManifest = Schema.Struct({
  version: Schema.Literal(3),
  codec: Schema.Literals(["identity", "deflate"]),
  chunkTargetBytes: Schema.Finite,
  chunks: Schema.Array(PersistedChunk),
});
export type PersistedManifestInfo = (typeof PersistedManifest)["Type"];
export type PersistedChunkInfo = PersistedManifestInfo["chunks"][number];

const PersistedBlobRows = Schema.Array(
  Schema.Struct({
    oid: Schema.String,
    ordinal: Schema.Finite,
    state: Schema.Literals(["searchable", "unindexed", "binary", "too-large"]),
  }),
);
const PersistedPostingRows = Schema.Array(
  Schema.Struct({ bigram: Schema.Finite, ordinals: Schema.Array(Schema.Finite) }),
);

export interface IndexSnapshot {
  readonly manifest: Uint8Array;
  readonly chunks: ReadonlyArray<{
    readonly name: string;
    readonly bytes: Uint8Array;
    /** SHA-1 of the raw bytes, matching the manifest entry — used to skip unchanged chunk writes. */
    readonly checksum: string;
  }>;
}

const PERSISTED_VERSION = 3;
const CHUNK_TARGET_BYTES = 256 * 1024;

/** `CompressionStream` is a global on every host: browser, workerd, node >= 17. */
const pipeThrough = async (
  bytes: Uint8Array,
  transform: CompressionStream | DecompressionStream,
): Promise<Uint8Array> => {
  const writer = transform.writable.getWriter();
  // SAFETY: as in `adapters/Opfs.ts`, chunk bytes are never shared-memory
  // backed; the stream's chunk type just spells that out.
  const written = writer.write(bytes as Uint8Array<ArrayBuffer>).then(() => writer.close());
  // A corrupt stream reports on the read side; without this, the write side's
  // matching rejection escapes unhandled after the read has already failed.
  const suppressed = written.catch(() => undefined);
  const chunks: Uint8Array[] = [];
  const reader = transform.readable.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  } catch (cause) {
    await reader.cancel().catch(() => undefined);
    await suppressed;
    throw cause;
  }
  await written;
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
};

/** Compress/expand one chunk with the manifest's codec. `null` means unreadable. */
export const decodeChunk = async (
  codec: PersistedManifestInfo["codec"],
  bytes: Uint8Array,
): Promise<Uint8Array | null> => {
  if (codec === "identity") return bytes;
  try {
    return await pipeThrough(bytes, new DecompressionStream("deflate"));
  } catch {
    return null;
  }
};

/** A manifest that fails to parse is an old or corrupt cache: cold, never wrong. */
export const parseManifest = (bytes: Uint8Array): PersistedManifestInfo | null => {
  let decoded: unknown;
  try {
    decoded = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return null;
  }
  const parsed = Schema.decodeUnknownOption(PersistedManifest)(decoded);
  return parsed._tag === "None" ? null : parsed.value;
};

/** A small growable bitset; its private layout is never shared between hosts. */
class BitSet {
  #words = new Uint32Array(0);

  add(bit: number): void {
    const word = Math.floor(bit / 32);
    if (word >= this.#words.length) {
      const words = new Uint32Array(word + 1);
      words.set(this.#words);
      this.#words = words;
    }
    this.#words[word] = (this.#words[word] ?? 0) | (1 << (bit % 32));
  }

  intersect(other: BitSet): BitSet {
    const result = new BitSet();
    const words = new Uint32Array(Math.min(this.#words.length, other.#words.length));
    for (let index = 0; index < words.length; index += 1) {
      words[index] = this.#words[index]! & other.#words[index]!;
    }
    result.#words = words;
    return result;
  }

  remove(bit: number): void {
    const word = Math.floor(bit / 32);
    if (word >= this.#words.length) return;
    this.#words[word] = (this.#words[word] ?? 0) & ~(1 << (bit % 32));
  }

  values(): ReadonlySet<number> {
    const values = new Set<number>();
    for (let word = 0; word < this.#words.length; word += 1) {
      let bits = this.#words[word]!;
      while (bits !== 0) {
        const lowest = 31 - Math.clz32(bits & -bits);
        values.add(word * 32 + lowest);
        bits &= bits - 1;
      }
    }
    return values;
  }
}

const asciiLower = (byte: number): number => (byte >= 0x41 && byte <= 0x5a ? byte + 0x20 : byte);

/** `null` means the byte prefilter cannot safely model this query. */
export const queryBigrams = (pattern: string): ReadonlyArray<number> | null => {
  const bytes = new TextEncoder().encode(pattern);
  if (bytes.length < 2 || bytes.some((byte) => byte < 0x20 || byte > 0x7e)) return null;
  const bigrams = new Set<number>();
  for (let index = 1; index < bytes.length; index += 1) {
    bigrams.add((asciiLower(bytes[index - 1]!) << 8) | asciiLower(bytes[index]!));
  }
  return [...bigrams];
};

/**
 * Derived cache state keyed by immutable blob OID. A cache miss is important:
 * callers must read and verify it, rather than treating an incomplete index as
 * evidence that the blob cannot match.
 */
export class BlobIndex {
  #byOid = new Map<Oid, IndexedBlob>();
  #byOrdinal = new Map<number, IndexedBlob>();
  #byBigram = new Map<number, BitSet>();
  #nextOrdinal = 0;
  #keysByOid = new Map<Oid, ReadonlyArray<number>>();
  /**
   * Bigram ranges whose posting chunks have not been read yet after a lazy
   * restore. `candidates` must refuse a query touching one: an unloaded
   * posting list is not an empty one, and answering would be a false negative.
   */
  #unloadedRanges: Array<{ readonly start: number; readonly end: number }> = [];

  get(oid: Oid): IndexedBlob | undefined {
    return this.#byOid.get(oid);
  }

  get postingsComplete(): boolean {
    return this.#unloadedRanges.length === 0;
  }

  observe(oid: Oid, data: Uint8Array): IndexedBlob {
    const existing = this.#byOid.get(oid);
    if (existing !== undefined) return existing;

    // JavaScript case folding can turn a Unicode character into ASCII (for
    // example Kelvin sign → `k`). Such a blob cannot safely be rejected by
    // byte bigrams for an ASCII query, so it remains verifier-only.
    const state: BlobState =
      data.length > MAX_FILE_BYTES
        ? "too-large"
        : isBinary(data)
          ? "binary"
          : data.some((byte) => byte > 0x7f)
            ? "unindexed"
            : "searchable";
    const blob: IndexedBlob = { oid, ordinal: this.#nextOrdinal, state };
    this.#nextOrdinal += 1;
    this.#byOid.set(oid, blob);
    this.#byOrdinal.set(blob.ordinal, blob);
    if (state !== "searchable") return blob;

    const seen = new Set<number>();
    for (let index = 1; index < data.length; index += 1) {
      const bigram = (asciiLower(data[index - 1]!) << 8) | asciiLower(data[index]!);
      if (seen.has(bigram)) continue;
      seen.add(bigram);
      let posting = this.#byBigram.get(bigram);
      if (posting === undefined) {
        posting = new BitSet();
        this.#byBigram.set(bigram, posting);
      }
      posting.add(blob.ordinal);
    }
    this.#keysByOid.set(oid, [...seen]);
    return blob;
  }

  forget(oid: Oid): boolean {
    const blob = this.#byOid.get(oid);
    if (blob === undefined) return false;
    this.#byOid.delete(oid);
    this.#byOrdinal.delete(blob.ordinal);
    const bigrams = this.#keysByOid.get(oid) ?? [];
    this.#keysByOid.delete(oid);
    for (const bigram of bigrams) this.#byBigram.get(bigram)?.remove(blob.ordinal);
    return true;
  }

  stats(): BlobIndexStats {
    return {
      blobs: this.#byOid.size,
      bigrams: this.#byBigram.size,
      snapshotBytes: this.snapshot().length,
    };
  }

  snapshot(): Uint8Array {
    const payload = JSON.stringify({
      version: INDEX_VERSION,
      blobs: [...this.#byOid.values()].map((blob) => ({
        oid: blob.oid,
        state: blob.state,
        bigrams: this.#keysByOid.get(blob.oid) ?? [],
      })),
    });
    const bytes = new TextEncoder().encode(payload);
    return new TextEncoder().encode(JSON.stringify({ payload, checksum: checksum(bytes) }));
  }

  /** A corrupt or old derived cache is discarded, never trusted. */
  static restore(bytes: Uint8Array): BlobIndex | null {
    let envelope: unknown;
    try {
      envelope = JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      return null;
    }
    const file = Schema.decodeUnknownOption(SnapshotFile)(envelope);
    if (file._tag === "None") return null;
    const payload = new TextEncoder().encode(file.value.payload);
    if (checksum(payload) !== file.value.checksum) return null;
    let value: unknown;
    try {
      value = JSON.parse(file.value.payload);
    } catch {
      return null;
    }
    const decoded = Schema.decodeUnknownOption(Snapshot)(value);
    if (decoded._tag === "None") return null;
    const index = new BlobIndex();
    for (const row of decoded.value.blobs) {
      if (!isOid(row.oid)) return null;
      const blob: IndexedBlob = { oid: row.oid, ordinal: index.#nextOrdinal, state: row.state };
      index.#nextOrdinal += 1;
      index.#byOid.set(blob.oid, blob);
      if (blob.state !== "searchable") continue;
      index.#keysByOid.set(blob.oid, row.bigrams);
      for (const bigram of row.bigrams) {
        let posting = index.#byBigram.get(bigram);
        if (posting === undefined) {
          posting = new BitSet();
          index.#byBigram.set(bigram, posting);
        }
        posting.add(blob.ordinal);
      }
    }
    return index;
  }

  /**
   * Host-private, chunked persistence. Chunks are written before this manifest;
   * publishing the manifest last makes an interrupted checkpoint a cold cache,
   * never negative search evidence. `null` when the index is only partially
   * restored — persisting that would record unloaded postings as absent.
   */
  async persisted(
    compress?: (bytes: Uint8Array) => Promise<Uint8Array>,
    targetBytes: number = CHUNK_TARGET_BYTES,
  ): Promise<IndexSnapshot | null> {
    if (this.#unloadedRanges.length > 0) return null;
    const blobs = [...this.#byOid.values()].map((blob) => ({
      oid: blob.oid,
      ordinal: blob.ordinal,
      state: blob.state,
    }));
    // Sorted so each chunk can declare its bigram coverage and a query can
    // load only the chunks whose ranges intersect its pattern.
    const postings = [...this.#byBigram.entries()]
      .sort(([left], [right]) => left - right)
      .map(([bigram, posting]) => ({ bigram, ordinals: [...posting.values()] }));
    const partition = <A>(rows: ReadonlyArray<A>): ReadonlyArray<ReadonlyArray<A>> => {
      const groups: Array<Array<A>> = [];
      let current: A[] = [];
      let bytes = 2;
      for (const row of rows) {
        const size = new TextEncoder().encode(JSON.stringify(row)).length + 1;
        if (bytes + size > targetBytes && current.length > 0) {
          groups.push(current);
          current = [];
          bytes = 2;
        }
        current.push(row);
        bytes += size;
      }
      if (current.length > 0) groups.push(current);
      return groups;
    };
    const chunk = async (
      name: string,
      kind: "blobs" | "postings",
      rows: ReadonlyArray<{
        readonly ordinal?: number;
        readonly bigram?: number;
        readonly ordinals?: ReadonlyArray<number>;
      }>,
      ordinals: ReadonlyArray<number>,
    ) => {
      const raw = new TextEncoder().encode(JSON.stringify(rows));
      const stored = compress === undefined ? raw : await compress(raw);
      const first = rows[0]?.bigram;
      const last = rows.at(-1)?.bigram;
      const coverage =
        kind === "postings" && first !== undefined && last !== undefined
          ? { bigramStart: first, bigramEnd: last }
          : { bigramStart: undefined, bigramEnd: undefined };
      return {
        name,
        kind,
        ordinalStart: ordinals.length === 0 ? 0 : Math.min(...ordinals),
        ordinalEnd: ordinals.length === 0 ? 0 : Math.max(...ordinals),
        bigramStart: coverage.bigramStart,
        bigramEnd: coverage.bigramEnd,
        checksum: checksum(raw),
        size: raw.length,
        compressedSize: stored.length,
        bytes: stored,
      };
    };
    // The target makes append-heavy blob/posting tables independently
    // replaceable. Their ordinals remain local to this host's index.
    const parts = [
      ...(await Promise.all(
        partition(blobs).map((rows, index) =>
          chunk(
            `blobs-${index}`,
            "blobs",
            rows,
            rows.map((row) => row.ordinal),
          ),
        ),
      )),
      ...(await Promise.all(
        partition(postings).map((rows, index) =>
          chunk(
            `postings-${index}`,
            "postings",
            rows,
            rows.flatMap((row) => row.ordinals ?? []),
          ),
        ),
      )),
    ];
    const manifest = new TextEncoder().encode(
      JSON.stringify({
        version: PERSISTED_VERSION,
        codec: compress === undefined ? "identity" : "deflate",
        chunkTargetBytes: CHUNK_TARGET_BYTES,
        chunks: parts.map(({ bytes: _bytes, ...part }) => part),
      }),
    );
    return {
      manifest,
      chunks: parts.map(({ name, bytes, checksum: rawChecksum }) => ({
        name,
        bytes,
        checksum: rawChecksum,
      })),
    };
  }

  /**
   * Eager half of a lazy restore: blob rows only, from already-decompressed
   * chunk bytes. Posting chunks stay on disk until a query's bigrams need them;
   * until then `candidates` refuses rather than answering from a partial index.
   */
  static restoreBlobs(
    info: PersistedManifestInfo,
    chunks: ReadonlyMap<string, Uint8Array>,
  ): BlobIndex | null {
    const index = new BlobIndex();
    for (const part of info.chunks) {
      if (part.kind !== "blobs") continue;
      const bytes = chunks.get(part.name);
      if (bytes === undefined || bytes.length !== part.size || checksum(bytes) !== part.checksum) {
        return null;
      }
      let value: unknown;
      try {
        value = JSON.parse(new TextDecoder().decode(bytes));
      } catch {
        return null;
      }
      const rows = Schema.decodeUnknownOption(PersistedBlobRows)(value);
      if (rows._tag === "None") return null;
      for (const row of rows.value) {
        if (!isOid(row.oid) || index.#byOrdinal.has(row.ordinal)) return null;
        const blob: IndexedBlob = { oid: row.oid, ordinal: row.ordinal, state: row.state };
        index.#byOid.set(blob.oid, blob);
        index.#byOrdinal.set(blob.ordinal, blob);
        index.#nextOrdinal = Math.max(index.#nextOrdinal, row.ordinal + 1);
      }
    }
    index.#unloadedRanges = info.chunks
      .filter((chunk) => chunk.kind === "postings")
      // A chunk without declared coverage could hold anything: refuse until read.
      .map((chunk) => ({
        start: chunk.bigramStart ?? 0,
        end: chunk.bigramEnd ?? Number.MAX_SAFE_INTEGER,
      }));
    return index;
  }

  /** Merge one posting chunk a query needed. `false` means corrupt: go cold. */
  loadPostings(chunk: PersistedChunkInfo, bytes: Uint8Array): boolean {
    if (chunk.kind !== "postings") return false;
    if (bytes.length !== chunk.size || checksum(bytes) !== chunk.checksum) return false;
    let value: unknown;
    try {
      value = JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      return false;
    }
    const rows = Schema.decodeUnknownOption(PersistedPostingRows)(value);
    if (rows._tag === "None") return false;
    for (const row of rows.value) {
      for (const ordinal of row.ordinals) {
        const blob = this.#byOrdinal.get(ordinal);
        // Unknown ordinals are blobs GC forgot after the snapshot: skip them.
        if (blob !== undefined && blob.state !== "searchable") return false;
      }
      const posting = this.#byBigram.get(row.bigram) ?? new BitSet();
      for (const ordinal of row.ordinals) {
        const blob = this.#byOrdinal.get(ordinal);
        if (blob === undefined) continue;
        posting.add(ordinal);
        const keys = this.#keysByOid.get(blob.oid) ?? [];
        this.#keysByOid.set(blob.oid, [...keys, row.bigram]);
      }
      this.#byBigram.set(row.bigram, posting);
    }
    const start = chunk.bigramStart ?? 0;
    const end = chunk.bigramEnd ?? Number.MAX_SAFE_INTEGER;
    const pending = this.#unloadedRanges.findIndex(
      (range) => range.start === start && range.end === end,
    );
    if (pending !== -1) this.#unloadedRanges.splice(pending, 1);
    return true;
  }

  /** Eager restore of a whole snapshot: blobs plus every posting chunk. */
  static restorePersisted(
    manifest: Uint8Array,
    chunks: ReadonlyMap<string, Uint8Array>,
  ): BlobIndex | null {
    const info = parseManifest(manifest);
    if (info === null) return null;
    const index = BlobIndex.restoreBlobs(info, chunks);
    if (index === null) return null;
    for (const chunk of info.chunks) {
      if (chunk.kind !== "postings") continue;
      const bytes = chunks.get(chunk.name);
      if (bytes === undefined || !index.loadPostings(chunk, bytes)) return null;
    }
    return index;
  }

  /** `null` asks callers to use the full verifier path. */
  candidates(pattern: string, enabled: boolean): ReadonlySet<number> | null {
    if (!enabled) return null;
    const bigrams = queryBigrams(pattern);
    if (bigrams === null) return null;
    if (
      bigrams.some((bigram) =>
        this.#unloadedRanges.some((range) => bigram >= range.start && bigram <= range.end),
      )
    ) {
      return null;
    }
    let candidates: BitSet | undefined;
    for (const bigram of bigrams) {
      const posting = this.#byBigram.get(bigram);
      if (posting === undefined) return new Set();
      candidates = candidates === undefined ? posting : candidates.intersect(posting);
    }
    return candidates?.values() ?? new Set();
  }
}

/** Derived-state port. Hosts may replace `memory` with persistent cache. */
export class SearchIndex extends Context.Service<
  SearchIndex,
  {
    readonly index: BlobIndex;
    readonly observe: (oid: Oid, data: Uint8Array) => Effect.Effect<IndexedBlob>;
    readonly forget: (oids: ReadonlyArray<Oid>) => Effect.Effect<void>;
    /**
     * Candidate ordinals for a literal pattern, loading posting chunks on
     * demand where the host persists them. `null` means full verification.
     */
    readonly candidates: (
      pattern: string,
      enabled: boolean,
    ) => Effect.Effect<ReadonlySet<number> | null>;
    readonly flush: Effect.Effect<void>;
  }
>()("git/SearchIndex") {}

/** The default is intentionally disposable and needs no storage capability. */
export const memory = Layer.sync(SearchIndex, () => {
  const index = new BlobIndex();
  return SearchIndex.of({
    index,
    observe: (oid, data) => Effect.sync(() => index.observe(oid, data)),
    forget: (oids) => Effect.sync(() => oids.forEach((oid) => index.forget(oid))),
    candidates: (pattern, enabled) => Effect.sync(() => index.candidates(pattern, enabled)),
    flush: Effect.void,
  });
});

/** Per-host cache caps; both default per platform when the adapter omits them. */
export interface PersistenceLimits {
  readonly softLimitBytes?: number;
  readonly hardLimitBytes?: number;
}

export interface PersistenceIo {
  /** `null` for a missing chunk or manifest; failures are caught by the host. */
  readonly read: (name: string) => Effect.Effect<Uint8Array | null>;
  readonly write: (name: string, bytes: Uint8Array) => Effect.Effect<void>;
  readonly remove: (name: string) => Effect.Effect<void>;
  /** Chunk and manifest names currently stored, for orphan cleanup. */
  readonly list: Effect.Effect<ReadonlyArray<string>>;
  /** Past this, a checkpoint warns and still persists: derived data is useful. */
  readonly softLimitBytes: number;
  /** Past this, the host keeps the index in memory only. */
  readonly hardLimitBytes: number;
  /** Test and tuning seam; defaults to 256 KiB. */
  readonly chunkTargetBytes?: number;
}

/**
 * Chunked, compressed, lazily-restored persistence shared by every host.
 *
 * Startup reads the manifest and blob chunks only; a posting chunk is read the
 * first time a query's bigrams fall inside its declared range. A missing,
 * corrupt, or unknown-version chunk makes `candidates` return `null` — the
 * caller falls back to reading and verifying blobs, so a bad cache is cold,
 * never negative. Writes publish chunks first and the manifest last, so an
 * interrupted checkpoint is invisible until complete.
 */
export const persistent = (io: PersistenceIo): Layer.Layer<SearchIndex> =>
  Layer.effect(
    SearchIndex,
    Effect.gen(function* () {
      const compress = (bytes: Uint8Array) => pipeThrough(bytes, new CompressionStream("deflate"));
      const readChunk = (codec: PersistedManifestInfo["codec"], name: string) =>
        io.read(name).pipe(
          Effect.flatMap((bytes) =>
            bytes === null ? Effect.succeed(null) : Effect.promise(() => decodeChunk(codec, bytes)),
          ),
          // A read failure is a cold cache, not a request failure.
          Effect.orElseSucceed(() => null),
        );

      const manifestBytes = yield* io.read("manifest.json").pipe(Effect.orElseSucceed(() => null));
      const info = manifestBytes === null ? null : parseManifest(manifestBytes);
      let current = new BlobIndex();
      let manifestInfo: PersistedManifestInfo | null = null;
      const loadedPostings = new Set<string>();
      /**
       * Raw checksum of every chunk the published manifest names. Seeded from
       * a same-codec manifest, this is what lets a flush skip chunks whose
       * content did not change — the incremental-write half of chunking.
       */
      const published = new Map<string, string>();
      if (manifestBytes !== null && info !== null) {
        const blobChunks = new Map<string, Uint8Array>();
        let readable = true;
        for (const chunk of info.chunks) {
          if (chunk.kind !== "blobs") continue;
          const bytes = yield* readChunk(info.codec, chunk.name);
          if (bytes === null) {
            readable = false;
            break;
          }
          blobChunks.set(chunk.name, bytes);
        }
        const restored = readable ? BlobIndex.restoreBlobs(info, blobChunks) : null;
        if (restored !== null) {
          current = restored;
          manifestInfo = info;
          if (info.codec === "deflate") {
            for (const chunk of info.chunks) published.set(chunk.name, chunk.checksum);
          }
        }
      }
      const postingChunks = (manifestInfo?.chunks ?? []).filter(
        (chunk) => chunk.kind === "postings",
      );

      let dirty = false;
      const observe = Effect.fn("SearchIndex.observe")((oid: Oid, data: Uint8Array) =>
        Effect.sync(() => {
          const known = current.get(oid);
          const blob = current.observe(oid, data);
          if (known === undefined) dirty = true;
          return blob;
        }),
      );
      const forget = (oids: ReadonlyArray<Oid>) =>
        Effect.sync(() => {
          for (const oid of oids) dirty = current.forget(oid) || dirty;
        });
      const candidates = Effect.fn("SearchIndex.candidates")((pattern: string, enabled: boolean) =>
        Effect.gen(function* () {
          if (manifestInfo === null) return current.candidates(pattern, enabled);
          const bigrams = enabled ? queryBigrams(pattern) : null;
          if (bigrams === null) return null;
          for (const chunk of postingChunks) {
            if (loadedPostings.has(chunk.name)) continue;
            const start = chunk.bigramStart ?? 0;
            const end = chunk.bigramEnd ?? -1;
            if (!bigrams.some((bigram) => bigram >= start && bigram <= end)) continue;
            const bytes = yield* readChunk(manifestInfo.codec, chunk.name);
            // Unknown or corrupt: this query verifies every candidate blob.
            if (bytes === null) return null;
            if (!current.loadPostings(chunk, bytes)) return null;
            loadedPostings.add(chunk.name);
          }
          return current.candidates(pattern, enabled);
        }),
      );
      const flush = Effect.suspend(() => {
        if (!dirty) return Effect.void;
        dirty = false;
        return Effect.gen(function* () {
          // A lazily restored index cannot publish partial postings: the first
          // checkpoint after a restart completes the restore, then persists.
          if (manifestInfo !== null) {
            for (const chunk of postingChunks) {
              if (loadedPostings.has(chunk.name)) continue;
              const bytes = yield* readChunk(manifestInfo.codec, chunk.name);
              if (bytes === null || !current.loadPostings(chunk, bytes)) return;
              loadedPostings.add(chunk.name);
            }
          }
          const snapshot = yield* Effect.promise(() =>
            current.persisted(compress, io.chunkTargetBytes ?? CHUNK_TARGET_BYTES),
          );
          // A partially restored index has nothing safe to publish yet.
          if (snapshot === null) return;
          const size =
            snapshot.manifest.length +
            snapshot.chunks.reduce((total, chunk) => total + chunk.bytes.length, 0);
          if (size > io.hardLimitBytes) {
            yield* Effect.logWarning(
              `search index is ${size} bytes, past the ${io.hardLimitBytes} hard limit; keeping it in memory only`,
            );
            return;
          }
          if (size > io.softLimitBytes) {
            yield* Effect.logWarning(
              `search index is ${size} bytes, past the ${io.softLimitBytes} soft limit`,
            );
          }
          // Chunks first, manifest last: the manifest is the publish. A chunk
          // whose checksum the published manifest already carries is skipped —
          // indexing a few new blobs rewrites only the chunks they landed in.
          const names = new Set(snapshot.chunks.map((chunk) => chunk.name));
          for (const chunk of snapshot.chunks) {
            if (published.get(chunk.name) === chunk.checksum) continue;
            yield* io.write(chunk.name, chunk.bytes);
          }
          yield* io.write("manifest.json", snapshot.manifest);
          published.clear();
          for (const chunk of snapshot.chunks) published.set(chunk.name, chunk.checksum);
          // Chunks a smaller or compacted snapshot no longer references —
          // including orphans from an interrupted checkpoint — are deleted.
          const stored = yield* io.list.pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
          for (const name of stored) {
            if (name !== "manifest.json" && !names.has(name)) yield* io.remove(name);
          }
        });
      });
      return SearchIndex.of({ index: current, observe, forget, candidates, flush });
    }),
  );

export const underPath = (path: string, prefix: string | undefined): boolean => {
  if (prefix === undefined || prefix === "") return true;
  const trimmed = prefix.endsWith("/") ? prefix.slice(0, -1) : prefix;
  return path === trimmed || path.startsWith(`${trimmed}/`);
};

/**
 * Bounded, case-insensitive subsequence matching for the explicit fallback.
 * It deliberately does not share the literal verifier: an approximate hit
 * must never be able to enter `matches`.
 */
export const fuzzy = (
  pattern: string,
  text: string,
): {
  readonly ranges: ReadonlyArray<CharacterRange>;
  readonly score: number;
} | null => {
  if (pattern.length === 0 || pattern.length > 128 || text.length > 8_192) return null;
  const needle = pattern.toLocaleLowerCase();
  const haystack = text.toLocaleLowerCase();
  const positions: Array<{ readonly start: number; readonly end: number }> = [];
  let at = 0;
  for (const character of needle) {
    const found = haystack.indexOf(character, at);
    if (found === -1) return null;
    const end = found + character.length;
    positions.push({ start: found, end });
    at = end;
  }
  const ranges: CharacterRange[] = [];
  for (const position of positions) {
    const previous = ranges.at(-1);
    if (previous !== undefined && previous.end === position.start) {
      ranges[ranges.length - 1] = { start: previous.start, end: position.end };
    } else {
      ranges.push(position);
    }
  }
  const span = (positions.at(-1)?.end ?? 0) - (positions[0]?.start ?? 0);
  // A contiguous pair is worth more than every possible span difference,
  // then a shorter span wins within the same fragmentation class.
  const contiguousPairs = positions.length - ranges.length;
  return { ranges, score: contiguousPairs * 10_000 - span };
};

const Continuation = Schema.Struct({
  version: Schema.Literal(INDEX_VERSION),
  pattern: Schema.String,
  revision: Schema.String,
  path: Schema.optional(Schema.String),
  fixed: Schema.Boolean,
  ignoreCase: Schema.Boolean,
  fuzzy: Schema.Boolean,
  afterPath: Schema.String,
  afterLine: Schema.Finite,
});

type Continuation = (typeof Continuation)["Type"];

const encodeContinuation = (value: Continuation): string => btoa(JSON.stringify(value));

/** Refuse cursors whose query or resolved revision no longer has the same scope. */
export const continuation = (input: {
  readonly token: string | undefined;
  readonly pattern: string;
  readonly revision: Oid;
  readonly path: string | undefined;
  readonly fixed: boolean;
  readonly ignoreCase: boolean;
  readonly fuzzy: boolean;
}): Result.Result<Continuation | null, Invalid> => {
  if (input.token === undefined) return Result.succeed(null);
  let decoded: unknown;
  try {
    decoded = JSON.parse(atob(input.token));
  } catch {
    return Result.fail(new Invalid({ field: "continuation", reason: "invalid cursor" }));
  }
  const parsed = Schema.decodeUnknownOption(Continuation)(decoded);
  if (parsed._tag === "None") {
    return Result.fail(new Invalid({ field: "continuation", reason: "invalid cursor" }));
  }
  const value = parsed.value;
  if (
    value.pattern !== input.pattern ||
    value.revision !== input.revision ||
    value.path !== input.path ||
    value.fixed !== input.fixed ||
    value.ignoreCase !== input.ignoreCase ||
    value.fuzzy !== input.fuzzy
  ) {
    return Result.fail(new Invalid({ field: "continuation", reason: "cursor scope changed" }));
  }
  return Result.succeed(value);
};

export const nextContinuation = (input: Omit<Continuation, "version">): string =>
  encodeContinuation({ ...input, version: INDEX_VERSION });
