/**
 * Literal blob search and its deliberately disposable in-memory prefilter.
 *
 * Blob ordinals belong only to one Repository instance. Object storage remains
 * authoritative: postings only decide which blobs merit exact verification.
 */
import { Context, Layer, Result } from "effect";

import { isBinary } from "./Diff.ts";
import { Invalid } from "./Error.ts";
import type { Oid } from "./Store.ts";

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

export interface SearchResult {
  readonly matches: ReadonlyArray<SearchMatch>;
  readonly truncated: boolean;
  readonly skipped: ReadonlyArray<string>;
}

export type LineMatcher = (line: string) => boolean;

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
  #byBigram = new Map<number, BitSet>();
  #nextOrdinal = 0;

  get(oid: Oid): IndexedBlob | undefined {
    return this.#byOid.get(oid);
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
    return blob;
  }

  /** `null` asks callers to use the full verifier path. */
  candidates(pattern: string, enabled: boolean): ReadonlySet<number> | null {
    if (!enabled) return null;
    const bigrams = queryBigrams(pattern);
    if (bigrams === null) return null;
    let candidates: BitSet | undefined;
    for (const bigram of bigrams) {
      const posting = this.#byBigram.get(bigram);
      if (posting === undefined) return new Set();
      candidates = candidates === undefined ? posting : candidates.intersect(posting);
    }
    return candidates?.values() ?? new Set();
  }
}

/** Derived-state port. Hosts may later replace `memory` with persistent cache. */
export class SearchIndex extends Context.Service<SearchIndex, { readonly index: BlobIndex }>()(
  "git/SearchIndex",
) {}

/** The default is intentionally disposable and needs no storage capability. */
export const memory = Layer.sync(SearchIndex, () => SearchIndex.of({ index: new BlobIndex() }));

export const underPath = (path: string, prefix: string | undefined): boolean => {
  if (prefix === undefined || prefix === "") return true;
  const trimmed = prefix.endsWith("/") ? prefix.slice(0, -1) : prefix;
  return path === trimmed || path.startsWith(`${trimmed}/`);
};
