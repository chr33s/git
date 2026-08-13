/**
 * Streaming zlib inflate (RFC 1950 wrapper, RFC 1951 blocks) in plain
 * JavaScript.
 *
 * Pack object boundaries are implicit: each object's data is its own zlib
 * stream and the next starts where it ends. `DecompressionStream` cannot
 * report that position and `node:zlib` is absent in browsers, so this is
 * pull-based — it consumes exactly the stream's bytes and pushes back any
 * over-pull.
 *
 * Decoding is canonical-Huffman bit-by-bit (the `puff` construction):
 * verifiable, and unoptimized until a profile says otherwise.
 */

export interface ByteSource {
  /** Next chunk, or `null` at end of input. */
  readonly next: () => Promise<Uint8Array | null>;
  /** Return unconsumed bytes to the front of the source. */
  readonly pushBack: (bytes: Uint8Array) => void;
}

export class InflateError extends Error {
  override readonly name = "InflateError";
}

const corrupt = (reason: string): never => {
  throw new InflateError(reason);
};

/* Length codes 257–285 and distance codes 0–29, per RFC 1951 §3.2.5. */
const LENGTH_BASE = [
  3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
  163, 195, 227, 258,
];
const LENGTH_EXTRA = [
  0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DISTANCE_BASE = [
  1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049,
  3073, 4097, 6145, 8193, 12289, 16385, 24577,
];
const DISTANCE_EXTRA = [
  0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13,
];
const CODE_LENGTH_ORDER = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];

interface Huffman {
  /** `count[n]` codes of length `n`. */
  readonly count: Int32Array;
  /** Symbols sorted by code. */
  readonly symbol: Int32Array;
}

const construct = (lengths: ReadonlyArray<number>): Huffman => {
  const count = new Int32Array(16);
  for (const length of lengths) count[length] = (count[length] ?? 0) + 1;
  if (count[0] === lengths.length) return { count, symbol: new Int32Array(0) };

  // Over-subscribed code sets are invalid; incomplete ones are tolerated the
  // way inflate implementations conventionally do (single-symbol distances).
  let left = 1;
  for (let length = 1; length <= 15; length++) {
    left = (left << 1) - count[length]!;
    if (left < 0) corrupt("over-subscribed Huffman code");
  }

  const offsets = new Int32Array(16);
  for (let length = 1; length < 15; length++) {
    offsets[length + 1] = offsets[length]! + count[length]!;
  }
  const symbol = new Int32Array(lengths.length);
  lengths.forEach((length, index) => {
    if (length !== 0) {
      symbol[offsets[length]!] = index;
      offsets[length] = offsets[length]! + 1;
    }
  });
  return { count, symbol };
};

/** Pull-based bit reader: awaits only when the current chunk runs dry. */
class Reader {
  readonly #source: ByteSource;
  #chunk: Uint8Array = new Uint8Array(0);
  #at = 0;
  #bitBuffer = 0;
  #bitCount = 0;

  constructor(source: ByteSource) {
    this.#source = source;
  }

  async #refill(): Promise<void> {
    for (;;) {
      const next = await this.#source.next();
      if (next === null) corrupt("truncated deflate stream");
      if (next!.length > 0) {
        this.#chunk = next!;
        this.#at = 0;
        return;
      }
    }
  }

  async #byte(): Promise<number> {
    if (this.#at >= this.#chunk.length) await this.#refill();
    return this.#chunk[this.#at++]!;
  }

  async bits(n: number): Promise<number> {
    while (this.#bitCount < n) {
      this.#bitBuffer |= (await this.#byte()) << this.#bitCount;
      this.#bitCount += 8;
    }
    const value = this.#bitBuffer & ((1 << n) - 1);
    this.#bitBuffer >>>= n;
    this.#bitCount -= n;
    return value;
  }

  alignByte(): void {
    const drop = this.#bitCount % 8;
    this.#bitBuffer >>>= drop;
    this.#bitCount -= drop;
  }

  /** A whole byte after alignment — buffered bits first, then the chunk. */
  async alignedByte(): Promise<number> {
    if (this.#bitCount >= 8) {
      const value = this.#bitBuffer & 0xff;
      this.#bitBuffer >>>= 8;
      this.#bitCount -= 8;
      return value;
    }
    return this.#byte();
  }

  /** Canonical decode, one bit at a time — the `puff` construction. */
  async decode(huffman: Huffman): Promise<number> {
    let code = 0;
    let first = 0;
    let index = 0;
    for (let length = 1; length <= 15; length++) {
      code |= await this.bits(1);
      const count = huffman.count[length]!;
      if (code - first < count) return huffman.symbol[index + (code - first)]!;
      index += count;
      first = (first + count) << 1;
      code <<= 1;
    }
    return corrupt("invalid Huffman code") as never;
  }

  /** Hand everything unconsumed back to the source. */
  release(): void {
    const buffered: number[] = [];
    while (this.#bitCount >= 8) {
      buffered.push(this.#bitBuffer & 0xff);
      this.#bitBuffer >>>= 8;
      this.#bitCount -= 8;
    }
    const rest = this.#chunk.subarray(this.#at);
    if (buffered.length > 0 || rest.length > 0) {
      const out = new Uint8Array(buffered.length + rest.length);
      out.set(buffered);
      out.set(rest, buffered.length);
      this.#source.pushBack(out);
    }
    this.#chunk = new Uint8Array(0);
    this.#at = 0;
  }
}

class Output {
  #buffer = new Uint8Array(64 * 1024);
  #length = 0;

  get length(): number {
    return this.#length;
  }

  #limit = MAX_INFLATED;

  bound(limit: number): void {
    this.#limit = limit;
  }

  #grow(needed: number): void {
    if (this.#length + needed > this.#limit) {
      corrupt(`inflated stream exceeds ${this.#limit} bytes`);
    }
    if (this.#length + needed <= this.#buffer.length) return;
    let capacity = this.#buffer.length * 2;
    while (capacity < this.#length + needed) capacity *= 2;
    const next = new Uint8Array(capacity);
    next.set(this.#buffer.subarray(0, this.#length));
    this.#buffer = next;
  }

  push(byte: number): void {
    this.#grow(1);
    this.#buffer[this.#length++] = byte;
  }

  copyBack(distance: number, length: number): void {
    if (distance > this.#length) corrupt("back-reference before output start");
    this.#grow(length);
    let from = this.#length - distance;
    for (let index = 0; index < length; index++) {
      this.#buffer[this.#length++] = this.#buffer[from++]!;
    }
  }

  take(): Uint8Array {
    return this.#buffer.subarray(0, this.#length);
  }
}

const FIXED_LITERALS = construct(
  Array.from({ length: 288 }, (_, index) =>
    index < 144 ? 8 : index < 256 ? 9 : index < 280 ? 7 : 8,
  ),
);
const FIXED_DISTANCES = construct(Array.from({ length: 30 }, () => 5));

const inflateBlocks = async (reader: Reader, output: Output): Promise<void> => {
  for (;;) {
    const final = await reader.bits(1);
    const type = await reader.bits(2);

    if (type === 0) {
      // Stored: aligned, length and its complement, then raw bytes.
      reader.alignByte();
      const length = (await reader.alignedByte()) | ((await reader.alignedByte()) << 8);
      const check = (await reader.alignedByte()) | ((await reader.alignedByte()) << 8);
      if (length !== (~check & 0xffff)) corrupt("stored block length mismatch");
      for (let index = 0; index < length; index++) output.push(await reader.alignedByte());
    } else if (type === 1 || type === 2) {
      let literals = FIXED_LITERALS;
      let distances = FIXED_DISTANCES;

      if (type === 2) {
        const hlit = (await reader.bits(5)) + 257;
        const hdist = (await reader.bits(5)) + 1;
        const hclen = (await reader.bits(4)) + 4;
        const codeLengths = Array.from({ length: 19 }, () => 0);
        for (let index = 0; index < hclen; index++) {
          codeLengths[CODE_LENGTH_ORDER[index]!] = await reader.bits(3);
        }
        const codeTable = construct(codeLengths);

        const lengths: number[] = [];
        while (lengths.length < hlit + hdist) {
          const symbol = await reader.decode(codeTable);
          if (symbol < 16) lengths.push(symbol);
          else if (symbol === 16) {
            const previous = lengths.at(-1);
            if (previous === undefined) corrupt("repeat with no previous length");
            const repeat = 3 + (await reader.bits(2));
            for (let index = 0; index < repeat; index++) lengths.push(previous!);
          } else if (symbol === 17) {
            const repeat = 3 + (await reader.bits(3));
            for (let index = 0; index < repeat; index++) lengths.push(0);
          } else {
            const repeat = 11 + (await reader.bits(7));
            for (let index = 0; index < repeat; index++) lengths.push(0);
          }
        }
        if (lengths.length !== hlit + hdist) corrupt("code lengths overflow their count");
        if (lengths[256] === 0) corrupt("dynamic block with no end-of-block code");

        literals = construct(lengths.slice(0, hlit));
        distances = construct(lengths.slice(hlit));
      }

      for (;;) {
        const symbol = await reader.decode(literals);
        if (symbol < 256) {
          output.push(symbol);
        } else if (symbol === 256) {
          break;
        } else {
          const lengthIndex = symbol - 257;
          if (lengthIndex >= LENGTH_BASE.length) corrupt(`invalid length code ${symbol}`);
          const length =
            LENGTH_BASE[lengthIndex]! + (await reader.bits(LENGTH_EXTRA[lengthIndex]!));
          const distanceSymbol = await reader.decode(distances);
          if (distanceSymbol >= DISTANCE_BASE.length) {
            corrupt(`invalid distance code ${distanceSymbol}`);
          }
          const distance =
            DISTANCE_BASE[distanceSymbol]! + (await reader.bits(DISTANCE_EXTRA[distanceSymbol]!));
          output.copyBack(distance, length);
        }
      }
    } else {
      corrupt("reserved block type");
    }

    if (final === 1) return;
  }
};

const adler32 = (bytes: Uint8Array): number => {
  let a = 1;
  let b = 0;
  for (let index = 0; index < bytes.length;) {
    // Defer the modulo: 5552 is the largest run that cannot overflow 2^32.
    const limit = Math.min(index + 5552, bytes.length);
    for (; index < limit; index++) {
      a += bytes[index]!;
      b += a;
    }
    a %= 65521;
    b %= 65521;
  }
  return ((b << 16) | a) >>> 0;
};

/**
 * Inflate one complete zlib stream from the source, consuming exactly its
 * bytes: whatever was over-pulled from the final chunk is pushed back.
 */
/**
 * The most bytes an inflate will produce before it gives up.
 *
 * A deflate stream expands by up to ~1000:1, so a few megabytes of pack can
 * ask for gigabytes of output — and the size the object header declared is
 * only compared *after* the stream has been inflated. Callers that know the
 * expected size pass it; the default is a backstop for callers that do not.
 */
export const MAX_INFLATED = 512 * 1024 * 1024;

export const inflate = async (source: ByteSource, limit = MAX_INFLATED): Promise<Uint8Array> => {
  const reader = new Reader(source);
  try {
    const cmf = await reader.alignedByte();
    const flg = await reader.alignedByte();
    if ((cmf & 0x0f) !== 8) corrupt(`unsupported compression method ${cmf & 0x0f}`);
    if (((cmf << 8) | flg) % 31 !== 0) corrupt("zlib header check failed");
    if ((flg & 0x20) !== 0) corrupt("preset dictionaries are not supported");

    const output = new Output();
    // Clamped, not replaced: the caller that knows the expected size reads it
    // out of the input it is validating, so letting it raise the ceiling
    // hands the bound to whoever wrote the pack.
    output.bound(Math.min(limit, MAX_INFLATED));
    await inflateBlocks(reader, output);
    const bytes = output.take();

    reader.alignByte();
    const expected =
      (((await reader.alignedByte()) << 24) |
        ((await reader.alignedByte()) << 16) |
        ((await reader.alignedByte()) << 8) |
        (await reader.alignedByte())) >>>
      0;
    if (adler32(bytes) !== expected) corrupt("adler-32 checksum mismatch");

    return bytes;
  } finally {
    reader.release();
  }
};
