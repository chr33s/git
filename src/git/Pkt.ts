/**
 * pkt-line framing, both directions.
 *
 * Shared by the server (`server/Protocol.ts`) and the client
 * (`client/Fetch.ts`) — and platform-neutral, because the client side of it
 * runs in browsers.
 */
import { Predicate } from "effect";

import { PackCorrupt } from "./Error.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const corrupt = (reason: string) => new PackCorrupt({ reason });

/** `<4-hex length including itself><payload>`; "0000" is the flush packet. */
export const pkt = (line: string | Uint8Array): Uint8Array => {
  const payload = Predicate.isString(line) ? encoder.encode(line) : line;
  const header = encoder.encode((payload.length + 4).toString(16).padStart(4, "0"));
  const out = new Uint8Array(header.length + payload.length);
  out.set(header);
  out.set(payload, header.length);
  return out;
};
export const FLUSH = encoder.encode("0000");
/** Protocol v2 separates a command's arguments from its header with `0001`. */
export const DELIM = encoder.encode("0001");
/** And ends a response with `0002`, so a client knows nothing more follows. */
export const RESPONSE_END = encoder.encode("0002");

/**
 * Side-band channels, as `side-band-64k` defines them: pack bytes on 1,
 * progress on 2, a fatal error on 3. Multiplexing exists so a fetch can say
 * something while the pack is still being built — without it a slow walk is
 * indistinguishable from a hung connection.
 */
export const BAND = { pack: 1, progress: 2, error: 3 } as const;

/**
 * The most pack bytes one side-band packet may carry.
 *
 * git's hard limit is 65520 bytes for the *whole* pkt-line, header included
 * (`LARGE_PACKET_MAX`), and a side-band packet spends 4 of those on the length
 * header and 1 on the channel byte. A payload of 65520 would frame as 65525
 * and real git refuses the stream with "bad line length" — while this
 * repository's own reader accepts it, so only an actual git client notices.
 */
export const SIDEBAND_MAX = 65_515;

/** One side-band packet. Callers on band 1 must respect `SIDEBAND_MAX`. */
export const band = (channel: (typeof BAND)[keyof typeof BAND], payload: string | Uint8Array) => {
  const bytes = Predicate.isString(payload) ? encoder.encode(payload) : payload;
  const framed = new Uint8Array(bytes.length + 1);
  framed[0] = channel;
  framed.set(bytes, 1);
  return pkt(framed);
};

/** A chunk split across as many band-1 packets as its length needs. */
export const bandChunks = (payload: Uint8Array): ReadonlyArray<Uint8Array> => {
  const out: Uint8Array[] = [];
  for (let offset = 0; offset < payload.length; offset += SIDEBAND_MAX) {
    out.push(band(BAND.pack, payload.subarray(offset, offset + SIDEBAND_MAX)));
  }
  return out;
};

/**
 * Pull-based pkt-line reader; `rest()` hands the remainder to the pack
 * parser. Exported because the client side of the protocol (`artifacts`
 * import) reads the same framing.
 */
export class PktReader {
  readonly #iterator: AsyncIterator<Uint8Array>;
  #pending: Uint8Array[] = [];

  constructor(input: AsyncIterable<Uint8Array>) {
    this.#iterator = input[Symbol.asyncIterator]();
  }

  async #read(n: number): Promise<Uint8Array | null> {
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const head = this.#pending.shift();
      if (head === undefined) {
        const result = await this.#iterator.next();
        if (result.done === true) {
          if (filled === 0) return null;
          throw corrupt("pkt-line truncated");
        }
        if (result.value.length > 0) this.#pending.push(result.value);
        continue;
      }
      const use = Math.min(head.length, n - filled);
      out.set(head.subarray(0, use), filled);
      if (use < head.length) this.#pending.unshift(head.subarray(use));
      filled += use;
    }
    return out;
  }

  /** One pkt-line's payload, or one of the special packets. */
  async next(): Promise<Uint8Array | "flush" | "delim" | "end" | "eof"> {
    const header = await this.#read(4);
    if (header === null) return "eof";
    const hex = decoder.decode(header);
    // All four bytes, not a hex prefix: `parseInt("00zz", 16)` is 0, which
    // would read as a flush packet and leave the rest of the header to be
    // misframed as the next one — a desynchronised stream instead of an error.
    if (!/^[0-9a-fA-F]{4}$/.test(hex)) throw corrupt(`bad pkt-line length '${hex}'`);
    const length = Number.parseInt(hex, 16);
    if (length === 0) return "flush";
    // 1 and 2 are v2's delimiter and response-end; they are lengths no v0
    // reader would accept, which is how the formats stay distinguishable.
    if (length === 1) return "delim";
    if (length === 2) return "end";
    if (length < 4) throw corrupt(`bad pkt-line length ${length}`);
    if (length === 4) return new Uint8Array(0);
    const payload = await this.#read(length - 4);
    if (payload === null) throw corrupt("pkt-line truncated");
    return payload;
  }

  /** Everything after the pkt-line section — for receive-pack, the packfile. */
  rest(): AsyncIterable<Uint8Array> {
    const pending = this.#pending;
    const iterator = this.#iterator;
    return (async function* () {
      yield* pending.splice(0);
      for (;;) {
        const result = await iterator.next();
        if (result.done === true) return;
        yield result.value;
      }
    })();
  }
}
