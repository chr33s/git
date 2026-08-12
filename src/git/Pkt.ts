/**
 * pkt-line framing, both directions.
 *
 * Shared by the server (`server/Protocol.ts`) and the client
 * (`client/Fetch.ts`) — and platform-neutral, because the client side of it
 * runs in browsers.
 */
import { PackCorrupt } from "./Error.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const corrupt = (reason: string) => new PackCorrupt({ reason });

/** `<4-hex length including itself><payload>`; "0000" is the flush packet. */
export const pkt = (line: string | Uint8Array): Uint8Array => {
  const payload = typeof line === "string" ? encoder.encode(line) : line;
  const header = encoder.encode((payload.length + 4).toString(16).padStart(4, "0"));
  const out = new Uint8Array(header.length + payload.length);
  out.set(header);
  out.set(payload, header.length);
  return out;
};
export const FLUSH = encoder.encode("0000");

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

  /** One pkt-line's payload, `"flush"`, or `"eof"`. */
  async next(): Promise<Uint8Array | "flush" | "eof"> {
    const header = await this.#read(4);
    if (header === null) return "eof";
    const length = Number.parseInt(decoder.decode(header), 16);
    if (Number.isNaN(length)) throw corrupt(`bad pkt-line length '${decoder.decode(header)}'`);
    if (length === 0) return "flush";
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
