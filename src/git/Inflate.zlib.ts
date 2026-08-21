/**
 * The pack reader's inflate, on the platform's own zlib.
 *
 * `Inflate.ts` decodes canonical Huffman a bit at a time, which is verifiable
 * and slow: on this repository's own objects it is 52x `node:zlib`, and a
 * clone served out of a packed repository spent about a third of its CPU
 * inside it. It exists because pack objects are stored back to back with no
 * length in front of them, so the reader has to learn where each zlib stream
 * ended — and `DecompressionStream`, the only decompressor a browser offers,
 * will not say.
 *
 * `PackFile.readAt` never needed that answer. It is handed an offset out of
 * the `.idx` and throws its `ByteSource` away when the object is decoded;
 * nothing downstream asks where the stream stopped. So the question it
 * actually has — "was the whole object inside the bytes I have?" — is one
 * zlib answers directly: a complete stream inflates and trailing bytes are
 * ignored, a truncated one is `Z_BUF_ERROR`. Reading a pack front to back off
 * the wire is the case that still needs the position, and `Pack.ts` still
 * uses the portable decoder for it.
 *
 * Node and workerd both provide `node:zlib` — the Worker already depends on
 * it through `server/Protocol.ts` — so both hosts read packs through this.
 * The browser has neither, and keeps the portable decoder.
 */
import * as zlib from "node:zlib";

import { type ByteSource, InflateError, MAX_INFLATED } from "./Inflate.ts";

/**
 * How many windows to pull before trying again.
 *
 * Doubling rather than one at a time: each attempt re-inflates from the start,
 * so growing the input by a constant would make a large object quadratic in
 * the number of windows. Doubling makes the attempts logarithmic and the bytes
 * re-inflated a geometric series — linear overall, at zlib's speed. Almost
 * every object is decided on the first attempt, the window being 64 KiB.
 */
const nextPull = (pulled: number): number => Math.max(1, pulled);

export const inflate = async (source: ByteSource, limit = MAX_INFLATED): Promise<Uint8Array> => {
  const held: Uint8Array[] = [];
  let size = 0;
  let pulled = 0;

  for (;;) {
    // Exhausting the source before the stream completes is the truncation
    // case, and it is reported as one rather than as an empty object.
    let want = nextPull(pulled);
    let drained = false;
    while (want > 0) {
      const chunk = await source.next();
      if (chunk === null) {
        drained = true;
        break;
      }
      held.push(chunk);
      size += chunk.length;
      pulled += 1;
      want -= 1;
    }
    if (held.length === 0) throw new InflateError({ reason: "no input" });

    // The window a pack reader hands over is 64 KiB and most objects are a
    // few hundred bytes, so joining it to nothing costs more than the inflate
    // does. One chunk is passed through as it is.
    let input: Uint8Array;
    if (held.length === 1) {
      input = held[0]!;
    } else {
      const joined = Buffer.allocUnsafe(size);
      let at = 0;
      for (const chunk of held) {
        joined.set(chunk, at);
        at += chunk.length;
      }
      input = joined;
    }

    try {
      return new Uint8Array(zlib.inflateSync(input, { maxOutputLength: limit }));
    } catch (error) {
      const code = error instanceof Error && "code" in error ? String(error.code) : undefined;
      // Out of input, not corrupt: the object runs past the bytes read so
      // far. Anything else — a bad header, a failed checksum, an object
      // larger than the caller allowed — is the stream's own fault.
      if (code !== "Z_BUF_ERROR") {
        throw new InflateError({
          reason:
            code === "ERR_BUFFER_TOO_LARGE"
              ? `inflated past the ${limit} byte limit`
              : `${code ?? "zlib"}: ${String(error)}`,
        });
      }
      if (drained) throw new InflateError({ reason: "truncated zlib stream" });
    }
  }
};
