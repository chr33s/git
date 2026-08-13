import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { bandChunks, pkt, PktReader } from "./Pkt.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const stream = (...chunks: ReadonlyArray<Uint8Array | string>): AsyncIterable<Uint8Array> =>
  (async function* () {
    for (const chunk of chunks) yield typeof chunk === "string" ? encoder.encode(chunk) : chunk;
  })();

describe("PktReader", () => {
  it("reads a payload, the special packets, then end of input", async () => {
    const reader = new PktReader(stream(pkt("want\n"), "0000", "0001", "0002"));

    const first = await reader.next();
    assert.equal(decoder.decode(first as Uint8Array), "want\n");
    assert.equal(await reader.next(), "flush");
    assert.equal(await reader.next(), "delim");
    assert.equal(await reader.next(), "end");
    assert.equal(await reader.next(), "eof");
  });

  it("reads a line split across chunk boundaries", async () => {
    const line = pkt("have 1234\n");
    const reader = new PktReader(
      stream(line.subarray(0, 2), line.subarray(2, 7), line.subarray(7)),
    );

    assert.equal(decoder.decode((await reader.next()) as Uint8Array), "have 1234\n");
  });

  it("keeps every side-band packet inside git's line limit", () => {
    // git's `LARGE_PACKET_MAX` is 65520 bytes for the whole line, header
    // included. A packet one byte over is refused with "bad line length" by
    // real git while this repository's own reader accepts it — so the bound
    // has to be asserted here rather than discovered by a clone.
    const framed = bandChunks(new Uint8Array(200_000));
    for (const packet of framed) {
      assert.ok(packet.length <= 65_520, `side-band packet is ${packet.length} bytes`);
    }
    assert.ok(framed.length > 1);
  });

  it("refuses a length header that is not four hex digits", async () => {
    // `parseInt("00zz", 16)` is 0, so a lenient reader would report a flush
    // packet here and then reparse the rest of the stream shifted by two
    // bytes — a desynchronised conversation rather than a rejected one.
    const reader = new PktReader(stream("00zz0009hello\n"));

    const failure = await reader.next().then(
      () => null,
      (error: unknown) => error as { _tag?: string },
    );
    assert.equal(failure?._tag, "PackCorrupt");
  });

  it("hands the unread remainder to the pack parser", async () => {
    const reader = new PktReader(stream(pkt("done\n"), "PACK-bytes"));
    await reader.next();

    const rest: string[] = [];
    for await (const chunk of reader.rest()) rest.push(decoder.decode(chunk));
    assert.equal(rest.join(""), "PACK-bytes");
  });
});
