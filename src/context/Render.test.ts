/**
 * The V1 framing, byte for byte.
 *
 * docs/context-pack.md §15 asks that `git+context-render/v1` produce
 * cross-language-identical framing bytes and digest, and that a change of
 * placement or order change the digest. Neither is checkable by asserting that
 * this implementation agrees with itself, so the framing is spelled out here
 * as a literal: a second implementation in another language can be held to
 * these exact bytes, and a refactor that quietly changes the header, the
 * integer widths or the byte order fails here rather than in an audit a year
 * from now.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import { bytesToHex } from "../git/Format.ts";
import * as Render from "./Render.ts";

const u32 = (value: number): Uint8Array => {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, value, false);
  return out;
};

const u64 = (value: bigint): Uint8Array => {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, value, false);
  return out;
};

const encode = (text: string) => new TextEncoder().encode(text);

const framed = (segments: ReadonlyArray<Render.Segment>) => {
  const result = Render.frame(segments);
  assert.equal(Result.isSuccess(result), true, "framing should succeed");
  return Result.isSuccess(result) ? result.success : new Uint8Array();
};

const one: ReadonlyArray<Render.Segment> = [
  { placement: "user", mediaType: "text/plain", body: encode("hi") },
];

describe("ContextRender framing", () => {
  it("writes the exact V1 bytes", () => {
    const bytes = framed(one);

    // "git+ContextRender\0v1\0" | u32be 1 | u32be 4 "user" | u32be 10
    // "text/plain" | u64be 2 "hi"
    assert.equal(
      bytesToHex(bytes),
      "6769742b436f6e7465787452656e64657200763100" +
        "00000001" +
        "00000004" +
        "75736572" +
        "0000000a" +
        "746578742f706c61696e" +
        "0000000000000002" +
        "6869",
    );
  });

  it.effect("commits to a digest of those exact bytes", () =>
    Effect.gen(function* () {
      const committed = yield* Render.commit(one);
      assert.equal(
        committed.digest,
        // sha256 of the literal above.
        yield* Render.sha256(framed(one)),
      );
      assert.equal(committed.digest.startsWith("sha256:"), true);
    }),
  );

  it.effect("changes the digest when placement changes", () =>
    Effect.gen(function* () {
      const user = yield* Render.commit(one);
      const system = yield* Render.commit([
        { placement: "system", mediaType: "text/plain", body: encode("hi") },
      ]);
      assert.notEqual(user.digest, system.digest);
    }),
  );

  it.effect("changes the digest when order changes", () =>
    Effect.gen(function* () {
      const segments: ReadonlyArray<Render.Segment> = [
        { placement: "system", mediaType: "text/plain", body: encode("a") },
        { placement: "user", mediaType: "text/plain", body: encode("b") },
      ];
      const forwards = yield* Render.commit(segments);
      const backwards = yield* Render.commit([...segments].reverse());
      assert.notEqual(forwards.digest, backwards.digest);
    }),
  );

  it.effect("changes the digest when the media type changes", () =>
    Effect.gen(function* () {
      const plain = yield* Render.commit(one);
      const markdown = yield* Render.commit([
        { placement: "user", mediaType: "text/markdown", body: encode("hi") },
      ]);
      assert.notEqual(plain.digest, markdown.digest);
    }),
  );

  it("hashes bodies exactly as supplied, with no normalization", () => {
    // The two spellings of the same character — U+00E9, and U+0065 followed by
    // a combining acute — are different byte strings, and a producer that
    // normalized would commit to bytes it did not hand over. Written as
    // escapes because the difference is invisible in a source file.
    const composed = framed([
      { placement: "user", mediaType: "text/plain", body: encode("\u00e9") },
    ]);
    const decomposed = framed([
      { placement: "user", mediaType: "text/plain", body: encode("e\u0301") },
    ]);
    assert.notEqual(bytesToHex(composed), bytesToHex(decomposed));
  });

  it("round-trips through parse", () => {
    const segments: ReadonlyArray<Render.Segment> = [
      { placement: "system", mediaType: "text/plain", body: encode("standing") },
      { placement: "developer", mediaType: "text/x-typescript", body: encode("const a = 1\n") },
      { placement: "user", mediaType: "text/plain", body: new Uint8Array() },
    ];
    const parsed = Render.parse(framed(segments));
    assert.equal(Result.isSuccess(parsed), true);
    if (!Result.isSuccess(parsed)) return;
    assert.deepEqual(
      parsed.success.map((segment) => [segment.placement, segment.mediaType]),
      [
        ["system", "text/plain"],
        ["developer", "text/x-typescript"],
        ["user", "text/plain"],
      ],
    );
    assert.deepEqual([...parsed.success[1]!.body], [...encode("const a = 1\n")]);
  });

  it("refuses bytes that follow the last segment", () => {
    const bytes = framed(one);
    const extra = new Uint8Array(bytes.length + 1);
    extra.set(bytes);
    const parsed = Render.parse(extra);
    assert.equal(Result.isFailure(parsed), true);
    if (Result.isFailure(parsed)) {
      assert.match(parsed.failure.reason, /follow the last segment/);
    }
  });

  it("refuses a truncated segment rather than reading past the end", () => {
    const bytes = framed(one);
    const parsed = Render.parse(bytes.subarray(0, bytes.length - 1));
    assert.equal(Result.isFailure(parsed), true);
  });

  it("refuses framing that is not this format", () => {
    const parsed = Render.parse(encode("git+ContextRender\0v2\0some other thing"));
    assert.equal(Result.isFailure(parsed), true);
    if (Result.isFailure(parsed)) {
      assert.match(parsed.failure.reason, /not git\+context-render\/v1/);
    }
  });

  it("bounds a placement by the bytes it will frame, not by its characters", () => {
    // A placement this module frames and then refuses to parse would make an
    // intact retained render audit as `unreadable` for good, on a record
    // nothing can remove. Sixty CJK characters are 180 UTF-8 bytes.
    const wide = `acme/${"\u4e2d".repeat(60)}`;
    assert.equal(wide.length <= Render.MAX_PLACEMENT, true, "short in UTF-16 units");
    assert.equal(Render.isPlacement(wide), false, "and over the bound in bytes");

    const framed = Render.frame([{ placement: wide, mediaType: "text/plain", body: encode("x") }]);
    assert.equal(Result.isFailure(framed), true);
  });

  it("round-trips a placement that is wide but within the byte bound", () => {
    const wide = `acme/${"\u4e2d".repeat(20)}`;
    const parsed = Render.parse(
      framed([{ placement: wide, mediaType: "text/plain", body: encode("x") }]),
    );
    assert.equal(Result.isSuccess(parsed), true);
    if (Result.isSuccess(parsed)) assert.equal(parsed.success[0]?.placement, wide);
  });

  it("takes namespaced extension placements and refuses nonsense", () => {
    assert.equal(Render.isPlacement("system"), true);
    assert.equal(Render.isPlacement("acme/scratchpad"), true);
    assert.equal(Render.isPlacement("acme/scratch/pad"), false);
    assert.equal(Render.isPlacement("/leading"), false);
    assert.equal(Render.isPlacement("trailing/"), false);
    assert.equal(Render.isPlacement("invented"), false);
  });

  it("bounds the whole render, not only each segment of it", () => {
    // `MAX_SEGMENTS` times `MAX_BODY` is sixteen gigabytes, and the only other
    // cap bounds what the *selector* chooses — not the task segment appended
    // after it, and not a library caller handing `expose` its own segments.
    // The result goes into `context/render.bin` on an append-only ref that
    // replicates, so the writer bounds what it writes.
    const chunk = new Uint8Array(4 * 1024 * 1024);
    const many = Array.from({ length: 20 }, () => ({
      placement: "user" as const,
      mediaType: "text/plain",
      body: chunk,
    }));
    assert.equal(many.length <= Render.MAX_SEGMENTS, true, "under the segment count");
    assert.equal(chunk.length <= Render.MAX_BODY, true, "and each one under the body bound");

    const framed = Render.frame(many);
    assert.equal(Result.isFailure(framed), true);
    if (Result.isFailure(framed)) assert.match(framed.failure.reason, /may not exceed/);
  });

  it("does not frame a render its own parser will refuse for size", () => {
    // The header is twenty-five bytes — the preamble plus the segment count —
    // and `parse` measures the whole blob. Counted only over the segments, the
    // writer accepted a render just over the bound and wrote it to
    // `context/render.bin`, and `recompute` then refused the intact retained
    // bytes: an exposure auditing as unreadable forever on a record nothing
    // can remove.
    // Four segments whose bodies and per-segment headers come to exactly
    // `MAX_RENDER`, so the writer's old count passed and the parser's did not.
    // One buffer, shared: the point is the arithmetic, not the allocation.
    const each = 30;
    const body = new Uint8Array(Render.MAX_RENDER / 4 - each);
    const framed = Render.frame(
      Array.from({ length: 4 }, () => ({ placement: "user", mediaType: "text/plain", body })),
    );
    if (Result.isSuccess(framed)) {
      assert.equal(
        Result.isSuccess(Render.parse(framed.success)),
        true,
        "anything framed must parse",
      );
    }
  });

  it("refuses framing its own writer would never produce", () => {
    // Hand-framed, because `frame` refuses to build these — which is the
    // point: `parse` bounded only the lengths, so a retained render carrying
    // an empty placement, a two-slash extension or a zero-length media type
    // parsed cleanly and `recompute` answered `ok`. That is the one
    // distinction `recompute` exists to make — "these are the bytes" versus
    // "these are bytes that are not V1 framing" — and it means a second
    // conforming implementation could write renders this one's writer rejects.
    const hand = (placement: string, mediaType: string): Uint8Array => {
      const parts = [
        encode("git+ContextRender\0v1\0"),
        new Uint8Array([0, 0, 0, 1]),
        u32(encode(placement).length),
        encode(placement),
        u32(encode(mediaType).length),
        encode(mediaType),
        u64(1n),
        encode("x"),
      ];
      const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
      let at = 0;
      for (const part of parts) {
        out.set(part, at);
        at += part.length;
      }
      return out;
    };

    for (const [placement, mediaType] of [
      ["", "text/plain"],
      ["acme/scratch/pad", "text/plain"],
      ["invented", "text/plain"],
      ["user", ""],
    ] as const) {
      const parsed = Render.parse(hand(placement, mediaType));
      assert.equal(Result.isFailure(parsed), true, `${placement} / ${mediaType}`);
    }

    // And the writer's own output still round-trips.
    assert.equal(
      Result.isSuccess(
        Render.parse(framed([{ placement: "user", mediaType: "text/plain", body: encode("x") }])),
      ),
      true,
    );
  });

  it.effect("recomputes a retained render against its commitment", () =>
    Effect.gen(function* () {
      const committed = yield* Render.commit(one);
      const good = yield* Render.recompute(committed.bytes, committed.digest);
      assert.equal(good.ok, true);
      assert.equal(good.segments.length, 1);

      // One byte of the body changed: the retained bytes are still valid
      // framing, and they are no longer the bytes that were committed to.
      const tampered = Uint8Array.from(committed.bytes);
      tampered[tampered.length - 1] = 0x21;
      const bad = yield* Render.recompute(tampered, committed.digest);
      assert.equal(bad.ok, false);
      assert.match(bad.reason ?? "", /hashes to sha256:/);
    }),
  );
});
