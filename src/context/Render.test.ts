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
