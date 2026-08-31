/**
 * ContextRender: the exact bytes that crossed the context-to-invocation
 * boundary, framed so two implementations cannot disagree about the digest.
 *
 * A Context Pack says which repository evidence was *selected*. That is not
 * the same claim as what was *handed over*: a producer can select honestly and
 * still reorder, retitle or truncate on the way out, and nothing in the pack
 * would show it. The render commitment closes that gap by hashing the ordered
 * segments themselves — placement, media type and body bytes — so any change
 * to any of the three changes the digest (docs/context-pack.md §8).
 *
 * ```text
 * ASCII "git+ContextRender\0v1\0"
 * u32be segmentCount
 * per segment: u32be placementLen, placement, u32be mediaTypeLen, mediaType,
 *              u64be bodyLen, body
 * ```
 *
 * There is exactly one framing, and it is spelled out to the byte. A format
 * with a profile, an alternative delimiter or a JSON encoding is a format two
 * conforming implementations can hash differently, which would make the digest
 * a claim about an implementation rather than about the bytes.
 *
 * What it does not prove is worth saying: the digest is a harness-side
 * boundary claim. It does not show that a provider received the request, kept
 * the hierarchy internally, or that the model read a word of it (§8.2).
 */
import { Effect, Result } from "effect";

import { Invalid } from "../git/Error.ts";
import { bytesToHex, concatBytes } from "../git/Format.ts";

/** The one format identifier a V1 exposure may name. */
export const FORMAT = "git+context-render/v1";

/** The magic and version, as ASCII with the two NUL terminators. */
const PREAMBLE = "git+ContextRender\0v1\0";

/**
 * Core placement values.
 *
 * Placement records where a segment sat in the invocation, not what authority
 * it carried. A `system` segment is not instruction-authoritative because it
 * is called `system`; §7 and `Pack.Authority` are where that question is
 * answered, and conflating the two is how retrieved content talks its way into
 * being obeyed.
 */
const encoder = new TextEncoder();

export const PLACEMENTS = ["system", "developer", "user", "tool", "other"] as const;
export type Placement = (typeof PLACEMENTS)[number];

const core = new Set<string>(PLACEMENTS);

/**
 * Whether a placement is one this implementation will frame.
 *
 * Core values, or a namespaced extension — `vendor/thing` — which §8 allows so
 * that a harness with a placement of its own does not have to call it `other`
 * and lose the distinction in every later audit.
 */
export const isPlacement = (value: string): boolean => {
  if (core.has(value)) return true;
  const slash = value.indexOf("/");
  return (
    slash > 0 &&
    slash < value.length - 1 &&
    // Bounded in *bytes*, which is what the framing writes and what `parse`
    // reads back. Measured in UTF-16 units, a placement of sixty CJK
    // characters passed here, framed to a hundred and eighty bytes, and was
    // then refused by this module's own parser — so an intact retained render
    // audited as `unreadable` forever, on a record nothing can remove.
    encoder.encode(value).length <= MAX_PLACEMENT &&
    !value.includes("\0") &&
    value.indexOf("/", slash + 1) === -1
  );
};

/**
 * Host-defined bounds (§12).
 *
 * The framing itself is unbounded — `u64be` body lengths are there so it never
 * has to change — but a parser reading somebody else's retained render is a
 * parser that must not be talked into allocating what a length field claims.
 */
export const MAX_SEGMENTS = 1024;
export const MAX_PLACEMENT = 128;
export const MAX_MEDIA_TYPE = 256;
export const MAX_BODY = 16 * 1024 * 1024;

/**
 * The whole framing, which no per-segment bound reaches.
 *
 * `MAX_SEGMENTS` and `MAX_BODY` bound the parts and multiply to sixteen
 * gigabytes. `context for` caps the *selector's* budget at
 * `Select.MAX_EVIDENCE`, which is the right place for the budget and covers
 * neither the task segment the selector appends nor a library caller handing
 * `expose` its own segments — and the result is written to
 * `context/render.bin` on an append-only ref that replicates. So the writer
 * bounds what it writes, which is the only place that sees the total.
 *
 * Well above any real render: `Select.MAX_EVIDENCE` is four megabytes, and the
 * framing adds a header per segment.
 */
export const MAX_RENDER = 64 * 1024 * 1024;

export interface Segment {
  /** Logical invocation placement understood by the harness. */
  readonly placement: string;
  readonly mediaType: string;
  /** The exact bytes crossing the boundary; hashed as supplied. */
  readonly body: Uint8Array;
}

const u32 = (value: number): Uint8Array => {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, false);
  return bytes;
};

const u64 = (value: number): Uint8Array => {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), false);
  return bytes;
};

/**
 * The exact V1 framing bytes.
 *
 * No Unicode normalization, no terminator on the strings, no padding: the
 * lengths are the framing, and the bodies are hashed exactly as supplied. A
 * producer that normalized here would hand the provider one byte string and
 * commit to another.
 */
export const frame = (segments: ReadonlyArray<Segment>): Result.Result<Uint8Array, Invalid> => {
  if (segments.length > MAX_SEGMENTS) {
    return Result.fail(
      new Invalid({
        field: "segments",
        reason: `a render may not carry more than ${MAX_SEGMENTS} segments`,
      }),
    );
  }

  // Seeded with the header, because `parse` measures the whole blob. Counting
  // only the segments, `frame` accepted a render twenty-five bytes over and
  // wrote it to `context/render.bin` — and `recompute` then refused the intact
  // retained bytes as "render is N bytes", which is an exposure auditing as
  // unreadable forever on a record nothing can remove. The same failure the
  // placement bound is written to avoid, reached through the total.
  const header = encoder.encode(PREAMBLE);
  let total = header.length + 4;
  const parts: Array<Uint8Array> = [header, u32(segments.length)];
  for (const [index, segment] of segments.entries()) {
    if (!isPlacement(segment.placement)) {
      return Result.fail(
        new Invalid({
          field: "placement",
          reason: `segment ${index} has placement '${segment.placement}', which is neither core nor a namespaced extension`,
        }),
      );
    }
    const placement = encoder.encode(segment.placement);
    const mediaType = encoder.encode(segment.mediaType);
    if (mediaType.length === 0 || mediaType.length > MAX_MEDIA_TYPE) {
      return Result.fail(
        new Invalid({
          field: "mediaType",
          reason: `segment ${index} has a media type of ${mediaType.length} bytes`,
        }),
      );
    }
    if (segment.body.length > MAX_BODY) {
      return Result.fail(
        new Invalid({
          field: "body",
          reason: `segment ${index} is ${segment.body.length} bytes, over the ${MAX_BODY}-byte bound`,
        }),
      );
    }
    parts.push(u32(placement.length), placement, u32(mediaType.length), mediaType);
    parts.push(u64(segment.body.length), segment.body);
    // Accumulated as it goes, so a caller cannot reach the bound by adding
    // segments each of which is under it. `MAX_SEGMENTS` times `MAX_BODY` is
    // sixteen gigabytes, and the only other cap — `--max-bytes` — bounds what
    // the *selector* chooses, not the task segment appended after it and not a
    // library caller supplying its own segments.
    total += 16 + placement.length + mediaType.length + segment.body.length;
    if (total > MAX_RENDER) {
      return Result.fail(
        new Invalid({
          field: "render",
          reason: `a render may not exceed ${MAX_RENDER} bytes`,
        }),
      );
    }
  }
  return Result.succeed(concatBytes(parts));
};

/** The SHA-256 of exact bytes, qualified the way a payload spells a digest. */
export const sha256 = Effect.fn("context.Render.sha256")(function* (bytes: Uint8Array) {
  const digested = yield* Effect.promise(() =>
    crypto.subtle.digest("SHA-256", bytes.slice().buffer),
  );
  return `sha256:${bytesToHex(new Uint8Array(digested))}`;
});

/**
 * The commitment: `sha256` of the exact framing bytes.
 *
 * Both halves are returned because a caller almost always needs both — the
 * digest goes in the exposure payload, and the bytes are what §10 retains so
 * the digest can be recomputed later rather than merely believed.
 */
export const commit = Effect.fn("context.Render.commit")(function* (
  segments: ReadonlyArray<Segment>,
) {
  const framed = yield* Effect.fromResult(frame(segments));
  return { bytes: framed, digest: yield* sha256(framed) } as const;
});

/**
 * The segments retained framing bytes hold, or why they are not V1 framing.
 *
 * Strict about the end as well as the beginning: trailing bytes mean the
 * reader and the writer disagree about where the render stopped, and a parser
 * that ignored them would recompute a digest over bytes it had not read.
 */
export const parse = (bytes: Uint8Array): Result.Result<ReadonlyArray<Segment>, Invalid> => {
  const failed = (reason: string) => Result.fail(new Invalid({ field: "render", reason }));

  const preamble = encoder.encode(PREAMBLE);
  if (bytes.length < preamble.length + 4) return failed("render is shorter than its own header");
  for (const [index, byte] of preamble.entries()) {
    if (bytes[index] !== byte) return failed(`render is not ${FORMAT} framing`);
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let at = preamble.length;

  const count = view.getUint32(at, false);
  at += 4;
  if (count > MAX_SEGMENTS) return failed(`render claims ${count} segments`);
  // The reader is held to the same total the writer is, for the reason it is
  // held to `isPlacement`: bytes this module would not write are not bytes it
  // should call V1 framing.
  if (bytes.length > MAX_RENDER) return failed(`render is ${bytes.length} bytes`);

  const segments: Array<Segment> = [];
  const decoder = new TextDecoder("utf-8", { fatal: false });

  for (let index = 0; index < count; index += 1) {
    if (at + 4 > bytes.length) return failed(`segment ${index} is truncated`);
    const placementLength = view.getUint32(at, false);
    at += 4;
    if (placementLength > MAX_PLACEMENT || at + placementLength > bytes.length) {
      return failed(`segment ${index} has an unusable placement length`);
    }
    const placement = decoder.decode(bytes.subarray(at, at + placementLength));
    at += placementLength;
    // The same rule the writer is held to. Bounded only in length, this
    // accepted framing `frame` would never produce — an empty placement, or
    // `vendor/a/b` — so `recompute` answered `ok` for bytes that are not V1
    // framing, which is the one distinction it exists to make, and a second
    // conforming implementation could write renders this one's writer rejects.
    if (!isPlacement(placement)) {
      return failed(`segment ${index} has placement '${placement}', which is not V1 framing`);
    }

    if (at + 4 > bytes.length) return failed(`segment ${index} is truncated`);
    const mediaTypeLength = view.getUint32(at, false);
    at += 4;
    if (mediaTypeLength > MAX_MEDIA_TYPE || at + mediaTypeLength > bytes.length) {
      return failed(`segment ${index} has an unusable media type length`);
    }
    if (mediaTypeLength === 0) return failed(`segment ${index} has no media type`);
    const mediaType = decoder.decode(bytes.subarray(at, at + mediaTypeLength));
    at += mediaTypeLength;

    if (at + 8 > bytes.length) return failed(`segment ${index} is truncated`);
    const bodyLength = view.getBigUint64(at, false);
    at += 8;
    // Compared as a bigint before it is ever narrowed: a length past
    // `Number.MAX_SAFE_INTEGER` narrowed first would round to something the
    // bounds check then accepted.
    if (bodyLength > BigInt(MAX_BODY) || BigInt(at) + bodyLength > BigInt(bytes.length)) {
      return failed(`segment ${index} claims ${bodyLength} body bytes`);
    }
    const size = Number(bodyLength);
    segments.push({ placement, mediaType, body: bytes.subarray(at, at + size) });
    at += size;
  }

  if (at !== bytes.length) return failed(`${bytes.length - at} bytes follow the last segment`);
  return Result.succeed(segments);
};

/**
 * Whether retained bytes still produce the digest an exposure committed to.
 *
 * Parsed as well as hashed, so the answer distinguishes "these are the bytes"
 * from "these are bytes that happen to hash the same way but are not V1
 * framing" — which cannot happen, and is exactly the kind of thing an audit
 * should not have to take on faith.
 */
export const recompute = Effect.fn("context.Render.recompute")(function* (
  bytes: Uint8Array,
  digest: string,
) {
  const parsed = parse(bytes);
  if (Result.isFailure(parsed)) {
    return { ok: false, reason: parsed.failure.reason, segments: [] } as const;
  }
  const recomputed = yield* sha256(bytes);
  return recomputed === digest
    ? ({ ok: true, reason: null, segments: parsed.success } as const)
    : ({
        ok: false,
        reason: `retained render hashes to ${recomputed}, not ${digest}`,
        segments: parsed.success,
      } as const);
});
