/**
 * Immutable static-artifact HTTP: Range, ETag, and cache headers in one place.
 *
 * Bundle bytes are the first caller. Later archives or SHA-addressed packs
 * should come through here rather than grow a second copy of the same rules.
 * Ref-relative endpoints (`/main/files/…`, `/HEAD/…`) must not use this —
 * they are not immutable, however cacheable they look.
 */
import { Effect, Stream } from "effect";

import { StorageFailure } from "../git/Error.ts";

export interface ArtifactBody {
  readonly bytes: number;
  readonly etag: string;
  readonly contentType: string;
  /**
   * `public` only when the repository itself is anonymously readable.
   * Private artifacts stay `private` so a shared cache cannot leak them.
   */
  readonly cache: "public" | "private";
  readonly read: (
    range?: ByteRange,
  ) => Effect.Effect<Stream.Stream<Uint8Array, StorageFailure>, StorageFailure>;
}

export interface ByteRange {
  readonly offset: number;
  readonly length: number;
}

export type RangeRequest =
  | { readonly kind: "all" }
  | { readonly kind: "range"; readonly range: ByteRange }
  | { readonly kind: "unsatisfiable" }
  | { readonly kind: "notModified" };

const IMMUTABLE = "max-age=31536000, immutable";

/**
 * One RFC 7233 byte range, or unsatisfiable.
 *
 * Only `bytes=` is accepted, and only a single closed or suffix range —
 * multi-range is a different response shape this helper does not implement.
 */
export const parseRange = (header: string | null, size: number): RangeRequest => {
  if (header === null || header === "") return { kind: "all" };
  if (!header.startsWith("bytes=")) return { kind: "unsatisfiable" };
  const spec = header.slice("bytes=".length);
  if (spec.includes(",")) return { kind: "unsatisfiable" };

  const dash = spec.indexOf("-");
  if (dash === -1) return { kind: "unsatisfiable" };
  const startText = spec.slice(0, dash);
  const endText = spec.slice(dash + 1);

  if (startText === "") {
    const suffix = Number.parseInt(endText, 10);
    if (!Number.isInteger(suffix) || suffix <= 0) return { kind: "unsatisfiable" };
    const length = Math.min(suffix, size);
    return { kind: "range", range: { offset: size - length, length } };
  }

  const start = Number.parseInt(startText, 10);
  if (!Number.isInteger(start) || start < 0) return { kind: "unsatisfiable" };
  if (start >= size) return { kind: "unsatisfiable" };

  const end = endText === "" ? size - 1 : Number.parseInt(endText, 10);
  if (!Number.isInteger(end) || end < start) return { kind: "unsatisfiable" };
  const last = Math.min(end, size - 1);
  return { kind: "range", range: { offset: start, length: last - start + 1 } };
};

const quoted = (etag: string): string => (etag.startsWith('"') ? etag : `"${etag}"`);

const matches = (header: string | null, etag: string): boolean => {
  if (header === null || header === "") return false;
  const wanted = quoted(etag);
  return header.split(",").some((part) => {
    const tag = part.trim();
    return tag === "*" || tag === wanted || tag === `W/${wanted}`;
  });
};

/** Decide which bytes, if any, this request should receive. */
export const decide = (request: Request, bytes: number, etag: string): RangeRequest => {
  const tagged = quoted(etag);
  if (matches(request.headers.get("if-none-match"), etag)) {
    const range = request.headers.get("range");
    // If-None-Match wins on a GET without a range; a ranged GET with a
    // matching If-Range is still a range, and a matching If-None-Match on a
    // range is 304 the same as an unconditioned one.
    if (range === null || range === "") return { kind: "notModified" };
    if (matches(request.headers.get("if-range"), etag)) return parseRange(range, bytes);
    return { kind: "notModified" };
  }

  const range = request.headers.get("range");
  if (range === null || range === "") return { kind: "all" };

  const ifRange = request.headers.get("if-range");
  if (ifRange !== null && ifRange !== "" && !matches(ifRange, tagged) && ifRange !== tagged) {
    return { kind: "all" };
  }
  return parseRange(range, bytes);
};

const cacheControl = (cache: "public" | "private"): string => `${cache}, ${IMMUTABLE}`;

/**
 * GET/HEAD answer for an immutable artifact, or `null` when the method is
 * something else so a router can fall through.
 */
export const respond = Effect.fn("Artifact.respond")(function* (
  request: Request,
  artifact: ArtifactBody,
) {
  if (request.method !== "GET" && request.method !== "HEAD") return null;

  const etag = quoted(artifact.etag);
  const decision = decide(request, artifact.bytes, artifact.etag);
  const common = {
    etag,
    "accept-ranges": "bytes",
    "cache-control": cacheControl(artifact.cache),
    "content-type": artifact.contentType,
  };

  if (decision.kind === "notModified") {
    return new Response(null, { status: 304, headers: common });
  }
  if (decision.kind === "unsatisfiable") {
    return new Response(null, {
      status: 416,
      headers: {
        ...common,
        "content-range": `bytes */${artifact.bytes}`,
      },
    });
  }

  const range = decision.kind === "range" ? decision.range : undefined;
  const length = range === undefined ? artifact.bytes : range.length;
  const headers = new Headers({
    ...common,
    "content-length": String(length),
  });
  if (range !== undefined) {
    const last = range.offset + range.length - 1;
    headers.set("content-range", `bytes ${range.offset}-${last}/${artifact.bytes}`);
  }

  if (request.method === "HEAD") {
    return new Response(null, {
      status: range === undefined ? 200 : 206,
      headers,
    });
  }

  const body = yield* artifact.read(range);
  return new Response(Stream.toReadableStream(body), {
    status: range === undefined ? 200 : 206,
    headers,
  });
});
