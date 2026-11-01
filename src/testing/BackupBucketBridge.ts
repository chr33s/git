/** Test-only streaming bridge. The native bucket outlives the coordinator subprocess. */
import assert from "node:assert/strict";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Schema } from "effect";
import type { R2Object } from "@cloudflare/workers-types";
import type { Destination } from "../../tools/copy-r2-backup.ts";
import { decodeBackupFrame, encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";

const Conditional = Schema.Struct({
  etagMatches: Schema.optional(Schema.String),
  etagDoesNotMatch: Schema.optional(Schema.String),
});
const Input = Schema.Struct({
  key: Schema.optional(Schema.String),
  options: Schema.Struct({
    limit: Schema.optional(Schema.Number),
    cursor: Schema.optional(Schema.String),
    onlyIf: Schema.optional(Conditional),
    httpMetadata: Schema.optional(
      Schema.Struct({
        contentType: Schema.optional(Schema.String),
        contentLanguage: Schema.optional(Schema.String),
        contentDisposition: Schema.optional(Schema.String),
        contentEncoding: Schema.optional(Schema.String),
        cacheControl: Schema.optional(Schema.String),
        cacheExpiry: Schema.optional(Schema.Date),
      }),
    ),
    customMetadata: Schema.optional(Schema.Record(Schema.String, Schema.String)),
    storageClass: Schema.optional(Schema.String),
    sha256: Schema.optional(Schema.instanceOf(ArrayBuffer)),
    md5: Schema.optional(Schema.instanceOf(ArrayBuffer)),
  }),
});
const metadata = (value: R2Object) => ({
  key: value.key,
  version: value.version,
  size: value.size,
  etag: value.etag,
  httpEtag: value.httpEtag,
  uploaded: value.uploaded,
  httpMetadata: value.httpMetadata,
  customMetadata: value.customMetadata,
  storageClass: value.storageClass,
  checksums: { md5: value.checksums.md5, sha256: value.checksums.sha256 },
});
export const bridgeBackupBucket = async (
  request: IncomingMessage,
  response: ServerResponse,
  bucket: Destination,
  knownLength: (
    body: ReadableStream<Uint8Array>,
    length: number,
  ) => Promise<Parameters<Destination["put"]>[1]>,
  stop: (key: string, point: "during-put" | "after-put") => boolean,
) => {
  const header = Schema.decodeUnknownSync(Schema.NonEmptyString)(request.headers["x-backup-input"]);
  const input = decodeBackupFrame(Buffer.from(header, "base64").toString("utf8"), Input);
  if (request.method === "POST") {
    const page = await bucket.list(input.options);
    response.end(encodeBackupFrame({ ...page, objects: page.objects.map(metadata) }));
    return;
  }
  assert.ok(input.key !== undefined);
  const key = input.key;
  if (request.method === "PUT") {
    let bytes = 0;
    const stream = Readable.from(
      (async function* () {
        for await (const chunk of request) {
          const value = Schema.decodeUnknownSync(Schema.instanceOf(Uint8Array))(chunk);
          bytes += value.byteLength;
          yield value;
          if (bytes > 0 && stop(key, "during-put")) return;
        }
      })(),
    );
    const length = Schema.decodeUnknownSync(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)))(
      Number(request.headers["content-length"]),
    );
    const body = await knownLength(Readable.toWeb(stream), length);
    const result = await bucket.put(key, body, input.options);
    if (stop(key, "after-put")) return;
    response.end(encodeBackupFrame(result === null ? null : metadata(result)));
    return;
  }
  assert.equal(request.method, "GET");
  const held = await bucket.get(key, input.options);
  response.setHeader(
    "x-backup-object",
    Buffer.from(encodeBackupFrame(held === null ? null : metadata(held))).toString("base64"),
  );
  if (held === null || !("body" in held)) {
    response.end();
    return;
  }
  const reader = held.body.getReader();
  try {
    await pipeline(
      Readable.from(
        (async function* () {
          while (true) {
            const chunk = await reader.read();
            if (chunk.done) return;
            yield chunk.value;
          }
        })(),
      ),
      response,
    );
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
};
