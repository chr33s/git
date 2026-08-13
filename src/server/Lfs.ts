/**
 * Git LFS: the batch API and the basic transfer.
 *
 *   POST …/info/lfs/objects/batch      what to upload or download, and where
 *   PUT  …/info/lfs/objects/:oid       the bytes
 *   GET  …/info/lfs/objects/:oid       the bytes back
 *
 * Large files are exactly what must not be buffered — an LFS object is large
 * by definition, and a Durable Object has 128 MiB — so the port is a stream in
 * both directions and every backend has to answer for it.
 *
 * LFS names objects by SHA-256, not by git's SHA-1, so this is a separate
 * keyspace from `ObjectStore` rather than a corner of it. Verifying that the
 * bytes hash to the name they were given is the backend's job, because the
 * streaming digest is the one primitive that differs per platform
 * (`node:crypto` on node, `crypto.DigestStream` on Workers).
 */
import { Context, Effect, Predicate, Schema, Stream } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";

/** LFS object ids are SHA-256, so 64 hex characters rather than git's 40. */
export const isLfsOid = (value: string): boolean => /^[0-9a-f]{64}$/.test(value);

export const MEDIA_TYPE = "application/vnd.git-lfs+json";

export interface LfsObject {
  readonly oid: string;
  readonly size: number;
}

export class LfsStore extends Context.Service<
  LfsStore,
  {
    /** `null` when absent — the batch API's whole job is answering this. */
    readonly head: (oid: string) => Effect.Effect<LfsObject | null, StorageFailure>;
    readonly read: (
      oid: string,
    ) => Effect.Effect<Stream.Stream<Uint8Array, StorageFailure>, ObjectNotFound | StorageFailure>;
    /**
     * Stores the bytes and returns what was actually written. Fails `Invalid`
     * when the content does not hash to `oid` — a corrupt LFS object that is
     * accepted silently is worse than a rejected upload.
     */
    readonly write: (
      oid: string,
      body: Stream.Stream<Uint8Array, StorageFailure>,
    ) => Effect.Effect<LfsObject, StorageFailure | Invalid>;
  }
>()("server/LfsStore") {}

/** The one thing a client is told to do next with an object, and for how long. */
interface BatchAction {
  readonly href: string;
  readonly expires_in: number;
}

/** The batch endpoint's verdict on one requested object. */
interface BatchVerdict {
  readonly oid: string;
  readonly size: number;
  readonly actions?: {
    readonly download?: BatchAction;
    readonly upload?: BatchAction;
  };
  readonly error?: { readonly code: number; readonly message: string };
}

/** Every body this module puts on the wire. */
type ResponseBody =
  | { readonly message: string }
  | { readonly transfer: "basic"; readonly objects: ReadonlyArray<BatchVerdict> };

const json = (value: ResponseBody, status = 200): Response =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": MEDIA_TYPE, "cache-control": "no-cache" },
  });

const failure = (status: number, message: string): Response => json({ message }, status);

/**
 * What a client may claim in a batch request. Everything is optional on the
 * wire, and `oid` and `size` stay unchecked here because each object is
 * judged individually — one bad entry gets a per-object error, not a 400.
 */
const BatchRequest = Schema.Struct({
  operation: Schema.optional(Schema.String),
  transfers: Schema.optional(Schema.Array(Schema.String)),
  objects: Schema.optional(
    Schema.Array(
      Schema.Struct({
        oid: Schema.optional(Schema.Unknown),
        size: Schema.optional(Schema.Unknown),
      }),
    ),
  ),
});

/**
 * The href a client should use for one object.
 *
 * Derived from the request rather than configured: the server may be behind
 * any hostname, and the one URL known to work is the one that just arrived.
 */
const hrefFor = (request: Request, oid: string): string => {
  const url = new URL(request.url);
  url.pathname = url.pathname.replace(/\/batch$/, `/${oid}`);
  url.search = "";
  return url.toString();
};

const batch = (request: Request): Effect.Effect<Response, never, LfsStore> =>
  Effect.gen(function* () {
    const store = yield* LfsStore;

    const parsed = yield* Effect.tryPromise(() => request.json()).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(BatchRequest)),
      Effect.orElseSucceed(() => null),
    );
    if (parsed === null) return failure(400, "malformed batch request");

    const operation = parsed.operation;
    if (operation !== "download" && operation !== "upload") {
      return failure(422, `unsupported operation '${String(operation)}'`);
    }

    // `basic` is the only transfer this server implements, and saying so is
    // how a client knows not to try for a fancier one.
    const transfers = parsed.transfers ?? ["basic"];
    if (!transfers.includes("basic")) {
      return failure(422, "only the 'basic' transfer is supported");
    }

    const objects = yield* Effect.forEach(parsed.objects ?? [], (requested) =>
      Effect.gen(function* () {
        const oid = Predicate.isString(requested.oid) ? requested.oid : "";
        const size = Predicate.isNumber(requested.size) ? requested.size : -1;

        if (!isLfsOid(oid) || size < 0) {
          return {
            oid,
            size: Math.max(size, 0),
            error: { code: 422, message: "invalid oid or size" },
          };
        }

        const existing = yield* store.head(oid).pipe(Effect.orElseSucceed(() => null));
        const href = hrefFor(request, oid);

        if (operation === "download") {
          if (existing === null) {
            return { oid, size, error: { code: 404, message: "object not found" } };
          }
          return {
            oid,
            size: existing.size,
            actions: { download: { href, expires_in: 3600 } },
          };
        }

        // Upload: an object already held needs no action at all, which is
        // what makes a re-push of an existing file free.
        if (existing !== null) return { oid, size: existing.size };
        return { oid, size, actions: { upload: { href, expires_in: 3600 } } };
      }),
    );

    return json({ transfer: "basic", objects });
  });

const download = (oid: string): Effect.Effect<Response, never, LfsStore> =>
  Effect.gen(function* () {
    const store = yield* LfsStore;
    if (!isLfsOid(oid)) return failure(422, "invalid oid");

    const found = yield* store.head(oid).pipe(Effect.orElseSucceed(() => null));
    if (found === null) return failure(404, "object not found");

    return yield* store.read(oid).pipe(
      Effect.map(
        (stream) =>
          new Response(Stream.toReadableStream(stream), {
            headers: {
              "content-type": "application/octet-stream",
              "content-length": String(found.size),
            },
          }),
      ),
      Effect.orElseSucceed(() => failure(404, "object not found")),
    );
  });

const upload = (request: Request, oid: string): Effect.Effect<Response, never, LfsStore> =>
  Effect.gen(function* () {
    const store = yield* LfsStore;
    if (!isLfsOid(oid)) return failure(422, "invalid oid");

    const body = request.body;
    const bytes: Stream.Stream<Uint8Array, StorageFailure> =
      body === null
        ? Stream.empty
        : Stream.fromReadableStream({
            evaluate: () => body,
            onError: (cause) => new StorageFailure({ operation: "lfs.upload", path: oid, cause }),
          });

    return yield* store.write(oid, bytes).pipe(
      Effect.map(() => new Response(null, { status: 200 })),
      Effect.catchTags({
        Invalid: (error) => Effect.succeed(failure(422, error.reason)),
        StorageFailure: () => Effect.succeed(failure(500, "could not store object")),
      }),
    );
  });

/**
 * Route an LFS request whose repository the caller has already resolved.
 * `null` means "not an LFS request", so a host can try the next handler.
 */
export const handle = (request: Request): Effect.Effect<Response | null, never, LfsStore> =>
  Effect.suspend(() => {
    const segments = new URL(request.url).pathname.split("/").filter((part) => part !== "");
    // …/info/lfs/objects/<batch|oid>
    const objectsAt = segments.findIndex(
      (part, index) =>
        part === "objects" && segments[index - 1] === "lfs" && segments[index - 2] === "info",
    );
    if (objectsAt === -1) return Effect.succeed(null);

    const last = segments[objectsAt + 1];
    if (last === undefined) return Effect.succeed(null);

    if (last === "batch") {
      return request.method === "POST"
        ? batch(request)
        : Effect.succeed(failure(405, "batch requires POST"));
    }
    if (request.method === "GET") return download(last);
    if (request.method === "PUT") return upload(request, last);
    return Effect.succeed(failure(405, `unsupported method ${request.method}`));
  });

/** The pointer file git-lfs writes in place of the content. */
export const parsePointer = (
  content: string,
): { readonly oid: string; readonly size: number } | null => {
  const lines = content.split("\n");
  if (lines[0] !== "version https://git-lfs.github.com/spec/v1") return null;

  const oid = lines.find((line) => line.startsWith("oid sha256:"))?.slice(11);
  const size = Number(lines.find((line) => line.startsWith("size "))?.slice(5));
  if (oid === undefined || !isLfsOid(oid) || !Number.isInteger(size) || size < 0) return null;
  return { oid, size };
};

export const formatPointer = (object: LfsObject): string =>
  `version https://git-lfs.github.com/spec/v1\noid sha256:${object.oid}\nsize ${object.size}\n`;

/** Bytes in memory: what a test wants, and what nothing else should use. */
export const memory = Effect.sync(() => {
  const objects = new Map<string, Uint8Array>();
  return LfsStore.of({
    head: (oid) =>
      Effect.sync(() => {
        const found = objects.get(oid);
        return found === undefined ? null : { oid, size: found.length };
      }),
    read: (oid) =>
      Effect.suspend(() => {
        const found = objects.get(oid);
        return found === undefined
          ? Effect.fail(new ObjectNotFound({ oid }))
          : Effect.succeed(Stream.make(found));
      }),
    write: (oid, body) =>
      Effect.gen(function* () {
        const chunks = yield* Stream.runCollect(body);
        const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
        const bytes = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.length;
        }

        const digest = yield* Effect.promise(() =>
          crypto.subtle.digest("SHA-256", bytes.slice().buffer),
        );
        const actual = [...new Uint8Array(digest)]
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("");
        if (actual !== oid) {
          return yield* new Invalid({ field: "oid", reason: `content hashes to ${actual}` });
        }

        objects.set(oid, bytes);
        return { oid, size: bytes.length };
      }),
  });
});
