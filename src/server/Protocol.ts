/**
 * Git smart-HTTP endpoints — phase 4's protocol half.
 *
 * Web `Request` in, web `Response` out, `Repository` the only requirement —
 * no store, no platform type reaches a handler, so the same functions serve
 * from a Durable Object and from `node:http`. Protocol v0, stateless-rpc, as
 * the smart-HTTP transport defines it:
 *
 *   GET  …/info/refs?service=git-{upload,receive}-pack   advertisement
 *   POST …/git-upload-pack                               wants/haves -> pack
 *   POST …/git-receive-pack                              commands + pack -> report
 *
 * Deliberately not advertised: `multi_ack` (a stateless round-trip either
 * concludes with `done` or restarts, so the client sends everything it has
 * and the worst case is a larger pack, never a wrong one), `side-band-64k`
 * (progress chatter), and `shallow` (rejected explicitly).
 */
import { createGunzip } from "node:zlib";

import { Effect, Stream } from "effect";

import { type GitError, Invalid, PackCorrupt } from "../git/Error.ts";
import { FLUSH, pkt, PktReader } from "../git/Pkt.ts";
import { type ReceiveResult, Repository } from "../git/Repository.ts";
import { isOid, type Oid, type RefUpdate } from "../git/Store.ts";

const decoder = new TextDecoder();

const ZERO_OID = "0".repeat(40);
const AGENT = "agent=chr33s-git/0";

const concat = (parts: ReadonlyArray<Uint8Array>): Uint8Array => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

const corrupt = (reason: string) => new PackCorrupt({ reason });

/** `Response` wants an `ArrayBuffer`-backed view; everything built here is. */
const asBody = (bytes: Uint8Array): Uint8Array<ArrayBuffer> => bytes as Uint8Array<ArrayBuffer>;

/** A text pkt-line, the conventional trailing newline stripped. */
const text = (payload: Uint8Array): string => {
  const decoded = decoder.decode(payload);
  return decoded.endsWith("\n") ? decoded.slice(0, -1) : decoded;
};

/**
 * The request body as chunks. git compresses large negotiation bodies, and
 * announces it the standard way; a pack body is already deflated per object,
 * so pushes arrive identity-encoded.
 */
const body = (request: Request): AsyncIterable<Uint8Array> => {
  const stream = request.body;
  if (stream === null) return (async function* () {})();
  const raw = stream as unknown as AsyncIterable<Uint8Array>;
  if (request.headers.get("content-encoding")?.includes("gzip") !== true) return raw;

  return (async function* () {
    const gunzip = createGunzip();
    const chunks: Uint8Array[] = [];
    let failed: unknown = null;
    gunzip.on("data", (chunk: Uint8Array) => chunks.push(chunk));
    gunzip.on("error", (error) => {
      failed = error;
    });
    for await (const chunk of raw) {
      if (!gunzip.write(chunk)) await new Promise((drained) => gunzip.once("drain", drained));
      if (failed !== null) throw failed;
      yield* chunks.splice(0);
    }
    await new Promise<void>((resolve, reject) => {
      gunzip.end(() => resolve());
      gunzip.on("error", reject);
    });
    if (failed !== null) throw failed;
    yield* chunks.splice(0);
  })();
};

const headers = (type: string) => ({
  "cache-control": "no-cache",
  "content-type": type,
});

/** `GET /info/refs?service=…` — refs, capabilities on the first line. */
export const advertise = (
  service: "git-upload-pack" | "git-receive-pack",
): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const refs = yield* repository.refs;
    const head = yield* repository.head;

    const caps =
      service === "git-upload-pack"
        ? `symref=HEAD:${head} ${AGENT}`
        : `report-status delete-refs atomic ${AGENT}`;

    const lines: string[] = [];
    if (service === "git-upload-pack") {
      const target = refs.find(([name]) => name === head);
      if (target !== undefined) lines.push(`${target[1]} HEAD`);
    }
    for (const [name, oid] of refs) lines.push(`${oid} ${name}`);

    const parts: Uint8Array[] = [pkt(`# service=${service}\n`), FLUSH];
    if (lines.length === 0) {
      // An empty repository still advertises capabilities, on a line whose
      // ref name is the placeholder git defined for exactly this case.
      parts.push(pkt(`${ZERO_OID} capabilities^{}\0${caps}\n`));
    } else {
      lines.forEach((line, index) => {
        parts.push(pkt(index === 0 ? `${line}\0${caps}\n` : `${line}\n`));
      });
    }
    parts.push(FLUSH);

    return new Response(asBody(concat(parts)), {
      headers: headers(`application/x-${service}-advertisement`),
    });
  });

const step = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) =>
      cause instanceof PackCorrupt || cause instanceof Invalid
        ? cause
        : corrupt(cause instanceof Error ? cause.message : JSON.stringify(cause)),
  });

/** `POST /git-upload-pack` — wants and haves in, ACK/NAK and a pack out. */
export const uploadPack = (request: Request): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const reader = new PktReader(body(request));

    const wants: Oid[] = [];
    const haves: Oid[] = [];
    let done = false;

    yield* step(async () => {
      for (;;) {
        const item = await reader.next();
        if (item === "eof") return;
        if (item === "flush") continue;
        const line = text(item);
        const [keyword] = line.split(" ", 1);
        const oid = line.slice((keyword?.length ?? 0) + 1, (keyword?.length ?? 0) + 41);
        if (keyword === "want" && isOid(oid)) wants.push(oid);
        else if (keyword === "have" && isOid(oid)) haves.push(oid);
        else if (line === "done") {
          done = true;
          return;
        } else if (keyword === "deepen" || keyword === "shallow") {
          throw new Invalid({ field: "depth", reason: "shallow fetch is not supported" });
        } else {
          throw new Invalid({ field: "upload-pack", reason: `unexpected line '${line}'` });
        }
      }
    });

    if (wants.length === 0) {
      return yield* new Invalid({ field: "upload-pack", reason: "no 'want' lines" });
    }

    // Protocol v0 without multi_ack: acknowledge the first `have` this
    // repository possesses, or NAK.
    let common: Oid | null = null;
    for (const have of haves) {
      if (common === null && (yield* repository.contains(have))) common = have;
    }

    if (!done) {
      // A pure negotiation round; with nothing more to add, NAK tells the
      // stateless client to come back with `done`.
      return new Response(asBody(pkt("NAK\n")), {
        headers: headers("application/x-git-upload-pack-result"),
      });
    }

    const prelude = common === null ? pkt("NAK\n") : pkt(`ACK ${common}\n`);
    const packStream = Stream.concat(
      Stream.fromIterable([prelude]),
      repository.packOf(wants, haves),
    );

    return new Response(Stream.toReadableStream(packStream), {
      headers: headers("application/x-git-upload-pack-result"),
    });
  });

const report = (results: ReadonlyArray<ReceiveResult>, unpacked: string): Uint8Array =>
  concat([
    pkt(`unpack ${unpacked}\n`),
    ...results.map((result) =>
      pkt(result.ok ? `ok ${result.ref}\n` : `ng ${result.ref} ${result.reason ?? "failed"}\n`),
    ),
    FLUSH,
  ]);

/** `POST /git-receive-pack` — ref commands and a pack in, report-status out. */
export const receivePack = (request: Request): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const reader = new PktReader(body(request));

    const updates: RefUpdate[] = [];
    let atomic = false;
    let first = true;

    yield* step(async () => {
      for (;;) {
        const item = await reader.next();
        if (item === "eof" || item === "flush") return;
        const line = text(item);

        let command = line;
        if (first) {
          const nul = line.indexOf("\0");
          if (nul !== -1) {
            command = line.slice(0, nul);
            atomic = line
              .slice(nul + 1)
              .split(" ")
              .includes("atomic");
          }
          first = false;
        }

        const old = command.slice(0, 40);
        const next = command.slice(41, 81);
        const name = command.slice(82);
        const validOld = old === ZERO_OID || isOid(old);
        const validNext = next === ZERO_OID || isOid(next);
        if (!validOld || !validNext || name.length === 0) {
          throw new Invalid({ field: "receive-pack", reason: `malformed command '${command}'` });
        }

        updates.push({
          name,
          value: next === ZERO_OID ? null : (next as Oid),
          expected: old === ZERO_OID ? null : (old as Oid),
          reason: "push",
        });
      }
    });

    if (updates.length === 0) {
      return yield* new Invalid({ field: "receive-pack", reason: "no commands" });
    }

    const respond = (results: ReadonlyArray<ReceiveResult>, unpacked: string) =>
      new Response(asBody(report(results, unpacked)), {
        headers: headers("application/x-git-receive-pack-result"),
      });
    const allFailed = (reason: string): ReadonlyArray<ReceiveResult> =>
      updates.map((update) => ({
        ref: update.name,
        from: update.expected ?? null,
        to: null,
        ok: false,
        reason,
      }));

    // The object phase. A pack arrives whenever any command creates or moves
    // a ref; a delete-only push sends none.
    if (updates.some((update) => update.value !== null)) {
      const unpacked = yield* repository
        .unpack(Stream.fromAsyncIterable(reader.rest(), (cause) => corrupt(String(cause))))
        .pipe(
          Effect.map(() => null),
          Effect.catch((error) => Effect.succeed(error)),
        );
      if (unpacked !== null) {
        const reason = unpacked instanceof PackCorrupt ? unpacked.reason : unpacked._tag;
        return respond(allFailed("unpacker error"), reason);
      }

      // No full connectivity check, but never point a ref at an object this
      // repository does not hold.
      for (const update of updates) {
        if (update.value !== null && !(yield* repository.contains(update.value))) {
          return respond(allFailed("missing necessary objects"), "ok");
        }
      }
    }

    const results = yield* repository.receive(updates, { atomic }).pipe(
      Effect.catchTag("HookRejected", (error) => Effect.succeed(allFailed(error.message))),
      Effect.catchTag("Invalid", (error) => Effect.succeed(allFailed(error.reason))),
    );

    return respond(results, "ok");
  });

/**
 * Route a request whose repository is already resolved — the caller scoped
 * the `Repository` layer, so the path prefix in front of these suffixes is
 * its business. `null` means "not a protocol request".
 */
export const handle = (request: Request): Effect.Effect<Response | null, GitError, Repository> =>
  Effect.suspend(() => {
    const url = new URL(request.url);
    const segments = url.pathname.split("/").filter((segment) => segment !== "");
    const last = segments.at(-1);

    if (request.method === "GET" && last === "refs" && segments.at(-2) === "info") {
      const service = url.searchParams.get("service");
      if (service === "git-upload-pack" || service === "git-receive-pack") {
        return advertise(service);
      }
      // The dumb protocol is not served here.
      return Effect.succeed(null);
    }
    if (request.method === "POST" && last === "git-upload-pack") return uploadPack(request);
    if (request.method === "POST" && last === "git-receive-pack") return receivePack(request);
    return Effect.succeed(null);
  });
