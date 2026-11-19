/**
 * Smart-HTTP receive-pack on the wire, with no repository behind it.
 *
 * `Push.ts` decides *what* to push from a local history. This module is only
 * the conversation: the advertisement, one POST carrying ref commands and a
 * pack, and the report. `pushObjects` is the whole path for a caller that
 * already holds the objects it means to send — a bot writing one commit, a
 * service mirroring a generated tree — and has no `Repository` to walk.
 *
 * Raw `fetch` throughout, so it runs in a browser and on Workers as well as
 * Node; a `Transport` service, where one is provided, replaces it.
 */
import { Effect, Schema, Stream } from "effect";

import { Invalid, ObjectNotFound, PackCorrupt, type StorageFailure } from "../git/Error.ts";
import { concatBytes, hashObject } from "../git/Format.ts";
import { pack } from "../git/Pack.ts";
import { FLUSH, pkt, PktReader } from "../git/Pkt.ts";
import { checkRefName, isOid, ObjectStore, type Oid, type RawObject } from "../git/Store.ts";
import { type Authorize, fetchAuthorized, operationOf } from "./Authorize.ts";
import * as Transport from "./Transport.ts";
import { prepare as prepareUpload } from "./Upload.ts";

const decoder = new TextDecoder();

/** The old or new value git spells for "no ref". */
export const ZERO_OID = "0".repeat(40);

/** What one ref command came to, as the remote reported it or as decided here. */
export interface PushResult {
  readonly ref: string;
  readonly ok: boolean;
  readonly reason?: string;
}

/** receive-pack's advertisement, and where the conversation continues. */
export interface Advertisement {
  readonly refs: ReadonlyMap<string, Oid>;
  readonly capabilities: ReadonlySet<string>;
  /** The repository URL after the discovery request's redirects. */
  readonly url: string;
  /** The caller's token, kept only while the redirects stayed on its origin. */
  readonly token: string | undefined;
}

/** One `<old> <new> <ref>` line. `null` is the zero oid on either side. */
export interface Command {
  readonly ref: string;
  readonly old: Oid | null;
  /** The value to move the ref to, or `null` for a delete. */
  readonly next: Oid | null;
}

/**
 * A response body as the chunks it delivers. Every runtime this client targets
 * can iterate a `ReadableStream` natively, but the lib this project compiles
 * against does not say so — and Safari genuinely cannot — so the stream is
 * read through an explicit reader, which is true everywhere.
 */
const chunks = async function* (stream: ReadableStream<Uint8Array>): AsyncIterable<Uint8Array> {
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done === true) return;
      yield value;
    }
  } finally {
    reader.releaseLock();
  }
};

const unreachable = (reason: string) => new Invalid({ field: "remote", reason });

/**
 * A `PktReader` failure is `PackCorrupt` and stays that way — losing it inside
 * a generic "remote unreachable" would report a framing bug as a network one.
 */
const failure = (cause: unknown): Invalid | PackCorrupt =>
  Schema.is(PackCorrupt)(cause) || Schema.is(Invalid)(cause) ? cause : unreachable(String(cause));

/** `fetch` rejects credentials in URLs, so a token travels as a header. */
const authorization = (token: string | undefined): Record<string, string> =>
  token === undefined ? {} : { authorization: `Bearer ${token}` };

/** A text pkt-line, the conventional trailing newline stripped. */
const text = (payload: Uint8Array): string => {
  const decoded = decoder.decode(payload);
  return decoded.endsWith("\n") ? decoded.slice(0, -1) : decoded;
};

/**
 * Unlike upload-pack's advertisement this one carries no `HEAD` line, and the
 * capabilities sit after a NUL on the first ref line — or on the
 * `capabilities^{}` placeholder when the repository has no refs at all, which
 * is exactly the case a first push is.
 */
const readAdvertisement = async (
  body: ReadableStream<Uint8Array> | null,
): Promise<Pick<Advertisement, "refs" | "capabilities">> => {
  const refs = new Map<string, Oid>();
  const capabilities = new Set<string>();
  if (body === null) return { refs, capabilities };

  const reader = new PktReader(chunks(body));
  for (;;) {
    const item = await reader.next();
    if (item === "eof") break;
    if (item === "flush" || item === "delim" || item === "end") continue;

    const line = text(item);
    if (line.startsWith("# service=")) continue;

    const nul = line.indexOf("\0");
    const command = nul === -1 ? line : line.slice(0, nul);
    if (nul !== -1) {
      for (const capability of line.slice(nul + 1).split(" ")) {
        if (capability !== "") capabilities.add(capability);
      }
    }

    const oid = command.slice(0, 40);
    const name = command.slice(41);
    if (isOid(oid) && name.length > 0 && name !== "capabilities^{}") refs.set(name, oid);
  }

  return { refs, capabilities };
};

/** The report, demultiplexed when the request asked for side-band. */
const readReport = async (
  body: ReadableStream<Uint8Array> | null,
  sideband: boolean,
): Promise<ReadonlyArray<string>> => {
  if (body === null) return [];
  const lines: string[] = [];

  const outer = new PktReader(chunks(body));
  const banded: Uint8Array[] = [];

  for (;;) {
    const item = await outer.next();
    if (item === "eof" || item === "flush") break;
    if (item === "delim" || item === "end") continue;

    if (!sideband) {
      lines.push(text(item));
      continue;
    }

    const channel = item[0];
    // Band 3 is the remote's fatal error, and it is the only thing that will
    // arrive — treating it as progress would hang waiting for a report.
    if (channel === 3) throw unreachable(`remote: ${text(item.subarray(1))}`);
    if (channel === 1) banded.push(item.subarray(1));
  }

  if (!sideband) return lines;

  const bytes = concatBytes(banded);
  const inner = new PktReader(
    (async function* () {
      yield bytes;
    })(),
  );
  for (;;) {
    const item = await inner.next();
    if (item === "eof" || item === "flush") break;
    if (item === "delim" || item === "end") continue;
    lines.push(text(item));
  }
  return lines;
};

/**
 * Ask a remote what it holds before pushing to it.
 *
 * Git follows the initial redirect and uses its final repository URL for the
 * requests after it; so does this, because a streamed pack cannot be replayed
 * by `fetch`'s own redirect handling.
 */
export const discover = Effect.fn("Wire.discover")(function* (input: {
  readonly url: string;
  readonly token?: string;
  /** How to answer a `Hub-SSH-v1` challenge; absent, a 401 stays a 401. */
  readonly authorize?: Authorize;
}) {
  const transport = yield* Transport.current;
  const { authorize, token, url } = input;
  return yield* Effect.tryPromise({
    // The signal, passed through to `fetch`. Without it an interrupt abandons
    // the *effect* and leaves the socket open — so a caller that bounded how
    // long it would wait for a push still waited on the connection, and a
    // mirror that accepts and never answers held the process that pushed to it
    // for as long as the peer cared to.
    try: async (signal): Promise<Advertisement> => {
      const target = `${url}/info/refs?service=git-receive-pack`;
      const response = await fetchAuthorized(
        target,
        { headers: authorization(token), signal },
        { operation: operationOf("GET", target), commands: [] },
        authorize,
        undefined,
        transport,
      );
      if (!response.ok) throw new Error(`advertisement returned ${response.status}`);
      const advertised = await readAdvertisement(response.body);
      const resolved = new URL(response.url || target);
      let base = url;
      if (resolved.pathname.endsWith("/info/refs")) {
        resolved.pathname = resolved.pathname.slice(0, -"/info/refs".length);
        resolved.search = "";
        resolved.hash = "";
        base = resolved.href.replace(/\/$/, "");
      }
      return {
        ...advertised,
        url: base,
        // Preserve fetch's cross-origin rule: a redirect elsewhere drops it.
        token: new URL(base).origin === new URL(url).origin ? token : undefined,
      };
    },
    catch: failure,
  });
});

/**
 * Send ref commands and the pack they need, and read back what the remote
 * made of each.
 *
 * Every command gets a result. A command the report does not mention still
 * says why when the object phase is what went wrong — the `unpack` line.
 * A failure of the pack stream itself is the failure this fails with, not
 * the transport error it caused.
 */
export const receivePack = Effect.fn("Wire.receivePack")(function* <E>(input: {
  readonly advertisement: Advertisement;
  /** At least one; an empty command list is one the server rejects. */
  readonly commands: ReadonlyArray<Command>;
  /**
   * The pack, or `Stream.empty` when every command is a delete. The server
   * only reads the object phase when a command creates or moves a ref, so
   * trailing pack bytes there would be read as another pkt-line.
   */
  readonly pack: Stream.Stream<Uint8Array, E>;
  /** The local shallow boundary, when the history being pushed has one. */
  readonly shallow?: Iterable<Oid>;
  readonly atomic?: boolean;
  readonly authorize?: Authorize;
}) {
  const transport = yield* Transport.current;
  const { advertisement, authorize, commands } = input;

  const sideband = advertisement.capabilities.has("side-band-64k");
  const capabilities = [
    "report-status",
    ...(sideband ? ["side-band-64k"] : []),
    ...(input.atomic === true && advertisement.capabilities.has("atomic") ? ["atomic"] : []),
  ].join(" ");

  const prefix = [
    ...[...(input.shallow ?? [])].map((oid) => pkt(`shallow ${oid}\n`)),
    ...commands.map((command, index) => {
      const line = `${command.old ?? ZERO_OID} ${command.next ?? ZERO_OID} ${command.ref}`;
      // Capabilities follow a NUL on the first command — receive-pack's
      // spelling, where upload-pack uses a space after the first `want`.
      return pkt(index === 0 ? `${line}\0${capabilities}\n` : `${line}\n`);
    }),
    FLUSH,
  ];
  const upload = yield* prepareUpload(Stream.fromIterable(prefix).pipe(Stream.concat(input.pack)));
  const lifetime = yield* Effect.abortSignal;

  const reported = yield* Effect.tryPromise({
    // As discovery: interruptible only if the signal reaches the socket, and
    // this is the half that streams a pack to a peer.
    try: async (signal) => {
      const response = await fetchAuthorized(
        `${advertisement.url}/git-receive-pack`,
        {
          method: "POST",
          // Discovery already resolved the canonical URL. Disable fetch's
          // implicit replay: Node 24 otherwise retains the entire streamed
          // pack for redirects/authentication (nodejs/undici#4058). The
          // signed challenge retry recreates the body through upload.options.
          // Workers reject error mode; manual gives them the same refusal
          // through the status check below. Node needs error to avoid teeing.
          // Node is told apart by its user agent rather than `process`, which
          // is not a global this portable module may name.
          redirect:
            globalThis.navigator?.userAgent.startsWith("Node.js") === true ? "error" : "manual",
          window: null,
          headers: {
            "content-type": "application/x-git-receive-pack-request",
            ...authorization(advertisement.token),
          },
          signal: AbortSignal.any([signal, lifetime]),
        },
        {
          operation: "git-receive-pack",
          // The envelope binds the exact compare-and-swap each command asks
          // for: the policy boundary refuses a signed request that names
          // other refs or other revisions, which is the point of signing.
          commands: commands.map((command) => ({
            ref: command.ref,
            from: command.old,
            to: command.next,
          })),
        },
        authorize,
        upload.options,
        transport,
      );
      if (!response.ok) throw new Error(`receive-pack returned ${response.status}`);
      return readReport(response.body, sideband);
    },
    catch: failure,
  }).pipe(
    Effect.catch((error): Effect.Effect<never, Invalid | PackCorrupt | E | StorageFailure> => {
      const cause = upload.failure();
      return cause === undefined ? Effect.fail(error) : Effect.failCause(cause);
    }),
  );

  const unpack = reported.find((line) => line.startsWith("unpack "))?.slice(7) ?? "ok";
  const statuses = new Map<string, PushResult>();
  for (const line of reported) {
    if (line.startsWith("ok ")) statuses.set(line.slice(3), { ref: line.slice(3), ok: true });
    else if (line.startsWith("ng ")) {
      const rest = line.slice(3);
      const space = rest.indexOf(" ");
      const ref = space === -1 ? rest : rest.slice(0, space);
      statuses.set(ref, {
        ref,
        ok: false,
        reason: space === -1 ? "rejected" : rest.slice(space + 1),
      });
    }
  }

  return commands.map(
    (command): PushResult =>
      statuses.get(command.ref) ?? {
        ref: command.ref,
        ok: false,
        reason: unpack === "ok" ? "no status reported" : `unpack ${unpack}`,
      },
  );
}, Effect.scoped);

/** A read-only store over objects the caller already holds, keyed by their hash. */
const heldObjects = (objects: ReadonlyMap<Oid, RawObject>): ObjectStore["Service"] => {
  const read = (oid: Oid): Effect.Effect<RawObject, ObjectNotFound> => {
    const object = objects.get(oid);
    return object === undefined ? Effect.fail(new ObjectNotFound({ oid })) : Effect.succeed(object);
  };
  return ObjectStore.of({
    read,
    readStream: (oid) => read(oid).pipe(Effect.map((object) => Stream.make(object.data))),
    write: (object) => hashObject(object),
    has: (oid) => Effect.succeed(objects.has(oid)),
    delete: () => Effect.void,
    list: Stream.fromIterable(objects.keys()),
  });
};

/**
 * Point `ref` at `oid` on the remote, sending `objects` as the pack.
 *
 * `objects` is everything the remote lacks and nothing is walked: the caller
 * owns completeness, exactly as `git pack-objects` reading oids on stdin
 * does. Objects the remote already advertises are still sent, which is
 * wasteful but valid. `expected` is a lease: the push is refused here, before
 * any pack is built, unless the remote's current value matches it (`null`
 * for "must not exist"). Without one, the remote's value is replaced
 * whatever it is — this module holds no history to check fast-forward with.
 *
 * `ref` is the full name (`refs/heads/main`); a short name is refused rather
 * than guessed at. A refusal — by the lease here or by the remote — is a
 * result with `ok: false` and a `reason`, not a failure: check `ok`. The
 * effect fails only when the push could not be attempted or completed.
 */
export const pushObjects = Effect.fn("Wire.pushObjects")(function* (input: {
  readonly url: string;
  readonly token?: string;
  readonly ref: string;
  readonly oid: Oid;
  readonly objects: Iterable<RawObject>;
  readonly expected?: Oid | null;
  readonly authorize?: Authorize;
}) {
  const malformed = checkRefName(input.ref);
  if (malformed !== null) return yield* new Invalid({ field: "ref", reason: malformed });

  const advertisement = yield* discover(input);
  const advertised = advertisement.refs.get(input.ref) ?? null;

  if (input.expected !== undefined && input.expected !== advertised) {
    return { ref: input.ref, ok: false, reason: "stale remote value" } satisfies PushResult;
  }
  if (advertised === input.oid) {
    return { ref: input.ref, ok: true, reason: "up to date" } satisfies PushResult;
  }

  const held = new Map<Oid, RawObject>();
  for (const object of input.objects) held.set(yield* hashObject(object), object);
  if (!held.has(input.oid)) {
    return yield* new Invalid({ field: "objects", reason: `missing ${input.oid}` });
  }

  const [result] = yield* receivePack({
    advertisement,
    commands: [{ ref: input.ref, old: advertised, next: input.oid }],
    pack: pack([...held.keys()]).pipe(Stream.provideService(ObjectStore, heldObjects(held))),
    authorize: input.authorize,
  });
  return result ?? { ref: input.ref, ok: false, reason: "no status reported" };
});
