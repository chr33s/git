/**
 * The client half of smart HTTP — enough to push.
 *
 * The mirror of `Fetch.ts`: an advertisement, then one POST carrying ref
 * commands and a pack, then a report. Same transport (raw `fetch`, so it runs
 * in a browser), same `PktReader`, and the pack is built by the same
 * `Repository` walk the server uses to answer a fetch — `wants` are the new
 * ref values, `haves` are what the remote already advertises.
 *
 * The one decision made here rather than on the server is the
 * fast-forward check. A client holds the history; it can see that the remote's
 * current commit is not an ancestor of what would replace it, and say so
 * before spending a pack upload on a push the operator did not mean.
 */
import { Effect, Stream } from "effect";

import { Invalid, type ObjectNotFound, PackCorrupt, type StorageFailure } from "../git/Error.ts";
import { FLUSH, pkt, PktReader } from "../git/Pkt.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";

const decoder = new TextDecoder();

const ZERO_OID = "0".repeat(40);

export interface PushRef {
  readonly local: string;
  readonly remote: string;
  /** Delete the remote ref. */
  readonly delete?: boolean;
}

export interface PushResult {
  readonly ref: string;
  readonly ok: boolean;
  readonly reason?: string;
}

const concat = (parts: ReadonlyArray<Uint8Array>): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out as Uint8Array<ArrayBuffer>;
};

const unreachable = (reason: string) => new Invalid({ field: "remote", reason });

/**
 * A `PktReader` failure is `PackCorrupt` and stays that way — losing it inside
 * a generic "remote unreachable" would report a framing bug as a network one.
 */
const failure = (cause: unknown): Invalid | PackCorrupt =>
  cause instanceof PackCorrupt || cause instanceof Invalid ? cause : unreachable(String(cause));

/** `fetch` rejects credentials in URLs, so a token travels as a header. */
const authorization = (token: string | undefined): Record<string, string> =>
  token === undefined ? {} : { authorization: `Bearer ${token}` };

/** A text pkt-line, the conventional trailing newline stripped. */
const text = (payload: Uint8Array): string => {
  const decoded = decoder.decode(payload);
  return decoded.endsWith("\n") ? decoded.slice(0, -1) : decoded;
};

interface Advertisement {
  readonly refs: ReadonlyMap<string, Oid>;
  readonly capabilities: ReadonlySet<string>;
}

/**
 * receive-pack's advertisement. Unlike upload-pack's it carries no `HEAD`
 * line, and the capabilities sit after a NUL on the first ref line — or on the
 * `capabilities^{}` placeholder when the repository has no refs at all, which
 * is exactly the case a first push is.
 */
const readAdvertisement = async (
  body: ReadableStream<Uint8Array> | null,
): Promise<Advertisement> => {
  const refs = new Map<string, Oid>();
  const capabilities = new Set<string>();
  if (body === null) return { refs, capabilities };

  const reader = new PktReader(body as unknown as AsyncIterable<Uint8Array>);
  for (;;) {
    const item = await reader.next();
    if (item === "eof") break;
    if (typeof item === "string") continue;

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

/** One `<old> <new> <ref>` line, and which requested ref it answers for. */
interface Command {
  readonly index: number;
  readonly ref: string;
  readonly old: string;
  readonly next: string;
}

/** The report, demultiplexed when the request asked for side-band. */
const readReport = async (
  body: ReadableStream<Uint8Array> | null,
  sideband: boolean,
): Promise<ReadonlyArray<string>> => {
  if (body === null) return [];
  const lines: string[] = [];

  const outer = new PktReader(body as unknown as AsyncIterable<Uint8Array>);
  const banded: Uint8Array[] = [];

  for (;;) {
    const item = await outer.next();
    if (item === "eof" || item === "flush") break;
    if (typeof item === "string") continue;

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

  const bytes = concat(banded);
  const inner = new PktReader(
    (async function* () {
      yield bytes;
    })(),
  );
  for (;;) {
    const item = await inner.next();
    if (item === "eof" || item === "flush") break;
    if (typeof item === "string") continue;
    lines.push(text(item));
  }
  return lines;
};

/**
 * Push local refs to a remote over smart HTTP.
 *
 * Every requested ref gets a result, whether it reached the server or was
 * refused here; a rejection is a value, not a failure, because a push of five
 * branches where one lost a race is four successes.
 */
export const push = Effect.fn("Client.push")(function* (input: {
  readonly url: string;
  readonly refs: ReadonlyArray<PushRef>;
  readonly token?: string;
  readonly force?: boolean;
  readonly atomic?: boolean;
}) {
  const repository = yield* Repository;
  const { atomic, force, refs, token, url } = input;

  const advertisement = yield* Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${url}/info/refs?service=git-receive-pack`, {
        headers: authorization(token),
      });
      if (!response.ok) throw new Error(`advertisement returned ${response.status}`);
      return readAdvertisement(response.body);
    },
    catch: failure,
  });

  const outcomes: Array<PushResult | null> = refs.map(() => null);
  const commands: Command[] = [];

  for (const [index, request] of refs.entries()) {
    const name = request.remote;
    const old = advertisement.refs.get(name) ?? ZERO_OID;

    if (request.delete === true) {
      // Nothing to delete is a failed command, not a silent success: the
      // caller named a ref the remote does not have.
      if (old === ZERO_OID) {
        outcomes[index] = { ref: name, ok: false, reason: "remote ref does not exist" };
        continue;
      }
      commands.push({ index, ref: name, old, next: ZERO_OID });
      continue;
    }

    const next = isOid(request.local) ? request.local : yield* repository.resolve(request.local);
    if (next === null) {
      return yield* new Invalid({ field: "local", reason: `unknown ref '${request.local}'` });
    }

    if (old === next) {
      outcomes[index] = { ref: name, ok: true, reason: "up to date" };
      continue;
    }

    /**
     * A remote value this history cannot reach — a diverged branch, or a
     * commit this clone never fetched — would be lost by the update. `force`
     * is the caller saying that is the intent.
     */
    if (old !== ZERO_OID && force !== true) {
      const fastForward = yield* repository.isAncestor(old as Oid, next);
      if (!fastForward) {
        outcomes[index] = { ref: name, ok: false, reason: "non-fast-forward" };
        continue;
      }
    }

    commands.push({ index, ref: name, old, next });
  }

  const settle = (): ReadonlyArray<PushResult> =>
    outcomes.map(
      (outcome, index) =>
        outcome ?? { ref: refs[index]?.remote ?? "", ok: false, reason: "no status reported" },
    );

  // Every requested ref was already where it belongs, or refused here. There
  // is no request to make, and an empty command list is one the server rejects.
  if (commands.length === 0) return settle();

  const wants = [
    ...new Set(
      commands.filter((command) => command.next !== ZERO_OID).map((command) => command.next as Oid),
    ),
  ];

  /**
   * A delete-only push sends no pack at all — not an empty one. The server
   * only reads the object phase when a command creates or moves a ref, so
   * trailing pack bytes there would be read as another pkt-line.
   */
  const packBytes =
    wants.length === 0
      ? new Uint8Array(0)
      : yield* Effect.gen(function* () {
          const plan = yield* repository.fetch({
            wants,
            haves: [...new Set(advertisement.refs.values())],
          });
          return concat(yield* Stream.runCollect(repository.packOids(plan.oids)));
        });

  const sideband = advertisement.capabilities.has("side-band-64k");
  const capabilities = [
    "report-status",
    ...(sideband ? ["side-band-64k"] : []),
    ...(atomic === true && advertisement.capabilities.has("atomic") ? ["atomic"] : []),
  ].join(" ");

  const body = concat([
    ...commands.map((command, index) => {
      const line = `${command.old} ${command.next} ${command.ref}`;
      // Capabilities follow a NUL on the first command — receive-pack's
      // spelling, where upload-pack uses a space after the first `want`.
      return pkt(index === 0 ? `${line}\0${capabilities}\n` : `${line}\n`);
    }),
    FLUSH,
    packBytes,
  ]);

  const reported = yield* Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${url}/git-receive-pack`, {
        method: "POST",
        headers: {
          "content-type": "application/x-git-receive-pack-request",
          ...authorization(token),
        },
        body,
      });
      if (!response.ok) throw new Error(`receive-pack returned ${response.status}`);
      return readReport(response.body, sideband);
    },
    catch: failure,
  });

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

  for (const command of commands) {
    outcomes[command.index] = statuses.get(command.ref) ?? {
      ref: command.ref,
      ok: false,
      // A report with no line for a command still says why, on the `unpack`
      // line, whenever the object phase is what went wrong.
      reason: unpack === "ok" ? "no status reported" : `unpack ${unpack}`,
    };
  }

  return settle();
}) satisfies (input: {
  readonly url: string;
  readonly refs: ReadonlyArray<PushRef>;
  readonly token?: string;
  readonly force?: boolean;
  readonly atomic?: boolean;
}) => Effect.Effect<
  ReadonlyArray<PushResult>,
  Invalid | PackCorrupt | ObjectNotFound | StorageFailure,
  Repository
>;
