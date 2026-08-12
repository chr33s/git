/**
 * The client half of smart HTTP — enough to clone, and to fetch.
 *
 * Advertisement, then the `have`/`ACK` negotiation, then a final `done` round
 * whose pack streams into the target stores through the same `Pack.unpack` the
 * server uses. Serves the Artifacts provider's `import` and the CLI's `clone`
 * from one implementation — a clone is this with nothing to offer.
 *
 * Platform-neutral end to end: `fetch`, web streams, and `Pack.ts`'s own
 * pull-based inflate (`git/Inflate.ts`) — `Browser.test.ts` executes a full
 * clone inside real Chromium to hold the claim.
 */
import { Effect, Layer, Stream } from "effect";

import {
  Invalid,
  type ObjectNotFound,
  type PackCorrupt,
  type StorageFailure,
} from "../git/Error.ts";
import * as Pack from "../git/Pack.ts";
import { noPacks } from "../git/Packed.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type ObjectStore, type Oid, type RefStore, type RefUpdate } from "../git/Store.ts";
import { ObjectStore as ObjectStoreTag, RefStore as RefStoreTag } from "../git/Store.ts";
import { PktReader } from "../git/Pkt.ts";

const decoder = new TextDecoder();
const encoder = new TextEncoder();

export interface RemoteRef {
  readonly oid: Oid;
  readonly name: string;
}

/** The two ports a fetch writes through; the caller owns their lifetime. */
export interface FetchStores {
  readonly objects: ObjectStore["Service"];
  readonly refs: RefStore["Service"];
}

const unreachable = (reason: string) => new Invalid({ field: "remote", reason });

const advertisedRefs = async (body: ReadableStream<Uint8Array> | null) => {
  if (body === null) return [];
  const reader = new PktReader(body as unknown as AsyncIterable<Uint8Array>);
  const refs: RemoteRef[] = [];
  for (;;) {
    const item = await reader.next();
    if (item === "eof") break;
    // `delim`/`end` belong to protocol v2; this is a v0 advertisement, so
    // they are nothing to act on either way.
    if (item === "flush" || item === "delim" || item === "end") continue;
    const line = decoder.decode(item).replace(/\n$/, "");
    if (line.startsWith("# service=")) continue;
    const oid = line.slice(0, 40);
    const name = line.slice(41).split("\0")[0] ?? "";
    if (isOid(oid) && name.length > 0 && name !== "capabilities^{}") refs.push({ oid, name });
  }
  return refs;
};

/** `fetch` rejects credentials in URLs, so a token travels as a header. */
const authorization = (token: string | undefined): Record<string, string> =>
  token === undefined ? {} : { authorization: `Bearer ${token}` };

/** The refs a remote advertises for fetching, `HEAD` included. */
export const lsRemote = (
  url: string,
  options?: { readonly token?: string | undefined },
): Effect.Effect<ReadonlyArray<RemoteRef>, Invalid> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${url}/info/refs?service=git-upload-pack`, {
        headers: authorization(options?.token),
      });
      if (!response.ok) throw new Error(`advertisement returned ${response.status}`);
      return advertisedRefs(response.body);
    },
    catch: (cause) => unreachable(String(cause)),
  });

export interface FetchResult {
  readonly refs: ReadonlyArray<RefUpdate>;
  /** The branch the remote's `HEAD` points at, when it can be named. */
  readonly defaultBranch: string | undefined;
}

/**
 * How many `have` lines a fetch offers before it stops looking for a better
 * base, and how many of them go in one round. Both are git's own defaults.
 *
 * The cap is what keeps a client with a decade of history from spending the
 * fetch listing it: every round of a stateless conversation repeats every
 * have sent so far, so the bytes grow with the square of the count. Hitting
 * the cap costs pack size and nothing else — the server sends everything the
 * haves it did see fail to cover, so a truncated offer is a larger pack, never
 * a wrong one.
 */
const MAX_HAVES = 256;
const HAVES_PER_ROUND = 32;

/**
 * Capabilities ride on the first `want`, space-separated after the oid.
 *
 * Empty, and deliberately: `server/Protocol.ts` advertises neither `multi_ack`
 * nor `multi_ack_detailed` (its own comment says why — a stateless round trip
 * concludes with `done` or restarts), so the negotiation below is the baseline
 * single-ACK one every upload-pack speaks. `side-band-64k` is advertised but
 * not asked for either: the pack is read straight off the response body, and
 * requesting the capability would only add a demultiplexing layer. Asking for
 * a capability the remote did not agree to is how a client ends up parsing a
 * format the server never sent.
 */
const CAPABILITIES: ReadonlyArray<string> = [];

const pktLine = (line: string) => `${(line.length + 4).toString(16).padStart(4, "0")}${line}`;

/**
 * One request body: the wants, then the haves offered so far.
 *
 * The whole prefix repeats every round because smart HTTP is stateless-rpc —
 * the server that answers the second round has no memory of the first.
 */
const negotiation = (
  wants: ReadonlyArray<Oid>,
  haves: ReadonlyArray<Oid>,
  done: boolean,
): Uint8Array<ArrayBuffer> =>
  encoder.encode(
    [
      ...wants.map((oid, index) =>
        pktLine(
          `want ${oid}${index === 0 ? CAPABILITIES.map((name) => ` ${name}`).join("") : ""}\n`,
        ),
      ),
      "0000",
      ...haves.map((oid) => pktLine(`have ${oid}\n`)),
      done ? pktLine("done\n") : "0000",
    ].join(""),
  );

const uploadPack = async (
  url: string,
  token: string | undefined,
  body: Uint8Array<ArrayBuffer>,
): Promise<Response> => {
  const response = await fetch(`${url}/git-upload-pack`, {
    method: "POST",
    headers: {
      "content-type": "application/x-git-upload-pack-request",
      ...authorization(token),
    },
    body,
  });
  if (!response.ok || response.body === null) {
    throw new Error(`upload-pack returned ${response.status}`);
  }
  return response;
};

/**
 * The local repository over the caller's stores, for the history walk alone.
 *
 * `noPacks` is not a claim that there are none — only `gc`/`repack` consult
 * that port, and the reads below go through the object store the caller
 * handed in, which is already pack-aware wherever its backend is.
 */
const localRepository = (stores: FetchStores): Layer.Layer<Repository> =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(Layer.succeed(ObjectStoreTag, stores.objects)),
    Layer.provide(Layer.succeed(RefStoreTag, stores.refs)),
    Layer.provide(noPacks),
  );

/**
 * The commits to offer as `have`, newest first.
 *
 * Newest first is what makes 32 lines usually enough: a shared base sits near
 * the tips, not deep in history, so the first round is the one that lands.
 * First-parent only, because that is the walk `Repository.log` does — a have
 * is a hint rather than an inventory, and missing a side branch costs pack
 * size, not correctness.
 */
const localHaves = Effect.fn("Fetch.haves")(function* (stores: FetchStores) {
  const tips = yield* stores.refs.list();
  if (tips.length === 0) return [];

  const dated = new Map<Oid, number>();
  yield* Effect.gen(function* () {
    const repository = yield* Repository;
    for (const [, tip] of tips) {
      const commits = yield* Stream.runCollect(
        repository.log(tip, { limit: MAX_HAVES }).pipe(
          // A ref whose history is not all here — an interrupted clone, a
          // shallow one — still contributes the part that is.
          Stream.catch(() => Stream.empty),
        ),
      );
      for (const commit of commits) dated.set(commit.oid, commit.committer.at.getTime());
    }
  }).pipe(Effect.provide(localRepository(stores)));

  return [...dated]
    .sort(([, left], [, right]) => right - left)
    .slice(0, MAX_HAVES)
    .map(([oid]) => oid);
});

/**
 * The acknowledgment lines a response opens with, and the bytes after them.
 *
 * There is no count and no delimiter between the two. Stock upload-pack emits
 * one `ACK <oid>` per have it recognises — not the single line the protocol
 * description reads like — and the packfile starts immediately after the last
 * of them. What tells them apart is that a pkt-line begins with four hex
 * digits and a pack begins with `PACK`, so the lines are read until the length
 * header stops being a number, and those four bytes are the pack's own magic.
 *
 * `PktReader` cannot do this: a length it cannot parse is corruption as far as
 * it is concerned, and by the time it says so the four bytes are gone.
 */
const prelude = async (
  body: AsyncIterable<Uint8Array>,
): Promise<{
  readonly lines: ReadonlyArray<string>;
  readonly rest: AsyncIterable<Uint8Array>;
}> => {
  const iterator = body[Symbol.asyncIterator]();
  let buffered = new Uint8Array(0);

  const buffer = async (least: number): Promise<boolean> => {
    while (buffered.length < least) {
      const next = await iterator.next();
      if (next.done === true) return false;
      const grown = new Uint8Array(buffered.length + next.value.length);
      grown.set(buffered);
      grown.set(next.value, buffered.length);
      buffered = grown;
    }
    return true;
  };

  const lines: string[] = [];
  for (;;) {
    if (!(await buffer(4))) break;
    const header = decoder.decode(buffered.subarray(0, 4));
    if (!/^[0-9a-f]{4}$/.test(header)) break;
    const length = Number.parseInt(header, 16);
    // 0000/0001/0002 are the flush, delimiter and response-end packets: four
    // bytes each and nothing to read past them.
    if (length < 4) {
      buffered = buffered.subarray(4);
      continue;
    }
    if (!(await buffer(length))) break;
    lines.push(decoder.decode(buffered.subarray(4, length)).replace(/\n$/, ""));
    buffered = buffered.subarray(length);
  }

  const head = buffered;
  return {
    lines,
    rest: (async function* () {
      if (head.length > 0) yield head;
      for (;;) {
        const next = await iterator.next();
        if (next.done === true) return;
        yield next.value;
      }
    })(),
  };
};

/**
 * Whether a round's acknowledgments end the negotiation.
 *
 * Single-ACK has two answers: a bare `ACK <oid>` means the server found a base
 * and wants `done` next, `NAK` means keep offering. `ACK <oid> continue` is
 * `multi_ack`'s "noted, carry on" and `ready` is `multi_ack_detailed`'s "stop,
 * I can build the pack" — neither capability is requested, but reading them
 * correctly is cheaper than the failure mode of not doing so.
 */
const acknowledged = (lines: ReadonlyArray<string>): boolean =>
  lines.some((line) => {
    if (!line.startsWith("ACK ")) return false;
    const trailer = line.slice(44).trim();
    return trailer === "" || trailer === "ready";
  });

/**
 * The rounds before `done`, and how many haves they got through — the prefix
 * the final request repeats.
 *
 * Against `server/Protocol.ts` this always runs to exhaustion: that server
 * acknowledges only on the round that carries `done`, so every round here
 * comes back NAK and the loop ends when the haves do. The rounds exist for
 * the servers that do answer early, and the cap is what bounds the cost of
 * the ones that do not.
 */
const negotiate = Effect.fn("Fetch.negotiate")(function* (input: {
  readonly url: string;
  readonly token: string | undefined;
  readonly wants: ReadonlyArray<Oid>;
  readonly haves: ReadonlyArray<Oid>;
}) {
  const { haves, token, url, wants } = input;
  let offered = 0;
  while (offered < haves.length) {
    const next = Math.min(offered + HAVES_PER_ROUND, haves.length);
    const stop = yield* Effect.tryPromise({
      try: async () => {
        const response = await uploadPack(
          url,
          token,
          negotiation(wants, haves.slice(0, next), false),
        );
        const { lines } = await prelude(response.body as unknown as AsyncIterable<Uint8Array>);
        return acknowledged(lines);
      },
      catch: (cause) => unreachable(String(cause)),
    });
    offered = next;
    if (stop) break;
  }
  return offered;
});

/**
 * Fetch everything reachable from the remote's branches (or one `branch`)
 * into the given stores and set the refs. A missing branch fails with
 * `Invalid` on the `branch` field; an unreachable remote on `remote`.
 */
export const fetchRepository = (options: {
  readonly url: string;
  readonly branch?: string | undefined;
  readonly token?: string | undefined;
  readonly stores: FetchStores;
}): Effect.Effect<FetchResult, Invalid | PackCorrupt | ObjectNotFound | StorageFailure> =>
  Effect.gen(function* () {
    const { branch, stores, token, url } = options;
    const advertisement = yield* lsRemote(url, { token });

    const head = advertisement.find((ref) => ref.name === "HEAD")?.oid;
    const picked = advertisement.filter(
      (ref) =>
        (branch === undefined
          ? ref.name.startsWith("refs/heads/") || ref.name.startsWith("refs/tags/")
          : ref.name === `refs/heads/${branch}`) && ref.name !== "HEAD",
    );
    if (picked.length === 0) {
      if (branch !== undefined) {
        return yield* new Invalid({ field: "branch", reason: `remote has no branch '${branch}'` });
      }
      return { refs: [], defaultBranch: undefined };
    }

    const wants = [...new Set(picked.map((ref) => ref.oid))];

    // Empty target, empty offer: the clone case sends `done` straight away
    // rather than a round that could only say "I have nothing".
    const haves = yield* localHaves(stores);
    const offered = yield* negotiate({ url, token, wants, haves });

    const packBody = yield* Effect.tryPromise({
      try: async () => {
        const response = await uploadPack(
          url,
          token,
          negotiation(wants, haves.slice(0, offered), true),
        );
        const { rest } = await prelude(response.body as unknown as AsyncIterable<Uint8Array>);
        return rest;
      },
      catch: (cause) => unreachable(String(cause)),
    });

    yield* Pack.unpack(
      Stream.fromAsyncIterable(packBody, (cause) => unreachable(String(cause))),
    ).pipe(Effect.provideService(ObjectStoreTag, stores.objects));

    const updates: RefUpdate[] = picked.map((ref) => ({
      name: ref.name,
      value: ref.oid,
      reason: "fetch",
    }));
    yield* stores.refs.apply(updates);

    const defaultBranch = picked
      .find((ref) => ref.name.startsWith("refs/heads/") && ref.oid === head)
      ?.name.slice("refs/heads/".length);
    return { refs: updates, defaultBranch: branch ?? defaultBranch };
  });
