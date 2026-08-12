/**
 * The client half of smart HTTP — enough to clone.
 *
 * Advertisement, then one `want … done` round, then the pack streamed into
 * the target stores with the same `PktReader` and `Pack.unpack` the server
 * uses. Serves the Artifacts provider's `import` and the CLI's `clone` from
 * one implementation; the browser client grows from here too, since nothing
 * in this file is node-specific — `fetch` and web streams only.
 */
import { Effect, Stream } from "effect";

import {
  Invalid,
  type ObjectNotFound,
  type PackCorrupt,
  type StorageFailure,
} from "../git/Error.ts";
import * as Pack from "../git/Pack.ts";
import { isOid, type ObjectStore, type Oid, type RefStore, type RefUpdate } from "../git/Store.ts";
import { ObjectStore as ObjectStoreTag } from "../git/Store.ts";
import { PktReader } from "../server/Protocol.ts";

const decoder = new TextDecoder();

export interface RemoteRef {
  readonly oid: Oid;
  readonly name: string;
}

const unreachable = (reason: string) => new Invalid({ field: "remote", reason });

const advertisedRefs = async (body: ReadableStream<Uint8Array> | null) => {
  if (body === null) return [];
  const reader = new PktReader(body as unknown as AsyncIterable<Uint8Array>);
  const refs: RemoteRef[] = [];
  for (;;) {
    const item = await reader.next();
    if (item === "eof") break;
    if (item === "flush") continue;
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
 * Fetch everything reachable from the remote's branches (or one `branch`)
 * into the given stores and set the refs. A missing branch fails with
 * `Invalid` on the `branch` field; an unreachable remote on `remote`.
 */
export const fetchRepository = (options: {
  readonly url: string;
  readonly branch?: string | undefined;
  readonly token?: string | undefined;
  readonly stores: {
    readonly objects: ObjectStore["Service"];
    readonly refs: RefStore["Service"];
  };
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
    const encoder = new TextEncoder();
    const pktLine = (line: string) => `${(line.length + 4).toString(16).padStart(4, "0")}${line}`;
    const request = encoder.encode(
      `${wants.map((oid) => pktLine(`want ${oid}\n`)).join("")}0000${pktLine("done\n")}`,
    );

    const packBody = yield* Effect.tryPromise({
      try: async () => {
        const response = await fetch(`${url}/git-upload-pack`, {
          method: "POST",
          headers: {
            "content-type": "application/x-git-upload-pack-request",
            ...authorization(token),
          },
          body: request,
        });
        if (!response.ok || response.body === null) {
          throw new Error(`upload-pack returned ${response.status}`);
        }
        const reader = new PktReader(response.body as unknown as AsyncIterable<Uint8Array>);
        await reader.next(); // ACK/NAK prelude
        return reader.rest();
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
