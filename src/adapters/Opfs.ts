/**
 * Browser stores: the ports over OPFS (Origin Private File System).
 *
 * The fourth backend, and the second filesystem-shaped one — the layout is
 * git's own, byte-for-byte the same loose objects `git/Node.ts` writes, so a
 * repository synced down to a browser is the same repository everywhere else.
 * Compression is `CompressionStream("deflate")`, which browsers, node and
 * workerd all provide; nothing here imports `node:*`.
 *
 * Atomicity leans on the platform the way each backend does: OPFS's
 * `createWritable` stages into a swap file and replaces the target on
 * `close()`, the async-API analogue of the `rename(2)` the node backend uses.
 * Cross-tab races remain the host's problem, exactly as cross-process races
 * are on the node host.
 */
import { Effect, Layer, Stream } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { decodeObject, encodeObject, hashObject } from "../git/Format.ts";
import {
  ObjectStore,
  type Oid,
  type ReflogEntry,
  RefStore,
  type RefUpdate,
  type RefUpdateResult,
} from "../git/Store.ts";

const failure = (operation: string, target: string) => (cause: unknown) =>
  new StorageFailure({ operation, path: target, cause });

const notFound = (cause: unknown): boolean =>
  cause instanceof DOMException
    ? cause.name === "NotFoundError"
    : cause instanceof Error && cause.name === "NotFoundError";

const pipeThrough = async (
  bytes: Uint8Array,
  transform: { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> },
) =>
  new Uint8Array(
    await new Response(
      new Blob([bytes as Uint8Array<ArrayBuffer>]).stream().pipeThrough(transform),
    ).arrayBuffer(),
  );

const asPair = (stream: CompressionStream | DecompressionStream) =>
  stream as unknown as {
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  };

const deflate = (bytes: Uint8Array) => pipeThrough(bytes, asPair(new CompressionStream("deflate")));
const inflate = (bytes: Uint8Array) =>
  pipeThrough(bytes, asPair(new DecompressionStream("deflate")));

/** Walk `a/b/c` to the parent directory handle plus the leaf name. */
const parentOf = async (root: FileSystemDirectoryHandle, target: string, create: boolean) => {
  const parts = target.split("/");
  const leaf = parts.pop()!;
  let directory = root;
  for (const part of parts) directory = await directory.getDirectoryHandle(part, { create });
  return { directory, leaf };
};

const readBytes = async (
  root: FileSystemDirectoryHandle,
  target: string,
): Promise<Uint8Array | null> => {
  try {
    const { directory, leaf } = await parentOf(root, target, false);
    const handle = await directory.getFileHandle(leaf);
    return new Uint8Array(await (await handle.getFile()).arrayBuffer());
  } catch (cause) {
    if (notFound(cause)) return null;
    throw cause;
  }
};

const writeBytes = async (root: FileSystemDirectoryHandle, target: string, bytes: Uint8Array) => {
  const { directory, leaf } = await parentOf(root, target, true);
  const handle = await directory.getFileHandle(leaf, { create: true });
  const writable = await handle.createWritable();
  await writable.write(bytes as Uint8Array<ArrayBuffer>);
  await writable.close();
};

const removePath = async (root: FileSystemDirectoryHandle, target: string) => {
  try {
    const { directory, leaf } = await parentOf(root, target, false);
    await directory.removeEntry(leaf);
  } catch (cause) {
    if (!notFound(cause)) throw cause;
  }
};

const entriesOf = (directory: FileSystemDirectoryHandle) =>
  (directory as unknown as { entries(): AsyncIterable<[string, FileSystemHandle]> }).entries();

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const objectStore = (
  rootHandle: FileSystemDirectoryHandle | Promise<FileSystemDirectoryHandle>,
) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const root = yield* Effect.promise(() => Promise.resolve(rootHandle));
      const pathFor = (oid: Oid) => `objects/${oid.slice(0, 2)}/${oid.slice(2)}`;

      const read = (oid: Oid) =>
        Effect.tryPromise({
          try: async () => {
            const deflated = await readBytes(root, pathFor(oid));
            return deflated === null ? null : await inflate(deflated);
          },
          catch: failure("read", pathFor(oid)),
        }).pipe(
          Effect.flatMap((bytes) =>
            bytes === null
              ? Effect.fail(new ObjectNotFound({ oid }))
              : Effect.fromResult(decodeObject(bytes)).pipe(
                  Effect.mapError(() => new ObjectNotFound({ oid })),
                ),
          ),
        );

      return ObjectStore.of({
        read,
        readStream: (oid) =>
          read(oid).pipe(
            Effect.map(
              (object) =>
                Stream.fromIterable([object.data]) as Stream.Stream<Uint8Array, StorageFailure>,
            ),
          ),
        write: (object) =>
          Effect.gen(function* () {
            const oid = yield* hashObject(object);
            const target = pathFor(oid);
            yield* Effect.tryPromise({
              try: async () => {
                // Content-addressed: a rewrite would be identical bytes.
                if ((await readBytes(root, target)) !== null) return;
                await writeBytes(root, target, await deflate(encodeObject(object)));
              },
              catch: failure("write", target),
            });
            return oid;
          }),
        has: (oid) =>
          Effect.tryPromise({
            try: async () => (await readBytes(root, pathFor(oid))) !== null,
            catch: failure("has", pathFor(oid)),
          }),
        delete: (oid) =>
          Effect.tryPromise({
            try: () => removePath(root, pathFor(oid)),
            catch: failure("delete", pathFor(oid)),
          }),
        list: () =>
          Stream.unwrap(
            Effect.tryPromise({
              try: async () => {
                const oids: Oid[] = [];
                let objects: FileSystemDirectoryHandle;
                try {
                  objects = await root.getDirectoryHandle("objects");
                } catch (cause) {
                  if (notFound(cause)) return [];
                  throw cause;
                }
                for await (const [prefix, entry] of entriesOf(objects)) {
                  if (entry.kind !== "directory") continue;
                  for await (const [rest] of entriesOf(entry as FileSystemDirectoryHandle)) {
                    oids.push(`${prefix}${rest}` as Oid);
                  }
                }
                return oids;
              },
              catch: failure("list", "objects"),
            }).pipe(Effect.map(Stream.fromIterable)),
          ) as Stream.Stream<Oid, StorageFailure>,
      });
    }),
  );

export const refStore = (
  rootHandle: FileSystemDirectoryHandle | Promise<FileSystemDirectoryHandle>,
) =>
  Layer.effect(
    RefStore,
    Effect.gen(function* () {
      const root = yield* Effect.promise(() => Promise.resolve(rootHandle));

      const readText = (target: string) =>
        Effect.tryPromise({
          try: async () => {
            const bytes = await readBytes(root, target);
            return bytes === null ? null : decoder.decode(bytes).trim();
          },
          catch: failure("read", target),
        });

      const writeText = (target: string, contents: string) =>
        Effect.tryPromise({
          try: () => writeBytes(root, target, encoder.encode(contents)),
          catch: failure("write", target),
        });

      const read = (name: string) =>
        readText(name).pipe(Effect.map((value) => value as Oid | null));

      const head = readText("HEAD").pipe(
        Effect.map((value) => (value === null ? "refs/heads/main" : value.replace(/^ref:\s*/, ""))),
      );

      const appendReflog = (update: RefUpdate, from: Oid | null, at: Date) =>
        Effect.tryPromise({
          try: async () => {
            const target = `logs/${update.name}`;
            const zero = "0".repeat(40);
            const line = `${from ?? zero} ${update.value ?? zero} ${at.toISOString()}\t${update.reason ?? "update"}\n`;
            const existing = await readBytes(root, target);
            const appended = encoder.encode(line);
            const combined =
              existing === null
                ? appended
                : (() => {
                    const out = new Uint8Array(existing.length + appended.length);
                    out.set(existing);
                    out.set(appended, existing.length);
                    return out;
                  })();
            await writeBytes(root, target, combined);
          },
          catch: failure("reflog", update.name),
        });

      const listRefs = (prefix?: string) =>
        Effect.tryPromise({
          try: async () => {
            const found: Array<readonly [string, Oid]> = [];
            const walk = async (directory: FileSystemDirectoryHandle, at: string) => {
              for await (const [name, entry] of entriesOf(directory)) {
                const full = `${at}/${name}`;
                if (entry.kind === "directory") {
                  await walk(entry as FileSystemDirectoryHandle, full);
                } else {
                  const value = await readBytes(root, full);
                  if (value !== null) found.push([full, decoder.decode(value).trim() as Oid]);
                }
              }
            };
            try {
              await walk(await root.getDirectoryHandle("refs"), "refs");
            } catch (cause) {
              if (!notFound(cause)) throw cause;
            }
            return found.filter(([name]) => prefix === undefined || name.startsWith(prefix));
          },
          catch: failure("list", "refs"),
        });

      return RefStore.of({
        read,
        resolve: (name) =>
          Effect.gen(function* () {
            let current = name;
            for (let depth = 0; depth < 8; depth++) {
              if (current === "HEAD") {
                current = yield* head;
                continue;
              }
              return yield* read(current);
            }
            return null;
          }),
        list: listRefs,
        apply: (updates, options) =>
          Effect.gen(function* () {
            for (const update of updates) {
              if (update.name.length === 0 || update.name.includes(" ")) {
                return yield* new Invalid({
                  field: "ref",
                  reason: `bad ref name '${update.name}'`,
                });
              }
            }

            const at = new Date();
            const results: RefUpdateResult[] = [];
            const pending: Array<{ from: Oid | null; update: RefUpdate }> = [];

            // Check everything before writing anything, like every backend.
            for (const update of updates) {
              const actual = yield* read(update.name);
              const matches = update.expected === undefined || update.expected === actual;
              results.push({
                name: update.name,
                applied: matches,
                current: matches ? update.value : actual,
              });
              if (matches) pending.push({ from: actual, update });
            }

            if (options?.atomic === true && results.some((result) => !result.applied)) {
              return yield* Effect.forEach(results, (result) =>
                read(result.name).pipe(
                  Effect.map((current) => ({ name: result.name, applied: false, current })),
                ),
              );
            }

            for (const { from, update } of pending) {
              yield* update.value === null
                ? Effect.tryPromise({
                    try: () => removePath(root, update.name),
                    catch: failure("delete", update.name),
                  })
                : writeText(update.name, `${update.value}\n`);
              yield* appendReflog(update, from, at);
            }

            return results;
          }),
        head,
        setHead: (target) => writeText("HEAD", `ref: ${target}\n`),
        reflog: (name) =>
          Effect.gen(function* () {
            const text = yield* readText(`logs/${name}`);
            if (text === null) return [] as ReflogEntry[];
            const zero = "0".repeat(40);
            return text
              .split("\n")
              .filter((line) => line.length > 0)
              .map((line): ReflogEntry => {
                const [values = "", message = ""] = line.split("\t");
                const [from = zero, to = zero, at = ""] = values.split(" ");
                return {
                  from: from === zero ? null : (from as Oid),
                  to: to === zero ? null : (to as Oid),
                  at: new Date(at),
                  message,
                };
              });
          }),
      });
    }),
  );

/** Both stores over one directory handle — in a browser, an OPFS directory. */
export const stores = (root: FileSystemDirectoryHandle | Promise<FileSystemDirectoryHandle>) =>
  Layer.mergeAll(objectStore(root), refStore(root));
