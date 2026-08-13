/**
 * The ports over OPFS (Origin Private File System).
 *
 * Byte-for-byte the same loose-object layout `git/Node.ts` writes, so a
 * repository synced into a browser is the same repository. Compression is
 * `CompressionStream("deflate")`; nothing here imports `node:*`.
 *
 * Atomicity comes from `createWritable`, which stages and replaces on
 * `close()` — the async analogue of the node backend's `rename(2)`. Cross-tab
 * races are the host's problem, as cross-process races are on node.
 */
import { Effect, Layer, Stream } from "effect";

import { ObjectNotFound, StorageFailure } from "../git/Error.ts";
import {
  decodeObject,
  encodeObject,
  encodeReflogLine,
  hashObject,
  parseReflogLine,
} from "../git/Format.ts";
import { noPacks } from "../git/Packed.ts";
import {
  checkHeadTarget,
  checkRefAddress,
  checkRefNames,
  isOid,
  ObjectStore,
  type Oid,
  type ReflogEntry,
  RefStore,
  tracedObjectStore,
  tracedRefStore,
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

      return ObjectStore.of(
        tracedObjectStore("OPFS", {
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
          list: Stream.unwrap(
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
        }),
      );
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

      /** As in `git/Node.ts`: every reader joins the name onto the root too. */
      const addressable = (name: string) => name === "HEAD" || checkRefAddress(name) === null;

      /** The file's text, whatever it holds — `resolve` needs the `ref: ` form. */
      const readRaw = (name: string) => (addressable(name) ? readText(name) : Effect.succeed(null));

      const read = (name: string) =>
        readRaw(name).pipe(
          // As in `git/Node.ts`: a symbolic ref's text is not an oid, and
          // `apply` would otherwise write it into a commit's parent.
          Effect.map((value) => (value !== null && isOid(value) ? (value as Oid) : null)),
        );

      const head = readText("HEAD").pipe(
        Effect.map((value) => (value === null ? "refs/heads/main" : value.replace(/^ref:\s*/, ""))),
      );

      const appendReflog = (update: RefUpdate, from: Oid | null, at: Date) =>
        Effect.tryPromise({
          try: async () => {
            const target = `logs/${update.name}`;
            const line = encodeReflogLine({
              from,
              to: update.value,
              at,
              message: update.reason ?? "update",
            });
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
            const found = new Map<string, Oid>();
            /** `refs/x` -> `refs/y`, for the symbolic ones. */
            const symbolic = new Map<string, string>();

            const walk = async (directory: FileSystemDirectoryHandle, at: string) => {
              for await (const [name, entry] of entriesOf(directory)) {
                const full = `${at}/${name}`;
                if (entry.kind === "directory") {
                  await walk(entry as FileSystemDirectoryHandle, full);
                  continue;
                }
                const value = await readBytes(root, full);
                if (value === null) continue;
                const text = decoder.decode(value).trim();
                // As in `git/Node.ts`: a symbolic ref's `ref: …` text is not
                // an oid, and branding it as one puts that text in the
                // advertisement and in gc's root set.
                if (text.startsWith("ref: ")) symbolic.set(full, text.slice("ref: ".length).trim());
                else if (isOid(text)) found.set(full, text);
              }
            };
            try {
              await walk(await root.getDirectoryHandle("refs"), "refs");
            } catch (cause) {
              if (!notFound(cause)) throw cause;
            }

            for (const [name, target] of symbolic) {
              const value = found.get(target);
              if (value !== undefined) found.set(name, value);
            }

            return [...found]
              .filter(([name]) => prefix === undefined || name.startsWith(prefix))
              .map(([name, value]) => [name, value] as const);
          },
          catch: failure("list", "refs"),
        });

      return RefStore.of(
        tracedRefStore("OPFS", {
          read,
          resolve: (name) =>
            Effect.gen(function* () {
              let current = name;
              // The same walk `git/Node.ts` does, for the same reasons: a
              // detached HEAD holds its commit, any ref may be symbolic, and
              // text that is not an oid is not an answer.
              for (let depth = 0; depth < 8; depth++) {
                if (isOid(current)) return current;
                if (current === "HEAD") {
                  current = yield* head;
                  continue;
                }
                const value = yield* readRaw(current);
                if (value === null) return null;
                if (value.startsWith("ref: ")) {
                  current = value.slice("ref: ".length).trim();
                  continue;
                }
                return isOid(value) ? value : null;
              }
              return null;
            }),
          list: listRefs,
          apply: (updates, options) =>
            Effect.gen(function* () {
              yield* checkRefNames(updates);

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

              /** One update, as a ref write: `null` deletes. */
              const put = (name: string, value: Oid | null) =>
                value === null
                  ? Effect.tryPromise({
                      try: () => removePath(root, name),
                      catch: failure("delete", name),
                    })
                  : writeText(name, `${value}\n`);

              /** What has been written, newest last, for an atomic undo. */
              const done: Array<{ from: Oid | null; update: RefUpdate }> = [];

              for (const { from, update } of pending) {
                const written = yield* put(update.name, update.value).pipe(
                  Effect.as(true),
                  Effect.catchTag("StorageFailure", () => Effect.succeed(false)),
                );

                if (!written) {
                  // `atomic` is a promise about the batch: the refs already
                  // written go back where they were and nothing is applied.
                  if (options?.atomic === true) {
                    for (const undo of done.reverse()) {
                      yield* put(undo.update.name, undo.from).pipe(Effect.ignore);
                    }
                    return yield* Effect.forEach(results, (result) =>
                      read(result.name).pipe(
                        Effect.map((current) => ({
                          name: result.name,
                          applied: false,
                          current,
                          // The batch failed because one ref could not be
                          // written, not because anyone else moved these.
                          reason: "cannot lock ref",
                        })),
                      ),
                    );
                  }

                  const index = results.findIndex((result) => result.name === update.name);
                  if (index !== -1) {
                    results[index] = {
                      name: update.name,
                      applied: false,
                      current: yield* read(update.name),
                      reason: "cannot lock ref",
                    };
                  }
                  continue;
                }

                done.push({ from, update });
              }

              // Once every write in the batch has landed, not as each one does:
              // the atomic undo above puts the refs back, but an appended log
              // line cannot be taken out again — so the log recorded a move
              // that did not happen, and `Maintenance.gc` treats reflog entries
              // as roots, pinning the rolled-back commits for the grace window.
              //
              // The log is the record of a move that has already happened;
              // failing the update because it could not be written would
              // report a ref as untouched while it sits at its new value.
              for (const { from, update } of done) {
                yield* appendReflog(update, from, at).pipe(Effect.ignore);
              }

              return results;
            }),
          head,
          setHead: (target) =>
            checkHeadTarget(target).pipe(Effect.andThen(writeText("HEAD", `ref: ${target}\n`))),
          reflog: (name) =>
            Effect.gen(function* () {
              if (!addressable(name)) return [] as ReflogEntry[];
              const text = yield* readText(`logs/${name}`);
              if (text === null) return [] as ReflogEntry[];
              return text
                .split("\n")
                .map(parseReflogLine)
                .filter((entry): entry is ReflogEntry => entry !== null);
            }),
          logged: Effect.tryPromise({
            try: async () => {
              const names: string[] = [];
              const walk = async (directory: FileSystemDirectoryHandle, at: string) => {
                for await (const [name, entry] of entriesOf(directory)) {
                  const full = at === "" ? name : `${at}/${name}`;
                  if (entry.kind === "directory") {
                    await walk(entry as FileSystemDirectoryHandle, full);
                  } else names.push(full);
                }
              };
              try {
                await walk(await root.getDirectoryHandle("logs"), "");
              } catch (cause) {
                if (!notFound(cause)) throw cause;
              }
              return names;
            },
            catch: failure("reflog.list", "logs"),
          }),
        }),
      );
    }),
  );

/** Both stores over one directory handle — in a browser, an OPFS directory. */
export const stores = (root: FileSystemDirectoryHandle | Promise<FileSystemDirectoryHandle>) =>
  // `noPacks`: a tab clones and reads, and the pack it receives is exploded
  // to loose objects on arrival as it always was. Packs at rest would buy a
  // browser the same key-count saving they buy R2, and the read path is
  // already shared — it is the write side that has no caller here yet.
  Layer.mergeAll(objectStore(root), refStore(root)).pipe(Layer.provideMerge(noPacks));
