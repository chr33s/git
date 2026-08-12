/**
 * Filesystem backend.
 *
 * The second implementation of the ports, and the one that proves they are
 * ports: it passes the same contract suite as `Memory.ts` without either the
 * suite or `Repository` knowing which is loaded.
 *
 * Layout is git's own, so a repository written here can be inspected with the
 * real `git` binary:
 *
 *   <root>/objects/ab/cdef…   loose objects, zlib-deflated
 *   <root>/refs/heads/main    one file per ref
 *   <root>/HEAD               symbolic ref
 *
 * Atomicity comes from `rename(2)`, which is atomic within a filesystem: a ref
 * update writes a temp file and renames it over the target. That is the same
 * guarantee `RefStore.apply` promises on Workers via the DO input gate, which
 * is why the port can demand it of every backend instead of leaving it
 * optional.
 */
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { deflateSync, inflateSync } from "node:zlib";

import { Effect, Layer, Stream } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "./Error.ts";
import { decodeObject, encodeObject, hashObject } from "./Format.ts";
import { packed, type PackHandle, PackStore } from "./Packed.ts";
import {
  ObjectStore,
  type Oid,
  type ReflogEntry,
  RefStore,
  tracedRefStore,
  type RefUpdate,
  type RefUpdateResult,
} from "./Store.ts";

const failure = (operation: string, target: string) => (cause: unknown) =>
  new StorageFailure({ operation, path: target, cause });

/** Objects are immutable, so a write is temp-file plus rename, no locking. */
export const packStore = (root: string) => Layer.sync(PackStore, () => filePacks(root));

export const objectStore = (root: string) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const packs = yield* PackStore;
      const objectsDir = path.join(root, "objects");
      const pathFor = (oid: Oid) => path.join(objectsDir, oid.slice(0, 2), oid.slice(2));

      const read = (oid: Oid) =>
        Effect.tryPromise({
          try: () => fs.readFile(pathFor(oid)),
          catch: () => new ObjectNotFound({ oid }),
        }).pipe(
          Effect.flatMap((deflated) =>
            Effect.try({
              try: () => inflateSync(deflated),
              catch: failure("read", pathFor(oid)),
            }),
          ),
          Effect.flatMap((bytes) =>
            Effect.fromResult(decodeObject(new Uint8Array(bytes))).pipe(
              Effect.mapError(() => new ObjectNotFound({ oid })),
            ),
          ),
        );

      const loose: ObjectStore["Service"] = {
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

            // Already there: objects are content-addressed, so a rewrite would
            // be identical bytes and only costs IO.
            if (existsSync(target)) return oid;

            yield* Effect.tryPromise({
              try: async () => {
                await fs.mkdir(path.dirname(target), { recursive: true });
                const temporary = `${target}.${crypto.randomUUID()}.tmp`;
                await fs.writeFile(temporary, deflateSync(encodeObject(object)));
                await fs.rename(temporary, target);
              },
              catch: failure("write", target),
            });

            return oid;
          }),
        has: (oid) => Effect.sync(() => existsSync(pathFor(oid))),
        delete: (oid) =>
          Effect.tryPromise({
            try: () => fs.rm(pathFor(oid), { force: true }),
            catch: failure("delete", pathFor(oid)),
          }),
        list: Stream.unwrap(
          Effect.tryPromise({
            try: async () => {
              if (!existsSync(objectsDir)) return [];
              const oids: Oid[] = [];
              for (const prefix of await fs.readdir(objectsDir)) {
                for (const rest of await fs.readdir(path.join(objectsDir, prefix))) {
                  oids.push(`${prefix}${rest}` as Oid);
                }
              }
              return oids;
            },
            catch: failure("list", objectsDir),
          }).pipe(Effect.map(Stream.fromIterable)),
        ) as Stream.Stream<Oid, StorageFailure>,
      };

      return ObjectStore.of(packed(loose, packs, "Node"));
    }),
  );

/**
 * Packs on disk, in git's own `objects/pack` layout.
 *
 * A `.pack` is read through `read(2)` at an offset rather than loaded, which
 * is what keeps a repository with a gigabyte pack usable from a process that
 * does not have a gigabyte.
 */
export const filePacks = (root: string): PackStore["Service"] => {
  const directory = path.join(root, "objects", "pack");

  return {
    list: Effect.tryPromise({
      try: async () => {
        if (!existsSync(directory)) return [];
        const names = (await fs.readdir(directory))
          .filter((name) => name.endsWith(".idx"))
          .map((name) => name.slice(0, -4));

        const handles: PackHandle[] = [];
        for (const name of names) {
          const packPath = path.join(directory, `${name}.pack`);
          // An `.idx` without its `.pack` is a half-finished write, not a
          // pack; skipping it beats failing every read in the repository.
          if (!existsSync(packPath)) continue;

          const index = new Uint8Array(await fs.readFile(path.join(directory, `${name}.idx`)));
          const size = (await fs.stat(packPath)).size;
          handles.push({
            name,
            index,
            source: {
              size,
              read: async (offset, length) => {
                const handle = await fs.open(packPath, "r");
                try {
                  const buffer = Buffer.alloc(Math.max(0, Math.min(length, size - offset)));
                  await handle.read(buffer, 0, buffer.length, offset);
                  return new Uint8Array(buffer);
                } finally {
                  await handle.close();
                }
              },
            },
          });
        }
        return handles;
      },
      catch: failure("packs.list", directory),
    }),

    write: ({ index, name, pack }) =>
      Effect.tryPromise({
        try: async () => {
          await fs.mkdir(directory, { recursive: true });
          // The pack lands before the index that points into it: an index
          // without a pack is skipped above, but a pack without an index is
          // simply not consulted, and neither state loses an object.
          await fs.writeFile(path.join(directory, `${name}.pack`), pack);
          await fs.writeFile(path.join(directory, `${name}.idx`), index);
        },
        catch: failure("packs.write", directory),
      }),

    delete: (name) =>
      Effect.tryPromise({
        try: async () => {
          // Index first: it is what makes the pack visible, so removing it
          // first means a reader never sees an index whose pack is gone.
          await fs.rm(path.join(directory, `${name}.idx`), { force: true });
          await fs.rm(path.join(directory, `${name}.pack`), { force: true });
        },
        catch: failure("packs.delete", directory),
      }),
  };
};

/**
 * Refs on disk.
 *
 * The batch is checked before anything is written, so an atomic batch with one
 * stale entry writes nothing. Serializing concurrent batches is the host's job
 * (the readme's "One Durable Object per repository"), exactly as the DO input
 * gate does it on Workers; this layer assumes it is not racing itself.
 */
export const refStore = (root: string) =>
  Layer.effect(
    RefStore,
    Effect.sync(() => {
      const pathFor = (name: string) => path.join(root, name);
      const headPath = path.join(root, "HEAD");

      const readFile = (target: string) =>
        Effect.tryPromise({
          try: async () => (existsSync(target) ? (await fs.readFile(target, "utf8")).trim() : null),
          catch: failure("read", target),
        });

      const writeAtomic = (target: string, contents: string) =>
        Effect.tryPromise({
          try: async () => {
            await fs.mkdir(path.dirname(target), { recursive: true });
            const temporary = `${target}.${crypto.randomUUID()}.tmp`;
            await fs.writeFile(temporary, contents);
            // rename(2) is atomic within a filesystem: a concurrent reader sees
            // either the old ref or the new one, never a half-written file.
            await fs.rename(temporary, target);
          },
          catch: failure("write", target),
        });

      const read = (name: string) =>
        readFile(pathFor(name)).pipe(Effect.map((value) => value as Oid | null));

      const head = readFile(headPath).pipe(
        Effect.map((value) => (value === null ? "refs/heads/main" : value.replace(/^ref:\s*/, ""))),
      );

      const appendReflog = (update: RefUpdate, from: Oid | null, at: Date) =>
        Effect.tryPromise({
          try: async () => {
            const target = path.join(root, "logs", update.name);
            await fs.mkdir(path.dirname(target), { recursive: true });
            const line = `${from ?? "0".repeat(40)} ${update.value ?? "0".repeat(40)} ${at.toISOString()}\t${update.reason ?? "update"}\n`;
            await fs.appendFile(target, line);
          },
          catch: failure("reflog", update.name),
        });

      return RefStore.of(
        tracedRefStore("Node", {
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
          list: (prefix) =>
            Effect.tryPromise({
              try: async () => {
                const base = path.join(root, "refs");
                if (!existsSync(base)) return [] as Array<readonly [string, Oid]>;

                const found: Array<readonly [string, Oid]> = [];
                const walk = async (dir: string): Promise<void> => {
                  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
                    const full = path.join(dir, entry.name);
                    if (entry.isDirectory()) {
                      await walk(full);
                      continue;
                    }
                    if (full.endsWith(".tmp")) continue;
                    const name = path.relative(root, full).split(path.sep).join("/");
                    const value = (await fs.readFile(full, "utf8")).trim() as Oid;
                    found.push([name, value] as const);
                  }
                };
                await walk(base);
                return found.filter(([name]) => prefix === undefined || name.startsWith(prefix));
              },
              catch: failure("list", root),
            }),
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

              // Check everything first: an atomic batch that fails must not have
              // written anything, and rename(2) cannot be undone.
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
                const target = pathFor(update.name);
                yield* update.value === null
                  ? Effect.tryPromise({
                      try: () => fs.rm(target, { force: true }),
                      catch: failure("delete", target),
                    })
                  : writeAtomic(target, `${update.value}\n`);
                yield* appendReflog(update, from, at);
              }

              return results;
            }),
          head,
          setHead: (target) => writeAtomic(headPath, `ref: ${target}\n`),
          reflog: (name) =>
            Effect.tryPromise({
              try: async () => {
                const target = path.join(root, "logs", name);
                if (!existsSync(target)) return [] as ReflogEntry[];
                const zero = "0".repeat(40);
                const lines: string[] = (await fs.readFile(target, "utf8")).split("\n");
                return lines
                  .filter((line: string) => line.length > 0)
                  .map((line: string): ReflogEntry => {
                    const [values = "", message = ""] = line.split("\t");
                    const [from = zero, to = zero, at = ""] = values.split(" ");
                    return {
                      from: from === zero ? null : (from as Oid),
                      to: to === zero ? null : (to as Oid),
                      at: new Date(at),
                      message,
                    };
                  });
              },
              catch: failure("reflog", name),
            }),
        }),
      );
    }),
  );

/** Both stores over one directory. */
export const stores = (root: string) =>
  Layer.mergeAll(objectStore(root), refStore(root)).pipe(Layer.provideMerge(packStore(root)));
