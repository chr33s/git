/**
 * Cloudflare backend: objects in R2, refs in Durable Object SQLite.
 *
 * The third implementation of the ports, and the one that runs in production.
 * It passes the same `Store.contract.ts` suite as the in-memory and filesystem
 * backends — see `src/git/Cloudflare.integration.ts`, which runs that suite
 * inside workerd against real bindings.
 *
 * Refs are rows and objects are keys, with no `.git/refs/heads/main` path
 * string in the middle.
 *
 * Two properties come from the platform rather than from this code:
 *
 *   - **atomicity** — a Durable Object processes one request at a time, so the
 *     check-then-write in `apply` cannot interleave with another caller's. The
 *     filesystem backend has to buy the same guarantee with `rename(2)`;
 *   - **durability** — `storage.sql` writes are committed when the handler
 *     returns, so a batch that throws part-way is rolled back by the runtime.
 */
import { Effect, Layer, Option, Stream } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "./Error.ts";
import { hashObject } from "./Format.ts";
import { packed, type PackHandle, PackStore } from "./Packed.ts";
import {
  ObjectStore,
  type ObjectType,
  type Oid,
  type ReflogEntry,
  RefStore,
  tracedRefStore,
  type RefUpdate,
  type RefUpdateResult,
} from "./Store.ts";

const failure = (operation: string, target: string) => (cause: unknown) =>
  new StorageFailure({ operation, path: target, cause });

/**
 * Objects in R2, keyed `<repo>/objects/<oid>`.
 *
 * Unlike the loose-object files on disk, these are stored unframed with the
 * type in R2 custom metadata: R2 already gives us the length, and skipping the
 * `<type> <len>\0` prefix means a blob read can be served straight from the
 * object body without re-parsing a header.
 */
export const packStore = (bucket: R2Bucket, repo: string) =>
  Layer.sync(PackStore, () => r2Packs(bucket, repo));

export const objectStore = (bucket: R2Bucket, repo: string) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const packs = yield* PackStore;
      const key = (oid: Oid) => `${repo}/objects/${oid}`;

      const head = (oid: Oid) =>
        Effect.tryPromise({
          try: () => bucket.head(key(oid)),
          catch: failure("head", key(oid)),
        });

      const get = (oid: Oid) =>
        Effect.tryPromise({
          try: () => bucket.get(key(oid)),
          catch: failure("read", key(oid)),
        }).pipe(
          Effect.flatMap((object) =>
            object === null ? Effect.fail(new ObjectNotFound({ oid })) : Effect.succeed(object),
          ),
        );

      const typeOf = (object: R2Object, oid: Oid) => {
        const type = object.customMetadata?.["type"];
        return type === "blob" || type === "tree" || type === "commit" || type === "tag"
          ? Effect.succeed(type as ObjectType)
          : Effect.fail(new ObjectNotFound({ oid }));
      };

      const loose: ObjectStore["Service"] = {
        read: (oid) =>
          get(oid).pipe(
            Effect.flatMap((object) =>
              Effect.all([
                typeOf(object, oid),
                Effect.tryPromise({
                  try: () => object.arrayBuffer(),
                  catch: failure("read", key(oid)),
                }),
              ]),
            ),
            Effect.map(([type, buffer]) => ({ type, data: new Uint8Array(buffer) })),
          ),
        readStream: (oid) =>
          get(oid).pipe(
            Effect.map((object) =>
              Stream.fromReadableStream({
                evaluate: () => object.body as ReadableStream<Uint8Array>,
                onError: failure("readStream", key(oid)),
              }),
            ),
          ),
        write: (object) =>
          Effect.gen(function* () {
            const oid = yield* hashObject(object);

            // Content-addressed: if it is already there, the bytes are identical
            // and the PUT is pure cost.
            const existing = yield* head(oid);
            if (existing !== null) return oid;

            yield* Effect.tryPromise({
              try: () =>
                bucket.put(key(oid), object.data as unknown as ArrayBuffer, {
                  customMetadata: { type: object.type },
                }),
              catch: failure("write", key(oid)),
            });

            return oid;
          }),
        has: (oid) => head(oid).pipe(Effect.map((object) => object !== null)),
        delete: (oid) =>
          Effect.tryPromise({
            try: () => bucket.delete(key(oid)),
            catch: failure("delete", key(oid)),
          }),
        list: Stream.paginate(undefined as string | undefined, (cursor) =>
          Effect.tryPromise({
            try: () =>
              bucket.list({
                prefix: `${repo}/objects/`,
                ...(cursor === undefined ? {} : { cursor }),
              }),
            catch: failure("list", repo),
          }).pipe(
            Effect.map(
              (page) =>
                [
                  page.objects.map(
                    (object) => object.key.slice(object.key.lastIndexOf("/") + 1) as Oid,
                  ),
                  page.truncated ? Option.some(page.cursor) : Option.none<string>(),
                ] as const,
            ),
          ),
        ),
      };

      return ObjectStore.of(packed(loose, packs, "Cloudflare"));
    }),
  );

/**
 * Packs in R2, read with range GETs.
 *
 * This is where random access pays for itself: pulling one small object out
 * of a pack costs a couple of ranged reads rather than the whole object, so a
 * repository whose history is a gigabyte still serves a single blob from a
 * Durable Object with 128 MiB.
 */
export const r2Packs = (bucket: R2Bucket, repo: string): PackStore["Service"] => {
  const prefix = `${repo}/pack/`;

  return {
    list: Effect.tryPromise({
      try: async () => {
        const handles: PackHandle[] = [];
        let cursor: string | undefined;

        do {
          const page = await bucket.list({ prefix, ...(cursor === undefined ? {} : { cursor }) });
          for (const entry of page.objects) {
            if (!entry.key.endsWith(".idx")) continue;
            const name = entry.key.slice(prefix.length, -4);

            const packKey = `${prefix}${name}.pack`;
            const packHead = await bucket.head(packKey);
            // An index whose pack is missing is a half-finished write; a
            // reader that failed on it would break the whole repository.
            if (packHead === null) continue;

            const index = await bucket.get(entry.key);
            if (index === null) continue;

            handles.push({
              name,
              index: new Uint8Array(await index.arrayBuffer()),
              source: {
                size: packHead.size,
                read: async (offset, length) => {
                  const ranged = await bucket.get(packKey, { range: { offset, length } });
                  if (ranged === null) return new Uint8Array(0);
                  return new Uint8Array(await ranged.arrayBuffer());
                },
              },
            });
          }
          cursor = page.truncated ? page.cursor : undefined;
        } while (cursor !== undefined);

        return handles;
      },
      catch: failure("packs.list", prefix),
    }),

    write: ({ index, name, pack }) =>
      Effect.tryPromise({
        try: async () => {
          // Pack before index, so a reader never sees an index pointing into
          // bytes that are not there yet.
          await bucket.put(`${prefix}${name}.pack`, pack as unknown as ArrayBuffer);
          await bucket.put(`${prefix}${name}.idx`, index as unknown as ArrayBuffer);
        },
        catch: failure("packs.write", prefix),
      }),

    delete: (name) =>
      Effect.tryPromise({
        try: async () => {
          // Index first: it is what makes the pack visible.
          await bucket.delete(`${prefix}${name}.idx`);
          await bucket.delete(`${prefix}${name}.pack`);
        },
        catch: failure("packs.delete", prefix),
      }),
  };
};

/**
 * Refs in DO SQLite.
 *
 * One table, one row per ref. The compare-and-swap in `apply` is safe without
 * explicit locking because the Durable Object is single-threaded per instance —
 * that is the whole reason a repository maps onto a DO so neatly.
 */
export const refStore = (storage: DurableObjectStorage, repo: string) =>
  Layer.effect(
    RefStore,
    Effect.sync(() => {
      const sql = storage.sql;

      sql.exec(`
        CREATE TABLE IF NOT EXISTS refs (
          repo TEXT NOT NULL,
          name TEXT NOT NULL,
          oid  TEXT NOT NULL,
          PRIMARY KEY (repo, name)
        )
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS reflog (
          repo     TEXT NOT NULL,
          name     TEXT NOT NULL,
          old_oid  TEXT,
          new_oid  TEXT,
          at       TEXT NOT NULL,
          message  TEXT NOT NULL
        )
      `);

      const readSync = (name: string): Oid | null => {
        const rows = sql
          .exec<{ oid: string }>(`SELECT oid FROM refs WHERE repo = ? AND name = ?`, repo, name)
          .toArray();
        return (rows[0]?.oid ?? null) as Oid | null;
      };

      const read = (name: string) =>
        Effect.try({ try: () => readSync(name), catch: failure("read", name) });

      const headKey = `HEAD:${repo}`;
      const head = Effect.tryPromise({
        try: () => storage.get<string>(headKey),
        catch: failure("read", headKey),
      }).pipe(Effect.map((value) => value ?? "refs/heads/main"));

      return RefStore.of(
        tracedRefStore("Cloudflare", {
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
            Effect.try({
              try: () =>
                sql
                  .exec<{ name: string; oid: string }>(
                    `SELECT name, oid FROM refs WHERE repo = ? ORDER BY name`,
                    repo,
                  )
                  .toArray()
                  .filter((row) => prefix === undefined || row.name.startsWith(prefix))
                  .map((row) => [row.name, row.oid as Oid] as const),
              catch: failure("list", repo),
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

              return yield* Effect.try({
                try: () => {
                  const at = new Date().toISOString();
                  const results: RefUpdateResult[] = [];
                  const pending: Array<{ from: Oid | null; update: RefUpdate }> = [];

                  // Check the whole batch before writing any of it, so an atomic
                  // batch that fails leaves nothing behind.
                  for (const update of updates) {
                    const actual = readSync(update.name);
                    const matches = update.expected === undefined || update.expected === actual;
                    results.push({
                      name: update.name,
                      applied: matches,
                      current: matches ? update.value : actual,
                    });
                    if (matches) pending.push({ from: actual, update });
                  }

                  if (options?.atomic === true && results.some((result) => !result.applied)) {
                    return results.map((result) => ({
                      name: result.name,
                      applied: false,
                      current: readSync(result.name),
                    }));
                  }

                  for (const { from, update } of pending) {
                    if (update.value === null) {
                      sql.exec(`DELETE FROM refs WHERE repo = ? AND name = ?`, repo, update.name);
                    } else {
                      sql.exec(
                        `INSERT INTO refs (repo, name, oid) VALUES (?, ?, ?)
                       ON CONFLICT (repo, name) DO UPDATE SET oid = excluded.oid`,
                        repo,
                        update.name,
                        update.value,
                      );
                    }
                    sql.exec(
                      `INSERT INTO reflog (repo, name, old_oid, new_oid, at, message)
                     VALUES (?, ?, ?, ?, ?, ?)`,
                      repo,
                      update.name,
                      from,
                      update.value,
                      at,
                      update.reason ?? "update",
                    );
                  }

                  return results;
                },
                catch: failure("apply", repo),
              });
            }),
          head,
          setHead: (target) =>
            Effect.tryPromise({
              try: () => storage.put(headKey, target),
              catch: failure("write", headKey),
            }),
          reflog: (name) =>
            Effect.try({
              try: () =>
                sql
                  .exec<{
                    at: string;
                    message: string;
                    new_oid: string | null;
                    old_oid: string | null;
                  }>(
                    `SELECT old_oid, new_oid, at, message FROM reflog
                   WHERE repo = ? AND name = ? ORDER BY rowid`,
                    repo,
                    name,
                  )
                  .toArray()
                  .map((row): ReflogEntry => ({
                    from: row.old_oid as Oid | null,
                    to: row.new_oid as Oid | null,
                    at: new Date(row.at),
                    message: row.message,
                  })),
              catch: failure("reflog", name),
            }),
        }),
      );
    }),
  );

/** Both stores for one repository, from the bindings a Durable Object has. */
export const stores = (options: {
  readonly bucket: R2Bucket;
  readonly repo: string;
  readonly storage: DurableObjectStorage;
}) =>
  Layer.mergeAll(
    objectStore(options.bucket, options.repo),
    refStore(options.storage, options.repo),
  ).pipe(Layer.provideMerge(packStore(options.bucket, options.repo)));
