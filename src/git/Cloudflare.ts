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
 * The platform supplies persistence, but the backend still owns ordering:
 *
 *   - **atomicity** — `apply` uses an explicit synchronous SQLite transaction.
 *     Durable Object requests can interleave across external I/O; a whole
 *     asynchronous handler is not a transaction.
 *   - **durability** — SQLite uses the platform's persistence/output gates.
 *     R2 writes/deletes must settle before interruption returns, including
 *     both mutations of a pack/index pair. There is no transaction spanning
 *     R2 and SQLite, and a rejected native call can have an uncertain outcome.
 */
import { Effect, Layer, Option, Predicate, Schema, Stream } from "effect";

import { ObjectNotFound, StorageFailure } from "./Error.ts";
import * as Capacity from "./Capacity.cloudflare.ts";
import * as HeadCapacity from "./HeadCapacity.cloudflare.ts";
import * as MetadataCapacity from "./MetadataCapacity.cloudflare.ts";
import { hashObject } from "./Format.ts";
import { inflate as nativeInflate } from "./Inflate.zlib.ts";
import { packed, type PackHandle, PackStore } from "./Packed.ts";
import {
  checkHeadTarget,
  checkRefName,
  checkRefNames,
  isOid,
  ObjectStore,
  type ObjectType,
  type Oid,
  type ReflogEntry,
  RefStore,
  tracedRefStore,
  type RefUpdate,
  type RefUpdateResult,
  Storage,
} from "./Store.ts";

const failure = (operation: string, target: string) => (cause: unknown) =>
  cause instanceof StorageFailure ? cause : new StorageFailure({ operation, path: target, cause });

const StoredOid = Schema.String.pipe(Schema.refine(isOid));
const decodeOid = Schema.decodeUnknownResult(StoredOid);
const decodeRef = Schema.decodeUnknownResult(
  Schema.Struct({ name: Schema.String, oid: StoredOid }),
);
const decodeReflog = Schema.decodeUnknownResult(
  Schema.Struct({
    old_oid: Schema.NullOr(StoredOid),
    new_oid: Schema.NullOr(StoredOid),
    at: Schema.String,
    message: Schema.String,
  }),
);
const decodeHead = Schema.decodeUnknownEffect(
  Schema.String.check(Schema.makeFilter((value) => checkRefName(value) === null)),
);

const reportMutationFailure = Effect.fn("Cloudflare.reportMutationFailure")(function* (
  error: StorageFailure,
) {
  // Only closed categories cross the logging boundary. Paths, native causes,
  // and arbitrary operation strings can contain repository or credential data.
  switch (error.operation) {
    case "quota.bytes":
    case "quota.keys":
      return yield* Effect.logWarning("Git retained capacity exhausted", {
        component: "GitStorage",
        event: "quota_exceeded",
        resource: error.operation === "quota.bytes" ? "bytes" : "keys",
      });
    case "quota.reservation":
      return yield* Effect.logWarning(
        "Git storage reserved; retry or reconcile after stopping writers",
        { component: "GitStorage", event: "quota_reserved" },
      );
    case "quota.metadata.head":
      return yield* Effect.logWarning("Git HEAD capacity exhausted", {
        component: "GitStorage",
        event: "quota_exceeded",
        resource: "head_bytes",
      });
    case "quota.metadata.bytes":
    case "quota.metadata.rows":
      return yield* Effect.logWarning("Git ref metadata capacity exhausted", {
        component: "GitStorage",
        event: "quota_exceeded",
        resource: error.operation === "quota.metadata.bytes" ? "metadata_bytes" : "metadata_rows",
      });
    case "quota.inspect":
    case "quota.inventory":
    case "quota.metadata.accounting":
      return yield* Effect.logError("Git accounting requires offline reconciliation", {
        component: "GitStorage",
        event: "quota_reconciliation_required",
      });
    default:
      return yield* Effect.logError("Git mutation failed; inspect storage and reservations", {
        component: "GitStorage",
        event: "mutation_failed",
      });
  }
});

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
        }).pipe(Effect.filterOrFail(Predicate.isNotNull, () => new ObjectNotFound({ oid })));

      const typeOf = (object: R2Object, oid: Oid) => {
        const type = object.customMetadata?.["type"];
        return type === "blob" || type === "tree" || type === "commit" || type === "tag"
          ? // Stated as `ObjectType` so inference does not widen the narrowed
            // literals back to `string`.
            Effect.succeed<ObjectType>(type)
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
              // The element type is stated up front because workerd leaves R2
              // bodies untyped; what comes out of a body is always bytes.
              Stream.fromReadableStream<Uint8Array, StorageFailure>({
                evaluate: () => object.body,
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
                bucket.put(key(oid), object.data, {
                  customMetadata: { type: object.type },
                }),
              catch: failure("write", key(oid)),
            }).pipe(Effect.tapError(reportMutationFailure), Effect.uninterruptible);

            return oid;
          }),
        has: (oid) => head(oid).pipe(Effect.map((object) => object !== null)),
        delete: (oid) =>
          Effect.tryPromise({
            try: () => bucket.delete(key(oid)),
            catch: failure("delete", key(oid)),
          }).pipe(Effect.tapError(reportMutationFailure), Effect.uninterruptible),
        list: Stream.paginate<string | undefined, Oid, StorageFailure>(undefined, (cursor) => {
          const options: R2ListOptions = { prefix: `${repo}/objects/` };
          if (cursor !== undefined) options.cursor = cursor;
          return Effect.tryPromise({
            try: () => bucket.list(options),
            catch: failure("list", repo),
          }).pipe(
            Effect.map(
              (page) =>
                [
                  // Every key under the prefix ends in the oid it was written
                  // under; anything else is not an object and is not listed.
                  page.objects
                    .map((object) => object.key.slice(object.key.lastIndexOf("/") + 1))
                    .filter(isOid),
                  page.truncated ? Option.some(page.cursor) : Option.none<string>(),
                ] as const,
            ),
          );
        }),
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

  /**
   * The handles, remembered between lookups.
   *
   * `packed`'s `locate` consults the pack list once per object read, and
   * a listing here costs a bucket list plus an index download per pack —
   * per object, that turned one clone of a packed repository into
   * thousands of listings of the same unchanged directory. The Durable
   * Object is this repository's single writer, so the one store instance
   * it holds sees every `write` and `delete` that could change the answer:
   * invalidating on those is real invalidation, not hope. (The stateless
   * read Worker builds a fresh store per request, so its memo is naturally
   * request-scoped — packs listed once per clone, never staleable.)
   */
  let remembered: Promise<PackHandle[]> | null = null;

  const listNow = async (): Promise<PackHandle[]> => {
    const handles: PackHandle[] = [];
    let cursor: string | undefined;

    do {
      const options: R2ListOptions = { prefix };
      if (cursor !== undefined) options.cursor = cursor;
      const page = await bucket.list(options);
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
  };

  return {
    // workerd carries `node:zlib` under `nodejs_compat`, which this Worker
    // already needs for `server/Protocol.ts`. Reading a pack object at a
    // bit at a time is not something to do on request time here either.
    inflate: nativeInflate,
    list: Effect.tryPromise({
      try: () => {
        // A failed listing is not remembered: the promise is cleared so the
        // next lookup asks the bucket again instead of replaying the fault.
        remembered ??= listNow().catch((cause: unknown) => {
          remembered = null;
          throw cause;
        });
        return remembered;
      },
      catch: failure("packs.list", prefix),
    }),

    write: ({ index, name, pack }) =>
      Effect.tryPromise({
        try: async () => {
          // Pack before index, so a reader never sees an index pointing into
          // bytes that are not there yet.
          await bucket.put(`${prefix}${name}.pack`, pack);
          await bucket.put(`${prefix}${name}.idx`, index);
          remembered = null;
        },
        catch: failure("packs.write", prefix),
      }).pipe(Effect.tapError(reportMutationFailure), Effect.uninterruptible),

    delete: (name) =>
      Effect.tryPromise({
        try: async () => {
          // Index first: it is what makes the pack visible.
          await bucket.delete(`${prefix}${name}.idx`);
          await bucket.delete(`${prefix}${name}.pack`);
          remembered = null;
        },
        catch: failure("packs.delete", prefix),
      }).pipe(Effect.tapError(reportMutationFailure), Effect.uninterruptible),
  };
};

/**
 * Refs in DO SQLite.
 *
 * One row per ref. `apply` compares and mutates refs and reflogs in one
 * synchronous SQLite transaction, so a later SQL failure rolls back earlier
 * writes even when Effect catches the exception before the DO handler returns.
 */
export const refStore = (
  storage: DurableObjectStorage,
  repo: string,
  metadata: MetadataCapacity.Limits = MetadataCapacity.defaults,
  headBytes = HeadCapacity.defaults,
) =>
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

      const corrupt = (field: string) =>
        new StorageFailure({ operation: `decode.${field}`, path: repo });
      const storedOid = (value: SqlStorageValue, field: string): Oid => {
        const decoded = decodeOid(value);
        if (decoded._tag === "Failure") throw corrupt(field);
        return decoded.success;
      };
      const readSync = (name: string): Oid | null => {
        const rows = sql
          .exec<{ oid: string }>(`SELECT oid FROM refs WHERE repo = ? AND name = ?`, repo, name)
          .toArray();
        const row = rows[0];
        return row === undefined ? null : storedOid(row.oid, "refs");
      };

      const read = (name: string) =>
        Effect.try({ try: () => readSync(name), catch: failure("read", name) });

      const headKey = `HEAD:${repo}`;
      sql.exec(
        `CREATE TABLE IF NOT EXISTS shallow (repo TEXT NOT NULL, oid TEXT NOT NULL, PRIMARY KEY (repo, oid))`,
      );
      const storedHead = Effect.tryPromise({
        try: () => storage.get<unknown>(headKey),
        catch: failure("read", headKey),
      }).pipe(
        Effect.flatMap((value) =>
          value === undefined
            ? Effect.succeed("refs/heads/main")
            : decodeHead(value).pipe(Effect.mapError(() => corrupt("head"))),
        ),
      );
      const boundedHead = (target: string) =>
        Effect.try({
          try: () => {
            if (
              target.length > headBytes ||
              new TextEncoder().encode(target).byteLength > headBytes
            )
              throw new StorageFailure({ operation: "quota.metadata.head", path: repo });
            return target;
          },
          catch: failure("head.capacity", repo),
        }).pipe(Effect.tapError(reportMutationFailure));
      const head = storedHead.pipe(Effect.flatMap(boundedHead));
      const shallowRows = () =>
        sql.exec<{ oid: string }>("SELECT oid FROM shallow WHERE repo = ?", repo);
      const shallowSync = () =>
        new Set(
          shallowRows()
            .toArray()
            .map((row) => storedOid(row.oid, "shallow")),
        );

      const capacity = MetadataCapacity.accounting(sql, repo, metadata);

      return RefStore.of(
        tracedRefStore("Cloudflare", {
          shallow: Effect.try({
            try: shallowSync,
            catch: failure("read", `shallow:${repo}`),
          }),
          updateShallow: ({ add, remove }) =>
            Effect.try({
              try: () =>
                storage.transactionSync(() => {
                  capacity.prepare();
                  // Validate the existing inventory without allocating another Set.
                  for (const row of shallowRows()) storedOid(row.oid, "shallow");
                  for (const oid of remove)
                    sql.exec("DELETE FROM shallow WHERE repo = ? AND oid = ?", repo, oid);
                  for (const oid of add) {
                    sql.exec("INSERT OR IGNORE INTO shallow (repo, oid) VALUES (?, ?)", repo, oid);
                    capacity.check();
                  }
                  capacity.check();
                }),
              catch: failure("write", `shallow:${repo}`),
            }).pipe(Effect.tapError(reportMutationFailure)),
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
                  .map((row) => {
                    const decoded = decodeRef(row);
                    if (decoded._tag === "Failure") throw corrupt("refs");
                    return [decoded.success.name, decoded.success.oid] as const;
                  })
                  .filter(([name]) => prefix === undefined || name.startsWith(prefix)),
              catch: failure("list", repo),
            }),
          apply: (updates, options) =>
            Effect.gen(function* () {
              yield* checkRefNames(updates);

              return yield* Effect.try({
                try: () =>
                  storage.transactionSync(() => {
                    capacity.prepare();
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
                      capacity.check();
                    }

                    capacity.check();
                    return results;
                  }),
                catch: failure("apply", repo),
              }).pipe(Effect.tapError(reportMutationFailure));
            }),
          head,
          setHead: Effect.fn("Cloudflare.RefStore.setHead")((target) =>
            checkHeadTarget(target).pipe(
              Effect.andThen(boundedHead(target)),
              Effect.andThen(storedHead),
              Effect.andThen(
                Effect.tryPromise({
                  try: () => storage.put(headKey, target),
                  catch: failure("write", headKey),
                }),
              ),
            ),
          ),
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
                  .map((row): ReflogEntry => {
                    const decoded = decodeReflog(row);
                    if (decoded._tag === "Failure") throw corrupt("reflog");
                    const value = decoded.success;
                    const at = new Date(value.at);
                    if (!Number.isFinite(at.getTime()) || at.toISOString() !== value.at)
                      throw corrupt("reflog");
                    return {
                      from: value.old_oid,
                      to: value.new_oid,
                      at,
                      message: value.message,
                    };
                  }),
              catch: failure("reflog", name),
            }),
          logged: Effect.try({
            try: () =>
              sql
                .exec<{ name: string }>(`SELECT DISTINCT name FROM reflog WHERE repo = ?`, repo)
                .toArray()
                .map((row) => row.name),
            catch: failure("reflog.list", repo),
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
  readonly capacity?: Capacity.Limits;
  readonly metadata?: MetadataCapacity.Limits;
  readonly headBytes?: number;
}) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const capacity = options.capacity ?? (yield* Capacity.configuration().pipe(Effect.orDie));
      const metadata =
        options.metadata ?? (yield* MetadataCapacity.configuration().pipe(Effect.orDie));
      const headBytes = options.headBytes ?? (yield* HeadCapacity.configuration.pipe(Effect.orDie));
      const bucket = Capacity.bounded(options.bucket, options.storage, options.repo, capacity);
      return Layer.mergeAll(
        objectStore(bucket, options.repo),
        refStore(options.storage, options.repo, metadata, headBytes),
        // The bucket prefix, which is what actually separates two repositories
        // here — an origin and its mirror share everything else, the genesis bytes
        // included; see `Storage`.
        Layer.succeed(Storage)(options.repo),
      ).pipe(Layer.provideMerge(packStore(bucket, options.repo)));
    }),
  );
