/**
 * Cloudflare store implementations.
 *
 * Today `CloudflareStorage` (`src/server.storage.ts`, 709 lines) is one class
 * doing four jobs — object blobs in R2, refs and reflog in DO SQLite, LFS
 * metadata, webhook rows — behind a filesystem-shaped interface, so a ref read
 * is spelled `readFile(".git/refs/heads/main")` and then re-parsed.
 *
 * Sketch: one layer per port. The SQL stays hand-written (it is a handful of
 * statements against DO SQLite and it is the hot path), but it is scoped to
 * `RefStore` instead of leaking through a path-string API.
 */
import * as Cloudflare from "alchemy/Cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Stream } from "effect";
import { ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { hashObject } from "../git/Format.ts";
import { ObjectStore, type Oid, type RawObject, RefStore, type RefUpdate } from "../git/Store.ts";

/** Declared in the stack (`alchemy.run.ts`) and referenced by the binding here. */
export declare const Objects: Cloudflare.R2.Bucket;

const fail = (operation: string, path: string) => (cause: unknown) =>
  new StorageFailure({ operation, path, cause });

/**
 * The invocation context, captured once and handed to every method.
 *
 * This is the whole reason the ports can stay `R = never`. Alchemy's bindings
 * are `RuntimeContext`-coloured because they close over the workerd invocation;
 * capturing it here and providing it inward keeps that fact inside the adapter.
 *
 * The consequence, and it is a real one: this layer must be built *inside* the
 * invocation, not memoized across invocations, because a cached context would
 * hold a stale `ExecutionContext`. Layers are closures, so rebuilding is cheap
 * — but it is a deliberate choice, not an oversight.
 */
const captureRuntime = Effect.context<RuntimeContext | Cloudflare.DurableObjectState>();

type Runtime = Context.Context<RuntimeContext | Cloudflare.DurableObjectState>;
const provided =
  (runtime: Runtime) =>
  <A, E>(effect: Effect.Effect<A, E, RuntimeContext | Cloudflare.DurableObjectState>) =>
    Effect.provideContext(effect, runtime);

/**
 * Objects live in R2, keyed `<repo>/objects/<oid>`. Reads and writes are
 * streams end to end — alchemy's binding accepts a `Stream` directly and hands
 * one back, so a large blob never materializes in the isolate.
 */
export const objectStoreLayer = (repo: string) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const bucket = yield* Cloudflare.R2.ReadWriteBucket(Objects);
      const runtime = yield* captureRuntime;
      const run = provided(runtime);
      const key = (oid: Oid) => `${repo}/objects/${oid}`;

      const get = (oid: Oid) =>
        bucket.get(key(oid)).pipe(
          Effect.mapError(fail("read", key(oid))),
          Effect.flatMap((object) =>
            object === null ? Effect.fail(new ObjectNotFound({ oid })) : Effect.succeed(object),
          ),
        );

      return ObjectStore.of({
        read: (oid) =>
          run(get(oid)).pipe(
            Effect.flatMap((object) =>
              run(object.bytes()).pipe(Effect.mapError(fail("read", key(oid)))),
            ),
            Effect.map(decodeObject),
          ),
        readStream: (oid) =>
          run(get(oid)).pipe(
            Effect.map((object) =>
              object.body.pipe(
                Stream.mapError(fail("readStream", key(oid))),
                Stream.provideContext(runtime),
              ),
            ),
          ),
        write: (object) =>
          Effect.gen(function* () {
            const oid = yield* hashObject(object);
            yield* run(bucket.put(key(oid), encodeObject(object))).pipe(
              Effect.mapError(fail("write", key(oid))),
            );
            return oid;
          }),
        writeStream: (type, body) =>
          // The oid is not known until the last byte, so the object is
          // streamed to a staging key and promoted on completion. That is
          // also what makes a resumed push cheap.
          Effect.gen(function* () {
            const staging = `${repo}/staging/${crypto.randomUUID()}`;
            yield* run(bucket.put(staging, body)).pipe(
              Effect.mapError(fail("writeStream", staging)),
            );
            return yield* run(promoteStaged(staging, type));
          }),
        has: (oid) =>
          run(bucket.head(key(oid))).pipe(
            Effect.map((object) => object !== null),
            Effect.mapError(fail("has", key(oid))),
          ),
        delete: (oid) =>
          run(bucket.delete(key(oid))).pipe(
            Effect.mapError(fail("delete", key(oid))),
            Effect.asVoid,
          ),
        list: () =>
          Stream.unwrap(
            run(bucket.list({ prefix: `${repo}/objects/` })).pipe(
              Effect.map((page) => Stream.fromIterable(page.objects.map((o) => basename(o.key)))),
              Effect.mapError(fail("list", repo)),
            ),
          ),
      });
    }),
  );

/**
 * Refs in DO SQLite. The DO's input gate already serializes calls, so `apply`
 * is one transaction with a compare-and-swap per row — which is what makes
 * `atomic` a parameter instead of the second code path it is today
 * (`server.storage.ts:324`).
 */
export const refStoreLayer = (repo: string) =>
  Layer.effect(
    RefStore,
    Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      const storage = state.storage;
      const runtime = yield* captureRuntime;
      const run = provided(runtime);

      yield* run(
        storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS refs (
          repo TEXT NOT NULL,
          name TEXT NOT NULL,
          oid  TEXT NOT NULL,
          PRIMARY KEY (repo, name)
        )
      `),
      );

      const read = (name: string) =>
        run(
          storage.sql
            .exec<{ oid: string }>(`SELECT oid FROM refs WHERE repo = ? AND name = ?`, repo, name)
            .pipe(
              Effect.flatMap((cursor) => cursor.toArray()),
              Effect.map((rows) => (rows[0]?.oid ?? null) as Oid | null),
            ),
        );

      return RefStore.of({
        read,
        resolve: read,
        list: () =>
          run(
            storage.sql
              .exec<{ name: string; oid: string }>(
                `SELECT name, oid FROM refs WHERE repo = ?`,
                repo,
              )
              .pipe(
                Effect.flatMap((cursor) => cursor.toArray()),
                Effect.map((rows) =>
                  rows.map((row) => [row.name, row.oid as Oid] as [string, Oid]),
                ),
              ),
          ),
        apply: (updates, options) =>
          // One transaction; every CAS is checked inside it, and a single
          // mismatch rolls the whole batch back when `atomic` is set.
          run(
            storage.transaction(() =>
              Effect.forEach(updates, (update) =>
                Effect.gen(function* () {
                  const actual = yield* read(update.name);
                  const matches = update.expected === undefined || update.expected === actual;
                  if (matches) yield* upsert(repo, update);
                  return {
                    name: update.name,
                    applied: matches,
                    current: matches ? update.value : actual,
                  };
                }),
              ).pipe(
                Effect.tap((results) =>
                  options?.atomic === true && results.some((result) => !result.applied)
                    ? rollback
                    : Effect.void,
                ),
              ),
            ),
          ),
        head: run(storage.get<string>("HEAD")).pipe(
          Effect.map((head) => head ?? "refs/heads/main"),
        ),
        setHead: (target) => run(storage.put("HEAD", target)),
        reflog: () => Effect.succeed([]),
      });
    }),
  );

declare const decodeObject: (bytes: Uint8Array) => RawObject;
declare const encodeObject: (object: RawObject) => Uint8Array;
declare const promoteStaged: (
  key: string,
  type: import("../git/Store.ts").ObjectType,
) => Effect.Effect<Oid, StorageFailure, RuntimeContext>;
declare const upsert: (repo: string, update: RefUpdate) => Effect.Effect<void>;
declare const rollback: Effect.Effect<never>;
declare const basename: (key: string) => Oid;
