/**
 * In-memory stores.
 *
 * As a layer it is a one-line swap at the edge of a test, and everything under
 * it — `Repository`, and later the HTTP handlers — is the same code that runs
 * on Workers.
 *
 * `RefStore.apply` here is a real implementation of the atomic contract, not a
 * stub: the checks and the writes happen in one synchronous pass, and under
 * `atomic` a single mismatch discards the batch.
 */
import { Effect, Layer, Stream } from "effect";
import { Invalid, ObjectNotFound } from "./Error.ts";
import { hashObject } from "./Format.ts";
import {
  ObjectStore,
  type Oid,
  type RawObject,
  type ReflogEntry,
  RefStore,
  tracedRefStore,
  type RefUpdate,
  type RefUpdateResult,
} from "./Store.ts";
import { bufferSource } from "./PackFile.ts";
import { packed, PackStore } from "./Packed.ts";

export const packStore = Layer.sync(PackStore, () => memoryPacks());

export const objectStore = Layer.effect(
  ObjectStore,
  Effect.gen(function* () {
    // Taken from context rather than constructed here: the reader and a
    // repack have to be looking at the same packs, and an in-memory store
    // built twice is two different repositories.
    const packs = yield* PackStore;
    const objects = new Map<Oid, RawObject>();

    const loose: ObjectStore["Service"] = {
      read: (oid) => {
        const object = objects.get(oid);
        return object === undefined
          ? Effect.fail(new ObjectNotFound({ oid }))
          : Effect.succeed({ type: object.type, data: new Uint8Array(object.data) });
      },
      readStream: (oid) => {
        const object = objects.get(oid);
        return object === undefined
          ? Effect.fail(new ObjectNotFound({ oid }))
          : Effect.succeed(Stream.fromIterable([new Uint8Array(object.data)]));
      },
      write: (object) =>
        hashObject(object).pipe(
          Effect.tap((oid) =>
            Effect.sync(() => {
              objects.set(oid, { type: object.type, data: new Uint8Array(object.data) });
            }),
          ),
        ),
      has: (oid) => Effect.sync(() => objects.has(oid)),
      delete: (oid) =>
        Effect.sync(() => {
          objects.delete(oid);
        }),
      list: Stream.suspend(() => Stream.fromIterable([...objects.keys()])),
    };

    return ObjectStore.of(packed(loose, packs, "Memory"));
  }),
);

/** Packs held as bytes — what the contract suite exercises the read path with. */
export const memoryPacks = (): PackStore["Service"] => {
  const packs = new Map<string, { pack: Uint8Array; index: Uint8Array }>();

  return {
    list: Effect.sync(() =>
      [...packs.entries()].map(([name, stored]) => ({
        name,
        index: stored.index,
        source: bufferSource(stored.pack),
      })),
    ),
    write: ({ index, name, pack }) =>
      Effect.sync(() => {
        packs.set(name, { pack, index });
      }),
    delete: (name) =>
      Effect.sync(() => {
        packs.delete(name);
      }),
  };
};

export const refStore = Layer.effect(
  RefStore,
  Effect.sync(() => {
    const refs = new Map<string, Oid>();
    const reflogs = new Map<string, ReflogEntry[]>();
    let head = "refs/heads/main";

    const read = (name: string) => refs.get(name) ?? null;

    const resolve = (name: string): Oid | null => {
      // Bounded so a `HEAD -> HEAD` loop fails instead of hanging.
      let current = name;
      for (let depth = 0; depth < 8; depth++) {
        if (current === "HEAD") {
          current = head;
          continue;
        }
        return read(current);
      }
      return null;
    };

    const log = (update: RefUpdate, from: Oid | null, at: Date) => {
      const entries = reflogs.get(update.name) ?? [];
      entries.push({
        from,
        to: update.value,
        at,
        message: update.reason ?? "update",
      });
      reflogs.set(update.name, entries);
    };

    return RefStore.of(
      tracedRefStore("Memory", {
        read: (name) => Effect.sync(() => read(name)),
        resolve: (name) => Effect.sync(() => resolve(name)),
        list: (prefix) =>
          Effect.sync(() =>
            [...refs.entries()]
              .filter(([name]) => prefix === undefined || name.startsWith(prefix))
              .map(([name, oid]) => [name, oid] as const),
          ),
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
            const applied: Array<{ update: RefUpdate; from: Oid | null }> = [];

            for (const update of updates) {
              const actual = read(update.name);
              const matches = update.expected === undefined || update.expected === actual;
              results.push({
                name: update.name,
                applied: matches,
                current: matches ? update.value : actual,
              });
              if (matches) applied.push({ update, from: actual });
            }

            const rejected = results.some((result) => !result.applied);
            if (options?.atomic === true && rejected) {
              // Nothing was written yet, so "rolling back" is reporting the
              // batch as unapplied — the whole point of doing the checks first.
              return results.map((result) => ({
                name: result.name,
                applied: false,
                current: read(result.name),
              }));
            }

            for (const { from, update } of applied) {
              if (update.value === null) refs.delete(update.name);
              else refs.set(update.name, update.value);
              log(update, from, at);
            }

            return results;
          }),
        head: Effect.sync(() => head),
        setHead: (target) =>
          Effect.sync(() => {
            head = target;
          }),
        reflog: (name) => Effect.sync(() => reflogs.get(name) ?? []),
      }),
    );
  }),
);

/** Both stores, for tests and for the browser/CLI until their backends land. */
export const stores = Layer.mergeAll(objectStore, refStore).pipe(Layer.provideMerge(packStore));
