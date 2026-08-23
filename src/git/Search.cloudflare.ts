/** Durable Object storage persistence for the derived search index. */
import { Effect, Layer } from "effect";

import { StorageFailure } from "./Error.ts";
import * as Search from "./Search.ts";
import type { Oid } from "./Store.ts";

/** One atomic Durable Object value per repository; missing/corrupt means cold. */
export const durable = (storage: DurableObjectStorage, repo: string) =>
  Layer.effect(
    Search.SearchIndex,
    Effect.gen(function* () {
      const key = `search-index-v1:${repo}`;
      const saved = yield* Effect.tryPromise({
        try: () => storage.get<Uint8Array>(key),
        catch: () => new StorageFailure({ operation: "search.read", path: key }),
      }).pipe(Effect.orElseSucceed(() => undefined));
      const index = saved === undefined ? new Search.BlobIndex() : Search.BlobIndex.restore(saved);
      const current = index ?? new Search.BlobIndex();
      const checkpoint = () =>
        Effect.tryPromise({
          try: () => storage.put(key, current.snapshot()),
          catch: () => new StorageFailure({ operation: "search.write", path: key }),
        }).pipe(Effect.ignore);
      let dirty = false;
      const observe = Effect.fn("Cloudflare.SearchIndex.observe")((oid: Oid, data: Uint8Array) =>
        Effect.sync(() => {
          const known = current.get(oid);
          const blob = current.observe(oid, data);
          if (known === undefined) dirty = true;
          return blob;
        }),
      );
      const forget = (oids: ReadonlyArray<Oid>) =>
        Effect.sync(() => {
          for (const oid of oids) dirty = current.forget(oid) || dirty;
        });
      const flush = Effect.suspend(() => {
        if (!dirty) return Effect.void;
        dirty = false;
        return checkpoint();
      });
      return Search.SearchIndex.of({ index: current, observe, forget, flush });
    }),
  );
