/** Node filesystem persistence for the derived search index. */
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { StorageFailure } from "./Error.ts";
import * as Search from "./Search.ts";
import type { Oid } from "./Store.ts";

const failure = (operation: string, target: string) =>
  new StorageFailure({ operation, path: target });

/** One atomic snapshot per bare repository; it is never Git object state. */
export const file = (directory: string) =>
  Layer.effect(
    Search.SearchIndex,
    Effect.gen(function* () {
      const target = path.join(directory, "search-index-v1.json");
      const temporary = `${target}.tmp`;
      const bytes = yield* Effect.tryPromise({
        try: () => fs.readFile(target),
        catch: () => failure("search.read", target),
      }).pipe(Effect.orElseSucceed(() => null));
      const index = bytes === null ? new Search.BlobIndex() : Search.BlobIndex.restore(bytes);
      const current = index ?? new Search.BlobIndex();
      const checkpoint = () =>
        Effect.tryPromise({
          try: async () => {
            await fs.writeFile(temporary, current.snapshot());
            await fs.rename(temporary, target);
          },
          catch: () => failure("search.write", target),
        }).pipe(Effect.ignore);
      let dirty = false;
      const observe = Effect.fn("Node.SearchIndex.observe")((oid: Oid, data: Uint8Array) =>
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
