/** Durable Object storage persistence for the derived search index. */
import { Effect, Layer } from "effect";

import { StorageFailure } from "./Error.ts";
import * as Search from "./Search.ts";
import type { PersistenceLimits } from "./Search.ts";

/**
 * One value per chunk, keyed under the repository. DO storage has no multi-key
 * atomic batch on this path, so ordering is the atomicity story: the manifest
 * value is written last and is the only key a reader trusts.
 */
export const durable = (
  storage: DurableObjectStorage,
  repo: string,
  limits?: PersistenceLimits,
) => {
  const root = `search-index-v3:${repo}`;
  return Layer.unwrap(
    Effect.gen(function* () {
      // Superseded v1/v2 keys are never read again; delete them once.
      yield* Effect.tryPromise({
        try: async () => {
          await storage.delete(`search-index-v1:${repo}`);
          const old = await storage.list({ prefix: `search-index-v2:${repo}:` });
          if (old.size > 0) await storage.delete([...old.keys()]);
        },
        catch: () => new StorageFailure({ operation: "search.clean", path: repo }),
      }).pipe(Effect.ignore);
      return Search.persistent({
        softLimitBytes: limits?.softLimitBytes ?? 10 * 1024 * 1024,
        hardLimitBytes: limits?.hardLimitBytes ?? 20 * 1024 * 1024,
        read: (name) =>
          Effect.tryPromise({
            try: async () => (await storage.get<Uint8Array>(`${root}:${name}`)) ?? null,
            catch: () => new StorageFailure({ operation: "search.read", path: `${root}:${name}` }),
          }).pipe(Effect.orElseSucceed(() => null)),
        write: (name, bytes) =>
          Effect.tryPromise({
            try: () => storage.put(`${root}:${name}`, bytes),
            catch: () => new StorageFailure({ operation: "search.write", path: `${root}:${name}` }),
          }).pipe(Effect.ignore),
        remove: (name) =>
          Effect.tryPromise({
            try: () => storage.delete(`${root}:${name}`),
            catch: () =>
              new StorageFailure({ operation: "search.remove", path: `${root}:${name}` }),
          }).pipe(Effect.ignore),
        list: Effect.tryPromise({
          try: async () => {
            const keys = await storage.list({ prefix: `${root}:` });
            return [...keys.keys()].map((key) => key.slice(root.length + 1));
          },
          catch: () => new StorageFailure({ operation: "search.list", path: root }),
        }).pipe(Effect.orElseSucceed((): ReadonlyArray<string> => [])),
      });
    }),
  );
};
