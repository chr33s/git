/** Durable Object storage persistence for the derived search index. */
import { Effect } from "effect";

import { StorageFailure } from "./Error.ts";
import * as Search from "./Search.ts";

/**
 * One value per chunk, keyed under the repository. DO storage has no multi-key
 * atomic batch on this path, so ordering is the atomicity story: the manifest
 * value is written last and is the only key a reader trusts.
 */
export const durable = (storage: DurableObjectStorage, repo: string) => {
  const root = `search-index-v3:${repo}`;
  return Search.persistent({
    softLimitBytes: 10 * 1024 * 1024,
    hardLimitBytes: 20 * 1024 * 1024,
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
  });
};
