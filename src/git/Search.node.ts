/** Node filesystem persistence for the derived search index. */
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { StorageFailure } from "./Error.ts";
import * as Search from "./Search.ts";
import type { PersistenceLimits } from "./Search.ts";

const failure = (operation: string, target: string) =>
  new StorageFailure({ operation, path: target });

export type { PersistenceLimits };

/**
 * Chunks under one directory beside the bare repository; never Git object
 * state. Soft limit warns, hard limit keeps the index in memory only — both
 * are initial values to be tuned by `bench:search`, not measured truths yet.
 */
export const file = (directory: string, limits?: PersistenceLimits) => {
  const root = path.join(directory, "search-index-v3");
  return Layer.unwrap(
    Effect.gen(function* () {
      // One retirement of superseded formats; missing is the common case.
      yield* Effect.tryPromise({
        try: async () => {
          await fs.rm(path.join(directory, "search-index-v1.json"), { force: true });
          await fs.rm(path.join(directory, "search-index-v1.json.tmp"), { force: true });
          await fs.rm(path.join(directory, "search-index-v2"), { force: true, recursive: true });
        },
        catch: () => failure("search.clean", directory),
      }).pipe(Effect.ignore);
      return Search.persistent({
        softLimitBytes: limits?.softLimitBytes ?? 100 * 1024 * 1024,
        hardLimitBytes: limits?.hardLimitBytes ?? 250 * 1024 * 1024,
        read: (name) =>
          Effect.tryPromise({
            try: async () => new Uint8Array(await fs.readFile(path.join(root, name))),
            catch: () => failure("search.read", path.join(root, name)),
          }).pipe(Effect.orElseSucceed(() => null)),
        write: (name, bytes) =>
          Effect.tryPromise({
            try: async () => {
              await fs.mkdir(root, { recursive: true });
              const target = path.join(root, name);
              // The manifest publishes the checkpoint; it alone needs atomic rename.
              if (name === "manifest.json") {
                const temporary = `${target}.tmp`;
                await fs.writeFile(temporary, bytes);
                await fs.rename(temporary, target);
              } else {
                await fs.writeFile(target, bytes);
              }
            },
            catch: () => failure("search.write", path.join(root, name)),
          }).pipe(Effect.ignore),
        remove: (name) =>
          Effect.tryPromise({
            try: () => fs.rm(path.join(root, name), { force: true }),
            catch: () => failure("search.remove", path.join(root, name)),
          }).pipe(Effect.ignore),
        list: Effect.tryPromise({
          try: async () => (await fs.readdir(root)).filter((name) => !name.endsWith(".tmp")),
          catch: () => failure("search.list", root),
        }).pipe(Effect.orElseSucceed((): ReadonlyArray<string> => [])),
      });
    }),
  );
};
