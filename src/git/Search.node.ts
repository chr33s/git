/** Node filesystem persistence for the derived search index. */
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { Effect } from "effect";

import { StorageFailure } from "./Error.ts";
import * as Search from "./Search.ts";

const failure = (operation: string, target: string) =>
  new StorageFailure({ operation, path: target });

/**
 * Chunks under one directory beside the bare repository; never Git object
 * state. Soft limit warns, hard limit keeps the index in memory only — both
 * are initial values to be tuned by `bench:search`, not measured truths yet.
 */
export const file = (directory: string) => {
  const root = path.join(directory, "search-index-v3");
  return Search.persistent({
    softLimitBytes: 100 * 1024 * 1024,
    hardLimitBytes: 250 * 1024 * 1024,
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
  });
};
