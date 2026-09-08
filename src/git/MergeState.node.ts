import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Effect, Layer } from "effect";
import { Invalid, StorageFailure } from "./Error.ts";
import { MergeState } from "./MergeState.ts";
import { isOid, type Oid } from "./Store.ts";

export const mergeState = (gitDirectory: string): Layer.Layer<MergeState> =>
  Layer.effect(
    MergeState,
    Effect.sync(() => {
      const location = path.join(gitDirectory, "MERGE_HEAD");
      const heads = Effect.gen(function* () {
        const text = yield* Effect.tryPromise({
          try: async () => {
            try {
              return await fs.readFile(location, "utf8");
            } catch (cause) {
              if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null;
              throw cause;
            }
          },
          catch: (cause) => new StorageFailure({ operation: "merge.read", path: location, cause }),
        });
        if (text === null) return [];
        const parents: Oid[] = [];
        for (const line of text.trim().split("\n")) {
          if (!isOid(line))
            return yield* new Invalid({
              field: "MERGE_HEAD",
              reason: "expected commit object IDs",
            });
          parents.push(line);
        }
        return parents;
      });
      const clear = Effect.tryPromise({
        try: async () => {
          for (const name of ["AUTO_MERGE", "MERGE_MSG", "MERGE_MODE", "MERGE_HEAD"]) {
            await fs.rm(path.join(gitDirectory, name), { force: true });
          }
        },
        catch: (cause) =>
          new StorageFailure({ operation: "merge.clear", path: gitDirectory, cause }),
      });
      return MergeState.of({ heads, clear });
    }),
  );
