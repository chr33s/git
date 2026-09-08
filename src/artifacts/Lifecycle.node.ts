/** Cross-process reservations for complete local repository operations. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { ArtifactsError } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import { Effect } from "effect";

import type { RepoStores } from "./Namespace.ts";

const busyCodes = new Set(["IMPORT_IN_PROGRESS", "FORK_IN_PROGRESS", "PRECONDITION_FAILED"]);

export const reserve = (root: string): RepoStores["Service"]["reserve"] =>
  Effect.fn("Artifacts.Lifecycle.reserve")(function* <A, E>(
    names: ReadonlyArray<string>,
    code: string,
    use: Effect.Effect<A, E>,
  ) {
    for (const name of [...new Set(names)].sort()) {
      const lock = path.join(root, ".operations", `${Buffer.from(name).toString("hex")}.lock`);
      yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: async () => {
            await fs.mkdir(path.dirname(lock), { recursive: true });
            const handle = await fs.open(lock, "wx", 0o600);
            try {
              await handle.writeFile(code);
              await handle.close();
            } catch (cause) {
              await handle.close().catch(() => undefined);
              await fs.rm(lock, { force: true }).catch(() => undefined);
              throw cause;
            }
          },
          catch: (cause) =>
            new ArtifactsError({
              message: `INTERNAL_ERROR: could not reserve repo '${name}': ${String(cause)}`,
              cause: cause instanceof Error ? cause : new Error(String(cause)),
            }),
        }).pipe(
          Effect.catchTag("ArtifactsError", (error) => {
            const cause = error.cause;
            if (!(cause instanceof Error && "code" in cause && cause.code === "EEXIST")) {
              return Effect.fail(error);
            }
            return Effect.promise(() => fs.readFile(lock, "utf8").catch(() => "")).pipe(
              Effect.flatMap((held) =>
                Effect.fail(
                  new ArtifactsError({
                    message: `${busyCodes.has(held) ? held : "PRECONDITION_FAILED"}: repo '${name}' is busy`,
                    cause,
                  }),
                ),
              ),
            );
          }),
        ),
        () => Effect.promise(() => fs.rm(lock, { force: true }).catch(() => undefined)),
      );
    }
    return yield* use;
  }, Effect.scoped);
