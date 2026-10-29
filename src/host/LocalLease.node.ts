/** Local exclusion owned by the OS, including after SIGKILL. Metadata is informational. */
import * as fs from "node:fs";
import * as path from "node:path";
import * as net from "node:net";
import { createHash } from "node:crypto";
import { Effect } from "effect";
import { StorageFailure } from "../git/Error.ts";

const storage = (operation: string, location: string, cause: unknown) =>
  new StorageFailure({ operation, path: location, cause });

export const withLock = Effect.fn("host.LocalLease.withLock")(function* <A, E, R>(
  location: string,
  effect: Effect.Effect<A, E, R>,
) {
  return yield* Effect.acquireUseRelease(
    Effect.gen(function* () {
      const canonical = yield* Effect.try({
        try: () => path.join(fs.realpathSync(path.dirname(location)), path.basename(location)),
        catch: (cause) => storage("lease.path", location, cause),
      });
      // One fixed port per canonical path: collisions cause contention, never concurrent owners.
      const port =
        16384 + (createHash("sha256").update(canonical).digest().readUInt16BE(0) % 16384);
      const server = net.createServer((socket) => socket.destroy());
      yield* Effect.tryPromise({
        try: () =>
          new Promise<void>((resolve, reject) => {
            const failed = (cause: Error) => reject(cause);
            server.once("error", failed);
            server.listen({ host: "127.0.0.1", port, exclusive: true }, () => {
              server.removeListener("error", failed);
              resolve();
            });
          }),
        catch: (cause) => storage("lease.acquire", location, cause),
      });
      return { server, port };
    }),
    ({ port }) =>
      Effect.acquireUseRelease(
        Effect.try({
          try: () =>
            fs.writeFileSync(location, JSON.stringify({ pid: process.pid, port }), { mode: 0o600 }),
          catch: (cause) => storage("lease.metadata", location, cause),
        }),
        () => effect,
        () => Effect.sync(() => fs.rmSync(location, { force: true })),
      ),
    ({ server }) =>
      Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
  );
});
