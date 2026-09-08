import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { serve } from "./Node.ts";

const close = (server: http.Server) =>
  new Promise<void>((resolve, reject) => {
    if (!server.listening) {
      resolve();
      return;
    }
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });

describe("Node host lifecycle", () => {
  for (const cleanupFails of [false, true]) {
    it.live(
      `closes initialized development middleware when the HTTP bind fails (cleanup fails: ${cleanupFails})`,
      () =>
        Effect.promise(async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "node-startup-"));
          const occupied = await serve({ root });
          const development = http.createServer();
          await new Promise<void>((resolve) => development.listen(0, "127.0.0.1", resolve));
          const cleanupFailure = new Error("middleware cleanup failed");
          try {
            await assert.rejects(
              serve({
                root,
                port: Number(new URL(occupied.url).port),
                development: async () => ({
                  handle: (_request, _response, next) => next(),
                  close: async () => {
                    await close(development);
                    if (cleanupFails) throw cleanupFailure;
                  },
                }),
              }),
              (error: Error) => {
                if (cleanupFails) {
                  assert.ok(error instanceof AggregateError);
                  assert.match(String(error.errors[0]), /EADDRINUSE/);
                  assert.equal(error.errors[1], cleanupFailure);
                  assert.equal(error.cause, error.errors[0]);
                } else {
                  assert.match(String(error), /EADDRINUSE/);
                }
                return true;
              },
            );
            assert.equal(
              development.listening,
              false,
              "failed startup left middleware resources open",
            );
          } finally {
            await close(development);
            await occupied.close();
            await fs.rm(root, { recursive: true, force: true });
          }
        }),
    );
  }

  it.live("closes the HTTP listener even when development shutdown fails", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "node-shutdown-"));
      let listener: http.Server | undefined;
      const failure = new Error("development shutdown failed");
      const host = await serve({
        root,
        development: async (server) => {
          listener = server;
          return {
            handle: (_request, _response, next) => next(),
            close: async () => {
              throw failure;
            },
          };
        },
      });
      try {
        await assert.rejects(host.close(), failure);
        assert.ok(listener !== undefined);
        assert.equal(listener.listening, false, "shutdown failure left the HTTP listener open");
      } finally {
        if (listener !== undefined) await close(listener);
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
