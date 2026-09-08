import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { Predicate } from "effect";

import { serve } from "./Node.ts";

for (const queuedWrite of [false, true]) {
  it(
    queuedWrite
      ? "discards a disconnected write waiting behind another request"
      : "releases the repository gate and remote fetch when the HTTP caller disconnects",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "host-cancel-"));
      const started = Promise.withResolvers<void>();
      const closed = Promise.withResolvers<void>();
      let release = () => {};
      const upstream = http.createServer((_request, response) => {
        response.on("close", () => closed.resolve());
        release = () => {
          response.writeHead(503).end();
        };
        started.resolve();
      });
      await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
      const address = upstream.address();
      assert.ok(address !== null && !Predicate.isString(address));
      const queued = Promise.withResolvers<void>();
      const disconnected = Promise.withResolvers<void>();
      const server = await serve({
        root,
        allowAnonymousWrites: true,
        development: async (listener) => {
          listener.on("request", (request, response) => {
            if (request.url !== "/repo/commit") return;
            queued.resolve();
            response.on("close", () => disconnected.resolve());
          });
          return { handle: (_request, _response, next) => next(), close: async () => {} };
        },
      });
      const controller = new AbortController();
      const fetching = fetch(`${server.url}/repo/fetch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url: `http://127.0.0.1:${address.port}/remote` }),
        signal: controller.signal,
      }).then(
        () => "completed",
        () => "aborted",
      );
      try {
        await started.promise;
        if (queuedWrite) {
          const canceled = new AbortController();
          const committing = fetch(`${server.url}/repo/commit`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              branch: "main",
              message: "canceled",
              author: {
                name: "T",
                email: "t@example.com",
                at: "2026-01-01T00:00:00.000Z",
                offset: 0,
              },
              files: [{ path: "file.txt", content: "canceled\n" }],
            }),
            signal: canceled.signal,
          }).then(
            () => "completed",
            () => "aborted",
          );
          await queued.promise;
          canceled.abort();
          assert.equal(await committing, "aborted");
          await disconnected.promise;
          // Complete the request holding the gate normally: the queued
          // write's own disconnection must be what prevents its commit.
          release();
          assert.equal(await fetching, "completed");
        } else {
          controller.abort();
          assert.equal(await fetching, "aborted");
        }
        const refs = await fetch(`${server.url}/repo/refs`, { signal: AbortSignal.timeout(2_000) });
        assert.equal(refs.status, 200);
        assert.deepEqual(await refs.json(), { refs: [], head: "refs/heads/main" });
        await closed.promise;
      } finally {
        controller.abort();
        upstream.closeAllConnections();
        await new Promise<void>((resolve) => upstream.close(() => resolve()));
        await server.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    },
  );
}
