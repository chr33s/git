import assert from "node:assert/strict";
import * as http from "node:http";
import { afterEach, describe, it } from "@effect/vitest";
import { Effect, Fiber, Predicate, Stream } from "effect";
import { lsRemote, requestPack } from "./Fetch.ts";

const original = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = original;
});

describe("fetch cancellation", () => {
  it("closes a real upload-pack response that stalls after its first bytes", async () => {
    const received = Promise.withResolvers<void>();
    const closed = Promise.withResolvers<void>();
    const server = http.createServer((_request, response) => {
      response.on("close", () => closed.resolve());
      response.writeHead(200, { "content-type": "application/x-git-upload-pack-result" });
      response.write("0008NAK\nPACK");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address !== null && !Predicate.isString(address));
    const fiber = Effect.runFork(
      Effect.scoped(
        Effect.gen(function* () {
          const body = yield* requestPack({
            url: `http://127.0.0.1:${address.port}`,
            wants: [],
            haves: [],
          });
          yield* Stream.runForEach(Stream.fromAsyncIterable(body, String), () =>
            Effect.sync(() => {
              received.resolve();
            }),
          );
        }),
      ),
    );
    try {
      await received.promise;
      await Effect.runPromise(Fiber.interrupt(fiber));
      await closed.promise;
    } finally {
      server.closeAllConnections();
      await Effect.runPromise(Fiber.interrupt(fiber));
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  for (const stage of ["advertisement", "pack headers", "pack body"] as const) {
    it(`aborts the HTTP request during ${stage}`, async () => {
      const started = Promise.withResolvers<void>();
      let held: AbortSignal | null | undefined;
      let release = () => {};
      globalThis.fetch = async (_url, init) => {
        held = init?.signal;
        if (stage === "pack body") {
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new TextEncoder().encode("0008NAK\nPACK"));
                release = () => controller.close();
                held?.addEventListener(
                  "abort",
                  () => {
                    release = () => {};
                    controller.error(held?.reason);
                  },
                  { once: true },
                );
              },
            }),
          );
        }
        const response = Promise.withResolvers<Response>();
        release = () => response.resolve(new Response());
        held?.addEventListener("abort", () => response.reject(held?.reason), { once: true });
        started.resolve();
        return response.promise;
      };
      const operation =
        stage === "advertisement"
          ? lsRemote("http://fixture")
          : Effect.scoped(
              Effect.gen(function* () {
                const pack = yield* requestPack({ url: "http://fixture", wants: [], haves: [] });
                yield* Stream.runForEach(Stream.fromAsyncIterable(pack, String), () =>
                  Effect.sync(() => {
                    started.resolve();
                  }),
                );
              }),
            );
      const fiber = Effect.runFork(operation);
      try {
        await started.promise;
        assert.ok(held, "the in-flight request must carry an abort signal");
        // Abort must happen before an async iterator's finalizer waits for its
        // pending read. A signal attached only to the headers cannot do that.
        await Effect.runPromise(Fiber.interrupt(fiber));
        assert.equal(held.aborted, true);
      } finally {
        release();
        await Effect.runPromise(Fiber.interrupt(fiber));
      }
    });
  }
});
