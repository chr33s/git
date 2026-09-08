import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as http from "node:http";
import { describe, it } from "@effect/vitest";
import { Effect, Predicate } from "effect";
import { chromium } from "playwright";
import { browserBundle } from "../testing/Browser.ts";

describe.skipIf(!existsSync(chromium.executablePath()))("browser upload bodies", () => {
  it.live("replays OPFS files over HTTP/1 and reclaims them safely across exits and tabs", () =>
    Effect.promise(async () => {
      const bundle = await browserBundle(
        `
        import { Deferred, Effect, Exit, Fiber, Stream } from "effect";
        import { prepare } from "./src/client/Upload.ts";
        import { fetchAuthorized } from "./src/client/Authorize.ts";
        import { StorageFailure } from "./src/git/Error.ts";
        let stopUpload;
        export async function hold() {
          const ready = Promise.withResolvers();
          const fiber = Effect.runFork(Effect.gen(function* () {
            const upload = yield* prepare(Stream.make(new Uint8Array(1024 * 1024)));
            const options = yield* Effect.promise(upload.options);
            ready.resolve({ name: options.body.name, size: options.body.size });
            yield* Effect.never;
          }).pipe(Effect.scoped));
          stopUpload = () => Effect.runPromise(Fiber.interrupt(fiber));
          return ready.promise;
        }
        export async function stop() { await stopUpload(); }
        export async function holdWriting() {
          const ready = Promise.withResolvers();
          Effect.runFork(prepare(Stream.make(new Uint8Array(1024 * 1024)).pipe(Stream.concat(
            Stream.fromEffect(Effect.sync(() => ready.resolve()).pipe(Effect.andThen(Effect.never)))
          ))).pipe(Effect.scoped));
          await ready.promise;
          const root = await navigator.storage.getDirectory();
          const names = [];
          for await (const [name] of root.entries()) if (name.startsWith(".git-upload-")) names.push(name);
          return names.sort();
        }
        export async function fresh() {
          await Effect.runPromise(prepare(Stream.make(Uint8Array.of(1))).pipe(Effect.scoped));
          const root = await navigator.storage.getDirectory();
          const names = [];
          for await (const [name] of root.entries()) if (name.startsWith(".git-upload-")) names.push(name);
          return names.sort();
        }
        export async function run() {
          const root = await navigator.storage.getDirectory();
          const files = async () => {
            const names = [];
            for await (const [name] of root.entries()) if (name.startsWith(".git-upload-")) names.push(name);
            return names;
          };
          const payload = new TextEncoder().encode("replayable upload");
          const sent = await Effect.runPromise(Effect.gen(function* () {
            const upload = yield* prepare(Stream.make(payload.subarray(0, 6), payload.subarray(6)));
            const options = yield* Effect.promise(upload.options);
            const during = yield* Effect.promise(files);
            const response = yield* Effect.promise(() => fetchAuthorized(location.origin + "/upload",
              { method: "POST" }, { operation: "git-receive-pack", commands: [] },
              async () => "fixture", upload.options));
            return { file: options.body instanceof File, during: during.length, text: yield* Effect.promise(() => response.text()) };
          }).pipe(Effect.scoped));
          const afterSuccess = await files();
          const failed = await Effect.runPromise(prepare(Stream.make(payload).pipe(Stream.concat(
            Stream.fail(new StorageFailure({ operation: "read", path: "fixture", cause: "unavailable" }))
          ))).pipe(Effect.scoped, Effect.exit));
          const afterFailure = await files();
          const interrupted = await Effect.runPromise(Effect.gen(function* () {
            const entered = yield* Deferred.make();
            const stalled = Stream.fromEffect(Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)));
            const writing = yield* prepare(Stream.make(payload).pipe(Stream.concat(stalled))).pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            const during = yield* Effect.promise(files);
            yield* Fiber.interrupt(writing);
            return during.length > 0;
          }).pipe(Effect.scoped));
          return { sent, afterSuccess, failed: Exit.isFailure(failed), afterFailure, interrupted, afterInterrupt: await files() };
        }
      `,
        "UploadReview",
      );
      const bodies: string[] = [];
      const server = http.createServer(async (request, response) => {
        if (request.method !== "POST") {
          response.writeHead(200, { "content-type": "text/html" });
          response.end("<!doctype html><title>Upload regression</title>");
          return;
        }
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(Buffer.from(chunk));
        const body = Buffer.concat(chunks).toString();
        bodies.push(body);
        if (request.headers.authorization === undefined) {
          response.writeHead(401, { "www-authenticate": 'Hub-SSH-v1 nonce="fixture"' });
          response.end();
        } else response.end(body);
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      try {
        const address = server.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const browser = await chromium.launch();
        try {
          const context = await browser.newContext();
          const url = `http://127.0.0.1:${address.port}`;
          const page = await context.newPage();
          await page.goto(url);
          await page.addScriptTag({ content: bundle });
          assert.deepEqual(await page.evaluate("UploadReview.run()"), {
            sent: { file: true, during: 1, text: "replayable upload" },
            afterSuccess: [],
            failed: true,
            afterFailure: [],
            interrupted: true,
            afterInterrupt: [],
          });
          assert.deepEqual(bodies, ["replayable upload", "replayable upload"]);
          const abandoned = await page.evaluate<{ name: string; size: number }>(
            "UploadReview.hold()",
          );
          assert.equal(abandoned.size, 1024 * 1024);
          // Closing the tab destroys the Effect runtime without running its
          // finalizers. A later upload must reclaim that completed file.
          await page.close();
          const live = await context.newPage();
          await live.goto(url);
          await live.addScriptTag({ content: bundle });
          const held = await live.evaluate<{ name: string; size: number }>("UploadReview.hold()");
          const other = await context.newPage();
          await other.goto(url);
          await other.addScriptTag({ content: bundle });
          assert.deepEqual(await other.evaluate("UploadReview.fresh()"), [held.name]);
          assert.equal(
            await other.evaluate(async (name) => {
              const root = await navigator.storage.getDirectory();
              return (await (await root.getFileHandle(name)).getFile()).size;
            }, held.name),
            held.size,
          );
          await live.evaluate("UploadReview.stop()");
          assert.deepEqual(await other.evaluate("UploadReview.fresh()"), []);
          const writing = await live.evaluate<string[]>("UploadReview.holdWriting()");
          assert.ok(writing.length > 0);
          assert.deepEqual(await other.evaluate("UploadReview.fresh()"), writing);
          await live.close();
          assert.deepEqual(await other.evaluate("UploadReview.fresh()"), []);
        } finally {
          await browser.close();
        }
      } finally {
        server.closeAllConnections();
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    }),
  );
});
