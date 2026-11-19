import assert from "node:assert/strict";
import { describe, it, vi } from "@effect/vitest";
import { Invalid } from "../git/Error.ts";
import { requestUploadContract } from "./RequestUpload.contract.ts";
import { configuration, open } from "./RequestUpload.ts";
import { ConfigProvider, Effect } from "effect";

for (const invalid of ["0", "-1", "1.5", "2147483648", "Infinity"])
  it.effect(`rejects invalid upload idle configuration ${invalid}`, () =>
    Effect.gen(function* () {
      const result = yield* configuration().pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }).pipe(
      Effect.provide(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ GIT_CF_UPLOAD_IDLE_MS: invalid })),
      ),
    ),
  );

for (const phase of ["before-read", "pending-read", "already-aborted"] as const)
  it(`cancels ${phase} without accepting cancellation as EOF`, async () => {
    const controller = new AbortController();
    let cancelled = 0;
    const original = new Request("http://host/repo/git-receive-pack", {
      method: "POST",
      body: new ReadableStream<Uint8Array>({
        cancel(reason) {
          assert.ok(reason instanceof Invalid);
          cancelled += 1;
          return new Promise<void>(() => {});
        },
      }),
      duplex: "half",
    });
    if (phase === "already-aborted") controller.abort("private abort reason");
    const upload = open(original, controller.signal);
    const reading = phase === "pending-read" ? upload.request.text() : undefined;
    controller.abort("private abort reason");
    await assert.rejects(
      reading ?? upload.request.text(),
      (error) => error instanceof Invalid && error.reason === "request body cancelled",
    );
    upload.close();
    assert.equal(cancelled, 1);
    assert.equal(original.body?.locked, false);
  });

it("preserves request metadata and normal EOF, then ignores later abort", async () => {
  const controller = new AbortController();
  const original = new Request("http://host/repo/commit-pack?mode=stream", {
    method: "POST",
    headers: { authorization: "fixture", "content-type": "application/x-ndjson" },
    body: "payload",
  });
  const upload = open(original, controller.signal);
  assert.equal(upload.request.url, original.url);
  assert.equal(upload.request.method, original.method);
  assert.deepEqual([...upload.request.headers], [...original.headers]);
  assert.equal(await upload.request.text(), "payload");
  upload.close();
  controller.abort();
  assert.equal(original.body?.locked, false);
});

it("closes an unread body after a refused request", async () => {
  let cancelled = 0;
  const original = new Request("http://host/repo/blob", {
    method: "POST",
    body: new ReadableStream({
      cancel() {
        cancelled += 1;
      },
    }),
    duplex: "half",
  });
  const upload = open(original, new AbortController().signal);
  upload.close();
  upload.close();
  await assert.rejects(upload.request.text(), Invalid);
  assert.equal(cancelled, 1);
  assert.equal(original.body?.locked, false);
});

it("preserves source errors and releases the source reader", async () => {
  const failure = new Error("source failed");
  const original = new Request("http://host/repo/blob", {
    method: "POST",
    body: new ReadableStream({
      pull(controller) {
        controller.error(failure);
      },
    }),
    duplex: "half",
  });
  const upload = open(original, new AbortController().signal);
  await assert.rejects(upload.request.text(), failure);
  upload.close();
  assert.equal(original.body?.locked, false);
});

it("releases the transport when the consumer cancels its reader", async () => {
  let cancelled = 0;
  const original = new Request("http://host/repo/blob", {
    method: "POST",
    body: new ReadableStream<Uint8Array>({
      cancel(reason) {
        assert.ok(reason instanceof Invalid);
        cancelled += 1;
      },
    }),
    duplex: "half",
  });
  const upload = open(original, new AbortController().signal);
  assert.ok(upload.request.body);
  const reader = upload.request.body.getReader();
  const pending = reader.read();
  await reader.cancel("private consumer reason");
  assert.equal((await pending).done, true);
  upload.close();
  assert.equal(cancelled, 1);
  assert.equal(original.body?.locked, false);
});

it("keeps bodyless requests bodyless", () => {
  const upload = open(new Request("http://host/repo/refs"), new AbortController().signal);
  assert.equal(upload.request.body, null);
  upload.close();
});

requestUploadContract({ describe, it });

it("counts pending reads, clears on progress, and excludes consumer backpressure", async () => {
  // This wrapper uses platform timers, outside the Effect test clock.
  vi.useFakeTimers();
  const source = new TransformStream<Uint8Array, Uint8Array>();
  const writer = source.writable.getWriter();
  const upload = open(
    new Request("http://host/repo/blob", {
      method: "POST",
      body: source.readable,
      duplex: "half",
    }),
    new AbortController().signal,
    100,
  );
  let expired = false;
  void upload.idle.then(() => {
    expired = true;
  });
  assert.ok(upload.request.body);
  const reader = upload.request.body.getReader();
  try {
    await vi.advanceTimersByTimeAsync(200);
    assert.equal(expired, false);
    const first = reader.read();
    await vi.advanceTimersByTimeAsync(90);
    await writer.write(new Uint8Array([1]));
    assert.deepEqual((await first).value, new Uint8Array([1]));
    await vi.advanceTimersByTimeAsync(200);
    assert.equal(expired, false);
    const second = reader.read();
    const rejected = assert.rejects(second);
    await vi.advanceTimersByTimeAsync(100);
    assert.equal(expired, true);
    upload.close();
    await rejected;
    assert.equal(vi.getTimerCount(), 0);
  } finally {
    upload.close();
    vi.useRealTimers();
  }
});
