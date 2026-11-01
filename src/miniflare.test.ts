import assert from "node:assert/strict";
import { ReadableStream } from "node:stream/web";
import { it } from "@effect/vitest";
import { prefixStream } from "miniflare";

it("propagates a proxy upload error after releasing the prefix writer", async () => {
  const failure = new Error("interrupted upload");
  const stream = prefixStream(
    new Uint8Array([1]),
    new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.error(failure);
      },
    }),
  );
  const reader = stream.getReader();
  try {
    assert.deepEqual(await reader.read(), { done: false, value: new Uint8Array([1]) });
    await assert.rejects(reader.read(), failure);
  } finally {
    reader.releaseLock();
  }
});

it("cancels a proxy upload while the prefix writer still owns its lock", async () => {
  let cancelled = false;
  const stream = prefixStream(
    new Uint8Array([1]),
    new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    }),
  );
  await stream.cancel("operator stopped");
  // The prefix has not been accepted, so the body was never piped.
  assert.equal(cancelled, false);
});
