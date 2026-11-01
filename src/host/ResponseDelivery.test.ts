import assert from "node:assert/strict";
import { describe, it, vi } from "@effect/vitest";
import { track } from "./ResponseDelivery.ts";
import { responseDeliveryContract } from "./ResponseDelivery.contract.ts";

responseDeliveryContract({ describe, it });

for (const outcome of ["complete", "failure", "cancel"] as const) {
  it(`releases a response delivery after ${outcome}`, async () => {
    const settled = Promise.withResolvers<void>();
    const fault = new Error("source unavailable");
    let calls = 0;
    let cancelled = false;
    let pulls = 0;
    const source = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls += 1;
        if (pulls === 1) controller.enqueue(new Uint8Array([42]));
        else if (outcome === "failure") controller.error(fault);
        else if (outcome === "complete") controller.close();
      },
      cancel() {
        cancelled = true;
      },
    });
    const response = track(new Response(source, { status: 202 }), () => {
      calls += 1;
      settled.resolve();
    });
    assert.equal(response.status, 202);
    assert.ok(response.body);
    const reader = response.body.getReader();
    assert.deepEqual((await reader.read()).value, new Uint8Array([42]));
    if (outcome === "cancel") await reader.cancel();
    else if (outcome === "failure") await assert.rejects(reader.read(), fault);
    else assert.equal((await reader.read()).done, true);
    await settled.promise;
    assert.equal(calls, 1);
    assert.equal(cancelled, outcome === "cancel");
  });
}

it("settles a bodyless response immediately", () => {
  let settled = false;
  const original = new Response(null, { status: 204 });
  assert.equal(
    track(original, () => {
      settled = true;
    }),
    original,
  );
  assert.equal(settled, true);
});

// This native Web Streams boundary uses platform timers, outside Effect's clock.
for (const stalled of ["source", "reader"] as const) {
  it(`expires an idle ${stalled} and cancels its source`, async () => {
    vi.useFakeTimers();
    try {
      let cancelled = false;
      let finished = false;
      const source = new ReadableStream<Uint8Array>({
        pull(controller) {
          if (stalled === "reader") controller.enqueue(new Uint8Array([42]));
        },
        cancel() {
          cancelled = true;
        },
      });
      const response = track(
        new Response(source),
        () => {
          finished = true;
        },
        {
          idleMs: 30,
          deadlineMs: 100,
        },
      );
      assert.ok(response.body);
      const reader = response.body.getReader();
      const rejected =
        stalled === "source"
          ? assert.rejects(reader.read(), /response idle deadline exceeded/)
          : undefined;
      await vi.advanceTimersByTimeAsync(29);
      assert.equal(finished, false);
      await vi.advanceTimersByTimeAsync(1);
      if (rejected === undefined)
        await assert.rejects(reader.read(), /response idle deadline exceeded/);
      else await rejected;
      assert.equal(cancelled, true);
      assert.equal(finished, true);
      assert.equal(vi.getTimerCount(), 0);
    } finally {
      vi.useRealTimers();
    }
  });
}

it("progress resets idle expiry but cannot extend the absolute response deadline", async () => {
  vi.useFakeTimers();
  try {
    let enqueue: (chunk: Uint8Array) => void = () => undefined;
    let cancelled = false;
    let finished = false;
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        enqueue = (chunk) => controller.enqueue(chunk);
      },
      cancel() {
        cancelled = true;
      },
    });
    const response = track(
      new Response(source),
      () => {
        finished = true;
      },
      {
        idleMs: 30,
        deadlineMs: 100,
      },
    );
    assert.ok(response.body);
    const reader = response.body.getReader();
    for (let at = 0; at < 100; at += 20) {
      enqueue(new Uint8Array([42]));
      assert.deepEqual((await reader.read()).value, new Uint8Array([42]));
      assert.equal(finished, false);
      await vi.advanceTimersByTimeAsync(20);
    }
    await assert.rejects(reader.read(), /response absolute deadline exceeded/);
    assert.equal(cancelled, true);
    assert.equal(finished, true);
    assert.equal(vi.getTimerCount(), 0);
  } finally {
    vi.useRealTimers();
  }
});

it("clears response timers after normal delivery", async () => {
  vi.useFakeTimers();
  try {
    const settled = Promise.withResolvers<void>();
    const response = track(new Response("ok"), settled.resolve, { idleMs: 30, deadlineMs: 100 });
    assert.equal(await response.text(), "ok");
    await settled.promise;
    assert.equal(vi.getTimerCount(), 0);
  } finally {
    vi.useRealTimers();
  }
});

it("does not release delivery until timed-out source cancellation settles", async () => {
  vi.useFakeTimers();
  const cancellation = Promise.withResolvers<void>();
  try {
    let finished = false;
    const source = new ReadableStream<Uint8Array>({ cancel: () => cancellation.promise });
    const response = track(
      new Response(source),
      () => {
        finished = true;
      },
      {
        idleMs: 30,
        deadlineMs: 100,
      },
    );
    assert.ok(response.body);
    const rejected = assert.rejects(
      response.body.getReader().read(),
      /response idle deadline exceeded/,
    );
    await vi.advanceTimersByTimeAsync(30);
    await rejected;
    assert.equal(finished, false);
    cancellation.resolve();
    await vi.advanceTimersByTimeAsync(0);
    assert.equal(finished, true);
    assert.equal(vi.getTimerCount(), 0);
  } finally {
    cancellation.resolve();
    vi.useRealTimers();
  }
});
