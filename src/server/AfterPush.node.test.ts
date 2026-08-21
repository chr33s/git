import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Fiber, Layer } from "effect";

import { Hooks } from "../git/Repository.ts";
import type { ReceiveResult } from "../git/Repository.ts";
import { collected, deliveries } from "./AfterPush.node.ts";

describe("AfterPush.deliveries", () => {
  it("returns from the hook before the receiver has answered", async () => {
    // The property the fork is for. `postReceive` runs inside
    // `Repository.receive`, between the ref moving and everything the caller
    // does after it — `git+ queue run` writes `pr.merged` and `queue.leave`
    // there — so a hook that waited would hold that window open for as long as
    // a slow receiver took, with the branch already swapped.
    const started = deliveries("50 millis");
    const returned = await Effect.runPromise(
      Effect.gen(function* () {
        yield* started.background(Effect.never);
        return "the hook came back";
      }),
    );
    assert.equal(returned, "the hook came back");
    await Effect.runPromise(started.settle);
  });

  it("waits at the end for work that is still running", async () => {
    // And the property the join is for: a process that exits cannot detach and
    // forget, because the fork dies with the verb and the landing is reported
    // with nobody told.
    const started = deliveries();
    let delivered = false;
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* started.background(
          Effect.sync(() => {
            delivered = true;
          }).pipe(Effect.delay("20 millis")),
        );
        assert.equal(delivered, false, "not yet, or it was never forked");
        yield* started.settle;
      }),
    );
    assert.equal(delivered, true);
  });

  it("interrupts a receiver that never answers rather than only stopping waiting", async () => {
    // A mirror that black-holes is a socket with no bound on it, and this verb
    // runs on a wake. Abandoning the *wait* is not enough: the fiber keeps
    // running, the socket keeps the event loop alive, and the verb prints its
    // result and then hangs anyway — which is the thing the bound exists to
    // prevent. The delivery has to be interrupted, and that is what closes it.
    const started = deliveries("50 millis");
    let interrupted = false;
    await Effect.runPromise(
      started.background(
        Effect.never.pipe(
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              interrupted = true;
            }),
          ),
        ),
      ),
    );
    await Effect.runPromise(started.settle);
    assert.equal(interrupted, true);
  });

  it("waits for work that starting the work started", async () => {
    // A receiver's work can land a ref of its own — a forward is a push — so
    // joining one batch is what starts the next, and a single pass would leave
    // that one detached.
    const started = deliveries();
    let second = false;
    await Effect.runPromise(
      started
        .background(
          Effect.suspend(() =>
            started.background(
              Effect.sync(() => {
                second = true;
              }).pipe(Effect.delay("20 millis")),
            ),
          ),
        )
        .pipe(Effect.flatMap(() => started.settle)),
    );
    assert.equal(second, true);
  });

  it("keeps a failing delivery to itself", async () => {
    // A webhook cannot fail a push that already happened.
    const started = deliveries();
    const fiber = await Effect.runPromise(
      Effect.gen(function* () {
        yield* started.background(Effect.fail("receiver said no"));
        return yield* Effect.forkDetach(started.settle);
      }),
    );
    await Effect.runPromise(Fiber.join(fiber));
  });
});

describe("AfterPush.collected", () => {
  /** A chain that records what it was told, in the batches it was told it in. */
  const recording = (sent: Array<ReadonlyArray<ReceiveResult>>) =>
    Layer.succeed(Hooks, {
      preReceive: () => Effect.void,
      update: () => Effect.void,
      postReceive: (results: ReadonlyArray<ReceiveResult>) =>
        Effect.sync(() => {
          sent.push(results);
        }),
    });

  const landed = (ref: string): ReceiveResult => ({ ref, from: null, to: null, ok: true });

  it("sends what a verb did as one thing that happened", async () => {
    // A landing moves the branch through `receive` and then appends the records
    // that say what it holds. Announced separately those are separate pushes to
    // every mirror, separately lost — so one can arrive without the other and
    // leave the mirror holding the merge commit while still showing the pull
    // request that carried it as open. Wrong, rather than behind.
    const sent: Array<ReadonlyArray<ReceiveResult>> = [];
    const held = collected(recording(sent));
    await Effect.runPromise(
      Effect.gen(function* () {
        const hooks = yield* Hooks;
        yield* hooks.postReceive([landed("refs/heads/main")]);
        yield* hooks.postReceive([landed("refs/hub/pr/one"), landed("refs/hub/queue/q")]);
        assert.deepEqual(sent, [], "nothing goes out while the verb is still working");
      }).pipe(Effect.provide(held.layer)),
    );

    await Effect.runPromise(held.flush);
    assert.equal(sent.length, 1);
    assert.deepEqual(
      sent[0]?.map((result) => result.ref),
      ["refs/heads/main", "refs/hub/pr/one", "refs/hub/queue/q"],
    );
  });

  it("sends nothing when the verb wrote nothing", async () => {
    const sent: Array<ReadonlyArray<ReceiveResult>> = [];
    const held = collected(recording(sent));
    await Effect.runPromise(Effect.void.pipe(Effect.provide(held.layer)));
    await Effect.runPromise(held.flush);
    assert.deepEqual(sent, []);
  });

  it("flushes without a chain ever having been built", async () => {
    // A verb that never touched the repository never built the layer, so there
    // is nothing to send and nothing to send it with. `flush` runs regardless —
    // it is a finalizer — and must not be the thing that fails the verb.
    const held = collected(recording([]));
    await Effect.runPromise(held.flush);
  });

  it("asks a refusal while it can still refuse", async () => {
    // Deferring `postReceive` is safe because it reports; deferring the two
    // that can say no would ask after the write they were meant to prevent.
    let asked = 0;
    const gate = Layer.succeed(Hooks, {
      preReceive: () =>
        Effect.sync(() => {
          asked += 1;
        }),
      update: () =>
        Effect.sync(() => {
          asked += 1;
        }),
      postReceive: () => Effect.void,
    });
    const held = collected(gate);
    await Effect.runPromise(
      Effect.gen(function* () {
        const hooks = yield* Hooks;
        yield* hooks.preReceive([]);
        yield* hooks.update({ name: "refs/heads/main", value: null });
      }).pipe(Effect.provide(held.layer)),
    );
    assert.equal(asked, 2);
  });
});
