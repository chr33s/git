import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { memoryLayer, Operations } from "./Operations.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Operations>) =>
  Effect.runPromise(effect.pipe(Effect.provide(memoryLayer)));

describe("Operations state", () => {
  it.effect("transitions queued → running → succeeded", () =>
    Effect.promise(async () => {
      const operation = await scenario(
        Effect.gen(function* () {
          const operations = yield* Operations;
          const created = yield* operations.create({ repo: "demo", kind: "bundle.build" });
          yield* operations.start(created.id);
          yield* operations.progress(created.id, { current: 1, total: 2, unit: "objects" }, "walk");
          return yield* operations.succeed(created.id);
        }),
      );
      assert.equal(operation.state, "succeeded");
      assert.equal(operation.progress?.current, 1);
    }),
  );

  it.effect("refuses cancellation after the commit point", () =>
    Effect.promise(async () => {
      const tag = await scenario(
        Effect.gen(function* () {
          const operations = yield* Operations;
          const created = yield* operations.create({ repo: "demo", kind: "bundle.build" });
          yield* operations.start(created.id);
          yield* operations.markCommitted(created.id);
          return (yield* Effect.flip(operations.cancel(created.id)))._tag;
        }),
      );
      assert.equal(tag, "Invalid");
    }),
  );

  it.effect("marks a cooperative cancel as cancelled", () =>
    Effect.promise(async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const operations = yield* Operations;
          const created = yield* operations.create({ repo: "demo", kind: "walk" });
          yield* operations.start(created.id);
          return (yield* operations.cancel(created.id)).state;
        }),
      );
      assert.equal(state, "cancelled");
    }),
  );
});

describe("Operations.memory layer", () => {
  it.effect("lists by state", () =>
    Effect.promise(async () => {
      const running = await scenario(
        Effect.gen(function* () {
          const operations = yield* Operations;
          const first = yield* operations.create({ repo: "a", kind: "maintenance.fsck" });
          yield* operations.start(first.id);
          yield* operations.create({ repo: "a", kind: "maintenance.gc" });
          return yield* operations.list({ state: "running" });
        }),
      );
      assert.equal(running.length, 1);
      assert.equal(running[0]?.kind, "maintenance.fsck");
    }),
  );
});
