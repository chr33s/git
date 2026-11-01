import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Clock, Effect, Logger } from "effect";
import { TestClock } from "effect/testing";
import * as Snapshot from "./Snapshot.ts";
import * as Publisher from "./SnapshotPublisher.ts";

const now = Date.parse("2026-10-30T00:00:00.000Z");
const captured = (publishedAt: string): Snapshot.Published => ({
  version: 1,
  publishedAt,
  head: "refs/heads/private-head",
  anonymousRead: false,
  refs: [{ name: "refs/private-ref", oid: "private-object" }],
});

for (const [date, state, age] of [
  ["2026-10-29T00:00:00.000Z", "known", 86400000],
  ["2026-10-30T00:00:00.000Z", "known", 0],
  ["2026-10-31T00:00:00.000Z", "future", null],
  ["private-invalid-timestamp", "invalid", null],
  ["2026-02-31T00:00:00.000Z", "invalid", null],
] as const)
  it.effect(
    `snapshot age reports ${state}/${age} without changing the view or exposing metadata`,
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(now);
        const logged: string[] = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          logged.push(JSON.stringify(message));
        });
        const original = captured(date);
        const loaded = yield* Snapshot.load(Effect.succeed(Snapshot.encode(original))).pipe(
          Effect.provide(Logger.layer([logger])),
        );
        assert.deepEqual(loaded, original);
        assert.equal(logged.length, 1);
        assert.ok(logged[0]?.includes('"operation":"observed"'));
        assert.ok(logged[0]?.includes(`"ageState":"${state}"`));
        assert.ok(logged[0]?.includes(`"ageMs":${age}`));
        assert.doesNotMatch(logged.join(""), /private-|publishedAt|anonymousRead|2026-/);
      }),
  );

it.effect(
  "publication telemetry includes capture and persistence time, excludes the handler, and reports unchanged views",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(now);
      const logged: string[] = [];
      const logger = Logger.make<unknown, void>(({ message }) => {
        logged.push(JSON.stringify(message));
      });
      let sequence = 0;
      const store = Publisher.Store.of({
        read: () => Effect.succeed(null),
        remove: () => Effect.void,
        write: (key) => TestClock.adjust(key.includes("/journal/") ? 20 : 30),
        sequence: () => Effect.succeed(sequence),
        setSequence: (_, value) =>
          TestClock.adjust(5).pipe(
            Effect.andThen(
              Effect.sync(() => {
                sequence = value;
              }),
            ),
          ),
      });
      const mutate = yield* Publisher.make(() =>
        Effect.gen(function* () {
          yield* TestClock.adjust(10);
          return captured(new Date(yield* Clock.currentTimeMillis).toISOString());
        }),
      ).pipe(Effect.provideService(Publisher.Store, store));
      yield* mutate("private-repo", TestClock.adjust(50)).pipe(
        Effect.provide(Logger.layer([logger])),
      );
      assert.equal(logged.length, 1);
      assert.ok(logged[0]?.includes('"operation":"published"'));
      assert.ok(logged[0]?.includes('"durationMs":65'));
      assert.ok(logged[0]?.includes('"ageMs":55'));
      assert.ok(logged[0]?.includes('"changed":true'));
      yield* mutate("private-repo", TestClock.adjust(50)).pipe(
        Effect.provide(Logger.layer([logger])),
      );
      assert.equal(logged.length, 2);
      assert.ok(logged[1]?.includes('"durationMs":40'));
      assert.ok(logged[1]?.includes('"ageMs":30'));
      assert.ok(logged[1]?.includes('"changed":false'));
      assert.equal(sequence, 1);
      assert.doesNotMatch(logged.join(""), /private-|publishedAt|anonymousRead|2026-/);
    }),
);
