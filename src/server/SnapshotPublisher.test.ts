import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";

import * as Snapshot from "./Snapshot.ts";
import * as Publisher from "./SnapshotPublisher.ts";

const snapshot = (oid: string): Snapshot.Published => ({
  version: 1,
  publishedAt: "2026-09-07T00:00:00.000Z",
  head: "refs/heads/main",
  anonymousRead: true,
  refs: [{ name: "refs/heads/main", oid }],
});

describe("snapshot publication", () => {
  for (const { delayed, interrupt } of [
    { delayed: "latest", interrupt: false },
    { delayed: "journal", interrupt: false },
    { delayed: "latest", interrupt: true },
  ]) {
    it.effect(
      `preserves publication order during ${delayed} delay${interrupt ? " and interruption" : ""}`,
      () =>
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const files = new Map<string, Uint8Array>();
          let sequence = 0;
          const before = snapshot("1".repeat(40));
          const after = snapshot("2".repeat(40));
          let current = before;
          let delayedOnce = false;
          const store = Publisher.Store.of({
            read: (key) => Effect.sync(() => files.get(key) ?? null),
            write: (key, bytes) =>
              Effect.gen(function* () {
                const matches =
                  delayed === "latest" ? key === Snapshot.keyOf("r") : key.includes("/journal/");
                if (!delayedOnce && matches) {
                  delayedOnce = true;
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                }
                files.set(key, bytes);
              }),
            remove: (key) =>
              Effect.sync(() => {
                files.delete(key);
              }),
            sequence: () => Effect.sync(() => sequence),
            setSequence: (_, value) =>
              Effect.sync(() => {
                sequence = value;
              }),
          });
          const publish = yield* Publisher.make(() => Effect.sync(() => current)).pipe(
            Effect.provideService(Publisher.Store, store),
          );

          const first = yield* publish("r").pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          const interruption = interrupt
            ? yield* Fiber.interrupt(first).pipe(Effect.forkScoped({ startImmediately: true }))
            : null;
          current = after;
          // The second publication runs synchronously until completion or until
          // it waits for the publisher's permit; no timing delay drives the race.
          const second = yield* publish("r").pipe(Effect.forkScoped({ startImmediately: true }));
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.await(first);
          if (interruption !== null) yield* Fiber.join(interruption);
          yield* Fiber.join(second);

          const latest = files.get(Snapshot.keyOf("r"));
          assert.ok(latest);
          assert.deepEqual(Snapshot.decode(latest), after);
          assert.equal(sequence, 2);
          const firstJournal = files.get(Snapshot.journalKeyOf("r", 1));
          const secondJournal = files.get(Snapshot.journalKeyOf("r", 2));
          assert.ok(firstJournal && secondJournal);
          assert.deepEqual(Snapshot.decodeJournal(firstJournal)?.refs, before.refs);
          assert.deepEqual(Snapshot.decodeJournal(secondJournal)?.changes, [
            { name: "refs/heads/main", from: before.refs[0]?.oid, to: after.refs[0]?.oid },
          ]);
        }),
    );
  }
});
