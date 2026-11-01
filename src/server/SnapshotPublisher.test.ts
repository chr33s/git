import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Logger, Result } from "effect";

import * as Snapshot from "./Snapshot.ts";
import * as Publisher from "./SnapshotPublisher.ts";
import { StorageFailure } from "../git/Error.ts";

const snapshot = (oid: string): Snapshot.Published => ({
  version: 1,
  publishedAt: "2026-09-07T00:00:00.000Z",
  head: "refs/heads/main",
  anonymousRead: true,
  refs: [{ name: "refs/heads/main", oid }],
});

describe("snapshot publication", () => {
  for (const fault of ["invalidate", "publish", "retention"] as const) {
    it.effect(`reports ${fault} without repository names or storage secrets`, () =>
      Effect.gen(function* () {
        const logged: string[] = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          logged.push(JSON.stringify(message));
        });
        const secret = "private-repository-and-credential";
        const failed = () =>
          Effect.fail(new StorageFailure({ operation: "fixture", path: secret, cause: secret }));
        const store = Publisher.Store.of({
          read: () => Effect.succeed(null),
          write: () => (fault === "publish" ? failed() : Effect.void),
          remove: (key) =>
            fault === "invalidate" || (fault === "retention" && key.includes("/journal/"))
              ? failed()
              : Effect.void,
          sequence: () => Effect.succeed(Snapshot.RETAIN),
          setSequence: () => Effect.void,
        });
        const mutate = yield* Publisher.make(() => Effect.succeed(snapshot("1".repeat(40)))).pipe(
          Effect.provideService(Publisher.Store, store),
        );
        let changed = false;
        const result = yield* mutate(
          secret,
          Effect.sync(() => {
            changed = true;
          }),
        ).pipe(Effect.result, Effect.provide(Logger.layer([logger])));
        assert.equal(Result.isFailure(result), fault === "invalidate");
        assert.equal(changed, fault !== "invalidate");
        assert.equal(logged.length, 1);
        assert.ok(logged[0]?.includes('"component":"SnapshotPublisher"'));
        assert.ok(logged[0]?.includes(`"operation":"${fault}"`));
        assert.equal(logged.join("\n").includes(secret), false);
      }),
    );
  }

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

          const first = yield* publish("r", Effect.void).pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          const interruption = interrupt
            ? yield* Fiber.interrupt(first).pipe(Effect.forkScoped({ startImmediately: true }))
            : null;
          // The second publication runs synchronously until completion or until
          // it waits for the publisher's permit; no timing delay drives the race.
          const second = yield* publish(
            "r",
            Effect.sync(() => {
              assert.equal(
                files.has(Snapshot.keyOf("r")),
                false,
                "old view is invalidated before mutation",
              );
              current = after;
            }),
          ).pipe(Effect.forkScoped({ startImmediately: true }));
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

  it.effect("refuses privacy changes when invalidation fails, even if writes also fail", () =>
    Effect.gen(function* () {
      const before = snapshot("1".repeat(40));
      const bytes = Snapshot.encode(before);
      let changed = false;
      const failed = () => Effect.fail(new StorageFailure({ operation: "fixture", path: "r" }));
      const store = Publisher.Store.of({
        read: () => Effect.succeed(bytes),
        write: failed,
        remove: failed,
        sequence: () => Effect.succeed(0),
        setSequence: failed,
      });
      const mutate = yield* Publisher.make(() =>
        Effect.succeed({ ...before, anonymousRead: false }),
      ).pipe(Effect.provideService(Publisher.Store, store));
      const result = yield* mutate(
        "r",
        Effect.sync(() => {
          changed = true;
        }),
      ).pipe(Effect.result);
      assert.ok(Result.isFailure(result));
      assert.equal(result.failure._tag, "StorageFailure");
      assert.equal(changed, false);
    }),
  );

  for (const fault of ["capture", "journal", "latest", "sequence"] as const) {
    it.effect(
      `a ${fault} failure leaves no stale view after a privacy change, including after restart`,
      () =>
        Effect.gen(function* () {
          let current = snapshot("1".repeat(40));
          const files = new Map([[Snapshot.keyOf("r"), Snapshot.encode(current)]]);
          let sequence = 0;
          let broken = true;
          const failed = () => Effect.fail(new StorageFailure({ operation: "fixture", path: "r" }));
          const store = Publisher.Store.of({
            read: (key) => Effect.sync(() => files.get(key) ?? null),
            write: (key, bytes) =>
              Effect.suspend(() =>
                broken &&
                ((fault === "journal" && key.includes("/journal/")) ||
                  (fault === "latest" && key === Snapshot.keyOf("r")))
                  ? failed()
                  : Effect.sync(() => {
                      files.set(key, bytes);
                    }),
              ),
            remove: (key) =>
              Effect.sync(() => {
                files.delete(key);
              }),
            sequence: () => Effect.sync(() => sequence),
            setSequence: (_, value) =>
              Effect.suspend(() =>
                broken && fault === "sequence"
                  ? failed()
                  : Effect.sync(() => {
                      sequence = value;
                    }),
              ),
          });
          const build = Publisher.make(() =>
            broken && fault === "capture" ? failed() : Effect.succeed(current),
          ).pipe(Effect.provideService(Publisher.Store, store));
          const mutate = yield* build;
          yield* mutate(
            "r",
            Effect.sync(() => {
              assert.equal(files.has(Snapshot.keyOf("r")), false);
              current = { ...current, anonymousRead: false };
            }),
          );
          assert.equal(files.has(Snapshot.keyOf("r")), false);
          broken = false;
          // A new publisher has no memory of the previous instance or failure.
          const restarted = yield* build;
          yield* restarted("r", Effect.void);
          const restored = files.get(Snapshot.keyOf("r"));
          assert.ok(restored);
          assert.equal(Snapshot.decode(restored)?.anonymousRead, false);
          yield* restarted("r", Effect.void);
          assert.equal(
            files.has(Snapshot.keyOf("r")),
            true,
            "an unchanged view is republished after invalidation",
          );
        }),
    );
  }

  it.effect("interrupted mutations keep the invalidated public view out of R2", () =>
    Effect.gen(function* () {
      let current = snapshot("1".repeat(40));
      const files = new Map([[Snapshot.keyOf("r"), Snapshot.encode(current)]]);
      const entered = yield* Deferred.make<void>();
      const store = Publisher.Store.of({
        read: (key) => Effect.sync(() => files.get(key) ?? null),
        write: (key, bytes) =>
          Effect.sync(() => {
            files.set(key, bytes);
          }),
        remove: (key) =>
          Effect.sync(() => {
            files.delete(key);
          }),
        sequence: () => Effect.succeed(0),
        setSequence: () => Effect.void,
      });
      const mutate = yield* Publisher.make(() => Effect.succeed(current)).pipe(
        Effect.provideService(Publisher.Store, store),
      );
      const updating = yield* mutate(
        "r",
        Effect.gen(function* () {
          current = { ...current, anonymousRead: false };
          yield* Deferred.succeed(entered, undefined);
          return yield* Effect.never;
        }),
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      assert.equal(files.has(Snapshot.keyOf("r")), false);
      yield* Fiber.interrupt(updating);
      const final = files.get(Snapshot.keyOf("r"));
      assert.ok(final);
      assert.equal(Snapshot.decode(final)?.anonymousRead, false);
    }),
  );
});

it.effect("retention outages do not grow the journal and restart retries the same retirement", () =>
  Effect.gen(function* () {
    const files = new Map<string, Uint8Array>();
    for (let seq = 1; seq <= Snapshot.RETAIN; seq++)
      files.set(Snapshot.journalKeyOf("r", seq), new Uint8Array([seq % 256]));
    let sequence = Snapshot.RETAIN;
    let broken = true;
    let mutations = 0;
    const retired: string[] = [];
    const store = Publisher.Store.of({
      read: (key) => Effect.sync(() => files.get(key) ?? null),
      write: (key, bytes) =>
        Effect.sync(() => {
          files.set(key, bytes);
        }),
      remove: (key) =>
        Effect.suspend(() => {
          if (key.includes("/journal/")) {
            retired.push(key);
            if (broken)
              return Effect.fail(new StorageFailure({ operation: "delete", path: "private" }));
          }
          files.delete(key);
          return Effect.void;
        }),
      sequence: () => Effect.sync(() => sequence),
      setSequence: (_, value) =>
        Effect.sync(() => {
          sequence = value;
        }),
    });
    const build = Publisher.make(() => Effect.succeed(snapshot("1".repeat(40)))).pipe(
      Effect.provideService(Publisher.Store, store),
    );
    for (let attempt = 0; attempt < 4; attempt++) {
      const restarted = yield* build;
      yield* restarted(
        "r",
        Effect.sync(() => {
          mutations++;
        }),
      );
      assert.equal(sequence, Snapshot.RETAIN);
      assert.equal(files.size, Snapshot.RETAIN);
      assert.equal(files.has(Snapshot.keyOf("r")), false);
      assert.equal(files.has(Snapshot.journalKeyOf("r", Snapshot.RETAIN + 1)), false);
    }
    assert.equal(mutations, 4);
    assert.deepEqual(retired, Array(4).fill(Snapshot.journalKeyOf("r", 1)));
    broken = false;
    const recovered = yield* build;
    yield* recovered("r", Effect.void);
    assert.equal(sequence, Snapshot.RETAIN + 1);
    assert.equal(files.size, Snapshot.RETAIN + 1);
    assert.equal(files.has(Snapshot.journalKeyOf("r", 1)), false);
    assert.equal(files.has(Snapshot.keyOf("r")), true);
  }),
);

for (const sequence of [-1, 1.5, Number.NaN, Infinity, 9_999_999_999, Number.MAX_SAFE_INTEGER])
  it.effect(`invalid/exhausted snapshot sequence ${sequence} cannot create journal entries`, () =>
    Effect.gen(function* () {
      const writes: string[] = [];
      const store = Publisher.Store.of({
        read: () => Effect.succeed(null),
        write: (key) =>
          Effect.sync(() => {
            writes.push(key);
          }),
        remove: () => Effect.void,
        sequence: () => Effect.succeed(sequence),
        setSequence: () => Effect.die("invalid sequence must never advance"),
      });
      const mutate = yield* Publisher.make(() => Effect.succeed(snapshot("1".repeat(40)))).pipe(
        Effect.provideService(Publisher.Store, store),
      );
      yield* mutate("r", Effect.void);
      assert.deepEqual(writes, []);
    }),
  );

it.effect("the final ten-digit journal sequence publishes once and then refuses growth", () =>
  Effect.gen(function* () {
    let sequence = 9_999_999_998;
    const writes: string[] = [];
    const store = Publisher.Store.of({
      read: () => Effect.succeed(null),
      write: (key) =>
        Effect.sync(() => {
          writes.push(key);
        }),
      remove: () => Effect.void,
      sequence: () => Effect.sync(() => sequence),
      setSequence: (_, value) =>
        Effect.sync(() => {
          sequence = value;
        }),
    });
    const build = Publisher.make(() => Effect.succeed(snapshot("1".repeat(40)))).pipe(
      Effect.provideService(Publisher.Store, store),
    );
    yield* (yield* build)("r", Effect.void);
    assert.equal(sequence, 9_999_999_999);
    assert.deepEqual(writes, [Snapshot.journalKeyOf("r", 9_999_999_999), Snapshot.keyOf("r")]);
    writes.length = 0;
    yield* (yield* build)("r", Effect.void);
    assert.deepEqual(writes, []);
    assert.equal(sequence, 9_999_999_999);
  }),
);
