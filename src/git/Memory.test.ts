/**
 * Contract tests for the storage ports.
 *
 * These are written against the ports, not against the in-memory backend — the
 * Cloudflare (DO SQLite + R2), OPFS and node backends have to pass this same
 * file. That is the point of narrowing `GitStorage` into three services: the
 * atomicity guarantee is now testable, where `applyRefChanges?` being optional
 * meant it was whatever each backend happened to do.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Effect, Stream } from "effect";

import { stores } from "./Memory.ts";
import { ObjectStore, type Oid, RefStore } from "./Store.ts";

const run = <A, E>(effect: Effect.Effect<A, E, ObjectStore | RefStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(stores)) as Effect.Effect<A, E>);

const a = "a".repeat(40) as Oid;
const b = "b".repeat(40) as Oid;

describe("ObjectStore contract", () => {
  it("writes content-addressed and reads back", async () => {
    const result = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const data = new TextEncoder().encode("hello\n");
        const oid = yield* objects.write({ type: "blob", data });
        const read = yield* objects.read(oid);
        return { oid, text: new TextDecoder().decode(read.data), type: read.type };
      }),
    );

    assert.equal(result.oid, "ce013625030ba8dba906f756967f9e9ca394464a");
    assert.equal(result.type, "blob");
    assert.equal(result.text, "hello\n");
  });

  it("is idempotent: the same bytes yield the same oid", async () => {
    const [first, second] = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const data = new TextEncoder().encode("same");
        return [
          yield* objects.write({ type: "blob", data }),
          yield* objects.write({ type: "blob", data }),
        ] as const;
      }),
    );

    assert.equal(first, second);
  });

  it("fails with ObjectNotFound rather than returning nothing", async () => {
    const error = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        return yield* Effect.flip(objects.read(a));
      }),
    );

    assert.equal(error._tag, "ObjectNotFound");
  });

  it("does not hand out a reference to its own buffer", async () => {
    const mutated = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const oid = yield* objects.write({
          type: "blob",
          data: new TextEncoder().encode("stable"),
        });
        const first = yield* objects.read(oid);
        first.data[0] = 0;
        const second = yield* objects.read(oid);
        return new TextDecoder().decode(second.data);
      }),
    );

    assert.equal(mutated, "stable");
  });

  it("streams and lists", async () => {
    const result = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const oid = yield* objects.write({
          type: "blob",
          data: new TextEncoder().encode("chunked"),
        });
        const chunks = yield* Stream.runCollect(yield* objects.readStream(oid));
        const listed = yield* Stream.runCollect(objects.list());
        return {
          listed,
          text: new TextDecoder().decode(chunks[0]),
        };
      }),
    );

    assert.equal(result.text, "chunked");
    assert.equal(result.listed.length, 1);
  });

  it("deletes", async () => {
    const has = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        const oid = yield* objects.write({ type: "blob", data: new Uint8Array([1]) });
        yield* objects.delete(oid);
        return yield* objects.has(oid);
      }),
    );

    assert.equal(has, false);
  });
});

describe("RefStore contract", () => {
  it("creates a ref only when it does not exist", async () => {
    const results = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        const created = yield* refs.apply([{ name: "refs/heads/main", value: a, expected: null }]);
        const again = yield* refs.apply([{ name: "refs/heads/main", value: b, expected: null }]);
        return { again, created, current: yield* refs.read("refs/heads/main") };
      }),
    );

    assert.equal(results.created[0]?.applied, true);
    assert.equal(results.again[0]?.applied, false);
    assert.equal(results.again[0]?.current, a);
    assert.equal(results.current, a);
  });

  it("treats an undefined expectation as 'don't care'", async () => {
    const applied = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.apply([{ name: "refs/heads/main", value: a, expected: null }]);
        const forced = yield* refs.apply([{ name: "refs/heads/main", value: b }]);
        return { forced, current: yield* refs.read("refs/heads/main") };
      }),
    );

    assert.equal(applied.forced[0]?.applied, true);
    assert.equal(applied.current, b);
  });

  it("deletes with a null value", async () => {
    const current = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.apply([{ name: "refs/heads/gone", value: a, expected: null }]);
        yield* refs.apply([{ name: "refs/heads/gone", value: null, expected: a }]);
        return yield* refs.read("refs/heads/gone");
      }),
    );

    assert.equal(current, null);
  });

  it("applies all or nothing when atomic", async () => {
    const state = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.apply([{ name: "refs/heads/taken", value: a, expected: null }]);

        const results = yield* refs.apply(
          [
            { name: "refs/heads/fresh", value: b, expected: null },
            // stale: this one already exists
            { name: "refs/heads/taken", value: b, expected: null },
          ],
          { atomic: true },
        );

        return { fresh: yield* refs.read("refs/heads/fresh"), results };
      }),
    );

    assert.ok(state.results.every((result) => !result.applied));
    assert.equal(state.fresh, null, "an atomic batch must not partially apply");
  });

  it("applies the entries that match when not atomic", async () => {
    const state = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.apply([{ name: "refs/heads/taken", value: a, expected: null }]);

        const results = yield* refs.apply([
          { name: "refs/heads/fresh", value: b, expected: null },
          { name: "refs/heads/taken", value: b, expected: null },
        ]);

        return { fresh: yield* refs.read("refs/heads/fresh"), results };
      }),
    );

    assert.equal(state.results.filter((result) => result.applied).length, 1);
    assert.equal(state.fresh, b);
  });

  it("rejects malformed ref names before writing anything", async () => {
    const state = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        const failure = yield* Effect.flip(
          refs.apply([
            { name: "refs/heads/ok", value: a, expected: null },
            { name: "bad name", value: b, expected: null },
          ]),
        );
        return { failure, ok: yield* refs.read("refs/heads/ok") };
      }),
    );

    assert.equal(state.failure._tag, "Invalid");
    assert.equal(state.ok, null);
  });

  it("resolves HEAD through to an oid", async () => {
    const resolved = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.setHead("refs/heads/trunk");
        yield* refs.apply([{ name: "refs/heads/trunk", value: a, expected: null }]);
        return yield* refs.resolve("HEAD");
      }),
    );

    assert.equal(resolved, a);
  });

  it("records a reflog entry per applied update", async () => {
    const entries = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.apply([
          { name: "refs/heads/main", value: a, expected: null, reason: "create" },
        ]);
        yield* refs.apply([{ name: "refs/heads/main", value: b, expected: a, reason: "advance" }]);
        return yield* refs.reflog("refs/heads/main");
      }),
    );

    assert.equal(entries.length, 2);
    assert.equal(entries[0]?.from, null);
    assert.equal(entries[0]?.to, a);
    assert.equal(entries[1]?.from, a);
    assert.equal(entries[1]?.to, b);
    assert.equal(entries[1]?.message, "advance");
  });

  it("filters list by prefix", async () => {
    const listed = await run(
      Effect.gen(function* () {
        const refs = yield* RefStore;
        yield* refs.apply([
          { name: "refs/heads/main", value: a, expected: null },
          { name: "refs/tags/v1", value: b, expected: null },
        ]);
        return yield* refs.list("refs/tags/");
      }),
    );

    assert.equal(listed.length, 1);
    assert.equal(listed[0]?.[0], "refs/tags/v1");
  });
});
