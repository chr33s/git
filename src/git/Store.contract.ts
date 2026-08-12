/**
 * The storage contract, as one suite every backend has to pass.
 *
 * "Atomic ref update" is written once here and run against every
 * implementation, so a backend either provides the guarantee or fails the
 * suite — it cannot quietly mean something different per backend.
 *
 * The suite is parameterised over both the backend *and* the test runner, so
 * the same assertions run under `node:test` for the in-memory and filesystem
 * backends, and inside workerd via the `Conformance.ts` collector for the
 * Cloudflare one. A contract that only holds on the runtime it was written for
 * is not much of a contract.
 *
 * Not a `*.test.ts` file: it is imported by `Memory.test.ts`, `Node.test.ts`
 * and `Durable.ts`'s conformance route.
 */
import assert from "node:assert/strict";

import { Effect, Stream } from "effect";

import { ObjectStore, type Oid, RefStore } from "./Store.ts";

const a = "a".repeat(40) as Oid;
const b = "b".repeat(40) as Oid;

export interface Backend {
  /**
   * Run one effect against a fresh, empty pair of stores.
   *
   * Freshness is the backend's business — a temp directory here, isolated
   * per-test storage there. Each test calls this exactly once.
   */
  readonly run: <A, E>(effect: Effect.Effect<A, E, ObjectStore | RefStore>) => Promise<A>;
}

/** Just enough of a runner for this suite; `node:test` and Vitest both fit. */
export interface Runner {
  readonly describe: (name: string, body: () => void) => void;
  readonly it: (name: string, body: () => Promise<void> | void) => void;
}

export const storeContract = (label: string, backend: Backend, runner: Runner): void => {
  const { describe, it } = runner;
  const { run } = backend;

  describe(`${label}: ObjectStore contract`, () => {
    it("writes content-addressed and reads back", async () => {
      const result = await run(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const oid = yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("hello\n"),
          });
          const read = yield* objects.read(oid);
          return { oid, text: new TextDecoder().decode(read.data), type: read.type };
        }),
      );

      // The oid real git produces for this blob.
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

    it("preserves the object type through a round trip", async () => {
      const type = await run(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const oid = yield* objects.write({ type: "commit", data: new Uint8Array([1, 2, 3]) });
          return (yield* objects.read(oid)).type;
        }),
      );

      assert.equal(type, "commit");
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

    it("does not hand out a reference to its own storage", async () => {
      const text = await run(
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

      assert.equal(text, "stable");
    });

    it("streams, lists and deletes", async () => {
      const result = await run(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const oid = yield* objects.write({
            type: "blob",
            data: new TextEncoder().encode("chunked"),
          });

          const chunks = yield* Stream.runCollect(yield* objects.readStream(oid));
          const listed = yield* Stream.runCollect(objects.list);

          yield* objects.delete(oid);
          return {
            listed: listed.length,
            present: yield* objects.has(oid),
            text: new TextDecoder().decode(chunks[0]),
          };
        }),
      );

      assert.equal(result.text, "chunked");
      assert.equal(result.listed, 1);
      assert.equal(result.present, false);
    });
  });

  describe(`${label}: RefStore contract`, () => {
    it("creates a ref only when it does not exist", async () => {
      const state = await run(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          const created = yield* refs.apply([
            { name: "refs/heads/main", value: a, expected: null },
          ]);
          const again = yield* refs.apply([{ name: "refs/heads/main", value: b, expected: null }]);
          return { again, created, current: yield* refs.read("refs/heads/main") };
        }),
      );

      assert.equal(state.created[0]?.applied, true);
      assert.equal(state.again[0]?.applied, false);
      assert.equal(state.again[0]?.current, a);
      assert.equal(state.current, a);
    });

    it("treats an undefined expectation as 'don't care'", async () => {
      const state = await run(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          yield* refs.apply([{ name: "refs/heads/main", value: a, expected: null }]);
          const forced = yield* refs.apply([{ name: "refs/heads/main", value: b }]);
          return { forced, current: yield* refs.read("refs/heads/main") };
        }),
      );

      assert.equal(state.forced[0]?.applied, true);
      assert.equal(state.current, b);
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
              // stale: this ref already exists
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
          yield* refs.apply([
            { name: "refs/heads/main", value: b, expected: a, reason: "advance" },
          ]);
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
};
