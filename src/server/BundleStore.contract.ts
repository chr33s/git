/**
 * BundleStore contract, run against every backend the way ObjectStore is.
 */
import assert from "node:assert/strict";

import { Effect, Stream } from "effect";

import { emptyManifest } from "./BundleFormat.ts";
import { BundleStore } from "./BundleStore.ts";

export interface Backend {
  readonly run: <A, E>(effect: Effect.Effect<A, E, BundleStore>) => Promise<A>;
}

export interface Runner {
  readonly describe: (name: string, body: () => void) => void;
  readonly it: (name: string, body: () => Promise<void> | void) => void;
}

export const bundleStoreContract = (label: string, backend: Backend, runner: Runner): void => {
  const { describe, it } = runner;
  const { run } = backend;

  describe(`${label}: BundleStore contract`, () => {
    it("starts with no manifest", async () => {
      const listed = await run(Effect.flatMap(BundleStore, (store) => store.list("repo")));
      assert.equal(listed, null);
    });

    it("writes bytes, stats them, and reads them back", async () => {
      const result = await run(
        Effect.gen(function* () {
          const store = yield* BundleStore;
          const payload = new TextEncoder().encode("bundle-bytes");
          const written = yield* store.write("full/1-test.bundle", Stream.fromIterable([payload]));
          const stat = yield* store.stat("full/1-test.bundle");
          const chunks = yield* Stream.runCollect(yield* store.read("full/1-test.bundle"));
          return {
            bytes: written.bytes,
            checksum: written.checksum,
            stat,
            text: new TextDecoder().decode(chunks[0]),
          };
        }),
      );
      assert.equal(result.bytes, 12);
      assert.equal(result.checksum.length, 40);
      assert.equal(result.stat?.bytes, 12);
      assert.equal(result.text, "bundle-bytes");
    });

    it("serves a byte range", async () => {
      const text = await run(
        Effect.gen(function* () {
          const store = yield* BundleStore;
          yield* store.write(
            "full/2-range.bundle",
            Stream.fromIterable([new TextEncoder().encode("abcdef")]),
          );
          const chunks = yield* Stream.runCollect(
            yield* store.read("full/2-range.bundle", { offset: 2, length: 3 }),
          );
          return new TextDecoder().decode(chunks[0]);
        }),
      );
      assert.equal(text, "cde");
    });

    it("moves an artifact and forgets the old key", async () => {
      const state = await run(
        Effect.gen(function* () {
          const store = yield* BundleStore;
          yield* store.write("tmp/x", Stream.fromIterable([new Uint8Array([1, 2, 3])]));
          yield* store.move("tmp/x", "full/3-moved.bundle");
          return {
            from: yield* store.stat("tmp/x"),
            to: yield* store.stat("full/3-moved.bundle"),
          };
        }),
      );
      assert.equal(state.from, null);
      assert.equal(state.to?.bytes, 3);
    });

    it("publishes a manifest atomically relative to unreferenced writes", async () => {
      const listed = await run(
        Effect.gen(function* () {
          const store = yield* BundleStore;
          yield* store.write("tmp/unadvertised", Stream.fromIterable([new Uint8Array([9])]));
          yield* store.publish("repo", emptyManifest());
          return yield* store.list("repo");
        }),
      );
      assert.equal(listed?.version, 1);
      assert.equal(listed?.families[0]?.full, null);
    });

    it("lists stored ids including unreferenced ones", async () => {
      const ids = await run(
        Effect.gen(function* () {
          const store = yield* BundleStore;
          yield* store.write("tmp/a", Stream.fromIterable([new Uint8Array([1])]));
          yield* store.write("full/1-b.bundle", Stream.fromIterable([new Uint8Array([2])]));
          return yield* store.listIds("repo");
        }),
      );
      assert.ok(ids.includes("tmp/a"));
      assert.ok(ids.includes("full/1-b.bundle"));
    });
  });
};
