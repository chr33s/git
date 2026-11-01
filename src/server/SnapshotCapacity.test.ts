import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Result } from "effect";
import { Miniflare } from "miniflare";
import { StorageFailure } from "../git/Error.ts";
import * as Capacity from "./SnapshotCapacity.ts";
import * as Publisher from "./SnapshotPublisher.ts";

function emptyGet<T>(key: string): Promise<T | undefined>;
function emptyGet<T>(keys: string[]): Promise<Map<string, T>>;
async function emptyGet<T>(key: string | string[]): Promise<T | undefined | Map<string, T>> {
  return Array.isArray(key) ? new Map<string, T>() : undefined;
}

it("snapshot reads enforce metadata and streamed byte bounds and cancel refused bodies", async () => {
  for (const [size, chunks, limit, accepted] of [
    [4, [2, 2], 4, true],
    [5, [5], 4, false],
    [4, [3, 2], 4, false],
    [4, [2], 4, false],
    [0, [], 4, true],
    [-1, [], 4, false],
    [Infinity, [], 4, false],
  ] as const) {
    let cancelled = false;
    let delivered = 0;
    const body = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          const next = chunks[delivered++];
          if (next === undefined) controller.close();
          else controller.enqueue(new Uint8Array(next).fill(7));
        },
        cancel() {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
    if (accepted) {
      const bytes = await Capacity.read({ size, body }, limit);
      assert.equal(bytes.byteLength, size);
      assert.ok(bytes.every((byte) => byte === 7));
    } else {
      await assert.rejects(Capacity.read({ size, body }, limit), (error: Error) => {
        assert.ok(error instanceof StorageFailure);
        return true;
      });
      if (size > limit || size < 0) {
        assert.equal(delivered, 0, "metadata refusal must not read the body");
        assert.equal(cancelled, true);
      }
      if (size === 4 && chunks.length === 2) assert.equal(cancelled, true);
    }
    assert.equal(body.locked, false);
  }
});

it.effect("snapshot byte configuration validates defaults and bounds", () =>
  Effect.gen(function* () {
    assert.equal(
      yield* Capacity.configuration.pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
      ),
      16 * 1024 ** 2,
    );
    for (const value of [0, -1, 0.5, 64 * 1024 ** 2 + 1, "invalid"])
      assert.ok(
        Result.isFailure(
          yield* Effect.result(Capacity.configuration).pipe(
            Effect.provide(
              ConfigProvider.layer(ConfigProvider.fromUnknown({ GIT_CF_SNAPSHOT_BYTES: value })),
            ),
          ),
        ),
      );
  }),
);

it("snapshot publisher bounds reads and writes on real local R2", async () => {
  const mf = new Miniflare({
    workers: [
      {
        config: {
          name: "snapshot-capacity",
          compatibilityDate: "2025-12-10",
          manifest: {
            mainModule: "worker.js",
            modules: {
              "worker.js": {
                type: "esm",
                contents: 'export default { fetch() { return new Response("fixture"); } };',
              },
            },
          },
          env: { SNAPSHOTS: { type: "r2", name: "snapshot-capacity" } },
        },
      },
    ],
  });
  try {
    const bucket = await mf.getR2Bucket("SNAPSHOTS");
    // SAFETY: Miniflare exposes the same R2 methods; its DOM declarations differ from workerd's.
    const native: R2Bucket = bucket as never;
    const store = await Effect.runPromise(
      Publisher.Store.pipe(
        Effect.provide(
          Publisher.cloudflare(native, { get: emptyGet, put: async () => undefined }, 4),
        ),
      ),
    );
    await Effect.runPromise(store.write("latest", new Uint8Array([1, 2, 3, 4])));
    assert.deepEqual(await Effect.runPromise(store.read("latest")), new Uint8Array([1, 2, 3, 4]));
    const rejected = await Effect.runPromise(
      Effect.result(store.write("latest", new Uint8Array(5))),
    );
    assert.ok(Result.isFailure(rejected));
    assert.equal((await bucket.head("latest"))?.size, 4);
    await bucket.put("legacy", new Uint8Array(5));
    assert.ok(Result.isFailure(await Effect.runPromise(Effect.result(store.read("legacy")))));
    assert.equal((await bucket.head("legacy"))?.size, 5, "oversized persisted data is preserved");
    await Effect.runPromise(store.remove("legacy"));
    assert.equal(await bucket.head("legacy"), null);
  } finally {
    await mf.dispose();
  }
});
