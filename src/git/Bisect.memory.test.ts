import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

const run = promisify(execFile);
const Measurement = Schema.Struct({
  remaining: Schema.Int,
  midpoint: Schema.Boolean,
  addedBufferBytes: Schema.Int,
});

describe("bisect memory", () => {
  it.live("bisects 40,000 suspects without a quadratic ancestry buffer", () =>
    Effect.promise(async () => {
      // A separate process keeps the memory measurement independent of other
      // concurrently running tests. Use the actual repository and object store.
      const source = `
        import { Effect, Layer } from ${JSON.stringify(import.meta.resolve("effect"))};
        import { next } from ${JSON.stringify(new URL("./Bisect.ts", import.meta.url).href)};
        import { stores } from ${JSON.stringify(new URL("./Memory.ts", import.meta.url).href)};
        import * as Repository from ${JSON.stringify(new URL("./Repository.ts", import.meta.url).href)};
        import { EMPTY_TREE_OID } from ${JSON.stringify(new URL("./Format.ts", import.meta.url).href)};
        const layer = Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores));
        await Effect.runPromise(Effect.gen(function* () {
          const repository = yield* Repository.Repository;
          let head;
          let first;
          let midpoint;
          for (let index = 0; index <= 40000; index++) {
            head = yield* repository.commitTree({
              tree: EMPTY_TREE_OID,
              parents: head === undefined ? [] : [head],
              message: String(index),
              author: { name: 'T', email: 't@e.com', at: new Date(1700000000000), offset: 0 },
            });
            first ??= head;
            if (index === 20000) midpoint = head;
          }
          globalThis.gc();
          const before = process.memoryUsage().arrayBuffers;
          const result = yield* next({ bad: head, good: [first] });
          console.log(JSON.stringify({
            remaining: result.remaining,
            midpoint: result.commit === midpoint,
            addedBufferBytes: process.memoryUsage().arrayBuffers - before,
          }));
        }).pipe(Effect.provide(layer)));
      `;
      const { stdout } = await run(process.execPath, [
        "--expose-gc",
        "--input-type=module",
        "-e",
        source,
      ]);
      const result = await Effect.runPromise(
        Schema.decodeUnknownEffect(Measurement)(JSON.parse(stdout)),
      );
      assert.equal(result.remaining, 40_000);
      assert.equal(result.midpoint, true);
      assert.ok(
        result.addedBufferBytes < 64 * 1024 * 1024,
        `bisect added ${result.addedBufferBytes} buffer bytes for a 40,000-commit interval`,
      );
    }),
  );
});
