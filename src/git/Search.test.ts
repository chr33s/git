import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { hashObject } from "./Format.ts";
import { BlobIndex } from "./Search.ts";

describe("Search", () => {
  it.effect("round-trips versioned postings and rejects a changed snapshot", () =>
    Effect.gen(function* () {
      const oid = yield* hashObject({
        type: "blob",
        data: new TextEncoder().encode("Repository search\n"),
      });
      const index = new BlobIndex();
      index.observe(oid, new TextEncoder().encode("Repository search\n"));
      const restored = BlobIndex.restore(index.snapshot());
      if (restored === null) assert.fail("expected a valid snapshot");
      assert.equal(restored.candidates("repository", true)?.has(0), true);
      assert.equal(restored.forget(oid), true);
      assert.equal(restored.candidates("repository", true)?.size, 0);

      const corrupt = index.snapshot().slice();
      const position = corrupt.length - 2;
      corrupt[position] = (corrupt[position] ?? 0) ^ 1;
      assert.equal(BlobIndex.restore(corrupt), null);
    }),
  );
});
