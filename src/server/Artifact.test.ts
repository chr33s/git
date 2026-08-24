import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { decide, parseRange } from "./Artifact.ts";

describe("Artifact range parsing", () => {
  it.effect("treats a missing header as the whole object", () =>
    Effect.sync(() => {
      assert.deepEqual(parseRange(null, 100), { kind: "all" });
      assert.deepEqual(parseRange("", 100), { kind: "all" });
    }),
  );

  it.effect("parses closed, open, and suffix ranges", () =>
    Effect.sync(() => {
      assert.deepEqual(parseRange("bytes=0-49", 100), {
        kind: "range",
        range: { offset: 0, length: 50 },
      });
      assert.deepEqual(parseRange("bytes=50-", 100), {
        kind: "range",
        range: { offset: 50, length: 50 },
      });
      assert.deepEqual(parseRange("bytes=-20", 100), {
        kind: "range",
        range: { offset: 80, length: 20 },
      });
    }),
  );

  it.effect("rejects unsatisfiable and multi ranges", () =>
    Effect.sync(() => {
      assert.equal(parseRange("bytes=100-200", 100).kind, "unsatisfiable");
      assert.equal(parseRange("bytes=0-10,20-30", 100).kind, "unsatisfiable");
      assert.equal(parseRange("items=0-1", 100).kind, "unsatisfiable");
    }),
  );

  it.effect("honours If-None-Match and If-Range", () =>
    Effect.sync(() => {
      const etag = "abc";
      const none = decide(
        new Request("http://host/a", { headers: { "if-none-match": '"abc"' } }),
        10,
        etag,
      );
      assert.equal(none.kind, "notModified");

      const ranged = decide(
        new Request("http://host/a", {
          headers: { range: "bytes=0-3", "if-range": '"abc"' },
        }),
        10,
        etag,
      );
      assert.equal(ranged.kind, "range");

      const stale = decide(
        new Request("http://host/a", {
          headers: { range: "bytes=0-3", "if-range": '"old"' },
        }),
        10,
        etag,
      );
      assert.equal(stale.kind, "all");
    }),
  );
});
