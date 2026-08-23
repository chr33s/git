import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { hashObject } from "./Format.ts";
import { file } from "./Search.node.ts";
import { SearchIndex } from "./Search.ts";

describe("Node SearchIndex", () => {
  it.effect("restores an indexed blob after a fresh layer is built", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-search-"));
      try {
        const data = new TextEncoder().encode("Repository search\n");
        const oid = await Effect.runPromise(hashObject({ type: "blob", data }));
        await Effect.runPromise(
          Effect.gen(function* () {
            const index = yield* SearchIndex;
            yield* index.observe(oid, data);
            yield* index.flush;
          }).pipe(Effect.provide(file(root))),
        );
        const candidates = await Effect.runPromise(
          Effect.gen(function* () {
            const index = yield* SearchIndex;
            // Through the port: posting chunks load on demand from disk.
            return yield* index.candidates("repository", true);
          }).pipe(Effect.provide(file(root))),
        );
        assert.equal(candidates?.has(0), true);
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );
});
