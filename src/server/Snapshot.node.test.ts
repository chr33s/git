import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Result } from "effect";

import { refStore } from "../git/Node.ts";
import { RefStore } from "../git/Store.ts";
import * as Snapshot from "./Snapshot.ts";

describe("snapshot recovery on disk", () => {
  it.effect("reports a refused ref write and preserves HEAD and the other refs", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "snapshot-restore-"));
      try {
        // An empty ref directory can remain after its child branches are
        // deleted. It is not a ref, but rename cannot replace it with a file.
        await fs.mkdir(path.join(root, "refs/heads/main"), { recursive: true });
        await fs.writeFile(path.join(root, "HEAD"), "ref: refs/heads/original\n");
        const entry: Snapshot.JournalEntry = {
          version: 1,
          seq: 1,
          publishedAt: "2026-09-07T00:00:00.000Z",
          anonymousRead: true,
          head: "refs/heads/main",
          refs: [
            { name: "refs/heads/topic", oid: "1".repeat(40) },
            { name: "refs/heads/main", oid: "2".repeat(40) },
          ],
          changes: [],
        };
        await Effect.runPromise(
          Effect.gen(function* () {
            const refs = yield* RefStore;
            const result = yield* Snapshot.restore(entry).pipe(Effect.result);
            assert.ok(
              Result.isFailure(result),
              "recovery must not report an incomplete restore as success",
            );
            assert.equal(result.failure._tag, "StorageFailure");
            assert.equal(yield* refs.read("refs/heads/topic"), null);
            assert.equal(yield* refs.read("refs/heads/main"), null);
            assert.equal(yield* refs.head, "refs/heads/original");
          }).pipe(Effect.provide(refStore(root))),
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
