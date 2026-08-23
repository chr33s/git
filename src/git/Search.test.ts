import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { hashObject } from "./Format.ts";
import {
  BlobIndex,
  continuation,
  fuzzy,
  nextContinuation,
  persistent,
  SearchIndex,
} from "./Search.ts";

/** In-memory chunk storage, so the shared host persistence runs in unit tests. */
const fakeIo = (hardLimitBytes = 250 * 1024 * 1024, chunkTargetBytes?: number) => {
  const files = new Map<string, Uint8Array>();
  const reads: string[] = [];
  const writes = new Map<string, number>();
  return {
    files,
    reads,
    writes,
    io: {
      softLimitBytes: Math.floor(hardLimitBytes / 2),
      hardLimitBytes,
      chunkTargetBytes,
      read: (name: string) =>
        Effect.sync(() => {
          reads.push(name);
          return files.get(name) ?? null;
        }),
      write: (name: string, bytes: Uint8Array) =>
        Effect.sync(() => {
          writes.set(name, (writes.get(name) ?? 0) + 1);
          files.set(name, bytes);
        }),
      remove: (name: string) => Effect.sync(() => void files.delete(name)),
      list: Effect.sync(() => [...files.keys()]),
    },
  };
};

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

      const chunked = yield* Effect.promise(() => index.persisted());
      if (chunked === null) assert.fail("expected a persistable index");
      const chunks = new Map(chunked.chunks.map((chunk) => [chunk.name, chunk.bytes]));
      assert.equal(
        BlobIndex.restorePersisted(chunked.manifest, chunks)
          ?.candidates("repository", true)
          ?.has(0),
        true,
      );
      const first = chunked.chunks[0];
      if (first === undefined) assert.fail("expected a blob-table chunk");
      const changed = first.bytes.slice();
      changed[0] = (changed[0] ?? 0) ^ 1;
      chunks.set(first.name, changed);
      assert.equal(BlobIndex.restorePersisted(chunked.manifest, chunks), null);
      const old = new TextEncoder().encode(
        JSON.stringify({ ...JSON.parse(new TextDecoder().decode(chunked.manifest)), version: 1 }),
      );
      assert.equal(
        BlobIndex.restorePersisted(
          old,
          new Map(chunked.chunks.map((chunk) => [chunk.name, chunk.bytes])),
        ),
        null,
      );

      const later = yield* hashObject({
        type: "blob",
        data: new TextEncoder().encode("later searchable blob\n"),
      });
      index.observe(later, new TextEncoder().encode("later searchable blob\n"));
      index.forget(oid);
      const compacted = yield* Effect.promise(() => index.persisted());
      if (compacted === null) assert.fail("expected a persistable index");
      const reloaded = BlobIndex.restorePersisted(
        compacted.manifest,
        new Map(compacted.chunks.map((chunk) => [chunk.name, chunk.bytes])),
      );
      assert.equal(reloaded?.candidates("later", true)?.has(1), true);
    }),
  );

  it.effect("loads only the posting chunks a query's bigrams touch", () =>
    Effect.gen(function* () {
      const data = new TextEncoder().encode("Repository search\n");
      const oid = yield* hashObject({ type: "blob", data });
      const store = fakeIo();

      // Build and checkpoint through the shared host path.
      yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        yield* index.observe(oid, data);
        yield* index.flush;
      }).pipe(Effect.provide(persistent(store.io)));
      assert.equal(store.files.has("manifest.json"), true);

      // A fresh layer reads the manifest and blob chunks only; the posting
      // chunk is read when the first query's bigrams ask for it.
      const before = store.reads.length;
      const found = yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        const startupReads = store.reads.slice(before);
        assert.equal(
          startupReads.some((name) => name.startsWith("postings-")),
          false,
        );
        return yield* index.candidates("repository", true);
      }).pipe(Effect.provide(persistent(store.io)));
      assert.equal(found?.size, 1);
      assert.equal(
        store.reads.slice(before).some((name) => name.startsWith("postings-")),
        true,
      );

      // A corrupt posting chunk makes the query take the full verifier path,
      // never an empty answer.
      for (const [name, bytes] of store.files) {
        if (!name.startsWith("postings-")) continue;
        const broken = bytes.slice();
        broken[0] = (broken[0] ?? 0) ^ 1;
        store.files.set(name, broken);
      }
      const degraded = yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        return yield* index.candidates("repository", true);
      }).pipe(Effect.provide(persistent(store.io)));
      assert.equal(degraded, null);
    }),
  );

  it.effect("keeps a too-large index in memory only past the hard limit", () =>
    Effect.gen(function* () {
      const store = fakeIo(64);
      yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        yield* index.observe(
          yield* hashObject({ type: "blob", data: new TextEncoder().encode("oversized\n") }),
          new TextEncoder().encode("oversized\n"),
        );
        yield* index.flush;
        // The warm index keeps answering even though nothing was persisted.
        assert.equal(index.index.candidates("oversized", true)?.size, 1);
      }).pipe(Effect.provide(persistent(store.io)));
      assert.equal(store.files.size, 0);
    }),
  );

  it.effect("rewrites only the chunks new blobs landed in", () =>
    Effect.gen(function* () {
      // A 120-byte target gives each ~70-byte blob row its own chunk, so an
      // appended blob must leave the earlier chunk files untouched.
      const store = fakeIo(undefined, 120);
      const observe = (content: string) =>
        Effect.gen(function* () {
          const data = new TextEncoder().encode(content);
          const index = yield* SearchIndex;
          yield* index.observe(yield* hashObject({ type: "blob", data }), data);
          yield* index.flush;
        });
      const layer = persistent(store.io);
      yield* observe("first blob content\n").pipe(Effect.provide(layer));
      yield* observe("second blob content\n").pipe(Effect.provide(layer));
      const before = new Map(store.writes);
      yield* observe("third blob content\n").pipe(Effect.provide(layer));
      assert.equal(store.writes.get("blobs-0"), before.get("blobs-0"));
      assert.equal(store.writes.get("blobs-1"), before.get("blobs-1"));
      assert.equal(
        (store.writes.get("manifest.json") ?? 0) > (before.get("manifest.json") ?? 0),
        true,
      );
    }),
  );

  it.effect("deletes chunks a compacted snapshot no longer references", () =>
    Effect.gen(function* () {
      const store = fakeIo(undefined, 120);
      const layer = persistent(store.io);
      const data = (content: string) => new TextEncoder().encode(content);
      const oid = (content: string) => hashObject({ type: "blob", data: data(content) });
      yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        yield* index.observe(yield* oid("keep me\n"), data("keep me\n"));
        yield* index.observe(yield* oid("collect me\n"), data("collect me\n"));
        yield* index.flush;
      }).pipe(Effect.provide(layer));
      assert.equal(store.files.has("blobs-1"), true);

      // What GC does: forget the collected blob, then the next flush publishes
      // a smaller snapshot whose manifest no longer names `blobs-1`.
      yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        yield* index.forget([yield* oid("collect me\n")]);
        yield* index.flush;
      }).pipe(Effect.provide(layer));
      assert.equal(store.files.has("blobs-1"), false);
      assert.equal(store.files.has("manifest.json"), true);
    }),
  );

  it.effect("keeps approximate ranges separate and refuses a cursor from another scope", () =>
    Effect.gen(function* () {
      assert.deepEqual(fuzzy("rpt", "Repository")?.ranges, [
        { start: 0, end: 1 },
        { start: 2, end: 3 },
        { start: 6, end: 7 },
      ]);
      const oid = yield* hashObject({ type: "blob", data: new Uint8Array() });
      const token = nextContinuation({
        pattern: "repository",
        revision: oid,
        path: undefined,
        fixed: true,
        ignoreCase: true,
        fuzzy: false,
        afterPath: "a.txt",
        afterLine: 1,
      });
      assert.equal(
        continuation({
          token,
          pattern: "other",
          revision: oid,
          path: undefined,
          fixed: true,
          ignoreCase: true,
          fuzzy: false,
        })._tag,
        "Failure",
      );
    }),
  );
});
