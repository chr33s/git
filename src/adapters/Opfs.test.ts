/**
 * The OPFS backend against the same storage contract as every other backend.
 *
 * Node has no OPFS, so the tests run over an in-memory fake of the
 * `FileSystemDirectoryHandle` surface the adapter actually uses — handle
 * traversal, `createWritable`, `removeEntry`, entry iteration. What the fake
 * does not fake is the contract: the suite is the same one DO SQLite and the
 * real filesystem pass.
 */
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import assert from "node:assert/strict";

import { hashObject } from "../git/Format.ts";
import { SearchIndex } from "../git/Search.ts";
import { storeContract } from "../git/Store.contract.ts";
import { fakeRoot } from "./Opfs.fake.ts";
import { searchIndex, stores } from "./Opfs.ts";

storeContract(
  "OPFS",
  {
    // A fresh in-memory origin per test, like a fresh browser profile.
    run: (effect) => Effect.runPromise(effect.pipe(Effect.provide(stores(fakeRoot())))),
  },
  { describe, it },
);

describe("OPFS SearchIndex", () => {
  it.effect("restores an indexed blob from a fresh layer over the same root", () =>
    Effect.gen(function* () {
      const root = fakeRoot();
      const data = new TextEncoder().encode("Repository search\n");
      const oid = yield* hashObject({ type: "blob", data });
      yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        yield* index.observe(oid, data);
        yield* index.flush;
      }).pipe(Effect.provide(searchIndex(root)));
      const candidates = yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        return yield* index.candidates("repository", true);
      }).pipe(Effect.provide(searchIndex(root)));
      assert.equal(candidates?.size, 1);
    }),
  );

  it.effect("keeps the index in memory only past the host hard limit", () =>
    Effect.gen(function* () {
      const root = fakeRoot();
      const data = new TextEncoder().encode("Repository search\n");
      const oid = yield* hashObject({ type: "blob", data });
      const candidates = yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        yield* index.observe(oid, data);
        yield* index.flush;
        return yield* index.candidates("repository", true);
      }).pipe(Effect.provide(searchIndex(root, { hardLimitBytes: 64 })));
      assert.equal(candidates?.size, 1);
      const second = yield* Effect.gen(function* () {
        const index = yield* SearchIndex;
        return index.index.get(oid);
      }).pipe(Effect.provide(searchIndex(root, { hardLimitBytes: 64 })));
      // Nothing was persisted, so a fresh layer over the same root is cold.
      assert.equal(second, undefined);
    }),
  );
});
