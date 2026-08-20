import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { earliestUnique } from "./Lineage.ts";

const author: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date("2026-08-20T00:00:00Z"),
  offset: 0,
};

const live = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

describe("repository lineage", () => {
  it.effect("uses the root for an origin and the first unique commit for a fork", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const root = yield* repository.commit({
        branch: "refs/heads/main",
        tree: EMPTY_TREE_OID,
        message: "root",
        author,
      });
      const upstream = yield* repository.commit({
        branch: "refs/heads/main",
        tree: EMPTY_TREE_OID,
        message: "upstream",
        author,
      });
      yield* repository.branch({ name: "fork", base: upstream });
      const first = yield* repository.commit({
        branch: "refs/heads/fork",
        tree: EMPTY_TREE_OID,
        message: "fork one",
        author,
      });
      const head = yield* repository.commit({
        branch: "refs/heads/fork",
        tree: EMPTY_TREE_OID,
        message: "fork two",
        author,
      });

      assert.equal(yield* earliestUnique({ head: upstream }), `sha1:${root}`);
      assert.equal(yield* earliestUnique({ head, upstream }), `sha1:${first}`);
    }).pipe(Effect.provide(live)),
  );
});
