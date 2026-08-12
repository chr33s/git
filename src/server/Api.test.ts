/**
 * The JSON API, driven through its own derived client.
 *
 * `HttpApiTest` builds a client from the same `HttpApi` value the server
 * implements and runs it against the handlers in-process — no socket, no
 * spawned server. What this buys is the no-drift property: a payload or
 * error-shape change that broke the client would fail to compile here, and a
 * `RefConflict` comes back as a typed value in the failure channel, not as a
 * status code to interpret.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Effect, FileSystem, Layer, Path } from "effect";
import { Etag, HttpPlatform } from "effect/unstable/http";
import { HttpApiTest } from "effect/unstable/httpapi";

import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Api from "./Api.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const live = Layer.mergeAll(
  Api.handlers,
  HttpPlatform.layer.pipe(Layer.provide(FileSystem.layerNoop({}))),
  Etag.layerWeak,
  FileSystem.layerNoop({}),
  Path.layer,
).pipe(Layer.provideMerge(repository));

const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

describe("Api", () => {
  it("drives the derived client end to end, typed errors included", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const created = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "first", author: alice },
        });
        assert.match(created.oid, /^[0-9a-f]{40}$/);

        const read = yield* client.repo.read({ params: { repo: "r", oid: created.oid } });
        assert.equal(read.message, "first");
        assert.deepEqual(read.parents, []);

        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "second", author: alice },
        });
        const log = yield* client.repo.log({ params: { repo: "r", oid: second.oid } });
        assert.deepEqual(
          log.commits.map((commit) => commit.message),
          ["second", "first"],
        );

        const refs = yield* client.repo.refs({ params: { repo: "r" } });
        assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: second.oid }]);

        // The failure channel carries the domain error, decoded.
        const conflict = yield* client.repo
          .create({
            params: { repo: "r" },
            payload: { message: "third", author: alice, expected: null },
          })
          .pipe(Effect.flip);
        assert.equal(conflict._tag, "RefConflict");
      }).pipe(Effect.scoped, Effect.provide(live)) as Effect.Effect<void>,
    );
  });
});
