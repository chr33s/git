/**
 * Tests.
 *
 * Today: 22 `*.test.ts` files on `node:test`, run with
 * `--test-concurrency=1` because they share global state, plus
 * `test.helpers.ts` which spawns `wrangler dev` on port 8080 for `e2e.test.ts`.
 * Timing-dependent paths (webhook retry backoff, gc grace period) either sleep
 * or are untested.
 *
 * Sketch: the same suites, but the environment is a layer. No spawned server
 * for the HTTP tests — `HttpApiTest` drives the real handler in-process — and
 * time is controlled, so the retry schedule is asserted rather than waited out.
 */
import { assert, describe, it } from "@effect/vitest";
import { Effect, Fiber, Layer, type Scope } from "effect";
import { TestClock } from "effect/testing";
import { HttpApiTest } from "effect/unstable/httpapi";
import { memory } from "../adapters/Local.sketch.ts";
import * as GitRepository from "../git/Repository.sketch.ts";
import { Hooks, Repository } from "../git/Repository.sketch.ts";
import { api } from "../server/Api.sketch.ts";
import * as App from "../server/App.sketch.ts";

const noHooks = Layer.succeed(Hooks, {
  preReceive: () => Effect.void,
  update: () => Effect.void,
  postReceive: () => Effect.void,
});

const TestLive = GitRepository.layer.pipe(Layer.provide(Layer.mergeAll(memory, noHooks)));

describe("Repository", () => {
  it.effect("commit fails with RefConflict when the head moved", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const first = yield* repository.commit({
        branch: "main",
        tree: yield* emptyTree,
        message: "one",
        author: alice,
      });

      const result = yield* repository
        .commit({
          branch: "main",
          tree: yield* emptyTree,
          message: "two",
          // deliberately stale
          expected: null,
          author: alice,
        })
        .pipe(Effect.flip);

      // The assertion is on a tagged value, not a message string or a `.code`
      // field — this is what the API contract now guarantees to clients too.
      assert.strictEqual(result._tag, "RefConflict");
      if (result._tag === "RefConflict") assert.strictEqual(result.actual, first);
    }).pipe(Effect.provide(TestLive)),
  );

  it.effect("webhook retry backs off without real time passing", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(deliverToFailingSubscriber);
      // Four retries over ~15s of virtual time; the suite stays instant.
      yield* TestClock.adjust("30 seconds");
      yield* Fiber.await(fiber);
      assert.strictEqual(yield* attemptCount, 5);
    }),
  );
});

describe("app", () => {
  // The portability claim, as a test: no Worker, no node:http, no host at all —
  // just the app bound to in-memory stores and a `Request`.
  it.effect("serves the ref advertisement over in-memory stores", () =>
    Effect.gen(function* () {
      const { dispose, handler } = App.forRepo(Layer.mergeAll(memory, testHost));
      const response = yield* Effect.promise(() =>
        handler(new Request("http://x/demo/info/refs?service=git-upload-pack")),
      );
      assert.strictEqual(response.status, 200);
      yield* Effect.promise(dispose);
    }),
  );
});

describe("api", () => {
  // Runs the actual router and codecs in-process: no port, no wrangler, no
  // global setup file — and it exercises the same handler the DO serves.
  it.effect("POST /api/:repo/branches/create", () =>
    Effect.gen(function* () {
      const client = yield* HttpApiTest.groups(api, ["refs"]);
      const response = yield* client.refs.branch({
        params: { repo: "demo" },
        payload: { name: "feature", base: "refs/heads/main" },
      });
      assert.strictEqual(response.name, "feature");
    }).pipe(withApi),
  );
});

/**
 * The harness layer, elided: `Api.layer` over the in-memory stores, plus the
 * etag/filesystem/path services `HttpApiTest` needs to build responses. Written
 * once in a test helper — the point is that it is a value, so a suite that
 * wants a different backend swaps one argument.
 */
declare const withApi: <A, E, R>(
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, Scope.Scope>;

/** A `RepoHost` that isolates nothing and serializes nothing — a test is alone. */
declare const testHost: Layer.Layer<Exclude<App.Env, import("../git/Store.ts").ServerStores>>;

declare const emptyTree: Effect.Effect<import("../git/Store.ts").Oid>;
declare const alice: import("../git/Format.ts").Signature;
declare const deliverToFailingSubscriber: Effect.Effect<void>;
declare const attemptCount: Effect.Effect<number>;
