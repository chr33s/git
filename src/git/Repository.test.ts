import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Effect, Layer, Stream } from "effect";

import { stores } from "./Memory.ts";
import * as GitRepository from "./Repository.ts";
import { Hooks, Repository } from "./Repository.ts";
import { EMPTY_TREE_OID, type Signature } from "./Format.ts";
import { HookRejected } from "./Error.ts";
import { RefStore } from "./Store.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** Each test gets its own stores, so there is no shared global state to reset. */
const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | RefStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        // `provideMerge` so the test and `Repository` share one store instance,
        // rather than relying on layer memoization to make that true.
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ) as Effect.Effect<A, E>,
  );

describe("Repository", () => {
  it("commits onto an empty branch and reads it back", async () => {
    const commit = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "first",
          author: alice,
        });
        return yield* repository.readCommit(oid);
      }),
    );

    assert.equal(commit.message, "first");
    assert.equal(commit.parents.length, 0);
    assert.equal(commit.author.email, "alice@example.com");
  });

  it("chains commits and walks the log newest first", async () => {
    const messages = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });
        const second = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "two",
          author: alice,
        });

        const commits = yield* Stream.runCollect(repository.log(second));
        return commits.map((commit) => commit.message);
      }),
    );

    assert.deepEqual(messages, ["two", "one"]);
  });

  it("honours the log limit", async () => {
    const count = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        let head = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "0",
          author: alice,
        });
        for (let index = 1; index < 5; index++) {
          head = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: String(index),
            author: alice,
          });
        }
        const commits = yield* Stream.runCollect(repository.log(head, { limit: 2 }));
        return commits.length;
      }),
    );

    assert.equal(count, 2);
  });

  it("fails with RefConflict when the caller pinned a stale head", async () => {
    const error = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const first = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        const failure = yield* Effect.flip(
          repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "two",
            // stale on purpose: the branch exists now
            expected: null,
            author: alice,
          }),
        );

        return { failure, first };
      }),
    );

    assert.equal(error.failure._tag, "RefConflict");
    if (error.failure._tag === "RefConflict") {
      assert.equal(error.failure.actual, error.first);
      assert.equal(error.failure.expected, null);
    }
  });

  it("creates a branch from a base ref and refuses to clobber it", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const head = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        const created = yield* repository.branch({ name: "feature", base: "refs/heads/main" });
        const again = yield* Effect.flip(
          repository.branch({ name: "feature", base: "refs/heads/main" }),
        );

        return { again, created, head };
      }),
    );

    assert.equal(result.created, result.head);
    assert.equal(result.again._tag, "RefConflict");
  });

  it("rejects a branch off an unknown base", async () => {
    const error = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        return yield* Effect.flip(repository.branch({ name: "x", base: "refs/heads/nope" }));
      }),
    );

    assert.equal(error._tag, "Invalid");
  });

  it("lists refs after a commit", async () => {
    const refs = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });
        return yield* repository.refs;
      }),
    );

    assert.equal(refs.length, 1);
    assert.equal(refs[0]?.[0], "refs/heads/main");
  });

  it("writes and reads a tree", async () => {
    const entries = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("hello\n"));
        const tree = yield* repository.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
        return yield* repository.readTree(tree);
      }),
    );

    assert.equal(entries.length, 1);
    assert.equal(entries[0]?.name, "a.txt");
    assert.equal(entries[0]?.oid, "ce013625030ba8dba906f756967f9e9ca394464a");
  });

  it("reads the empty tree without it having been written", async () => {
    const entries = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        return yield* repository.readTree(EMPTY_TREE_OID);
      }),
    );

    assert.deepEqual(entries, []);
  });

  it("reports ObjectNotFound for a commit oid that is a blob", async () => {
    const error = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("x"));
        return yield* Effect.flip(repository.readCommit(blob));
      }),
    );

    assert.equal(error._tag, "ObjectNotFound");
  });
});

describe("Repository.receive", () => {
  const withHooks = <A, E>(
    hooks: Layer.Layer<Hooks>,
    effect: Effect.Effect<A, E, Repository | RefStore>,
  ) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(GitRepository.layer.pipe(Layer.provide(hooks), Layer.provideMerge(stores))),
      ) as Effect.Effect<A, E>,
    );

  it("applies a batch and reports each ref", async () => {
    const results = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        return yield* repository.receive(
          [
            { name: "refs/heads/a", value: oid, expected: null },
            { name: "refs/heads/b", value: oid, expected: null },
          ],
          { atomic: true },
        );
      }),
    );

    assert.equal(results.length, 2);
    assert.ok(results.every((result) => result.ok));
  });

  it("applies nothing when one ref in an atomic batch is stale", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* RefStore;
        const oid = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        const results = yield* repository.receive(
          [
            { name: "refs/heads/a", value: oid, expected: null },
            // stale: main already points at `oid`
            { name: "refs/heads/main", value: oid, expected: null },
          ],
          { atomic: true },
        );

        return { a: yield* refs.read("refs/heads/a"), results };
      }),
    );

    assert.ok(state.results.every((result) => !result.ok));
    assert.equal(state.a, null, "the good ref in the batch must not have been written");
  });

  it("applies the good refs when the batch is not atomic", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* RefStore;
        const oid = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        const results = yield* repository.receive([
          { name: "refs/heads/a", value: oid, expected: null },
          { name: "refs/heads/main", value: oid, expected: null },
        ]);

        return { a: yield* refs.read("refs/heads/a"), results };
      }),
    );

    assert.equal(state.results.filter((result) => result.ok).length, 1);
    assert.notEqual(state.a, null);
  });

  it("carries a hook rejection as a typed failure and writes nothing", async () => {
    const rejecting = Layer.succeed(Hooks, {
      preReceive: () =>
        Effect.fail(new HookRejected({ hook: "pre-receive", message: "denied by policy" })),
      update: () => Effect.void,
      postReceive: () => Effect.void,
    });

    const outcome = await withHooks(
      rejecting,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* RefStore;
        const failure = yield* Effect.flip(
          repository.receive([
            { name: "refs/heads/a", value: "0".repeat(40) as never, expected: null },
          ]),
        );
        return { a: yield* refs.read("refs/heads/a"), failure };
      }),
    );

    assert.equal(outcome.failure._tag, "HookRejected");
    if (outcome.failure._tag === "HookRejected") {
      assert.equal(outcome.failure.message, "denied by policy");
    }
    assert.equal(outcome.a, null);
  });

  it("runs post-receive with the results", async () => {
    const seen: string[] = [];
    const recording = Layer.succeed(Hooks, {
      preReceive: () => Effect.void,
      update: () => Effect.void,
      postReceive: (results) =>
        Effect.sync(() => {
          for (const result of results) seen.push(result.ref);
        }),
    });

    await withHooks(
      recording,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });
        return yield* repository.receive([{ name: "refs/heads/a", value: oid, expected: null }]);
      }),
    );

    assert.deepEqual(seen, ["refs/heads/a"]);
  });
});
