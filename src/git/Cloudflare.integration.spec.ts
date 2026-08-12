/**
 * The Cloudflare backend, tested inside workerd.
 *
 * Not a mock and not a simulation of R2: `@cloudflare/vitest-pool-workers`
 * boots the Worker from `wrangler.test.json` under Miniflare, and these tests
 * run in the same isolate — so `runInDurableObject` hands us the real
 * `DurableObjectState`, with real SQLite, and `env.GIT_OBJECTS` is a real R2
 * binding.
 *
 * Two things are being checked here that unit tests cannot reach:
 *
 *   1. the storage contract holds on the backend that actually ships — the
 *      same suite the in-memory and filesystem backends run;
 *   2. `Repository` works when its stores are DO SQLite and R2, including
 *      across a Durable Object eviction, which is where "state lives in the
 *      instance" assumptions break.
 */
import { env, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { stores } from "./Cloudflare.ts";
import type { GitRepo } from "./Durable.ts";
import { EMPTY_TREE_OID, type Signature } from "./Format.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { storeContract } from "./Store.contract.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** A distinct repo (and so a distinct DO instance) per call. */
const instance = () => {
  const repo = `repo-${crypto.randomUUID()}`;
  return {
    repo,
    stub: env.GIT_REPO.get(env.GIT_REPO.idFromName(repo)) as DurableObjectStub<GitRepo>,
  };
};

/**
 * The contract suite, run against DO SQLite + R2.
 *
 * The effect is executed *inside* the Durable Object — `runInDurableObject`
 * gives us its `state`, which is the only place `storage.sql` exists. Isolated
 * per-test storage plus a fresh repo name per run is what makes each test
 * start empty.
 */
storeContract(
  "Cloudflare",
  {
    run: async (effect) => {
      const { repo, stub } = instance();
      return runInDurableObject(stub, (_instance, state) =>
        Effect.runPromise(
          effect.pipe(
            Effect.provide(stores({ bucket: env.GIT_OBJECTS, repo, storage: state.storage })),
          ) as Effect.Effect<never>,
        ),
      );
    },
  },
  { describe, it },
);

describe("Repository on Cloudflare storage", () => {
  const withRepository = <A, E>(effect: Effect.Effect<A, E, Repository>) => {
    const { repo, stub } = instance();
    return runInDurableObject(stub, (_instance, state) =>
      Effect.runPromise(
        effect.pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(stores({ bucket: env.GIT_OBJECTS, repo, storage: state.storage })),
            ),
          ),
        ) as Effect.Effect<A>,
      ),
    );
  };

  it("commits and walks the log", async () => {
    const messages = await withRepository(
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

    expect(messages).toEqual(["two", "one"]);
  });

  it("round-trips a blob through R2 with git's oid", async () => {
    const result = await withRepository(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("hello\n"));
        const tree = yield* repository.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
        const entries = yield* repository.readTree(tree);
        return { blob, entries };
      }),
    );

    expect(result.blob).toBe("ce013625030ba8dba906f756967f9e9ca394464a");
    expect(result.entries[0]?.name).toBe("a.txt");
  });

  it("fails with RefConflict on a stale expectation", async () => {
    const failure = await withRepository(
      Effect.gen(function* () {
        const repository = yield* Repository;
        yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        return yield* Effect.flip(
          repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "two",
            expected: null,
            author: alice,
          }),
        );
      }),
    );

    expect(failure._tag).toBe("RefConflict");
  });
});

describe("GitRepo durable object", () => {
  it("serves refs over RPC and over fetch", async () => {
    const { repo, stub } = instance();

    const commit = await stub.commit(repo, {
      author: alice,
      branch: "main",
      message: "first",
    });
    expect(commit.oid).toMatch(/^[0-9a-f]{40}$/);

    const refs = await stub.refs(repo);
    expect(refs).toEqual([{ name: "refs/heads/main", oid: commit.oid }]);

    const response = await stub.fetch(`https://example.com/${repo}/refs`);
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      refs: [{ name: "refs/heads/main", oid: commit.oid }],
    });
  });

  it("keeps refs across an eviction, because they are in SQLite not memory", async () => {
    const { repo, stub } = instance();

    const commit = await stub.commit(repo, {
      author: alice,
      branch: "main",
      message: "durable",
    });

    // Tears down the instance: in-memory state goes, storage stays. The layer
    // graph is rebuilt on the next call, which is the assumption worth
    // testing — a cached ref map would survive in memory and hide the bug.
    await evictDurableObject(stub);

    const refs = await stub.refs(repo);
    expect(refs).toEqual([{ name: "refs/heads/main", oid: commit.oid }]);
  });

  it("turns a typed failure into the status its annotation declares", async () => {
    const { repo, stub } = instance();

    // ObjectNotFound is annotated `httpApiStatus: 404`; nothing in the handler
    // maps tags to codes.
    const missing = await stub.fetch(`https://example.com/${repo}/commit/${"0".repeat(40)}`);
    expect(missing.status).toBe(404);
    await expect(missing.json()).resolves.toEqual({ error: "ObjectNotFound" });
  });

  it("serves a commit it wrote", async () => {
    const { repo, stub } = instance();
    const commit = await stub.commit(repo, { author: alice, branch: "main", message: "served" });

    const response = await stub.fetch(`https://example.com/${repo}/commit/${commit.oid}`);
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ message: "served" });
  });
});
