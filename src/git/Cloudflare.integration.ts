/**
 * Integration tests against a real Workers runtime.
 *
 * `createTestHarness` from `wrangler` starts the Worker in `wrangler.test.json`
 * as a local server — real workerd, real Durable Objects, real R2 — and this
 * file drives it from the outside over HTTP. Nothing here is mocked, and
 * nothing here runs inside the isolate, which is the point: it exercises the
 * Worker the way a client does.
 *
 * Named `*.integration.ts` rather than `*.test.ts` so node's default discovery
 * does not pull it into `npm test`; it boots a runtime and belongs behind
 * `npm run test:integration`.
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import { createTestHarness, type TestHarness } from "wrangler";

import type { ConformanceReport } from "./Conformance.ts";

const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

let harness: TestHarness;

/** One repo (and so one Durable Object instance) per test. */
const repoName = () => `repo-${crypto.randomUUID()}`;

/**
 * Structural on purpose: the harness hands back undici's `Response`, while the
 * generated worker types put workerd's `Response` in ambient scope. This file
 * runs in node, so it should not care which one it got.
 */
const json = async <T>(response: { json(): Promise<unknown> }): Promise<T> =>
  (await response.json()) as T;

const commit = (repo: string, body: Record<string, unknown>) =>
  harness.fetch(`/${repo}/commit`, {
    method: "POST",
    body: JSON.stringify({ author: alice, branch: "main", ...body }),
  });

before(async () => {
  harness = createTestHarness({ workers: [{ configPath: "./wrangler.test.json" }] });
  await harness.listen();
});

after(async () => {
  await harness.close();
});

describe("Cloudflare storage conformance", () => {
  it("passes the same storage contract as the in-memory and filesystem backends", async () => {
    // The suite runs inside the Durable Object — see `Conformance.ts`, since the
    // test process cannot reach `state.storage.sql` from out here — and reports
    // back as JSON.
    const response = await harness.fetch(`/${repoName()}/conformance`);
    assert.equal(response.status, 200);

    const report = await json<ConformanceReport>(response);
    const failures = report.results.filter((result) => !result.ok);

    assert.deepEqual(
      failures.map((failure) => `${failure.name}: ${failure.error ?? ""}`),
      [],
      "the contract must hold on DO SQLite and R2, not just in memory",
    );
    assert.ok(report.passed >= 15, `expected the full suite, ran ${report.passed}`);
  });
});

describe("GitRepo over HTTP", () => {
  it("commits, lists refs and reads the commit back", async () => {
    const repo = repoName();

    const created = await json<{ oid: string }>(await commit(repo, { message: "first" }));
    assert.match(created.oid, /^[0-9a-f]{40}$/);

    const refs = await json<{ refs: Array<{ name: string; oid: string }> }>(
      await harness.fetch(`/${repo}/refs`),
    );
    assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: created.oid }]);

    const read = await json<{ message: string }>(
      await harness.fetch(`/${repo}/commit/${created.oid}`),
    );
    assert.equal(read.message, "first");
  });

  it("walks history newest first", async () => {
    const repo = repoName();
    await commit(repo, { message: "one" });
    const second = await json<{ oid: string }>(await commit(repo, { message: "two" }));

    const log = await json<{ commits: Array<{ message: string }> }>(
      await harness.fetch(`/${repo}/log/${second.oid}`),
    );
    assert.deepEqual(
      log.commits.map((entry) => entry.message),
      ["two", "one"],
    );
  });

  it("takes the status from the error's annotation", async () => {
    const repo = repoName();

    // `ObjectNotFound` is annotated `httpApiStatus: 404`; no handler maps tags
    // to codes.
    const missing = await harness.fetch(`/${repo}/commit/${"0".repeat(40)}`);
    assert.equal(missing.status, 404);
    assert.deepEqual(await json(missing), { error: "ObjectNotFound" });
  });

  it("returns RefConflict when the caller pins a stale head", async () => {
    const repo = repoName();
    await commit(repo, { message: "one" });

    const conflict = await commit(repo, { message: "two", expected: null });
    assert.equal(conflict.status, 409);
    assert.deepEqual(await json(conflict), { error: "RefConflict" });
  });

  it("rejects a request with no repository", async () => {
    const response = await harness.fetch("/");
    assert.equal(response.status, 400);
  });
});

describe("durability", () => {
  it("writes refs to SQLite, not to instance memory", async () => {
    const repo = repoName();
    const created = await json<{ oid: string }>(await commit(repo, { message: "durable" }));

    // Straight into the Durable Object's own SQLite, from the test process.
    // Untyped env: the generated worker types describe workerd's
    // `DurableObjectNamespace`, which is not the one wrangler's harness types
    // name, and this file only needs the binding's string name.
    const worker = harness.getWorker();
    const storage = await worker.getDurableObjectStorage("GIT_REPO", { name: repo });
    const rows = await storage.exec<{ name: string; oid: string }>(
      "SELECT name, oid FROM refs WHERE repo = ?",
      repo,
    );

    assert.deepEqual(rows, [{ name: "refs/heads/main", oid: created.oid }]);
  });

  it("survives an eviction, because the layer is rebuilt from storage", async () => {
    const repo = repoName();
    const created = await json<{ oid: string }>(await commit(repo, { message: "evict me" }));

    // Tears the instance down: in-memory state goes, durable storage stays. A
    // ref map cached on the instance would survive here and hide the bug.
    const worker = harness.getWorker();
    await worker.evictDurableObject("GIT_REPO", { name: repo });

    const refs = await json<{ refs: Array<{ oid: string }> }>(await harness.fetch(`/${repo}/refs`));
    assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: created.oid }]);
  });

  it("keeps repositories isolated from each other", async () => {
    const [first, second] = [repoName(), repoName()];
    await commit(first, { message: "only in first" });

    const refs = await json<{ refs: unknown[] }>(await harness.fetch(`/${second}/refs`));
    assert.deepEqual(refs.refs, []);
  });
});
