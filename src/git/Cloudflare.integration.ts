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
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect } from "effect";
import { promisify } from "node:util";

import { createTestHarness, type TestHarness } from "wrangler";

import { hasGit } from "../testing/Git.ts";
import type { ConformanceReport } from "./Conformance.ts";

const execFileAsync = promisify(execFile);
const git = async (cwd: string, ...args: string[]): Promise<string> => {
  const result = await execFileAsync(
    "git",
    ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args],
    { cwd, encoding: "utf8", env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } },
  );
  return result.stdout;
};

const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

let harness: TestHarness;
let base: URL;

/** One repo (and so one Durable Object instance) per test. */
const repoName = () => `repo-${crypto.randomUUID()}`;

/**
 * Structural on purpose: the harness hands back undici's `Response`, while the
 * generated worker types put workerd's `Response` in ambient scope. This file
 * runs in node, so it should not care which one it got.
 */
// SAFETY: each caller names the shape the Worker's own handler encodes, and
// asserts on the fields right after — a body that does not match fails the
// test rather than slipping through.
const json = async <T>(response: { text(): Promise<string> }): Promise<T> =>
  JSON.parse(await response.text()) as T;

/** The slice of the `/commit` payload these tests exercise. */
interface CommitBody {
  readonly message: string;
  /** `null` pins "the branch must not exist", mirroring `RefUpdate.expected`. */
  readonly expected?: string | null;
}

const commit = (repo: string, body: CommitBody) =>
  harness.fetch(`/${repo}/commit`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ author: alice, branch: "main", ...body }),
  });

beforeAll(async () => {
  harness = createTestHarness({ workers: [{ configPath: "./wrangler.test.json" }] });
  base = (await harness.listen()).url;
});

afterAll(async () => {
  await harness.close();
});

describe("Cloudflare storage conformance", () => {
  it.effect("passes the same storage contract as the in-memory and filesystem backends", () =>
    Effect.promise(async () => {
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
    }),
  );
});

describe("Artifacts registry conformance", () => {
  it.effect("passes the registry and token contract on Durable Object SQLite", () =>
    Effect.promise(async () => {
      // The durable form of the provider's `Registry`/`Tokens`, running the
      // same suite as the in-memory and JSON-file backends — inside workerd.
      const response = await harness.fetch(`/${repoName()}/registry-conformance`);
      assert.equal(response.status, 200);

      const report = await json<ConformanceReport>(response);
      assert.deepEqual(
        report.results.filter((result) => !result.ok).map((f) => `${f.name}: ${f.error ?? ""}`),
        [],
        "the contract must hold on DO SQLite, not just in memory",
      );
      assert.ok(report.passed >= 10, `expected the full suite, ran ${report.passed}`);
    }),
  );
});

describe("GitRepo over HTTP", () => {
  it.effect("commits, lists refs and reads the commit back", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("walks history newest first", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("takes the status from the error's annotation", () =>
    Effect.promise(async () => {
      const repo = repoName();

      // `ObjectNotFound` is annotated `httpApiStatus: 404`; no handler maps tags
      // to codes. The body is the schema-encoded error itself — a value a client
      // can match on, not a code string it has to sniff.
      const missing = await harness.fetch(`/${repo}/commit/${"0".repeat(40)}`);
      assert.equal(missing.status, 404);
      assert.deepEqual(await json(missing), {
        _tag: "ObjectNotFound",
        oid: "0".repeat(40),
      });
    }),
  );

  it.effect("returns RefConflict when the caller pins a stale head", () =>
    Effect.promise(async () => {
      const repo = repoName();
      await commit(repo, { message: "one" });

      const conflict = await commit(repo, { message: "two", expected: null });
      assert.equal(conflict.status, 409);
      // The full conflict crosses the wire: which ref, what the caller pinned,
      // where the ref actually is.
      const body = await json<{ _tag: string; ref: string; expected: null; actual: string }>(
        conflict,
      );
      assert.equal(body._tag, "RefConflict");
      assert.equal(body.ref, "refs/heads/main");
      assert.equal(body.expected, null);
      assert.match(body.actual, /^[0-9a-f]{40}$/);
    }),
  );

  it.effect("rejects a request with no repository", () =>
    Effect.promise(async () => {
      const response = await harness.fetch("/");
      assert.equal(response.status, 400);
    }),
  );
});

describe.skipIf(!hasGit)("smart HTTP against workerd", () => {
  it.effect("clones from and pushes to the Durable Object with the real git binary", () =>
    Effect.promise(async () => {
      const repo = repoName();
      await commit(repo, { message: "seed" });

      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-workerd-"));
      const work = path.join(root, "clone");
      await git(root, "clone", "--quiet", new URL(repo, base).href, work);
      assert.match(await git(work, "log", "--format=%s"), /^seed$/m);

      await fs.writeFile(path.join(work, "pushed.txt"), "from git, into workerd\n");
      await git(work, "add", ".");
      await git(work, "commit", "--quiet", "-m", "pushed");
      await git(work, "push", "--quiet", "origin", "main");

      // The JSON surface and the smart-HTTP surface agree on where main is.
      const pushed = (await git(work, "rev-parse", "HEAD")).trim();
      const refs = await json<{ refs: Array<{ name: string; oid: string }> }>(
        await harness.fetch(`/${repo}/refs`),
      );
      assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: pushed }]);

      const verify = path.join(root, "verify");
      await git(root, "clone", "--quiet", new URL(repo, base).href, verify);
      assert.equal(
        await fs.readFile(path.join(verify, "pushed.txt"), "utf8"),
        "from git, into workerd\n",
      );

      await fs.rm(root, { recursive: true, force: true });
    }),
  );
});

describe("durability", () => {
  it.effect("writes refs to SQLite, not to instance memory", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("survives an eviction, because the layer is rebuilt from storage", () =>
    Effect.promise(async () => {
      const repo = repoName();
      const created = await json<{ oid: string }>(await commit(repo, { message: "evict me" }));

      // Tears the instance down: in-memory state goes, durable storage stays. A
      // ref map cached on the instance would survive here and hide the bug.
      const worker = harness.getWorker();
      await worker.evictDurableObject("GIT_REPO", { name: repo });

      const refs = await json<{ refs: Array<{ oid: string }> }>(
        await harness.fetch(`/${repo}/refs`),
      );
      assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: created.oid }]);
    }),
  );

  it.effect("keeps repositories isolated from each other", () =>
    Effect.promise(async () => {
      const [first, second] = [repoName(), repoName()];
      await commit(first, { message: "only in first" });

      const refs = await json<{ refs: unknown[] }>(await harness.fetch(`/${second}/refs`));
      assert.deepEqual(refs.refs, []);
    }),
  );
});
