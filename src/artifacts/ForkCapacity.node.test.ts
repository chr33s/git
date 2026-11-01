import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Result } from "effect";
import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { RepoStores, repoStoresNode } from "./Namespace.ts";

const open = (root: string, rows: number, bytes = 1024 ** 2) =>
  Effect.runPromise(
    RepoStores.pipe(
      Effect.provide(repoStoresNode(root)),
      Effect.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            GIT_ARTIFACTS_FORK_ROWS: rows,
            GIT_ARTIFACTS_FORK_BYTES: bytes,
          }),
        ),
      ),
    ),
  );
const refused = async <A>(operation: Effect.Effect<A, ArtifactsError>) => {
  const result = await Effect.runPromise(Effect.result(operation));
  assert.ok(Result.isFailure(result));
  assert.ok(result.failure instanceof ArtifactsError);
  assert.match(result.failure.message, /^RESOURCE_EXHAUSTED:/);
  assert.doesNotMatch(JSON.stringify(result.failure), /private-parent|fork-capacity/);
};

it("fork capacity refuses before disk changes, retries are idempotent, removal recovers", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "fork-capacity-"));
  const borrowers = path.join(root, "private-parent", "objects", "info", "borrowers");
  try {
    await fs.mkdir(path.dirname(borrowers), { recursive: true });
    const stores = await open(root, 1);
    await Effect.runPromise(stores.fork("first", "private-parent"));
    const before = await fs.readFile(path.join(root, ".forks.json"), "utf8");
    for (let index = 0; index < 5; index++)
      await Effect.runPromise(stores.fork("first", "private-parent"));
    assert.equal(await fs.readFile(borrowers, "utf8"), "first\n");
    await refused(stores.fork("second", "private-parent"));
    assert.equal(await fs.readFile(path.join(root, ".forks.json"), "utf8"), before);
    await assert.rejects(fs.stat(path.join(root, "second")), { code: "ENOENT" });
    assert.equal(await fs.readFile(borrowers, "utf8"), "first\n");
    const lowered = await open(root, 1, 1);
    assert.deepEqual(await Effect.runPromise(lowered.dependents("private-parent")), ["first"]);
    await refused(lowered.fork("second", "private-parent"));
    await Effect.runPromise(lowered.drop("first"));
    assert.deepEqual(await Effect.runPromise(stores.dependents("private-parent")), []);
    await Effect.runPromise(stores.fork("second", "private-parent"));
    assert.equal(await fs.readFile(borrowers, "utf8"), "second\n");
    assert.ok(!(await fs.readdir(root)).some((name) => name.endsWith(".lock")));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("borrower budgets retain external registrations and refuse before alternates are written", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "fork-capacity-"));
  const borrowers = path.join(root, "parent", "objects", "info", "borrowers");
  try {
    await fs.mkdir(path.dirname(borrowers), { recursive: true });
    await fs.writeFile(borrowers, "external\n");
    const stores = await open(root, 1);
    await refused(stores.fork("child", "parent"));
    assert.equal(await fs.readFile(borrowers, "utf8"), "external\n");
    await assert.rejects(fs.stat(path.join(root, "child")), { code: "ENOENT" });
    await assert.rejects(fs.stat(path.join(root, ".forks.json")), { code: "ENOENT" });
    const raised = await open(root, 2);
    await Effect.runPromise(raised.fork("child", "parent"));
    assert.equal(await fs.readFile(borrowers, "utf8"), "external\nchild\n");
    await Effect.runPromise(raised.forget("child"));
    assert.equal(await fs.readFile(borrowers, "utf8"), "external\n");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("oversized fork metadata is refused before parsing and borrower byte limits preserve disk", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "fork-capacity-"));
  const borrowers = path.join(root, "parent", "objects", "info", "borrowers");
  try {
    await fs.mkdir(path.dirname(borrowers), { recursive: true });
    const stores = await open(root, 10, 256);
    const invalid = "{".repeat(257);
    await fs.writeFile(path.join(root, ".forks.json"), invalid);
    await refused(stores.fork("child", "parent"));
    assert.equal(await fs.readFile(path.join(root, ".forks.json"), "utf8"), invalid);
    await fs.writeFile(path.join(root, ".forks.json"), "{}");
    const contents = `${"東".repeat(85)}\n`;
    assert.equal(Buffer.byteLength(contents), 256);
    await fs.writeFile(borrowers, contents);
    await refused(stores.fork("child", "parent"));
    assert.equal(await fs.readFile(borrowers, "utf8"), contents);
    await assert.rejects(fs.stat(path.join(root, "child")), { code: "ENOENT" });
    assert.equal(await fs.readFile(path.join(root, ".forks.json"), "utf8"), "{}");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("fork links enforce the exact serialized UTF-8 byte boundary", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "fork-capacity-"));
  const child = "東".repeat(60);
  const serialized = JSON.stringify({ [child]: "parent" }, null, 1);
  const bytes = Buffer.byteLength(serialized);
  try {
    await fs.mkdir(path.join(root, "parent", "objects", "info"), { recursive: true });
    await refused((await open(root, 1, bytes - 1)).fork(child, "parent"));
    await assert.rejects(fs.stat(path.join(root, child)), { code: "ENOENT" });
    await Effect.runPromise((await open(root, 1, bytes)).fork(child, "parent"));
    assert.equal(await fs.readFile(path.join(root, ".forks.json"), "utf8"), serialized);
    assert.equal(
      await fs.readFile(path.join(root, "parent", "objects", "info", "borrowers"), "utf8"),
      `${child}\n`,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
