import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitRefStore } from "./git.ref.ts";
import { MemoryStorage } from "./git.storage.ts";

function oid(value: number) {
  return value.toString(16).padStart(40, "0").slice(-40);
}

async function setupStore() {
  const storage = new MemoryStorage();
  await storage.init("test-repo");

  const refStore = new GitRefStore(storage);
  await refStore.init();

  return { refStore, storage };
}

void describe("GitRefStore", () => {
  void it("should initialize ref and reflog directories", async () => {
    const { storage } = await setupStore();

    assert.ok(await storage.exists(".git/refs"));
    assert.ok(await storage.exists(".git/refs/heads"));
    assert.ok(await storage.exists(".git/refs/tags"));
    assert.ok(await storage.exists(".git/logs"));
    assert.ok(await storage.exists(".git/logs/refs/heads"));
  });

  void it("should write and read direct refs", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("heads/main", oid(1));

    assert.equal(await refStore.readRef("heads/main"), oid(1));
  });

  void it("should resolve symbolic refs", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("refs/heads/main", oid(1));
    await refStore.writeSymbolicRef("refs/heads/current", "refs/heads/main", "alias");

    assert.equal(await refStore.readSymbolicRef("refs/heads/current"), "refs/heads/main");
    assert.equal(await refStore.readRef("refs/heads/current"), oid(1));
  });

  void it("should return null for a missing ref", async () => {
    const { refStore } = await setupStore();

    assert.equal(await refStore.readRef("refs/heads/missing"), null);
  });

  void it("should delete refs", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("refs/heads/test", oid(1));
    await refStore.deleteRef("refs/heads/test");

    assert.equal(await refStore.readRef("refs/heads/test"), null);
  });

  void it("should reject invalid OIDs", async () => {
    const { refStore } = await setupStore();

    await assert.rejects(() => refStore.writeRef("refs/heads/main", "ABC123"), /Invalid OID/);
  });

  void it("should reject invalid ref names", async () => {
    const { refStore } = await setupStore();

    await assert.rejects(() => refStore.writeRef("refs/heads/../main", oid(1)), /Invalid ref name/);
  });

  void it("should compare-and-swap refs", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("refs/heads/main", oid(1));

    assert.equal(await refStore.compareAndSwapRef("refs/heads/main", oid(1), oid(2)), true);
    assert.equal(await refStore.compareAndSwapRef("refs/heads/main", oid(1), oid(3)), false);
    assert.equal(await refStore.readRef("refs/heads/main"), oid(2));
  });

  void it("should roll back atomic batches when one update fails", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("refs/heads/main", oid(1));

    const results = await refStore.applyRefUpdates(
      [
        { ref: "refs/heads/main", old: oid(1), new: oid(2), message: "move main" },
        { ref: "refs/heads/feature", old: null, new: oid(3), message: "create feature" },
        { ref: "refs/heads/stale", old: oid(9), new: oid(4), message: "stale create" },
      ],
      { atomic: true },
    );

    assert.equal(
      results.every((result) => !result.ok),
      true,
    );
    assert.equal(await refStore.readRef("refs/heads/main"), oid(1));
    assert.equal(await refStore.readRef("refs/heads/feature"), null);
  });

  void it("should append reflog entries for writes and deletes", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("refs/heads/main", oid(1), "create");
    await refStore.writeRef("refs/heads/main", oid(2), "update");
    await refStore.deleteRef("refs/heads/main", "delete");

    const reflog = await refStore.readReflog("refs/heads/main");
    assert.equal(reflog.length, 3);
    assert.equal(reflog[0]?.oldOid, "0".repeat(40));
    assert.equal(reflog[0]?.newOid, oid(1));
    assert.equal(reflog[1]?.oldOid, oid(1));
    assert.equal(reflog[1]?.newOid, oid(2));
    assert.equal(reflog[2]?.oldOid, oid(2));
    assert.equal(reflog[2]?.newOid, "0".repeat(40));
  });

  void it("should cap reflogs at 1000 entries", async () => {
    const { refStore } = await setupStore();

    for (let index = 1; index <= 1005; index++) {
      await refStore.writeRef("refs/heads/main", oid(index), `entry-${index}`);
    }

    const reflog = await refStore.readReflog("refs/heads/main");
    assert.equal(reflog.length, 1000);
    assert.equal(reflog[0]?.message, "entry-6");
    assert.equal(reflog.at(-1)?.message, "entry-1005");
  });

  void it("should return all refs", async () => {
    const { refStore } = await setupStore();

    await refStore.writeRef("refs/heads/main", oid(1));
    await refStore.writeRef("refs/heads/develop", oid(2));

    const refs = await refStore.getAllRefs();
    assert.deepEqual(refs.map((ref) => ref.name).sort(), ["refs/heads/develop", "refs/heads/main"]);
  });
});
