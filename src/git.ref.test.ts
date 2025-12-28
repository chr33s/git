import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitRefStore } from "./git.ref.ts";
import { MemoryStorage } from "./git.storage.ts";

void describe("GitRefStore", () => {
	void it("should initialize ref store", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const refStore = new GitRefStore(storage);
		await refStore.init();

		assert.ok(await storage.exists(".git/refs"));
		assert.ok(await storage.exists(".git/refs/heads"));
		assert.ok(await storage.exists(".git/refs/tags"));
	});

	void it("should write and read refs", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const refStore = new GitRefStore(storage);
		await refStore.init();

		const oid = "abc123def456abc123def456abc123def456abc1";
		await refStore.writeRef("heads/main", oid);

		const readOid = await refStore.readRef("heads/main");
		assert.equal(readOid, oid);
	});

	void it("should return null for non-existent ref", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const refStore = new GitRefStore(storage);
		await refStore.init();

		const readOid = await refStore.readRef("heads/nonexistent");
		assert.equal(readOid, null);
	});

	void it("should delete refs", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const refStore = new GitRefStore(storage);
		await refStore.init();

		const oid = "abc123def456abc123def456abc123def456abc1";
		await refStore.writeRef("heads/test", oid);
		assert.ok(await refStore.readRef("heads/test"));

		await refStore.deleteRef("heads/test");
		const readOid = await refStore.readRef("heads/test");
		assert.equal(readOid, null);
	});

	void it("should get all refs", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const refStore = new GitRefStore(storage);
		await refStore.init();

		const oid1 = "abc123def456abc123def456abc123def456abc1";
		const oid2 = "def456abc123def456abc123def456abc123def4";

		await refStore.writeRef("heads/main", oid1);
		await refStore.writeRef("heads/develop", oid2);

		const allRefs = await refStore.getAllRefs();
		assert.ok(allRefs.length >= 2);
		const mainRef = allRefs.find((r) => r.name.includes("main"));
		assert.ok(mainRef);
	});
});
