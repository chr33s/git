import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitIndex } from "./git.index.ts";
import { GitObjectStore } from "./git.object.ts";
import { MemoryStorage } from "./git.storage.ts";

void describe("GitIndex", () => {
	void describe("init", () => {
		void it("should initialize index", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			const entries = index.getEntries();
			assert.ok(Array.isArray(entries));
		});

		void it("should start with empty entries", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			const entries = index.getEntries();
			assert.equal(entries.length, 0);
		});
	});

	void describe("load", () => {
		void it("should load initial state", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.load();

			const entries = index.getEntries();
			assert.ok(Array.isArray(entries));
		});

		void it("should handle missing index file", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.load();

			// Should not throw, just return empty entries
			assert.deepEqual(index.getEntries(), []);
		});
	});

	void describe("addEntry", () => {
		void it("should add entry to index", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			await index.addEntry({
				path: "test.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 100,
				mtime: Date.now(),
			});

			const entries = index.getEntries();
			assert.equal(entries.length, 1);
			assert.equal(entries[0].path, "test.txt");
		});

		void it("should replace existing entry with same path", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			await index.addEntry({
				path: "test.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 100,
				mtime: Date.now(),
			});

			await index.addEntry({
				path: "test.txt",
				oid: "b".repeat(40),
				mode: "100644",
				size: 200,
				mtime: Date.now(),
			});

			const entries = index.getEntries();
			assert.equal(entries.length, 1);
			assert.equal(entries[0].oid, "b".repeat(40));
		});

		void it("should sort entries by path", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			await index.addEntry({
				path: "z.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 1,
				mtime: Date.now(),
			});

			await index.addEntry({
				path: "a.txt",
				oid: "b".repeat(40),
				mode: "100644",
				size: 1,
				mtime: Date.now(),
			});

			const entries = index.getEntries();
			assert.equal(entries[0].path, "a.txt");
			assert.equal(entries[1].path, "z.txt");
		});
	});

	void describe("removeEntry", () => {
		void it("should remove entry from index", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			await index.addEntry({
				path: "test.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 100,
				mtime: Date.now(),
			});

			await index.removeEntry("test.txt");

			const entries = index.getEntries();
			assert.equal(entries.length, 0);
		});

		void it("should handle removing non-existent entry", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			// Should not throw
			await index.removeEntry("nonexistent.txt");

			const entries = index.getEntries();
			assert.equal(entries.length, 0);
		});

		void it("should only remove specified entry", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			await index.addEntry({
				path: "keep.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 1,
				mtime: Date.now(),
			});

			await index.addEntry({
				path: "remove.txt",
				oid: "b".repeat(40),
				mode: "100644",
				size: 1,
				mtime: Date.now(),
			});

			await index.removeEntry("remove.txt");

			const entries = index.getEntries();
			assert.equal(entries.length, 1);
			assert.equal(entries[0].path, "keep.txt");
		});
	});

	void describe("save and reload", () => {
		void it("should persist entries to disk", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index1 = new GitIndex(storage);
			await index1.init();

			await index1.addEntry({
				path: "persisted.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 50,
				mtime: 1234567890000,
			});

			// Create new index instance and load
			const index2 = new GitIndex(storage);
			await index2.load();

			const entries = index2.getEntries();
			assert.equal(entries.length, 1);
			assert.equal(entries[0].path, "persisted.txt");
			assert.equal(entries[0].oid, "a".repeat(40));
		});
	});

	void describe("updateFromTree", () => {
		void it("should update index from tree object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			// Create a blob
			const blobContent = new TextEncoder().encode("file content");
			const blobOid = await objectStore.writeObject("blob", blobContent);

			// Create a tree containing the blob
			const treeData = new Uint8Array([
				...new TextEncoder().encode("100644 myfile.txt\0"),
				...hexToBytes(blobOid),
			]);
			const treeOid = await objectStore.writeObject("tree", treeData);

			// Update index from tree
			const index = new GitIndex(storage);
			await index.init();
			await index.updateFromTree(treeOid, objectStore);

			const entries = index.getEntries();
			assert.equal(entries.length, 1);
			assert.equal(entries[0].path, "myfile.txt");
			assert.equal(entries[0].oid, blobOid);
		});

		void it("should handle nested tree structures", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			// Create blobs
			const blob1Oid = await objectStore.writeObject("blob", new TextEncoder().encode("content1"));
			const blob2Oid = await objectStore.writeObject("blob", new TextEncoder().encode("content2"));

			// Create subtree
			const subTreeData = new Uint8Array([
				...new TextEncoder().encode("100644 nested.txt\0"),
				...hexToBytes(blob2Oid),
			]);
			const subTreeOid = await objectStore.writeObject("tree", subTreeData);

			// Create root tree with file and subtree
			const rootTreeData = new Uint8Array([
				...new TextEncoder().encode("100644 root.txt\0"),
				...hexToBytes(blob1Oid),
				...new TextEncoder().encode("40000 subdir\0"),
				...hexToBytes(subTreeOid),
			]);
			const rootTreeOid = await objectStore.writeObject("tree", rootTreeData);

			const index = new GitIndex(storage);
			await index.init();
			await index.updateFromTree(rootTreeOid, objectStore);

			const entries = index.getEntries();
			assert.equal(entries.length, 2);
			assert.ok(entries.some((e) => e.path === "root.txt"));
			assert.ok(entries.some((e) => e.path === "subdir/nested.txt"));
		});

		void it("should handle non-existent tree gracefully", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const index = new GitIndex(storage);
			await index.init();

			// Add an entry first
			await index.addEntry({
				path: "existing.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 1,
				mtime: Date.now(),
			});

			// Update from non-existent tree should clear entries
			await index.updateFromTree("nonexistent" + "0".repeat(32), objectStore);

			const entries = index.getEntries();
			assert.equal(entries.length, 0);
		});
	});

	void describe("getEntries", () => {
		void it("should return a copy of entries", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const index = new GitIndex(storage);
			await index.init();

			await index.addEntry({
				path: "test.txt",
				oid: "a".repeat(40),
				mode: "100644",
				size: 1,
				mtime: Date.now(),
			});

			const entries1 = index.getEntries();
			const entries2 = index.getEntries();

			// Should be different arrays
			assert.notEqual(entries1, entries2);
			// But with same content
			assert.deepEqual(entries1, entries2);
		});
	});
});

// Helper function
function hexToBytes(hex: string): Uint8Array {
	const bytes = new Uint8Array(hex.length / 2);
	for (let i = 0; i < bytes.length; i++) {
		bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	}
	return bytes;
}
