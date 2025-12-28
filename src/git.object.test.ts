import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitObjectStore } from "./git.object.ts";
import { MemoryStorage } from "./git.storage.ts";

void describe("GitObjectStore", () => {
	void describe("init", () => {
		void it("should initialize object store", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			assert.ok(await storage.exists(".git/objects"));
		});
	});

	void describe("writeObject", () => {
		void it("should write a blob object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const content = new TextEncoder().encode("Hello, World!");
			const oid = await objectStore.writeObject("blob", content);

			assert.equal(oid.length, 40);
			assert.ok(/^[0-9a-f]+$/.test(oid));
		});

		void it("should write a tree object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const treeData = new Uint8Array([1, 2, 3, 4]);
			const oid = await objectStore.writeObject("tree", treeData);

			assert.equal(oid.length, 40);
		});

		void it("should write a commit object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const commitData = new TextEncoder().encode("tree abc\nauthor x\n\nmessage");
			const oid = await objectStore.writeObject("commit", commitData);

			assert.equal(oid.length, 40);
		});

		void it("should write a tag object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const tagData = new TextEncoder().encode("object abc\ntype commit\ntag v1.0\n");
			const oid = await objectStore.writeObject("tag", tagData);

			assert.equal(oid.length, 40);
		});

		void it("should produce consistent hashes for same content", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const content = new TextEncoder().encode("test content");
			const oid1 = await objectStore.writeObject("blob", content);
			const oid2 = await objectStore.writeObject("blob", content);

			assert.equal(oid1, oid2);
		});

		void it("should store object in correct path", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const content = new TextEncoder().encode("test");
			const oid = await objectStore.writeObject("blob", content);

			const dir = oid.substring(0, 2);
			const file = oid.substring(2);
			assert.ok(await storage.exists(`.git/objects/${dir}/${file}`));
		});
	});

	void describe("readObject", () => {
		void it("should read a written blob object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const content = new TextEncoder().encode("Hello, World!");
			const oid = await objectStore.writeObject("blob", content);

			const obj = await objectStore.readObject(oid);

			assert.equal(obj.type, "blob");
			assert.deepEqual(obj.data, content);
		});

		void it("should read a written tree object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const treeData = new Uint8Array([10, 20, 30]);
			const oid = await objectStore.writeObject("tree", treeData);

			const obj = await objectStore.readObject(oid);

			assert.equal(obj.type, "tree");
			assert.deepEqual(obj.data, treeData);
		});

		void it("should read a written commit object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const commitData = new TextEncoder().encode("tree hash\nauthor test\n\ncommit message");
			const oid = await objectStore.writeObject("commit", commitData);

			const obj = await objectStore.readObject(oid);

			assert.equal(obj.type, "commit");
			assert.deepEqual(obj.data, commitData);
		});

		void it("should throw when reading non-existent object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			try {
				await objectStore.readObject("0000000000000000000000000000000000000000");
				assert.fail("Should have thrown error");
			} catch (error: any) {
				assert.ok(error.message.includes("not found"));
			}
		});

		void it("should throw with invalid OID", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			try {
				await objectStore.readObject("invalid");
				assert.fail("Should have thrown error");
			} catch (error: any) {
				assert.ok(error.message.includes("not found"));
			}
		});
	});

	void describe("hasObject", () => {
		void it("should return true for existing object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const content = new TextEncoder().encode("test content");
			const oid = await objectStore.writeObject("blob", content);

			const exists = await objectStore.hasObject(oid);
			assert.equal(exists, true);
		});

		void it("should return false for non-existent object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const exists = await objectStore.hasObject("0000000000000000000000000000000000000000");
			assert.equal(exists, false);
		});

		void it("should return false for invalid OID", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const exists = await objectStore.hasObject("invalid");
			assert.equal(exists, false);
		});
	});

	void describe("roundtrip", () => {
		void it("should preserve data through write/read cycle", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const testCases = [
				{ type: "blob" as const, data: new TextEncoder().encode("simple text") },
				{ type: "blob" as const, data: new Uint8Array([0, 1, 2, 255, 254, 253]) },
				{ type: "blob" as const, data: new TextEncoder().encode("") },
				{ type: "tree" as const, data: new Uint8Array(100).fill(42) },
			];

			for (const testCase of testCases) {
				const oid = await objectStore.writeObject(testCase.type, testCase.data);
				const obj = await objectStore.readObject(oid);

				assert.equal(obj.type, testCase.type);
				assert.deepEqual(obj.data, testCase.data);
			}
		});
	});
});
