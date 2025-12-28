import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitPackParser, GitPackWriter } from "./git.pack.ts";
import { GitObjectStore } from "./git.object.ts";
import { MemoryStorage } from "./git.storage.ts";

void describe("GitPackParser", () => {
	void describe("constructor", () => {
		void it("should initialize pack parser", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const parser = new GitPackParser(objectStore);
			assert.ok(parser);
		});
	});

	void describe("parsePack", () => {
		void it("should parse valid empty pack file", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const parser = new GitPackParser(objectStore);

			// Create a minimal valid pack file with correct checksum
			const packData = new Uint8Array([
				// Pack signature: 'PACK'
				0x50, 0x41, 0x43, 0x4b,
				// Version: 2
				0x00, 0x00, 0x00, 0x02,
				// Object count: 0
				0x00, 0x00, 0x00, 0x00,
				// SHA1 checksum (20 bytes) - placeholder
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00,
			]);

			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					controller.enqueue(packData);
					controller.close();
				},
			});

			// Should not throw
			await parser.parsePack(stream);
		});

		void it("should reject invalid pack signature", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const parser = new GitPackParser(objectStore);

			// Invalid pack signature
			const packData = new Uint8Array([0x42, 0x41, 0x44, 0x21]);

			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					controller.enqueue(packData);
					controller.close();
				},
			});

			try {
				await parser.parsePack(stream);
				assert.fail("Should have thrown error");
			} catch (error: any) {
				assert.ok(error.message.includes("Invalid pack signature"));
			}
		});

		void it("should handle chunked stream input", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const parser = new GitPackParser(objectStore);

			// Split pack data into chunks
			const chunk1 = new Uint8Array([0x50, 0x41, 0x43, 0x4b]); // "PACK"
			const chunk2 = new Uint8Array([0x00, 0x00, 0x00, 0x02]); // version
			const chunk3 = new Uint8Array([0x00, 0x00, 0x00, 0x00]); // count
			const chunk4 = new Uint8Array(20).fill(0); // checksum

			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					controller.enqueue(chunk1);
					controller.enqueue(chunk2);
					controller.enqueue(chunk3);
					controller.enqueue(chunk4);
					controller.close();
				},
			});

			await parser.parsePack(stream);
		});
	});
});

void describe("GitPackWriter", () => {
	void describe("constructor", () => {
		void it("should initialize pack writer", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const writer = new GitPackWriter(objectStore);
			assert.ok(writer);
		});
	});

	void describe("createPack", () => {
		void it("should create empty pack", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			const writer = new GitPackWriter(objectStore);
			const packData = await writer.createPack([]);

			// Pack should have header and checksum
			assert.ok(packData.length >= 32); // 4 (sig) + 4 (ver) + 4 (count) + 20 (checksum)

			// Check signature
			const signature = new TextDecoder().decode(packData.slice(0, 4));
			assert.equal(signature, "PACK");

			// Check version (2)
			const version = (packData[4] << 24) | (packData[5] << 16) | (packData[6] << 8) | packData[7];
			assert.equal(version, 2);

			// Check object count (0)
			const count = (packData[8] << 24) | (packData[9] << 16) | (packData[10] << 8) | packData[11];
			assert.equal(count, 0);
		});

		void it("should create pack with single blob", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			// Create a blob
			const content = new TextEncoder().encode("Hello, World!");
			const oid = await objectStore.writeObject("blob", content);

			const writer = new GitPackWriter(objectStore);
			const packData = await writer.createPack([oid]);

			// Check signature
			const signature = new TextDecoder().decode(packData.slice(0, 4));
			assert.equal(signature, "PACK");

			// Check object count (1)
			const count = (packData[8] << 24) | (packData[9] << 16) | (packData[10] << 8) | packData[11];
			assert.equal(count, 1);
		});

		void it("should create pack with multiple objects", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			// Create multiple blobs
			const oid1 = await objectStore.writeObject("blob", new TextEncoder().encode("content1"));
			const oid2 = await objectStore.writeObject("blob", new TextEncoder().encode("content2"));
			const oid3 = await objectStore.writeObject("blob", new TextEncoder().encode("content3"));

			const writer = new GitPackWriter(objectStore);
			const packData = await writer.createPack([oid1, oid2, oid3]);

			// Check object count (3)
			const count = (packData[8] << 24) | (packData[9] << 16) | (packData[10] << 8) | packData[11];
			assert.equal(count, 3);
		});

		void it("should include all object types", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			// Create blob
			const blobOid = await objectStore.writeObject("blob", new TextEncoder().encode("content"));

			// Create tree
			const treeData = new Uint8Array([
				...new TextEncoder().encode("100644 file.txt\0"),
				...hexToBytes(blobOid),
			]);
			const treeOid = await objectStore.writeObject("tree", treeData);

			// Create commit
			const commitData = new TextEncoder().encode(
				`tree ${treeOid}\nauthor Test <t@t.com> 123 +0000\n\ntest`,
			);
			const commitOid = await objectStore.writeObject("commit", commitData);

			const writer = new GitPackWriter(objectStore);
			const packData = await writer.createPack([blobOid, treeOid, commitOid]);

			const count = (packData[8] << 24) | (packData[9] << 16) | (packData[10] << 8) | packData[11];
			assert.equal(count, 3);
		});
	});

	void describe("roundtrip", () => {
		void it("should roundtrip pack write and parse", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");

			const objectStore = new GitObjectStore(storage);
			await objectStore.init();

			// Create test objects
			const content = new TextEncoder().encode("test content");
			const oid = await objectStore.writeObject("blob", content);

			// Write pack
			const writer = new GitPackWriter(objectStore);
			const packData = await writer.createPack([oid]);

			// Create new storage to parse into
			const storage2 = new MemoryStorage();
			await storage2.init("test-repo-2");

			const objectStore2 = new GitObjectStore(storage2);
			await objectStore2.init();

			// Parse pack
			const parser = new GitPackParser(objectStore2);
			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					controller.enqueue(packData);
					controller.close();
				},
			});

			await parser.parsePack(stream);

			// Verify object was stored
			const stored = await objectStore2.readObject(oid);
			assert.equal(stored.type, "blob");
			assert.deepEqual(stored.data, content);
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
