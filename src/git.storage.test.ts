import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { MemoryStorage } from "./git.storage.ts";

void describe("MemoryStorage", () => {
	void it("should initialize storage", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		assert.ok(await storage.exists(".git"));
	});

	void it("should write and read files", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const testData = new Uint8Array([1, 2, 3, 4, 5]);
		await storage.writeFile("test.txt", testData);

		const readData = await storage.readFile("test.txt");
		assert.deepEqual(readData, testData);
	});

	void it("should check if file exists", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const testData = new Uint8Array([1, 2, 3]);
		await storage.writeFile("exists-test.txt", testData);

		assert.ok(await storage.exists("exists-test.txt"));
		assert.ok(!(await storage.exists("nonexistent.txt")));
	});

	void it("should delete files", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const testData = new Uint8Array([1, 2, 3]);
		await storage.writeFile("delete-test.txt", testData);

		assert.ok(await storage.exists("delete-test.txt"));
		await storage.deleteFile("delete-test.txt");
		assert.ok(!(await storage.exists("delete-test.txt")));
	});

	void it("should throw error when deleting non-existent file", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		try {
			await storage.deleteFile("nonexistent.txt");
			assert.fail("Should have thrown error");
		} catch (error: any) {
			assert.ok(error.message.includes("File not found"));
		}
	});

	void it("should create directories", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		await storage.createDirectory("test/nested/dir");
		assert.ok(await storage.exists("test/nested/dir"));
	});

	void it("should list directory contents", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		await storage.createDirectory("list-test");
		await storage.writeFile("list-test/file1.txt", new Uint8Array([1]));
		await storage.writeFile("list-test/file2.txt", new Uint8Array([2]));
		await storage.createDirectory("list-test/subdir");

		const contents = await storage.listDirectory("list-test");
		assert.ok(contents.includes("file1.txt"));
		assert.ok(contents.includes("file2.txt"));
		assert.ok(contents.includes("subdir"));
	});

	void it("should delete directories recursively", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		await storage.createDirectory("delete-dir-test");
		await storage.writeFile("delete-dir-test/file.txt", new Uint8Array([1]));
		await storage.createDirectory("delete-dir-test/subdir");
		await storage.writeFile("delete-dir-test/subdir/nested.txt", new Uint8Array([2]));

		assert.ok(await storage.exists("delete-dir-test"));
		await storage.deleteDirectory("delete-dir-test");
		assert.ok(!(await storage.exists("delete-dir-test")));
	});

	void it("should get file info", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		const testData = new Uint8Array([1, 2, 3, 4, 5]);
		await storage.writeFile("info-test.txt", testData);

		const info = await storage.getFileInfo("info-test.txt");
		assert.equal(info.size, 5);
		assert.ok(info.lastModified instanceof Date);
	});

	void it("should throw error when storage not initialized", async () => {
		const storage = new MemoryStorage();

		try {
			await storage.readFile("test.txt");
			assert.fail("Should have thrown error");
		} catch (error: any) {
			assert.ok(error.message.includes("Storage not initialized"));
		}
	});

	void it("should throw error when reading non-existent file", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		try {
			await storage.readFile("nonexistent.txt");
			assert.fail("Should have thrown error");
		} catch (error: any) {
			assert.ok(error.message.includes("File not found"));
		}
	});

	void it("should automatically create parent directories on write", async () => {
		const storage = new MemoryStorage();
		await storage.init("test-repo");

		await storage.writeFile("deep/nested/path/file.txt", new Uint8Array([1, 2, 3]));

		assert.ok(await storage.exists("deep"));
		assert.ok(await storage.exists("deep/nested"));
		assert.ok(await storage.exists("deep/nested/path"));
		assert.ok(await storage.exists("deep/nested/path/file.txt"));
	});
});
