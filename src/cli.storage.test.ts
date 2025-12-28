import * as assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { FsStorage } from "./cli.storage.ts";

void describe("FsStorage", () => {
	let tempDir: string;
	let storage: FsStorage;

	before(async () => {
		// Create a temporary directory for testing
		const timestamp = Date.now();
		tempDir = join(tmpdir(), `git-test-${timestamp}`);
		await fs.mkdir(tempDir, { recursive: true });

		// Change to the temp directory
		process.chdir(tempDir);
		storage = new FsStorage();
	});

	after(async () => {
		// Clean up temp directory
		try {
			await fs.rm(tempDir, { recursive: true, force: true });
		} catch {
			// Ignore cleanup errors
		}
	});

	void it("should initialize storage", async () => {
		await storage.init("test-repo");
		const gitDirExists = await storage.exists(".git");
		assert.ok(gitDirExists);
	});

	void it("should write and read files", async () => {
		await storage.init("test-repo");
		const testData = new Uint8Array([1, 2, 3, 4, 5]);
		await storage.writeFile("test.txt", testData);

		const readData = await storage.readFile("test.txt");
		assert.deepEqual(readData, testData);
	});

	void it("should check if file exists", async () => {
		await storage.init("test-repo");
		const testData = new Uint8Array([1, 2, 3]);
		await storage.writeFile("exists-test.txt", testData);

		assert.ok(await storage.exists("exists-test.txt"));
		assert.ok(!(await storage.exists("nonexistent.txt")));
	});

	void it("should delete files", async () => {
		await storage.init("test-repo");
		const testData = new Uint8Array([1, 2, 3]);
		await storage.writeFile("delete-test.txt", testData);

		assert.ok(await storage.exists("delete-test.txt"));
		await storage.deleteFile("delete-test.txt");
		assert.ok(!(await storage.exists("delete-test.txt")));
	});

	void it("should create directories", async () => {
		await storage.init("test-repo");
		await storage.createDirectory("test/nested/dir");

		assert.ok(await storage.exists("test/nested/dir"));
	});

	void it("should list directory contents", async () => {
		await storage.init("test-repo");
		await storage.createDirectory("list-test");
		await storage.writeFile("list-test/file1.txt", new Uint8Array([1]));
		await storage.writeFile("list-test/file2.txt", new Uint8Array([2]));

		const contents = await storage.listDirectory("list-test");
		assert.ok(contents.includes("file1.txt"));
		assert.ok(contents.includes("file2.txt"));
	});

	void it("should delete directories", async () => {
		await storage.init("test-repo");
		await storage.createDirectory("delete-dir-test");
		await storage.writeFile("delete-dir-test/file.txt", new Uint8Array([1]));

		assert.ok(await storage.exists("delete-dir-test"));
		await storage.deleteDirectory("delete-dir-test");
		assert.ok(!(await storage.exists("delete-dir-test")));
	});

	void it("should get file info", async () => {
		await storage.init("test-repo");
		const testData = new Uint8Array([1, 2, 3, 4, 5]);
		await storage.writeFile("info-test.txt", testData);

		const info = await storage.getFileInfo("info-test.txt");
		assert.equal(info.size, 5);
		assert.ok(info.lastModified instanceof Date);
	});

	void it("should throw error when storage not initialized", async () => {
		const uninitializedStorage = new FsStorage();

		try {
			await uninitializedStorage.readFile("test.txt");
			assert.fail("Should have thrown error");
		} catch (error: any) {
			assert.ok(error.message.includes("Storage not initialized"));
		}
	});
});
