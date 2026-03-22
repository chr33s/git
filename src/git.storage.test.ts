import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { MemoryStorage, validateStoragePath } from "./git.storage.ts";

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
    await storage.writeFile(".git/test.txt", testData);

    const readData = await storage.readFile(".git/test.txt");
    assert.deepEqual(readData, testData);
  });

  void it("should check if file exists", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    const testData = new Uint8Array([1, 2, 3]);
    await storage.writeFile(".git/exists-test.txt", testData);

    assert.ok(await storage.exists(".git/exists-test.txt"));
    assert.ok(!(await storage.exists(".git/nonexistent.txt")));
  });

  void it("should delete files", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    const testData = new Uint8Array([1, 2, 3]);
    await storage.writeFile(".git/delete-test.txt", testData);

    assert.ok(await storage.exists(".git/delete-test.txt"));
    await storage.deleteFile(".git/delete-test.txt");
    assert.ok(!(await storage.exists(".git/delete-test.txt")));
  });

  void it("should throw error when deleting non-existent file", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    try {
      await storage.deleteFile(".git/nonexistent.txt");
      assert.fail("Should have thrown error");
    } catch (error: any) {
      assert.ok(error.message.includes("File not found"));
    }
  });

  void it("should create directories", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await storage.createDirectory(".git/test/nested/dir");
    assert.ok(await storage.exists(".git/test/nested/dir"));
  });

  void it("should list directory contents", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await storage.createDirectory(".git/list-test");
    await storage.writeFile(".git/list-test/file1.txt", new Uint8Array([1]));
    await storage.writeFile(".git/list-test/file2.txt", new Uint8Array([2]));
    await storage.createDirectory(".git/list-test/subdir");

    const contents = await storage.listDirectory(".git/list-test");
    assert.ok(contents.includes("file1.txt"));
    assert.ok(contents.includes("file2.txt"));
    assert.ok(contents.includes("subdir"));
  });

  void it("should delete directories recursively", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await storage.createDirectory(".git/delete-dir-test");
    await storage.writeFile(".git/delete-dir-test/file.txt", new Uint8Array([1]));
    await storage.createDirectory(".git/delete-dir-test/subdir");
    await storage.writeFile(".git/delete-dir-test/subdir/nested.txt", new Uint8Array([2]));

    assert.ok(await storage.exists(".git/delete-dir-test"));
    await storage.deleteDirectory(".git/delete-dir-test");
    assert.ok(!(await storage.exists(".git/delete-dir-test")));
  });

  void it("should get file info", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    const testData = new Uint8Array([1, 2, 3, 4, 5]);
    await storage.writeFile(".git/info-test.txt", testData);

    const info = await storage.getFileInfo(".git/info-test.txt");
    assert.equal(info.size, 5);
    assert.ok(info.lastModified instanceof Date);
  });

  void it("should throw error when storage not initialized", async () => {
    const storage = new MemoryStorage();

    try {
      await storage.readFile(".git/test.txt");
      assert.fail("Should have thrown error");
    } catch (error: any) {
      assert.ok(error.message.includes("Storage not initialized"));
    }
  });

  void it("should throw error when reading non-existent file", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    try {
      await storage.readFile(".git/nonexistent.txt");
      assert.fail("Should have thrown error");
    } catch (error: any) {
      assert.ok(error.message.includes("File not found"));
    }
  });

  void it("should automatically create parent directories on write", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await storage.writeFile(".git/deep/nested/path/file.txt", new Uint8Array([1, 2, 3]));

    assert.ok(await storage.exists(".git"));
    assert.ok(await storage.exists(".git/deep"));
    assert.ok(await storage.exists(".git/deep/nested"));
    assert.ok(await storage.exists(".git/deep/nested/path"));
    assert.ok(await storage.exists(".git/deep/nested/path/file.txt"));
  });

  void it("should isolate repository namespaces", async () => {
    const storage = new MemoryStorage();

    await storage.init("repo-a");
    await storage.writeFile(".git/shared.txt", new Uint8Array([1]));

    await storage.init("repo-b");
    assert.ok(!(await storage.exists(".git/shared.txt")));
    await storage.writeFile(".git/shared.txt", new Uint8Array([2]));

    await storage.init("repo-a");
    assert.deepEqual(await storage.readFile(".git/shared.txt"), new Uint8Array([1]));

    await storage.init("repo-b");
    assert.deepEqual(await storage.readFile(".git/shared.txt"), new Uint8Array([2]));
  });
});

void describe("validateStoragePath", () => {
  void it("should accept valid paths", () => {
    assert.doesNotThrow(() => validateStoragePath(".git"));
    assert.doesNotThrow(() => validateStoragePath(".git/HEAD"));
    assert.doesNotThrow(() => validateStoragePath(".git/refs/heads/main"));
    assert.doesNotThrow(() =>
      validateStoragePath(".git/objects/ab/cdef1234567890abcdef1234567890abcdef12"),
    );
    assert.doesNotThrow(() => validateStoragePath("README.md"));
    assert.doesNotThrow(() => validateStoragePath("src/index.ts"));
  });

  void it("should reject paths with '..' traversal", () => {
    assert.throws(() => validateStoragePath(".git/../etc/passwd"), /traversal/);
    assert.throws(() => validateStoragePath(".git/refs/../../etc/passwd"), /traversal/);
    assert.throws(() => validateStoragePath("../outside"), /traversal/);
  });

  void it("should reject absolute paths", () => {
    assert.throws(() => validateStoragePath("/etc/passwd"), /absolute/);
    assert.throws(() => validateStoragePath("/root/.git/HEAD"), /absolute/);
  });

  void it("should reject null bytes", () => {
    assert.throws(() => validateStoragePath(".git/HEAD\0"), /null byte/);
    assert.throws(() => validateStoragePath(".git/refs\0/heads/main"), /null byte/);
  });

  void it("should reject path traversal in MemoryStorage operations", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await assert.rejects(() => storage.readFile("../etc/passwd"), /traversal/);
    await assert.rejects(
      () => storage.writeFile("../etc/passwd", new Uint8Array([1])),
      /traversal/,
    );
    await assert.rejects(() => storage.deleteFile(".git/../../etc/passwd"), /traversal/);
    await assert.rejects(() => storage.exists("/etc/passwd"), /absolute/);
    await assert.rejects(() => storage.listDirectory("../outside"), /traversal/);
    await assert.rejects(() => storage.createDirectory("/absolute/dir"), /absolute/);
    await assert.rejects(() => storage.deleteDirectory("../.."), /traversal/);
    await assert.rejects(() => storage.getFileInfo(".git/HEAD\0"), /null byte/);
  });
});
