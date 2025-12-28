import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { CloudflareStorage } from "./server.storage.ts";

class MockR2Bucket implements Partial<R2Bucket> {
	#store = new Map<string, Uint8Array>();

	async get(key: string): Promise<R2ObjectBody | null> {
		const data = this.#store.get(key);
		if (!data) return null;
		return {
			arrayBuffer: async () =>
				data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
		} as R2ObjectBody;
	}

	async put(key: string, value: ArrayBuffer | Uint8Array): Promise<R2Object> {
		this.#store.set(key, new Uint8Array(value));
		return {} as R2Object;
	}

	async delete(keys: string | string[]): Promise<void> {
		const keyArray = Array.isArray(keys) ? keys : [keys];
		for (const key of keyArray) {
			this.#store.delete(key);
		}
	}
}

type SqlRow = Record<string, SqlStorageValue>;

class MockSqlStorage {
	#tables = new Map<string, Map<string, SqlRow>>();

	exec<T extends SqlRow = SqlRow>(
		query: string,
		...bindings: SqlStorageValue[]
	): SqlStorageCursor<T> {
		const rows: SqlRow[] = [];
		const q = query.trim().toUpperCase();

		if (q.startsWith("CREATE TABLE") || q.startsWith("CREATE INDEX")) {
			// Handle table/index creation - extract table name
			const tableMatch = query.match(
				/(?:TABLE|INDEX)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\s+ON\s+)?(\w+)/i,
			);
			if (tableMatch?.[1]) {
				const tableName = tableMatch[1].toLowerCase();
				if (!this.#tables.has(tableName)) {
					this.#tables.set(tableName, new Map());
				}
			}
			return this.#cursor(rows);
		}

		if (q.startsWith("INSERT") || q.startsWith("REPLACE")) {
			// INSERT OR REPLACE INTO git_files (repository, path, size, last_modified, r2_key) VALUES (?, ?, ?, ?, ?)
			const table = this.#tables.get("git_files") ?? new Map();
			this.#tables.set("git_files", table);
			const [repo, path, size, lastModified, r2Key] = bindings as [
				string,
				string,
				number,
				string,
				string,
			];
			const key = `${repo}:${path}`;
			table.set(key, { repository: repo, path, size, last_modified: lastModified, r2_key: r2Key });
			return this.#cursor(rows);
		}

		if (q.startsWith("SELECT")) {
			const table = this.#tables.get("git_files");
			if (!table) return this.#cursor(rows);

			if (q.includes("LIMIT 1") && bindings.length === 2) {
				// SELECT 1 FROM git_files WHERE repository = ? AND path = ? LIMIT 1
				const [repo, path] = bindings as [string, string];
				const key = `${repo}:${path}`;
				if (table.has(key)) {
					rows.push({ "1": 1 });
				}
			} else if (q.includes("SIZE") && q.includes("LAST_MODIFIED") && bindings.length === 2) {
				// SELECT size, last_modified FROM git_files WHERE repository = ? AND path = ?
				const [repo, path] = bindings as [string, string];
				const key = `${repo}:${path}`;
				const row = table.get(key);
				if (row) {
					rows.push({ size: row.size ?? 0, last_modified: row.last_modified ?? "" });
				}
			} else if (q.includes("PATH") && q.includes("LIKE")) {
				// SELECT path FROM git_files WHERE repository = ? AND path LIKE ?
				// or SELECT path, r2_key FROM git_files WHERE repository = ? AND path LIKE ?
				const [repo, pattern] = bindings as [string, string];
				const prefix = pattern.replace(/%$/, "");
				for (const [key, row] of table) {
					if (key.startsWith(`${repo}:`) && (row.path as string).startsWith(prefix)) {
						rows.push({ path: row.path ?? "", r2_key: row.r2_key ?? "" });
					}
				}
			}
			return this.#cursor(rows);
		}

		if (q.startsWith("DELETE")) {
			const table = this.#tables.get("git_files");
			if (!table) return this.#cursor(rows);

			if (q.includes("LIKE")) {
				// DELETE FROM git_files WHERE repository = ? AND path LIKE ?
				const [repo, pattern] = bindings as [string, string];
				const prefix = pattern.replace(/%$/, "");
				for (const [key, row] of table) {
					if (key.startsWith(`${repo}:`) && (row.path as string).startsWith(prefix)) {
						table.delete(key);
					}
				}
			} else {
				// DELETE FROM git_files WHERE repository = ? AND path = ?
				const [repo, path] = bindings as [string, string];
				const key = `${repo}:${path}`;
				table.delete(key);
			}
			return this.#cursor(rows);
		}

		return this.#cursor(rows);
	}

	#cursor<T extends SqlRow>(rows: SqlRow[]): SqlStorageCursor<T> {
		let index = 0;
		return {
			toArray: () => rows as T[],
			one: () => (rows[0] as T) ?? null,
			raw: () =>
				({ next: () => ({ done: true, value: undefined }) }) as IterableIterator<SqlStorageValue[]>,
			next: () => {
				if (index < rows.length) {
					return { done: false, value: rows[index++] as T };
				}
				return { done: true, value: undefined as unknown as T };
			},
			columnNames: [],
			rowsRead: rows.length,
			rowsWritten: 0,
			[Symbol.iterator]: function* () {
				yield* rows as T[];
			},
		} as SqlStorageCursor<T>;
	}
}

function createMockContext(): { ctx: DurableObjectState; env: Env } {
	const mockSql = new MockSqlStorage();
	const mockR2 = new MockR2Bucket();

	const ctx = {
		storage: {
			sql: mockSql,
		},
	} as unknown as DurableObjectState;

	const env = {
		GIT_OBJECTS: mockR2,
	} as unknown as Env;

	return { ctx, env };
}

void describe("CloudflareStorage", () => {
	const testData = {
		repoName: "test-repo",
		filePath: "objects/test.txt",
		fileContent: new Uint8Array([72, 101, 108, 108, 111]), // "Hello"
		dirPath: "objects",
	};

	void it("should initialize CloudflareStorage with ctx.storage.sql and env.GIT_OBJECTS", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init(testData.repoName);
		assert.ok(true, "CloudflareStorage initialized successfully");
	});

	void it("should write and read a file using ctx.storage.sql and env.GIT_OBJECTS", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("read-write-repo");

		await storage.writeFile(testData.filePath, testData.fileContent);
		const readData = await storage.readFile(testData.filePath);

		assert.deepStrictEqual(readData, testData.fileContent, "File content should match");
	});

	void it("should check if a file exists in storage", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("exists-repo");

		await storage.writeFile(testData.filePath, testData.fileContent);
		const exists = await storage.exists(testData.filePath);

		assert.strictEqual(exists, true, "File should exist");

		const notExists = await storage.exists("non-existent.txt");
		assert.strictEqual(notExists, false, "File should not exist");
	});

	void it("should delete a file from storage", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("delete-repo");

		await storage.writeFile(testData.filePath, testData.fileContent);
		let exists = await storage.exists(testData.filePath);
		assert.strictEqual(exists, true, "File should exist before deletion");

		await storage.deleteFile(testData.filePath);
		exists = await storage.exists(testData.filePath);
		assert.strictEqual(exists, false, "File should not exist after deletion");
	});

	void it("should retrieve file information from SQL storage", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("fileinfo-repo");

		await storage.writeFile(testData.filePath, testData.fileContent);
		const fileInfo = await storage.getFileInfo(testData.filePath);

		assert.strictEqual(fileInfo.size, testData.fileContent.length, "File size should match");
		assert.ok(fileInfo.lastModified instanceof Date, "lastModified should be a Date");
	});

	void it("should list directory contents from SQL storage", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("listdir-repo");

		await storage.writeFile("objects/file1.txt", testData.fileContent);
		await storage.writeFile("objects/file2.txt", testData.fileContent);

		const files = await storage.listDirectory("objects");

		assert.ok(files.includes("file1.txt"), "file1.txt should be in directory listing");
		assert.ok(files.includes("file2.txt"), "file2.txt should be in directory listing");
	});

	void it("should delete a directory and all contents from storage", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("deletedir-repo");

		await storage.writeFile("temp/file1.txt", testData.fileContent);
		await storage.writeFile("temp/file2.txt", testData.fileContent);

		let exists = await storage.exists("temp/file1.txt");
		assert.strictEqual(exists, true, "File should exist before directory deletion");

		await storage.deleteDirectory("temp");

		exists = await storage.exists("temp/file1.txt");
		assert.strictEqual(exists, false, "File should not exist after directory deletion");
	});

	void it("should throw error when accessing storage before initialization", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);

		await assert.rejects(
			() => storage.exists("test.txt"),
			/Storage not initialized/,
			"Should throw error when storage is not initialized",
		);
	});

	void it("should store file metadata in SQL and content in GIT_OBJECTS R2", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("metadata-repo");

		const testPath = "metadata-test.txt";
		await storage.writeFile(testPath, testData.fileContent);

		// Verify file can be read back from R2
		const readData = await storage.readFile(testPath);
		assert.deepStrictEqual(
			readData,
			testData.fileContent,
			"Content should be retrievable from GIT_OBJECTS",
		);

		// Verify metadata exists in SQL
		const fileInfo = await storage.getFileInfo(testPath);
		assert.strictEqual(
			fileInfo.size,
			testData.fileContent.length,
			"Metadata should be in ctx.storage.sql",
		);
	});

	void it("should create directories implicitly for R2 storage", async () => {
		const { ctx, env } = createMockContext();

		const storage = new CloudflareStorage(ctx, env);
		await storage.init("mkdir-repo");

		// createDirectory should not throw even though R2 doesn't require it
		await storage.createDirectory("nested/path/dir");
		assert.ok(true, "createDirectory should handle implicit directories");
	});
});
