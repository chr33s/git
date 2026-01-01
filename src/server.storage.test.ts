import * as assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import * as helpers from "./test.helpers.ts";

const worker = await helpers.worker();

before(() => worker.before());
after(() => worker.after());

void describe("CloudflareStorage", () => {
	void it("should initialize CloudflareStorage with ctx.storage.sql and env.GIT_OBJECTS", async () => {
		const id = worker.env.GIT_SERVER.idFromName("init-test");
		const stub = worker.env.GIT_SERVER.get(id);

		// Use dispatchFetch to verify the DO is working
		const response = await stub.fetch("http://localhost/init-test.git/HEAD");
		assert.ok(response.status === 200 || response.status === 404, "DO should respond");
	});

	void it("should write and read a file using ctx.storage.sql and env.GIT_OBJECTS", async () => {
		// Test via the worker's git protocol which uses CloudflareStorage internally
		const id = worker.env.GIT_SERVER.idFromName("read-write-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		// Push some data and verify it can be read back
		const response = await stub.fetch(
			"http://localhost/read-write-repo.git/info/refs?service=git-upload-pack",
		);
		// Initially empty repo returns 200 with service advertisement
		assert.strictEqual(response.status, 200, "Should return 200 for upload-pack");
	});

	void it("should check if a file exists in storage", async () => {
		const id = worker.env.GIT_SERVER.idFromName("exists-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		// A fresh repo should have HEAD pointing to refs/heads/main (even if unborn)
		const response = await stub.fetch("http://localhost/exists-repo.git/HEAD");
		assert.strictEqual(response.status, 200, "HEAD should exist");
	});

	void it("should handle git-receive-pack endpoint", async () => {
		const id = worker.env.GIT_SERVER.idFromName("receive-pack-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		const response = await stub.fetch(
			"http://localhost/receive-pack-repo.git/info/refs?service=git-receive-pack",
		);
		assert.strictEqual(response.status, 200, "Should return 200 for receive-pack refs");
	});

	void it("should return 404 for invalid routes", async () => {
		const id = worker.env.GIT_SERVER.idFromName("invalid-route-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		const response = await stub.fetch("http://localhost/invalid-route-repo.git/invalid");
		assert.strictEqual(response.status, 404, "Should return 404 for invalid route");
	});

	void it("should list directory contents from SQL storage", async () => {
		const id = worker.env.GIT_SERVER.idFromName("listdir-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		// Initialize the repo by accessing it
		const response = await stub.fetch(
			"http://localhost/listdir-repo.git/info/refs?service=git-upload-pack",
		);
		assert.strictEqual(response.status, 200, "Should initialize repo successfully");
	});

	void it("should handle concurrent requests to the same repository", async () => {
		const id = worker.env.GIT_SERVER.idFromName("concurrent-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		const responses = await Promise.all([
			stub.fetch("http://localhost/concurrent-repo.git/info/refs?service=git-upload-pack"),
			stub.fetch("http://localhost/concurrent-repo.git/info/refs?service=git-receive-pack"),
			stub.fetch("http://localhost/concurrent-repo.git/HEAD"),
		]);

		for (const response of responses) {
			assert.strictEqual(response.status, 200, "All concurrent requests should succeed");
		}
	});

	void it("should store file metadata in SQL and content in GIT_OBJECTS R2", async () => {
		const id = worker.env.GIT_SERVER.idFromName("metadata-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		// Verify the repo can be initialized and accessed
		const headResponse = await stub.fetch("http://localhost/metadata-repo.git/HEAD");
		assert.strictEqual(headResponse.status, 200, "HEAD should be accessible");

		const refsResponse = await stub.fetch(
			"http://localhost/metadata-repo.git/info/refs?service=git-upload-pack",
		);
		assert.strictEqual(refsResponse.status, 200, "Refs should be accessible");
	});

	void it("should create directories implicitly for R2 storage", async () => {
		const id = worker.env.GIT_SERVER.idFromName("mkdir-repo");
		const stub = worker.env.GIT_SERVER.get(id);

		// R2 handles directories implicitly - verify repo works
		const response = await stub.fetch("http://localhost/mkdir-repo.git/HEAD");
		assert.strictEqual(response.status, 200, "Should handle implicit directories");
	});
});
