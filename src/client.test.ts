import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Client } from "./client.ts";
import { MemoryStorage } from "./git.storage.ts";

// Create a testable client that uses MemoryStorage
class TestableClient extends Client {
	constructor(storage: MemoryStorage) {
		super({ repoName: "test-repo" }, storage);
	}
}

void describe("Client", () => {
	void describe("init", () => {
		void it("should initialize repository", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);

			await client.init();

			assert.ok(await storage.exists(".git"));
			assert.ok(await storage.exists(".git/HEAD"));
		});
	});

	void describe("add", () => {
		void it("should add file to index", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			// Create a file in storage
			await storage.writeFile("README.md", new TextEncoder().encode("# Hello"));

			await client.add("README.md");

			const status = await client.status();
			assert.ok(status.staged.includes("README.md"));
		});
	});

	void describe("commit", () => {
		void it("should create a commit", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			// Add a file
			await storage.writeFile("test.txt", new TextEncoder().encode("content"));
			await client.add("test.txt");

			const commitOid = await client.commit("Initial commit", {
				name: "Test User",
				email: "test@example.com",
			});

			assert.ok(commitOid);
			assert.equal(commitOid.length, 40); // SHA-1 hex string
		});
	});

	void describe("status", () => {
		void it("should return empty status for new repo", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			const status = await client.status();

			assert.deepEqual(status.staged, []);
			assert.deepEqual(status.modified, []);
			assert.deepEqual(status.untracked, []);
		});

		void it("should show staged files", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("hello"));
			await client.add("file.txt");

			const status = await client.status();
			assert.ok(status.staged.includes("file.txt"));
		});
	});

	void describe("log", () => {
		void it("should return empty log for new repo", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			const commits = await client.log();

			assert.deepEqual(commits, []);
		});

		void it("should return commits after commit", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("test.txt", new TextEncoder().encode("content"));
			await client.add("test.txt");
			await client.commit("Test commit", { name: "Test", email: "test@test.com" });

			const commits = await client.log();

			assert.equal(commits.length, 1);
			assert.ok(commits[0].message.includes("Test commit"));
		});
	});

	void describe("branch", () => {
		void it("should list branches", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			// Create initial commit so we can create branches
			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");
			await client.commit("Initial", { name: "Test", email: "test@test.com" });

			const branches = await client.branch();

			assert.ok(Array.isArray(branches));
			assert.ok(branches?.includes("main"));
		});

		void it("should create a branch", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");
			await client.commit("Initial", { name: "Test", email: "test@test.com" });

			await client.branch("feature");

			const branches = await client.branch();
			assert.ok(branches?.includes("feature"));
		});
	});

	void describe("checkout", () => {
		void it("should checkout a commit", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");
			const commitOid = await client.commit("Initial", { name: "Test", email: "test@test.com" });

			await client.checkout(commitOid);

			// HEAD should be updated
			const head = await storage.readFile(".git/HEAD");
			assert.ok(new TextDecoder().decode(head).includes("refs/heads/"));
		});
	});

	void describe("switch", () => {
		void it("should switch to an existing branch", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");
			await client.commit("Initial", { name: "Test", email: "test@test.com" });

			await client.branch("feature");
			await client.switch("feature");

			const head = await storage.readFile(".git/HEAD");
			assert.ok(new TextDecoder().decode(head).includes("refs/heads/feature"));
		});

		void it("should throw for non-existent branch", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			try {
				await client.switch("nonexistent");
				assert.fail("Should have thrown");
			} catch (error: any) {
				assert.ok(error.message.includes("not found"));
			}
		});
	});

	void describe("rm", () => {
		void it("should remove file from index", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");

			await client.rm("file.txt", { cached: true });

			const status = await client.status();
			assert.ok(!status.staged.includes("file.txt"));
		});

		void it("should throw for non-existent file", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			try {
				await client.rm("nonexistent.txt");
				assert.fail("Should have thrown");
			} catch (error: any) {
				assert.ok(error.message.includes("did not match"));
			}
		});
	});

	void describe("mv", () => {
		void it("should move file in index", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("old.txt", new TextEncoder().encode("content"));
			await client.add("old.txt");

			await client.mv("old.txt", "new.txt");

			const status = await client.status();
			assert.ok(!status.staged.includes("old.txt"));
			assert.ok(status.staged.includes("new.txt"));
		});

		void it("should throw for non-indexed file", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			try {
				await client.mv("nonexistent.txt", "new.txt");
				assert.fail("Should have thrown");
			} catch (error: any) {
				assert.ok(error.message.includes("not found"));
			}
		});
	});

	void describe("restore", () => {
		void it("should restore file from HEAD", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			// Create and commit a file
			await storage.writeFile("file.txt", new TextEncoder().encode("original"));
			await client.add("file.txt");
			await client.commit("Initial", { name: "Test", email: "test@test.com" });

			// Modify the file
			await storage.writeFile("file.txt", new TextEncoder().encode("modified"));

			// Restore from HEAD
			await client.restore("file.txt");

			const content = await storage.readFile("file.txt");
			assert.equal(new TextDecoder().decode(content), "original");
		});

		void it("should throw for repo without commits", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			try {
				await client.restore("file.txt");
				assert.fail("Should have thrown");
			} catch (error: any) {
				assert.ok(error.message.includes("No HEAD commit"));
			}
		});
	});

	void describe("tag", () => {
		void it("should create a tag", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");
			await client.commit("Initial", { name: "Test", email: "test@test.com" });

			await client.tag("v1.0.0");

			assert.ok(await storage.exists(".git/refs/tags/v1.0.0"));
		});

		void it("should throw for repo without commits", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			try {
				await client.tag("v1.0.0");
				assert.fail("Should have thrown");
			} catch (error: any) {
				assert.ok(error.message.includes("No HEAD commit"));
			}
		});
	});

	void describe("show", () => {
		void it("should show a commit object", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content"));
			await client.add("file.txt");
			const commitOid = await client.commit("Initial", { name: "Test", email: "test@test.com" });

			const obj = await client.show(commitOid);

			assert.equal(obj.type, "commit");
			assert.ok(obj.data instanceof Uint8Array);
		});
	});

	void describe("reset", () => {
		void it("should reset to a commit", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await storage.writeFile("file.txt", new TextEncoder().encode("content1"));
			await client.add("file.txt");
			const commit1 = await client.commit("Commit 1", { name: "Test", email: "test@test.com" });

			await storage.writeFile("file2.txt", new TextEncoder().encode("content2"));
			await client.add("file2.txt");
			await client.commit("Commit 2", { name: "Test", email: "test@test.com" });

			await client.reset(true, commit1);

			const log = await client.log();
			assert.equal(log.length, 1);
		});
	});

	void describe("remote", () => {
		void it("should add a remote", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await client.remote("add", "origin", "https://github.com/user/repo.git");

			const remotes = await client.getAllRemotes();
			assert.equal(remotes["origin"], "https://github.com/user/repo.git");
		});

		void it("should remove a remote", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await client.remote("add", "origin", "https://github.com/user/repo.git");
			await client.remote("remove", "origin");

			const remotes = await client.getAllRemotes();
			assert.equal(remotes["origin"], undefined);
		});

		void it("should get a specific remote", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			await client.remote("add", "origin", "https://github.com/user/repo.git");

			const url = await client.getRemote("origin");
			assert.equal(url, "https://github.com/user/repo.git");
		});

		void it("should return null for non-existent remote", async () => {
			const storage = new MemoryStorage();
			await storage.init("test-repo");
			const client = new TestableClient(storage);
			await client.init();

			const url = await client.getRemote("nonexistent");
			assert.equal(url, null);
		});
	});
});
