import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitObjectStore } from "./git.object.ts";
import { GitPackWriter } from "./git.pack.ts";
import { GitRepository } from "./git.repository.ts";
import { MemoryStorage } from "./git.storage.ts";
import { hexToBytes } from "./git.utils.ts";
import { toStream } from "./test.helpers.ts";

void describe("GitRepository", () => {
  void describe("init", () => {
    void it("should initialize repository", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      assert.ok(await storage.exists(".git"));
    });

    void it("should create git directory structure", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      assert.ok(await storage.exists(".git/objects"));
      assert.ok(await storage.exists(".git/refs"));
      assert.ok(await storage.exists(".git/hooks"));
      assert.ok(await storage.exists(".git/info"));
    });

    void it("should create HEAD file pointing to main", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const head = await storage.readFile(".git/HEAD");
      const content = new TextDecoder().decode(head);
      assert.ok(content.includes("refs/heads/main"));
    });

    void it("should use custom branch name", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test", branch: "master" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const head = await storage.readFile(".git/HEAD");
      const content = new TextDecoder().decode(head);
      assert.ok(content.includes("refs/heads/master"));
    });
  });

  void describe("refs", () => {
    void it("should store and retrieve HEAD", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const headRef = await repo.getRef("HEAD");
      assert.ok(typeof headRef === "string" || headRef === null);
    });

    void it("should get all refs", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const allRefs = await repo.getAllRefs();
      assert.ok(Array.isArray(allRefs));
    });

    void it("should write and read a ref", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const oid = "a".repeat(40);
      await repo.writeRef("refs/heads/test", oid);

      const ref = await repo.getRef("refs/heads/test");
      assert.equal(ref, oid);
    });

    void it("should delete a ref", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const oid = "a".repeat(40);
      await repo.writeRef("refs/heads/test", oid);
      await repo.deleteRef("refs/heads/test");

      const ref = await repo.getRef("refs/heads/test");
      assert.equal(ref, null);
    });
  });

  void describe("objects", () => {
    void it("should write and read a blob object", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const content = new TextEncoder().encode("Hello, World!");
      const oid = await repo.writeObject("blob", content);

      assert.equal(oid.length, 40);

      const obj = await repo.readObject(oid);
      assert.equal(obj.type, "blob");
      assert.deepEqual(obj.data, content);
    });

    void it("should write and read a tree object", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Create a blob first
      const blobContent = new TextEncoder().encode("file content");
      const blobOid = await repo.writeObject("blob", blobContent);

      // Create tree entry
      const treeData = new Uint8Array([
        ...new TextEncoder().encode("100644 test.txt\0"),
        ...hexToBytes(blobOid),
      ]);
      const treeOid = await repo.writeObject("tree", treeData);

      assert.equal(treeOid.length, 40);

      const obj = await repo.readObject(treeOid);
      assert.equal(obj.type, "tree");
    });
  });

  void describe("parseGitUrl", () => {
    void it("should parse HTTPS URL", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const info = repo.parseGitUrl("https://github.com/user/repo.git");
      assert.equal(info.host, "github.com");
      assert.equal(info.repo, "repo");
      assert.equal(info.protocol, "http");
    });

    void it("should parse SSH URL", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const info = repo.parseGitUrl("git@github.com:user/repo.git");
      assert.equal(info.host, "github.com");
      assert.equal(info.repo, "repo");
      assert.equal(info.protocol, "ssh");
    });

    void it("should throw on invalid URL", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      try {
        repo.parseGitUrl("invalid-url");
        assert.fail("Should have thrown");
      } catch (error: any) {
        assert.ok(error.message.includes("Invalid git URL"));
      }
    });
  });

  void describe("parseCommit", () => {
    void it("should parse commit data", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const commitText = `tree ${"a".repeat(40)}
parent ${"b".repeat(40)}
author Test User <test@example.com> 1234567890 +0000
committer Test User <test@example.com> 1234567890 +0000

Test commit message`;

      const data = new TextEncoder().encode(commitText);
      const parsed = repo.parseCommit(data);

      assert.equal(parsed.tree, "a".repeat(40));
      assert.equal(parsed.parent, "b".repeat(40));
      assert.ok(parsed.author.includes("Test User"));
      assert.ok(parsed.message.includes("Test commit message"));
    });

    void it("should parse commit without parent", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const commitText = `tree ${"a".repeat(40)}
author Test <test@test.com> 1234567890 +0000
committer Test <test@test.com> 1234567890 +0000

Initial commit`;

      const data = new TextEncoder().encode(commitText);
      const parsed = repo.parseCommit(data);

      assert.equal(parsed.tree, "a".repeat(40));
      assert.equal(parsed.parent, undefined);
    });

    void it("should parse commit with multiple parents", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const parent1 = "b".repeat(40);
      const parent2 = "c".repeat(40);
      const commitText = `tree ${"a".repeat(40)}
parent ${parent1}
parent ${parent2}
author Test <test@test.com> 1234567890 +0000
committer Test <test@test.com> 1234567890 +0000

Merge commit`;

      const data = new TextEncoder().encode(commitText);
      const parsed = repo.parseCommit(data);

      assert.equal(parsed.parent, parent1);
      assert.deepEqual(parsed.parents, [parent1, parent2]);
    });
  });

  void describe("shallow grafts", () => {
    void it("should read and write shallow commits", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Initially empty
      const empty = await repo.getShallowCommits();
      assert.equal(empty.size, 0);

      // Write shallow commits
      const oid1 = "a".repeat(40);
      const oid2 = "b".repeat(40);
      await repo.setShallowCommits(new Set([oid1, oid2]));

      const result = await repo.getShallowCommits();
      assert.equal(result.size, 2);
      assert.ok(result.has(oid1));
      assert.ok(result.has(oid2));
    });

    void it("should delete shallow file when set is empty", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const oid = "a".repeat(40);
      await repo.setShallowCommits(new Set([oid]));
      assert.equal((await repo.getShallowCommits()).size, 1);

      await repo.setShallowCommits(new Set());
      assert.equal((await repo.getShallowCommits()).size, 0);
    });
  });

  void describe("parseTree", () => {
    void it("should parse tree data", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Create a blob
      const blobOid = await repo.writeObject("blob", new TextEncoder().encode("content"));

      // Create tree data manually
      const mode = "100644";
      const name = "file.txt";
      const header = new TextEncoder().encode(`${mode} ${name}\0`);
      const oidBytes = hexToBytes(blobOid);
      const treeData = new Uint8Array(header.length + oidBytes.length);
      treeData.set(header);
      treeData.set(oidBytes, header.length);

      const entries = repo.parseTree(treeData);

      assert.equal(entries.length, 1);
      assert.equal(entries[0]!.mode, "100644");
      assert.equal(entries[0]!.name, "file.txt");
      assert.equal(entries[0]!.oid, blobOid);
    });
  });

  void describe("commit", () => {
    void it("should create a commit", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Add a file to the index
      const content = new TextEncoder().encode("Hello!");
      await repo.add("test.txt", content);

      const commitOid = await repo.commit("Initial commit", {
        name: "Test",
        email: "test@test.com",
      });

      assert.equal(commitOid.length, 40);

      const obj = await repo.readObject(commitOid);
      assert.equal(obj.type, "commit");
    });

    void it("should create commit with parent", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // First commit
      await repo.add("file1.txt", new TextEncoder().encode("content1"));
      const commit1 = await repo.commit("Commit 1", { name: "Test", email: "t@t.com" });

      // Second commit
      await repo.add("file2.txt", new TextEncoder().encode("content2"));
      const commit2 = await repo.commit("Commit 2", { name: "Test", email: "t@t.com" });

      const obj = await repo.readObject(commit2);
      const parsed = repo.parseCommit(obj.data);

      assert.equal(parsed.parent, commit1);
    });
  });

  void describe("getCurrentHead", () => {
    void it("should return current HEAD ref", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const head = await repo.getCurrentHead();
      assert.equal(head, "refs/heads/main");
    });

    void it("should initialize HEAD in each storage namespace", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "server-root" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      await repo.initStorage("repo-a");
      assert.equal(await repo.getCurrentHead(), "refs/heads/main");

      await repo.initStorage("repo-b");
      assert.equal(await repo.getCurrentHead(), "refs/heads/main");

      await repo.writeFile(".git/HEAD", new TextEncoder().encode("ref: refs/heads/custom\n"));
      assert.equal(await repo.getCurrentHead(), "refs/heads/custom");

      await repo.initStorage("repo-a");
      assert.equal(await repo.getCurrentHead(), "refs/heads/main");
    });
  });

  void describe("getCurrentCommitOid", () => {
    void it("should return null for empty repo", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const oid = await repo.getCurrentCommitOid();
      assert.equal(oid, null);
    });

    void it("should return commit OID after commit", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      await repo.add("test.txt", new TextEncoder().encode("content"));
      const commitOid = await repo.commit("Test", { name: "T", email: "t@t.com" });

      const currentOid = await repo.getCurrentCommitOid();
      assert.equal(currentOid, commitOid);
    });

    void it("should return commit OID for detached HEAD", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const commitOid = "a".repeat(40);
      await repo.writeFile(".git/HEAD", new TextEncoder().encode(`${commitOid}\n`));

      const currentOid = await repo.getCurrentCommitOid();
      assert.equal(currentOid, commitOid);
    });
  });

  void describe("hashObject", () => {
    void it("should calculate correct hash", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const content = new TextEncoder().encode("test content\n");
      const hash = await repo.hashObject("blob", content);

      // SHA-1 hash should be 40 hex characters
      assert.equal(hash.length, 40);
      assert.ok(/^[0-9a-f]+$/.test(hash));
    });
  });

  void describe("index operations", () => {
    void it("should add and get index entries", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      await repo.add("file.txt", new TextEncoder().encode("content"));

      const entries = repo.getIndexEntries();
      assert.equal(entries.length, 1);
      assert.equal(entries[0]!.path, "file.txt");
    });

    void it("should add index entry directly", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      await repo.addIndexEntry({
        path: "direct.txt",
        oid: "a".repeat(40),
        mode: "100644",
        size: 10,
        mtime: Date.now(),
      });

      const entries = repo.getIndexEntries();
      assert.ok(entries.some((e) => e.path === "direct.txt"));
    });

    void it("should remove index entry", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      await repo.add("file.txt", new TextEncoder().encode("content"));
      await repo.removeIndexEntry("file.txt");

      const entries = repo.getIndexEntries();
      assert.ok(!entries.some((e) => e.path === "file.txt"));
    });
  });

  void describe("file operations", () => {
    void it("should read and write files", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const content = new TextEncoder().encode("file content");
      await repo.writeFile("myfile.txt", content);

      const read = await repo.readFile("myfile.txt");
      assert.deepEqual(read, content);
    });

    void it("should delete files", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      await repo.writeFile("delete.txt", new TextEncoder().encode("x"));
      await repo.deleteFile("delete.txt");

      try {
        await repo.readFile("delete.txt");
        assert.fail("Should have thrown");
      } catch {
        assert.ok(true);
      }
    });
  });

  void describe("collectTreeObjects", () => {
    void it("should collect all objects in a tree", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Create blobs
      const blob1 = await repo.writeObject("blob", new TextEncoder().encode("content1"));
      const blob2 = await repo.writeObject("blob", new TextEncoder().encode("content2"));

      // Create tree with blobs
      const treeData = new Uint8Array([
        ...new TextEncoder().encode("100644 file1.txt\0"),
        ...hexToBytes(blob1),
        ...new TextEncoder().encode("100644 file2.txt\0"),
        ...hexToBytes(blob2),
      ]);
      const treeOid = await repo.writeObject("tree", treeData);

      const objects = await repo.collectTreeObjects(treeOid);

      assert.ok(objects.includes(treeOid));
      assert.ok(objects.includes(blob1));
      assert.ok(objects.includes(blob2));
    });
  });

  void describe("findInTree", () => {
    void it("should find file in tree", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Create blob
      const blobOid = await repo.writeObject("blob", new TextEncoder().encode("content"));

      // Create tree
      const treeData = new Uint8Array([
        ...new TextEncoder().encode("100644 myfile.txt\0"),
        ...hexToBytes(blobOid),
      ]);
      const treeOid = await repo.writeObject("tree", treeData);

      const result = await repo.findInTree(treeOid, "myfile.txt");

      assert.ok(result);
      assert.equal(result!.oid, blobOid);
      assert.equal(result!.mode, "100644");
    });

    void it("should return null for non-existent file", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      const blobOid = await repo.writeObject("blob", new TextEncoder().encode("content"));
      const treeData = new Uint8Array([
        ...new TextEncoder().encode("100644 file.txt\0"),
        ...hexToBytes(blobOid),
      ]);
      const treeOid = await repo.writeObject("tree", treeData);

      const result = await repo.findInTree(treeOid, "nonexistent.txt");
      assert.equal(result, null);
    });
  });

  void describe("checkoutCommit", () => {
    void it("should checkout a commit and update index", async () => {
      const storage = new MemoryStorage();
      const config = { repoName: "test" };

      const repo = new GitRepository(storage, config);
      await repo.init();

      // Create and commit a file
      await repo.add("checkout.txt", new TextEncoder().encode("checkout content"));
      const commitOid = await repo.commit("Checkout test", { name: "T", email: "t@t.com" });

      // Clear index
      await repo.removeIndexEntry("checkout.txt");
      assert.equal(repo.getIndexEntries().length, 0);

      // Checkout the commit
      await repo.checkoutCommit(commitOid);

      // Index should be updated
      const entries = repo.getIndexEntries();
      assert.ok(entries.some((e) => e.path === "checkout.txt"));
    });
  });

  void describe("reachability and gc", () => {
    void it("should collect reachable objects from refs", async () => {
      const storage = new MemoryStorage();
      const repo = new GitRepository(storage, { repoName: "test" });
      await repo.init();

      await repo.add("reachable.txt", new TextEncoder().encode("reachable"));
      const commitOid = await repo.commit("Reachable", { name: "Test", email: "test@test.com" });
      const commit = await repo.readObject(commitOid);
      const info = repo.parseCommit(commit.data);
      const treeEntries = repo.parseTree((await repo.readObject(info.tree)).data);

      const reachable = await repo.getReachableObjects();
      assert.ok(reachable.has(commitOid));
      assert.ok(reachable.has(info.tree));
      assert.ok(reachable.has(treeEntries[0]!.oid));
    });

    void it("should delete unreachable loose objects when grace period is disabled", async () => {
      const storage = new MemoryStorage();
      const repo = new GitRepository(storage, { repoName: "test" });
      await repo.init();

      const danglingOid = await repo.writeObject("blob", new TextEncoder().encode("dangling"));
      const result = await repo.gc(0);

      assert.equal(result.deleted, 1);
      assert.ok(result.freedBytes > 0);
      await assert.rejects(() => repo.readObject(danglingOid), /not found/);
    });

    void it("should keep objects that are only reachable from reflogs", async () => {
      const storage = new MemoryStorage();
      const repo = new GitRepository(storage, { repoName: "test" });
      await repo.init();

      await repo.add("one.txt", new TextEncoder().encode("one"));
      const commit1 = await repo.commit("one", { name: "Test", email: "test@test.com" });
      await repo.add("two.txt", new TextEncoder().encode("two"));
      const commit2 = await repo.commit("two", { name: "Test", email: "test@test.com" });

      const rewound = await repo.compareAndSwapRef("refs/heads/main", commit2, commit1, "rewind");
      assert.equal(rewound, true);

      const result = await repo.gc(0);
      assert.equal(result.deleted, 0);

      const preserved = await repo.readObject(commit2);
      assert.equal(preserved.type, "commit");
    });

    void it("should delete unreachable packed objects", async () => {
      const sourceStorage = new MemoryStorage();
      await sourceStorage.init("source");
      const sourceObjectStore = new GitObjectStore(sourceStorage);
      await sourceObjectStore.init();

      const packedOid = await sourceObjectStore.writeObject(
        "blob",
        new TextEncoder().encode("packed"),
      );
      const writer = new GitPackWriter(sourceObjectStore);
      const artifacts = await writer.createPackArtifacts([packedOid]);

      const storage = new MemoryStorage();
      const repo = new GitRepository(storage, { repoName: "test" });
      await repo.init();
      await repo.parsePack(toStream(artifacts.packData));

      const before = await repo.readObject(packedOid);
      assert.equal(before.type, "blob");

      const result = await repo.gc(0);
      assert.equal(result.deleted, 1);
      await assert.rejects(() => repo.readObject(packedOid), /not found/);
    });
  });
});
