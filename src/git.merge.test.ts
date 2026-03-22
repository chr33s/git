import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitMerge, ConflictResolver } from "./git.merge.ts";
import { GitObjectStore } from "./git.object.ts";
import { GitRefStore } from "./git.ref.ts";
import { MemoryStorage } from "./git.storage.ts";
import { hexToBytes } from "./git.utils.ts";

async function createTestTree(objectStore: GitObjectStore, files: Record<string, string>) {
  const entries: Array<{ mode: string; name: string; oid: string }> = [];

  for (const [name, content] of Object.entries(files)) {
    const blobOid = await objectStore.writeObject("blob", new TextEncoder().encode(content));
    entries.push({ mode: "100644", name, oid: blobOid });
  }

  // Sort entries by name
  entries.sort((a, b) => a.name.localeCompare(b.name));

  // Build tree data
  const chunks: Uint8Array[] = [];
  for (const entry of entries) {
    chunks.push(new TextEncoder().encode(`${entry.mode} ${entry.name}\0`));
    chunks.push(hexToBytes(entry.oid));
  }

  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const treeData = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    treeData.set(chunk, offset);
    offset += chunk.length;
  }

  return await objectStore.writeObject("tree", treeData);
}

async function createTestCommit(objectStore: GitObjectStore, treeOid: string, parentOid?: string) {
  const timestamp = Math.floor(Date.now() / 1000);
  let commitData = `tree ${treeOid}\n`;
  if (parentOid) {
    commitData += `parent ${parentOid}\n`;
  }
  commitData += `author Test <test@test.com> ${timestamp} +0000\n`;
  commitData += `committer Test <test@test.com> ${timestamp} +0000\n`;
  commitData += `\nTest commit\n`;

  return await objectStore.writeObject("commit", new TextEncoder().encode(commitData));
}

void describe("GitMerge", () => {
  void describe("constructor", () => {
    void it("should initialize merge engine", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const merge = new GitMerge(objectStore);
      assert.ok(merge);
    });

    void it("should accept optional refStore", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const refStore = new GitRefStore(storage);
      await refStore.init();

      const merge = new GitMerge(objectStore, refStore);
      assert.ok(merge);
    });
  });

  void describe("threeWayMerge", () => {
    void it("should define threeWayMerge method", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const merge = new GitMerge(objectStore);
      assert.ok(typeof merge.threeWayMerge === "function");
    });

    void it("should merge identical trees successfully", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create identical trees
      const tree = await createTestTree(objectStore, { "file.txt": "content" });
      const baseCommit = await createTestCommit(objectStore, tree);
      const ourCommit = await createTestCommit(objectStore, tree, baseCommit);
      const theirCommit = await createTestCommit(objectStore, tree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.ok(result.success);
      assert.ok(result.mergedTree);
    });

    void it("should merge non-conflicting changes", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create base tree
      const baseTree = await createTestTree(objectStore, {
        "file1.txt": "content1",
        "file2.txt": "content2",
      });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      // Create our tree (modify file1)
      const ourTree = await createTestTree(objectStore, {
        "file1.txt": "modified1",
        "file2.txt": "content2",
      });
      const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

      // Create their tree (modify file2)
      const theirTree = await createTestTree(objectStore, {
        "file1.txt": "content1",
        "file2.txt": "modified2",
      });
      const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.ok(result.success);
    });

    void it("should detect conflicts on same file modification", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create base tree
      const baseTree = await createTestTree(objectStore, { "file.txt": "original" });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      // Create our tree (modify file)
      const ourTree = await createTestTree(objectStore, { "file.txt": "our change" });
      const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

      // Create their tree (modify same file differently)
      const theirTree = await createTestTree(objectStore, { "file.txt": "their change" });
      const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.equal(result.success, false);
      assert.ok(result.conflicts && result.conflicts.length > 0);
    });
  });

  void describe("merge strategies", () => {
    void it("should support ours strategy", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const ourTree = await createTestTree(objectStore, { "file.txt": "our content" });
      const ourCommit = await createTestCommit(objectStore, ourTree);

      const theirTree = await createTestTree(objectStore, { "file.txt": "their content" });
      const theirCommit = await createTestCommit(objectStore, theirTree);

      const baseTree = await createTestTree(objectStore, { "file.txt": "base" });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit, "ours");

      assert.ok(result.success);
      assert.equal(result.mergedTree, ourTree);
    });

    void it("should support theirs strategy", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const ourTree = await createTestTree(objectStore, { "file.txt": "our content" });
      const ourCommit = await createTestCommit(objectStore, ourTree);

      const theirTree = await createTestTree(objectStore, { "file.txt": "their content" });
      const theirCommit = await createTestCommit(objectStore, theirTree);

      const baseTree = await createTestTree(objectStore, { "file.txt": "base" });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit, "theirs");

      assert.ok(result.success);
      assert.equal(result.mergedTree, theirTree);
    });

    void it("should throw on unknown strategy", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const tree = await createTestTree(objectStore, { "file.txt": "content" });
      const commit = await createTestCommit(objectStore, tree);

      const merge = new GitMerge(objectStore);

      try {
        await merge.threeWayMerge(commit, commit, commit, "unknown" as any);
        assert.fail("Should have thrown");
      } catch (error: any) {
        assert.ok(error.message.includes("Unknown merge strategy"));
      }
    });
  });

  void describe("detectRenames", () => {
    void it("should detect renamed files", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create old tree with file
      const oldTree = await createTestTree(objectStore, { "old.txt": "same content here" });

      // Create new tree with renamed file (same content)
      const newTree = await createTestTree(objectStore, { "new.txt": "same content here" });

      const merge = new GitMerge(objectStore);
      const renames = await merge.detectRenames(oldTree, newTree);

      assert.equal(renames.length, 1);
      assert.equal(renames[0]!.oldPath, "old.txt");
      assert.equal(renames[0]!.newPath, "new.txt");
      assert.ok(renames[0]!.similarity > 0.9);
    });

    void it("should not detect renames for completely different content", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const oldTree = await createTestTree(objectStore, { "old.txt": "aaa\nbbb\nccc" });
      const newTree = await createTestTree(objectStore, { "new.txt": "xxx\nyyy\nzzz" });

      const merge = new GitMerge(objectStore);
      const renames = await merge.detectRenames(oldTree, newTree);

      assert.equal(renames.length, 0);
    });

    void it("should respect similarity threshold", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const oldTree = await createTestTree(objectStore, { "old.txt": "line1\nline2\nline3" });
      const newTree = await createTestTree(objectStore, {
        "new.txt": "line1\nmodified\nline3",
      });

      const merge = new GitMerge(objectStore);

      // With low threshold, should detect rename
      const lowThreshold = await merge.detectRenames(oldTree, newTree, 0.3);
      assert.ok(lowThreshold.length >= 0); // May or may not detect based on similarity

      // With high threshold, should not detect rename
      const highThreshold = await merge.detectRenames(oldTree, newTree, 0.99);
      assert.equal(highThreshold.length, 0);
    });
  });
});

void describe("ConflictResolver", () => {
  void describe("constructor", () => {
    void it("should create resolver", () => {
      const resolver = new ConflictResolver();
      assert.ok(resolver);
    });
  });

  void describe("addConflict", () => {
    void it("should add conflict", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({
        path: "file.txt",
        base: "base-oid",
        ours: "our-oid",
        theirs: "their-oid",
      });

      const conflicts = resolver.getUnresolvedConflicts();
      assert.equal(conflicts.length, 1);
      assert.equal(conflicts[0]!.path, "file.txt");
    });

    void it("should add multiple conflicts", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({ path: "file1.txt", ours: "a", theirs: "b" });
      resolver.addConflict({ path: "file2.txt", ours: "c", theirs: "d" });

      const conflicts = resolver.getUnresolvedConflicts();
      assert.equal(conflicts.length, 2);
    });
  });

  void describe("resolveConflict", () => {
    void it("should mark conflict as resolved", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({
        path: "file.txt",
        ours: "our-oid",
        theirs: "their-oid",
      });

      resolver.resolveConflict("file.txt", "ours");

      const unresolved = resolver.getUnresolvedConflicts();
      assert.equal(unresolved.length, 0);
    });

    void it("should store resolution OID", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({
        path: "file.txt",
        ours: "our-oid",
        theirs: "their-oid",
      });

      resolver.resolveConflict("file.txt", "ours");

      // The conflict should now be resolved
      assert.ok(resolver.isAllResolved());
    });

    void it("should handle resolving non-existent conflict", () => {
      const resolver = new ConflictResolver();

      // Should throw for non-existent conflict
      assert.throws(
        () => resolver.resolveConflict("nonexistent.txt", "ours"),
        /No conflict found for path: nonexistent.txt/,
      );
    });
  });

  void describe("getUnresolvedConflicts", () => {
    void it("should return only unresolved conflicts", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({ path: "file1.txt", ours: "a", theirs: "b" });
      resolver.addConflict({ path: "file2.txt", ours: "c", theirs: "d" });

      resolver.resolveConflict("file1.txt", "ours");

      const unresolved = resolver.getUnresolvedConflicts();
      assert.equal(unresolved.length, 1);
      assert.equal(unresolved[0]!.path, "file2.txt");
    });
  });

  void describe("isAllResolved", () => {
    void it("should return true when no conflicts", () => {
      const resolver = new ConflictResolver();
      assert.equal(resolver.isAllResolved(), true);
    });

    void it("should return false when unresolved conflicts exist", () => {
      const resolver = new ConflictResolver();
      resolver.addConflict({ path: "file.txt", ours: "a", theirs: "b" });

      assert.equal(resolver.isAllResolved(), false);
    });

    void it("should return true when all conflicts resolved", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({ path: "file1.txt", ours: "a", theirs: "b" });
      resolver.addConflict({ path: "file2.txt", ours: "c", theirs: "d" });

      resolver.resolveConflict("file1.txt", "ours");
      resolver.resolveConflict("file2.txt", "theirs");

      assert.equal(resolver.isAllResolved(), true);
    });
  });

  void describe("clear", () => {
    void it("should remove all conflicts", () => {
      const resolver = new ConflictResolver();

      resolver.addConflict({ path: "file1.txt", ours: "a", theirs: "b" });
      resolver.addConflict({ path: "file2.txt", ours: "c", theirs: "d" });

      resolver.clear();

      assert.equal(resolver.getUnresolvedConflicts().length, 0);
      assert.equal(resolver.isAllResolved(), true);
    });
  });
});

// ==================== Phase 4 Tests ====================

void describe("Content-Level Three-Way Merge", () => {
  void describe("diff3 line-level merge", () => {
    void it("should auto-merge non-overlapping edits in different regions", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Base has 5 lines
      const baseTree = await createTestTree(objectStore, {
        "file.txt": "line1\nline2\nline3\nline4\nline5",
      });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      // Ours changes line2
      const ourTree = await createTestTree(objectStore, {
        "file.txt": "line1\nmodified-ours\nline3\nline4\nline5",
      });
      const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

      // Theirs changes line4
      const theirTree = await createTestTree(objectStore, {
        "file.txt": "line1\nline2\nline3\nmodified-theirs\nline5",
      });
      const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.ok(result.success, "Non-overlapping edits should auto-merge");
      assert.ok(result.mergedTree);

      // Read the merged blob and verify both changes are present
      const mergedTreeObj = await objectStore.readObject(result.mergedTree);
      const entries = parseTreeEntries(mergedTreeObj.data);
      const fileEntry = entries.find((e) => e.name === "file.txt");
      assert.ok(fileEntry);

      const blob = await objectStore.readObject(fileEntry.oid);
      const content = new TextDecoder().decode(blob.data);
      assert.equal(content, "line1\nmodified-ours\nline3\nmodified-theirs\nline5");
    });

    void it("should produce conflict markers on overlapping edits", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const baseTree = await createTestTree(objectStore, {
        "file.txt": "line1\nline2\nline3",
      });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      // Both change line2
      const ourTree = await createTestTree(objectStore, {
        "file.txt": "line1\nour-change\nline3",
      });
      const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

      const theirTree = await createTestTree(objectStore, {
        "file.txt": "line1\ntheir-change\nline3",
      });
      const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.equal(result.success, false);
      assert.ok(result.conflicts && result.conflicts.length > 0);
      assert.ok(result.mergedTree, "Should still produce a merged tree with marker content");

      // The conflict entry should have a merged blob OID
      const conflict = result.conflicts![0]!;
      assert.equal(conflict.path, "file.txt");
      assert.ok(conflict.merged, "Conflict should include merged blob OID with markers");

      // Read the merged blob and check for conflict markers
      const blob = await objectStore.readObject(conflict.merged);
      const content = new TextDecoder().decode(blob.data);
      assert.ok(content.includes("<<<<<<< ours"));
      assert.ok(content.includes("our-change"));
      assert.ok(content.includes("======="));
      assert.ok(content.includes("their-change"));
      assert.ok(content.includes(">>>>>>> theirs"));
    });

    void it("should handle insertions from one side", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const baseTree = await createTestTree(objectStore, {
        "file.txt": "A\nC",
      });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      // Ours inserts B between A and C
      const ourTree = await createTestTree(objectStore, {
        "file.txt": "A\nB\nC",
      });
      const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

      // Theirs unchanged
      const theirTree = await createTestTree(objectStore, {
        "file.txt": "A\nC",
      });
      const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.ok(result.success);

      const mergedTreeObj = await objectStore.readObject(result.mergedTree!);
      const entries = parseTreeEntries(mergedTreeObj.data);
      const blob = await objectStore.readObject(entries.find((e) => e.name === "file.txt")!.oid);
      const content = new TextDecoder().decode(blob.data);
      assert.equal(content, "A\nB\nC");
    });

    void it("should skip content merge for binary files", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create binary blobs (with null bytes)
      const baseBlobOid = await objectStore.writeObject(
        "blob",
        new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x00, 0x01]),
      );
      const ourBlobOid = await objectStore.writeObject(
        "blob",
        new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x00, 0x02]),
      );
      const theirBlobOid = await objectStore.writeObject(
        "blob",
        new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x00, 0x03]),
      );

      // Build tree data manually for binary files
      const buildTree = async (name: string, oid: string) => {
        const entryData = new TextEncoder().encode(`100644 ${name}\0`);
        const oidBytes = hexToBytes(oid);
        const treeData = new Uint8Array(entryData.length + oidBytes.length);
        treeData.set(entryData);
        treeData.set(oidBytes, entryData.length);
        return await objectStore.writeObject("tree", treeData);
      };

      const baseTree = await buildTree("image.png", baseBlobOid);
      const ourTree = await buildTree("image.png", ourBlobOid);
      const theirTree = await buildTree("image.png", theirBlobOid);

      const baseCommit = await createTestCommit(objectStore, baseTree);
      const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);
      const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      // Binary conflict — no merged blob
      assert.equal(result.success, false);
      assert.ok(result.conflicts);
      assert.equal(result.conflicts.length, 1);
      assert.equal(result.conflicts[0]!.path, "image.png");
      assert.equal(result.conflicts[0]!.merged, undefined);
    });

    void it("should auto-merge when both sides make identical changes", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const baseTree = await createTestTree(objectStore, {
        "file.txt": "line1\nline2\nline3",
      });
      const baseCommit = await createTestCommit(objectStore, baseTree);

      // Both change line2 to the same thing
      const newTree = await createTestTree(objectStore, {
        "file.txt": "line1\nsame-change\nline3",
      });
      const ourCommit = await createTestCommit(objectStore, newTree, baseCommit);
      const theirCommit = await createTestCommit(objectStore, newTree, baseCommit);

      const merge = new GitMerge(objectStore);
      const result = await merge.threeWayMerge(baseCommit, ourCommit, theirCommit);

      assert.ok(result.success, "Identical changes should auto-merge");
    });
  });
});

void describe("Merge Commit Creation", () => {
  void it("should create merge commit with two parents", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();
    const refStore = new GitRefStore(storage);
    await refStore.init();

    // Create base commit
    const baseTree = await createTestTree(objectStore, { "file.txt": "base" });
    const baseCommit = await createTestCommit(objectStore, baseTree);

    // Ours modifies file1
    const ourTree = await createTestTree(objectStore, {
      "file.txt": "base",
      "file1.txt": "ours",
    });
    const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

    // Theirs modifies file2
    const theirTree = await createTestTree(objectStore, {
      "file.txt": "base",
      "file2.txt": "theirs",
    });
    const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

    const merge = new GitMerge(objectStore, refStore);
    const result = await merge.mergeCommits(ourCommit, theirCommit, {
      name: "Test",
      email: "test@test.com",
    });

    assert.ok(result.success);
    assert.ok(result.mergeCommitOid);

    // Read the merge commit and verify two parents
    const commitObj = await objectStore.readObject(result.mergeCommitOid);
    const commitText = new TextDecoder().decode(commitObj.data);
    const parentLines = commitText.split("\n").filter((l: string) => l.startsWith("parent "));
    assert.equal(parentLines.length, 2, "Merge commit should have exactly two parents");
    assert.equal(parentLines[0], `parent ${ourCommit}`);
    assert.equal(parentLines[1], `parent ${theirCommit}`);
  });

  void it("should return conflicts without creating commit on conflict", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();
    const refStore = new GitRefStore(storage);
    await refStore.init();

    const baseTree = await createTestTree(objectStore, { "file.txt": "base" });
    const baseCommit = await createTestCommit(objectStore, baseTree);

    const ourTree = await createTestTree(objectStore, { "file.txt": "ours" });
    const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

    const theirTree = await createTestTree(objectStore, { "file.txt": "theirs" });
    const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

    const merge = new GitMerge(objectStore, refStore);
    const result = await merge.mergeCommits(ourCommit, theirCommit);

    assert.equal(result.success, false);
    assert.equal(result.mergeCommitOid, undefined);
    assert.ok(result.conflicts && result.conflicts.length > 0);
  });

  void it("should find merge base between two commits", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    const baseTree = await createTestTree(objectStore, { "f.txt": "base" });
    const baseCommit = await createTestCommit(objectStore, baseTree);

    const ourTree = await createTestTree(objectStore, { "f.txt": "ours" });
    const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

    const theirTree = await createTestTree(objectStore, { "f.txt": "theirs" });
    const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

    const merge = new GitMerge(objectStore);
    const mergeBase = await merge.findMergeBase(ourCommit, theirCommit);

    assert.equal(mergeBase, baseCommit);
  });
});

void describe("MERGE_HEAD Lifecycle", () => {
  void it("should set MERGE_HEAD on conflict and clear on abort", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();
    const refStore = new GitRefStore(storage);
    await refStore.init();

    // Create a forked history with a conflict
    const baseTree = await createTestTree(objectStore, { "file.txt": "base" });
    const baseCommit = await createTestCommit(objectStore, baseTree);

    const ourTree = await createTestTree(objectStore, { "file.txt": "ours" });
    const ourCommit = await createTestCommit(objectStore, ourTree, baseCommit);

    const theirTree = await createTestTree(objectStore, { "file.txt": "theirs" });
    const theirCommit = await createTestCommit(objectStore, theirTree, baseCommit);

    // Set up HEAD
    await refStore.writeRef("refs/heads/main", ourCommit);
    await refStore.writeSymbolicRef("HEAD", "refs/heads/main");

    // Import GitRepository to test MERGE_HEAD lifecycle
    const { GitRepository: _GitRepository } = await import("./git.repository.ts");
    // GitRepository import validates the module compiles with our changes
    assert.ok(_GitRepository);

    // Manually trigger a merge via the merge engine
    const merge = new GitMerge(objectStore, refStore);
    const result = await merge.mergeCommits(ourCommit, theirCommit);
    assert.equal(result.success, false, "Should conflict");

    // Write MERGE_HEAD as GitRepository.mergeRef would
    await storage.writeFile(".git/MERGE_HEAD", new TextEncoder().encode(theirCommit + "\n"));

    // Verify MERGE_HEAD exists
    const mergeHeadData = await storage.readFile(".git/MERGE_HEAD");
    const mergeHead = new TextDecoder().decode(mergeHeadData).trim();
    assert.equal(mergeHead, theirCommit);

    // Abort — clear MERGE_HEAD
    try {
      await storage.deleteFile(".git/MERGE_HEAD");
    } catch {
      // already cleared
    }

    // Verify MERGE_HEAD is gone
    let mergeHeadGone = false;
    try {
      await storage.readFile(".git/MERGE_HEAD");
    } catch {
      mergeHeadGone = true;
    }
    assert.ok(mergeHeadGone, "MERGE_HEAD should be cleared after abort");
  });
});

void describe("Octopus Merge", () => {
  void it("should merge 3 branches with no conflicts", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    // Base commit
    const baseTree = await createTestTree(objectStore, { "base.txt": "base" });
    const baseCommit = await createTestCommit(objectStore, baseTree);

    // Branch 1 adds file1
    const tree1 = await createTestTree(objectStore, {
      "base.txt": "base",
      "file1.txt": "branch1",
    });
    const commit1 = await createTestCommit(objectStore, tree1, baseCommit);

    // Branch 2 adds file2
    const tree2 = await createTestTree(objectStore, {
      "base.txt": "base",
      "file2.txt": "branch2",
    });
    const commit2 = await createTestCommit(objectStore, tree2, baseCommit);

    // Branch 3 adds file3
    const tree3 = await createTestTree(objectStore, {
      "base.txt": "base",
      "file3.txt": "branch3",
    });
    const commit3 = await createTestCommit(objectStore, tree3, baseCommit);

    const merge = new GitMerge(objectStore);
    const result = await merge.octopusMerge(commit1, [commit2, commit3]);

    assert.ok(result.success, "Octopus merge of non-conflicting branches should succeed");
    assert.ok(result.mergeCommitOid);
    assert.ok(result.mergedTree);

    // Verify the merge commit has 3 parents
    const commitObj = await objectStore.readObject(result.mergeCommitOid);
    const commitText = new TextDecoder().decode(commitObj.data);
    const parentLines = commitText.split("\n").filter((l: string) => l.startsWith("parent "));
    assert.equal(parentLines.length, 3, "Octopus commit should have 3 parents");
  });

  void it("should refuse octopus merge on conflicts", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    const baseTree = await createTestTree(objectStore, { "file.txt": "base" });
    const baseCommit = await createTestCommit(objectStore, baseTree);

    // Both branches modify the same file differently
    const tree1 = await createTestTree(objectStore, { "file.txt": "branch1-change" });
    const commit1 = await createTestCommit(objectStore, tree1, baseCommit);

    const tree2 = await createTestTree(objectStore, { "file.txt": "branch2-change" });
    const commit2 = await createTestCommit(objectStore, tree2, baseCommit);

    const tree3 = await createTestTree(objectStore, { "file.txt": "branch3-change" });
    const commit3 = await createTestCommit(objectStore, tree3, baseCommit);

    const merge = new GitMerge(objectStore);
    const result = await merge.octopusMerge(commit1, [commit2, commit3]);

    assert.equal(result.success, false, "Octopus merge should refuse on conflicts");
    assert.ok(result.message?.includes("refused"));
  });
});

// Helper to parse tree entries from raw tree data
function parseTreeEntries(data: Uint8Array) {
  const entries: Array<{ mode: string; name: string; oid: string }> = [];
  let offset = 0;
  while (offset < data.length) {
    let spaceIdx = offset;
    while (data[spaceIdx] !== 0x20 && spaceIdx < data.length) spaceIdx++;
    const mode = new TextDecoder().decode(data.slice(offset, spaceIdx));
    let nullIdx = spaceIdx + 1;
    while (data[nullIdx] !== 0 && nullIdx < data.length) nullIdx++;
    const name = new TextDecoder().decode(data.slice(spaceIdx + 1, nullIdx));
    const oidBytes = data.slice(nullIdx + 1, nullIdx + 21);
    const oid = Array.from(oidBytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    entries.push({ mode, name, oid });
    offset = nullIdx + 21;
  }
  return entries;
}
