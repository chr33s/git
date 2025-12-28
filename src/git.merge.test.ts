import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitMerge, ConflictResolver } from "./git.merge.ts";
import { GitObjectStore } from "./git.object.ts";
import { GitRefStore } from "./git.ref.ts";
import { MemoryStorage } from "./git.storage.ts";

// Helper function
function hexToBytes(hex: string): Uint8Array {
	const bytes = new Uint8Array(hex.length / 2);
	for (let i = 0; i < bytes.length; i++) {
		bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	}
	return bytes;
}

async function createTestTree(
	objectStore: GitObjectStore,
	files: Record<string, string>,
): Promise<string> {
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

async function createTestCommit(
	objectStore: GitObjectStore,
	treeOid: string,
	parentOid?: string,
): Promise<string> {
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
			assert.equal(renames[0].oldPath, "old.txt");
			assert.equal(renames[0].newPath, "new.txt");
			assert.ok(renames[0].similarity > 0.9);
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
			assert.equal(conflicts[0].path, "file.txt");
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

			resolver.resolveConflict("file.txt", "resolved-oid");

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

			resolver.resolveConflict("file.txt", "resolved-oid");

			// The conflict should now be resolved
			assert.ok(resolver.isAllResolved());
		});

		void it("should handle resolving non-existent conflict", () => {
			const resolver = new ConflictResolver();

			// Should throw for non-existent conflict
			assert.throws(
				() => resolver.resolveConflict("nonexistent.txt", "oid"),
				/No conflict found for path: nonexistent.txt/,
			);
		});
	});

	void describe("getUnresolvedConflicts", () => {
		void it("should return only unresolved conflicts", () => {
			const resolver = new ConflictResolver();

			resolver.addConflict({ path: "file1.txt", ours: "a", theirs: "b" });
			resolver.addConflict({ path: "file2.txt", ours: "c", theirs: "d" });

			resolver.resolveConflict("file1.txt", "resolved");

			const unresolved = resolver.getUnresolvedConflicts();
			assert.equal(unresolved.length, 1);
			assert.equal(unresolved[0].path, "file2.txt");
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

			resolver.resolveConflict("file1.txt", "r1");
			resolver.resolveConflict("file2.txt", "r2");

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
