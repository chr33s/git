import { GitObjectStore } from "./git.object.ts";
import { GitRefStore } from "./git.ref.ts";
import { bytesToHex, hexToBytes } from "./git.utils.ts";

export interface MergeResult {
	success: boolean;
	mergedTree?: string;
	conflicts?: ConflictEntry[];
	message?: string;
}

export interface ConflictEntry {
	path: string;
	base?: string;
	ours?: string;
	theirs?: string;
	resolved?: boolean;
	resolution?: string;
}

export interface TreeEntry {
	mode: string;
	name: string;
	oid: string;
}

export class GitMerge {
	#objectStore: GitObjectStore;
	#refStore?: GitRefStore;

	constructor(objectStore: GitObjectStore, refStore?: GitRefStore) {
		this.#objectStore = objectStore;
		this.#refStore = refStore;
	}

	async threeWayMerge(
		baseCommit: string,
		ourCommit: string,
		theirCommit: string,
		strategy: "recursive" | "resolve" | "ours" | "theirs" | "octopus" = "recursive",
	): Promise<MergeResult> {
		// Get tree OIDs from commits
		const baseTree = await this.#getTreeFromCommit(baseCommit);
		const ourTree = await this.#getTreeFromCommit(ourCommit);
		const theirTree = await this.#getTreeFromCommit(theirCommit);

		switch (strategy) {
			case "recursive":
				return await this.#recursiveMerge(baseTree, ourTree, theirTree);
			case "resolve":
				return await this.#resolveMerge(baseTree, ourTree, theirTree);
			case "ours":
				return await this.#oursMerge(ourTree);
			case "theirs":
				return await this.#theirsMerge(theirTree);
			case "octopus":
				return await this.#octopusMerge([baseTree, ourTree, theirTree]);
			default:
				throw new Error(`Unknown merge strategy: ${String(strategy)}`);
		}
	}

	async #recursiveMerge(
		baseTree: string,
		ourTree: string,
		theirTree: string,
	): Promise<MergeResult> {
		const conflicts: ConflictEntry[] = [];
		const mergedEntries: TreeEntry[] = [];

		// Get all unique file paths
		const allPaths = await this.#getAllPaths(baseTree, ourTree, theirTree);

		for (const path of allPaths) {
			const baseEntry = await this.#getTreeEntry(baseTree, path);
			const ourEntry = await this.#getTreeEntry(ourTree, path);
			const theirEntry = await this.#getTreeEntry(theirTree, path);

			const mergeResult = await this.#mergeEntry(path, baseEntry, ourEntry, theirEntry);

			if (mergeResult.conflict) {
				conflicts.push(mergeResult.conflict);
			} else if (mergeResult.entry) {
				mergedEntries.push(mergeResult.entry);
			}
		}

		if (conflicts.length > 0) {
			return {
				success: false,
				conflicts,
				message: `Merge failed with ${conflicts.length} conflicts`,
			};
		}

		// Create merged tree
		const mergedTree = await this.#createTree(mergedEntries);

		return {
			success: true,
			mergedTree,
			message: "Merge successful",
		};
	}

	async #resolveMerge(baseTree: string, ourTree: string, theirTree: string): Promise<MergeResult> {
		const conflicts: ConflictEntry[] = [];
		const mergedEntries: TreeEntry[] = [];

		const allPaths = await this.#getAllPaths(baseTree, ourTree, theirTree);

		for (const path of allPaths) {
			const baseEntry = await this.#getTreeEntry(baseTree, path);
			const ourEntry = await this.#getTreeEntry(ourTree, path);
			const theirEntry = await this.#getTreeEntry(theirTree, path);

			// Simple resolution: if both modified differently, conflict
			if (ourEntry && theirEntry) {
				if (ourEntry.oid === theirEntry.oid) {
					mergedEntries.push(ourEntry);
				} else if (
					!baseEntry ||
					(ourEntry.oid !== baseEntry.oid && theirEntry.oid !== baseEntry.oid)
				) {
					conflicts.push({
						path,
						base: baseEntry?.oid,
						ours: ourEntry.oid,
						theirs: theirEntry.oid,
					});
				} else if (ourEntry.oid === baseEntry.oid) {
					mergedEntries.push(theirEntry);
				} else {
					mergedEntries.push(ourEntry);
				}
			} else if (ourEntry) {
				mergedEntries.push(ourEntry);
			} else if (theirEntry) {
				mergedEntries.push(theirEntry);
			}
		}

		if (conflicts.length > 0) {
			return {
				success: false,
				conflicts,
				message: `Merge failed with ${conflicts.length} conflicts`,
			};
		}

		const mergedTree = await this.#createTree(mergedEntries);

		return {
			success: true,
			mergedTree,
			message: "Merge successful",
		};
	}

	async #oursMerge(ourTree: string): Promise<MergeResult> {
		return {
			success: true,
			mergedTree: ourTree,
			message: "Merge successful (ours strategy)",
		};
	}

	async #theirsMerge(theirTree: string): Promise<MergeResult> {
		return {
			success: true,
			mergedTree: theirTree,
			message: "Merge successful (theirs strategy)",
		};
	}

	async #octopusMerge(trees: string[]): Promise<MergeResult> {
		if (trees.length < 3) {
			throw new Error("Octopus merge requires at least 3 trees");
		}

		const baseTree = trees[0];
		if (!baseTree) {
			throw new Error("Base tree is required for octopus merge");
		}

		let currentTree = baseTree;

		for (let i = 1; i < trees.length; i++) {
			const targetTree = trees[i];
			if (!targetTree) {
				throw new Error(`Tree at index ${i} is undefined`);
			}

			const result = await this.#recursiveMerge(
				baseTree, // Always use first as base
				currentTree,
				targetTree,
			);

			if (!result.success) {
				return {
					success: false,
					conflicts: result.conflicts,
					message: `Octopus merge failed at branch ${i}`,
				};
			}

			currentTree = result.mergedTree!;
		}

		return {
			success: true,
			mergedTree: currentTree,
			message: `Octopus merge successful (${trees.length} branches)`,
		};
	}

	async #mergeEntry(
		path: string,
		baseEntry: TreeEntry | null,
		ourEntry: TreeEntry | null,
		theirEntry: TreeEntry | null,
	): Promise<{ entry?: TreeEntry; conflict?: ConflictEntry }> {
		// Both deleted
		if (!ourEntry && !theirEntry) {
			return {};
		}

		// Only in ours
		if (ourEntry && !theirEntry) {
			if (!baseEntry || ourEntry.oid !== baseEntry.oid) {
				return { entry: ourEntry };
			}
			return {};
		}

		// Only in theirs
		if (!ourEntry && theirEntry) {
			if (!baseEntry || theirEntry.oid !== baseEntry.oid) {
				return { entry: theirEntry };
			}
			return {};
		}

		// Both have the file
		if (ourEntry && theirEntry) {
			// Same content
			if (ourEntry.oid === theirEntry.oid) {
				return { entry: ourEntry };
			}

			// Try content merge for text files
			if (await this.#isTextFile(ourEntry.oid)) {
				const mergedContent = await this.#mergeTextContent(
					baseEntry?.oid,
					ourEntry.oid,
					theirEntry.oid,
				);

				if (mergedContent.success) {
					const mergedOid = await this.#objectStore.writeObject("blob", mergedContent.content!);
					return {
						entry: {
							mode: ourEntry.mode,
							name: ourEntry.name,
							oid: mergedOid,
						},
					};
				} else {
					return {
						conflict: {
							path,
							base: baseEntry?.oid,
							ours: ourEntry.oid,
							theirs: theirEntry.oid,
						},
					};
				}
			}

			// Binary conflict
			return {
				conflict: {
					path,
					base: baseEntry?.oid,
					ours: ourEntry.oid,
					theirs: theirEntry.oid,
				},
			};
		}

		return {};
	}

	async #mergeTextContent(
		baseOid: string | undefined,
		ourOid: string,
		theirOid: string,
	): Promise<{ success: boolean; content?: Uint8Array; conflicts?: string[] }> {
		const ourContent = await this.#readBlobAsText(ourOid);
		const theirContent = await this.#readBlobAsText(theirOid);
		const baseContent = baseOid ? await this.#readBlobAsText(baseOid) : "";

		const ourLines = ourContent.split("\n");
		const theirLines = theirContent.split("\n");
		const baseLines = baseContent.split("\n");

		// Perform line-by-line three-way merge
		const mergedLines: string[] = [];
		const conflicts: string[] = [];

		const maxLength = Math.max(ourLines.length, theirLines.length, baseLines.length);

		for (let i = 0; i < maxLength; i++) {
			const baseLine = baseLines[i] || "";
			const ourLine = ourLines[i] || "";
			const theirLine = theirLines[i] || "";

			if (ourLine === theirLine) {
				mergedLines.push(ourLine);
			} else if (ourLine === baseLine) {
				mergedLines.push(theirLine);
			} else if (theirLine === baseLine) {
				mergedLines.push(ourLine);
			} else {
				// Conflict
				conflicts.push(`Line ${i + 1}`);
				mergedLines.push("<<<<<<< ours");
				mergedLines.push(ourLine);
				mergedLines.push("=======");
				mergedLines.push(theirLine);
				mergedLines.push(">>>>>>> theirs");
			}
		}

		if (conflicts.length > 0) {
			return {
				success: false,
				conflicts,
			};
		}

		return {
			success: true,
			content: new TextEncoder().encode(mergedLines.join("\n")),
		};
	}

	async detectRenames(
		oldTree: string,
		newTree: string,
		threshold: number = 0.5,
	): Promise<Array<{ oldPath: string; newPath: string; similarity: number }>> {
		const renames: Array<{
			oldPath: string;
			newPath: string;
			similarity: number;
		}> = [];

		const oldPaths = await this.#getTreePaths(oldTree);
		const newPaths = await this.#getTreePaths(newTree);

		const deleted = oldPaths.filter((p) => !newPaths.includes(p));
		const added = newPaths.filter((p) => !oldPaths.includes(p));

		for (const deletedPath of deleted) {
			const deletedEntry = await this.#getTreeEntry(oldTree, deletedPath);
			if (!deletedEntry) continue;

			const deletedContent = await this.#readBlob(deletedEntry.oid);

			for (const addedPath of added) {
				const addedEntry = await this.#getTreeEntry(newTree, addedPath);
				if (!addedEntry) continue;

				const addedContent = await this.#readBlob(addedEntry.oid);

				const similarity = this.#calculateSimilarity(deletedContent, addedContent);

				if (similarity >= threshold) {
					renames.push({
						oldPath: deletedPath,
						newPath: addedPath,
						similarity,
					});
				}
			}
		}

		// Sort by similarity and remove duplicates
		renames.sort((a, b) => b.similarity - a.similarity);

		const seen = new Set<string>();
		return renames.filter((r) => {
			if (seen.has(r.oldPath) || seen.has(r.newPath)) {
				return false;
			}
			seen.add(r.oldPath);
			seen.add(r.newPath);
			return true;
		});
	}

	#calculateSimilarity(content1: Uint8Array, content2: Uint8Array): number {
		if (content1.length === 0 || content2.length === 0) {
			return 0;
		}

		// Simple similarity based on common substrings
		const str1 = new TextDecoder().decode(content1);
		const str2 = new TextDecoder().decode(content2);

		const lines1 = new Set(str1.split("\n"));
		const lines2 = new Set(str2.split("\n"));

		const intersection = new Set([...lines1].filter((x) => lines2.has(x)));
		const union = new Set([...lines1, ...lines2]);

		return intersection.size / union.size;
	}

	async cherryPick(commitOid: string, targetBranch: string): Promise<MergeResult> {
		const commit = await this.#objectStore.readObject(commitOid);
		const commitData = this.#parseCommit(commit.data);

		if (!commitData.parent) {
			throw new Error("Cannot cherry-pick a root commit");
		}

		const parentTree = await this.#getTreeFromCommit(commitData.parent);
		const commitTree = commitData.tree;
		const targetTree = await this.#getTreeFromBranch(targetBranch);

		// Apply the changes from parent->commit onto target
		return await this.#recursiveMerge(parentTree, targetTree, commitTree);
	}

	async rebase(
		sourceBranch: string,
		targetBranch: string,
		_interactive: boolean = false,
	): Promise<{
		success: boolean;
		commits: string[];
		conflicts?: ConflictEntry[];
	}> {
		const sourceCommits = await this.#getCommitList(sourceBranch);
		const targetHead = await this.#getBranchHead(targetBranch);

		const rebasedCommits: string[] = [];
		let currentBase = targetHead;

		for (const commit of sourceCommits) {
			const result = await this.cherryPick(commit, currentBase);

			if (!result.success) {
				return {
					success: false,
					commits: rebasedCommits,
					conflicts: result.conflicts,
				};
			}

			rebasedCommits.push(result.mergedTree!);
			currentBase = result.mergedTree!;
		}

		return {
			success: true,
			commits: rebasedCommits,
		};
	}

	async #getTreeFromCommit(commitOid: string): Promise<string> {
		const commit = await this.#objectStore.readObject(commitOid);
		return this.#parseCommit(commit.data).tree;
	}

	async #getTreeFromBranch(branch: string): Promise<string> {
		// Read branch ref and get tree from commit
		const commitOid = await this.#getBranchHead(branch);
		return await this.#getTreeFromCommit(commitOid);
	}

	async #getBranchHead(branch: string): Promise<string> {
		// Read branch reference from .git/refs/heads/{branch} or packed-refs file
		return await this.#readBranchRef(branch);
	}

	async #readBranchRef(branch: string): Promise<string> {
		if (!this.#refStore) {
			throw new Error("GitRefStore is required for branch operations");
		}

		try {
			// Try to read the branch reference
			const refName = branch.startsWith("refs/") ? branch : `refs/heads/${branch}`;
			const oid = await this.#refStore.readRef(refName);

			if (!oid) {
				// Check if it's HEAD reference
				if (branch === "HEAD") {
					const headOid = await this.#refStore.readRef("HEAD");
					if (!headOid) {
						throw new Error("HEAD reference not found");
					}

					// If HEAD contains a ref, resolve it
					if (headOid.startsWith("ref: ")) {
						const targetRef = headOid.slice(5).trim();
						const targetOid = await this.#refStore.readRef(targetRef);
						if (!targetOid) {
							throw new Error(`Target reference ${targetRef} not found`);
						}
						return targetOid;
					}

					return headOid;
				}

				throw new Error(`Branch reference ${refName} not found`);
			}

			return oid;
		} catch (error) {
			throw new Error(
				`Failed to read branch ${branch}: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}

	async #getCommitList(branch: string): Promise<string[]> {
		// Walk commit history starting from branch head
		const commits: string[] = [];
		let currentCommit = await this.#getBranchHead(branch);

		try {
			while (currentCommit) {
				commits.push(currentCommit);

				// Get parent commit
				const commit = await this.#objectStore.readObject(currentCommit);
				const parsed = this.#parseCommit(commit.data);

				// Move to parent (first parent in case of merge)
				currentCommit = parsed.parent || "";
			}
		} catch (error) {
			// Stop walking on error (e.g., commit not found)
			console.warn(
				`Error walking commit history: ${error instanceof Error ? error.message : String(error)}`,
			);
		}

		return commits;
	}

	async #getAllPaths(...trees: string[]): Promise<Set<string>> {
		const paths = new Set<string>();

		for (const tree of trees) {
			const treePaths = await this.#getTreePaths(tree);
			treePaths.forEach((p) => paths.add(p));
		}

		return paths;
	}

	async #getTreePaths(treeOid: string): Promise<string[]> {
		const paths: string[] = [];
		await this.#walkTree(treeOid, "", paths);
		return paths;
	}

	async #walkTree(treeOid: string, prefix: string, paths: string[]) {
		const tree = await this.#objectStore.readObject(treeOid);
		const entries = this.#parseTree(tree.data);

		for (const entry of entries) {
			const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;

			if (entry.mode === "40000") {
				await this.#walkTree(entry.oid, fullPath, paths);
			} else {
				paths.push(fullPath);
			}
		}
	}

	async #getTreeEntry(treeOid: string, path: string): Promise<TreeEntry | null> {
		const parts = path.split("/");
		let currentTree = treeOid;

		for (let i = 0; i < parts.length; i++) {
			const tree = await this.#objectStore.readObject(currentTree);
			const entries = this.#parseTree(tree.data);
			const entry = entries.find((e) => e.name === parts[i]);

			if (!entry) return null;

			if (i === parts.length - 1) {
				return entry;
			}

			currentTree = entry.oid;
		}

		return null;
	}

	async #createTree(entries: TreeEntry[]): Promise<string> {
		// Sort entries
		entries.sort((a, b) => a.name.localeCompare(b.name));

		// Build tree data
		let treeData = new Uint8Array(0);

		for (const entry of entries) {
			const mode = entry.mode.padStart(6, "0");
			const entryData = new TextEncoder().encode(`${mode} ${entry.name}\0`);
			const oidBytes = hexToBytes(entry.oid);

			const combined = new Uint8Array(entryData.length + oidBytes.length);
			combined.set(entryData);
			combined.set(oidBytes, entryData.length);

			const newTreeData = new Uint8Array(treeData.length + combined.length);
			newTreeData.set(treeData);
			newTreeData.set(combined, treeData.length);
			treeData = newTreeData;
		}

		return await this.#objectStore.writeObject("tree", treeData);
	}

	async #isTextFile(oid: string): Promise<boolean> {
		const blob = await this.#objectStore.readObject(oid);

		// Check for null bytes (binary indicator)
		for (let i = 0; i < Math.min(blob.data.length, 8192); i++) {
			if (blob.data[i] === 0) {
				return false;
			}
		}

		return true;
	}

	async #readBlob(oid: string): Promise<Uint8Array> {
		const blob = await this.#objectStore.readObject(oid);
		return blob.data;
	}

	async #readBlobAsText(oid: string): Promise<string> {
		const data = await this.#readBlob(oid);
		return new TextDecoder().decode(data);
	}

	#parseCommit(data: Uint8Array): {
		tree: string;
		parent?: string;
		author: string;
		message: string;
	} {
		const text = new TextDecoder().decode(data);
		const lines = text.split("\n");

		const tree = lines.find((l) => l.startsWith("tree "))?.slice(5) || "";
		const parent = lines.find((l) => l.startsWith("parent "))?.slice(7);
		const author = lines.find((l) => l.startsWith("author "))?.slice(7) || "";

		const messageStart = lines.findIndex((l) => l === "") + 1;
		const message = lines.slice(messageStart).join("\n");

		return { tree, parent, author, message };
	}

	#parseTree(data: Uint8Array): TreeEntry[] {
		const entries: TreeEntry[] = [];
		let offset = 0;

		while (offset < data.length) {
			// Find space
			let spaceIdx = offset;
			while (data[spaceIdx] !== 0x20 && spaceIdx < data.length) spaceIdx++;

			const mode = new TextDecoder().decode(data.slice(offset, spaceIdx));

			// Find null
			let nullIdx = spaceIdx + 1;
			while (data[nullIdx] !== 0 && nullIdx < data.length) nullIdx++;

			const name = new TextDecoder().decode(data.slice(spaceIdx + 1, nullIdx));

			// Read 20 bytes for SHA1
			const oid = bytesToHex(data.slice(nullIdx + 1, nullIdx + 21));

			entries.push({ mode, name, oid });
			offset = nullIdx + 21;
		}

		return entries;
	}
}

export class ConflictResolver {
	#conflicts: Map<string, ConflictEntry> = new Map();

	addConflict(conflict: ConflictEntry) {
		this.#conflicts.set(conflict.path, conflict);
	}

	resolveConflict(path: string, resolution: "ours" | "theirs" | "manual", manualContent?: string) {
		const conflict = this.#conflicts.get(path);
		if (!conflict) {
			throw new Error(`No conflict found for path: ${path}`);
		}

		conflict.resolved = true;

		switch (resolution) {
			case "ours":
				conflict.resolution = conflict.ours;
				break;
			case "theirs":
				conflict.resolution = conflict.theirs;
				break;
			case "manual":
				if (!manualContent) {
					throw new Error("Manual resolution requires content");
				}
				conflict.resolution = manualContent;
				break;
		}
	}

	getUnresolvedConflicts(): ConflictEntry[] {
		return Array.from(this.#conflicts.values()).filter((c) => !c.resolved);
	}

	getAllConflicts(): ConflictEntry[] {
		return Array.from(this.#conflicts.values());
	}

	isAllResolved(): boolean {
		return Array.from(this.#conflicts.values()).every((c) => c.resolved);
	}

	clear() {
		this.#conflicts.clear();
	}
}
