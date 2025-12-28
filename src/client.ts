import type { GitStorage } from "./git.storage.ts";
import { OpfsStorage } from "./client.storage.ts";
import { GitRepository, type GitConfig } from "./git.repository.ts";

export class Client {
	#repository: GitRepository;

	constructor(config: GitConfig, storage: GitStorage = new OpfsStorage()) {
		this.#repository = new GitRepository(storage, config);
	}

	async init() {
		await this.#repository.init();
	}

	async clone(url: string) {
		return this.#repository.clone(url);
	}

	async add(path: string) {
		const content = await this.#repository.readFile(path);
		await this.#repository.add(path, content);
	}

	async mv(oldPath: string, newPath: string) {
		// Get the entry from index
		const entries = this.#repository.getIndexEntries();
		const oldEntry = entries.find((e) => e.path === oldPath);

		if (!oldEntry) {
			throw new Error(`File ${oldPath} not found in index`);
		}

		// Remove old entry
		await this.#repository.removeIndexEntry(oldPath);

		// Add new entry with same OID (preserves history)
		await this.#repository.addIndexEntry({
			path: newPath,
			oid: oldEntry.oid,
			mode: oldEntry.mode,
			size: oldEntry.size,
			mtime: Date.now(),
		});

		// Move file in storage
		const content = await this.#repository.readFile(oldPath);
		await this.#repository.writeFile(newPath, content);
		await this.#repository.deleteFile(oldPath);
	}

	async restore(path: string) {
		// Restore file from index/HEAD
		const headCommitOid = await this.#repository.getCurrentCommitOid();
		if (!headCommitOid) {
			throw new Error("No HEAD commit");
		}

		const commit = await this.#repository.readObject(headCommitOid);
		if (commit.type !== "commit") {
			throw new Error("HEAD is not a commit");
		}

		const commitInfo = this.#repository.parseCommit(commit.data);
		const fileEntry = await this.#repository.findInTree(commitInfo.tree, path);

		if (!fileEntry) {
			throw new Error(`File ${path} not found in HEAD`);
		}

		const blob = await this.#repository.readObject(fileEntry.oid);
		await this.#repository.writeFile(path, blob.data);
	}

	async rm(paths: string | string[], options: { cached?: boolean; recursive?: boolean } = {}) {
		const pathsArray = Array.isArray(paths) ? paths : [paths];

		for (const path of pathsArray) {
			// Check if path is a directory
			const entries = this.#repository.getIndexEntries();
			const matchingEntries = entries.filter((e) =>
				options.recursive ? e.path.startsWith(path + "/") || e.path === path : e.path === path,
			);

			if (matchingEntries.length === 0) {
				throw new Error(`pathspec '${path}' did not match any files`);
			}

			// Remove matching entries
			for (const entry of matchingEntries) {
				await this.#repository.removeIndexEntry(entry.path);

				// Delete from working tree if not --cached
				if (!options.cached) {
					try {
						await this.#repository.deleteFile(entry.path);
					} catch {
						// File might not exist in working tree, that's ok
					}
				}
			}
		}
	}

	async commit(message: string, author?: { name: string; email: string }) {
		return this.#repository.commit(message, author);
	}

	async status() {
		const indexEntries = this.#repository.getIndexEntries();
		const headCommitOid = await this.#repository.getCurrentCommitOid();

		let staged: string[] = [];
		let modified: string[] = [];
		let untracked: string[] = [];

		if (headCommitOid) {
			const commit = await this.#repository.readObject(headCommitOid);
			if (commit.type === "commit") {
				const commitInfo = this.#repository.parseCommit(commit.data);
				const headTree = commitInfo.tree;

				// Get all paths in HEAD tree
				const headPaths = new Set<string>();
				await this.#walkTreePaths(headTree, "", headPaths);

				// Staged: files in index that differ from HEAD
				for (const entry of indexEntries) {
					staged.push(entry.path);
				}

				// Modified: files that exist but differ from index
				// This is simplified - in reality would compare file contents
				modified = [];
			}
		} else {
			// No HEAD - all files in index are staged
			staged = indexEntries.map((e) => e.path);
		}

		return { staged, modified, untracked };
	}

	async #walkTreePaths(treeOid: string, prefix: string, paths: Set<string>): Promise<void> {
		const tree = await this.#repository.readObject(treeOid);
		const entries = this.#repository.parseTree(tree.data);

		for (const entry of entries) {
			const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;

			if (entry.mode === "40000") {
				// Directory - recurse
				await this.#walkTreePaths(entry.oid, fullPath, paths);
			} else {
				// File
				paths.add(fullPath);
			}
		}
	}

	async log() {
		const commits: any[] = [];
		let commitOid = await this.#repository.getCurrentCommitOid();

		while (commitOid) {
			const commit = await this.#repository.readObject(commitOid);
			if (commit.type !== "commit") break;

			const info = this.#repository.parseCommit(commit.data);
			commits.push({
				oid: commitOid,
				author: info.author,
				message: info.message,
			});

			commitOid = info.parent || null;
		}

		return commits;
	}

	async show(ref: string) {
		// Resolve ref to OID
		let oid = ref;

		// Try to read as ref first
		const refOid = await this.#repository.getRef(ref);
		if (refOid) {
			oid = refOid;
		}

		const obj = await this.#repository.readObject(oid);
		return {
			type: obj.type,
			data: obj.data,
		};
	}

	async branch(ref?: string) {
		if (ref) {
			// Create branch
			const headOid = await this.#repository.getCurrentCommitOid();
			if (!headOid) {
				throw new Error("No HEAD commit");
			}
			await this.#repository.writeRef(`refs/heads/${ref}`, headOid);
		} else {
			// List branches
			const refs = await this.#repository.getAllRefs();
			return refs
				.filter((r) => r.name.startsWith("refs/heads/"))
				.map((r) => r.name.replace("refs/heads/", ""));
		}
	}

	async checkout(ref: string) {
		// Resolve ref to commit
		let commitOid = ref;

		const refOid = await this.#repository.getRef(ref);
		if (refOid) {
			commitOid = refOid;
		}

		// Check out the commit
		await this.#repository.checkoutCommit(commitOid);

		// Update HEAD
		await this.#repository.writeFile(
			".git/HEAD",
			new TextEncoder().encode(`ref: refs/heads/${ref}\n`),
		);
	}

	async switch(name: string) {
		// Switch to existing branch
		const branchRef = `refs/heads/${name}`;
		const branchOid = await this.#repository.getRef(branchRef);

		if (!branchOid) {
			throw new Error(`Branch ${name} not found`);
		}

		await this.#repository.checkoutCommit(branchOid);
		await this.#repository.writeFile(".git/HEAD", new TextEncoder().encode(`ref: ${branchRef}\n`));
	}

	async merge(ref: string) {
		// Get current HEAD commit
		const headOid = await this.#repository.getCurrentCommitOid();
		if (!headOid) {
			throw new Error("No HEAD commit");
		}

		// Resolve ref to commit OID
		let mergeOid = ref;
		const refOid = await this.#repository.getRef(ref);
		if (refOid) {
			mergeOid = refOid;
		}

		// Find the common ancestor (lowest common ancestor - LCA)
		const commonAncestorOid = await this.#findCommonAncestor(headOid, mergeOid);
		if (!commonAncestorOid) {
			throw new Error("No common ancestor found");
		}

		// Get the three commits
		const currentCommit = await this.#repository.readObject(headOid);
		const currentInfo = this.#repository.parseCommit(currentCommit.data);

		const mergeCommit = await this.#repository.readObject(mergeOid);
		const mergeInfo = this.#repository.parseCommit(mergeCommit.data);

		const baseCommit = await this.#repository.readObject(commonAncestorOid);
		const baseInfo = this.#repository.parseCommit(baseCommit.data);

		// Perform three-way merge with proper base tree
		const result = await this.#repository.merge(baseInfo.tree, currentInfo.tree, mergeInfo.tree);

		if (!result.success) {
			throw new Error(`Merge conflict: ${result.message || "Unable to merge branches"}`);
		}

		// Create merge commit with both parents
		const authorStr = "Git Client <client@example.com>";
		const timestamp = Math.floor(Date.now() / 1000);
		const timezone = "+0000";

		let mergeCommitData = `tree ${result.mergedTree}\n`;
		mergeCommitData += `parent ${headOid}\n`;
		mergeCommitData += `parent ${mergeOid}\n`;
		mergeCommitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
		mergeCommitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
		mergeCommitData += `\nMerge branch '${ref}' into current branch\n`;

		const mergeCommitOid = await this.#repository.writeObject(
			"commit",
			new TextEncoder().encode(mergeCommitData),
		);

		// Update HEAD to point to merge commit
		const headRef = await this.#repository.getCurrentHead();
		if (headRef) {
			await this.#repository.writeRef(headRef, mergeCommitOid);
		}

		return {
			success: true,
			mergedTree: result.mergedTree,
			mergeCommitOid,
		};
	}

	async #findCommonAncestor(oid1: string, oid2: string): Promise<string | null> {
		// Build history set for first commit
		const history1 = new Set<string>();
		let current: string | null = oid1;

		while (current) {
			history1.add(current);
			const commit = await this.#repository.readObject(current);
			if (commit.type !== "commit") break;
			const info = this.#repository.parseCommit(commit.data);
			current = info.parent || null;
		}

		// Walk second commit's history to find intersection
		current = oid2;
		while (current) {
			if (history1.has(current)) {
				return current; // Found common ancestor
			}

			const commit = await this.#repository.readObject(current);
			if (commit.type !== "commit") break;
			const info = this.#repository.parseCommit(commit.data);
			current = info.parent || null;
		}

		return null; // No common ancestor found
	}

	async rebase(onto: string) {
		// Get current HEAD commit
		const currentHeadOid = await this.#repository.getCurrentCommitOid();
		if (!currentHeadOid) {
			throw new Error("No HEAD commit");
		}

		// Resolve onto ref to commit OID
		let ontoOid = onto;
		const ontoRefOid = await this.#repository.getRef(onto);
		if (ontoRefOid) {
			ontoOid = ontoRefOid;
		}

		// Find the fork point (common ancestor)
		const forkPoint = await this.#findCommonAncestor(currentHeadOid, ontoOid);
		if (!forkPoint) {
			throw new Error("No common ancestor found");
		}

		// If already rebased, do nothing
		if (forkPoint === currentHeadOid) {
			throw new Error("Current branch is already up to date");
		}

		// Collect all commits from current HEAD back to fork point (in chronological order)
		const commitsToReplay: string[] = [];
		let current: string | null = currentHeadOid;

		while (current && current !== forkPoint) {
			commitsToReplay.unshift(current); // prepend to get chronological order
			const commit = await this.#repository.readObject(current);
			if (commit.type !== "commit") break;
			const info = this.#repository.parseCommit(commit.data);
			current = info.parent || null;
		}

		if (commitsToReplay.length === 0) {
			throw new Error("No commits to replay");
		}

		// Start rebasing from the onto commit
		let newParent = ontoOid;

		// Replay each commit
		for (const commitOid of commitsToReplay) {
			const commit = await this.#repository.readObject(commitOid);
			if (commit.type !== "commit") break;

			const commitInfo = this.#repository.parseCommit(commit.data);

			// Get the original parent commit's tree
			if (!commitInfo.parent) {
				// First commit - no parent
				throw new Error("Cannot rebase commit with no parent");
			}

			const oldParentCommit = await this.#repository.readObject(commitInfo.parent);
			if (oldParentCommit.type !== "commit") break;

			const oldParentInfo = this.#repository.parseCommit(oldParentCommit.data);
			const oldParentTree = oldParentInfo.tree;

			// Get the new parent's tree
			const newParentCommit = await this.#repository.readObject(newParent);
			if (newParentCommit.type !== "commit") break;
			const newParentInfo = this.#repository.parseCommit(newParentCommit.data);
			const newParentTree = newParentInfo.tree;

			// Three-way merge: base=oldParentTree, ours=newParentTree, theirs=commitTree
			const result = await this.#repository.merge(oldParentTree, newParentTree, commitInfo.tree);

			if (!result.success || !result.mergedTree) {
				throw new Error(`Rebase conflict on commit ${commitOid}: ${result.message}`);
			}

			// Create new commit with new parent and merged tree
			const authorStr = commitInfo.author;
			const timestamp = Math.floor(Date.now() / 1000);
			const timezone = "+0000";

			let newCommitData = `tree ${result.mergedTree}\n`;
			newCommitData += `parent ${newParent}\n`;
			newCommitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
			newCommitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
			newCommitData += `\n${commitInfo.message}\n`;

			newParent = await this.#repository.writeObject(
				"commit",
				new TextEncoder().encode(newCommitData),
			);
		}

		// Update HEAD to point to the new rebased commit
		const headRef = await this.#repository.getCurrentHead();
		if (headRef) {
			await this.#repository.writeRef(headRef, newParent);
		}

		// Check out the rebased commit
		await this.#repository.checkoutCommit(newParent);

		return {
			success: true,
			newHead: newParent,
			replayed: commitsToReplay.length,
		};
	}

	async reset(hard: boolean, ref: string) {
		// Resolve ref to commit
		let commitOid = ref;
		const refOid = await this.#repository.getRef(ref);
		if (refOid) {
			commitOid = refOid;
		}

		// Check out the commit
		await this.#repository.checkoutCommit(commitOid);

		// If hard reset, also update HEAD
		if (hard) {
			const headRef = await this.#repository.getCurrentHead();
			if (headRef) {
				await this.#repository.writeRef(headRef, commitOid);
			}
		}
	}

	async tag(name: string) {
		// Create tag pointing to current HEAD
		const headOid = await this.#repository.getCurrentCommitOid();
		if (!headOid) {
			throw new Error("No HEAD commit");
		}

		await this.#repository.writeRef(`refs/tags/${name}`, headOid);
	}

	async fetch(remote: string = "origin") {
		await this.#repository.fetch(remote);
	}

	async pull(remote: string = "origin", branch: string = "main") {
		// Fetch from remote
		await this.#repository.fetch(remote);

		// Merge remote tracking branch
		const remoteBranch = `refs/remotes/${remote}/${branch}`;
		const remoteOid = await this.#repository.getRef(remoteBranch);

		if (remoteOid) {
			await this.merge(remoteBranch);
		}
	}

	async push(remote: string = "origin", branch: string = "main", force: boolean = false) {
		// Get local branch OID
		const localBranchRef = `refs/heads/${branch}`;
		const localOid = await this.#repository.getRef(localBranchRef);

		if (!localOid) {
			throw new Error(`Branch ${branch} not found`);
		}

		// Collect all objects to push (walk commit graph)
		const objectsToSend: Set<string> = new Set();
		const commitsToProcess: string[] = [localOid];
		const processedCommits: Set<string> = new Set();

		while (commitsToProcess.length > 0) {
			const commitOid = commitsToProcess.pop();
			if (!commitOid || processedCommits.has(commitOid)) continue;

			processedCommits.add(commitOid);
			objectsToSend.add(commitOid);

			// Get commit object
			const commit = await this.#repository.readObject(commitOid);
			if (commit.type !== "commit") continue;

			const commitInfo = this.#repository.parseCommit(commit.data);

			// Add tree and all nested objects
			await this.#collectTreeObjects(commitInfo.tree, objectsToSend);

			// Add parent commit to process
			if (commitInfo.parent) {
				commitsToProcess.push(commitInfo.parent);
			}
		}

		if (objectsToSend.size === 0) {
			console.log("Everything up to date");
			return {
				success: true,
				pushed: 0,
			};
		}

		// Create pack file
		const packData = await this.#repository.createPack(Array.from(objectsToSend));

		// Get old ref value (for send-pack protocol)
		const remoteTrackingRef = `refs/remotes/${remote}/${branch}`;
		const oldOid = await this.#repository.getRef(remoteTrackingRef);
		const oldValue = oldOid || "0000000000000000000000000000000000000000";

		// Build ref update for send-pack
		const refUpdates = [
			{
				ref: `refs/heads/${branch}`,
				old: oldValue,
				new: localOid,
			},
		];

		// Send pack to remote via send-pack protocol
		const success = await this.#repository.sendPack(refUpdates, packData, force);

		if (success) {
			// Update remote tracking branch locally
			await this.#repository.writeRef(remoteTrackingRef, localOid);

			console.log(`Successfully pushed ${objectsToSend.size} objects to ${remote}/${branch}`);
		}

		return {
			success,
			pushed: objectsToSend.size,
			localOid,
			packSize: packData.length,
		};
	}

	async pushDelete(remote: string = "origin", branch: string): Promise<any> {
		// Delete branch on remote by sending null OID
		const remoteTrackingRef = `refs/remotes/${remote}/${branch}`;
		const oldOid = await this.#repository.getRef(remoteTrackingRef);

		if (!oldOid) {
			throw new Error(`No remote tracking branch ${remoteTrackingRef} found`);
		}

		// Create empty pack (no objects for deletion)
		const packData = await this.#repository.createPack([]);

		// Build ref delete command
		const refUpdates = [
			{
				ref: `refs/heads/${branch}`,
				old: oldOid,
				new: "0000000000000000000000000000000000000000",
			},
		];

		// Send delete to remote
		const success = await this.#repository.sendPack(refUpdates, packData, false);

		if (success) {
			// Delete local tracking branch
			await this.#repository.deleteRef(remoteTrackingRef);
			console.log(`Successfully deleted ${remote}/${branch}`);
		}

		return {
			success,
			deleted: branch,
		};
	}

	async #collectTreeObjects(treeOid: string, objects: Set<string>): Promise<void> {
		if (objects.has(treeOid)) return;

		objects.add(treeOid);

		const tree = await this.#repository.readObject(treeOid);
		const entries = this.#repository.parseTree(tree.data);

		for (const entry of entries) {
			if (entry.mode === "40000") {
				// Directory - recurse
				await this.#collectTreeObjects(entry.oid, objects);
			} else {
				// File (blob)
				objects.add(entry.oid);
			}
		}
	}

	async remote(action: "add" | "remove" | "set-url", name: string, url?: string) {
		const configPath = ".git/config";
		let config: Record<string, Record<string, string>> = {};

		// Read existing config
		try {
			const configData = await this.#repository.readFile(configPath);
			const configText = new TextDecoder().decode(configData);
			config = this.#parseConfig(configText);
		} catch {
			// Config doesn't exist yet, start with empty
			config = {};
		}

		// Initialize remotes section if it doesn't exist
		if (!config["remote"]) {
			config["remote"] = {};
		}

		// Perform action
		if (action === "add") {
			if (!url) {
				throw new Error("URL required for adding remote");
			}
			config["remote"][name] = url;
		} else if (action === "remove") {
			delete config["remote"][name];
		} else if (action === "set-url") {
			if (!url) {
				throw new Error("URL required for setting remote URL");
			}
			if (!config["remote"][name]) {
				throw new Error(`Remote '${name}' not found`);
			}
			config["remote"][name] = url;
		}

		// Write config back
		const configText = this.#serializeConfig(config);
		await this.#repository.writeFile(configPath, new TextEncoder().encode(configText));
	}

	async getAllRemotes(): Promise<Record<string, string>> {
		const configPath = ".git/config";
		let config: Record<string, Record<string, string>> = {};

		try {
			const configData = await this.#repository.readFile(configPath);
			const configText = new TextDecoder().decode(configData);
			config = this.#parseConfig(configText);
		} catch {
			return {};
		}

		return config["remote"] || {};
	}

	async getRemote(name: string): Promise<string | null> {
		const remotes = await this.getAllRemotes();
		return remotes[name] || null;
	}

	#parseConfig(text: string): Record<string, Record<string, string>> {
		const config: Record<string, Record<string, string>> = {};
		let currentSection = "";

		for (const line of text.split("\n")) {
			const trimmed = line.trim();
			if (!trimmed || trimmed.startsWith("#")) continue;

			// Section header like [remote]
			if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
				currentSection = trimmed.slice(1, -1);
				if (!config[currentSection]) {
					config[currentSection] = {};
				}
			} else if (currentSection && trimmed.includes("=")) {
				const [key, ...valueParts] = trimmed.split("=");
				if (key !== undefined) {
					const value = valueParts.join("=").trim();
					const section = config[currentSection];
					if (section !== undefined) {
						section[key.trim()] = value;
					}
				}
			}
		}

		return config;
	}

	#serializeConfig(config: Record<string, Record<string, string>>): string {
		let text = "";

		for (const [section, values] of Object.entries(config)) {
			if (Object.keys(values).length === 0) continue;

			text += `[${section}]\n`;
			for (const [key, value] of Object.entries(values)) {
				text += `\t${key} = ${value}\n`;
			}
		}

		return text;
	}
}
