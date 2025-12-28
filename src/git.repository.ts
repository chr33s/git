import { GitIndex } from "./git.index.ts";
import { GitObjectStore } from "./git.object.ts";
import { GitPackParser, GitPackWriter } from "./git.pack.ts";
import { GitProtocol } from "./git.protocol.ts";
import { GitRefStore } from "./git.ref.ts";
import { GitMerge, type MergeResult } from "./git.merge.ts";
import type { GitStorage } from "./git.storage.ts";
import { hexToBytes, bytesToHex } from "./git.utils.ts";

export interface GitConfig {
	repoName: string;
	remote?: string;
	branch?: string;
}

export interface GitRepositoryStatus {
	staged: string[];
	modified: string[];
	untracked: string[];
}

export interface GitAuthor {
	name: string;
	email: string;
}

export interface GitCommitInfo {
	tree: string;
	parent?: string;
	author: string;
	message: string;
}

export interface GitTreeEntry {
	mode: string;
	name: string;
	oid: string;
}

export interface GitRepoInfo {
	host: string;
	repo: string;
	protocol: string;
}

export class GitRepository {
	protected storage: GitStorage;
	protected objectStore: GitObjectStore;
	protected refStore: GitRefStore;
	protected protocol: GitProtocol;
	protected index: GitIndex;
	protected config: GitConfig;

	constructor(storage: GitStorage, config: GitConfig) {
		this.storage = storage;
		this.config = config;
		this.objectStore = new GitObjectStore(storage);
		this.refStore = new GitRefStore(storage);
		this.protocol = new GitProtocol();
		this.index = new GitIndex(storage);
	}

	async init() {
		// Initialize storage
		await this.storage.init(this.config.repoName);

		// Initialize git components
		await this.objectStore.init();
		await this.refStore.init();
		await this.index.init();

		// Create initial git directory structure
		await this.storage.createDirectory(".git/hooks");
		await this.storage.createDirectory(".git/info");
		await this.storage.createDirectory(".git/objects/info");
		await this.storage.createDirectory(".git/objects/pack");

		// Initialize HEAD
		const initialBranch = this.config.branch || "main";
		await this.storage.writeFile(
			".git/HEAD",
			new TextEncoder().encode(`ref: refs/heads/${initialBranch}\n`),
		);
	}

	async clone(url: string) {
		console.log(`Cloning from ${url}...`);

		// Parse repository URL
		const repoInfo = this.parseGitUrl(url);

		// Discover refs
		const refs = await this.protocol.discoverRefs(repoInfo);
		console.log("Discovered refs:", refs);

		// Get default branch
		const headRef = refs.find((r) => r.name === "HEAD");
		const defaultBranch = headRef?.target || "refs/heads/main";
		const headCommit = refs.find((r) => r.name === defaultBranch)?.oid;

		if (!headCommit) {
			throw new Error("Could not find HEAD commit");
		}

		// Negotiate and fetch pack
		const packStream = await this.protocol.fetchPack(repoInfo, [headCommit], []);

		// Parse and store pack
		const parser = new GitPackParser(this.objectStore);
		await parser.parsePack(packStream);

		// Update refs
		for (const ref of refs) {
			if (ref.oid) {
				await this.refStore.writeRef(ref.name, ref.oid);
			}
		}

		return { headCommit, defaultBranch };
	}

	async fetch(remote: string = "origin") {
		if (!this.config.remote) {
			throw new Error("No remote configured");
		}

		const repoInfo = this.parseGitUrl(this.config.remote);

		// Get local refs
		const localRefs = await this.refStore.getAllRefs();

		// Discover remote refs
		const remoteRefs = await this.protocol.discoverRefs(repoInfo);

		// Determine what we need to fetch
		const wants: string[] = [];
		const haves: string[] = [];

		for (const ref of remoteRefs) {
			if (ref.oid && !ref.name.includes("refs/tags/")) {
				const localRef = localRefs.find((l) => l.name === ref.name);
				if (!localRef || localRef.oid !== ref.oid) {
					wants.push(ref.oid);
				}
				if (localRef) {
					haves.push(localRef.oid);
				}
			}
		}

		if (wants.length === 0) {
			console.log("Already up to date");
			return;
		}

		// Fetch pack
		const packStream = await this.protocol.fetchPack(repoInfo, wants, haves);

		// Parse and store pack
		const parser = new GitPackParser(this.objectStore);
		await parser.parsePack(packStream);

		// Update remote refs
		for (const ref of remoteRefs) {
			if (ref.oid) {
				const remoteName = `refs/remotes/${remote}/${ref.name.replace("refs/heads/", "")}`;
				await this.refStore.writeRef(remoteName, ref.oid);
			}
		}
	}

	async checkoutCommit(commitOid: string) {
		// Read commit object
		const commit = await this.objectStore.readObject(commitOid);
		if (commit.type !== "commit") {
			throw new Error("Not a commit");
		}

		// Parse commit to get tree
		const treeOid = this.parseCommit(commit.data).tree;

		// Update index
		await this.index.updateFromTree(treeOid, this.objectStore);

		return treeOid;
	}

	async add(path: string, content: Uint8Array) {
		// Create blob object
		const oid = await this.objectStore.writeObject("blob", content);

		// Update index
		await this.index.addEntry({
			path,
			oid,
			mode: "100644",
			size: content.byteLength,
			mtime: Date.now(),
		});
	}

	async commit(message: string, author?: GitAuthor): Promise<string> {
		// Get current HEAD
		const headRef = await this.getCurrentHead();
		let parentOid: string | undefined;

		if (headRef) {
			parentOid = (await this.refStore.readRef(headRef)) || undefined;
		}

		// Create tree from index
		const treeOid = await this.createTreeFromIndex();

		// Create commit object
		const authorStr = author
			? `${author.name} <${author.email}>`
			: "Anonymous <anonymous@example.com>";

		const timestamp = Math.floor(Date.now() / 1000);
		const timezone = "+0000";

		let commitData = `tree ${treeOid}\n`;
		if (parentOid) {
			commitData += `parent ${parentOid}\n`;
		}
		commitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
		commitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
		commitData += `\n${message}\n`;

		const commitOid = await this.objectStore.writeObject(
			"commit",
			new TextEncoder().encode(commitData),
		);

		// Update HEAD
		if (headRef) {
			await this.refStore.writeRef(headRef, commitOid);
		}

		return commitOid;
	}

	async createTreeFromIndex(): Promise<string> {
		const entries = this.index.getEntries();

		// Group entries by directory
		const tree = new Map<string, any[]>();

		for (const entry of entries) {
			const parts = entry.path.split("/");
			const name = parts.pop()!;
			const dir = parts.join("/") || ".";

			if (!tree.has(dir)) {
				tree.set(dir, []);
			}

			tree.get(dir)!.push({
				name,
				mode: entry.mode,
				oid: entry.oid,
			});
		}

		// Create tree objects recursively
		return await this.createTreeObject(tree, ".");
	}

	async createTreeObject(tree: Map<string, any[]>, path: string): Promise<string> {
		const entries = tree.get(path) || [];

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

		return await this.objectStore.writeObject("tree", treeData);
	}

	parseGitUrl(url: string): GitRepoInfo {
		const match = url.match(/^(https?:\/\/|git@)([^:/]+)[:\\/]([^/]+)\/(.+?)(\.git)?$/);
		if (!match || match.length < 5) {
			throw new Error("Invalid git URL");
		}

		return {
			protocol: match[1]!.includes("http") ? "http" : "ssh",
			host: match[2]!,
			repo: match[4]!,
		};
	}

	parseCommit(data: Uint8Array): GitCommitInfo {
		const text = new TextDecoder().decode(data);
		const lines = text.split("\n");

		const tree = lines.find((l) => l.startsWith("tree "))?.slice(5) || "";
		const parent = lines.find((l) => l.startsWith("parent "))?.slice(7);
		const author = lines.find((l) => l.startsWith("author "))?.slice(7) || "";

		const messageStart = lines.findIndex((l) => l === "") + 1;
		const message = lines.slice(messageStart).join("\n");

		return { tree, parent, author, message };
	}

	parseTree(data: Uint8Array): GitTreeEntry[] {
		const entries: GitTreeEntry[] = [];
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

	async findInTree(treeOid: string, path: string): Promise<{ oid: string; mode: string } | null> {
		const parts = path.split("/");
		let currentTree = treeOid;

		for (let i = 0; i < parts.length; i++) {
			const tree = await this.objectStore.readObject(currentTree);
			const entries = this.parseTree(tree.data);
			const entry = entries.find((e) => e.name === parts[i]);

			if (!entry) return null;

			if (i === parts.length - 1) {
				return { oid: entry.oid, mode: entry.mode };
			}

			currentTree = entry.oid;
		}

		return null;
	}

	async getCurrentHead(): Promise<string | null> {
		try {
			const headContent = await this.storage.readFile(".git/HEAD");
			const content = new TextDecoder().decode(headContent);

			if (content.startsWith("ref: ")) {
				return content.slice(5).trim();
			}

			return null;
		} catch {
			return null;
		}
	}

	async getCurrentCommitOid(): Promise<string | null> {
		const headRef = await this.getCurrentHead();
		if (headRef) {
			return await this.refStore.readRef(headRef);
		}
		return null;
	}

	async hashObject(type: string, data: Uint8Array): Promise<string> {
		const header = new TextEncoder().encode(`${type} ${data.length}\0`);
		const combined = new Uint8Array(header.length + data.length);
		combined.set(header);
		combined.set(data, header.length);

		const hash = await crypto.subtle.digest("SHA-1", combined);
		return bytesToHex(new Uint8Array(hash));
	}

	async sendPack(
		refs: Array<{ ref: string; old: string; new: string }>,
		packData: Uint8Array,
		force: boolean = false,
	): Promise<boolean> {
		if (!this.config.remote) {
			throw new Error("No remote configured");
		}

		const repoInfo = this.parseGitUrl(this.config.remote);

		// Discover remote refs
		const remoteRefs = await this.protocol.discoverRefs(repoInfo);

		// Validate refs before sending
		for (const ref of refs) {
			// Check if force push is required (non-fast-forward)
			const remoteRef = remoteRefs.find((r) => r.name === ref.ref);

			if (remoteRef?.oid && remoteRef.oid !== ref.old) {
				if (!force) {
					throw new Error(`Non-fast-forward push to ${ref.ref}. Use force push to override.`);
				}
			}

			// For deletion, new should be all zeros
			if (ref.new === "0000000000000000000000000000000000000000") {
				console.log(`Deleting ${ref.ref}`);
			}
		}

		// Send pack to remote
		await this.protocol.pushPack(repoInfo, refs, packData);

		return true;
	}

	// ==================== Encapsulation Methods ====================

	async readObject(oid: string) {
		return await this.objectStore.readObject(oid);
	}

	async writeObject(type: "blob" | "tree" | "commit" | "tag", data: Uint8Array): Promise<string> {
		return await this.objectStore.writeObject(type, data);
	}

	async getAllRefs() {
		return await this.refStore.getAllRefs();
	}

	async getRef(name: string): Promise<string | null> {
		return await this.refStore.readRef(name);
	}

	async writeRef(name: string, oid: string): Promise<void> {
		return await this.refStore.writeRef(name, oid);
	}

	async deleteRef(name: string): Promise<void> {
		return await this.refStore.deleteRef(name);
	}

	async collectTreeObjects(treeOid: string): Promise<string[]> {
		const objects: Set<string> = new Set();
		await this.#collectTreeObjectsRecursive(treeOid, objects);
		return Array.from(objects);
	}

	getIndexEntries() {
		return this.index.getEntries();
	}

	async addIndexEntry(entry: {
		path: string;
		oid: string;
		mode: string;
		size: number;
		mtime: number;
	}): Promise<void> {
		return await this.index.addEntry(entry);
	}

	async removeIndexEntry(path: string): Promise<void> {
		return await this.index.removeEntry(path);
	}

	async readFile(path: string): Promise<Uint8Array> {
		return await this.storage.readFile(path);
	}

	async writeFile(path: string, content: Uint8Array): Promise<void> {
		return await this.storage.writeFile(path, content);
	}

	async deleteFile(path: string): Promise<void> {
		return await this.storage.deleteFile(path);
	}

	async initStorage(repoName: string): Promise<void> {
		return await this.storage.init(repoName);
	}

	async #collectTreeObjectsRecursive(treeOid: string, objects: Set<string>): Promise<void> {
		if (objects.has(treeOid)) return;

		objects.add(treeOid);

		try {
			const tree = await this.objectStore.readObject(treeOid);
			const entries = this.parseTree(tree.data);

			for (const entry of entries) {
				if (entry.mode === "40000") {
					// Directory - recurse
					await this.#collectTreeObjectsRecursive(entry.oid, objects);
				} else {
					// File (blob)
					objects.add(entry.oid);
				}
			}
		} catch {
			// Tree not found, skip
		}
	}

	// ==================== Merge & Pack Delegation Methods ====================

	async merge(baseTree: string, ourTree: string, theirTree: string): Promise<MergeResult> {
		const merger = new GitMerge(this.objectStore, this.refStore);
		return await merger.threeWayMerge(baseTree, ourTree, theirTree);
	}

	async createPack(objects: string[]): Promise<Uint8Array> {
		const packWriter = new GitPackWriter(this.objectStore);
		return await packWriter.createPack(objects);
	}

	async parsePack(packStream: ReadableStream<Uint8Array>): Promise<void> {
		const parser = new GitPackParser(this.objectStore);
		return await parser.parsePack(packStream);
	}
}
