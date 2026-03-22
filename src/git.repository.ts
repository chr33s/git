import { GitIndex } from "./git.index.ts";
import { GitObjectStore, type FsckResult } from "./git.object.ts";
import { GitPackParser, GitPackWriter } from "./git.pack.ts";
import { GitProtocol } from "./git.protocol.ts";
import { GitRefStore, type GitRefUpdate } from "./git.ref.ts";
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
  parents: string[];
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

const ZERO_OID = "0".repeat(40);

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

    await this.#initializeRepositoryLayout(this.config.branch || "main");
  }

  async #initializeRepositoryLayout(initialBranch: string) {
    // Initialize git components
    await this.objectStore.init();
    await this.refStore.init();
    await this.index.init();

    // Create initial git directory structure
    await this.storage.createDirectory(".git/hooks");
    await this.storage.createDirectory(".git/info");
    await this.storage.createDirectory(".git/objects/info");
    await this.storage.createDirectory(".git/objects/pack");

    // Initialize HEAD for this repository namespace if it doesn't exist yet.
    if (!(await this.storage.exists(".git/HEAD"))) {
      await this.refStore.writeSymbolicRef("HEAD", `refs/heads/${initialBranch}`, "init");
    }
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
    await this.refStore.writeSymbolicRef("HEAD", defaultBranch, "clone");
    for (const ref of refs) {
      if (ref.oid && ref.name !== "HEAD") {
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

  async commit(message: string, author?: GitAuthor) {
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
      const updated = await this.refStore.compareAndSwapRef(
        headRef,
        parentOid || null,
        commitOid,
        "commit",
      );

      if (!updated) {
        throw new Error(`HEAD moved during commit for ${headRef}`);
      }
    }

    return commitOid;
  }

  async createTreeFromIndex() {
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

  async createTreeObject(tree: Map<string, any[]>, path: string) {
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

  parseGitUrl(url: string) {
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

  parseCommit(data: Uint8Array) {
    const text = new TextDecoder().decode(data);
    const lines = text.split("\n");

    const tree = lines.find((l) => l.startsWith("tree "))?.slice(5) || "";
    const parents = lines.filter((l) => l.startsWith("parent ")).map((l) => l.slice(7));
    const parent = parents[0];
    const author = lines.find((l) => l.startsWith("author "))?.slice(7) || "";

    const messageStart = lines.findIndex((l) => l === "") + 1;
    const message = lines.slice(messageStart).join("\n");

    return { tree, parent, parents, author, message };
  }

  parseTree(data: Uint8Array) {
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

  async findInTree(treeOid: string, path: string) {
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

  async getCurrentHead() {
    return await this.refStore.readSymbolicRef("HEAD");
  }

  async getCurrentCommitOid() {
    return await this.refStore.readRef("HEAD");
  }

  async hashObject(type: string, data: Uint8Array) {
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
  ) {
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

  async validateObject(oid: string): Promise<FsckResult> {
    return await this.objectStore.validateObject(oid);
  }

  async fsckAll(): Promise<FsckResult[]> {
    return await this.objectStore.fsckAll();
  }

  async writeObject(type: "blob" | "tree" | "commit" | "tag", data: Uint8Array) {
    return await this.objectStore.writeObject(type, data);
  }

  async getAllRefs() {
    return await this.refStore.getAllRefs();
  }

  async getRef(name: string) {
    try {
      return await this.refStore.readRef(name);
    } catch {
      return null;
    }
  }

  async readSymbolicRef(name: string) {
    return await this.refStore.readSymbolicRef(name);
  }

  async writeRef(name: string, oid: string, message?: string) {
    return await this.refStore.writeRef(name, oid, message);
  }

  async writeSymbolicRef(name: string, target: string, message?: string) {
    return await this.refStore.writeSymbolicRef(name, target, message);
  }

  async compareAndSwapRef(name: string, expectedOld: string | null, oid: string, message?: string) {
    return await this.refStore.compareAndSwapRef(name, expectedOld, oid, message);
  }

  async deleteRef(name: string, message?: string) {
    return await this.refStore.deleteRef(name, message);
  }

  async updateRefs(
    updates: GitRefUpdate[],
    options?: { atomic?: boolean; compareOldOid?: boolean },
  ) {
    return await this.refStore.applyRefUpdates(updates, options);
  }

  async readReflog(name: string) {
    return await this.refStore.readReflog(name);
  }

  async getReachableObjects() {
    const reachable = new Set<string>();
    const queue: string[] = [];

    const refs = await this.refStore.getAllRefs();
    for (const ref of refs) {
      if (this.#isReachableOid(ref.oid)) {
        queue.push(ref.oid);
      }
    }

    const headOid = await this.refStore.readRef("HEAD");
    if (this.#isReachableOid(headOid)) {
      queue.push(headOid);
    }

    const reflogRefs = new Set<string>(["HEAD", ...(await this.refStore.listReflogRefs())]);
    for (const reflogRef of reflogRefs) {
      const entries = await this.refStore.readReflog(reflogRef);
      for (const entry of entries) {
        if (this.#isReachableOid(entry.oldOid)) {
          queue.push(entry.oldOid);
        }
        if (this.#isReachableOid(entry.newOid)) {
          queue.push(entry.newOid);
        }
      }
    }

    while (queue.length > 0) {
      const oid = queue.pop();
      if (!oid || reachable.has(oid) || !this.#isReachableOid(oid)) {
        continue;
      }

      let object;
      try {
        object = await this.objectStore.readObject(oid);
      } catch {
        continue;
      }

      reachable.add(oid);

      if (object.type === "commit") {
        const info = this.parseCommit(object.data);
        if (this.#isReachableOid(info.tree)) {
          queue.push(info.tree);
        }
        for (const parent of info.parents) {
          if (this.#isReachableOid(parent)) {
            queue.push(parent);
          }
        }
      } else if (object.type === "tree") {
        const entries = this.parseTree(object.data);
        for (const entry of entries) {
          if (this.#isReachableOid(entry.oid)) {
            queue.push(entry.oid);
          }
        }
      } else if (object.type === "tag") {
        const targetOid = this.#parseTagTarget(object.data);
        if (this.#isReachableOid(targetOid)) {
          queue.push(targetOid);
        }
      }
    }

    return reachable;
  }

  async gc(gracePeriodMinutes: number = 10) {
    const reachable = await this.getReachableObjects();
    const gracePeriodMs = Math.max(0, gracePeriodMinutes) * 60_000;
    const cutoff = Date.now() - gracePeriodMs;
    const useGracePeriod = gracePeriodMs > 0;

    const protectedOids = new Set<string>(reachable);
    const looseObjects = await this.objectStore.listLooseObjects();
    for (const object of looseObjects) {
      if (useGracePeriod && object.lastModified.getTime() >= cutoff) {
        protectedOids.add(object.oid);
      }
    }

    const packs = await this.objectStore.listPackFiles();
    const packsToDelete: Array<{ deletedObjects: number; pack: (typeof packs)[number] }> = [];
    const repackCandidates: Array<{ keepOids: string[]; pack: (typeof packs)[number] }> = [];

    for (const pack of packs) {
      if (useGracePeriod && pack.lastModified.getTime() >= cutoff) {
        for (const entry of pack.index.entries) {
          protectedOids.add(entry.oid);
        }
        continue;
      }

      const keepOids = pack.index.entries
        .filter((entry) => protectedOids.has(entry.oid))
        .map((entry) => entry.oid);

      if (keepOids.length === pack.index.entries.length) {
        continue;
      }

      if (keepOids.length === 0) {
        packsToDelete.push({ deletedObjects: pack.index.entries.length, pack });
      } else {
        repackCandidates.push({ keepOids: Array.from(new Set(keepOids)), pack });
      }
    }

    let deleted = 0;
    let freedBytes = 0;

    for (const object of looseObjects) {
      if (protectedOids.has(object.oid)) {
        continue;
      }

      await this.storage.deleteFile(object.path);
      deleted++;
      freedBytes += object.size;
    }

    const repackOids = Array.from(
      new Set(repackCandidates.flatMap((candidate) => candidate.keepOids)),
    );

    let rewrittenBytes = 0;
    if (repackOids.length > 0) {
      const writer = new GitPackWriter(this.objectStore);
      const artifacts = await writer.createPackArtifacts(repackOids);
      const { idxPath, packPath } = await this.objectStore.writePack(
        artifacts.packData,
        artifacts.indexEntries,
      );
      const [packInfo, idxInfo] = await Promise.all([
        this.storage.getFileInfo(packPath),
        this.storage.getFileInfo(idxPath),
      ]);
      rewrittenBytes = packInfo.size + idxInfo.size;
    }

    for (const candidate of repackCandidates) {
      const removedCount = candidate.pack.index.entries.length - candidate.keepOids.length;
      deleted += removedCount;
      freedBytes += candidate.pack.packSize + candidate.pack.idxSize;
      await this.storage.deleteFile(candidate.pack.packPath);
      await this.storage.deleteFile(candidate.pack.idxPath);
    }

    for (const candidate of packsToDelete) {
      deleted += candidate.deletedObjects;
      freedBytes += candidate.pack.packSize + candidate.pack.idxSize;
      await this.storage.deleteFile(candidate.pack.packPath);
      await this.storage.deleteFile(candidate.pack.idxPath);
    }

    return {
      deleted,
      freedBytes: Math.max(0, freedBytes - rewrittenBytes),
    };
  }

  async collectTreeObjects(treeOid: string) {
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
  }) {
    return await this.index.addEntry(entry);
  }

  async removeIndexEntry(path: string) {
    return await this.index.removeEntry(path);
  }

  async readFile(path: string) {
    return await this.storage.readFile(path);
  }

  async writeFile(path: string, content: Uint8Array) {
    return await this.storage.writeFile(path, content);
  }

  async deleteFile(path: string) {
    return await this.storage.deleteFile(path);
  }

  async initStorage(repoName: string, branch: string = this.config.branch || "main") {
    await this.storage.init(repoName);
    await this.#initializeRepositoryLayout(branch);
  }

  async #collectTreeObjectsRecursive(treeOid: string, objects: Set<string>) {
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

  // ==================== Shallow Graft Methods ====================

  async getShallowCommits() {
    const shallow = new Set<string>();
    try {
      const data = await this.storage.readFile(".git/shallow");
      const text = new TextDecoder().decode(data);
      for (const line of text.split("\n")) {
        const oid = line.trim();
        if (oid) shallow.add(oid);
      }
    } catch {
      // No shallow file
    }
    return shallow;
  }

  async setShallowCommits(oids: Set<string>) {
    if (oids.size === 0) {
      try {
        await this.storage.deleteFile(".git/shallow");
      } catch {
        // File doesn't exist
      }
      return;
    }
    const text = Array.from(oids).join("\n") + "\n";
    await this.storage.writeFile(".git/shallow", new TextEncoder().encode(text));
  }

  // ==================== Merge & Pack Delegation Methods ====================

  async merge(baseTree: string, ourTree: string, theirTree: string): Promise<MergeResult> {
    const merger = new GitMerge(this.objectStore, this.refStore);
    return await merger.mergeTrees(baseTree, ourTree, theirTree);
  }

  async mergeRef(
    ref: string,
    author: GitAuthor = { name: "Git", email: "git@example.com" },
    message?: string,
  ): Promise<MergeResult> {
    const headRef = await this.getCurrentHead();
    const headOid = headRef ? await this.refStore.readRef(headRef) : null;
    if (!headOid) {
      throw new Error("No HEAD commit");
    }

    // Resolve ref to commit OID
    let theirOid = ref;
    const refOid = await this.getRef(ref);
    if (refOid) theirOid = refOid;

    const merger = new GitMerge(this.objectStore, this.refStore);

    // Find merge base
    const baseOid = await merger.findMergeBase(headOid, theirOid);
    if (!baseOid) {
      throw new Error("No common ancestor found");
    }

    const result = await merger.mergeCommits(headOid, theirOid, author, message);

    if (!result.success) {
      // Write MERGE_HEAD for conflict resolution flow
      await this.storage.writeFile(".git/MERGE_HEAD", new TextEncoder().encode(theirOid + "\n"));
      return result;
    }

    // Update HEAD to point to merge commit
    if (headRef && result.mergeCommitOid) {
      const updated = await this.refStore.compareAndSwapRef(
        headRef,
        headOid,
        result.mergeCommitOid,
        "merge",
      );
      if (!updated) {
        throw new Error(`HEAD moved during merge for ${headRef}`);
      }

      // Clear MERGE_HEAD on success
      await this.#clearMergeHead();
    }

    return result;
  }

  async abortMerge(): Promise<void> {
    await this.#clearMergeHead();
  }

  async getMergeHead(): Promise<string | null> {
    try {
      const data = await this.storage.readFile(".git/MERGE_HEAD");
      return new TextDecoder().decode(data).trim() || null;
    } catch {
      return null;
    }
  }

  async #clearMergeHead() {
    try {
      await this.storage.deleteFile(".git/MERGE_HEAD");
    } catch {
      // Already cleared
    }
  }

  async findMergeBase(commit1: string, commit2: string): Promise<string | null> {
    const merger = new GitMerge(this.objectStore, this.refStore);
    return await merger.findMergeBase(commit1, commit2);
  }

  async createPack(objects: string[]) {
    const packWriter = new GitPackWriter(this.objectStore);
    return await packWriter.createPack(objects);
  }

  async parsePack(packStream: ReadableStream<Uint8Array>) {
    const parser = new GitPackParser(this.objectStore);
    return await parser.parsePack(packStream);
  }

  #parseTagTarget(data: Uint8Array) {
    const text = new TextDecoder().decode(data);
    const objectLine = text
      .split("\n")
      .find((line) => line.startsWith("object "))
      ?.slice(7);

    return objectLine || null;
  }

  #isReachableOid(oid: string | null | undefined): oid is string {
    return !!oid && oid !== ZERO_OID && /^[0-9a-f]{40}$/.test(oid);
  }
}
