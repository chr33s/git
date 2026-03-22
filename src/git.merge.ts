import { GitObjectStore } from "./git.object.ts";
import { GitRefStore } from "./git.ref.ts";
import { bytesToHex, hexToBytes } from "./git.utils.ts";

// --- Diff3 three-way merge algorithm ---

function computeLCS(a: string[], b: string[]): [number, number][] {
  const m = a.length;
  const n = b.length;
  if (m * n > 10_000_000) return [];

  const dp: Uint32Array[] = [];
  for (let i = 0; i <= m; i++) dp[i] = new Uint32Array(n + 1);

  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (a[i - 1] === b[j - 1]) {
        dp[i]![j] = dp[i - 1]![j - 1]! + 1;
      } else {
        dp[i]![j] = Math.max(dp[i - 1]![j]!, dp[i]![j - 1]!);
      }
    }
  }

  const matches: [number, number][] = [];
  let i = m,
    j = n;
  while (i > 0 && j > 0) {
    if (a[i - 1] === b[j - 1]) {
      matches.push([i - 1, j - 1]);
      i--;
      j--;
    } else if (dp[i - 1]![j]! > dp[i]![j - 1]!) {
      i--;
    } else {
      j--;
    }
  }
  matches.reverse();
  return matches;
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

interface Diff3Result {
  merged: string[];
  hasConflicts: boolean;
  conflictRanges: Array<{ start: number; end: number }>;
}

function diff3Merge(base: string[], ours: string[], theirs: string[]): Diff3Result {
  const matchesOurs = computeLCS(base, ours);
  const matchesTheirs = computeLCS(base, theirs);

  const baseToOurs = new Map<number, number>();
  for (const [b, o] of matchesOurs) baseToOurs.set(b, o);

  const baseToTheirs = new Map<number, number>();
  for (const [b, t] of matchesTheirs) baseToTheirs.set(b, t);

  // Find stable points: base lines matched in both ours and theirs
  const stablePoints: Array<{ base: number; ours: number; theirs: number }> = [];
  for (const [bi] of matchesOurs) {
    if (baseToTheirs.has(bi)) {
      const oi = baseToOurs.get(bi)!;
      const ti = baseToTheirs.get(bi)!;
      if (stablePoints.length === 0) {
        stablePoints.push({ base: bi, ours: oi, theirs: ti });
      } else {
        const prev = stablePoints[stablePoints.length - 1]!;
        if (oi > prev.ours && ti > prev.theirs) {
          stablePoints.push({ base: bi, ours: oi, theirs: ti });
        }
      }
    }
  }

  const points = [
    { base: -1, ours: -1, theirs: -1 },
    ...stablePoints,
    { base: base.length, ours: ours.length, theirs: theirs.length },
  ];

  const merged: string[] = [];
  const conflictRanges: Array<{ start: number; end: number }> = [];
  let hasConflicts = false;

  for (let i = 0; i < points.length - 1; i++) {
    const p = points[i]!;
    const q = points[i + 1]!;

    if (p.base >= 0) {
      merged.push(base[p.base]!);
    }

    const baseGap = base.slice(p.base + 1, q.base);
    const oursGap = ours.slice(p.ours + 1, q.ours);
    const theirsGap = theirs.slice(p.theirs + 1, q.theirs);

    if (baseGap.length === 0 && oursGap.length === 0 && theirsGap.length === 0) {
      // No gap
    } else if (arraysEqual(oursGap, theirsGap)) {
      merged.push(...oursGap);
    } else if (arraysEqual(baseGap, oursGap)) {
      merged.push(...theirsGap);
    } else if (arraysEqual(baseGap, theirsGap)) {
      merged.push(...oursGap);
    } else {
      hasConflicts = true;
      const start = merged.length;
      merged.push("<<<<<<< ours");
      merged.push(...oursGap);
      merged.push("=======");
      merged.push(...theirsGap);
      merged.push(">>>>>>> theirs");
      conflictRanges.push({ start, end: merged.length });
    }
  }

  return { merged, hasConflicts, conflictRanges };
}

export interface MergeResult {
  success: boolean;
  mergedTree?: string;
  mergeCommitOid?: string;
  conflicts?: ConflictEntry[];
  message?: string;
}

export interface ConflictEntry {
  path: string;
  base?: string;
  ours?: string;
  theirs?: string;
  merged?: string;
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
    strategy: "recursive" | "resolve" | "ours" | "theirs" = "recursive",
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
      default:
        throw new Error(`Unknown merge strategy: ${String(strategy)}`);
    }
  }

  async mergeTrees(baseTree: string, ourTree: string, theirTree: string): Promise<MergeResult> {
    return await this.#recursiveMerge(baseTree, ourTree, theirTree);
  }

  async findMergeBase(commit1: string, commit2: string): Promise<string | null> {
    const history1 = new Set<string>();
    const queue1: string[] = [commit1];

    while (queue1.length > 0) {
      const current = queue1.pop()!;
      if (history1.has(current)) continue;
      history1.add(current);
      try {
        const obj = await this.#objectStore.readObject(current);
        if (obj.type !== "commit") continue;
        const info = this.#parseCommitFull(obj.data);
        for (const parent of info.parents) queue1.push(parent);
      } catch {
        // Object not found, stop this branch
      }
    }

    // BFS from commit2 to find the first intersection
    const queue2: string[] = [commit2];
    const visited = new Set<string>();

    while (queue2.length > 0) {
      const current = queue2.shift()!;
      if (visited.has(current)) continue;
      visited.add(current);
      if (history1.has(current)) return current;
      try {
        const obj = await this.#objectStore.readObject(current);
        if (obj.type !== "commit") continue;
        const info = this.#parseCommitFull(obj.data);
        for (const parent of info.parents) queue2.push(parent);
      } catch {
        // Object not found
      }
    }

    return null;
  }

  async mergeCommits(
    ourCommit: string,
    theirCommit: string,
    author: { name: string; email: string } = { name: "Git", email: "git@example.com" },
    message?: string,
  ): Promise<MergeResult> {
    const baseCommit = await this.findMergeBase(ourCommit, theirCommit);
    if (!baseCommit) {
      throw new Error("No common ancestor found");
    }

    const result = await this.threeWayMerge(baseCommit, ourCommit, theirCommit);

    if (!result.success) {
      return result;
    }

    // Create merge commit with two parents
    const authorStr = `${author.name} <${author.email}>`;
    const timestamp = Math.floor(Date.now() / 1000);
    const timezone = "+0000";
    const commitMessage = message || `Merge commit '${theirCommit.slice(0, 7)}'`;

    let commitData = `tree ${result.mergedTree}\n`;
    commitData += `parent ${ourCommit}\n`;
    commitData += `parent ${theirCommit}\n`;
    commitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
    commitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
    commitData += `\n${commitMessage}\n`;

    const mergeCommitOid = await this.#objectStore.writeObject(
      "commit",
      new TextEncoder().encode(commitData),
    );

    return {
      ...result,
      mergeCommitOid,
    };
  }

  async octopusMerge(
    ourCommit: string,
    theirCommits: string[],
    author: { name: string; email: string } = { name: "Git", email: "git@example.com" },
    message?: string,
  ): Promise<MergeResult> {
    if (theirCommits.length < 2) {
      throw new Error("Octopus merge requires at least 2 branches to merge");
    }

    let currentTree = await this.#getTreeFromCommit(ourCommit);
    let currentCommit = ourCommit;

    for (let i = 0; i < theirCommits.length; i++) {
      const theirCommit = theirCommits[i]!;

      // Find merge base between current state and this branch
      const base = await this.findMergeBase(currentCommit, theirCommit);
      if (!base) {
        return {
          success: false,
          message: `Octopus merge refused: no common ancestor with branch ${i + 1}`,
        };
      }

      const baseTree = await this.#getTreeFromCommit(base);
      const theirTree = await this.#getTreeFromCommit(theirCommit);

      const result = await this.#recursiveMerge(baseTree, currentTree, theirTree);

      if (!result.success) {
        return {
          success: false,
          message: `Octopus merge refused: conflicts with branch ${i + 1}`,
        };
      }

      currentTree = result.mergedTree!;
      currentCommit = theirCommit;
    }

    // Create octopus merge commit with N+1 parents
    const authorStr = `${author.name} <${author.email}>`;
    const timestamp = Math.floor(Date.now() / 1000);
    const timezone = "+0000";
    const commitMessage = message || `Merge ${theirCommits.length} branches`;

    let commitData = `tree ${currentTree}\n`;
    commitData += `parent ${ourCommit}\n`;
    for (const tc of theirCommits) {
      commitData += `parent ${tc}\n`;
    }
    commitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
    commitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
    commitData += `\n${commitMessage}\n`;

    const mergeCommitOid = await this.#objectStore.writeObject(
      "commit",
      new TextEncoder().encode(commitData),
    );

    return {
      success: true,
      mergedTree: currentTree,
      mergeCommitOid,
      message: `Octopus merge successful (${theirCommits.length + 1} branches)`,
    };
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
      }
      if (mergeResult.entry) {
        mergedEntries.push(mergeResult.entry);
      }
    }

    // Always create merged tree (even with conflicts — marker content is embedded)
    const mergedTree = await this.#createTree(mergedEntries);

    if (conflicts.length > 0) {
      return {
        success: false,
        mergedTree,
        conflicts,
        message: `Merge failed with ${conflicts.length} conflicts`,
      };
    }

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

        const mergedOid = await this.#objectStore.writeObject("blob", mergedContent.content);
        const entry: TreeEntry = {
          mode: ourEntry.mode,
          name: ourEntry.name,
          oid: mergedOid,
        };

        if (mergedContent.hasConflicts) {
          return {
            entry,
            conflict: {
              path,
              base: baseEntry?.oid,
              ours: ourEntry.oid,
              theirs: theirEntry.oid,
              merged: mergedOid,
            },
          };
        }

        return { entry };
      }

      // Binary conflict — no merged blob
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
  ): Promise<{
    content: Uint8Array;
    hasConflicts: boolean;
    conflictRanges: Array<{ start: number; end: number }>;
  }> {
    const ourContent = await this.#readBlobAsText(ourOid);
    const theirContent = await this.#readBlobAsText(theirOid);
    const baseContent = baseOid ? await this.#readBlobAsText(baseOid) : "";

    const result = diff3Merge(
      baseContent.split("\n"),
      ourContent.split("\n"),
      theirContent.split("\n"),
    );

    return {
      content: new TextEncoder().encode(result.merged.join("\n")),
      hasConflicts: result.hasConflicts,
      conflictRanges: result.conflictRanges,
    };
  }

  async detectRenames(oldTree: string, newTree: string, threshold: number = 0.5) {
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

  #calculateSimilarity(content1: Uint8Array, content2: Uint8Array) {
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

  async cherryPick(commitOid: string, targetBranch: string) {
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

  async rebase(sourceBranch: string, targetBranch: string, _interactive: boolean = false) {
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

  async #getTreeFromCommit(commitOid: string) {
    const commit = await this.#objectStore.readObject(commitOid);
    return this.#parseCommit(commit.data).tree;
  }

  async #getTreeFromBranch(branch: string) {
    // Read branch ref and get tree from commit
    const commitOid = await this.#getBranchHead(branch);
    return await this.#getTreeFromCommit(commitOid);
  }

  async #getBranchHead(branch: string) {
    // Read branch reference from .git/refs/heads/{branch} or packed-refs file
    return await this.#readBranchRef(branch);
  }

  async #readBranchRef(branch: string) {
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

  async #getCommitList(branch: string) {
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

  async #getAllPaths(...trees: string[]) {
    const paths = new Set<string>();

    for (const tree of trees) {
      const treePaths = await this.#getTreePaths(tree);
      treePaths.forEach((p) => paths.add(p));
    }

    return paths;
  }

  async #getTreePaths(treeOid: string) {
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

  async #getTreeEntry(treeOid: string, path: string) {
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

  async #createTree(entries: TreeEntry[]) {
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

  async #isTextFile(oid: string) {
    const blob = await this.#objectStore.readObject(oid);

    // Check for null bytes (binary indicator)
    for (let i = 0; i < Math.min(blob.data.length, 8192); i++) {
      if (blob.data[i] === 0) {
        return false;
      }
    }

    return true;
  }

  async #readBlob(oid: string) {
    const blob = await this.#objectStore.readObject(oid);
    return blob.data;
  }

  async #readBlobAsText(oid: string) {
    const data = await this.#readBlob(oid);
    return new TextDecoder().decode(data);
  }

  #parseCommit(data: Uint8Array) {
    const text = new TextDecoder().decode(data);
    const lines = text.split("\n");

    const tree = lines.find((l) => l.startsWith("tree "))?.slice(5) || "";
    const parent = lines.find((l) => l.startsWith("parent "))?.slice(7);
    const author = lines.find((l) => l.startsWith("author "))?.slice(7) || "";

    const messageStart = lines.findIndex((l) => l === "") + 1;
    const message = lines.slice(messageStart).join("\n");

    return { tree, parent, author, message };
  }

  #parseCommitFull(data: Uint8Array) {
    const text = new TextDecoder().decode(data);
    const lines = text.split("\n");

    const tree = lines.find((l) => l.startsWith("tree "))?.slice(5) || "";
    const parents = lines.filter((l) => l.startsWith("parent ")).map((l) => l.slice(7));
    const author = lines.find((l) => l.startsWith("author "))?.slice(7) || "";

    const messageStart = lines.findIndex((l) => l === "") + 1;
    const message = lines.slice(messageStart).join("\n");

    return { tree, parents, author, message };
  }

  #parseTree(data: Uint8Array) {
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

  getUnresolvedConflicts() {
    return Array.from(this.#conflicts.values()).filter((c) => !c.resolved);
  }

  getAllConflicts() {
    return Array.from(this.#conflicts.values());
  }

  isAllResolved() {
    return Array.from(this.#conflicts.values()).every((c) => c.resolved);
  }

  clear() {
    this.#conflicts.clear();
  }
}
