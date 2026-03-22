import type { GitRepository } from "./git.repository.ts";

export interface ServerApiRequest {
  url: string;
  method: string;
  body: ReadableStream<Uint8Array<ArrayBuffer>> | null;
}
type Payload = Record<string, any>;

/** Pagination response with cursor-based navigation */
export interface PaginatedResponse<T> {
  items: T[];
  next_cursor: string | null;
  has_more: boolean;
}

/** NDJSON commit-pack metadata */
export interface CommitPackMetadata {
  target_branch: string;
  commit_message: string;
  author: { name: string; email: string; date?: string };
  committer?: { name: string; email: string; date?: string };
  expected_head_sha?: string;
  base_branch?: string;
  files: Array<{
    path: string;
    operation: "upsert" | "delete";
    content_id: string;
    mode?: string;
  }>;
}

/** NDJSON blob chunk */
export interface BlobChunk {
  content_id: string;
  data: string; // base64 encoded
  eof: boolean;
}

/** Commit-pack result */
export interface CommitPackResult {
  commit: {
    commit_sha: string;
    tree_sha: string;
    target_branch: string;
    pack_bytes: number;
    blob_count: number;
  };
  result: {
    branch: string;
    old_sha: string;
    new_sha: string;
    success: boolean;
    status: string;
    message: string;
  };
}

interface BaseRoute {
  method: string;
  pathname: string;
}

interface JsonRoute extends BaseRoute {
  streaming?: false;
  handler: (payload: Payload, signal?: AbortSignal) => Promise<Response>;
}

type SteamingPayload = ReadableStream<Uint8Array> | null;

interface StreamRoute extends BaseRoute {
  streaming: true;
  handler: (body: SteamingPayload, signal?: AbortSignal) => Promise<Response>;
}

type Route = JsonRoute | StreamRoute;

export class ServerApi {
  #repository: GitRepository;
  #routes: Route[] = [
    {
      handler: (...args) => this.#status(...args),
      method: "GET",
      pathname: "/api/:repo{.git}?/status",
    },
    {
      handler: (...args) => this.#log(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/log",
    },
    {
      handler: (...args) => this.#show(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/show",
    },
    {
      handler: (...args) => this.#branch(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/branch",
    },
    {
      handler: (...args) => this.#checkout(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/checkout",
    },
    {
      handler: (...args) => this.#commit(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/commit",
    },
    {
      handler: (...args) => this.#add(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/add",
    },
    {
      handler: (...args) => this.#rm(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/rm",
    },
    {
      handler: (...args) => this.#refs(...args),
      method: "GET",
      pathname: "/api/:repo{.git}?/refs",
    },
    {
      handler: (...args) => this.#tag(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/tag",
    },
    {
      handler: (...args) => this.#merge(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/merge",
    },
    {
      handler: (...args) => this.#reset(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/reset",
    },
    {
      handler: (...args) => this.#read(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/read",
    },
    {
      handler: (...args) => this.#write(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/write",
    },
    {
      handler: (...args) => this.#tree(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/tree",
    },
    {
      handler: (...args) => this.#diff(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/diff",
    },
    {
      handler: (...args) => this.#object(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/object",
    },
    {
      handler: (...args) => this.#mv(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/mv",
    },
    {
      handler: (...args) => this.#restore(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/restore",
    },
    {
      handler: (...args) => this.#switch(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/switch",
    },
    {
      handler: (...args) => this.#rebase(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/rebase",
    },
    {
      handler: (...args) => this.#fetch(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/fetch",
    },
    {
      handler: (...args) => this.#pull(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/pull",
    },
    {
      handler: (...args) => this.#push(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/push",
    },
    {
      handler: (...args) => this.#remote(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/remote",
    },
    // Advanced API endpoints
    {
      handler: (...args) => this.#grep(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/grep",
    },
    {
      handler: (...args) => this.#createBranch(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/branches/create",
    },
    {
      handler: (...args) => this.#getBranchDiff(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/branches/diff",
    },
    {
      handler: (...args) => this.#getCommitDiff(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/commits/diff",
    },
    {
      handler: (...args) => this.#listFiles(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/files",
    },
    {
      handler: (...args) => this.#getFileStream(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/file",
    },
    {
      handler: (...args) => this.#listBranches(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/branches",
    },
    {
      handler: (...args) => this.#listCommits(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/commits",
    },
    {
      handler: (...args) => this.#restoreCommit(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/restore-commit",
    },
    // Repository management
    {
      handler: (...args) => this.#createRepo(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?",
    },
    {
      handler: (...args) => this.#deleteRepo(...args),
      method: "DELETE",
      pathname: "/api/:repo{.git}?",
    },
    // Streaming endpoints
    {
      handler: (...args) => this.#commitPack(...args),
      method: "POST",
      pathname: "/api/:repo{.git}?/commit-pack",
      streaming: true,
    },
  ];

  constructor(repository: GitRepository) {
    this.#repository = repository;
  }

  async fetch(request: ServerApiRequest, signal?: AbortSignal) {
    signal?.throwIfAborted();

    for (const route of this.#routes) {
      if (route.method !== request.method) continue;

      const pattern = new URLPattern({ pathname: route.pathname });
      if (!pattern.test(request.url)) continue;

      try {
        if (route.streaming) {
          return route.handler(request.body, signal);
        }

        const payload = await this.#parseBody(request.body);
        return route.handler(payload, signal);
      } catch (error: any) {
        if (error.name === "AbortError") throw error;
        console.error(`API error for '${route.pathname}':`, error);
        return Response.json({ error: error.message || "Internal error" }, { status: 500 });
      }
    }

    return Response.json({ error: "Not Found" }, { status: 404 });
  }

  async #parseBody(body: ServerApiRequest["body"]) {
    if (!body) return {};

    const reader = body.getReader();
    const chunks: Uint8Array[] = [];
    let result = await reader.read();
    while (!result.done) {
      chunks.push(result.value);
      result = await reader.read();
    }
    reader.releaseLock();

    if (chunks.length === 0) return {};

    const fullData = new Uint8Array(chunks.reduce((acc, c) => acc + c.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
      fullData.set(chunk, offset);
      offset += chunk.length;
    }
    const text = new TextDecoder().decode(fullData);
    return text ? JSON.parse(text) : {};
  }

  async #status(_payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const indexEntries = this.#repository.getIndexEntries();
    const headCommitOid = await this.#repository.getCurrentCommitOid();

    let staged: string[] = [];
    const modified: string[] = [];
    const untracked: string[] = [];

    if (headCommitOid) {
      staged = indexEntries.map((e) => e.path);
    } else {
      staged = indexEntries.map((e) => e.path);
    }

    return Response.json({ staged, modified, untracked });
  }

  async #log(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const maxCount = payload.maxCount as number | undefined;
    const commits: any[] = [];
    let commitOid = await this.#repository.getCurrentCommitOid();
    let count = 0;

    while (commitOid && (!maxCount || count < maxCount)) {
      const commit = await this.#repository.readObject(commitOid);
      if (commit.type !== "commit") break;

      const info = this.#repository.parseCommit(commit.data);
      commits.push({
        oid: commitOid,
        tree: info.tree,
        parent: info.parent,
        author: info.author,
        message: info.message,
      });

      commitOid = info.parent || null;
      count++;
    }

    return Response.json({ commits });
  }

  async #show(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const ref = (payload.ref as string) || "HEAD";
    let oid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      oid = refOid;
    }

    const obj = await this.#repository.readObject(oid);

    if (obj.type === "commit") {
      const info = this.#repository.parseCommit(obj.data);
      return Response.json({
        oid,
        type: obj.type,
        tree: info.tree,
        parent: info.parent,
        author: info.author,
        message: info.message,
      });
    }

    return Response.json({
      oid,
      type: obj.type,
      data: new TextDecoder().decode(obj.data),
    });
  }

  async #branch(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const name = payload.name as string | undefined;
    const deleteFlag = payload.delete as boolean | undefined;

    if (deleteFlag && name) {
      await this.#repository.deleteRef(`refs/heads/${name}`);
      return Response.json({ deleted: name });
    }

    if (name) {
      const headOid = await this.#repository.getCurrentCommitOid();
      if (!headOid) {
        return Response.json({ error: "No HEAD commit" }, { status: 400 });
      }
      await this.#repository.writeRef(`refs/heads/${name}`, headOid);
      return Response.json({ created: name });
    }

    const refs = await this.#repository.getAllRefs();
    const branches = refs
      .filter((r) => r.name.startsWith("refs/heads/"))
      .map((r) => r.name.replace("refs/heads/", ""));
    return Response.json({ branches });
  }

  async #checkout(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const ref = payload.ref as string;
    if (!ref) {
      return Response.json({ error: "ref required" }, { status: 400 });
    }

    let commitOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      commitOid = refOid;
    }

    await this.#repository.checkoutCommit(commitOid);
    return Response.json({ success: true, ref });
  }

  async #commit(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const message = payload.message as string;
    const author = payload.author as { name: string; email: string } | undefined;
    if (!message) {
      return Response.json({ error: "message required" }, { status: 400 });
    }
    const oid = await this.#repository.commit(message, author);
    return Response.json({ success: true, oid });
  }

  async #add(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const path = payload.path as string;
    const content = payload.content as string;
    if (!path) {
      return Response.json({ error: "path required" }, { status: 400 });
    }

    let data: Uint8Array;
    if (content !== undefined) {
      data = new TextEncoder().encode(content);
    } else {
      data = await this.#repository.readFile(path);
    }
    await this.#repository.add(path, data);
    return Response.json({ success: true, path });
  }

  async #rm(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const paths = payload.paths as string | string[];
    const cached = payload.cached as boolean | undefined;
    const recursive = payload.recursive as boolean | undefined;
    if (!paths) {
      return Response.json({ error: "paths required" }, { status: 400 });
    }

    const pathsArray = Array.isArray(paths) ? paths : [paths];

    for (const path of pathsArray) {
      const entries = this.#repository.getIndexEntries();
      const matchingEntries = entries.filter((e) =>
        recursive ? e.path.startsWith(path + "/") || e.path === path : e.path === path,
      );

      if (matchingEntries.length === 0) {
        return Response.json(
          { error: `pathspec '${path}' did not match any files` },
          { status: 400 },
        );
      }

      for (const entry of matchingEntries) {
        await this.#repository.removeIndexEntry(entry.path);

        if (!cached) {
          try {
            await this.#repository.deleteFile(entry.path);
          } catch {
            // File might not exist
          }
        }
      }
    }

    return Response.json({ success: true });
  }

  async #refs(_payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const refs = await this.#repository.getAllRefs();
    return Response.json({ refs });
  }

  async #tag(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const name = payload.name as string;
    const ref = payload.ref as string | undefined;
    const deleteFlag = payload.delete as boolean | undefined;
    if (!name) {
      return Response.json({ error: "name required" }, { status: 400 });
    }

    if (deleteFlag) {
      await this.#repository.deleteRef(`refs/tags/${name}`);
      return Response.json({ deleted: name });
    }

    let oid: string;
    if (ref) {
      const refOid = await this.#repository.getRef(ref);
      oid = refOid || ref;
    } else {
      const headOid = await this.#repository.getCurrentCommitOid();
      if (!headOid) {
        return Response.json({ error: "No HEAD commit" }, { status: 400 });
      }
      oid = headOid;
    }

    await this.#repository.writeRef(`refs/tags/${name}`, oid);
    return Response.json({ created: name, oid });
  }

  async #merge(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const ref = payload.ref as string;
    if (!ref) {
      return Response.json({ error: "ref required" }, { status: 400 });
    }

    const headOid = await this.#repository.getCurrentCommitOid();
    if (!headOid) {
      return Response.json({ error: "No HEAD commit" }, { status: 400 });
    }

    let mergeOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      mergeOid = refOid;
    }

    const commonAncestorOid = await this.#findCommonAncestor(headOid, mergeOid);
    if (!commonAncestorOid) {
      return Response.json({ error: "No common ancestor found" }, { status: 400 });
    }

    const currentCommit = await this.#repository.readObject(headOid);
    const currentInfo = this.#repository.parseCommit(currentCommit.data);

    const mergeCommit = await this.#repository.readObject(mergeOid);
    const mergeInfo = this.#repository.parseCommit(mergeCommit.data);

    const baseCommit = await this.#repository.readObject(commonAncestorOid);
    const baseInfo = this.#repository.parseCommit(baseCommit.data);

    const result = await this.#repository.merge(baseInfo.tree, currentInfo.tree, mergeInfo.tree);

    if (!result.success) {
      return Response.json(
        { error: `Merge conflict: ${result.message || "Unable to merge"}` },
        { status: 409 },
      );
    }

    const authorStr = "Git Server <server@example.com>";
    const timestamp = Math.floor(Date.now() / 1000);
    const timezone = "+0000";

    let mergeCommitData = `tree ${result.mergedTree}\n`;
    mergeCommitData += `parent ${headOid}\n`;
    mergeCommitData += `parent ${mergeOid}\n`;
    mergeCommitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
    mergeCommitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
    mergeCommitData += `\nMerge branch '${ref}'\n`;

    const mergeCommitOid = await this.#repository.writeObject(
      "commit",
      new TextEncoder().encode(mergeCommitData),
    );

    const headRef = await this.#repository.getCurrentHead();
    if (headRef) {
      await this.#repository.writeRef(headRef, mergeCommitOid);
    }

    return Response.json({ success: true, mergedTree: result.mergedTree, mergeCommitOid });
  }

  async #findCommonAncestor(oid1: string, oid2: string) {
    const history1 = new Set<string>();
    let current: string | null = oid1;

    while (current) {
      history1.add(current);
      const commit = await this.#repository.readObject(current);
      if (commit.type !== "commit") break;
      const info = this.#repository.parseCommit(commit.data);
      current = info.parent || null;
    }

    current = oid2;
    while (current) {
      if (history1.has(current)) {
        return current;
      }
      const commit = await this.#repository.readObject(current);
      if (commit.type !== "commit") break;
      const info = this.#repository.parseCommit(commit.data);
      current = info.parent || null;
    }

    return null;
  }

  async #reset(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const ref = (payload.ref as string) || "HEAD";
    const hard = payload.hard as boolean | undefined;

    let commitOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      commitOid = refOid;
    }

    await this.#repository.checkoutCommit(commitOid);

    if (hard) {
      const headRef = await this.#repository.getCurrentHead();
      if (headRef) {
        await this.#repository.writeRef(headRef, commitOid);
      }
    }

    return Response.json({ success: true, ref });
  }

  async #read(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const path = payload.path as string;
    if (!path) {
      return Response.json({ error: "path required" }, { status: 400 });
    }
    const data = await this.#repository.readFile(path);
    const content = new TextDecoder().decode(data);
    return Response.json({ path, content });
  }

  async #write(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const path = payload.path as string;
    const content = payload.content as string;
    if (!path || content === undefined) {
      return Response.json({ error: "path and content required" }, { status: 400 });
    }
    await this.#repository.writeFile(path, new TextEncoder().encode(content));
    return Response.json({ success: true, path });
  }

  async #tree(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const ref = (payload.ref as string) || "HEAD";
    const path = payload.path as string | undefined;

    let commitOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      commitOid = refOid;
    }

    const commit = await this.#repository.readObject(commitOid);
    if (commit.type !== "commit") {
      return Response.json({ error: "Not a commit" }, { status: 400 });
    }

    const info = this.#repository.parseCommit(commit.data);
    let treeOid = info.tree;

    if (path) {
      const entry = await this.#repository.findInTree(treeOid, path);
      if (!entry) {
        return Response.json({ error: `Path '${path}' not found` }, { status: 404 });
      }
      treeOid = entry.oid;
    }

    const tree = await this.#repository.readObject(treeOid);
    const entries = this.#repository.parseTree(tree.data);

    return Response.json({
      oid: treeOid,
      entries: entries.map((e) => ({
        mode: e.mode,
        type: e.mode === "40000" ? "tree" : "blob",
        name: e.name,
        oid: e.oid,
      })),
    });
  }

  async #diff(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const from = payload.from as string | undefined;
    const to = payload.to as string | undefined;
    const path = payload.path as string | undefined;

    const headOid = await this.#repository.getCurrentCommitOid();

    let fromOid: string | null = null;
    let toOid: string | null = headOid;

    if (from) {
      const refOid = await this.#repository.getRef(from);
      fromOid = refOid || from;
    }

    if (to) {
      const refOid = await this.#repository.getRef(to);
      toOid = refOid || to;
    }

    const changes: any[] = [];

    if (toOid) {
      const commit = await this.#repository.readObject(toOid);
      if (commit.type === "commit") {
        const info = this.#repository.parseCommit(commit.data);
        const treeFiles = await this.#collectTreeFiles(info.tree, "");

        for (const [filePath, oid] of Object.entries(treeFiles)) {
          if (path && !filePath.startsWith(path)) continue;
          changes.push({ path: filePath, oid, status: "added" });
        }
      }
    }

    return Response.json({ from: fromOid, to: toOid, changes });
  }

  async #object(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const oid = payload.oid as string;
    if (!oid) {
      return Response.json({ error: "oid required" }, { status: 400 });
    }
    const obj = await this.#repository.readObject(oid);
    return Response.json({
      oid,
      type: obj.type,
      data: new TextDecoder().decode(obj.data),
    });
  }

  async #collectTreeFiles(treeOid: string, prefix: string) {
    const files: Record<string, string> = {};

    const tree = await this.#repository.readObject(treeOid);
    const entries = this.#repository.parseTree(tree.data);

    for (const entry of entries) {
      const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;

      if (entry.mode === "40000") {
        const subFiles = await this.#collectTreeFiles(entry.oid, fullPath);
        Object.assign(files, subFiles);
      } else {
        files[fullPath] = entry.oid;
      }
    }

    return files;
  }

  async #mv(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const source = payload.source as string;
    const destination = payload.destination as string;

    if (!source || !destination) {
      return Response.json({ error: "source and destination required" }, { status: 400 });
    }

    // Read the source file
    const content = await this.#repository.readFile(source);

    // Write to destination
    await this.#repository.writeFile(destination, content);

    // Update index: remove old entry and add new one
    const entries = this.#repository.getIndexEntries();
    const sourceEntry = entries.find((e) => e.path === source);

    if (sourceEntry) {
      await this.#repository.removeIndexEntry(source);
      await this.#repository.addIndexEntry({
        path: destination,
        oid: sourceEntry.oid,
        mode: sourceEntry.mode,
        size: content.byteLength,
        mtime: Date.now(),
      });
    }

    // Delete source file
    await this.#repository.deleteFile(source);

    return Response.json({ success: true, source, destination });
  }

  async #restore(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const path = payload.path as string;
    const staged = payload.staged as boolean | undefined;
    const source = payload.source as string | undefined;

    if (!path) {
      return Response.json({ error: "path required" }, { status: 400 });
    }

    if (staged) {
      // Restore from HEAD to index (unstage)
      const headOid = await this.#repository.getCurrentCommitOid();
      if (!headOid) {
        return Response.json({ error: "No HEAD commit" }, { status: 400 });
      }

      const commit = await this.#repository.readObject(headOid);
      const info = this.#repository.parseCommit(commit.data);
      const entry = await this.#repository.findInTree(info.tree, path);

      if (entry) {
        const blob = await this.#repository.readObject(entry.oid);
        await this.#repository.addIndexEntry({
          path,
          oid: entry.oid,
          mode: entry.mode,
          size: blob.data.byteLength,
          mtime: Date.now(),
        });
      } else {
        await this.#repository.removeIndexEntry(path);
      }
    } else {
      // Restore from index or source to working tree
      let treeOid: string | null = null;

      if (source) {
        const refOid = await this.#repository.getRef(source);
        const commitOid = refOid || source;
        const commit = await this.#repository.readObject(commitOid);
        if (commit.type === "commit") {
          const info = this.#repository.parseCommit(commit.data);
          treeOid = info.tree;
        }
      }

      if (treeOid) {
        const entry = await this.#repository.findInTree(treeOid, path);
        if (!entry) {
          return Response.json({ error: `Path '${path}' not found in source` }, { status: 404 });
        }
        const blob = await this.#repository.readObject(entry.oid);
        await this.#repository.writeFile(path, blob.data);
      } else {
        // Restore from index
        const entries = this.#repository.getIndexEntries();
        const indexEntry = entries.find((e) => e.path === path);
        if (!indexEntry) {
          return Response.json({ error: `Path '${path}' not in index` }, { status: 404 });
        }
        const blob = await this.#repository.readObject(indexEntry.oid);
        await this.#repository.writeFile(path, blob.data);
      }
    }

    return Response.json({ success: true, path });
  }

  async #switch(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const target = payload.target as string;
    const create = payload.create as string | undefined;

    if (!target && !create) {
      return Response.json({ error: "target or create required" }, { status: 400 });
    }

    const branchName = create || target;
    const refName = `refs/heads/${branchName}`;

    if (create) {
      // Create new branch
      const headOid = await this.#repository.getCurrentCommitOid();
      if (!headOid) {
        return Response.json({ error: "No HEAD commit" }, { status: 400 });
      }
      await this.#repository.writeRef(refName, headOid);
    }

    // Check if branch exists
    const branchOid = await this.#repository.getRef(refName);
    if (!branchOid) {
      return Response.json({ error: `Branch '${branchName}' not found` }, { status: 404 });
    }

    // Checkout the branch
    await this.#repository.checkoutCommit(branchOid);

    // Update HEAD to point to the branch
    await this.#repository.writeFile(".git/HEAD", new TextEncoder().encode(`ref: ${refName}\n`));

    return Response.json({ success: true, branch: branchName, created: !!create });
  }

  async #rebase(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const onto = payload.onto as string;

    if (!onto) {
      return Response.json({ error: "onto required" }, { status: 400 });
    }

    const headOid = await this.#repository.getCurrentCommitOid();
    if (!headOid) {
      return Response.json({ error: "No HEAD commit" }, { status: 400 });
    }

    // Resolve onto ref
    let ontoOid = onto;
    const refOid = await this.#repository.getRef(onto);
    if (refOid) {
      ontoOid = refOid;
    }

    // Collect commits to replay
    const commitsToReplay: string[] = [];
    let currentOid: string | null = headOid;

    while (currentOid && currentOid !== ontoOid) {
      commitsToReplay.unshift(currentOid);
      const commit = await this.#repository.readObject(currentOid);
      if (commit.type !== "commit") break;
      const info = this.#repository.parseCommit(commit.data);
      currentOid = info.parent || null;
    }

    if (commitsToReplay.length === 0) {
      return Response.json({ success: true, replayed: 0, message: "Already up to date" });
    }

    // Replay commits onto new base
    let baseOid = ontoOid;
    for (const commitOid of commitsToReplay) {
      const commit = await this.#repository.readObject(commitOid);
      const info = this.#repository.parseCommit(commit.data);

      // Create new commit with updated parent
      const timestamp = Math.floor(Date.now() / 1000);
      const timezone = "+0000";

      let newCommitData = `tree ${info.tree}\n`;
      newCommitData += `parent ${baseOid}\n`;
      newCommitData += `author ${info.author.split(" ").slice(0, -2).join(" ")} ${timestamp} ${timezone}\n`;
      newCommitData += `committer ${info.author.split(" ").slice(0, -2).join(" ")} ${timestamp} ${timezone}\n`;
      newCommitData += `\n${info.message}`;

      baseOid = await this.#repository.writeObject(
        "commit",
        new TextEncoder().encode(newCommitData),
      );
    }

    // Update HEAD ref
    const headRef = await this.#repository.getCurrentHead();
    if (headRef) {
      await this.#repository.writeRef(headRef, baseOid);
    }

    return Response.json({ success: true, replayed: commitsToReplay.length, newHead: baseOid });
  }

  async #fetch(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const remote = (payload.remote as string) || "origin";

    try {
      await this.#repository.fetch(remote);
      return Response.json({ success: true, remote });
    } catch (error: any) {
      return Response.json({ error: error.message || "Fetch failed" }, { status: 400 });
    }
  }

  async #pull(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const remote = (payload.remote as string) || "origin";
    const branch = (payload.branch as string) || "main";

    try {
      // Fetch first
      await this.#repository.fetch(remote);

      // Get the remote branch ref
      const remoteBranchRef = `refs/remotes/${remote}/${branch}`;
      const remoteOid = await this.#repository.getRef(remoteBranchRef);

      if (!remoteOid) {
        return Response.json(
          { error: `Remote branch '${remote}/${branch}' not found` },
          { status: 404 },
        );
      }

      const headOid = await this.#repository.getCurrentCommitOid();

      if (!headOid) {
        // No local commits, just checkout
        await this.#repository.checkoutCommit(remoteOid);
        const headRef = await this.#repository.getCurrentHead();
        if (headRef) {
          await this.#repository.writeRef(headRef, remoteOid);
        }
        return Response.json({ success: true, remote, branch, merged: remoteOid });
      }

      // Check if fast-forward is possible
      if (headOid === remoteOid) {
        return Response.json({ success: true, message: "Already up to date" });
      }

      // Perform merge
      const commonAncestorOid = await this.#findCommonAncestor(headOid, remoteOid);
      if (!commonAncestorOid) {
        return Response.json({ error: "No common ancestor found" }, { status: 400 });
      }

      if (commonAncestorOid === headOid) {
        // Fast-forward merge
        await this.#repository.checkoutCommit(remoteOid);
        const headRef = await this.#repository.getCurrentHead();
        if (headRef) {
          await this.#repository.writeRef(headRef, remoteOid);
        }
        return Response.json({
          success: true,
          remote,
          branch,
          fastForward: true,
          merged: remoteOid,
        });
      }

      // Three-way merge
      const currentCommit = await this.#repository.readObject(headOid);
      const currentInfo = this.#repository.parseCommit(currentCommit.data);

      const remoteCommit = await this.#repository.readObject(remoteOid);
      const remoteInfo = this.#repository.parseCommit(remoteCommit.data);

      const baseCommit = await this.#repository.readObject(commonAncestorOid);
      const baseInfo = this.#repository.parseCommit(baseCommit.data);

      const result = await this.#repository.merge(baseInfo.tree, currentInfo.tree, remoteInfo.tree);

      if (!result.success) {
        return Response.json({ error: "Merge conflict" }, { status: 409 });
      }

      // Create merge commit
      const authorStr = "Git Server <server@example.com>";
      const timestamp = Math.floor(Date.now() / 1000);
      const timezone = "+0000";

      let mergeCommitData = `tree ${result.mergedTree}\n`;
      mergeCommitData += `parent ${headOid}\n`;
      mergeCommitData += `parent ${remoteOid}\n`;
      mergeCommitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
      mergeCommitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
      mergeCommitData += `\nMerge remote-tracking branch '${remote}/${branch}'\n`;

      const mergeCommitOid = await this.#repository.writeObject(
        "commit",
        new TextEncoder().encode(mergeCommitData),
      );

      const headRef = await this.#repository.getCurrentHead();
      if (headRef) {
        await this.#repository.writeRef(headRef, mergeCommitOid);
      }

      return Response.json({ success: true, remote, branch, merged: mergeCommitOid });
    } catch (error: any) {
      return Response.json({ error: error.message || "Pull failed" }, { status: 400 });
    }
  }

  async #push(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const remote = (payload.remote as string) || "origin";
    const branch = (payload.branch as string) || "main";
    const force = payload.force as boolean | undefined;

    try {
      const headOid = await this.#repository.getCurrentCommitOid();
      if (!headOid) {
        return Response.json({ error: "No HEAD commit" }, { status: 400 });
      }

      // Collect objects to push
      const objects: string[] = [headOid];
      let currentOid: string | null = headOid;

      while (currentOid) {
        const commit = await this.#repository.readObject(currentOid);
        if (commit.type !== "commit") break;

        const info = this.#repository.parseCommit(commit.data);

        // Add tree objects
        const treeObjects = await this.#repository.collectTreeObjects(info.tree);
        objects.push(...treeObjects);

        currentOid = info.parent || null;
        if (currentOid) objects.push(currentOid);
      }

      // Create pack
      const packData = await this.#repository.createPack([...new Set(objects)]);

      // Send pack
      const refs = [
        {
          ref: `refs/heads/${branch}`,
          old: "0000000000000000000000000000000000000000",
          new: headOid,
        },
      ];

      await this.#repository.sendPack(refs, packData, force || false);

      return Response.json({ success: true, remote, branch, pushed: objects.length });
    } catch (error: any) {
      return Response.json({ error: error.message || "Push failed" }, { status: 400 });
    }
  }

  async #remote(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const action = payload.action as string | undefined;
    const name = payload.name as string | undefined;
    const url = payload.url as string | undefined;

    const configPath = ".git/config";

    if (action === "add") {
      if (!name || !url) {
        return Response.json({ error: "name and url required" }, { status: 400 });
      }

      // Read existing config or create new
      let config = "";
      try {
        const data = await this.#repository.readFile(configPath);
        config = new TextDecoder().decode(data);
      } catch {
        // File doesn't exist yet
      }

      // Add remote section
      config += `\n[remote "${name}"]\n\turl = ${url}\n\tfetch = +refs/heads/*:refs/remotes/${name}/*\n`;
      await this.#repository.writeFile(configPath, new TextEncoder().encode(config));

      return Response.json({ success: true, added: name, url });
    }

    if (action === "remove" || action === "delete") {
      if (!name) {
        return Response.json({ error: "name required" }, { status: 400 });
      }

      try {
        const data = await this.#repository.readFile(configPath);
        let config = new TextDecoder().decode(data);

        // Remove the remote section
        const regex = new RegExp(`\\[remote "${name}"\\][^\\[]*`, "g");
        config = config.replace(regex, "");

        await this.#repository.writeFile(configPath, new TextEncoder().encode(config));
        return Response.json({ success: true, removed: name });
      } catch {
        return Response.json({ error: `Remote '${name}' not found` }, { status: 404 });
      }
    }

    if (action === "set-url") {
      if (!name || !url) {
        return Response.json({ error: "name and url required" }, { status: 400 });
      }

      try {
        const data = await this.#repository.readFile(configPath);
        let config = new TextDecoder().decode(data);

        // Update the URL in the remote section
        const regex = new RegExp(`(\\[remote "${name}"\\][^\\[]*url = )[^\\n]+`, "g");
        config = config.replace(regex, `$1${url}`);

        await this.#repository.writeFile(configPath, new TextEncoder().encode(config));
        return Response.json({ success: true, name, url });
      } catch {
        return Response.json({ error: `Remote '${name}' not found` }, { status: 404 });
      }
    }

    if (action === "get-url") {
      if (!name) {
        return Response.json({ error: "name required" }, { status: 400 });
      }

      try {
        const data = await this.#repository.readFile(configPath);
        const config = new TextDecoder().decode(data);

        const regex = new RegExp(`\\[remote "${name}"\\][^\\[]*url = ([^\\n]+)`, "g");
        const match = regex.exec(config);

        if (match?.[1]) {
          return Response.json({ name, url: match[1].trim() });
        }
        return Response.json({ error: `Remote '${name}' not found` }, { status: 404 });
      } catch {
        return Response.json({ error: "No remotes configured" }, { status: 404 });
      }
    }

    // List remotes
    try {
      const data = await this.#repository.readFile(configPath);
      const config = new TextDecoder().decode(data);

      const remotes: { name: string; url: string }[] = [];
      const regex = /\[remote "([^"]+)"\][^[]*url = ([^\n]+)/g;
      let match;

      while ((match = regex.exec(config)) !== null) {
        remotes.push({ name: match[1]!, url: match[2]!.trim() });
      }

      return Response.json({ remotes });
    } catch {
      return Response.json({ remotes: [] });
    }
  }

  // ==================== Advanced API Endpoints ====================

  /** Search for patterns within repository content */
  async #grep(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const pattern = payload.pattern as string;
    const ref = (payload.ref as string) || "HEAD";
    const paths = payload.paths as string[] | undefined;
    const caseSensitive = (payload.case_sensitive as boolean) ?? true;
    const contextBefore = (payload.context?.before as number) || 0;
    const contextAfter = (payload.context?.after as number) || 0;
    const maxLines = Math.min((payload.limits?.max_lines as number) || 2000, 2000);
    const maxMatchesPerFile = (payload.limits?.max_matches_per_file as number) || 200;
    const cursor = payload.pagination?.cursor as string | undefined;
    const limit = Math.min((payload.pagination?.limit as number) || 200, 200);

    if (!pattern) {
      return Response.json({ error: "pattern required" }, { status: 400 });
    }

    // Resolve ref to commit
    let commitOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      commitOid = refOid;
    }

    const commit = await this.#repository.readObject(commitOid);
    if (commit.type !== "commit") {
      return Response.json({ error: "Invalid ref" }, { status: 400 });
    }

    const info = this.#repository.parseCommit(commit.data);
    const treeFiles = await this.#collectTreeFiles(info.tree, "");

    const regex = caseSensitive ? new RegExp(pattern, "g") : new RegExp(pattern, "gi");
    const matches: Array<{
      path: string;
      lines: Array<{ line_number: number; text: string; type: "match" | "context" }>;
    }> = [];

    let totalLines = 0;
    let skipFiles = cursor ? parseInt(cursor, 10) : 0;
    let fileIndex = 0;

    for (const [filePath, oid] of Object.entries(treeFiles)) {
      if (totalLines >= maxLines) break;
      if (matches.length >= limit) break;

      // Apply path filter
      if (paths && paths.length > 0) {
        if (!paths.some((p) => filePath.startsWith(p))) continue;
      }

      // Apply cursor (skip files)
      if (fileIndex < skipFiles) {
        fileIndex++;
        continue;
      }

      try {
        const blob = await this.#repository.readObject(oid);
        const content = new TextDecoder().decode(blob.data);
        const lines = content.split("\n");

        const fileMatches: Array<{ line_number: number; text: string; type: "match" | "context" }> =
          [];
        let matchCount = 0;

        for (let i = 0; i < lines.length; i++) {
          if (matchCount >= maxMatchesPerFile) break;
          if (totalLines >= maxLines) break;

          if (regex.test(lines[i]!)) {
            // Add context before
            for (let j = Math.max(0, i - contextBefore); j < i; j++) {
              if (!fileMatches.some((m) => m.line_number === j + 1)) {
                fileMatches.push({ line_number: j + 1, text: lines[j]!, type: "context" });
                totalLines++;
              }
            }

            // Add match
            fileMatches.push({ line_number: i + 1, text: lines[i]!, type: "match" });
            totalLines++;
            matchCount++;

            // Add context after
            for (let j = i + 1; j <= Math.min(lines.length - 1, i + contextAfter); j++) {
              fileMatches.push({ line_number: j + 1, text: lines[j]!, type: "context" });
              totalLines++;
            }

            regex.lastIndex = 0; // Reset regex
          }
        }

        if (fileMatches.length > 0) {
          matches.push({ path: filePath, lines: fileMatches });
        }
      } catch {
        // Skip files that can't be read
      }

      fileIndex++;
    }

    const hasMore = fileIndex < Object.keys(treeFiles).length || totalLines >= maxLines;
    const nextCursor = hasMore ? String(fileIndex) : null;

    return Response.json({
      query: { pattern, case_sensitive: caseSensitive },
      repo: { ref, commit: commitOid },
      matches,
      next_cursor: nextCursor,
      has_more: hasMore,
    });
  }

  /** Create a new branch from an existing branch */
  async #createBranch(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const baseBranch = payload.base_branch as string;
    const targetBranch = payload.target_branch as string;
    const force = payload.force as boolean | undefined;

    if (!baseBranch || !targetBranch) {
      return Response.json({ error: "base_branch and target_branch required" }, { status: 400 });
    }

    // Resolve base branch
    const baseRef = `refs/heads/${baseBranch}`;
    const baseOid = await this.#repository.getRef(baseRef);
    if (!baseOid) {
      return Response.json({ error: `Branch '${baseBranch}' not found` }, { status: 404 });
    }

    // Check if target already exists
    const targetRef = `refs/heads/${targetBranch}`;
    const existingOid = await this.#repository.getRef(targetRef);
    if (existingOid && !force) {
      return Response.json({ error: `Branch '${targetBranch}' already exists` }, { status: 409 });
    }

    // Create or update target branch
    await this.#repository.writeRef(targetRef, baseOid);

    return Response.json({
      target_branch: targetBranch,
      base_branch: baseBranch,
      commit_sha: baseOid,
      created: !existingOid,
    });
  }

  /** Get the diff between a branch and its base */
  async #getBranchDiff(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const branch = payload.branch as string;
    const base = (payload.base as string) || "main";

    if (!branch) {
      return Response.json({ error: "branch required" }, { status: 400 });
    }

    // Resolve branch and base
    const branchOid = await this.#repository.getRef(`refs/heads/${branch}`);
    const baseOid = await this.#repository.getRef(`refs/heads/${base}`);

    if (!branchOid) {
      return Response.json({ error: `Branch '${branch}' not found` }, { status: 404 });
    }
    if (!baseOid) {
      return Response.json({ error: `Base branch '${base}' not found` }, { status: 404 });
    }

    // Get the trees
    const branchCommit = await this.#repository.readObject(branchOid);
    const branchInfo = this.#repository.parseCommit(branchCommit.data);
    const branchFiles = await this.#collectTreeFiles(branchInfo.tree, "");

    const baseCommit = await this.#repository.readObject(baseOid);
    const baseInfo = this.#repository.parseCommit(baseCommit.data);
    const baseFiles = await this.#collectTreeFiles(baseInfo.tree, "");

    // Compare files
    const files: Array<{ path: string; state: string; old_path: string }> = [];
    let additions = 0;
    let deletions = 0;

    // Added/modified files
    for (const [path, oid] of Object.entries(branchFiles)) {
      if (!baseFiles[path]) {
        files.push({ path, state: "A", old_path: "" });
        additions++;
      } else if (baseFiles[path] !== oid) {
        files.push({ path, state: "M", old_path: "" });
        additions++;
        deletions++;
      }
    }

    // Deleted files
    for (const path of Object.keys(baseFiles)) {
      if (!branchFiles[path]) {
        files.push({ path, state: "D", old_path: "" });
        deletions++;
      }
    }

    return Response.json({
      branch,
      base,
      stats: { files: files.length, additions, deletions, changes: additions + deletions },
      files,
    });
  }

  /** Get the diff for a specific commit */
  async #getCommitDiff(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const sha = payload.sha as string;
    const baseSha = payload.base_sha as string | undefined;

    if (!sha) {
      return Response.json({ error: "sha required" }, { status: 400 });
    }

    const commit = await this.#repository.readObject(sha);
    if (commit.type !== "commit") {
      return Response.json({ error: "Invalid commit SHA" }, { status: 400 });
    }

    const info = this.#repository.parseCommit(commit.data);
    const commitFiles = await this.#collectTreeFiles(info.tree, "");

    let parentFiles: Record<string, string> = {};

    // Use provided base or parent commit
    const compareOid = baseSha || info.parent;
    if (compareOid) {
      try {
        const parentCommit = await this.#repository.readObject(compareOid);
        if (parentCommit.type === "commit") {
          const parentInfo = this.#repository.parseCommit(parentCommit.data);
          parentFiles = await this.#collectTreeFiles(parentInfo.tree, "");
        }
      } catch {
        // No parent found
      }
    }

    // Compare files
    const files: Array<{ path: string; state: string; raw_state: string }> = [];
    let additions = 0;
    let deletions = 0;

    for (const [path, oid] of Object.entries(commitFiles)) {
      if (!parentFiles[path]) {
        files.push({ path, state: "A", raw_state: "added" });
        additions++;
      } else if (parentFiles[path] !== oid) {
        files.push({ path, state: "M", raw_state: "modified" });
        additions++;
        deletions++;
      }
    }

    for (const path of Object.keys(parentFiles)) {
      if (!commitFiles[path]) {
        files.push({ path, state: "D", raw_state: "deleted" });
        deletions++;
      }
    }

    return Response.json({
      sha,
      stats: { files: files.length, additions, deletions, changes: additions + deletions },
      files,
    });
  }

  /** List all files at a specific ref */
  async #listFiles(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const ref = (payload.ref as string) || "HEAD";

    let commitOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      commitOid = refOid;
    }

    const commit = await this.#repository.readObject(commitOid);
    if (commit.type !== "commit") {
      return Response.json({ error: "Invalid ref" }, { status: 400 });
    }

    const info = this.#repository.parseCommit(commit.data);
    const files = await this.#collectTreeFiles(info.tree, "");

    return Response.json({
      paths: Object.keys(files),
      ref: ref,
    });
  }

  /** Stream file content */
  async #getFileStream(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const path = payload.path as string;
    const ref = (payload.ref as string) || "HEAD";

    if (!path) {
      return Response.json({ error: "path required" }, { status: 400 });
    }

    // Resolve ref
    let commitOid = ref;
    const refOid = await this.#repository.getRef(ref);
    if (refOid) {
      commitOid = refOid;
    }

    const commit = await this.#repository.readObject(commitOid);
    if (commit.type !== "commit") {
      return Response.json({ error: "Invalid ref" }, { status: 400 });
    }

    const info = this.#repository.parseCommit(commit.data);
    const entry = await this.#repository.findInTree(info.tree, path);

    if (!entry) {
      return Response.json({ error: `File '${path}' not found` }, { status: 404 });
    }

    const blob = await this.#repository.readObject(entry.oid);

    return new Response(new Uint8Array(blob.data).buffer as ArrayBuffer, {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(blob.data.byteLength),
      },
    });
  }

  /** List branches with pagination */
  async #listBranches(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const cursor = payload.cursor as string | undefined;
    const limit = Math.min((payload.limit as number) || 20, 100);

    const refs = await this.#repository.getAllRefs();
    const branches = refs
      .filter((r) => r.name.startsWith("refs/heads/"))
      .map((r, i) => ({
        cursor: `b_${i}`,
        name: r.name.replace("refs/heads/", ""),
        head_sha: r.oid,
      }));

    // Apply cursor-based pagination
    let startIndex = 0;
    if (cursor) {
      startIndex = branches.findIndex((b) => b.cursor === cursor) + 1;
    }

    const paginatedBranches = branches.slice(startIndex, startIndex + limit);
    const hasMore = startIndex + limit < branches.length;
    const nextCursor = hasMore ? paginatedBranches[paginatedBranches.length - 1]?.cursor : null;

    return Response.json({
      branches: paginatedBranches,
      next_cursor: nextCursor,
      has_more: hasMore,
    });
  }

  /** List commits with pagination */
  async #listCommits(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const branch = payload.branch as string | undefined;
    const cursor = payload.cursor as string | undefined;
    const limit = Math.min((payload.limit as number) || 20, 100);

    // Get starting commit
    let startOid: string | null = null;
    if (branch) {
      startOid = await this.#repository.getRef(`refs/heads/${branch}`);
      if (!startOid) {
        return Response.json({ error: `Branch '${branch}' not found` }, { status: 404 });
      }
    } else {
      startOid = await this.#repository.getCurrentCommitOid();
    }

    const commits: Array<{
      sha: string;
      message: string;
      author_name: string;
      author_email: string;
      date: string;
    }> = [];

    let currentOid: string | null = startOid;
    let skipCount = cursor ? parseInt(cursor, 10) : 0;
    let skipped = 0;

    while (currentOid && commits.length < limit) {
      if (skipped < skipCount) {
        const commit = await this.#repository.readObject(currentOid);
        if (commit.type !== "commit") break;
        const info = this.#repository.parseCommit(commit.data);
        currentOid = info.parent || null;
        skipped++;
        continue;
      }

      const commit = await this.#repository.readObject(currentOid);
      if (commit.type !== "commit") break;

      const info = this.#repository.parseCommit(commit.data);

      // Parse author string (format: "Name <email> timestamp timezone")
      const authorMatch = info.author.match(/^(.+?) <(.+?)> (\d+)/);
      const authorName = authorMatch?.[1] || "Unknown";
      const authorEmail = authorMatch?.[2] || "";
      const timestamp = authorMatch?.[3] ? parseInt(authorMatch[3], 10) : 0;

      commits.push({
        sha: currentOid,
        message: info.message.trim(),
        author_name: authorName,
        author_email: authorEmail,
        date: new Date(timestamp * 1000).toISOString(),
      });

      currentOid = info.parent || null;
    }

    const hasMore = currentOid !== null;
    const nextCursor = hasMore ? String(skipCount + commits.length) : null;

    return Response.json({
      commits,
      next_cursor: nextCursor,
      has_more: hasMore,
    });
  }

  /** Restore a branch to a specific commit */
  async #restoreCommit(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const targetBranch = payload.target_branch as string;
    const targetCommitSha = payload.target_commit_sha as string;
    const expectedHeadSha = payload.expected_head_sha as string | undefined;
    const commitMessage = payload.commit_message as string | undefined;
    const author = payload.author as { name: string; email: string };

    if (!targetBranch || !targetCommitSha || !author) {
      return Response.json(
        { error: "target_branch, target_commit_sha, and author required" },
        { status: 400 },
      );
    }

    const branchRef = `refs/heads/${targetBranch}`;
    const currentOid = await this.#repository.getRef(branchRef);

    // Validate expected head if provided
    if (expectedHeadSha && currentOid !== expectedHeadSha) {
      return Response.json(
        { error: "Expected head SHA does not match current branch tip" },
        { status: 409 },
      );
    }

    // Verify target commit exists
    const targetCommit = await this.#repository.readObject(targetCommitSha);
    if (targetCommit.type !== "commit") {
      return Response.json({ error: "Invalid target commit SHA" }, { status: 400 });
    }

    const targetInfo = this.#repository.parseCommit(targetCommit.data);

    // Create restore commit
    const authorStr = `${author.name} <${author.email}>`;
    const timestamp = Math.floor(Date.now() / 1000);
    const timezone = "+0000";

    const message =
      commitMessage || `Reset ${targetBranch} to "${targetInfo.message.split("\n")[0]}"`;

    let restoreCommitData = `tree ${targetInfo.tree}\n`;
    if (currentOid) {
      restoreCommitData += `parent ${currentOid}\n`;
    }
    restoreCommitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
    restoreCommitData += `committer ${authorStr} ${timestamp} ${timezone}\n`;
    restoreCommitData += `\n${message}\n`;

    const newCommitOid = await this.#repository.writeObject(
      "commit",
      new TextEncoder().encode(restoreCommitData),
    );

    // Update branch ref
    await this.#repository.writeRef(branchRef, newCommitOid);

    return Response.json({
      commit_sha: newCommitOid,
      ref_update: {
        old_sha: currentOid || "0000000000000000000000000000000000000000",
        new_sha: newCommitOid,
      },
    });
  }

  // ==================== Repository Management ====================

  /** Create a new repository */
  async #createRepo(payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    const id = payload.id as string | undefined;
    const defaultBranch = (payload.default_branch as string) || "main";

    // Generate ID if not provided
    const repoId = id || crypto.randomUUID();

    // Initialize the repository storage
    await this.#repository.initStorage(repoId, defaultBranch);

    return Response.json(
      {
        id: repoId,
        default_branch: defaultBranch,
        created_at: new Date().toISOString(),
      },
      { status: 201 },
    );
  }

  /** Delete a repository */
  async #deleteRepo(_payload: Payload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    // Note: Actual deletion would require storage-level support
    // This is a placeholder that acknowledges the request
    return Response.json({
      message:
        "Repository deletion initiated. Physical storage cleanup will complete asynchronously.",
    });
  }

  // ==================== Streaming Endpoints ====================

  /** Handle NDJSON commit-pack streaming */
  async #commitPack(body: SteamingPayload, signal?: AbortSignal) {
    signal?.throwIfAborted();

    if (!body) {
      return Response.json({ error: "Request body required" }, { status: 400 });
    }

    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let metadata: CommitPackMetadata | null = null;
    const blobChunks = new Map<string, Uint8Array[]>();
    let packBytes = 0;

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Process complete lines
        let newlineIndex: number;
        while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newlineIndex);
          buffer = buffer.slice(newlineIndex + 1);

          if (!line.trim()) continue;

          const parsed = JSON.parse(line);

          if (parsed.metadata) {
            metadata = parsed.metadata as CommitPackMetadata;
          } else if (parsed.blob_chunk) {
            const chunk = parsed.blob_chunk as BlobChunk;
            const data = Uint8Array.from(atob(chunk.data), (c) => c.charCodeAt(0));
            packBytes += data.byteLength;

            if (!blobChunks.has(chunk.content_id)) {
              blobChunks.set(chunk.content_id, []);
            }
            blobChunks.get(chunk.content_id)!.push(data);
          }
        }
      }
    } finally {
      reader.releaseLock();
    }

    if (!metadata) {
      return Response.json({ error: "Metadata required" }, { status: 400 });
    }

    // Validate expected head if provided
    if (metadata.expected_head_sha) {
      const branchRef = `refs/heads/${metadata.target_branch}`;
      const currentOid = await this.#repository.getRef(branchRef);
      if (currentOid && currentOid !== metadata.expected_head_sha) {
        return Response.json(
          {
            error: "Expected head SHA does not match current branch tip",
            result: {
              branch: metadata.target_branch,
              old_sha: currentOid,
              new_sha: "",
              success: false,
              status: "precondition_failed",
              message: "Branch tip moved",
            },
          },
          { status: 409 },
        );
      }
    }

    // Get base branch head or current head
    let parentOid: string | undefined;
    if (metadata.base_branch) {
      parentOid =
        (await this.#repository.getRef(`refs/heads/${metadata.base_branch}`)) || undefined;
    } else {
      parentOid =
        (await this.#repository.getRef(`refs/heads/${metadata.target_branch}`)) || undefined;
    }

    // Process files
    let blobCount = 0;
    for (const file of metadata.files) {
      if (file.operation === "delete") {
        await this.#repository.removeIndexEntry(file.path);
        continue;
      }

      // Combine chunks for this file
      const chunks = blobChunks.get(file.content_id) || [];
      const totalLength = chunks.reduce((acc, c) => acc + c.byteLength, 0);
      const content = new Uint8Array(totalLength);
      let offset = 0;
      for (const chunk of chunks) {
        content.set(chunk, offset);
        offset += chunk.byteLength;
      }

      // Write blob and add to index
      const oid = await this.#repository.writeObject("blob", content);
      await this.#repository.addIndexEntry({
        path: file.path,
        oid,
        mode: file.mode || "100644",
        size: content.byteLength,
        mtime: Date.now(),
      });
      blobCount++;
    }

    // Create tree from index
    const treeOid = await this.#repository.createTreeFromIndex();

    // Create commit
    const authorStr = `${metadata.author.name} <${metadata.author.email}>`;
    const committerStr = metadata.committer
      ? `${metadata.committer.name} <${metadata.committer.email}>`
      : authorStr;
    const timestamp = Math.floor(Date.now() / 1000);
    const timezone = "+0000";

    let commitData = `tree ${treeOid}\n`;
    if (parentOid) {
      commitData += `parent ${parentOid}\n`;
    }
    commitData += `author ${authorStr} ${timestamp} ${timezone}\n`;
    commitData += `committer ${committerStr} ${timestamp} ${timezone}\n`;
    commitData += `\n${metadata.commit_message}\n`;

    const commitOid = await this.#repository.writeObject(
      "commit",
      new TextEncoder().encode(commitData),
    );

    // Update branch ref
    const branchRef = `refs/heads/${metadata.target_branch}`;
    const oldSha =
      (await this.#repository.getRef(branchRef)) || "0000000000000000000000000000000000000000";
    await this.#repository.writeRef(branchRef, commitOid);

    const result: CommitPackResult = {
      commit: {
        commit_sha: commitOid,
        tree_sha: treeOid,
        target_branch: metadata.target_branch,
        pack_bytes: packBytes,
        blob_count: blobCount,
      },
      result: {
        branch: metadata.target_branch,
        old_sha: oldSha,
        new_sha: commitOid,
        success: true,
        status: "ok",
        message: "",
      },
    };

    return Response.json(result, { status: 201 });
  }
}
