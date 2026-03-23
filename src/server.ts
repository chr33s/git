import { DurableObject } from "cloudflare:workers";

import { HookRunner } from "./git.hooks.ts";
import { GitRepository } from "./git.repository.ts";
import { ServerApi } from "./server.api.ts";
import { ServerLfs } from "./server.lfs.ts";
import { CloudflareStorage as Storage } from "./server.storage.ts";
import { concatenateUint8Arrays } from "./git.utils.ts";
import { ServerWebhooks } from "./server.webhooks.ts";

interface Route {
  handler: (request: Request) => Promise<Response>;
  method: string;
  pathname: string;
  search?: string;
}

export class Server extends DurableObject<Env> {
  #api: ServerApi;
  #hooks: HookRunner = new HookRunner();
  #lfs: ServerLfs;
  #repository: GitRepository;
  #webhooks: ServerWebhooks;
  #routes: Route[] = [
    {
      handler: (req) => this.#head(req),
      method: "GET",
      pathname: "/:repo{.git}?/HEAD",
    },
    {
      handler: (req) => this.#infoRefs(req),
      method: "GET",
      pathname: "/:repo{.git}?/info/refs",
      search: "?service=:service(git-upload-pack|git-receive-pack)",
    },
    {
      handler: (req) => this.#receivePack(req),
      method: "POST",
      pathname: "/:repo{.git}?/git-receive-pack",
    },
    {
      handler: (req) => this.#uploadPack(req),
      method: "POST",
      pathname: "/:repo{.git}?/git-upload-pack",
    },
    {
      handler: (req) => this.#lfsBatch(req),
      method: "POST",
      pathname: "/:repo{.git}?/info/lfs/objects/batch",
    },
    {
      handler: (req) => this.#lfsUpload(req),
      method: "PUT",
      pathname: "/:repo{.git}?/info/lfs/objects/:oid",
    },
    {
      handler: (req) => this.#lfsDownload(req),
      method: "GET",
      pathname: "/:repo{.git}?/info/lfs/objects/:oid",
    },
  ];
  #urlPattern: URLPatternResult | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    const storage = new Storage(ctx, env);
    const config = { repoName: ctx.id.toString() };
    this.#repository = new GitRepository(storage, config);
    this.#lfs = new ServerLfs({
      hasObject: (repo, oid) => storage.hasLfsObject(oid),
      getObjectSize: (repo, oid) => storage.getLfsObjectSize(oid),
      putObjectMeta: (repo, oid, size) => storage.putLfsObjectMeta(oid, size),
      deleteObjectMeta: (repo, oid) => storage.deleteLfsObjectMeta(oid),
      getR2: () => storage.getR2(),
    });
    this.#webhooks = new ServerWebhooks(storage);
    this.#api = new ServerApi(this.#repository, this.#webhooks);

    // Wire webhooks as a post-receive hook
    this.#hooks.register("post-receive", async (ctx) => {
      await this.#webhooks.deliver(ctx.repository, ctx.updates, async (oid) => {
        try {
          const obj = await this.#repository.readObject(oid);
          if (obj.type !== "commit") return null;
          const info = this.#repository.parseCommit(obj.data);
          return { id: oid, message: info.message, author: info.author };
        } catch {
          return null;
        }
      });
      return { ok: true };
    });
  }

  /** Access the hook runner to register hooks externally. */
  get hooks(): HookRunner {
    return this.#hooks;
  }

  async fetch(request: Request) {
    try {
      request.signal?.throwIfAborted();

      const url = new URL(request.url);

      if (url.pathname.startsWith("/api")) {
        const apiRepo = url.pathname.split("/")[2]?.replace(/\.git$/, "");

        await this.#repository.init();
        if (apiRepo) {
          await this.#repository.initStorage(apiRepo);
        }

        return this.#api.fetch(
          {
            body: request.body,
            method: request.method,
            url: request.url,
          },
          request.signal,
        );
      }

      await this.#repository.init();

      for (const route of this.#routes) {
        if (route.method !== request.method) continue;

        const pattern = new URLPattern({
          pathname: route.pathname,
          search: route.search,
        });
        if (!pattern.test(request.url)) continue;
        this.#urlPattern = pattern.exec(request.url);

        const repo = this.#urlPattern?.pathname.groups.repo!;
        await this.#repository.initStorage(repo);

        return await route.handler(request);
      }

      return Response.json({ message: "Not Found" }, { status: 404 });
    } catch (error: any) {
      if (error.name === "AbortError") {
        console.info("Request aborted:", error.message);
        return new Response(null, { status: 499 });
      }
      throw error;
    }
  }

  async #head(request: Request) {
    request.signal?.throwIfAborted();

    // Get the symbolic ref that HEAD points to (e.g., refs/heads/main)
    const symbolicRef = await this.#repository.getCurrentHead();
    if (symbolicRef) {
      return new Response(`ref: ${symbolicRef}\n`, {
        headers: { "Content-Type": "text/plain" },
      });
    }

    // Check if HEAD is detached (points directly to a commit)
    const headOid = await this.#repository.getCurrentCommitOid();
    if (headOid) {
      return new Response(`${headOid}\n`, {
        headers: { "Content-Type": "text/plain" },
      });
    }

    // Default for empty repository
    return new Response("ref: refs/heads/main\n", {
      headers: { "Content-Type": "text/plain" },
    });
  }

  async #infoRefs(request: Request) {
    request.signal?.throwIfAborted();

    const service = this.#urlPattern?.search.groups.service || "git-upload-pack";
    const isV2 = request.headers.get("git-protocol")?.includes("version=2");

    if (isV2 && service === "git-upload-pack") return this.#infoRefsV2();

    // Build response with pkt-line protocol
    const lines: string[] = [];

    // Service announcement
    lines.push(this.#pktLine(`# service=${service}`));
    lines.push("0000"); // Flush packet

    // Get all refs
    const allRefs = await this.#repository.getAllRefs();
    const symbolicHead = await this.#repository.getCurrentHead();
    const headOid = await this.#repository.getCurrentCommitOid();

    // Capabilities to advertise
    const capabilities =
      service === "git-receive-pack"
        ? "report-status delete-refs ofs-delta atomic"
        : [
            "multi_ack_detailed",
            "side-band-64k",
            "thin-pack",
            "ofs-delta",
            "shallow",
            symbolicHead ? `symref=HEAD:${symbolicHead}` : null,
          ]
            .filter((capability): capability is string => capability !== null)
            .join(" ");

    // Add HEAD if it exists
    let firstLine = true;

    if (headOid) {
      lines.push(this.#pktLine(`${headOid} HEAD\0${capabilities}\n`));
      firstLine = false;
    }

    // Add other refs
    for (const ref of allRefs) {
      if (ref.oid && ref.name !== "HEAD") {
        if (firstLine) {
          lines.push(this.#pktLine(`${ref.oid} ${ref.name}\0${capabilities}\n`));
          firstLine = false;
        } else {
          lines.push(this.#pktLine(`${ref.oid} ${ref.name}\n`));
        }
      }
    }

    // For empty repos, send zero-id with capabilities (required for git-receive-pack)
    if (firstLine && service === "git-receive-pack") {
      const zeroId = "0000000000000000000000000000000000000000";
      lines.push(this.#pktLine(`${zeroId} capabilities^{}\0${capabilities}\n`));
    }

    // Final flush packet
    lines.push("0000");

    const response = lines.join("");
    return new Response(response, {
      headers: {
        "Content-Type": `application/x-${service}-advertisement`,
      },
    });
  }

  #pktLine(text: string) {
    const length = text.length + 4;
    return length.toString(16).padStart(4, "0") + text;
  }

  async #receivePack(request: Request) {
    const body = request.body;
    const signal = request.signal;
    if (!body) {
      return new Response("No body", { status: 400 });
    }

    let reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>> | null = null;
    try {
      signal?.throwIfAborted();

      // Read all data from stream
      reader = body.getReader();
      const chunks: Uint8Array[] = [];
      let result = await reader.read();

      while (!result.done) {
        signal?.throwIfAborted();
        chunks.push(new Uint8Array(result.value));
        result = await reader.read();
      }

      reader.releaseLock();

      const fullData = new Uint8Array(chunks.reduce((acc, chunk) => acc + chunk.length, 0));
      let offset = 0;
      for (const chunk of chunks) {
        fullData.set(chunk, offset);
        offset += chunk.length;
      }

      // Parse ref updates and pack data
      let idx = 0;
      const capabilities = new Set<string>();
      const refUpdates: Array<{ ref: string; old: string; new: string }> = [];

      // Read ref update commands (format: "old new ref\0")
      while (idx < fullData.length) {
        signal?.throwIfAborted();
        const line = this.#readPktLine(fullData, idx);
        if (line === null) break;

        idx = line.nextIdx;

        if (line.data === "0000") {
          // End of commands, start of pack data
          break;
        }

        // Parse "old new ref" format
        const text = new TextDecoder().decode(line.content);
        const parts = text.split("\0");
        const refUpdate = parts[0]?.trim();
        const advertisedCapabilities = parts[1]?.trim();

        if (advertisedCapabilities) {
          for (const capability of advertisedCapabilities.split(/\s+/)) {
            if (capability) {
              capabilities.add(capability);
            }
          }
        }

        if (refUpdate) {
          const match = refUpdate.match(/^([0-9a-f]{40}) ([0-9a-f]{40}) (.+)$/);
          if (match && match[1] && match[2] && match[3]) {
            refUpdates.push({
              old: match[1],
              new: match[2],
              ref: match[3],
            });
          }
        }
      }

      // Parse and apply pack data
      const packData = this.#extractReceivePackData(fullData.slice(idx), capabilities);
      if (packData.length > 0) {
        signal?.throwIfAborted();
        const packStream = new ReadableStream({
          start(controller) {
            controller.enqueue(packData);
            controller.close();
          },
        });
        try {
          await this.#repository.parsePack(packStream);
        } catch (e) {
          const msg = e instanceof Error ? e.message : "Unknown error";
          const response =
            this.#pktLine(`unpack ${msg}\n`) +
            refUpdates.map((u) => this.#pktLine(`ng ${u.ref} unpack failed\n`)).join("") +
            "0000";
          return new Response(response, {
            headers: { "Content-Type": "application/x-git-receive-pack-result" },
          });
        }
      }

      const repo = this.#urlPattern?.pathname.groups.repo!;
      const ZERO = "0000000000000000000000000000000000000000";
      const hookUpdates = refUpdates.map((u) => ({
        ref: u.ref,
        oldOid: u.old,
        newOid: u.new,
      }));
      const hookContext = {
        repository: repo,
        updates: hookUpdates,
        capabilities,
      };

      // Run pre-receive hook — can reject the entire push
      const preResult = await this.#hooks.runPreReceive(hookContext);
      if (!preResult.ok) {
        const msg = preResult.message || "pre-receive hook declined";
        const response =
          this.#pktLine("unpack ok\n") +
          refUpdates.map((u) => this.#pktLine(`ng ${u.ref} ${msg}\n`)).join("") +
          "0000";
        return new Response(response, {
          headers: { "Content-Type": "application/x-git-receive-pack-result" },
        });
      }

      // Run per-ref update hooks
      const updateHookResults = await this.#hooks.runUpdate(hookContext);

      // Update refs (skip refs rejected by update hooks)
      const acceptedUpdates = refUpdates.filter((u) => {
        const hookResult = updateHookResults.get(u.ref);
        return !hookResult || hookResult.ok;
      });

      const updateResults = await this.#repository.updateRefs(
        acceptedUpdates.map((update) => ({
          message: "push",
          new: update.new === ZERO ? null : update.new,
          old: update.old === ZERO ? null : update.old,
          ref: update.ref,
        })),
        {
          atomic: capabilities.has("atomic"),
          compareOldOid: true,
        },
      );

      // Build per-ref result lines
      const updateResultMap = new Map(
        updateResults.map((r: { ok: boolean; ref: string; error?: string }) => [r.ref, r]),
      );
      const refResults = refUpdates.map((u) => {
        const hookResult = updateHookResults.get(u.ref);
        if (hookResult && !hookResult.ok) {
          return this.#pktLine(`ng ${u.ref} ${hookResult.message || "update hook declined"}\n`);
        }
        const result = updateResultMap.get(u.ref);
        if (!result) return this.#pktLine(`ng ${u.ref} update rejected\n`);
        return result.ok
          ? this.#pktLine(`ok ${u.ref}\n`)
          : this.#pktLine(`ng ${u.ref} ${result.error || "update rejected"}\n`);
      });

      // Send success response (unpack ok + ref status)
      const response = this.#pktLine("unpack ok\n") + refResults.join("") + "0000";

      // Run post-receive hooks (fire-and-forget, errors don't affect response)
      const successfulUpdates = refUpdates.filter((u) => {
        const hookResult = updateHookResults.get(u.ref);
        if (hookResult && !hookResult.ok) return false;
        const result = updateResultMap.get(u.ref);
        return result?.ok;
      });
      if (successfulUpdates.length > 0) {
        const postContext = {
          repository: repo,
          updates: successfulUpdates.map((u) => ({
            ref: u.ref,
            oldOid: u.old,
            newOid: u.new,
          })),
          capabilities,
        };
        // Don't await — post-receive is best-effort
        this.#hooks.runPostReceive(postContext).catch(() => {});
      }

      return new Response(response, {
        headers: { "Content-Type": "application/x-git-receive-pack-result" },
      });
    } catch (e) {
      if (reader) reader.releaseLock();
      if (e instanceof Error && e.name === "AbortError") throw e;

      const error = e instanceof Error ? e.message : "Unknown error";
      console.error("receive-pack error:", error);
      const response = this.#pktLine(`unpack ${error}\n`) + "0000";
      return new Response(response, {
        headers: { "Content-Type": "application/x-git-receive-pack-result" },
      });
    }
  }

  async #uploadPack(request: Request) {
    const isV2 = request.headers.get("git-protocol")?.includes("version=2");
    if (isV2) {
      return this.#uploadPackV2(request);
    }

    const body = request.body;
    const signal = request.signal;
    if (!body) {
      return new Response("No body", { status: 400 });
    }

    let reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>> | null = null;
    try {
      signal?.throwIfAborted();

      // Read all data from stream
      reader = body.getReader();
      const chunks: Uint8Array[] = [];
      let result = await reader.read();

      while (!result.done) {
        signal?.throwIfAborted();
        chunks.push(new Uint8Array(result.value));
        result = await reader.read();
      }

      reader.releaseLock();

      const fullData = new Uint8Array(chunks.reduce((acc, chunk) => acc + chunk.length, 0));
      let offset = 0;
      for (const chunk of chunks) {
        fullData.set(chunk, offset);
        offset += chunk.length;
      }

      // Parse want/shallow/deepen/have/done commands
      let idx = 0;
      const wants: string[] = [];
      const haves: string[] = [];
      const clientShallows = new Set<string>();
      let deepen = 0;
      let deepenSince = 0;
      let deepenNot: string | null = null;

      while (idx < fullData.length) {
        signal?.throwIfAborted();
        const line = this.#readPktLine(fullData, idx);
        if (line === null) break;

        idx = line.nextIdx;

        if (line.data === "0000") {
          // Flush packet — separates want/shallow/deepen from have section
          continue;
        }

        const text = new TextDecoder().decode(line.content).trim();

        if (text.startsWith("want ")) {
          const oidMatch = text.substring(5).match(/^([0-9a-f]{40})/);
          if (oidMatch && oidMatch[1]) {
            wants.push(oidMatch[1]);
          }
        } else if (text.startsWith("shallow ")) {
          const oidMatch = text.substring(8).match(/^([0-9a-f]{40})/);
          if (oidMatch && oidMatch[1]) {
            clientShallows.add(oidMatch[1]);
          }
        } else if (text.startsWith("deepen ")) {
          deepen = parseInt(text.substring(7), 10);
        } else if (text.startsWith("deepen-since ")) {
          deepenSince = parseInt(text.substring(13), 10);
        } else if (text.startsWith("deepen-not ")) {
          deepenNot = text.substring(11);
        } else if (text.startsWith("have ")) {
          const oidMatch = text.substring(5).match(/^([0-9a-f]{40})/);
          if (oidMatch && oidMatch[1]) {
            haves.push(oidMatch[1]);
          }
        } else if (text === "done") {
          break;
        }
      }

      if (wants.length === 0) {
        return new Response("0000", {
          headers: { "Content-Type": "application/x-git-upload-pack-result" },
        });
      }

      const { objectsToSend, newShallows, unshallows } = await this.#collectObjects(
        wants,
        haves,
        clientShallows,
        deepen,
        deepenSince,
        deepenNot,
        signal,
      );

      signal?.throwIfAborted();

      const packData = await this.#repository.createPack(Array.from(objectsToSend));

      // Build response: shallow-update flush, then NAK, then sideband pack data
      const preambleLines: string[] = [];

      for (const oid of newShallows) {
        preambleLines.push(this.#pktLine(`shallow ${oid}\n`));
      }
      for (const oid of unshallows) {
        preambleLines.push(this.#pktLine(`unshallow ${oid}\n`));
      }

      if (newShallows.size > 0 || unshallows.size > 0) {
        preambleLines.push("0000");
      }

      const nakLine = this.#pktLine("NAK\n");
      preambleLines.push(nakLine);

      // Wrap pack data in sideband channel 1 packets
      const sidebandPackets = this.#sidebandPackets(1, packData);

      // Progress message on channel 2
      const progressMsg = `Enumerating objects: ${objectsToSend.size}, done.\n`;
      const progressPacket = this.#sidebandPacket(2, new TextEncoder().encode(progressMsg));

      const flushPacket = new TextEncoder().encode("0000");

      const preambleBytes = new TextEncoder().encode(preambleLines.join(""));
      const totalSize =
        preambleBytes.length +
        progressPacket.length +
        sidebandPackets.reduce((sum, p) => sum + p.length, 0) +
        flushPacket.length;

      const response = new Uint8Array(totalSize);
      let respOffset = 0;
      response.set(preambleBytes, respOffset);
      respOffset += preambleBytes.length;
      response.set(progressPacket, respOffset);
      respOffset += progressPacket.length;
      for (const packet of sidebandPackets) {
        response.set(packet, respOffset);
        respOffset += packet.length;
      }
      response.set(flushPacket, respOffset);

      return new Response(response, {
        headers: { "Content-Type": "application/x-git-upload-pack-result" },
      });
    } catch (e) {
      if (reader) reader.releaseLock();
      if (e instanceof Error && e.name === "AbortError") throw e;

      const error = e instanceof Error ? e.message : "Unknown error";
      console.error("upload-pack error:", error);

      // Send error via sideband channel 3 so the client can display it
      const errPacket = this.#sidebandPacket(3, new TextEncoder().encode(`ERR ${error}\n`));
      const nakBytes = new TextEncoder().encode(this.#pktLine("NAK\n"));
      const flush = new TextEncoder().encode("0000");
      const errResp = new Uint8Array(nakBytes.length + errPacket.length + flush.length);
      errResp.set(nakBytes, 0);
      errResp.set(errPacket, nakBytes.length);
      errResp.set(flush, nakBytes.length + errPacket.length);

      return new Response(errResp, {
        headers: { "Content-Type": "application/x-git-upload-pack-result" },
      });
    }
  }

  async #collectObjects(
    wants: string[],
    haves: string[],
    clientShallows: Set<string>,
    deepen: number,
    deepenSince: number,
    deepenNot: string | null,
    signal?: AbortSignal,
  ) {
    const isShallowRequest = deepen > 0 || deepenSince > 0 || deepenNot !== null;
    const serverShallows = await this.#repository.getShallowCommits();

    const objectsToSend = new Set<string>();
    const processedCommits = new Set<string>();
    const newShallows = new Set<string>();
    const unshallows = new Set<string>();
    const commitsToProcess: Array<[string, number]> = wants.map((w) => [w, 1]);

    let deepenNotOid: string | null = null;
    if (deepenNot) {
      deepenNotOid = await this.#repository.getRef(
        deepenNot.startsWith("refs/") ? deepenNot : `refs/heads/${deepenNot}`,
      );
    }

    while (commitsToProcess.length > 0) {
      signal?.throwIfAborted();
      const entry = commitsToProcess.pop();
      if (!entry) continue;
      const [commitOid, depth] = entry;

      if (processedCommits.has(commitOid)) continue;

      processedCommits.add(commitOid);

      try {
        const commit = await this.#repository.readObject(commitOid);
        if (commit.type === "commit") {
          const clientHasCommit = haves.includes(commitOid);
          const clientHasShallowCommit = clientShallows.has(commitOid);
          const commitInfo = this.#repository.parseCommit(commit.data);

          if (!clientHasCommit) {
            objectsToSend.add(commitOid);
            const treeObjects = await this.#repository.collectTreeObjects(commitInfo.tree);
            treeObjects.forEach((oid) => objectsToSend.add(oid));
          }

          // During deepen/unshallow requests, the client already has commits from
          // the tip down to its current shallow boundary. Keep walking those
          // commits without adding them to the pack until we reach a boundary.
          if (isShallowRequest && clientHasCommit && !clientHasShallowCommit) {
            for (const parentOid of commitInfo.parents) {
              commitsToProcess.push([parentOid, depth]);
            }
            continue;
          }

          if (isShallowRequest && clientHasShallowCommit) {
            unshallows.add(commitOid);
            for (const parentOid of commitInfo.parents) {
              commitsToProcess.push([parentOid, 1]);
            }
            continue;
          }

          let shouldWalkParents = true;

          if (deepen > 0 && depth >= deepen) {
            shouldWalkParents = false;
            newShallows.add(commitOid);
          }
          if (deepenSince > 0 && commitInfo.author) {
            const tsMatch = commitInfo.author.match(/\s(\d+)\s[+-]\d{4}$/);
            if (tsMatch && parseInt(tsMatch[1]!, 10) < deepenSince) {
              shouldWalkParents = false;
              newShallows.add(commitOid);
            }
          }
          if (deepenNotOid && commitOid === deepenNotOid) {
            shouldWalkParents = false;
            newShallows.add(commitOid);
          }

          if (shouldWalkParents) {
            if (clientShallows.has(commitOid)) unshallows.add(commitOid);
            for (const parentOid of commitInfo.parents) {
              commitsToProcess.push([parentOid, depth + 1]);
            }
          }
        }
      } catch {
        continue;
      }
    }

    if (isShallowRequest) {
      for (const oid of unshallows) serverShallows.delete(oid);
      for (const oid of newShallows) serverShallows.add(oid);
      await this.#repository.setShallowCommits(serverShallows);
    }

    return { objectsToSend, newShallows, unshallows };
  }

  /** Wrap data in a single sideband pkt-line for the given channel (1=pack, 2=progress, 3=error) */
  #sidebandPacket(channel: 1 | 2 | 3, data: Uint8Array) {
    const packetLen = data.length + 5; // 4 length bytes + 1 channel byte + data
    const lenStr = packetLen.toString(16).padStart(4, "0");
    const packet = new Uint8Array(packetLen);
    packet.set(new TextEncoder().encode(lenStr), 0);
    packet[4] = channel;
    packet.set(data, 5);
    return packet;
  }

  /** Split data into max-size sideband packets for the given channel */
  #sidebandPackets(channel: 1 | 2 | 3, data: Uint8Array) {
    const maxChunk = 65515; // 65520 - 5 (4 len + 1 channel)
    const packets: Uint8Array[] = [];
    for (let i = 0; i < data.length; i += maxChunk) {
      packets.push(
        this.#sidebandPacket(channel, data.slice(i, Math.min(i + maxChunk, data.length))),
      );
    }
    return packets;
  }

  #infoRefsV2() {
    // Protocol v2 capability advertisement
    const lines: string[] = [];
    lines.push(this.#pktLine("version 2\n"));
    lines.push(this.#pktLine("agent=chr33s-git/0\n"));
    lines.push(this.#pktLine("ls-refs=unborn\n"));
    lines.push(this.#pktLine("fetch=shallow\n"));
    lines.push(this.#pktLine("object-format=sha1\n"));
    lines.push("0000");
    return new Response(lines.join(""), {
      headers: {
        "Content-Type": "application/x-git-upload-pack-advertisement",
      },
    });
  }

  async #uploadPackV2(request: Request) {
    const body = request.body;
    const signal = request.signal;
    if (!body) {
      return new Response("No body", { status: 400 });
    }

    let reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>> | null = null;
    try {
      signal?.throwIfAborted();

      reader = body.getReader();
      const chunks: Uint8Array[] = [];
      let result = await reader.read();
      while (!result.done) {
        signal?.throwIfAborted();
        chunks.push(new Uint8Array(result.value));
        result = await reader.read();
      }
      reader.releaseLock();
      reader = null;

      const fullData = new Uint8Array(chunks.reduce((acc, c) => acc + c.length, 0));
      let off = 0;
      for (const c of chunks) {
        fullData.set(c, off);
        off += c.length;
      }

      // First pkt-line is the command
      let idx = 0;
      const cmdLine = this.#readPktLine(fullData, idx);
      if (!cmdLine) {
        return new Response("0000", {
          headers: { "Content-Type": "application/x-git-upload-pack-result" },
        });
      }
      idx = cmdLine.nextIdx;
      const command = new TextDecoder()
        .decode(cmdLine.content)
        .trim()
        .replace(/^command=/, "");

      if (command === "ls-refs") {
        return this.#lsRefs(fullData, idx, signal);
      } else if (command === "fetch") {
        return this.#fetchV2(fullData, idx, signal);
      }

      return Response.json({ message: "Unknown command" }, { status: 400 });
    } catch (e) {
      if (reader) reader.releaseLock();
      if (e instanceof Error && e.name === "AbortError") throw e;

      const error = e instanceof Error ? e.message : "Unknown error";
      console.error("upload-pack-v2 error:", error);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  async #lsRefs(data: Uint8Array, startIdx: number, signal?: AbortSignal) {
    signal?.throwIfAborted();

    // Parse ls-refs arguments after the delimiter packet (0001)
    let idx = startIdx;
    const refPrefixes: string[] = [];
    let wantSymrefs = false;
    let wantPeel = false;

    while (idx < data.length) {
      const line = this.#readPktLine(data, idx);
      if (!line) break;
      idx = line.nextIdx;

      if (line.data === "0000") break; // flush
      if (line.data === "0001") continue; // delimiter — args follow

      const text = new TextDecoder().decode(line.content).trim();
      if (text.startsWith("ref-prefix ")) {
        refPrefixes.push(text.substring(11));
      } else if (text === "symrefs") {
        wantSymrefs = true;
      } else if (text === "peel") {
        wantPeel = true;
      }
    }

    const allRefs = await this.#repository.getAllRefs();
    const headOid = await this.#repository.getCurrentCommitOid();
    const symbolicHead = await this.#repository.getCurrentHead();

    const lines: string[] = [];

    // Helper to check if a ref name matches any prefix filter
    const matchesPrefix = (name: string) =>
      refPrefixes.length === 0 || refPrefixes.some((p) => name.startsWith(p));

    // Always include HEAD. The protocol allows returning extra refs beyond the
    // requested prefixes, and single-branch clones rely on HEAD to resolve the
    // default branch.
    if (headOid) {
      let line = `${headOid} HEAD`;
      if (wantSymrefs && symbolicHead) {
        line += ` symref-target:${symbolicHead}`;
      }
      lines.push(this.#pktLine(`${line}\n`));
    }

    for (const ref of allRefs) {
      if (ref.oid && ref.name !== "HEAD" && matchesPrefix(ref.name)) {
        let line = `${ref.oid} ${ref.name}`;
        if (wantPeel) {
          // For non-tag refs, peeled value is the same OID
          line += ` peeled:${ref.oid}`;
        }
        lines.push(this.#pktLine(`${line}\n`));
      }
    }

    lines.push("0000");
    return new Response(lines.join(""), {
      headers: { "Content-Type": "application/x-git-upload-pack-result" },
    });
  }

  async #fetchV2(data: Uint8Array, startIdx: number, signal?: AbortSignal) {
    signal?.throwIfAborted();

    // Parse fetch arguments
    let idx = startIdx;
    const wants: string[] = [];
    const haves: string[] = [];
    const clientShallows = new Set<string>();
    let deepen = 0;
    let deepenSince = 0;
    let deepenNot: string | null = null;
    let done = false;

    while (idx < data.length) {
      const line = this.#readPktLine(data, idx);
      if (!line) break;
      idx = line.nextIdx;

      if (line.data === "0000") break; // flush
      if (line.data === "0001") continue; // delimiter

      const text = new TextDecoder().decode(line.content).trim();

      if (text.startsWith("want ")) {
        const oidMatch = text.substring(5).match(/^([0-9a-f]{40})/);
        if (oidMatch?.[1]) wants.push(oidMatch[1]);
      } else if (text.startsWith("have ")) {
        const oidMatch = text.substring(5).match(/^([0-9a-f]{40})/);
        if (oidMatch?.[1]) haves.push(oidMatch[1]);
      } else if (text.startsWith("shallow ")) {
        const oidMatch = text.substring(8).match(/^([0-9a-f]{40})/);
        if (oidMatch?.[1]) clientShallows.add(oidMatch[1]);
      } else if (text.startsWith("deepen ")) {
        deepen = parseInt(text.substring(7), 10);
      } else if (text.startsWith("deepen-since ")) {
        deepenSince = parseInt(text.substring(13), 10);
      } else if (text.startsWith("deepen-not ")) {
        deepenNot = text.substring(11);
      } else if (text === "done") {
        done = true;
      }
    }

    if (wants.length === 0) {
      return new Response("0000", {
        headers: { "Content-Type": "application/x-git-upload-pack-result" },
      });
    }

    // If client hasn't sent "done", send acknowledgements for haves
    if (!done && haves.length > 0) {
      const ackLines: string[] = [];
      for (const have of haves) {
        try {
          await this.#repository.readObject(have);
          ackLines.push(this.#pktLine(`acknowledgments\n`));
          ackLines.push(this.#pktLine(`ACK ${have}\n`));
        } catch {
          // Object not found — don't ACK
        }
      }
      if (ackLines.length === 0) {
        ackLines.push(this.#pktLine("acknowledgments\n"));
        ackLines.push(this.#pktLine("NAK\n"));
      }
      ackLines.push("0000");
      return new Response(ackLines.join(""), {
        headers: { "Content-Type": "application/x-git-upload-pack-result" },
      });
    }

    const { objectsToSend, newShallows, unshallows } = await this.#collectObjects(
      wants,
      haves,
      clientShallows,
      deepen,
      deepenSince,
      deepenNot,
      signal,
    );

    signal?.throwIfAborted();

    const packData = await this.#repository.createPack(Array.from(objectsToSend));

    // Build v2 response sections
    const responseChunks: Uint8Array[] = [];
    const encoder = new TextEncoder();
    const push = (s: string) => responseChunks.push(encoder.encode(s));

    // Shallow-info section (if applicable)
    if (newShallows.size > 0 || unshallows.size > 0) {
      push(this.#pktLine("shallow-info\n"));
      for (const oid of newShallows) push(this.#pktLine(`shallow ${oid}\n`));
      for (const oid of unshallows) push(this.#pktLine(`unshallow ${oid}\n`));
      push("0001"); // delimiter
    }

    // Packfile section
    push(this.#pktLine("packfile\n"));

    // Progress on channel 2
    const progressMsg = `Enumerating objects: ${objectsToSend.size}, done.\n`;
    responseChunks.push(this.#sidebandPacket(2, encoder.encode(progressMsg)));

    // Pack data on channel 1
    for (const pkt of this.#sidebandPackets(1, packData)) {
      responseChunks.push(pkt);
    }

    // Flush
    push("0000");

    // Assemble final response
    const totalLen = responseChunks.reduce((s, c) => s + c.length, 0);
    const response = new Uint8Array(totalLen);
    let respOff = 0;
    for (const chunk of responseChunks) {
      response.set(chunk, respOff);
      respOff += chunk.length;
    }

    return new Response(response, {
      headers: { "Content-Type": "application/x-git-upload-pack-result" },
    });
  }

  #readPktLine(data: Uint8Array, offset: number) {
    if (offset + 4 > data.length) return null;

    const lengthStr = new TextDecoder().decode(data.slice(offset, offset + 4));
    const length = parseInt(lengthStr, 16);

    if (length === 0) {
      return { content: new Uint8Array(0), nextIdx: offset + 4, data: "0000" };
    }

    // Delimiter packet (protocol v2)
    if (length === 1) {
      return { content: new Uint8Array(0), nextIdx: offset + 4, data: "0001" };
    }

    if (offset + length > data.length) return null;

    const content = data.slice(offset + 4, offset + length);
    return {
      content,
      nextIdx: offset + length,
      data: lengthStr,
    };
  }

  #extractReceivePackData(data: Uint8Array, capabilities: Set<string>) {
    if (data.length === 0) {
      return data;
    }

    const signature = new TextDecoder().decode(data.slice(0, 4));
    if (signature === "PACK") {
      return data;
    }

    if (!capabilities.has("side-band") && !capabilities.has("side-band-64k")) {
      return data;
    }

    const chunks: Uint8Array[] = [];
    let offset = 0;

    while (offset < data.length) {
      const packet = this.#readPktLine(data, offset);
      if (!packet) {
        throw new Error("Malformed side-band receive-pack payload");
      }

      offset = packet.nextIdx;

      if (packet.data === "0000") {
        break;
      }

      const channel = packet.content[0];
      const payload = packet.content.slice(1);

      if (channel === 1) {
        chunks.push(payload);
        continue;
      }

      if (channel === 2) {
        continue;
      }

      if (channel === 3) {
        const message = new TextDecoder().decode(payload).trim();
        throw new Error(message || "Client aborted pack transfer");
      }

      throw new Error(`Unsupported side-band channel ${channel}`);
    }

    return concatenateUint8Arrays(chunks);
  }

  async #lfsBatch(request: Request) {
    request.signal?.throwIfAborted();
    const repo = this.#urlPattern?.pathname.groups.repo!;
    const baseUrl = new URL(request.url).origin;
    return this.#lfs.batch(repo, request.body, baseUrl);
  }

  async #lfsUpload(request: Request) {
    request.signal?.throwIfAborted();
    const repo = this.#urlPattern?.pathname.groups.repo!;
    const oid = this.#urlPattern?.pathname.groups.oid!;
    return this.#lfs.upload(repo, oid, request.body);
  }

  async #lfsDownload(request: Request) {
    request.signal?.throwIfAborted();
    const repo = this.#urlPattern?.pathname.groups.repo!;
    const oid = this.#urlPattern?.pathname.groups.oid!;
    return this.#lfs.download(repo, oid);
  }
}
