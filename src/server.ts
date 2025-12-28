import { DurableObject } from "cloudflare:workers";

import { GitRepository } from "./git.repository.ts";
import { CloudflareStorage as Storage } from "./server.storage.ts";

type RequestBody = ReadableStream<Uint8Array<ArrayBuffer>> | null;

interface Route {
	handler: (body: RequestBody, signal?: AbortSignal) => Promise<Response>;
	method: string;
	pathname: string;
	search?: string;
}

export class Server extends DurableObject<Env> {
	#repository: GitRepository;
	#routes: Route[] = [
		{
			handler: (...args) => this.#head(...args),
			method: "GET",
			pathname: "/:repo{.git}?/HEAD",
		},
		{
			handler: (...args) => this.#infoRefs(...args),
			method: "GET",
			pathname: "/:repo{.git}?/info/refs",
			search: "?service=:service(git-upload-pack|git-receive-pack)",
		},
		{
			handler: (...args) => this.#receivePack(...args),
			method: "POST",
			pathname: "/:repo{.git}?/git-receive-pack",
		},
		{
			handler: (...args) => this.#uploadPack(...args),
			method: "POST",
			pathname: "/:repo{.git}?/git-upload-pack",
		},
	];
	#urlPattern: URLPatternResult | null = null;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		const storage = new Storage(ctx, env);
		const config = { repoName: ctx.id.toString() };
		this.#repository = new GitRepository(storage, config);
	}

	async fetch(request: Request) {
		try {
			request.signal?.throwIfAborted();

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

				return await route.handler(request.body, request.signal);
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

	async #head(_body: RequestBody, signal?: AbortSignal) {
		signal?.throwIfAborted();

		// Get the symbolic ref that HEAD points to (e.g., refs/heads/main)
		const symbolicRef = await this.#repository.getCurrentHead();
		if (symbolicRef) {
			return new Response(`ref: ${symbolicRef}\n`, {
				headers: { "Content-Type": "text/plain" },
			});
		}

		// Check if HEAD is detached (points directly to a commit)
		const headRef = await this.#repository.getRef("HEAD");
		if (headRef) {
			return new Response(`${headRef}\n`, {
				headers: { "Content-Type": "text/plain" },
			});
		}

		// Default for empty repository
		return new Response("ref: refs/heads/main\n", {
			headers: { "Content-Type": "text/plain" },
		});
	}

	async #infoRefs(_body: RequestBody, signal?: AbortSignal) {
		signal?.throwIfAborted();

		const service = this.#urlPattern?.search.groups.service || "git-upload-pack";

		// Build response with pkt-line protocol
		const lines: string[] = [];

		// Service announcement
		lines.push(this.#pktLine(`# service=${service}`));
		lines.push("0000"); // Flush packet

		// Get all refs
		const allRefs = await this.#repository.getAllRefs();

		// Capabilities to advertise
		const capabilities =
			service === "git-receive-pack"
				? "report-status delete-refs ofs-delta"
				: "multi_ack_detailed side-band-64k thin-pack ofs-delta";

		// Add HEAD if it exists
		const headRef = await this.#repository.getRef("HEAD");
		let firstLine = true;

		if (headRef) {
			lines.push(this.#pktLine(`${headRef} HEAD\0${capabilities}\n`));
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

	#pktLine(text: string): string {
		const length = text.length + 4;
		return length.toString(16).padStart(4, "0") + text;
	}

	async #receivePack(body: RequestBody, signal?: AbortSignal) {
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
				chunks.push(result.value);
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
			const packData = fullData.slice(idx);
			if (packData.length > 0) {
				signal?.throwIfAborted();
				const packStream = new ReadableStream({
					start(controller) {
						controller.enqueue(packData);
						controller.close();
					},
				});
				await this.#repository.parsePack(packStream);
			}

			// Update refs
			const refResults: string[] = [];
			for (const update of refUpdates) {
				if (update.new === "0000000000000000000000000000000000000000") {
					// Delete ref
					await this.#repository.deleteRef(update.ref);
				} else {
					// Write ref
					await this.#repository.writeRef(update.ref, update.new);
				}
				refResults.push(this.#pktLine(`ok ${update.ref}\n`));
			}

			// Send success response (unpack ok + ref status)
			const response = this.#pktLine("unpack ok\n") + refResults.join("") + "0000";
			return new Response(response, {
				headers: { "Content-Type": "application/x-git-receive-pack-result" },
			});
		} catch (e) {
			if (reader) reader.releaseLock();
			if (e instanceof Error && e.name === "AbortError") throw e;

			const error = e instanceof Error ? e.message : "Unknown error";
			console.error("receive-pack error:", error);
			const response = `001f${error}\n0000`;
			return new Response(response, { status: 400 });
		}
	}

	async #uploadPack(body: RequestBody, signal?: AbortSignal) {
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
				chunks.push(result.value);
				result = await reader.read();
			}

			reader.releaseLock();

			const fullData = new Uint8Array(chunks.reduce((acc, chunk) => acc + chunk.length, 0));
			let offset = 0;
			for (const chunk of chunks) {
				fullData.set(chunk, offset);
				offset += chunk.length;
			}

			// Parse want/have/done commands
			let idx = 0;
			const wants: string[] = [];
			const haves: string[] = [];

			while (idx < fullData.length) {
				signal?.throwIfAborted();
				const line = this.#readPktLine(fullData, idx);
				if (line === null) break;

				idx = line.nextIdx;
				const text = new TextDecoder().decode(line.content).trim();

				if (text.startsWith("want ")) {
					// Extract just the OID (40 hex chars), ignore capabilities
					const oidMatch = text.substring(5).match(/^([0-9a-f]{40})/);
					if (oidMatch && oidMatch[1]) {
						wants.push(oidMatch[1]);
					}
				} else if (text.startsWith("have ")) {
					// Extract just the OID (40 hex chars)
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

			// Collect objects to send (commits and their trees/blobs)
			const objectsToSend = new Set<string>();
			const processedCommits = new Set<string>();
			const commitsToProcess = [...wants];

			while (commitsToProcess.length > 0) {
				signal?.throwIfAborted();
				const commitOid = commitsToProcess.pop();
				if (!commitOid || processedCommits.has(commitOid)) continue;
				if (haves.includes(commitOid)) continue; // Client already has this

				processedCommits.add(commitOid);
				objectsToSend.add(commitOid);

				// Read commit
				try {
					const commit = await this.#repository.readObject(commitOid);
					if (commit.type === "commit") {
						const commitInfo = this.#repository.parseCommit(commit.data);

						// Add tree and all objects
						const treeObjects = await this.#repository.collectTreeObjects(commitInfo.tree);
						treeObjects.forEach((oid) => objectsToSend.add(oid));

						// Add parent to process
						if (commitInfo.parent) {
							commitsToProcess.push(commitInfo.parent);
						}
					}
				} catch {
					// Object not found, skip
					continue;
				}
			}

			signal?.throwIfAborted();

			// Create pack file
			const packData = await this.#repository.createPack(Array.from(objectsToSend));

			// Send NAK (no common commits) followed by pack data in sideband format
			// Protocol: NAK\n then sideband-wrapped pack data, then flush
			const nakLine = this.#pktLine("NAK\n");

			// Wrap pack data in sideband channel 1 packets
			// side-band-64k allows up to 65520 bytes per packet (65535 - 4 length - 1 channel - some margin)
			const maxPacketData = 65515;
			const sidebandPackets: Uint8Array[] = [];

			for (let i = 0; i < packData.length; i += maxPacketData) {
				const chunk = packData.slice(i, Math.min(i + maxPacketData, packData.length));
				// Packet format: 4-byte hex length, 1-byte channel (0x01), data
				const packetLen = chunk.length + 5; // 4 bytes length + 1 byte channel + data
				const lenStr = packetLen.toString(16).padStart(4, "0");
				const packet = new Uint8Array(packetLen);
				packet.set(new TextEncoder().encode(lenStr), 0);
				packet[4] = 0x01; // Channel 1 = pack data
				packet.set(chunk, 5);
				sidebandPackets.push(packet);
			}

			// Final flush packet
			const flushPacket = new TextEncoder().encode("0000");

			// Calculate total size
			const nakBytes = new TextEncoder().encode(nakLine);
			const totalSize =
				nakBytes.length +
				sidebandPackets.reduce((sum, p) => sum + p.length, 0) +
				flushPacket.length;

			const response = new Uint8Array(totalSize);
			let respOffset = 0;
			response.set(nakBytes, respOffset);
			respOffset += nakBytes.length;
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
			return new Response("Internal Server Error", { status: 500 });
		}
	}

	#readPktLine(
		data: Uint8Array,
		offset: number,
	): { content: Uint8Array; nextIdx: number; data: string } | null {
		if (offset + 4 > data.length) return null;

		const lengthStr = new TextDecoder().decode(data.slice(offset, offset + 4));
		const length = parseInt(lengthStr, 16);

		if (length === 0) {
			return { content: new Uint8Array(0), nextIdx: offset + 4, data: "0000" };
		}

		if (offset + length > data.length) return null;

		const content = data.slice(offset + 4, offset + length);
		return {
			content,
			nextIdx: offset + length,
			data: lengthStr,
		};
	}
}
