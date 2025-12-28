import { GitObjectStore } from "./git.object.ts";
import { GitDelta, GitDeltaCache } from "./git.delta.ts";
import {
	decompressData,
	compressData,
	concatenateUint8Arrays,
	createSha1,
	bytesToHex,
	hexToBytes,
} from "./git.utils.ts";

interface PackObject {
	type: "commit" | "tree" | "blob" | "tag" | "ofs_delta" | "ref_delta";
	size: number;
	data: Uint8Array;
	offset: number;
	crc32?: number;
}

interface DeltaObject extends PackObject {
	baseOffset?: number;
	baseOid?: string;
}

export class GitPackParser {
	#objectStore: GitObjectStore;
	#deltaCache: GitDeltaCache;
	#objects: Map<number, PackObject> = new Map();
	#oidToOffset: Map<string, number> = new Map();

	constructor(objectStore: GitObjectStore) {
		this.#objectStore = objectStore;
		this.#deltaCache = new GitDeltaCache();
	}

	async #readFullStream(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<Uint8Array> {
		const chunks: Uint8Array[] = [];

		try {
			while (true) {
				const { done, value } = await reader.read();
				if (done) break;

				chunks.push(value);
			}
		} finally {
			reader.releaseLock();
		}

		// Concatenate all chunks
		return concatenateUint8Arrays(chunks);
	}

	async parsePack(stream: ReadableStream<Uint8Array>) {
		const reader = stream.getReader();
		const buffer = await this.#readFullStream(reader);

		// Parse pack header
		const signature = new TextDecoder().decode(buffer.slice(0, 4));
		if (signature !== "PACK") {
			throw new Error("Invalid pack signature");
		}

		const version = this.#readUint32BE(buffer, 4);
		const objectCount = this.#readUint32BE(buffer, 8);

		console.log(`Pack version: ${version}, objects: ${objectCount}`);

		// First pass: parse all objects
		let offset = 12;
		for (let i = 0; i < objectCount; i++) {
			const result = await this.#parsePackObject(buffer, offset);
			this.#objects.set(offset, result.object);
			offset = result.nextOffset;
		}

		// Second pass: resolve deltas
		await this.#resolveDeltas();

		// Store all objects
		await this.#storeObjects();

		// Verify pack checksum
		const packChecksum = buffer.slice(buffer.length - 20);
		const calculatedChecksum = await this.#calculateChecksum(buffer.slice(0, buffer.length - 20));

		if (!this.#compareChecksums(packChecksum, calculatedChecksum)) {
			console.warn("Pack checksum mismatch");
		}
	}

	async #parsePackObject(
		buffer: Uint8Array,
		offset: number,
	): Promise<{
		object: PackObject;
		nextOffset: number;
	}> {
		const startOffset = offset;

		// Read object header
		let byte = buffer[offset++];
		if (byte === undefined) throw new Error("Unexpected end of buffer");
		const type = (byte >> 4) & 0x7;
		let size = byte & 0xf;
		let shift = 4;

		while (byte & 0x80) {
			byte = buffer[offset++];
			if (byte === undefined) throw new Error("Unexpected end of buffer");
			size |= (byte & 0x7f) << shift;
			shift += 7;
		}

		const typeNames = ["", "commit", "tree", "blob", "tag", "", "ofs_delta", "ref_delta"];
		const typeName = typeNames[type] as PackObject["type"];

		let object: PackObject;

		if (type === 6) {
			// OFS_DELTA
			let baseOffset = 0;
			// @ts-ignore - Complex byte manipulation, validated at runtime
			byte = buffer[offset++];
			// @ts-ignore - Complex byte manipulation, validated at runtime
			baseOffset = byte & 0x7f;

			// @ts-ignore - Complex byte manipulation, validated at runtime
			while (byte & 0x80) {
				// @ts-ignore - Complex byte manipulation, validated at runtime
				byte = buffer[offset++];
				// @ts-ignore - Complex byte manipulation, validated at runtime
				baseOffset = ((baseOffset + 1) << 7) | (byte & 0x7f);
			}

			const deltaData = await this.#readCompressedData(buffer, offset, size);

			object = {
				type: "ofs_delta",
				size,
				data: deltaData.data,
				offset: startOffset,
				baseOffset: startOffset - baseOffset,
			} as DeltaObject;

			offset = deltaData.nextOffset;
		} else if (type === 7) {
			// REF_DELTA
			const baseOid = bytesToHex(buffer.slice(offset, offset + 20));
			offset += 20;

			const deltaData = await this.#readCompressedData(buffer, offset, size);

			object = {
				type: "ref_delta",
				size,
				data: deltaData.data,
				offset: startOffset,
				baseOid,
			} as DeltaObject;

			offset = deltaData.nextOffset;
		} else {
			// Regular object
			const compressedData = await this.#readCompressedData(buffer, offset, size);

			object = {
				type: typeName,
				size,
				data: compressedData.data,
				offset: startOffset,
			};

			offset = compressedData.nextOffset;
		}

		// Calculate CRC32 if needed
		object.crc32 = this.#crc32(buffer.slice(startOffset, offset));

		return { object, nextOffset: offset };
	}

	async #readCompressedData(
		buffer: Uint8Array,
		offset: number,
		expectedSize: number,
	): Promise<{ data: Uint8Array; nextOffset: number }> {
		// Find the end of compressed data
		const compressedEnd = await this.#findCompressedEnd(buffer, offset);
		const compressed = buffer.slice(offset, compressedEnd);

		// Decompress
		const decompressed = await this.#decompress(compressed);

		// Verify size
		if (decompressed.length !== expectedSize) {
			console.warn(`Size mismatch: expected ${expectedSize}, got ${decompressed.length}`);
		}

		return {
			data: decompressed,
			nextOffset: compressedEnd,
		};
	}

	async #findCompressedEnd(buffer: Uint8Array, offset: number): Promise<number> {
		// Use zlib structure to find end
		// Scan for next object header or end of buffer
		let pos = offset + 2; // Skip zlib header

		while (pos < buffer.length - 20) {
			// Check for potential object header
			const byte = buffer[pos];
			if (byte === undefined) break;

			// Look for valid object type in header
			const type = (byte >> 4) & 0x7;
			if (type >= 1 && type <= 7) {
				// Verify it's actually an object header by checking size encoding
				let testPos = pos + 1;
				let testByte = buffer[pos];

				while (testByte !== undefined && testByte & 0x80 && testPos < buffer.length) {
					testByte = buffer[testPos++];
				}

				// If we successfully read a size, this might be the next object
				if (testByte !== undefined && !(testByte & 0x80)) {
					// Try to decompress what we have
					try {
						const test = buffer.slice(offset, pos);
						const decompressed = await this.#decompress(test);
						if (decompressed.length > 0) {
							return pos;
						}
					} catch {
						// Not the end yet
					}
				}
			}

			pos++;
		}

		// Reached end of buffer
		return buffer.length - 20;
	}

	async #resolveDeltas() {
		const maxIterations = 10;
		let iteration = 0;
		let unresolvedCount = 0;

		do {
			unresolvedCount = 0;

			for (const [offset, object] of this.#objects) {
				if (object.type === "ofs_delta" || object.type === "ref_delta") {
					const resolved = await this.#resolveDelta(object as DeltaObject);

					if (resolved) {
						// Replace with resolved object
						this.#objects.set(offset, resolved);
					} else {
						unresolvedCount++;
					}
				}
			}

			iteration++;
		} while (unresolvedCount > 0 && iteration < maxIterations);

		if (unresolvedCount > 0) {
			console.warn(`Failed to resolve ${unresolvedCount} delta objects`);
		}
	}

	async #resolveDelta(deltaObject: DeltaObject): Promise<PackObject | null> {
		let baseObject: PackObject | null = null;

		if (deltaObject.baseOffset !== undefined) {
			// OFS_DELTA
			baseObject = this.#objects.get(deltaObject.baseOffset) || null;
		} else if (deltaObject.baseOid) {
			// REF_DELTA
			const offset = this.#oidToOffset.get(deltaObject.baseOid);
			if (offset !== undefined) {
				baseObject = this.#objects.get(offset) || null;
			} else {
				// Try to get from object store
				try {
					const stored = await this.#objectStore.readObject(deltaObject.baseOid);
					baseObject = {
						type: stored.type,
						size: stored.data.length,
						data: stored.data,
						offset: -1,
					};
				} catch {
					// Base not found
				}
			}
		}

		if (!baseObject) {
			return null;
		}

		// Base object might itself be a delta
		if (baseObject.type === "ofs_delta" || baseObject.type === "ref_delta") {
			return null; // Will be resolved in next iteration
		}

		// Apply delta
		const resolvedData = GitDelta.applyDelta(baseObject.data, deltaObject.data);

		return {
			type: baseObject.type,
			size: resolvedData.length,
			data: resolvedData,
			offset: deltaObject.offset,
		};
	}

	async #storeObjects() {
		for (const [offset, object] of this.#objects) {
			if (object.type !== "ofs_delta" && object.type !== "ref_delta") {
				const oid = await this.#objectStore.writeObject(object.type, object.data);
				this.#oidToOffset.set(oid, offset);

				// Cache delta if beneficial
				for (const [otherOffset, otherObject] of this.#objects) {
					if (otherOffset !== offset && otherObject.type === object.type) {
						const delta = GitDelta.createDelta(object.data, otherObject.data);

						if (GitDelta.shouldUseDelta(otherObject.data, delta)) {
							const otherOid = await this.#objectStore.writeObject(
								otherObject.type,
								otherObject.data,
							);
							this.#deltaCache.set(otherOid, oid, delta);
						}
					}
				}
			}
		}
	}

	async #decompress(data: Uint8Array): Promise<Uint8Array> {
		return await decompressData(data);
	}

	async #calculateChecksum(data: Uint8Array): Promise<Uint8Array> {
		const oid = await createSha1(data);
		return new Uint8Array(oid.length / 2).map((_, i) =>
			parseInt(oid.substring(i * 2, i * 2 + 2), 16),
		);
	}

	#compareChecksums(a: Uint8Array, b: Uint8Array): boolean {
		if (a.length !== b.length) return false;

		for (let i = 0; i < a.length; i++) {
			if (a[i] !== b[i]) return false;
		}

		return true;
	}

	#crc32(data: Uint8Array): number {
		const table = this.#getCRC32Table();
		let crc = 0xffffffff;

		for (let i = 0; i < data.length; i++) {
			const byte = data[i];
			if (byte !== undefined) {
				const tableValue = table[(crc ^ byte) & 0xff];
				if (tableValue !== undefined) {
					crc = (crc >>> 8) ^ tableValue;
				}
			}
		}

		return (crc ^ 0xffffffff) >>> 0;
	}

	#getCRC32Table(): Uint32Array {
		const table = new Uint32Array(256);

		for (let i = 0; i < 256; i++) {
			let c = i;

			for (let j = 0; j < 8; j++) {
				c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
			}

			table[i] = c;
		}

		return table;
	}

	#readUint32BE(buffer: Uint8Array, offset: number): number {
		return (
			(buffer[offset] ?? 0 << 24) |
			(buffer[offset + 1] ?? 0 << 16) |
			(buffer[offset + 2] ?? 0 << 8) |
			(buffer[offset + 3] ?? 0)
		);
	}
}

export class GitPackWriter {
	#objectStore: GitObjectStore;

	constructor(objectStore: GitObjectStore) {
		this.#objectStore = objectStore;
	}

	async createPack(oids: string[]): Promise<Uint8Array> {
		// Collect all objects to pack
		const objects: Array<{ oid: string; type: string; data: Uint8Array }> = [];

		for (const oid of oids) {
			const obj = await this.#objectStore.readObject(oid);
			objects.push({
				oid,
				type: obj.type,
				data: obj.data,
			});
		}

		// Build pack data
		const packData = await this.#buildPack(objects);
		return packData;
	}

	async #buildPack(
		objects: Array<{ oid: string; type: string; data: Uint8Array }>,
	): Promise<Uint8Array> {
		const chunks: Uint8Array[] = [];

		// Header: "PACK"
		chunks.push(new TextEncoder().encode("PACK"));

		// Version: 2
		chunks.push(this.#writeUint32BE(2));

		// Object count
		chunks.push(this.#writeUint32BE(objects.length));

		// Write each object
		for (const obj of objects) {
			const typeNum = this.#getTypeNumber(obj.type);
			const objectData = await this.#encodeObject(typeNum, obj.data);
			chunks.push(objectData);
		}

		// Combine all chunks
		const allData = concatenateUint8Arrays(chunks);

		// Calculate checksum (SHA1 of pack data)
		const checksum = await this.#calculateChecksum(allData);
		const checksumBytes = hexToBytes(checksum);

		// Combine all data with checksum
		const finalData = concatenateUint8Arrays([allData, checksumBytes]);

		return finalData;
	}

	async #encodeObject(typeNum: number, data: Uint8Array): Promise<Uint8Array> {
		// Compress the data first
		const compressed = await compressData(data);

		// Encode header: type (3 bits) and size (variable length)
		const headerBytes = this.#encodeObjectHeader(typeNum, data.length);

		// Combine header and compressed data
		return concatenateUint8Arrays([headerBytes, compressed]);
	}

	#encodeObjectHeader(type: number, size: number): Uint8Array {
		const bytes: number[] = [];

		// First byte: type (top 3 bits) and first 4 bits of size (low 4 bits)
		let byte = (type << 4) | (size & 0xf);
		size >>= 4;

		// If size doesn't fit in 4 bits, set the high bit
		if (size > 0) {
			byte |= 0x80;
		}

		bytes.push(byte);

		// Additional bytes for remaining size (7 bits per byte, high bit indicates continuation)
		while (size > 0) {
			byte = size & 0x7f;
			size >>= 7;

			if (size > 0) {
				byte |= 0x80;
			}

			bytes.push(byte);
		}

		return new Uint8Array(bytes);
	}

	#getTypeNumber(type: string): number {
		const typeMap: Record<string, number> = {
			commit: 1,
			tree: 2,
			blob: 3,
			tag: 4,
			ofs_delta: 6,
			ref_delta: 7,
		};

		return typeMap[type] ?? 0;
	}

	async #calculateChecksum(data: Uint8Array): Promise<string> {
		return await createSha1(data);
	}

	#writeUint32BE(value: number): Uint8Array {
		const bytes = new Uint8Array(4);
		bytes[0] = (value >>> 24) & 0xff;
		bytes[1] = (value >>> 16) & 0xff;
		bytes[2] = (value >>> 8) & 0xff;
		bytes[3] = value & 0xff;
		return bytes;
	}
}
