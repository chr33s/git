import { readVarInt, writeVarInt } from "./git.utils.ts";

export class GitDelta {
	static applyDelta(base: Uint8Array, delta: Uint8Array): Uint8Array {
		let deltaOffset = 0;

		// Read base object size from delta
		const baseSize = readVarInt(delta, deltaOffset);
		deltaOffset += baseSize.bytesRead;

		if (baseSize.value !== base.length) {
			throw new Error(`Base size mismatch: expected ${baseSize.value}, got ${base.length}`);
		}

		// Read target object size from delta
		const targetSize = readVarInt(delta, deltaOffset);
		deltaOffset += targetSize.bytesRead;

		const result = new Uint8Array(targetSize.value);
		let resultOffset = 0;

		// Process delta instructions
		while (deltaOffset < delta.length) {
			const cmd = delta[deltaOffset++];

			if (cmd && cmd & 0x80) {
				// Copy instruction
				let copyOffset = 0;
				let copySize = 0;

				if (cmd & 0x01) copyOffset |= delta[deltaOffset++] ?? 0;
				if (cmd & 0x02) copyOffset |= (delta[deltaOffset++] ?? 0) << 8;
				if (cmd & 0x04) copyOffset |= (delta[deltaOffset++] ?? 0) << 16;
				if (cmd & 0x08) copyOffset |= (delta[deltaOffset++] ?? 0) << 24;

				if (cmd & 0x10) copySize |= delta[deltaOffset++] ?? 0;
				if (cmd & 0x20) copySize |= (delta[deltaOffset++] ?? 0) << 8;
				if (cmd & 0x40) copySize |= (delta[deltaOffset++] ?? 0) << 16;

				if (copySize === 0) copySize = 0x10000;

				// Copy from base to result
				result.set(base.slice(copyOffset, copyOffset + copySize), resultOffset);
				resultOffset += copySize;
			} else if (cmd && cmd > 0) {
				// Insert instruction
				const insertSize = cmd;
				result.set(delta.slice(deltaOffset, deltaOffset + insertSize), resultOffset);
				deltaOffset += insertSize;
				resultOffset += insertSize;
			} else {
				throw new Error("Invalid delta instruction");
			}
		}

		if (resultOffset !== targetSize.value) {
			throw new Error(`Result size mismatch: expected ${targetSize.value}, got ${resultOffset}`);
		}

		return result;
	}

	static createDelta(source: Uint8Array, target: Uint8Array): Uint8Array {
		const instructions: Array<{
			type: "copy" | "insert";
			offset?: number;
			size?: number;
			data?: Uint8Array;
		}> = [];

		// Build hash table for source chunks
		const chunkSize = 16;
		const sourceChunks = new Map<string, number[]>();

		for (let i = 0; i <= source.length - chunkSize; i++) {
			const chunk = source.slice(i, i + chunkSize);
			const hash = this.#hashChunk(chunk);

			if (!sourceChunks.has(hash)) {
				sourceChunks.set(hash, []);
			}
			sourceChunks.get(hash)!.push(i);
		}

		let targetOffset = 0;
		let pendingInsert: number[] = [];

		while (targetOffset < target.length) {
			let bestMatch = this.#findBestMatch(source, target, targetOffset, sourceChunks, chunkSize);

			if (bestMatch && bestMatch.size >= chunkSize) {
				// Flush pending insert
				if (pendingInsert.length > 0) {
					instructions.push({
						type: "insert",
						data: new Uint8Array(pendingInsert),
					});
					pendingInsert = [];
				}

				// Add copy instruction
				instructions.push({
					type: "copy",
					offset: bestMatch.offset,
					size: bestMatch.size,
				});

				targetOffset += bestMatch.size;
			} else {
				// Add to pending insert
				const byte = target[targetOffset];
				if (byte !== undefined) {
					pendingInsert.push(byte);
				}
				targetOffset++;
			}
		}

		// Flush final pending insert
		if (pendingInsert.length > 0) {
			instructions.push({
				type: "insert",
				data: new Uint8Array(pendingInsert),
			});
		}

		// Encode instructions
		return this.#encodeInstructions(source.length, target.length, instructions);
	}

	static #findBestMatch(
		source: Uint8Array,
		target: Uint8Array,
		targetOffset: number,
		sourceChunks: Map<string, number[]>,
		chunkSize: number,
	): { offset: number; size: number } | null {
		if (targetOffset + chunkSize > target.length) {
			return null;
		}

		const targetChunk = target.slice(targetOffset, targetOffset + chunkSize);
		const hash = this.#hashChunk(targetChunk);
		const positions = sourceChunks.get(hash);

		if (!positions || positions.length === 0) {
			return null;
		}

		let bestMatch: { offset: number; size: number } | null = null;

		for (const pos of positions) {
			let matchSize = 0;

			while (
				targetOffset + matchSize < target.length &&
				pos + matchSize < source.length &&
				target[targetOffset + matchSize] === source[pos + matchSize]
			) {
				matchSize++;
			}

			if (!bestMatch || matchSize > bestMatch.size) {
				bestMatch = { offset: pos, size: matchSize };
			}
		}

		return bestMatch;
	}

	static #encodeInstructions(
		sourceSize: number,
		targetSize: number,
		instructions: Array<{
			type: "copy" | "insert";
			offset?: number;
			size?: number;
			data?: Uint8Array;
		}>,
	): Uint8Array {
		const chunks: Uint8Array[] = [];

		// Add source size
		chunks.push(this.#encodeVarint(sourceSize));

		// Add target size
		chunks.push(this.#encodeVarint(targetSize));

		// Add instructions
		for (const inst of instructions) {
			if (inst.type === "copy") {
				const encoded = this.#encodeCopyInstruction(inst.offset!, inst.size!);
				chunks.push(encoded);
			} else if (inst.type === "insert" && inst.data) {
				const encoded = this.#encodeInsertInstruction(inst.data);
				chunks.push(encoded);
			}
		}

		// Concatenate all chunks
		const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
		const result = new Uint8Array(totalLength);
		let offset = 0;

		for (const chunk of chunks) {
			result.set(chunk, offset);
			offset += chunk.length;
		}

		return result;
	}

	static #encodeCopyInstruction(offset: number, size: number): Uint8Array {
		const bytes: number[] = [];
		let cmd = 0x80;

		// Encode offset
		if (offset & 0xff) {
			cmd |= 0x01;
			bytes.push(offset & 0xff);
		}
		if (offset & 0xff00) {
			cmd |= 0x02;
			bytes.push((offset >> 8) & 0xff);
		}
		if (offset & 0xff0000) {
			cmd |= 0x04;
			bytes.push((offset >> 16) & 0xff);
		}
		if (offset & 0xff000000) {
			cmd |= 0x08;
			bytes.push((offset >> 24) & 0xff);
		}

		// Encode size
		if (size & 0xff) {
			cmd |= 0x10;
			bytes.push(size & 0xff);
		}
		if (size & 0xff00) {
			cmd |= 0x20;
			bytes.push((size >> 8) & 0xff);
		}
		if (size & 0xff0000) {
			cmd |= 0x40;
			bytes.push((size >> 16) & 0xff);
		}

		const result = new Uint8Array(1 + bytes.length);
		result[0] = cmd;
		result.set(bytes, 1);

		return result;
	}

	static #encodeInsertInstruction(data: Uint8Array): Uint8Array {
		if (data.length === 0 || data.length > 127) {
			throw new Error(`Invalid insert size: ${data.length}`);
		}

		const result = new Uint8Array(1 + data.length);
		result[0] = data.length;
		result.set(data, 1);

		return result;
	}

	static #encodeVarint(value: number): Uint8Array {
		return writeVarInt(value);
	}

	static #hashChunk(chunk: Uint8Array): string {
		// Simple hash function for chunks
		let hash = 0;
		for (let i = 0; i < chunk.length; i++) {
			const byte = chunk[i];
			if (byte !== undefined) {
				hash = (hash << 5) - hash + byte;
				hash = hash & hash; // Convert to 32-bit integer
			}
		}
		return hash.toString(36);
	}

	static calculateCompressionRatio(original: Uint8Array, delta: Uint8Array): number {
		return 1 - delta.length / original.length;
	}

	static shouldUseDelta(original: Uint8Array, delta: Uint8Array): boolean {
		// Use delta if it saves at least 10% space
		return delta.length < original.length * 0.9;
	}
}

/** Delta cache for improved performance */
export class GitDeltaCache {
	#cache: Map<string, { base: string; delta: Uint8Array }> = new Map();
	#maxSize: number;

	constructor(maxSize: number = 100) {
		this.#maxSize = maxSize;
	}

	set(targetOid: string, baseOid: string, delta: Uint8Array) {
		if (this.#cache.size >= this.#maxSize) {
			// Remove oldest entry (simple FIFO)
			const firstKey = this.#cache.keys().next().value;
			if (firstKey !== undefined) {
				this.#cache.delete(firstKey);
			}
		}

		this.#cache.set(targetOid, { base: baseOid, delta });
	}

	get(targetOid: string): { base: string; delta: Uint8Array } | undefined {
		return this.#cache.get(targetOid);
	}

	has(targetOid: string): boolean {
		return this.#cache.has(targetOid);
	}

	clear() {
		this.#cache.clear();
	}
}
