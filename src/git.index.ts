import type { GitStorage } from "./git.storage.ts";
import type { GitObjectStore } from "./git.object.ts";
import { bytesToHex, hexToBytes } from "./git.utils.ts";

export interface IndexEntry {
	path: string;
	oid: string;
	mode: string;
	size: number;
	mtime: number;
}

export class GitIndex {
	#entries: IndexEntry[] = [];
	#storage: GitStorage;

	constructor(storage: GitStorage) {
		this.#storage = storage;
	}

	async init(): Promise<void> {
		await this.load();
	}

	async load() {
		try {
			const buffer = await this.#storage.readFile(".git/index");
			this.#entries = this.#parseIndex(buffer);
		} catch {
			// Index doesn't exist yet
			this.#entries = [];
		}
	}

	async save() {
		const buffer = await this.#serializeIndex();
		await this.#storage.writeFile(".git/index", new Uint8Array(buffer));
	}

	async addEntry(entry: IndexEntry) {
		// Remove existing entry if present
		this.#entries = this.#entries.filter((e) => e.path !== entry.path);

		// Add new entry
		this.#entries.push(entry);

		// Sort entries
		this.#entries.sort((a, b) => a.path.localeCompare(b.path));

		await this.save();
	}

	async removeEntry(path: string) {
		this.#entries = this.#entries.filter((e) => e.path !== path);
		await this.save();
	}

	getEntries(): IndexEntry[] {
		return [...this.#entries];
	}

	async updateFromTree(treeOid: string, objectStore: GitObjectStore) {
		// This would walk the tree and update index entries
		// Implementation: Walk the tree and create index entries for all blobs
		this.#entries = [];

		try {
			await this.#walkTreeAndAddEntries(objectStore, treeOid, "");
		} catch (error) {
			console.warn(
				`Failed to update from tree ${treeOid}: ${error instanceof Error ? error.message : String(error)}`,
			);
			this.#entries = [];
		}

		await this.save();
	}

	async #walkTreeAndAddEntries(objectStore: GitObjectStore, treeOid: string, prefix: string) {
		try {
			const tree = await objectStore.readObject(treeOid);
			const entries = this.#parseTreeObject(tree.data);

			for (const entry of entries) {
				const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;

				if (entry.mode === "40000") {
					// Directory - recurse
					await this.#walkTreeAndAddEntries(objectStore, entry.oid, fullPath);
				} else {
					// File - add to index
					this.#entries.push({
						path: fullPath,
						oid: entry.oid,
						mode: entry.mode,
						size: 0, // Would need to read blob to get actual size
						mtime: Date.now(),
					});
				}
			}
		} catch (error) {
			console.warn(
				`Failed to walk tree ${treeOid}: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}

	#parseTreeObject(data: Uint8Array): Array<{ mode: string; name: string; oid: string }> {
		const entries: Array<{ mode: string; name: string; oid: string }> = [];
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

	#parseIndex(buffer: Uint8Array): IndexEntry[] {
		const entries: IndexEntry[] = [];

		// Check signature
		const signature = new TextDecoder().decode(buffer.slice(0, 4));
		if (signature !== "DIRC") {
			throw new Error("Invalid index signature");
		}

		// Read version
		const _version = this.#readUint32BE(buffer, 4);

		// Read entry count
		const entryCount = this.#readUint32BE(buffer, 8);

		let offset = 12;

		for (let i = 0; i < entryCount; i++) {
			// Parse entry - reading all fields from git index format
			const _ctimeSec = this.#readUint32BE(buffer, offset);
			const _ctimeNano = this.#readUint32BE(buffer, offset + 4);
			const mtimeSec = this.#readUint32BE(buffer, offset + 8);
			const _mtimeNano = this.#readUint32BE(buffer, offset + 12);
			const _dev = this.#readUint32BE(buffer, offset + 16);
			const _ino = this.#readUint32BE(buffer, offset + 20);
			const mode = this.#readUint32BE(buffer, offset + 24);
			const _uid = this.#readUint32BE(buffer, offset + 28);
			const _gid = this.#readUint32BE(buffer, offset + 32);
			const size = this.#readUint32BE(buffer, offset + 36);

			// Read SHA1
			const oid = bytesToHex(buffer.slice(offset + 40, offset + 60));

			// Read flags
			const flags = this.#readUint16BE(buffer, offset + 60);
			const nameLength = flags & 0xfff;

			// Read path
			const pathBytes = buffer.slice(offset + 62, offset + 62 + nameLength);
			const path = new TextDecoder().decode(pathBytes);

			entries.push({
				path,
				oid,
				mode: mode.toString(8),
				size,
				mtime: mtimeSec * 1000,
			});

			// Calculate next entry offset (entries are padded to 8 bytes)
			offset += 62 + nameLength;
			const padding = (8 - (offset % 8)) % 8;
			offset += padding;
		}

		return entries;
	}

	async #serializeIndex(): Promise<Uint8Array> {
		// Calculate size - must match actual write offsets including header
		let size = 12; // Header

		for (const entry of this.#entries) {
			size += 62 + entry.path.length;
			// Padding is calculated based on current offset (size), not entry size alone
			const padding = (8 - (size % 8)) % 8;
			size += padding;
		}

		size += 20; // SHA1 checksum

		const buffer = new Uint8Array(size);
		let offset = 0;

		// Write header
		new TextEncoder().encodeInto("DIRC", buffer);
		offset += 4;

		this.#writeUint32BE(buffer, offset, 2); // Version
		offset += 4;

		this.#writeUint32BE(buffer, offset, this.#entries.length);
		offset += 4;

		// Write entries
		for (const entry of this.#entries) {
			// Write times - using modification time for both ctime and mtime
			this.#writeUint32BE(buffer, offset, Math.floor(entry.mtime / 1000));
			this.#writeUint32BE(buffer, offset + 4, 0);
			this.#writeUint32BE(buffer, offset + 8, Math.floor(entry.mtime / 1000));
			this.#writeUint32BE(buffer, offset + 12, 0);
			offset += 16;

			// Write dev, ino, mode, uid, gid
			this.#writeUint32BE(buffer, offset, 0);
			this.#writeUint32BE(buffer, offset + 4, 0);
			this.#writeUint32BE(buffer, offset + 8, parseInt(entry.mode, 8));
			this.#writeUint32BE(buffer, offset + 12, 0);
			this.#writeUint32BE(buffer, offset + 16, 0);
			offset += 20;

			// Write size
			this.#writeUint32BE(buffer, offset, entry.size);
			offset += 4;

			// Write SHA1
			const oidBytes = hexToBytes(entry.oid);
			buffer.set(oidBytes, offset);
			offset += 20;

			// Write flags
			const nameLength = Math.min(entry.path.length, 0xfff);
			this.#writeUint16BE(buffer, offset, nameLength);
			offset += 2;

			// Write path
			const pathBytes = new TextEncoder().encode(entry.path);
			buffer.set(pathBytes, offset);
			offset += pathBytes.length;

			// Add padding
			const padding = (8 - (offset % 8)) % 8;
			offset += padding;
		}

		// Calculate and write checksum
		const hash = await crypto.subtle.digest("SHA-1", buffer.slice(0, offset));
		buffer.set(new Uint8Array(hash), offset);

		return buffer;
	}

	#readUint32BE(buffer: Uint8Array, offset: number): number {
		if (offset + 3 >= buffer.length) {
			throw new Error("Buffer underrun in readUint32BE");
		}
		return (
			(buffer[offset]! << 24) |
			(buffer[offset + 1]! << 16) |
			(buffer[offset + 2]! << 8) |
			buffer[offset + 3]!
		);
	}

	#readUint16BE(buffer: Uint8Array, offset: number): number {
		if (offset + 1 >= buffer.length) {
			throw new Error("Buffer underrun in readUint16BE");
		}
		return (buffer[offset]! << 8) | buffer[offset + 1]!;
	}

	#writeUint32BE(buffer: Uint8Array, offset: number, value: number) {
		buffer[offset] = (value >>> 24) & 0xff;
		buffer[offset + 1] = (value >>> 16) & 0xff;
		buffer[offset + 2] = (value >>> 8) & 0xff;
		buffer[offset + 3] = value & 0xff;
	}

	#writeUint16BE(buffer: Uint8Array, offset: number, value: number) {
		buffer[offset] = (value >>> 8) & 0xff;
		buffer[offset + 1] = value & 0xff;
	}
}
