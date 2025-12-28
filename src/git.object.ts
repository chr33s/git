import type { GitStorage } from "./git.storage.ts";
import { compressData, decompressData, createSha1 } from "./git.utils.ts";

export interface GitObject {
	type: "blob" | "tree" | "commit" | "tag";
	data: Uint8Array;
}

export class GitObjectStore {
	#storage: GitStorage;

	constructor(storage: GitStorage) {
		this.#storage = storage;
	}

	async init(): Promise<void> {
		// Ensure the objects directory structure exists
		await this.#storage.createDirectory(".git/objects");
	}

	async readObject(oid: string): Promise<GitObject> {
		const dir = oid.substring(0, 2);
		const file = oid.substring(2);
		const path = `.git/objects/${dir}/${file}`;

		try {
			const compressed = await this.#storage.readFile(path);
			const decompressed = await decompressData(compressed);

			// Parse object
			const nullIdx = decompressed.indexOf(0);
			const header = new TextDecoder().decode(decompressed.slice(0, nullIdx));
			const [type, _size] = header.split(" ");
			const data = decompressed.slice(nullIdx + 1);

			return {
				type: type as GitObject["type"],
				data,
			};
		} catch (error) {
			throw new Error(
				`Object ${oid} not found: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}

	async writeObject(type: GitObject["type"], data: Uint8Array): Promise<string> {
		// Create object content
		const header = new TextEncoder().encode(`${type} ${data.length}\0`);
		const content = new Uint8Array(header.length + data.length);
		content.set(header);
		content.set(data, header.length);

		// Calculate SHA-1
		const oid = await createSha1(content);

		// Compress the content
		const compressed = await compressData(content);

		// Store object
		const dir = oid.substring(0, 2);
		const file = oid.substring(2);
		const path = `.git/objects/${dir}/${file}`;

		await this.#storage.writeFile(path, compressed);

		return oid;
	}

	async hasObject(oid: string): Promise<boolean> {
		const dir = oid.substring(0, 2);
		const file = oid.substring(2);
		const path = `.git/objects/${dir}/${file}`;

		return await this.#storage.exists(path);
	}
}
