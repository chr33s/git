import type { GitStorage } from "./git.storage.ts";

export interface GitRef {
	name: string;
	oid: string;
}

export class GitRefStore {
	#storage: GitStorage;

	constructor(storage: GitStorage) {
		this.#storage = storage;
	}

	async init(): Promise<void> {
		// Ensure the refs directory structure exists
		await this.#storage.createDirectory(".git/refs");
		await this.#storage.createDirectory(".git/refs/heads");
		await this.#storage.createDirectory(".git/refs/tags");
	}

	async readRef(refName: string): Promise<string | null> {
		try {
			const path = this.#getRefPath(refName);
			const data = await this.#storage.readFile(path);
			const content = new TextDecoder().decode(data);
			return content.trim();
		} catch {
			return null;
		}
	}

	async writeRef(refName: string, oid: string) {
		const path = this.#getRefPath(refName);
		const content = new TextEncoder().encode(oid + "\n");
		await this.#storage.writeFile(path, content);
	}

	async deleteRef(refName: string) {
		try {
			const path = this.#getRefPath(refName);
			await this.#storage.deleteFile(path);
		} catch {
			// Ref doesn't exist
		}
	}

	async getAllRefs(): Promise<GitRef[]> {
		const refs: GitRef[] = [];

		try {
			// Get all files from refs directory
			const refsPath = ".git/refs";
			await this.#walkRefs(refsPath, "refs", refs);
		} catch {
			// No refs directory or empty
		}

		return refs;
	}

	async #walkRefs(dirPath: string, prefix: string, refs: GitRef[]) {
		try {
			const entries = await this.#storage.listDirectory(dirPath);

			for (const entry of entries) {
				const entryPath = `${dirPath}/${entry}`;
				const fullRefName = `${prefix}/${entry}`;

				try {
					// Try to read as file first
					const content = await this.#storage.readFile(entryPath);
					const oid = new TextDecoder().decode(content).trim();
					refs.push({
						name: fullRefName,
						oid,
					});
				} catch {
					// Not a file, might be a directory
					try {
						await this.#walkRefs(entryPath, fullRefName, refs);
					} catch {
						// Neither file nor directory, skip
					}
				}
			}
		} catch {
			// Directory doesn't exist or can't be read
		}
	}

	#getRefPath(refName: string): string {
		// Remove 'refs/' prefix if present
		const cleanRefName = refName.startsWith("refs/") ? refName : `refs/${refName}`;

		return `.git/${cleanRefName}`;
	}
}
