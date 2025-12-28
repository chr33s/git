export interface GitStorage {
	init(repositoryName: string): Promise<void>;
	exists(path: string): Promise<boolean>;
	readFile(path: string): Promise<Uint8Array>;
	writeFile(path: string, data: Uint8Array): Promise<void>;
	deleteFile(path: string): Promise<void>;
	createDirectory(path: string): Promise<void>;
	listDirectory(path: string): Promise<string[]>;
	deleteDirectory(path: string): Promise<void>;
	getFileInfo(path: string): Promise<{ size: number; lastModified: Date }>;
}

export class MemoryStorage implements GitStorage {
	#files = new Map<string, Uint8Array>();
	#directories = new Set<string>();
	#initialized = false;

	async init(_repositoryName: string): Promise<void> {
		this.#directories.add(".git");
		this.#initialized = true;
	}

	async exists(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		return this.#files.has(path) || this.#directories.has(path);
	}

	async readFile(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		const data = this.#files.get(path);
		if (!data) throw new Error(`File not found: ${path}`);

		return new Uint8Array(data);
	}

	async writeFile(path: string, data: Uint8Array) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		// Ensure parent directories exist
		const parts = path.split("/");
		for (let i = 1; i < parts.length; i++) {
			const dir = parts.slice(0, i).join("/");
			this.#directories.add(dir);
		}

		this.#files.set(path, new Uint8Array(data));
	}

	async deleteFile(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		if (!this.#files.has(path)) {
			throw new Error(`File not found: ${path}`);
		}

		this.#files.delete(path);
	}

	async createDirectory(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		this.#directories.add(path);
	}

	async listDirectory(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		const items = new Set<string>();

		// Find all direct children
		const prefix = path.endsWith("/") ? path : path + "/";
		for (const filePath of this.#files.keys()) {
			if (filePath.startsWith(prefix)) {
				const relative = filePath.slice(prefix.length);
				const firstPart = relative.split("/")[0];
				if (firstPart) items.add(firstPart);
			}
		}

		for (const dirPath of this.#directories) {
			if (dirPath.startsWith(prefix)) {
				const relative = dirPath.slice(prefix.length);
				const firstPart = relative.split("/")[0];
				if (firstPart) items.add(firstPart);
			}
		}

		return Array.from(items);
	}

	async deleteDirectory(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		const prefix = path.endsWith("/") ? path : path + "/";

		// Delete all files in this directory
		for (const filePath of this.#files.keys()) {
			if (filePath.startsWith(prefix)) {
				this.#files.delete(filePath);
			}
		}

		// Delete all subdirectories
		for (const dirPath of this.#directories) {
			if (dirPath.startsWith(prefix) || dirPath === path) {
				this.#directories.delete(dirPath);
			}
		}
	}

	async getFileInfo(path: string) {
		if (!this.#initialized) throw new Error("Storage not initialized");

		const data = this.#files.get(path);
		if (!data) throw new Error(`File not found: ${path}`);

		return {
			size: data.length,
			lastModified: new Date(),
		};
	}
}
