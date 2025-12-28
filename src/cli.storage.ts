import { promises as fs } from "node:fs";
import { dirname, join } from "node:path";

import { type GitStorage } from "./git.storage.ts";

export class FsStorage implements GitStorage {
	#rootPath?: string;

	async init(_repositoryName: string): Promise<void> {
		// Use current working directory as the repository root
		this.#rootPath = process.cwd();

		// Ensure .git directory exists
		const gitPath = join(this.#rootPath, ".git");
		try {
			await fs.access(gitPath);
		} catch {
			await fs.mkdir(gitPath, { recursive: true });
		}
	}

	async exists(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		try {
			await fs.access(join(this.#rootPath, path));
			return true;
		} catch {
			return false;
		}
	}

	async readFile(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		const buffer = await fs.readFile(fullPath);
		return new Uint8Array(buffer);
	}

	async writeFile(path: string, data: Uint8Array) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		const dir = dirname(fullPath);

		// Ensure directory exists
		await fs.mkdir(dir, { recursive: true });

		// Convert Uint8Array to Buffer for Node.js
		const buffer = Buffer.from(data);
		await fs.writeFile(fullPath, buffer);
	}

	async deleteFile(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		await fs.unlink(fullPath);
	}

	async createDirectory(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		await fs.mkdir(fullPath, { recursive: true });
	}

	async listDirectory(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		return await fs.readdir(fullPath);
	}

	async deleteDirectory(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		await fs.rm(fullPath, { recursive: true });
	}

	async getFileInfo(path: string) {
		if (!this.#rootPath) throw new Error("Storage not initialized");

		const fullPath = join(this.#rootPath, path);
		const stats = await fs.stat(fullPath);

		return {
			size: stats.size,
			lastModified: stats.mtime,
		};
	}
}
