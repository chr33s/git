import { type GitStorage } from "./git.storage.ts";

export class OpfsStorage implements GitStorage {
	#rootHandle?: FileSystemDirectoryHandle;
	#repositoryHandle?: FileSystemDirectoryHandle;

	async init(repositoryName: string): Promise<void> {
		this.#rootHandle = await navigator.storage.getDirectory();
		this.#repositoryHandle = await this.#rootHandle.getDirectoryHandle(repositoryName, {
			create: true,
		});
	}

	get repositoryHandle(): FileSystemDirectoryHandle | undefined {
		return this.#repositoryHandle;
	}

	async exists(path: string): Promise<boolean> {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		try {
			await this.#getHandle(path);
			return true;
		} catch {
			return false;
		}
	}

	async readFile(path: string): Promise<Uint8Array> {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const handle = await this.#getHandle(path);
		if (handle.kind !== "file") {
			throw new Error(`Path ${path} is not a file`);
		}

		const fileHandle = handle as FileSystemFileHandle;
		const file = await fileHandle.getFile();
		return new Uint8Array(await file.arrayBuffer());
	}

	async writeFile(path: string, data: Uint8Array) {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const parts = path.split("/");
		const fileName = parts.pop()!;
		const dirPath = parts.join("/");

		// Ensure directory exists
		if (dirPath) {
			await this.createDirectory(dirPath);
		}

		const dirHandle = dirPath ? await this.#getDirectoryHandle(dirPath) : this.#repositoryHandle;

		const fileHandle = await dirHandle.getFileHandle(fileName, {
			create: true,
		});
		const writable = await fileHandle.createWritable();
		// Ensure we have an ArrayBuffer, not SharedArrayBuffer
		const buffer =
			data.buffer instanceof ArrayBuffer
				? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
				: new ArrayBuffer(data.length);
		if (!(data.buffer instanceof ArrayBuffer)) {
			new Uint8Array(buffer).set(data);
		}
		await writable.write(buffer);
		await writable.close();
	}

	async deleteFile(path: string) {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const parts = path.split("/");
		const fileName = parts.pop()!;
		const dirPath = parts.join("/");

		const dirHandle = dirPath ? await this.#getDirectoryHandle(dirPath) : this.#repositoryHandle;

		await dirHandle.removeEntry(fileName);
	}

	async createDirectory(path: string) {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const parts = path.split("/").filter((p) => p);
		let _current = this.#repositoryHandle;

		for (const part of parts) {
			_current = await _current.getDirectoryHandle(part, { create: true });
		}
	}

	async listDirectory(path: string): Promise<string[]> {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const dirHandle = path ? await this.#getDirectoryHandle(path) : this.#repositoryHandle;

		const entries: string[] = [];
		// @ts-ignore - Using experimental FileSystemDirectoryHandle iteration
		for await (const [name] of dirHandle.entries()) {
			entries.push(name);
		}

		return entries;
	}

	async deleteDirectory(path: string) {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const parts = path.split("/");
		const dirName = parts.pop()!;
		const parentPath = parts.join("/");

		const parentHandle = parentPath
			? await this.#getDirectoryHandle(parentPath)
			: this.#repositoryHandle;

		await parentHandle.removeEntry(dirName, { recursive: true });
	}

	async getFileInfo(path: string): Promise<{ size: number; lastModified: Date }> {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const handle = await this.#getHandle(path);
		if (handle.kind !== "file") {
			throw new Error(`Path ${path} is not a file`);
		}

		const fileHandle = handle as FileSystemFileHandle;
		const file = await fileHandle.getFile();
		return {
			size: file.size,
			lastModified: new Date(file.lastModified),
		};
	}

	async #getHandle(path: string): Promise<FileSystemHandle> {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const parts = path.split("/").filter((p) => p);
		let current: FileSystemDirectoryHandle = this.#repositoryHandle;

		for (let i = 0; i < parts.length - 1; i++) {
			const part = parts[i];
			if (!part) throw new Error(`Invalid path: ${path}`);
			current = await current.getDirectoryHandle(part);
		}

		const lastPart = parts[parts.length - 1];
		if (!lastPart) return current;

		try {
			return await current.getFileHandle(lastPart);
		} catch {
			return await current.getDirectoryHandle(lastPart);
		}
	}

	async #getDirectoryHandle(path: string): Promise<FileSystemDirectoryHandle> {
		if (!this.#repositoryHandle) throw new Error("Storage not initialized");

		const parts = path.split("/").filter((p) => p);
		let current = this.#repositoryHandle;

		for (const part of parts) {
			current = await current.getDirectoryHandle(part);
		}

		return current;
	}
}
