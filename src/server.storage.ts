import { type GitStorage } from "./git.storage.ts";

export class CloudflareStorage implements GitStorage {
	#repoName?: string;
	#r2: R2Bucket;
	#sql: SqlStorage;

	constructor(ctx: DurableObjectState, env: Env) {
		this.#sql = ctx.storage.sql;
		this.#r2 = env.GIT_OBJECTS;
	}

	async init(repoName: string): Promise<void> {
		this.#repoName = repoName;

		// Ensure database tables exist
		this.#sql.exec(/* SQL */ `
			CREATE TABLE IF NOT EXISTS git_files (
				repository TEXT NOT NULL,
				path TEXT NOT NULL,
				size INTEGER NOT NULL,
				last_modified DATETIME NOT NULL,
				r2_key TEXT NOT NULL,
				PRIMARY KEY (repository, path)
			);
			
			CREATE INDEX IF NOT EXISTS idx_git_files_repo 
			ON git_files(repository);
			
			CREATE INDEX IF NOT EXISTS idx_git_files_path 
			ON git_files(repository, path);
		`);
	}

	async exists(path: string): Promise<boolean> {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const rows = this.#sql
			.exec(
				/* SQL */ `SELECT 1 FROM git_files WHERE repository = ? AND path = ? LIMIT 1`,
				this.#repoName,
				path,
			)
			.toArray();

		return rows.length > 0;
	}

	async readFile(path: string): Promise<Uint8Array> {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const key = this.#key(path);
		const object = await this.#r2.get(key);

		if (!object) {
			throw new Error(`File not found: ${path}`);
		}

		return new Uint8Array(await object.arrayBuffer());
	}

	async writeFile(path: string, data: Uint8Array) {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const key = this.#key(path);
		const now = new Date().toISOString();

		await this.#r2.put(key, data);

		this.#sql.exec(
			/* SQL */ `
				INSERT OR REPLACE INTO git_files 
				(repository, path, size, last_modified, r2_key)
				VALUES (?, ?, ?, ?, ?)
			`,
			this.#repoName,
			path,
			data.length,
			now,
			key,
		);
	}

	async deleteFile(path: string) {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const key = this.#key(path);
		await this.#r2.delete(key);

		this.#sql.exec(
			/* SQL */ `DELETE FROM git_files WHERE repository = ? AND path = ?`,
			this.#repoName,
			path,
		);
	}

	async createDirectory(_path: string) {
		// R2 doesn't require explicit directory creation
		// Directories are implicit in object keys
	}

	async listDirectory(path: string): Promise<string[]> {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const pathPattern = this.#pattern(path);
		const normalizedPath = path.replace(/\/$/, "");
		const prefixLength = normalizedPath.length + 1; // +1 for the trailing slash

		const rows = this.#sql
			.exec(
				/* SQL */ `
					SELECT path FROM git_files 
					WHERE repository = ? 
					AND path LIKE ?
				`,
				this.#repoName,
				pathPattern,
			)
			.toArray();

		// Extract immediate children (files or virtual directories)
		const children = new Set<string>();
		for (const row of rows) {
			const fullPath = (row as any).path as string;
			// Get the part after the prefix
			const remainder = fullPath.slice(prefixLength);
			// Get the first path component
			const slashIndex = remainder.indexOf("/");
			const child = slashIndex === -1 ? remainder : remainder.slice(0, slashIndex);
			if (child) {
				children.add(child);
			}
		}

		return Array.from(children);
	}

	async deleteDirectory(path: string) {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const pathPattern = this.#pattern(path);

		const files = this.#sql
			.exec(
				/* SQL */ `
					SELECT path, r2_key FROM git_files 
					WHERE repository = ? AND path LIKE ?
				`,
				this.#repoName,
				pathPattern,
			)
			.toArray();

		await Promise.all(files.map((file) => this.#r2.delete((file as any).r2_key)));

		this.#sql.exec(
			/* SQL */ `
				DELETE FROM git_files 
				WHERE repository = ? AND path LIKE ?
			`,
			this.#repoName,
			pathPattern,
		);
	}

	async getFileInfo(path: string): Promise<{ size: number; lastModified: Date }> {
		if (!this.#repoName) throw new Error("Storage not initialized");

		const result = this.#sql
			.exec(
				/* SQL */ `
					SELECT size, last_modified FROM git_files 
					WHERE repository = ? AND path = ?
				`,
				this.#repoName,
				path,
			)
			.one();

		if (!result) {
			throw new Error(`File not found: ${path}`);
		}

		return {
			size: result.size as number,
			lastModified: new Date(result.last_modified as string),
		};
	}

	#pattern(path: string) {
		return path ? `${path.replace(/\/$/, "")}/%` : "%";
	}

	#key(path: string): string {
		return `${this.#repoName}/${path}`;
	}
}
