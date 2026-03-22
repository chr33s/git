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
  applyRefChanges?(
    changes: GitStorageRefChange[],
    options?: { atomic?: boolean },
  ): Promise<GitStorageRefChangeResult[]>;
  readReflog?(refName: string): Promise<GitReflogEntry[]>;
  listReflogRefs?(): Promise<string[]>;
}

export interface GitReflogEntry {
  oldOid: string;
  newOid: string;
  timestamp: string;
  message: string;
}

export interface GitStorageRefChange {
  refName: string;
  path: string;
  newValue: string | null;
  expectedOid?: string | null;
  reflogEntry?: GitReflogEntry;
}

export interface GitStorageRefChangeResult {
  refName: string;
  applied: boolean;
  currentOid: string | null;
  currentValue: string | null;
}

interface MemoryStorageState {
  directories: Set<string>;
  files: Map<string, Uint8Array>;
}

export class MemoryStorage implements GitStorage {
  #repositories = new Map<string, MemoryStorageState>();
  #currentRepository: MemoryStorageState | null = null;

  async init(repositoryName: string) {
    let repository = this.#repositories.get(repositoryName);
    if (!repository) {
      repository = {
        directories: new Set([".git"]),
        files: new Map(),
      };
      this.#repositories.set(repositoryName, repository);
    }

    this.#currentRepository = repository;
  }

  async exists(path: string) {
    const repository = this.#requireRepository();

    return repository.files.has(path) || repository.directories.has(path);
  }

  async readFile(path: string) {
    const repository = this.#requireRepository();

    const data = repository.files.get(path);
    if (!data) throw new Error(`File not found: ${path}`);

    return new Uint8Array(data);
  }

  async writeFile(path: string, data: Uint8Array) {
    const repository = this.#requireRepository();

    // Ensure parent directories exist
    const parts = path.split("/");
    for (let i = 1; i < parts.length; i++) {
      const dir = parts.slice(0, i).join("/");
      repository.directories.add(dir);
    }

    repository.files.set(path, new Uint8Array(data));
  }

  async deleteFile(path: string) {
    const repository = this.#requireRepository();

    if (!repository.files.has(path)) {
      throw new Error(`File not found: ${path}`);
    }

    repository.files.delete(path);
  }

  async createDirectory(path: string) {
    const repository = this.#requireRepository();

    repository.directories.add(path);
  }

  async listDirectory(path: string) {
    const repository = this.#requireRepository();

    const items = new Set<string>();

    // Find all direct children
    const prefix = path.endsWith("/") ? path : path + "/";
    for (const filePath of repository.files.keys()) {
      if (filePath.startsWith(prefix)) {
        const relative = filePath.slice(prefix.length);
        const firstPart = relative.split("/")[0];
        if (firstPart) items.add(firstPart);
      }
    }

    for (const dirPath of repository.directories) {
      if (dirPath.startsWith(prefix)) {
        const relative = dirPath.slice(prefix.length);
        const firstPart = relative.split("/")[0];
        if (firstPart) items.add(firstPart);
      }
    }

    return Array.from(items);
  }

  async deleteDirectory(path: string) {
    const repository = this.#requireRepository();

    const prefix = path.endsWith("/") ? path : path + "/";

    // Delete all files in this directory
    for (const filePath of Array.from(repository.files.keys())) {
      if (filePath.startsWith(prefix)) {
        repository.files.delete(filePath);
      }
    }

    // Delete all subdirectories
    for (const dirPath of Array.from(repository.directories)) {
      if (dirPath.startsWith(prefix) || dirPath === path) {
        repository.directories.delete(dirPath);
      }
    }
  }

  async getFileInfo(path: string) {
    const repository = this.#requireRepository();

    const data = repository.files.get(path);
    if (!data) throw new Error(`File not found: ${path}`);

    return {
      size: data.length,
      lastModified: new Date(),
    };
  }

  #requireRepository() {
    if (!this.#currentRepository) throw new Error("Storage not initialized");
    return this.#currentRepository;
  }
}
