/**
 * An in-memory fake of the `FileSystemDirectoryHandle` surface
 * `adapters/Opfs.ts` uses — node has no OPFS, so tests stand this in for a
 * browser origin. Deliberately not a `*.test.ts` file: the contract suite and
 * the client tests both build on it.
 */

const concat = (parts: Uint8Array[]): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

const missing = (name: string) => new DOMException(`'${name}' not found`, "NotFoundError");

class FakeFile {
  readonly kind = "file" as const;
  readonly name: string;
  #data: Uint8Array<ArrayBuffer> = new Uint8Array(0);
  constructor(name: string) {
    this.name = name;
  }

  getFile() {
    const bytes = this.#data;
    return Promise.resolve({
      arrayBuffer: () => Promise.resolve(bytes.slice().buffer),
    });
  }

  createWritable() {
    const chunks: Uint8Array[] = [];
    return Promise.resolve({
      write: (chunk: Uint8Array) => {
        chunks.push(new Uint8Array(chunk));
        return Promise.resolve();
      },
      // The real API stages into a swap file and replaces on close; the fake
      // mirrors the observable half of that: nothing lands until `close`.
      close: () => {
        this.#data = concat(chunks);
        return Promise.resolve();
      },
    });
  }
}

class FakeDirectory {
  readonly kind = "directory" as const;
  readonly name: string;
  readonly #entries = new Map<string, FakeDirectory | FakeFile>();
  constructor(name = "") {
    this.name = name;
  }

  getDirectoryHandle(name: string, options?: { create?: boolean }) {
    const existing = this.#entries.get(name);
    if (existing instanceof FakeDirectory) return Promise.resolve(existing);
    if (existing !== undefined || options?.create !== true) {
      return Promise.reject(missing(name));
    }
    const directory = new FakeDirectory(name);
    this.#entries.set(name, directory);
    return Promise.resolve(directory);
  }

  getFileHandle(name: string, options?: { create?: boolean }) {
    const existing = this.#entries.get(name);
    if (existing instanceof FakeFile) return Promise.resolve(existing);
    if (existing !== undefined || options?.create !== true) {
      return Promise.reject(missing(name));
    }
    const file = new FakeFile(name);
    this.#entries.set(name, file);
    return Promise.resolve(file);
  }

  removeEntry(name: string) {
    return this.#entries.delete(name) ? Promise.resolve() : Promise.reject(missing(name));
  }

  async *entries(): AsyncIterableIterator<[string, FakeDirectory | FakeFile]> {
    yield* this.#entries.entries();
  }
}

export const fakeRoot = () => {
  // SAFETY: the fake implements exactly the directory surface
  // `adapters/Opfs.ts` reaches for; the intersection lets it stand in for the
  // DOM handle type that node never provides at runtime.
  return new FakeDirectory() as FakeDirectory & FileSystemDirectoryHandle;
};
