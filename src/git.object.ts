import { ObjectNotFoundError, ValidationError } from "./git.error.ts";
import type { GitStorage } from "./git.storage.ts";
import {
  buildPackIndex,
  findPackIndexEntry,
  parsePackIndex,
  readPackObjectAtOffset,
  type PackIndex,
  type PackIndexEntry,
} from "./git.pack.ts";
import { compressData, createSha1, decompressData } from "./git.utils.ts";

export interface GitObject {
  type: "blob" | "tree" | "commit" | "tag";
  data: Uint8Array;
}

export interface FsckResult {
  errors: string[];
  oid: string;
  type: GitObject["type"] | "unknown";
  valid: boolean;
}

export interface LooseObjectInfo {
  kind: "loose";
  lastModified: Date;
  oid: string;
  path: string;
  size: number;
}

export interface StoredPackInfo {
  idxPath: string;
  idxSize: number;
  index: PackIndex;
  lastModified: Date;
  packName: string;
  packPath: string;
  packSize: number;
}

const VALID_TREE_MODES = new Set([0o40000, 0o100644, 0o100755, 0o120000, 0o160000]);

export class GitObjectStore {
  #storage: GitStorage;

  constructor(storage: GitStorage) {
    this.#storage = storage;
  }

  async init() {
    await this.#storage.createDirectory(".git/objects");
    await this.#storage.createDirectory(".git/objects/pack");
  }

  async readObject(oid: string) {
    if (!this.#isValidOid(oid)) {
      throw new ValidationError(`Object ${oid}: invalid object id`);
    }

    const looseObject = await this.#readLooseObject(oid);
    if (looseObject) {
      return looseObject;
    }

    const packedObject = await this.#readPackedObject(oid);
    if (packedObject) {
      return packedObject;
    }

    throw new ObjectNotFoundError(oid);
  }

  async writeObject(type: GitObject["type"], data: Uint8Array) {
    const header = new TextEncoder().encode(`${type} ${data.length}\0`);
    const content = new Uint8Array(header.length + data.length);
    content.set(header);
    content.set(data, header.length);

    const oid = await createSha1(content);
    const compressed = await compressData(content);

    const dir = oid.slice(0, 2);
    const file = oid.slice(2);
    const path = `.git/objects/${dir}/${file}`;

    await this.#storage.writeFile(path, compressed);
    return oid;
  }

  async writePack(packData: Uint8Array, indexEntries: PackIndexEntry[]) {
    if (packData.length < 20) {
      throw new Error("Invalid pack data");
    }

    const packChecksum = packData.slice(packData.length - 20);
    const packName = `pack-${Array.from(packChecksum)
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("")}`;
    const packPath = `.git/objects/pack/${packName}.pack`;
    const idxPath = `.git/objects/pack/${packName}.idx`;
    const { data: idxData } = await buildPackIndex(indexEntries, packChecksum);

    await this.#storage.writeFile(packPath, packData);
    await this.#storage.writeFile(idxPath, idxData);

    return { idxPath, packPath };
  }

  async hasObject(oid: string) {
    if (!this.#isValidOid(oid)) {
      return false;
    }

    const dir = oid.slice(0, 2);
    const file = oid.slice(2);
    if (await this.#storage.exists(`.git/objects/${dir}/${file}`)) {
      return true;
    }

    return (await this.#findPackedObjectLocation(oid)) !== null;
  }

  async readPackIndex(path: string) {
    const data = await this.#storage.readFile(path);
    return parsePackIndex(data);
  }

  async listLooseObjects() {
    const objectDirs = await this.#safeListDirectory(".git/objects");
    const result: LooseObjectInfo[] = [];

    for (const dir of objectDirs) {
      if (!/^[0-9a-f]{2}$/.test(dir)) {
        continue;
      }

      const files = await this.#safeListDirectory(`.git/objects/${dir}`);
      for (const file of files) {
        if (!/^[0-9a-f]{38}$/.test(file)) {
          continue;
        }

        const path = `.git/objects/${dir}/${file}`;
        const info = await this.#storage.getFileInfo(path);
        result.push({
          kind: "loose",
          lastModified: info.lastModified,
          oid: `${dir}${file}`,
          path,
          size: info.size,
        });
      }
    }

    return result;
  }

  async listPackFiles() {
    const entries = await this.#safeListDirectory(".git/objects/pack");
    const packs: StoredPackInfo[] = [];

    for (const entry of entries) {
      if (!/^pack-[0-9a-f]{40}\.idx$/.test(entry)) {
        continue;
      }

      const packName = entry.slice(0, -4);
      const idxPath = `.git/objects/pack/${entry}`;
      const packPath = `.git/objects/pack/${packName}.pack`;
      if (!(await this.#storage.exists(packPath))) {
        continue;
      }

      const [index, packInfo, idxInfo] = await Promise.all([
        this.readPackIndex(idxPath),
        this.#storage.getFileInfo(packPath),
        this.#storage.getFileInfo(idxPath),
      ]);

      packs.push({
        idxPath,
        idxSize: idxInfo.size,
        index,
        lastModified: packInfo.lastModified,
        packName,
        packPath,
        packSize: packInfo.size,
      });
    }

    return packs;
  }

  async listAllObjects() {
    const oids = new Set<string>();

    for (const object of await this.listLooseObjects()) {
      oids.add(object.oid);
    }

    for (const pack of await this.listPackFiles()) {
      for (const entry of pack.index.entries) {
        oids.add(entry.oid);
      }
    }

    return Array.from(oids).sort();
  }

  async validateObject(oid: string): Promise<FsckResult> {
    const errors: string[] = [];
    let object: GitObject;

    try {
      object = await this.readObject(oid);
    } catch (error) {
      return {
        errors: [error instanceof Error ? error.message : String(error)],
        oid,
        type: "unknown",
        valid: false,
      };
    }

    const calculatedOid = await this.#hashObject(object.type, object.data);
    if (calculatedOid !== oid) {
      errors.push(`Object hash mismatch: expected ${oid}, got ${calculatedOid}`);
    }

    if (object.type === "commit") {
      await this.#validateCommit(oid, object.data, errors);
    } else if (object.type === "tree") {
      await this.#validateTree(object.data, errors);
    } else if (object.type === "tag") {
      await this.#validateTag(object.data, errors);
    }

    return {
      errors,
      oid,
      type: object.type,
      valid: errors.length === 0,
    };
  }

  async fsckAll() {
    const results: FsckResult[] = [];

    for (const oid of await this.listAllObjects()) {
      results.push(await this.validateObject(oid));
    }

    return results;
  }

  async #readLooseObject(oid: string) {
    const dir = oid.slice(0, 2);
    const file = oid.slice(2);
    const path = `.git/objects/${dir}/${file}`;

    if (!(await this.#storage.exists(path))) {
      return null;
    }

    const compressed = await this.#storage.readFile(path);
    return await this.#decodeStoredObject(compressed);
  }

  async #readPackedObject(oid: string) {
    const location = await this.#findPackedObjectLocation(oid);
    if (!location) {
      return null;
    }

    const packData = await this.#storage.readFile(location.pack.packPath);
    const cache = new Map<number, Awaited<ReturnType<typeof readPackObjectAtOffset>>>();

    const resolveByOid = async (baseOid: string): Promise<GitObject> => {
      const nestedEntry = findPackIndexEntry(location.pack.index, baseOid);
      if (nestedEntry) {
        const nestedObject = await readPackObjectAtOffset(
          packData,
          nestedEntry.offset,
          resolveByOid,
          cache,
        );
        return {
          data: nestedObject.data,
          type: nestedObject.type,
        };
      }

      return await this.readObject(baseOid);
    };

    const packed = await readPackObjectAtOffset(
      packData,
      location.entry.offset,
      resolveByOid,
      cache,
    );

    return {
      data: packed.data,
      type: packed.type,
    } satisfies GitObject;
  }

  async #findPackedObjectLocation(oid: string) {
    for (const pack of await this.listPackFiles()) {
      const entry = findPackIndexEntry(pack.index, oid);
      if (entry) {
        return { entry, pack };
      }
    }

    return null;
  }

  async #decodeStoredObject(compressed: Uint8Array) {
    const decompressed = await decompressData(compressed);
    const nullIndex = decompressed.indexOf(0);
    if (nullIndex === -1) {
      throw new Error("Invalid object header");
    }

    const header = new TextDecoder().decode(decompressed.slice(0, nullIndex));
    const [type, sizeText] = header.split(" ");
    const data = decompressed.slice(nullIndex + 1);

    if (!type || !sizeText) {
      throw new Error("Invalid object header");
    }

    const size = parseInt(sizeText, 10);
    if (!Number.isFinite(size) || size !== data.length) {
      throw new Error(`Object size mismatch: expected ${sizeText}, got ${data.length}`);
    }

    if (!["blob", "tree", "commit", "tag"].includes(type)) {
      throw new Error(`Unsupported object type '${type}'`);
    }

    return {
      data,
      type: type as GitObject["type"],
    };
  }

  async #hashObject(type: GitObject["type"], data: Uint8Array) {
    const header = new TextEncoder().encode(`${type} ${data.length}\0`);
    const content = new Uint8Array(header.length + data.length);
    content.set(header);
    content.set(data, header.length);
    return await createSha1(content);
  }

  async #validateCommit(oid: string, data: Uint8Array, errors: string[]) {
    const text = new TextDecoder().decode(data);
    const headerText = text.split("\n\n", 1)[0] || text;
    const lines = headerText.split("\n").filter(Boolean);

    const tree = lines.find((line) => line.startsWith("tree "))?.slice(5);
    const author = lines.find((line) => line.startsWith("author "));
    const committer = lines.find((line) => line.startsWith("committer "));
    const parents = lines.filter((line) => line.startsWith("parent ")).map((line) => line.slice(7));

    if (!tree) {
      errors.push(`Commit ${oid} is missing a tree header`);
    } else {
      await this.#validateReferencedObject(tree, "tree", "commit tree", errors);
    }

    if (!author) {
      errors.push(`Commit ${oid} is missing an author header`);
    }

    if (!committer) {
      errors.push(`Commit ${oid} is missing a committer header`);
    }

    for (const parent of parents) {
      await this.#validateReferencedObject(parent, "commit", "commit parent", errors);
    }
  }

  async #validateTree(data: Uint8Array, errors: string[]) {
    let entries: Array<{ mode: string; name: string; oid: string }>;

    try {
      entries = this.#parseTreeEntries(data);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
      return;
    }

    const seenNames = new Set<string>();
    let previousKey: Uint8Array | null = null;

    for (const entry of entries) {
      const parsedMode = parseInt(entry.mode, 8);
      if (!/^[0-7]+$/.test(entry.mode) || !VALID_TREE_MODES.has(parsedMode)) {
        errors.push(`Invalid tree mode '${entry.mode}' for entry '${entry.name}'`);
      }

      if (seenNames.has(entry.name)) {
        errors.push(`Duplicate tree entry '${entry.name}'`);
      }
      seenNames.add(entry.name);

      const sortKey = new TextEncoder().encode(
        parsedMode === 0o40000 ? `${entry.name}/` : entry.name,
      );
      if (previousKey && this.#compareBytes(previousKey, sortKey) > 0) {
        errors.push("Tree entries are not sorted in Git order");
        previousKey = sortKey;
        continue;
      }

      previousKey = sortKey;
    }
  }

  async #validateTag(data: Uint8Array, errors: string[]) {
    const text = new TextDecoder().decode(data);
    const headerText = text.split("\n\n", 1)[0] || text;
    const lines = headerText.split("\n").filter(Boolean);

    const objectOid = lines.find((line) => line.startsWith("object "))?.slice(7);
    const objectType = lines.find((line) => line.startsWith("type "))?.slice(5);
    const tagName = lines.find((line) => line.startsWith("tag "))?.slice(4);
    const tagger = lines.find((line) => line.startsWith("tagger "));

    if (!objectOid) {
      errors.push("Tag is missing an object header");
    } else if (objectType && this.#isGitObjectType(objectType)) {
      await this.#validateReferencedObject(objectOid, objectType, "tag target", errors);
    } else {
      await this.#validateReferencedObject(objectOid, undefined, "tag target", errors);
    }

    if (!objectType) {
      errors.push("Tag is missing a type header");
    } else if (!this.#isGitObjectType(objectType)) {
      errors.push(`Tag references unsupported object type '${objectType}'`);
    }

    if (!tagName) {
      errors.push("Tag is missing a tag header");
    }

    if (!tagger) {
      errors.push("Tag is missing a tagger header");
    }
  }

  async #validateReferencedObject(
    oid: string,
    expectedType: GitObject["type"] | undefined,
    label: string,
    errors: string[],
  ) {
    if (!this.#isValidOid(oid)) {
      errors.push(`Invalid ${label} OID '${oid}'`);
      return;
    }

    try {
      const object = await this.readObject(oid);
      if (expectedType && object.type !== expectedType) {
        errors.push(`Expected ${label} ${oid} to be ${expectedType}, got ${object.type}`);
      }
    } catch {
      errors.push(`Missing ${label} object ${oid}`);
    }
  }

  #parseTreeEntries(data: Uint8Array) {
    const entries: Array<{ mode: string; name: string; oid: string }> = [];
    let offset = 0;

    while (offset < data.length) {
      let spaceIndex = offset;
      while (spaceIndex < data.length && data[spaceIndex] !== 0x20) {
        spaceIndex++;
      }
      if (spaceIndex >= data.length) {
        throw new Error("Malformed tree object: missing space separator");
      }

      let nullIndex = spaceIndex + 1;
      while (nullIndex < data.length && data[nullIndex] !== 0) {
        nullIndex++;
      }
      if (nullIndex >= data.length) {
        throw new Error("Malformed tree object: missing NUL separator");
      }

      if (nullIndex + 21 > data.length) {
        throw new Error("Malformed tree object: truncated object id");
      }

      const mode = new TextDecoder().decode(data.slice(offset, spaceIndex));
      const name = new TextDecoder().decode(data.slice(spaceIndex + 1, nullIndex));
      const oid = Array.from(data.slice(nullIndex + 1, nullIndex + 21))
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");

      entries.push({ mode, name, oid });
      offset = nullIndex + 21;
    }

    return entries;
  }

  #compareBytes(left: Uint8Array, right: Uint8Array) {
    const length = Math.min(left.length, right.length);
    for (let index = 0; index < length; index++) {
      const delta = (left[index] ?? 0) - (right[index] ?? 0);
      if (delta !== 0) {
        return delta;
      }
    }

    return left.length - right.length;
  }

  #isGitObjectType(value: string): value is GitObject["type"] {
    return value === "blob" || value === "tree" || value === "commit" || value === "tag";
  }

  #isValidOid(oid: string) {
    return /^[0-9a-f]{40}$/.test(oid);
  }

  async #safeListDirectory(path: string) {
    try {
      return await this.#storage.listDirectory(path);
    } catch {
      return [];
    }
  }
}
