import { GitDelta } from "./git.delta.ts";
import {
  bytesToHex,
  compressData,
  concatenateUint8Arrays,
  createSha1,
  decompressData,
  hexToBytes,
} from "./git.utils.ts";

interface PackObject {
  type: "commit" | "tree" | "blob" | "tag" | "ofs_delta" | "ref_delta";
  size: number;
  data: Uint8Array;
  offset: number;
  crc32: number;
}

interface DeltaObject extends PackObject {
  baseOffset?: number;
  baseOid?: string;
}

interface StoredObject {
  type: "blob" | "tree" | "commit" | "tag";
  data: Uint8Array;
}

export interface PackIndexEntry {
  crc32: number;
  offset: number;
  oid: string;
}

export interface PackIndex {
  checksum: string;
  entries: PackIndexEntry[];
  fanoutTable: number[];
  packChecksum: string;
  version: 2;
}

export interface ResolvedPackObject extends StoredObject {
  crc32: number;
  nextOffset: number;
  offset: number;
}

const PACK_SIGNATURE = "PACK";
const PACK_INDEX_MAGIC = new Uint8Array([0xff, 0x74, 0x4f, 0x63]);
const PACK_TRAILER_SIZE = 20;

function readUint32BE(buffer: Uint8Array, offset: number) {
  return (
    (((buffer[offset] ?? 0) << 24) |
      ((buffer[offset + 1] ?? 0) << 16) |
      ((buffer[offset + 2] ?? 0) << 8) |
      (buffer[offset + 3] ?? 0)) >>>
    0
  );
}

function writeUint32BE(value: number) {
  const bytes = new Uint8Array(4);
  bytes[0] = (value >>> 24) & 0xff;
  bytes[1] = (value >>> 16) & 0xff;
  bytes[2] = (value >>> 8) & 0xff;
  bytes[3] = value & 0xff;
  return bytes;
}

function writeUint64BE(value: number) {
  let remaining = BigInt(value);
  const bytes = new Uint8Array(8);

  for (let index = 7; index >= 0; index--) {
    bytes[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }

  return bytes;
}

function readUint64BE(buffer: Uint8Array, offset: number) {
  let value = 0n;

  for (let index = 0; index < 8; index++) {
    value = (value << 8n) | BigInt(buffer[offset + index] ?? 0);
  }

  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("Pack offset exceeds JavaScript safe integer range");
  }

  return Number(value);
}

function compareChecksums(a: Uint8Array, b: Uint8Array) {
  if (a.length !== b.length) {
    return false;
  }

  for (let index = 0; index < a.length; index++) {
    if (a[index] !== b[index]) {
      return false;
    }
  }

  return true;
}

function compareLexicographically(left: string, right: string) {
  if (left === right) {
    return 0;
  }

  return left < right ? -1 : 1;
}

function getCRC32Table() {
  const table = new Uint32Array(256);

  for (let index = 0; index < 256; index++) {
    let value = index;

    for (let inner = 0; inner < 8; inner++) {
      value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
    }

    table[index] = value;
  }

  return table;
}

function crc32(data: Uint8Array) {
  const table = getCRC32Table();
  let crc = 0xffffffff;

  for (let index = 0; index < data.length; index++) {
    const byte = data[index];
    if (byte === undefined) {
      continue;
    }

    const tableValue = table[(crc ^ byte) & 0xff];
    if (tableValue !== undefined) {
      crc = (crc >>> 8) ^ tableValue;
    }
  }

  return (crc ^ 0xffffffff) >>> 0;
}

async function calculateObjectOid(type: StoredObject["type"], data: Uint8Array) {
  const header = new TextEncoder().encode(`${type} ${data.length}\0`);
  const content = new Uint8Array(header.length + data.length);
  content.set(header);
  content.set(data, header.length);
  return await createSha1(content);
}

async function calculateChecksum(data: Uint8Array) {
  return hexToBytes(await createSha1(data));
}

function isPlausiblePackBoundary(buffer: Uint8Array, offset: number) {
  const trailerOffset = buffer.length - PACK_TRAILER_SIZE;

  if (offset === trailerOffset) {
    return true;
  }

  if (offset < 0 || offset > trailerOffset) {
    return false;
  }

  let cursor = offset;
  let byte = buffer[cursor++];
  if (byte === undefined) {
    return false;
  }

  const type = (byte >> 4) & 0x7;
  if (type !== 1 && type !== 2 && type !== 3 && type !== 4 && type !== 6 && type !== 7) {
    return false;
  }

  while (byte & 0x80) {
    byte = buffer[cursor++];
    if (byte === undefined) {
      return false;
    }
  }

  if (type === 6) {
    byte = buffer[cursor++];
    if (byte === undefined) {
      return false;
    }

    while (byte & 0x80) {
      byte = buffer[cursor++];
      if (byte === undefined) {
        return false;
      }
    }
  }

  if (type === 7) {
    return cursor + 20 <= trailerOffset;
  }

  return true;
}

async function readCompressedData(
  buffer: Uint8Array,
  offset: number,
  options?: {
    expectedSize?: number;
  },
) {
  const maxEnd = buffer.length - PACK_TRAILER_SIZE;
  let lastError: Error | null = null;
  let lastValid: { data: Uint8Array; nextOffset: number } | null = null;

  for (let compressedEnd = offset + 2; compressedEnd <= maxEnd; compressedEnd++) {
    try {
      const compressed = buffer.slice(offset, compressedEnd);
      const decompressed = await decompressData(compressed);

      if (options?.expectedSize !== undefined && decompressed.length !== options.expectedSize) {
        continue;
      }

      if (!isPlausiblePackBoundary(buffer, compressedEnd)) {
        continue;
      }

      lastValid = {
        data: decompressed,
        nextOffset: compressedEnd,
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      if (lastValid && lastError.message.includes("Trailing bytes after end of compressed data")) {
        return lastValid;
      }
    }
  }

  if (lastValid) {
    return lastValid;
  }

  const details = [
    `offset=${offset}`,
    options?.expectedSize !== undefined ? `expectedSize=${options.expectedSize}` : null,
    lastError ? `lastError=${lastError.message}` : null,
  ]
    .filter((value): value is string => value !== null)
    .join(", ");

  throw new Error(
    `Unable to locate end of compressed pack object${details ? ` (${details})` : ""}`,
    { cause: lastError || undefined },
  );
}

export function findPackIndexEntry(index: PackIndex, oid: string) {
  let low = 0;
  let high = index.entries.length - 1;

  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const entry = index.entries[middle];
    if (!entry) {
      break;
    }

    const comparison = compareLexicographically(entry.oid, oid);
    if (comparison === 0) {
      return entry;
    }

    if (comparison < 0) {
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }

  return null;
}

export async function readPackObjectAtOffset(
  packData: Uint8Array,
  offset: number,
  resolveByOid: (oid: string) => Promise<StoredObject>,
  cache: Map<number, ResolvedPackObject> = new Map(),
): Promise<ResolvedPackObject> {
  const cached = cache.get(offset);
  if (cached) {
    return cached;
  }

  const startOffset = offset;
  let byte = packData[offset++];
  if (byte === undefined) {
    throw new Error("Unexpected end of pack data");
  }

  const type = (byte >> 4) & 0x7;
  let size = byte & 0xf;
  let shift = 4;

  while (byte & 0x80) {
    byte = packData[offset++];
    if (byte === undefined) {
      throw new Error("Unexpected end of pack data");
    }

    size |= (byte & 0x7f) << shift;
    shift += 7;
  }

  const typeNames = ["", "commit", "tree", "blob", "tag", "", "ofs_delta", "ref_delta"];
  const typeName = typeNames[type];
  if (!typeName) {
    throw new Error(`Unsupported pack object type ${type}`);
  }

  if (typeName === "ofs_delta") {
    let baseDistance = 0;
    byte = packData[offset++];
    if (byte === undefined) {
      throw new Error("Unexpected end of pack data");
    }

    baseDistance = byte & 0x7f;

    while (byte & 0x80) {
      byte = packData[offset++];
      if (byte === undefined) {
        throw new Error("Unexpected end of pack data");
      }

      baseDistance = ((baseDistance + 1) << 7) | (byte & 0x7f);
    }

    const baseOffset = startOffset - baseDistance;
    const deltaData = await readCompressedData(packData, offset, { expectedSize: size });
    const baseObject = await readPackObjectAtOffset(packData, baseOffset, resolveByOid, cache);
    const data = GitDelta.applyDelta(baseObject.data, deltaData.data);

    const resolved = {
      crc32: crc32(packData.slice(startOffset, deltaData.nextOffset)),
      data,
      nextOffset: deltaData.nextOffset,
      offset: startOffset,
      type: baseObject.type,
    } satisfies ResolvedPackObject;
    cache.set(startOffset, resolved);
    return resolved;
  }

  if (typeName === "ref_delta") {
    const baseOid = bytesToHex(packData.slice(offset, offset + 20));
    offset += 20;

    const deltaData = await readCompressedData(packData, offset, { expectedSize: size });
    const baseObject = await resolveByOid(baseOid);
    const data = GitDelta.applyDelta(baseObject.data, deltaData.data);

    const resolved = {
      crc32: crc32(packData.slice(startOffset, deltaData.nextOffset)),
      data,
      nextOffset: deltaData.nextOffset,
      offset: startOffset,
      type: baseObject.type,
    } satisfies ResolvedPackObject;
    cache.set(startOffset, resolved);
    return resolved;
  }

  const objectData = await readCompressedData(packData, offset, { expectedSize: size });
  const resolved = {
    crc32: crc32(packData.slice(startOffset, objectData.nextOffset)),
    data: objectData.data,
    nextOffset: objectData.nextOffset,
    offset: startOffset,
    type: typeName as StoredObject["type"],
  } satisfies ResolvedPackObject;
  cache.set(startOffset, resolved);
  return resolved;
}

export function parsePackIndex(data: Uint8Array): PackIndex {
  if (!compareChecksums(data.slice(0, 4), PACK_INDEX_MAGIC)) {
    throw new Error("Invalid pack index signature");
  }

  const version = readUint32BE(data, 4);
  if (version !== 2) {
    throw new Error(`Unsupported pack index version ${version}`);
  }

  let offset = 8;
  const fanoutTable: number[] = [];
  for (let index = 0; index < 256; index++) {
    fanoutTable.push(readUint32BE(data, offset));
    offset += 4;
  }

  const objectCount = fanoutTable[255] || 0;
  const oids: string[] = [];
  for (let index = 0; index < objectCount; index++) {
    oids.push(bytesToHex(data.slice(offset, offset + 20)));
    offset += 20;
  }

  const crcValues: number[] = [];
  for (let index = 0; index < objectCount; index++) {
    crcValues.push(readUint32BE(data, offset));
    offset += 4;
  }

  const rawOffsets: number[] = [];
  const largeOffsetPositions: number[] = [];
  for (let index = 0; index < objectCount; index++) {
    const value = readUint32BE(data, offset);
    rawOffsets.push(value);
    if (value & 0x80000000) {
      largeOffsetPositions.push(index);
    }
    offset += 4;
  }

  const largeOffsets: number[] = [];
  for (let index = 0; index < largeOffsetPositions.length; index++) {
    largeOffsets.push(readUint64BE(data, offset));
    offset += 8;
  }

  const entries: PackIndexEntry[] = [];
  for (let index = 0; index < objectCount; index++) {
    const rawOffset = rawOffsets[index] || 0;
    const entryOffset =
      rawOffset & 0x80000000 ? largeOffsets[rawOffset & 0x7fffffff] || 0 : rawOffset;

    entries.push({
      crc32: crcValues[index] || 0,
      offset: entryOffset,
      oid: oids[index] || "",
    });
  }

  const packChecksum = bytesToHex(data.slice(offset, offset + 20));
  offset += 20;
  const checksum = bytesToHex(data.slice(offset, offset + 20));

  return {
    checksum,
    entries,
    fanoutTable,
    packChecksum,
    version: 2,
  };
}

export async function buildPackIndex(entries: PackIndexEntry[], packChecksum: Uint8Array) {
  const sortedEntries = [...entries].sort((left, right) =>
    compareLexicographically(left.oid, right.oid),
  );
  const fanoutTable = Array.from({ length: 256 }, () => 0);

  for (const entry of sortedEntries) {
    const bucket = parseInt(entry.oid.slice(0, 2), 16);
    for (let index = bucket; index < 256; index++) {
      fanoutTable[index] = (fanoutTable[index] ?? 0) + 1;
    }
  }

  const oidBytes = concatenateUint8Arrays(sortedEntries.map((entry) => hexToBytes(entry.oid)));
  const crcBytes = concatenateUint8Arrays(
    sortedEntries.map((entry) => writeUint32BE(entry.crc32 >>> 0)),
  );

  const offsetChunks: Uint8Array[] = [];
  const largeOffsetChunks: Uint8Array[] = [];
  let largeOffsetIndex = 0;

  for (const entry of sortedEntries) {
    if (entry.offset >= 0x80000000) {
      offsetChunks.push(writeUint32BE(0x80000000 | largeOffsetIndex));
      largeOffsetChunks.push(writeUint64BE(entry.offset));
      largeOffsetIndex++;
    } else {
      offsetChunks.push(writeUint32BE(entry.offset));
    }
  }

  const body = concatenateUint8Arrays([
    PACK_INDEX_MAGIC,
    writeUint32BE(2),
    concatenateUint8Arrays(fanoutTable.map((count) => writeUint32BE(count))),
    oidBytes,
    crcBytes,
    concatenateUint8Arrays(offsetChunks),
    concatenateUint8Arrays(largeOffsetChunks),
    packChecksum,
  ]);
  const checksum = await createSha1(body);
  const data = concatenateUint8Arrays([body, hexToBytes(checksum)]);

  return {
    data,
    index: {
      checksum,
      entries: sortedEntries,
      fanoutTable,
      packChecksum: bytesToHex(packChecksum),
      version: 2,
    } satisfies PackIndex,
  };
}

export class GitPackParser {
  #indexEntries: PackIndexEntry[] = [];
  #objectStore: {
    readObject(oid: string): Promise<StoredObject>;
    writePack(packData: Uint8Array, indexEntries: PackIndexEntry[]): Promise<unknown>;
  };
  #objects: Map<number, PackObject> = new Map();
  #oidToOffset: Map<string, number> = new Map();

  constructor(objectStore: {
    readObject(oid: string): Promise<StoredObject>;
    writePack(packData: Uint8Array, indexEntries: PackIndexEntry[]): Promise<unknown>;
  }) {
    this.#objectStore = objectStore;
  }

  async #readFullStream(reader: ReadableStreamDefaultReader<Uint8Array>) {
    const chunks: Uint8Array[] = [];

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }

        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }

    return concatenateUint8Arrays(chunks);
  }

  async parsePack(stream: ReadableStream<Uint8Array>) {
    this.#indexEntries = [];
    this.#objects = new Map();
    this.#oidToOffset = new Map();

    const reader = stream.getReader();
    const buffer = await this.#readFullStream(reader);

    const signature = new TextDecoder().decode(buffer.slice(0, 4));
    if (signature !== PACK_SIGNATURE) {
      throw new Error("Invalid pack signature");
    }

    const _version = readUint32BE(buffer, 4);
    const objectCount = readUint32BE(buffer, 8);

    let offset = 12;
    for (let index = 0; index < objectCount; index++) {
      try {
        const result = await this.#parsePackObject(buffer, offset);
        this.#objects.set(offset, result.object);
        offset = result.nextOffset;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(
          `Failed to parse pack object ${index + 1}/${objectCount} at offset ${offset}: ${message}`,
          { cause: error instanceof Error ? error : undefined },
        );
      }
    }

    await this.#buildOidToOffset();
    await this.#resolveDeltas();

    const packChecksum = buffer.slice(buffer.length - PACK_TRAILER_SIZE);
    const calculatedChecksum = await calculateChecksum(
      buffer.slice(0, buffer.length - PACK_TRAILER_SIZE),
    );
    if (!compareChecksums(packChecksum, calculatedChecksum)) {
      throw new Error("Pack checksum mismatch");
    }

    await this.#buildIndexEntries();

    if (objectCount > 0) {
      await this.#objectStore.writePack(buffer, this.#indexEntries);
    }
  }

  async #buildOidToOffset() {
    for (const object of this.#objects.values()) {
      if (object.type === "ofs_delta" || object.type === "ref_delta") {
        continue;
      }

      const oid = await calculateObjectOid(object.type, object.data);
      this.#oidToOffset.set(oid, object.offset);
    }
  }

  async #buildIndexEntries() {
    const entries: PackIndexEntry[] = [];

    for (const object of this.#objects.values()) {
      if (object.type === "ofs_delta" || object.type === "ref_delta") {
        continue;
      }

      const oid = await calculateObjectOid(object.type, object.data);
      this.#oidToOffset.set(oid, object.offset);
      entries.push({
        crc32: object.crc32,
        offset: object.offset,
        oid,
      });
    }

    this.#indexEntries = entries;
  }

  async #parsePackObject(buffer: Uint8Array, offset: number) {
    const startOffset = offset;
    let byte = buffer[offset++];
    if (byte === undefined) {
      throw new Error("Unexpected end of buffer");
    }

    const type = (byte >> 4) & 0x7;
    let size = byte & 0xf;
    let shift = 4;

    while (byte & 0x80) {
      byte = buffer[offset++];
      if (byte === undefined) {
        throw new Error("Unexpected end of buffer");
      }

      size |= (byte & 0x7f) << shift;
      shift += 7;
    }

    const typeNames = ["", "commit", "tree", "blob", "tag", "", "ofs_delta", "ref_delta"];
    const typeName = typeNames[type] as PackObject["type"] | undefined;
    if (!typeName) {
      throw new Error(`Unsupported pack object type ${type}`);
    }

    try {
      let object: PackObject;
      if (typeName === "ofs_delta") {
        let baseDistance = 0;
        byte = buffer[offset++];
        if (byte === undefined) {
          throw new Error("Unexpected end of buffer");
        }

        baseDistance = byte & 0x7f;

        while (byte & 0x80) {
          byte = buffer[offset++];
          if (byte === undefined) {
            throw new Error("Unexpected end of buffer");
          }

          baseDistance = ((baseDistance + 1) << 7) | (byte & 0x7f);
        }

        const deltaData = await readCompressedData(buffer, offset, { expectedSize: size });
        object = {
          baseOffset: startOffset - baseDistance,
          crc32: crc32(buffer.slice(startOffset, deltaData.nextOffset)),
          data: deltaData.data,
          offset: startOffset,
          size,
          type: "ofs_delta",
        } as DeltaObject;
        offset = deltaData.nextOffset;
      } else if (typeName === "ref_delta") {
        const baseOid = bytesToHex(buffer.slice(offset, offset + 20));
        offset += 20;

        const deltaData = await readCompressedData(buffer, offset, { expectedSize: size });
        object = {
          baseOid,
          crc32: crc32(buffer.slice(startOffset, deltaData.nextOffset)),
          data: deltaData.data,
          offset: startOffset,
          size,
          type: "ref_delta",
        } as DeltaObject;
        offset = deltaData.nextOffset;
      } else {
        const objectData = await readCompressedData(buffer, offset, { expectedSize: size });
        object = {
          crc32: crc32(buffer.slice(startOffset, objectData.nextOffset)),
          data: objectData.data,
          offset: startOffset,
          size,
          type: typeName,
        };
        offset = objectData.nextOffset;
      }

      return { object, nextOffset: offset };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(
        `Failed to decode ${typeName} object at offset ${startOffset} with declared size ${size}: ${message}`,
        { cause: error instanceof Error ? error : undefined },
      );
    }
  }

  async #resolveDeltas() {
    const maxIterations = 10;
    let iteration = 0;
    let unresolvedCount = 0;

    do {
      unresolvedCount = 0;

      for (const [offset, object] of this.#objects) {
        if (object.type !== "ofs_delta" && object.type !== "ref_delta") {
          continue;
        }

        const resolved = await this.#resolveDelta(object as DeltaObject);
        if (resolved) {
          this.#objects.set(offset, resolved);
        } else {
          unresolvedCount++;
        }
      }

      iteration++;
    } while (unresolvedCount > 0 && iteration < maxIterations);

    if (unresolvedCount > 0) {
      throw new Error(
        `Unresolvable deltas: ${unresolvedCount} delta object(s) have no base in pack or object store`,
      );
    }
  }

  async #resolveDelta(deltaObject: DeltaObject) {
    let baseObject: PackObject | StoredObject | null = null;

    if (deltaObject.baseOffset !== undefined) {
      baseObject = this.#objects.get(deltaObject.baseOffset) || null;
    } else if (deltaObject.baseOid) {
      const offset = this.#oidToOffset.get(deltaObject.baseOid);
      if (offset !== undefined) {
        baseObject = this.#objects.get(offset) || null;
      } else {
        try {
          baseObject = await this.#objectStore.readObject(deltaObject.baseOid);
        } catch {
          baseObject = null;
        }
      }
    }

    if (!baseObject) {
      return null;
    }

    if (baseObject.type === "ofs_delta" || baseObject.type === "ref_delta") {
      return null;
    }

    const resolvedData = GitDelta.applyDelta(baseObject.data, deltaObject.data);

    return {
      crc32: deltaObject.crc32,
      data: resolvedData,
      offset: deltaObject.offset,
      size: resolvedData.length,
      type: baseObject.type,
    } satisfies PackObject;
  }
}

export class GitPackWriter {
  #objectStore: {
    readObject(oid: string): Promise<StoredObject>;
  };

  constructor(objectStore: { readObject(oid: string): Promise<StoredObject> }) {
    this.#objectStore = objectStore;
  }

  async createPack(oids: string[]) {
    const artifacts = await this.createPackArtifacts(oids);
    return artifacts.packData;
  }

  async createPackArtifacts(oids: string[]) {
    const objects: Array<{ data: Uint8Array; oid: string; type: string }> = [];

    for (const oid of oids) {
      const object = await this.#objectStore.readObject(oid);
      objects.push({
        data: object.data,
        oid,
        type: object.type,
      });
    }

    return await this.#buildPack(objects);
  }

  async #buildPack(objects: Array<{ data: Uint8Array; oid: string; type: string }>) {
    const chunks: Uint8Array[] = [];
    const indexEntries: PackIndexEntry[] = [];
    let offset = 12;

    chunks.push(new TextEncoder().encode(PACK_SIGNATURE));
    chunks.push(writeUint32BE(2));
    chunks.push(writeUint32BE(objects.length));

    for (const object of objects) {
      const typeNumber = this.#getTypeNumber(object.type);
      const objectData = await this.#encodeObject(typeNumber, object.data);
      indexEntries.push({
        crc32: crc32(objectData),
        offset,
        oid: object.oid,
      });
      chunks.push(objectData);
      offset += objectData.length;
    }

    const packWithoutChecksum = concatenateUint8Arrays(chunks);
    const checksum = hexToBytes(await createSha1(packWithoutChecksum));
    const packData = concatenateUint8Arrays([packWithoutChecksum, checksum]);

    return { indexEntries, packData };
  }

  async #encodeObject(typeNumber: number, data: Uint8Array) {
    const compressed = await compressData(data);
    const header = this.#encodeObjectHeader(typeNumber, data.length);
    return concatenateUint8Arrays([header, compressed]);
  }

  #encodeObjectHeader(type: number, size: number) {
    const bytes: number[] = [];
    let byte = (type << 4) | (size & 0xf);
    size >>= 4;

    if (size > 0) {
      byte |= 0x80;
    }

    bytes.push(byte);

    while (size > 0) {
      byte = size & 0x7f;
      size >>= 7;

      if (size > 0) {
        byte |= 0x80;
      }

      bytes.push(byte);
    }

    return new Uint8Array(bytes);
  }

  #getTypeNumber(type: string) {
    const typeMap: Record<string, number> = {
      blob: 3,
      commit: 1,
      ofs_delta: 6,
      ref_delta: 7,
      tag: 4,
      tree: 2,
    };

    return typeMap[type] ?? 0;
  }
}
