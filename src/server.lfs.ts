/**
 * Git LFS (Large File Storage) server implementation.
 *
 * Implements the Git LFS Batch API and object transfer endpoints:
 * - POST /:repo.git/info/lfs/objects/batch — Batch API
 * - PUT  /:repo.git/info/lfs/objects/:oid  — Object upload
 * - GET  /:repo.git/info/lfs/objects/:oid  — Object download
 *
 * @see https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md
 */

const LFS_MEDIA_TYPE = "application/vnd.git-lfs+json";
const LFS_POINTER_VERSION = "version https://git-lfs.github.com/spec/v1";

/** LFS object identifier (SHA-256 OID + size). */
export interface LfsObject {
  oid: string;
  size: number;
}

/** A single object in a batch response with transfer actions. */
export interface LfsBatchResponseObject {
  oid: string;
  size: number;
  authenticated?: boolean;
  actions?: {
    download?: LfsAction;
    upload?: LfsAction;
    verify?: LfsAction;
  };
  error?: { code: number; message: string };
}

interface LfsAction {
  href: string;
  header?: Record<string, string>;
  expires_in?: number;
}

/** Batch API request body. */
interface LfsBatchRequest {
  operation: "download" | "upload";
  transfers?: string[];
  objects: LfsObject[];
}

/** Batch API response body. */
interface LfsBatchResponse {
  transfer?: string;
  objects: LfsBatchResponseObject[];
}

/** Parsed LFS pointer file. */
export interface LfsPointer {
  version: string;
  oid: string;
  size: number;
}

interface LfsStorage {
  hasObject(repository: string, oid: string): boolean;
  getObjectSize(repository: string, oid: string): number | null;
  putObjectMeta(repository: string, oid: string, size: number): void;
  deleteObjectMeta(repository: string, oid: string): void;
  getR2(): R2Bucket;
}

export class ServerLfs {
  #storage: LfsStorage;

  constructor(storage: LfsStorage) {
    this.#storage = storage;
  }

  async batch(
    repository: string,
    body: ReadableStream<Uint8Array> | null,
    baseUrl: string,
  ): Promise<Response> {
    const request = await this.#parseJson<LfsBatchRequest>(body);
    if (!request || !request.operation || !Array.isArray(request.objects)) {
      return this.#error(422, "Invalid batch request");
    }

    if (request.operation !== "download" && request.operation !== "upload") {
      return this.#error(422, `Unsupported operation: ${String(request.operation)}`);
    }

    const objects: LfsBatchResponseObject[] = [];

    for (const obj of request.objects) {
      if (!this.#isValidOid(obj.oid) || typeof obj.size !== "number" || obj.size < 0) {
        objects.push({
          oid: obj.oid,
          size: obj.size,
          error: { code: 422, message: "Invalid object" },
        });
        continue;
      }

      const objectUrl = `${baseUrl}/${repository}.git/info/lfs/objects/${obj.oid}`;

      if (request.operation === "download") {
        if (!this.#storage.hasObject(repository, obj.oid)) {
          objects.push({
            oid: obj.oid,
            size: obj.size,
            error: { code: 404, message: "Object not found" },
          });
        } else {
          objects.push({
            oid: obj.oid,
            size: obj.size,
            authenticated: true,
            actions: {
              download: { href: objectUrl, expires_in: 3600 },
            },
          });
        }
      } else {
        // upload
        if (this.#storage.hasObject(repository, obj.oid)) {
          // Already exists — no action needed
          objects.push({ oid: obj.oid, size: obj.size, authenticated: true });
        } else {
          objects.push({
            oid: obj.oid,
            size: obj.size,
            authenticated: true,
            actions: {
              upload: { href: objectUrl, expires_in: 3600 },
            },
          });
        }
      }
    }

    const response: LfsBatchResponse = { transfer: "basic", objects };
    return Response.json(response, {
      headers: { "Content-Type": LFS_MEDIA_TYPE },
    });
  }

  async upload(
    repository: string,
    oid: string,
    body: ReadableStream<Uint8Array> | null,
  ): Promise<Response> {
    if (!this.#isValidOid(oid)) {
      return this.#error(422, "Invalid OID");
    }
    if (!body) {
      return this.#error(400, "Missing request body");
    }

    const r2Key = this.#r2Key(repository, oid);

    // Stream directly to R2
    await this.#storage.getR2().put(r2Key, body);

    // Get the actual size from R2
    const head = await this.#storage.getR2().head(r2Key);
    if (!head) {
      return this.#error(500, "Failed to store object");
    }

    this.#storage.putObjectMeta(repository, oid, head.size);

    return new Response(null, { status: 200 });
  }

  async download(repository: string, oid: string): Promise<Response> {
    if (!this.#isValidOid(oid)) {
      return this.#error(422, "Invalid OID");
    }

    if (!this.#storage.hasObject(repository, oid)) {
      return this.#error(404, "Object not found");
    }

    const r2Key = this.#r2Key(repository, oid);
    const object = await this.#storage.getR2().get(r2Key);
    if (!object) {
      // Stale metadata — clean up
      this.#storage.deleteObjectMeta(repository, oid);
      return this.#error(404, "Object not found");
    }

    return new Response(object.body, {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": object.size.toString(),
      },
    });
  }

  #r2Key(repository: string, oid: string) {
    return `lfs/${repository}/${oid}`;
  }

  #isValidOid(oid: string) {
    return typeof oid === "string" && /^[a-f0-9]{64}$/.test(oid);
  }

  #error(code: number, message: string) {
    return Response.json(
      { message },
      { status: code, headers: { "Content-Type": LFS_MEDIA_TYPE } },
    );
  }

  async #parseJson<T>(body: ReadableStream<Uint8Array> | null): Promise<T | null> {
    if (!body) return null;

    const reader = body.getReader();
    const chunks: Uint8Array[] = [];
    let result = await reader.read();
    while (!result.done) {
      chunks.push(result.value);
      result = await reader.read();
    }
    reader.releaseLock();

    if (chunks.length === 0) return null;

    const fullData = new Uint8Array(chunks.reduce((acc, c) => acc + c.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
      fullData.set(chunk, offset);
      offset += chunk.length;
    }
    return JSON.parse(new TextDecoder().decode(fullData));
  }
}

/**
 * Parse an LFS pointer file from blob content.
 * Returns null if the content is not a valid LFS pointer.
 */
export function parseLfsPointer(data: Uint8Array): LfsPointer | null {
  // LFS pointers are small text files (< 1KB typically)
  if (data.length > 1024) return null;

  const text = new TextDecoder().decode(data);
  if (!text.startsWith(LFS_POINTER_VERSION)) return null;

  let oid: string | undefined;
  let size: number | undefined;

  for (const line of text.split("\n")) {
    const oidMatch = line.match(/^oid sha256:([a-f0-9]{64})$/);
    if (oidMatch) {
      oid = oidMatch[1];
      continue;
    }
    const sizeMatch = line.match(/^size (\d+)$/);
    if (sizeMatch) {
      size = parseInt(sizeMatch[1], 10);
    }
  }

  if (!oid || size === undefined) return null;
  return { version: LFS_POINTER_VERSION, oid, size };
}

/**
 * Create an LFS pointer file for the given object.
 */
export function createLfsPointer(oid: string, size: number): Uint8Array {
  const text = `${LFS_POINTER_VERSION}\noid sha256:${oid}\nsize ${size}\n`;
  return new TextEncoder().encode(text);
}
