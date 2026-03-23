import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  ServerLfs,
  parseLfsPointer,
  createLfsPointer,
  type LfsBatchResponseObject,
} from "./server.lfs.ts";

const VALID_OID = "a".repeat(64);
const VALID_OID_2 = "b".repeat(64);

/** In-memory R2 mock for LFS object storage. */
function createMockR2(): R2Bucket {
  const store = new Map<string, Uint8Array>();

  return {
    async put(key: string, value: any) {
      if (value instanceof ReadableStream) {
        const reader = value.getReader();
        const chunks: Uint8Array[] = [];
        let result = await reader.read();
        while (!result.done) {
          chunks.push(result.value);
          result = await reader.read();
        }
        reader.releaseLock();
        const total = chunks.reduce((a, c) => a + c.length, 0);
        const data = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          data.set(chunk, offset);
          offset += chunk.length;
        }
        store.set(key, data);
      } else if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
        store.set(key, new Uint8Array(value));
      } else if (typeof value === "string") {
        store.set(key, new TextEncoder().encode(value));
      }
      return {} as R2Object;
    },
    async get(key: string) {
      const data = store.get(key);
      if (!data) return null;
      return {
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(data);
            controller.close();
          },
        }),
        size: data.length,
      } as unknown as R2ObjectBody;
    },
    async head(key: string) {
      const data = store.get(key);
      if (!data) return null;
      return { size: data.length } as R2Object;
    },
    async delete(key: string | string[]) {
      if (Array.isArray(key)) {
        for (const k of key) store.delete(k);
      } else {
        store.delete(key);
      }
    },
    async list() {
      return { objects: [], truncated: false } as unknown as R2Objects;
    },
    async createMultipartUpload() {
      throw new Error("Not implemented");
    },
    async resumeMultipartUpload() {
      throw new Error("Not implemented");
    },
  } as unknown as R2Bucket;
}

function createLfsStorage() {
  const objects = new Map<string, number>(); // key: "repo/oid" → size
  const r2 = createMockR2();

  const storage = {
    hasObject(repository: string, oid: string) {
      return objects.has(`${repository}/${oid}`);
    },
    getObjectSize(repository: string, oid: string) {
      return objects.get(`${repository}/${oid}`) ?? null;
    },
    putObjectMeta(repository: string, oid: string, size: number) {
      objects.set(`${repository}/${oid}`, size);
    },
    deleteObjectMeta(repository: string, oid: string) {
      objects.delete(`${repository}/${oid}`);
    },
    getR2() {
      return r2;
    },
  };

  return { storage, objects, r2 };
}

function jsonStream(data: unknown): ReadableStream<Uint8Array> {
  const encoded = new TextEncoder().encode(JSON.stringify(data));
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoded);
      controller.close();
    },
  });
}

function binaryStream(data: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

async function readJson(response: Response) {
  return response.json();
}

async function readBytes(response: Response) {
  const reader = response.body!.getReader();
  const chunks: Uint8Array[] = [];
  let result = await reader.read();
  while (!result.done) {
    chunks.push(result.value);
    result = await reader.read();
  }
  reader.releaseLock();
  const total = chunks.reduce((a, c) => a + c.length, 0);
  const data = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    data.set(chunk, offset);
    offset += chunk.length;
  }
  return data;
}

// --- Tests ---

void describe("ServerLfs", () => {
  void describe("batch API", () => {
    void it("should return upload actions for new objects", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "upload",
        objects: [{ oid: VALID_OID, size: 1024 }],
      });

      const response = await lfs.batch("test-repo", body, "https://example.com");
      assert.equal(response.status, 200);

      const json = await readJson(response);
      assert.equal(json.transfer, "basic");
      assert.equal(json.objects.length, 1);

      const obj = json.objects[0] as LfsBatchResponseObject;
      assert.equal(obj.oid, VALID_OID);
      assert.equal(obj.size, 1024);
      assert.ok(obj.actions?.upload?.href);
      assert.equal(
        obj.actions.upload.href,
        `https://example.com/test-repo.git/info/lfs/objects/${VALID_OID}`,
      );
    });

    void it("should skip upload action for existing objects", async () => {
      const { storage } = createLfsStorage();
      storage.putObjectMeta("test-repo", VALID_OID, 1024);
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "upload",
        objects: [{ oid: VALID_OID, size: 1024 }],
      });

      const response = await lfs.batch("test-repo", body, "https://example.com");
      const json = await readJson(response);

      const obj = json.objects[0] as LfsBatchResponseObject;
      assert.equal(obj.oid, VALID_OID);
      assert.equal(obj.actions, undefined); // No upload action needed
    });

    void it("should return download actions for existing objects", async () => {
      const { storage } = createLfsStorage();
      storage.putObjectMeta("test-repo", VALID_OID, 2048);
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "download",
        objects: [{ oid: VALID_OID, size: 2048 }],
      });

      const response = await lfs.batch("test-repo", body, "https://example.com");
      const json = await readJson(response);

      const obj = json.objects[0] as LfsBatchResponseObject;
      assert.equal(obj.oid, VALID_OID);
      assert.ok(obj.actions?.download?.href);
    });

    void it("should return 404 error for missing download objects", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "download",
        objects: [{ oid: VALID_OID, size: 1024 }],
      });

      const response = await lfs.batch("test-repo", body, "https://example.com");
      const json = await readJson(response);

      const obj = json.objects[0] as LfsBatchResponseObject;
      assert.equal(obj.error?.code, 404);
      assert.equal(obj.error?.message, "Object not found");
    });

    void it("should handle multiple objects in a batch", async () => {
      const { storage } = createLfsStorage();
      storage.putObjectMeta("test-repo", VALID_OID, 100);
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "download",
        objects: [
          { oid: VALID_OID, size: 100 },
          { oid: VALID_OID_2, size: 200 },
        ],
      });

      const response = await lfs.batch("test-repo", body, "https://example.com");
      const json = await readJson(response);

      assert.equal(json.objects.length, 2);
      assert.ok(json.objects[0].actions?.download);
      assert.equal(json.objects[1].error?.code, 404);
    });

    void it("should reject invalid OIDs in batch", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "upload",
        objects: [{ oid: "not-a-valid-oid", size: 100 }],
      });

      const response = await lfs.batch("test-repo", body, "https://example.com");
      const json = await readJson(response);

      assert.equal(json.objects[0].error?.code, 422);
    });

    void it("should reject invalid batch request", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const body = jsonStream({ foo: "bar" });
      const response = await lfs.batch("test-repo", body, "https://example.com");
      assert.equal(response.status, 422);
    });

    void it("should reject unsupported operation", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const body = jsonStream({
        operation: "verify",
        objects: [{ oid: VALID_OID, size: 100 }],
      });
      const response = await lfs.batch("test-repo", body, "https://example.com");
      assert.equal(response.status, 422);
    });
  });

  void describe("upload", () => {
    void it("should upload an LFS object", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);
      const data = new TextEncoder().encode("hello world LFS data");

      const response = await lfs.upload("test-repo", VALID_OID, binaryStream(data));
      assert.equal(response.status, 200);

      // Verify metadata was stored
      assert.ok(storage.hasObject("test-repo", VALID_OID));
      assert.equal(storage.getObjectSize("test-repo", VALID_OID), data.length);
    });

    void it("should reject upload with invalid OID", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const response = await lfs.upload("test-repo", "bad-oid", binaryStream(new Uint8Array(10)));
      assert.equal(response.status, 422);
    });

    void it("should reject upload with no body", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const response = await lfs.upload("test-repo", VALID_OID, null);
      assert.equal(response.status, 400);
    });
  });

  void describe("download", () => {
    void it("should download an uploaded LFS object", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);
      const data = new TextEncoder().encode("binary content here");

      // Upload first
      await lfs.upload("test-repo", VALID_OID, binaryStream(data));

      // Download
      const response = await lfs.download("test-repo", VALID_OID);
      assert.equal(response.status, 200);
      assert.equal(response.headers.get("Content-Type"), "application/octet-stream");
      assert.equal(response.headers.get("Content-Length"), String(data.length));

      const downloaded = await readBytes(response);
      assert.deepEqual(downloaded, data);
    });

    void it("should return 404 for missing object", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const response = await lfs.download("test-repo", VALID_OID);
      assert.equal(response.status, 404);
    });

    void it("should reject download with invalid OID", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      const response = await lfs.download("test-repo", "xyz");
      assert.equal(response.status, 422);
    });

    void it("should self-heal stale metadata", async () => {
      const { storage } = createLfsStorage();
      const lfs = new ServerLfs(storage);

      // Insert metadata without R2 object
      storage.putObjectMeta("test-repo", VALID_OID, 100);
      assert.ok(storage.hasObject("test-repo", VALID_OID));

      // Download should 404 and clean up metadata
      const response = await lfs.download("test-repo", VALID_OID);
      assert.equal(response.status, 404);

      // Metadata should be cleaned up
      assert.equal(storage.hasObject("test-repo", VALID_OID), false);
    });
  });
});

void describe("LFS pointer files", () => {
  void it("should parse a valid LFS pointer", () => {
    const pointer = createLfsPointer(VALID_OID, 12345);
    const parsed = parseLfsPointer(pointer);

    assert.ok(parsed);
    assert.equal(parsed.oid, VALID_OID);
    assert.equal(parsed.size, 12345);
    assert.equal(parsed.version, "version https://git-lfs.github.com/spec/v1");
  });

  void it("should return null for non-pointer content", () => {
    const data = new TextEncoder().encode("just some regular file content");
    assert.equal(parseLfsPointer(data), null);
  });

  void it("should return null for oversized content", () => {
    const big = new Uint8Array(2048);
    assert.equal(parseLfsPointer(big), null);
  });

  void it("should return null for incomplete pointer", () => {
    const text = "version https://git-lfs.github.com/spec/v1\noid sha256:" + VALID_OID + "\n";
    // Missing size line
    const data = new TextEncoder().encode(text);
    assert.equal(parseLfsPointer(data), null);
  });

  void it("should roundtrip create and parse", () => {
    const oid = "c".repeat(64);
    const pointer = createLfsPointer(oid, 99999);
    const parsed = parseLfsPointer(pointer);

    assert.ok(parsed);
    assert.equal(parsed.oid, oid);
    assert.equal(parsed.size, 99999);
  });
});
