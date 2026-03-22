import * as assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";

import { GitPackParser, GitPackWriter } from "./git.pack.ts";
import { GitObjectStore } from "./git.object.ts";
import { MemoryStorage } from "./git.storage.ts";
import { exec, run, toStream } from "./test.helpers.ts";
import { hexToBytes } from "./git.utils.ts";

void describe("GitPackParser", () => {
  void describe("constructor", () => {
    void it("should initialize pack parser", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const parser = new GitPackParser(objectStore);
      assert.ok(parser);
    });
  });

  void describe("parsePack", () => {
    void it("should parse valid empty pack file", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Use the writer to produce a pack with a correct checksum
      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([]);

      const parser = new GitPackParser(objectStore);
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(packData);
          controller.close();
        },
      });

      // Should not throw
      await parser.parsePack(stream);
    });

    void it("should reject invalid pack signature", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const parser = new GitPackParser(objectStore);

      // Invalid pack signature
      const packData = new Uint8Array([0x42, 0x41, 0x44, 0x21]);

      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(packData);
          controller.close();
        },
      });

      try {
        await parser.parsePack(stream);
        assert.fail("Should have thrown error");
      } catch (error: any) {
        assert.ok(error.message.includes("Invalid pack signature"));
      }
    });

    void it("should handle chunked stream input", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Use the writer to produce a valid pack, then split it into chunks
      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([]);

      // Split into header chunks + checksum
      const chunk1 = packData.slice(0, 4); // "PACK"
      const chunk2 = packData.slice(4, 8); // version
      const chunk3 = packData.slice(8, 12); // count
      const chunk4 = packData.slice(12); // checksum

      const parser = new GitPackParser(objectStore);
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(chunk1);
          controller.enqueue(chunk2);
          controller.enqueue(chunk3);
          controller.enqueue(chunk4);
          controller.close();
        },
      });

      await parser.parsePack(stream);
    });
  });
});

void describe("GitPackWriter", () => {
  void describe("constructor", () => {
    void it("should initialize pack writer", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const writer = new GitPackWriter(objectStore);
      assert.ok(writer);
    });
  });

  void describe("createPack", () => {
    void it("should create empty pack", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([]);

      // Pack should have header and checksum
      assert.ok(packData.length >= 32); // 4 (sig) + 4 (ver) + 4 (count) + 20 (checksum)

      // Check signature
      const signature = new TextDecoder().decode(packData.slice(0, 4));
      assert.equal(signature, "PACK");

      // Check version (2)
      const version =
        (packData[4]! << 24) | (packData[5]! << 16) | (packData[6]! << 8) | packData[7]!;
      assert.equal(version, 2);

      // Check object count (0)
      const count =
        (packData[8]! << 24) | (packData[9]! << 16) | (packData[10]! << 8) | packData[11]!;
      assert.equal(count, 0);
    });

    void it("should create pack with single blob", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create a blob
      const content = new TextEncoder().encode("Hello, World!");
      const oid = await objectStore.writeObject("blob", content);

      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([oid]);

      // Check signature
      const signature = new TextDecoder().decode(packData.slice(0, 4));
      assert.equal(signature, "PACK");

      // Check object count (1)
      const count =
        (packData[8]! << 24) | (packData[9]! << 16) | (packData[10]! << 8) | packData[11]!;
      assert.equal(count, 1);
    });

    void it("should create pack with multiple objects", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create multiple blobs
      const oid1 = await objectStore.writeObject("blob", new TextEncoder().encode("content1"));
      const oid2 = await objectStore.writeObject("blob", new TextEncoder().encode("content2"));
      const oid3 = await objectStore.writeObject("blob", new TextEncoder().encode("content3"));

      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([oid1, oid2, oid3]);

      // Check object count (3)
      const count =
        (packData[8]! << 24) | (packData[9]! << 16) | (packData[10]! << 8) | packData[11]!;
      assert.equal(count, 3);
    });

    void it("should include all object types", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create blob
      const blobOid = await objectStore.writeObject("blob", new TextEncoder().encode("content"));

      // Create tree
      const treeData = new Uint8Array([
        ...new TextEncoder().encode("100644 file.txt\0"),
        ...hexToBytes(blobOid),
      ]);
      const treeOid = await objectStore.writeObject("tree", treeData);

      // Create commit
      const commitData = new TextEncoder().encode(
        `tree ${treeOid}\nauthor Test <t@t.com> 123 +0000\n\ntest`,
      );
      const commitOid = await objectStore.writeObject("commit", commitData);

      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([blobOid, treeOid, commitOid]);

      const count =
        (packData[8]! << 24) | (packData[9]! << 16) | (packData[10]! << 8) | packData[11]!;
      assert.equal(count, 3);
    });
  });

  void describe("roundtrip", () => {
    void it("should roundtrip pack write and parse", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      // Create test objects
      const content = new TextEncoder().encode("test content");
      const oid = await objectStore.writeObject("blob", content);

      // Write pack
      const writer = new GitPackWriter(objectStore);
      const packData = await writer.createPack([oid]);

      // Create new storage to parse into
      const storage2 = new MemoryStorage();
      await storage2.init("test-repo-2");

      const objectStore2 = new GitObjectStore(storage2);
      await objectStore2.init();

      // Parse pack
      const parser = new GitPackParser(objectStore2);
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(packData);
          controller.close();
        },
      });

      await parser.parsePack(stream);

      // Verify object was stored
      const stored = await objectStore2.readObject(oid);
      assert.equal(stored.type, "blob");
      assert.deepEqual(stored.data, content);
    });

    void it("should persist and read a pack index", async () => {
      const storage = new MemoryStorage();
      await storage.init("test-repo");

      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const oid = await objectStore.writeObject("blob", new TextEncoder().encode("indexed"));
      const writer = new GitPackWriter(objectStore);
      const artifacts = await writer.createPackArtifacts([oid]);
      const { idxPath } = await objectStore.writePack(artifacts.packData, artifacts.indexEntries);

      const index = await objectStore.readPackIndex(idxPath);
      assert.equal(index.entries.length, 1);
      assert.equal(index.entries[0]!.oid, oid);

      const readBack = await objectStore.readObject(oid);
      assert.equal(readBack.type, "blob");
      assert.deepEqual(readBack.data, new TextEncoder().encode("indexed"));
    });
  });
});

function encodePackObjectHeader(type: number, size: number) {
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

void describe("thin pack handling", () => {
  void it("should resolve ref_delta against object in store", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    // Write a base blob to the store
    const baseContent = new TextEncoder().encode("Hello, World! This is the base content.");
    const baseOid = await objectStore.writeObject("blob", baseContent);

    // Create a second blob that shares most content with the base
    const targetContent = new TextEncoder().encode("Hello, World! This is the MODIFIED content.");

    // Use GitDelta to create a proper delta
    const { GitDelta } = await import("./git.delta.ts");
    const delta = GitDelta.createDelta(baseContent, targetContent);

    // Build a pack containing ONLY the ref_delta (thin pack — base is in store, not in pack)
    const { compressData, createSha1, concatenateUint8Arrays } = await import("./git.utils.ts");
    const compressed = await compressData(delta);

    const chunks: Uint8Array[] = [];
    // PACK header
    chunks.push(new TextEncoder().encode("PACK"));
    // Version 2
    chunks.push(new Uint8Array([0x00, 0x00, 0x00, 0x02]));
    // 1 object
    chunks.push(new Uint8Array([0x00, 0x00, 0x00, 0x01]));

    // ref_delta object header: type 7, size = decompressed delta length
    const headerBytes: number[] = [];
    let byte = (7 << 4) | (delta.length & 0xf);
    let remaining = delta.length >> 4;
    if (remaining > 0) byte |= 0x80;
    headerBytes.push(byte);
    while (remaining > 0) {
      byte = remaining & 0x7f;
      remaining >>= 7;
      if (remaining > 0) byte |= 0x80;
      headerBytes.push(byte);
    }
    chunks.push(new Uint8Array(headerBytes));

    // Base OID (20 bytes)
    chunks.push(hexToBytes(baseOid));

    // Compressed delta data
    chunks.push(compressed);

    // Combine everything except checksum to calculate it
    const packWithoutChecksum = concatenateUint8Arrays(chunks);
    const checksum = await createSha1(packWithoutChecksum);
    const packData = concatenateUint8Arrays([packWithoutChecksum, hexToBytes(checksum)]);

    // Parse the thin pack — should resolve the ref_delta using the base from the store
    const parser = new GitPackParser(objectStore);
    await parser.parsePack(toStream(packData));

    // The resolved object should be stored — find it by writing and comparing
    const expectedOid = await objectStore.writeObject("blob", targetContent);
    const stored = await objectStore.readObject(expectedOid);
    assert.deepEqual(stored.data, targetContent);
  });

  void it("should parse a delta object followed by another packed object", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    const baseContent = new TextEncoder().encode("Hello, World! This is the base content.");
    const baseOid = await objectStore.writeObject("blob", baseContent);
    const targetContent = new TextEncoder().encode("Hello, World! This is the MODIFIED content.");
    const trailingContent = new TextEncoder().encode("trailing blob");

    const { GitDelta } = await import("./git.delta.ts");
    const { compressData, createSha1, concatenateUint8Arrays } = await import("./git.utils.ts");
    const delta = GitDelta.createDelta(baseContent, targetContent);
    const compressedDelta = await compressData(delta);
    const compressedBlob = await compressData(trailingContent);

    const chunks: Uint8Array[] = [];
    chunks.push(new TextEncoder().encode("PACK"));
    chunks.push(new Uint8Array([0x00, 0x00, 0x00, 0x02]));
    chunks.push(new Uint8Array([0x00, 0x00, 0x00, 0x02]));

    chunks.push(encodePackObjectHeader(7, delta.length));
    chunks.push(hexToBytes(baseOid));
    chunks.push(compressedDelta);

    chunks.push(encodePackObjectHeader(3, trailingContent.length));
    chunks.push(compressedBlob);

    const packWithoutChecksum = concatenateUint8Arrays(chunks);
    const checksum = await createSha1(packWithoutChecksum);
    const packData = concatenateUint8Arrays([packWithoutChecksum, hexToBytes(checksum)]);

    const parser = new GitPackParser(objectStore);
    await parser.parsePack(toStream(packData));

    const resolvedOid = await objectStore.writeObject("blob", targetContent);
    const trailingOid = await objectStore.writeObject("blob", trailingContent);
    assert.deepEqual((await objectStore.readObject(resolvedOid)).data, targetContent);
    assert.deepEqual((await objectStore.readObject(trailingOid)).data, trailingContent);
  });

  void it("should parse a git-generated pack with multiple object boundaries", async () => {
    const repoDir = await mkdtemp(join(tmpdir(), "git-pack-"));

    try {
      await run("git init -b main", { cwd: repoDir });
      await run('git config user.name "Test User"', { cwd: repoDir });
      await run('git config user.email "test@example.com"', { cwd: repoDir });
      await run("git config commit.gpgSign false", { cwd: repoDir });

      await writeFile(join(repoDir, "file.txt"), "v1\n");
      await run("git add file.txt", { cwd: repoDir });
      await run('git commit -m "c1"', { cwd: repoDir });

      await writeFile(join(repoDir, "file.txt"), "v2 with more content\n");
      await run('git commit -am "c2"', { cwd: repoDir });

      await writeFile(
        join(repoDir, "file.txt"),
        "v3 with more content and enough repeated bytes to encourage git delta compression\n",
      );
      await run('git commit -am "c3"', { cwd: repoDir });

      const packData = new Uint8Array(
        await exec("git pack-objects --all --stdout", { cwd: repoDir }),
      );
      const latestBlobOid = (await run("git rev-parse HEAD:file.txt", { cwd: repoDir })).trim();
      const latestContent = new Uint8Array(await readFile(join(repoDir, "file.txt")));

      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const parser = new GitPackParser(objectStore);
      await parser.parsePack(toStream(packData));

      const stored = await objectStore.readObject(latestBlobOid);
      assert.equal(stored.type, "blob");
      assert.deepEqual(stored.data, latestContent);
    } finally {
      await rm(repoDir, { force: true, recursive: true });
    }
  });

  void it("should parse a pack extracted from a git push request", async () => {
    const tempDir = await mkdtemp(join(tmpdir(), "git-push-pack-"));

    try {
      const capturePath = join(tempDir, "capture.bin");
      const wrapperPath = join(tempDir, "capture-receive-pack.sh");
      const remoteDir = join(tempDir, "remote.git");
      const clientDir = join(tempDir, "client");

      await writeFile(wrapperPath, 'tee "$1" | git-receive-pack "$2"\n');

      await run(`git init --bare ${remoteDir}`, { cwd: tempDir });
      await run(`git init -b main ${clientDir}`, { cwd: tempDir });
      await run('git config user.name "Test User"', { cwd: clientDir });
      await run('git config user.email "test@example.com"', { cwd: clientDir });
      await run("git config commit.gpgSign false", { cwd: clientDir });

      await writeFile(join(clientDir, "file.txt"), "v1\n");
      await run("git add file.txt", { cwd: clientDir });
      await run('git commit -m "c1"', { cwd: clientDir });

      await writeFile(join(clientDir, "file.txt"), "v2 with more content\n");
      await run('git commit -am "c2"', { cwd: clientDir });

      await writeFile(
        join(clientDir, "file.txt"),
        "v3 with more content and enough repeated bytes to encourage git delta compression\n",
      );
      await run('git commit -am "c3"', { cwd: clientDir });

      await run(
        `git push '--receive-pack=sh ${wrapperPath} ${capturePath} ${remoteDir}' ${remoteDir} main`,
        { cwd: clientDir },
      );

      const body = new Uint8Array(await readFile(capturePath));
      let offset = 0;

      while (offset + 4 <= body.length) {
        const length = parseInt(new TextDecoder().decode(body.slice(offset, offset + 4)), 16);
        assert.ok(!Number.isNaN(length), "Expected pkt-line length prefix");
        offset += 4;

        if (length === 0) {
          break;
        }

        offset += length - 4;
      }

      const packData = body.slice(offset);
      assert.equal(new TextDecoder().decode(packData.slice(0, 4)), "PACK");

      const storage = new MemoryStorage();
      await storage.init("test-repo");
      const objectStore = new GitObjectStore(storage);
      await objectStore.init();

      const parser = new GitPackParser(objectStore);
      await parser.parsePack(toStream(packData));

      const latestBlobOid = (await run("git rev-parse HEAD:file.txt", { cwd: clientDir })).trim();
      const latestContent = new Uint8Array(await readFile(join(clientDir, "file.txt")));
      const stored = await objectStore.readObject(latestBlobOid);
      assert.equal(stored.type, "blob");
      assert.deepEqual(stored.data, latestContent);
    } finally {
      await rm(tempDir, { force: true, recursive: true });
    }
  });

  void it("should throw on unresolvable ref_delta", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    // Build a pack with a ref_delta whose base OID doesn't exist anywhere
    const { compressData, createSha1, concatenateUint8Arrays } = await import("./git.utils.ts");

    // Fake delta data (just needs to be valid compressed bytes)
    const fakeDelta = new Uint8Array([0x05, 0x05, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    const compressed = await compressData(fakeDelta);

    const chunks: Uint8Array[] = [];
    chunks.push(new TextEncoder().encode("PACK"));
    chunks.push(new Uint8Array([0x00, 0x00, 0x00, 0x02])); // version
    chunks.push(new Uint8Array([0x00, 0x00, 0x00, 0x01])); // 1 object

    // ref_delta header: type 7, size = decompressed delta length
    const headerBytes: number[] = [];
    let byte = (7 << 4) | (fakeDelta.length & 0xf);
    let remaining = fakeDelta.length >> 4;
    if (remaining > 0) byte |= 0x80;
    headerBytes.push(byte);
    while (remaining > 0) {
      byte = remaining & 0x7f;
      remaining >>= 7;
      if (remaining > 0) byte |= 0x80;
      headerBytes.push(byte);
    }
    chunks.push(new Uint8Array(headerBytes));

    // Non-existent base OID
    chunks.push(hexToBytes("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"));

    // Compressed delta
    chunks.push(compressed);

    const packWithoutChecksum = concatenateUint8Arrays(chunks);
    const checksum = await createSha1(packWithoutChecksum);
    const packData = concatenateUint8Arrays([packWithoutChecksum, hexToBytes(checksum)]);

    const parser = new GitPackParser(objectStore);
    await assert.rejects(() => parser.parsePack(toStream(packData)), {
      message: /[Uu]nresolvable deltas/,
    });
  });

  void it("should throw on pack checksum mismatch", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    // Create a valid pack then corrupt the checksum
    const content = new TextEncoder().encode("test");
    const oid = await objectStore.writeObject("blob", content);

    const writer = new GitPackWriter(objectStore);
    const packData = await writer.createPack([oid]);

    // Corrupt the last byte of the checksum
    const corrupted = new Uint8Array(packData);
    corrupted[corrupted.length - 1] = (corrupted[corrupted.length - 1]! + 1) % 256;

    // Parse into fresh store
    const storage2 = new MemoryStorage();
    await storage2.init("test-repo-2");
    const objectStore2 = new GitObjectStore(storage2);
    await objectStore2.init();

    const parser = new GitPackParser(objectStore2);
    await assert.rejects(() => parser.parsePack(toStream(corrupted)), {
      message: /[Cc]hecksum mismatch/,
    });
  });
});
