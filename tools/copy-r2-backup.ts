/** R2 copy primitive for a caller that has quiesced all source/destination writers.
 * No public endpoint. Does not capture SQL/KV, create a snapshot barrier or certify a backup.
 * Failure leaves partial destination data for inspection; never delete it automatically.
 */
import assert from "node:assert/strict";
import type { Resources } from "./cloudflare-backup-inventory.ts";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type {
  R2Bucket,
  R2GetOptions,
  R2Object,
  R2ObjectBody,
  R2PutOptions,
} from "@cloudflare/workers-types";

export type Source = {
  list: R2Bucket["list"];
  get: (key: string, options: R2GetOptions) => Promise<R2ObjectBody | R2Object | null>;
};
export type Destination = Source & {
  put: (key: string, body: R2ObjectBody["body"], options: R2PutOptions) => Promise<R2Object | null>;
};

/** R2's single PUT ceiling. Multipart completion does not support our conditional-create contract. */
export const maxBackupObjectBytes = 5 * 1024 ** 3;
const checkSize = (size: number) =>
  assert.ok(
    Number.isSafeInteger(size) && size >= 0 && size <= maxBackupObjectBytes,
    "R2 backup object exceeds the supported single PUT size",
  );

const readBack = async (bucket: Source, expected: R2Object) => {
  const held = await bucket.get(expected.key, { onlyIf: { etagMatches: expected.etag } });
  assert.ok(held !== null && "body" in held, "R2 verification object disappeared or changed");
  const reader = held.body.getReader();
  const hash = createHash("sha256");
  let bytes = 0;
  try {
    assert.equal(held.version, expected.version, "R2 verification version changed");
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      assert.ok(bytes <= expected.size, "R2 verification body exceeds expected size");
      hash.update(chunk.value);
    }
    assert.equal(bytes, expected.size, "R2 verification body is truncated");
    return { object: held, sha256: hash.digest("hex") };
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
};

const appendMetadata = (digest: ReturnType<typeof createHash>, object: R2Object) => {
  const http = object.httpMetadata;
  const custom = Object.entries(object.customMetadata ?? {}).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  );
  digest.update(
    JSON.stringify([
      object.key,
      http?.contentType ?? null,
      http?.contentLanguage ?? null,
      http?.contentDisposition ?? null,
      http?.contentEncoding ?? null,
      http?.cacheControl ?? null,
      http?.cacheExpiry?.toISOString() ?? null,
      custom,
      object.storageClass,
    ]) + "\n",
  );
};
const listing = async function* (source: Source, pageSize: number) {
  assert.ok(
    Number.isInteger(pageSize) && pageSize >= 1 && pageSize <= 1000,
    "invalid R2 copy page size",
  );
  let cursor: string | undefined;
  for (let pageNumber = 0; pageNumber < 10000; pageNumber++) {
    const page = await source.list({ limit: pageSize, cursor });
    assert.ok(page.objects.length <= pageSize, "R2 copy page exceeds requested limit");
    yield page;
    if (!page.truncated) return;
    assert.ok(page.cursor && page.cursor !== cursor, "R2 copy cursor did not advance");
    cursor = page.cursor;
  }
  throw new Error("R2 copy exceeds page budget");
};

/** Inspect the entire listing before copying any key, including objects on later pages. */
const preflightCopy = async (source: Source, pageSize: number) => {
  for await (const page of listing(source, pageSize))
    for (const entry of page.objects) checkSize(entry.size);
};

const copy = async (
  source: Source,
  destination: Destination,
  pageSize: number,
  resume: boolean,
) => {
  if (!resume) {
    const existing = await destination.list({ limit: 1 });
    assert.ok(
      existing.objects.length === 0 && !existing.truncated,
      "R2 backup destination must be empty",
    );
  }
  const inventory = createHash("sha256");
  const metadataInventory = createHash("sha256");
  let pages = 0;
  let objects = 0;
  let bytes = 0;
  for await (const page of listing(source, pageSize)) {
    for (const entry of page.objects) {
      checkSize(entry.size);
      const held = await source.get(entry.key, { onlyIf: { etagMatches: entry.etag } });
      assert.ok(held !== null && "body" in held, "R2 copy source disappeared or changed");
      try {
        assert.equal(held.version, entry.version, "R2 copy source version changed");
        checkSize(held.size);
        assert.equal(held.size, entry.size, "R2 copy source size changed");
        const checksum =
          held.checksums.sha256 !== undefined
            ? { sha256: held.checksums.sha256 }
            : held.checksums.md5 !== undefined
              ? { md5: held.checksums.md5 }
              : {};
        let copied: R2Object | null = null;
        if (resume) {
          const prior = await destination.get(entry.key, {});
          if (prior !== null) {
            assert.ok("body" in prior, "R2 restore destination body unavailable");
            await prior.body.cancel();
            copied = prior;
          }
        }
        if (copied === null)
          copied = await destination.put(entry.key, held.body, {
            onlyIf: { etagDoesNotMatch: "*" },
            httpMetadata: held.httpMetadata,
            customMetadata: held.customMetadata,
            storageClass: held.storageClass,
            ...checksum,
          });
        assert.ok(copied !== null, "R2 copy destination appeared concurrently");
        assert.equal(copied.key, entry.key, "R2 copy destination key differs");
        assert.equal(held.key, entry.key, "R2 copy source key differs");
        assert.equal(copied.size, held.size, "R2 copy size differs");
        const original = await readBack(source, held);
        const verified = await readBack(destination, copied);
        assert.equal(verified.sha256, original.sha256, "R2 copy content digest differs");
        assert.ok(
          isDeepStrictEqual(verified.object.httpMetadata, original.object.httpMetadata),
          "R2 copy HTTP metadata differs",
        );
        assert.ok(
          isDeepStrictEqual(verified.object.customMetadata, original.object.customMetadata),
          "R2 copy custom metadata differs",
        );
        assert.equal(
          verified.object.storageClass,
          original.object.storageClass,
          "R2 copy storage class differs",
        );
        inventory.update(JSON.stringify([entry.key, held.size, verified.sha256]) + "\n");
        appendMetadata(metadataInventory, verified.object);
        objects += 1;
        bytes += held.size;
        assert.ok(Number.isSafeInteger(bytes), "R2 copy byte count exceeds safe integer range");
      } finally {
        await held.body.cancel().catch(() => undefined);
      }
    }
    pages += 1;
  }
  return {
    schemaVersion: 2,
    objects,
    bytes,
    pages,
    contentInventorySha256: inventory.digest("hex"),
    metadataInventorySha256: metadataInventory.digest("hex"),
    limitsOfEvidence:
      "R2 copy only. Requires externally quiesced writers; not SQL/KV recovery, a consistent installation snapshot, or verification of production storage/account behavior. Object versions and upload times are newly assigned by R2.",
  };
};

/** Fresh backup copy: retain the original refusal to reuse any non-empty destination. */
export const copyR2Backup = async (source: Source, destination: Destination, pageSize = 1000) => {
  await preflightCopy(source, pageSize);
  return copy(source, destination, pageSize, false);
};

/** Read-only full content/metadata inventory; caller must keep all writers isolated. */
export const inventoryR2Backup = async (
  bucket: Source,
  pageSize = 1000,
): Promise<Resources["r2"]> => {
  const content = createHash("sha256");
  const metadata = createHash("sha256");
  let objects = 0;
  let bytes = 0;
  for await (const page of listing(bucket, pageSize)) {
    for (const entry of page.objects) {
      const verified = await readBack(bucket, entry);
      assert.equal(verified.object.key, entry.key, "R2 verification key differs");
      content.update(JSON.stringify([entry.key, entry.size, verified.sha256]) + "\n");
      appendMetadata(metadata, verified.object);
      objects += 1;
      bytes += entry.size;
      assert.ok(Number.isSafeInteger(bytes), "R2 inventory byte count exceeds safe integer range");
    }
  }
  return {
    objects,
    bytes,
    contentInventorySha256: content.digest("hex"),
    metadataInventorySha256: metadata.digest("hex"),
  };
};
const matches = (actual: Resources["r2"], expected: Resources["r2"]) =>
  assert.ok(
    actual.objects === expected.objects &&
      actual.bytes === expected.bytes &&
      actual.contentInventorySha256 === expected.contentInventorySha256 &&
      actual.metadataInventorySha256 === expected.metadataInventorySha256,
    "R2 restore inventory differs",
  );

/** Resume only under a verified signed inventory and isolated writers. Never overwrite or delete existing keys. */
export const restoreR2Backup = async (
  source: Source,
  destination: Destination,
  expected: Resources["r2"],
  pageSize = 1000,
) => {
  await preflightCopy(source, pageSize);
  matches(await inventoryR2Backup(source, pageSize), expected);
  const copied = await copy(source, destination, pageSize, true);
  matches(copied, expected);
  // Scan the complete destination, including keys absent from the source walk.
  matches(await inventoryR2Backup(destination, pageSize), expected);
  return copied;
};
