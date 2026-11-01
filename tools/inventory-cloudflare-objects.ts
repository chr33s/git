/** Read-only discovery for recovery planning. Does not export object contents. */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { constants } from "node:fs";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { readBackupLines } from "./cloudflare-backup-read.ts";
import { Config, Effect, Result, Schema } from "effect";

const ObjectRecord = Schema.Struct({
  id: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  hasStoredData: Schema.Boolean,
});
const Page = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Array(ObjectRecord),
  result_info: Schema.Struct({ cursor: Schema.optional(Schema.String) }),
});
const Manifest = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  measuredAt: Schema.String,
  account: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
  namespace: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
  pages: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
  objects: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000_000 })),
  storedObjects: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000_000 })),
  inventorySha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});
const inventoryHash = (account: string, namespace: string) =>
  createHash("sha256").update(
    JSON.stringify(["chr33s-git/namespace-inventory/v1", account, namespace]) + "\n",
  );
const distinctIds = async (directory: string) => {
  const file = path.join(directory, "seen.sqlite");
  await fs.writeFile(file, "", { flag: "wx", mode: 0o600 });
  const database = new DatabaseSync(file);
  try {
    // Disposable uniqueness index: bounded cache, no in-memory object-ID walk.
    database.exec(
      "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-2048; CREATE TABLE seen (id TEXT PRIMARY KEY) WITHOUT ROWID",
    );
  } catch (error) {
    database.close();
    throw error;
  }
  const insert = database.prepare("INSERT OR IGNORE INTO seen VALUES (?)");
  return {
    add: (id: string) =>
      assert.equal(insert.run(id).changes, 1, "Cloudflare inventory repeated an object ID"),
    close: async () => {
      database.close();
      await fs.rm(file);
    },
  };
};
const syncDirectory = async (directory: string) => {
  const handle = await fs.open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
};
const pageLimit = 1000;
const maxPages = 1000;
const responseLimit = 2 * 1024 * 1024;

export const inventoryCloudflareObjects = async (
  account: string,
  namespace: string,
  destination: string,
  token: string,
  request: typeof fetch = fetch,
) => {
  assert.match(account, /^[a-f0-9]{32}$/, "expected Cloudflare account ID");
  assert.match(namespace, /^[a-f0-9]{32}$/, "expected Durable Object namespace ID");
  assert.ok(token.length > 0, "Cloudflare API token is required");
  // Exclusive directory creation preserves existing inventories and partial work.
  await fs.mkdir(destination, { mode: 0o700 });
  let complete = false;
  try {
    const ids = await distinctIds(destination);
    try {
      const output = await fs.open(path.join(destination, "objects.ndjson"), "wx", 0o600);
      const digest = inventoryHash(account, namespace);
      let pages = 0;
      let objects = 0;
      let storedObjects = 0;
      let cursor = "";
      const seen = new Set<string>();
      try {
        do {
          assert.ok(pages < maxPages, "inventory exceeds page budget; no complete inventory saved");
          const url = new URL(
            `https://api.cloudflare.com/client/v4/accounts/${account}/workers/durable_objects/namespaces/${namespace}/objects`,
          );
          url.searchParams.set("limit", String(pageLimit));
          if (cursor !== "") url.searchParams.set("cursor", cursor);
          const response = await request(url, {
            headers: { Authorization: `Bearer ${token}` },
            redirect: "error",
            signal: AbortSignal.timeout(30_000),
          }).catch(() => {
            throw new Error("Cloudflare inventory request failed");
          });
          if (!response.ok) {
            await response.body?.cancel().catch(() => undefined);
            throw new Error(`Cloudflare inventory HTTP ${response.status}`);
          }
          assert.ok(response.body, "Cloudflare inventory response has no body");
          const reader = response.body.getReader();
          const body = Buffer.alloc(responseLimit);
          let bytes = 0;
          try {
            while (true) {
              const chunk = await reader.read();
              if (chunk.done) break;
              assert.ok(
                bytes + chunk.value.byteLength <= responseLimit,
                "Cloudflare inventory response exceeds budget",
              );
              body.set(chunk.value, bytes);
              bytes += chunk.value.byteLength;
            }
          } catch {
            throw new Error("Cloudflare inventory body failed or exceeded its budget");
          } finally {
            await reader.cancel().catch(() => undefined);
            reader.releaseLock();
          }
          const decoded = Schema.decodeResult(Schema.fromJsonString(Page))(
            body.toString("utf8", 0, bytes),
          );
          if (Result.isFailure(decoded)) throw new Error("Invalid Cloudflare inventory response");
          const page = decoded.success;
          assert.ok(
            page.result.length <= pageLimit,
            "Cloudflare inventory page exceeds requested limit",
          );
          for (const object of page.result) {
            ids.add(object.id);
            const line = JSON.stringify(object) + "\n";
            await output.writeFile(line);
            digest.update(line);
            objects += 1;
            if (object.hasStoredData) storedObjects += 1;
          }
          pages += 1;
          cursor = page.result_info.cursor ?? "";
          assert.ok(cursor.length <= 4096, "Cloudflare inventory cursor exceeds budget");
          if (cursor !== "") {
            assert.ok(!seen.has(cursor), "Cloudflare inventory pagination repeated a cursor");
            seen.add(cursor);
          }
        } while (cursor !== "");
        await output.sync();
      } finally {
        await output.close();
      }
      const manifest = await fs.open(path.join(destination, "inventory.json"), "wx", 0o600);
      try {
        await manifest.writeFile(
          JSON.stringify(
            {
              schemaVersion: 2,
              measuredAt: new Date().toISOString(),
              account,
              namespace,
              pages,
              objects,
              storedObjects,
              inventorySha256: digest.digest("hex"),
              limitsOfEvidence:
                "Object IDs and storage-presence flags only; not repository names, SQL/KV/R2 contents, a quiesced snapshot or a complete backup. Concurrent changes can affect enumeration. Reconcile under the backup's mutation barrier.",
            },
            null,
            2,
          ) + "\n",
        );
        await manifest.sync();
      } finally {
        await manifest.close();
      }
    } finally {
      await ids.close();
    }
    await syncDirectory(destination);
    await syncDirectory(path.dirname(path.resolve(destination)));
    complete = true;
  } finally {
    if (!complete) await fs.rm(destination, { recursive: true, force: true });
  }
};

export interface InventoryRow {
  readonly id: string;
  readonly hasStoredData: boolean;
  readonly line: string;
}

/** Bounded record reader; callers still verify the manifest, uniqueness and digest. */
export async function* readCloudflareObjectInventory(
  directory: string,
): AsyncGenerator<InventoryRow, void, void> {
  for await (const line of readBackupLines(path.join(directory, "objects.ndjson"), 256)) {
    const record = Schema.decodeSync(Schema.fromJsonString(ObjectRecord))(line);
    yield { ...record, line };
  }
}

/** Verify saved discovery before using its digest as signed backup resource evidence. */
export const verifyCloudflareObjectInventory = async (
  directory: string,
  account: string,
  namespace: string,
) => {
  const scratch = await fs.mkdtemp(path.join(tmpdir(), "git-inventory-verify-"));
  try {
    const manifestFile = await fs.open(
      path.join(directory, "inventory.json"),
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    let manifest;
    try {
      if (!(await manifestFile.stat()).isFile())
        throw new Error("Invalid Cloudflare inventory manifest");
      const buffer = Buffer.alloc(16385);
      let bytesRead = 0;
      while (bytesRead < buffer.length) {
        const result = await manifestFile.read(
          buffer,
          bytesRead,
          buffer.length - bytesRead,
          bytesRead,
        );
        if (result.bytesRead === 0) break;
        bytesRead += result.bytesRead;
      }
      if (bytesRead > 16384) throw new Error("Cloudflare inventory manifest exceeds budget");
      manifest = Schema.decodeSync(Schema.fromJsonString(Manifest))(
        new TextDecoder("utf-8", { fatal: true }).decode(buffer.subarray(0, bytesRead)),
      );
    } finally {
      await manifestFile.close();
    }
    assert.ok(
      manifest.objects <= manifest.pages * pageLimit,
      "Cloudflare inventory page count differs",
    );
    assert.equal(manifest.account, account, "Cloudflare inventory account mismatch");
    assert.equal(manifest.namespace, namespace, "Cloudflare inventory namespace mismatch");
    const ids = await distinctIds(scratch);
    try {
      const digest = inventoryHash(account, namespace);
      let objects = 0;
      let storedObjects = 0;
      for await (const record of readCloudflareObjectInventory(directory)) {
        digest.update(record.line + "\n");
        ids.add(record.id);
        objects += 1;
        assert.ok(objects <= 1_000_000, "Cloudflare inventory exceeds object budget");
        if (record.hasStoredData) storedObjects += 1;
      }
      assert.equal(objects, manifest.objects, "Cloudflare inventory object count differs");
      assert.equal(
        storedObjects,
        manifest.storedObjects,
        "Cloudflare inventory stored count differs",
      );
      assert.equal(
        digest.digest("hex"),
        manifest.inventorySha256,
        "Cloudflare inventory digest differs",
      );
      return manifest;
    } finally {
      await ids.close();
    }
  } catch {
    throw new Error("Cloudflare inventory verification failed");
  } finally {
    await fs.rm(scratch, { recursive: true, force: true });
  }
};

if (import.meta.main) {
  const verify = process.argv[2] === "--verify";
  const [account, namespace, destination] = process.argv.slice(verify ? 3 : 2);
  assert.ok(
    account && namespace && destination && process.argv.length === (verify ? 6 : 5),
    "usage: node tools/inventory-cloudflare-objects.ts [--verify] account-id namespace-id directory",
  );
  if (verify) {
    const manifest = await verifyCloudflareObjectInventory(destination, account, namespace);
    console.info(`Cloudflare object inventory verified: ${manifest.inventorySha256}`);
  } else {
    const token = await Effect.runPromise(Config.String("CLOUDFLARE_API_TOKEN"));
    await inventoryCloudflareObjects(account, namespace, destination, token);
    console.info("Cloudflare object inventory saved; object contents were not exported.");
  }
}
