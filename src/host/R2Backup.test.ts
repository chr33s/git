import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Miniflare } from "miniflare";
import type {
  R2Bucket,
  R2GetOptions,
  R2Object,
  R2PutOptions,
  ReadableStream,
} from "@cloudflare/workers-types";
import {
  copyR2Backup,
  inventoryR2Backup,
  restoreR2Backup,
  maxBackupObjectBytes,
} from "../../tools/copy-r2-backup.ts";

it("copies paginated real R2 bodies and metadata without replacing destination data", async () => {
  const mf = new Miniflare({
    workers: [
      {
        config: {
          name: "r2-backup",
          compatibilityDate: "2025-12-10",
          manifest: {
            mainModule: "worker.js",
            modules: {
              "worker.js": {
                type: "esm",
                contents: 'export default { fetch() { return new Response("fixture"); } };',
              },
            },
          },
          env: {
            SOURCE: { type: "r2", name: "backup-source" },
            RESUMED: { type: "r2", name: "backup-resumed" },
            BAD_RESUME: { type: "r2", name: "backup-bad-resume" },
            EXTRA_RESUME: { type: "r2", name: "backup-extra-resume" },
            UNTOUCHED: { type: "r2", name: "backup-untouched" },
            DESTINATION: { type: "r2", name: "backup-destination" },
            RACE: { type: "r2", name: "backup-race" },
            CHANGED: { type: "r2", name: "backup-changed" },
            METADATA: { type: "r2", name: "backup-metadata" },
            PARTIAL: { type: "r2", name: "backup-partial" },
            RESTORED: { type: "r2", name: "backup-restored" },
            CORRUPT: { type: "r2", name: "backup-corrupt" },
            WRONG_META: { type: "r2", name: "backup-wrong-meta" },
            REPLACED: { type: "r2", name: "backup-replaced" },
            META_VARIANT: { type: "r2", name: "backup-metadata-variant" },
          },
        },
      },
    ],
  });
  try {
    // SAFETY: Miniflare exposes native R2 semantics; its DOM types differ from workerd's.
    const source: R2Bucket = (await mf.getR2Bucket("SOURCE")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const destination: R2Bucket = (await mf.getR2Bucket("DESTINATION")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const race: R2Bucket = (await mf.getR2Bucket("RACE")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const changed: R2Bucket = (await mf.getR2Bucket("CHANGED")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const metadata: R2Bucket = (await mf.getR2Bucket("METADATA")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const partial: R2Bucket = (await mf.getR2Bucket("PARTIAL")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const restored: R2Bucket = (await mf.getR2Bucket("RESTORED")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const corrupt: R2Bucket = (await mf.getR2Bucket("CORRUPT")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const wrongMetadata: R2Bucket = (await mf.getR2Bucket("WRONG_META")) as never;
    // SAFETY: Same Miniflare/workerd DOM declaration boundary.
    const replaced: R2Bucket = (await mf.getR2Bucket("REPLACED")) as never;
    const payload = new Uint8Array(1024 * 1024).fill(127);
    for (const key of ["repo/objects/blob", "repo/lfs/object", "repo/snapshot.json"])
      await source.put(key, payload, {
        httpMetadata: { contentType: "application/octet-stream", cacheControl: "private" },
        customMetadata: { marker: "private-test-metadata" },
      });
    const initialInventory = await inventoryR2Backup(source);
    for (const size of [
      maxBackupObjectBytes + 1,
      -1,
      0.5,
      NaN,
      Infinity,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      let reads = 0;
      let writes = 0;
      const oversized = {
        list: async (options: Parameters<R2Bucket["list"]>[0]) => {
          const page = await source.list(options);
          return Object.assign({}, page, {
            objects: page.objects.map((entry) =>
              entry.key === "repo/snapshot.json"
                ? Object.assign({}, entry, {
                    size,
                    writeHttpMetadata: (headers: Parameters<R2Object["writeHttpMetadata"]>[0]) =>
                      entry.writeHttpMetadata(headers),
                  })
                : entry,
            ),
          });
        },
        get: (key: string, options: R2GetOptions) => {
          reads++;
          return source.get(key, options);
        },
      };
      const checkedDestination = {
        list: (options: Parameters<R2Bucket["list"]>[0]) => destination.list(options),
        get: (key: string, options: R2GetOptions) => destination.get(key, options),
        put: (key: string, body: ReadableStream, options: R2PutOptions) => {
          writes++;
          return destination.put(key, body, options);
        },
      };
      await assert.rejects(copyR2Backup(oversized, checkedDestination, 1), /single PUT size/);
      await assert.rejects(
        restoreR2Backup(oversized, checkedDestination, initialInventory, 1),
        /single PUT size/,
      );
      assert.equal(reads, 0, "unsupported size on the final page prevents all body reads");
      assert.equal(
        writes,
        0,
        "unsupported size on the final page prevents earlier keys being copied",
      );
      assert.equal((await destination.list()).objects.length, 0);
    }
    const report = await copyR2Backup(source, destination, 2);
    assert.equal(report.objects, 3);
    assert.equal(report.bytes, 3 * payload.byteLength);
    assert.equal(report.pages, 2);
    for (const entry of (await destination.list()).objects) {
      const held = await destination.get(entry.key);
      assert.ok(held);
      assert.deepEqual(new Uint8Array(await held.arrayBuffer()), payload);
      assert.equal(held.httpMetadata?.contentType, "application/octet-stream");
      assert.equal(held.httpMetadata?.cacheControl, "private");
      assert.equal(held.customMetadata?.marker, "private-test-metadata");
    }
    const restoredReport = await copyR2Backup(destination, restored, 1);
    assert.equal(restoredReport.objects, report.objects);
    assert.equal(restoredReport.bytes, report.bytes);
    assert.equal(restoredReport.contentInventorySha256, report.contentInventorySha256);
    assert.equal(restoredReport.metadataInventorySha256, report.metadataInventorySha256);
    assert.equal(report.schemaVersion, 2);
    // SAFETY: Miniflare exposes the same native R2 methods with different DOM declarations.
    const resumed: R2Bucket = (await mf.getR2Bucket("RESUMED")) as never;
    // SAFETY: Same native R2/DOM declaration boundary.
    const badResume: R2Bucket = (await mf.getR2Bucket("BAD_RESUME")) as never;
    // SAFETY: Same native R2/DOM declaration boundary.
    const extraResume: R2Bucket = (await mf.getR2Bucket("EXTRA_RESUME")) as never;
    // SAFETY: Same native R2/DOM declaration boundary.
    const untouched: R2Bucket = (await mf.getR2Bucket("UNTOUCHED")) as never;
    const expected = {
      objects: report.objects,
      bytes: report.bytes,
      contentInventorySha256: report.contentInventorySha256,
      metadataInventorySha256: report.metadataInventorySha256,
    };
    assert.deepEqual(await inventoryR2Backup(source, 1), expected);
    await assert.rejects(
      restoreR2Backup(
        source,
        {
          list: (options) => resumed.list(options),
          get: (key, options) => resumed.get(key, options),
          put: async (key, body, options) => {
            await resumed.put(key, body, options);
            throw new Error("lost R2 acknowledgement");
          },
        },
        expected,
        1,
      ),
      /lost R2 acknowledgement/,
    );
    const committed = (await resumed.list()).objects[0];
    assert.ok(committed);
    let resumedWrites = 0;
    const resumable = {
      list: (options: Parameters<R2Bucket["list"]>[0]) => resumed.list(options),
      get: (key: string, options: R2GetOptions) => resumed.get(key, options),
      put: (key: string, body: ReadableStream, options: R2PutOptions) => {
        resumedWrites += 1;
        return resumed.put(key, body, options);
      },
    };
    const resumedReport = await restoreR2Backup(source, resumable, expected, 1);
    assert.equal(resumedWrites, 2, "resume only writes missing objects");
    assert.equal(resumedReport.contentInventorySha256, expected.contentInventorySha256);
    assert.equal(resumedReport.metadataInventorySha256, expected.metadataInventorySha256);
    assert.equal((await resumed.head(committed.key))?.version, committed.version);
    const versions = (await resumed.list()).objects.map((object) => object.version);
    await restoreR2Backup(source, resumable, expected, 1);
    assert.equal(resumedWrites, 2, "complete retry does not rewrite any object");
    assert.deepEqual(
      (await resumed.list()).objects.map((object) => object.version),
      versions,
    );
    await assert.rejects(copyR2Backup(source, resumed), /must be empty/);
    let replacedDuringResume = false;
    await assert.rejects(
      restoreR2Backup(
        source,
        {
          list: (options) => resumed.list(options),
          get: async (key, options) => {
            const prior = await resumed.get(key, options);
            if (!replacedDuringResume && options.onlyIf === undefined) {
              replacedDuringResume = true;
              await resumed.put(key, payload, { customMetadata: { marker: "racing metadata" } });
            }
            return prior;
          },
          put: (key, body, options) => resumed.put(key, body, options),
        },
        expected,
        1,
      ),
      /verification version changed/,
    );

    const bad = await badResume.put(committed.key, new Uint8Array(payload.length).fill(128));
    assert.ok(bad);
    await assert.rejects(restoreR2Backup(source, badResume, expected, 1), /content digest differs/);
    assert.equal((await badResume.head(committed.key))?.version, bad.version);
    const badMetadata = await badResume.put(committed.key, payload, {
      httpMetadata: { contentType: "application/octet-stream", cacheControl: "private" },
      customMetadata: { marker: "private-corrupt-metadata" },
    });
    assert.ok(badMetadata);
    await assert.rejects(
      restoreR2Backup(source, badResume, expected, 1),
      (error: Error) =>
        error.message.includes("custom metadata differs") &&
        !error.message.includes("private-corrupt-metadata"),
    );
    assert.equal((await badResume.head(committed.key))?.version, badMetadata.version);
    await extraResume.put("unrelated/key", "keep this data");
    await assert.rejects(restoreR2Backup(source, extraResume, expected, 1), /inventory differs/);
    assert.equal(await (await extraResume.get("unrelated/key"))?.text(), "keep this data");
    assert.equal(
      (await extraResume.list()).objects.length,
      4,
      "unrelated keys are preserved but prevent successful restore",
    );
    await destination.put("repo/objects/blob", payload, {
      httpMetadata: { contentType: "application/octet-stream", cacheControl: "private" },
      customMetadata: { marker: "changed-private-metadata" },
    });
    // SAFETY: Miniflare and workerd expose the same R2 methods with differing DOM declarations.
    const metadataVariant: R2Bucket = (await mf.getR2Bucket("META_VARIANT")) as never;
    const variant = await copyR2Backup(destination, metadataVariant, 1);
    assert.equal(variant.contentInventorySha256, report.contentInventorySha256);
    assert.notEqual(variant.metadataInventorySha256, report.metadataInventorySha256);
    await assert.rejects(restoreR2Backup(destination, untouched, expected), /inventory differs/);
    assert.equal(
      (await untouched.list()).objects.length,
      0,
      "changed archive metadata must fail before target writes",
    );
    await assert.rejects(
      copyR2Backup(source, {
        list: (options) => corrupt.list(options),
        get: (key, options) => corrupt.get(key, options),
        put: (key, _body, options) =>
          corrupt.put(key, new Uint8Array(payload.length).fill(128), {
            onlyIf: options.onlyIf,
            httpMetadata: options.httpMetadata,
            customMetadata: options.customMetadata,
          }),
      }),
      /content digest differs/,
    );
    await assert.rejects(
      copyR2Backup(source, {
        list: (options) => wrongMetadata.list(options),
        get: (key, options) => wrongMetadata.get(key, options),
        put: (key, body, options) =>
          wrongMetadata.put(key, body, {
            ...options,
            customMetadata: { secret: "must-not-appear-in-error" },
          }),
      }),
      (error: Error) => {
        assert.match(error.message, /custom metadata differs/);
        assert.equal(error.message.includes("must-not-appear-in-error"), false);
        return true;
      },
    );
    await assert.rejects(
      copyR2Backup(source, {
        list: (options) => replaced.list(options),
        get: (key, options) => replaced.get(key, options),
        put: async (key, body, options) => {
          const copy = await replaced.put(key, body, options);
          await replaced.put(key, payload, { customMetadata: { later: "writer" } });
          return copy;
        },
      }),
      /verification version changed/,
    );
    await assert.rejects(copyR2Backup(source, destination), /must be empty/);
    await assert.rejects(
      copyR2Backup(source, {
        list: (options) => race.list(options),
        get: (key, options) => race.get(key, options),
        put: async (key, body, options) => {
          await race.put(key, "concurrent writer");
          return race.put(key, body, options);
        },
      }),
      /appeared concurrently/,
    );
    const raced = (await race.list()).objects[0];
    assert.ok(raced);
    assert.equal(await (await race.get(raced.key))?.text(), "concurrent writer");
    await assert.rejects(
      copyR2Backup(
        {
          list: (options) => source.list(options),
          get: async (key, options) => {
            await source.put(key, "changed after listing");
            return source.get(key, options);
          },
        },
        changed,
      ),
      /disappeared or changed/,
    );
    assert.equal((await changed.list()).objects.length, 0);
    await assert.rejects(
      copyR2Backup(
        {
          list: (options) => source.list(options),
          get: async (key, options) => {
            const original = await source.get(key);
            assert.ok(original);
            await source.put(key, original.body, { customMetadata: { changed: "metadata only" } });
            return source.get(key, options);
          },
        },
        metadata,
      ),
      /source version changed/,
    );
    assert.equal((await metadata.list()).objects.length, 0);
    let writes = 0;
    await assert.rejects(
      copyR2Backup(source, {
        list: (options) => partial.list(options),
        get: (key, options) => partial.get(key, options),
        put: async (key, body, options) => {
          writes += 1;
          if (writes === 2) throw new Error("injected output failure");
          return partial.put(key, body, options);
        },
      }),
      /injected output failure/,
    );
    assert.equal((await partial.list()).objects.length, 1, "keep partial data for inspection");
    assert.equal((await source.list()).objects.length, 3, "copy never deletes source data");
    await assert.rejects(copyR2Backup(source, partial), /must be empty/);
  } finally {
    await mf.dispose();
  }
});
