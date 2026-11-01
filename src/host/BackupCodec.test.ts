import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { releaseNamespaceArchive } from "../../tools/cloudflare-namespace-release.ts";
import { restoreNamespaceArchive } from "../../tools/cloudflare-namespace-restore.ts";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { it } from "@effect/vitest";
import type { Miniflare } from "miniflare";
import { startBackupRuntime } from "../testing/BackupRuntime.ts";
import {
  captureNamespaceArchive,
  verifyNamespaceArchive,
  Completion as NamespaceCompletion,
} from "../../tools/cloudflare-namespace-archive.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";
import { copyR2Backup } from "../../tools/copy-r2-backup.ts";
import { KvPage, restoreKey } from "../../tools/cloudflare-kv-backup.ts";
import { SqlCatalog, SqlPage } from "../../tools/cloudflare-sql-backup.ts";
import { Effect, Predicate, Schema } from "effect";
import * as Ssh from "../crypto/SshSignature.ts";
import {
  fenceKey,
  checkpointKey,
  legacyCheckpointKey,
} from "../../tools/cloudflare-backup-control.ts";
import {
  captureObjectArchive,
  restoreObjectArchive,
} from "../../tools/cloudflare-object-archive.ts";
import { Checkpoint } from "../../tools/cloudflare-object-import.ts";
import type { Request as BackupRequest } from "./CloudflareBackupAdmin.ts";
import * as Inventory from "../../tools/cloudflare-backup-inventory.ts";
import {
  packetBytes,
  verifyBackupFrame,
  signBackupFrame,
  type Position,
} from "../../tools/cloudflare-backup-auth.ts";
import {
  codec,
  frameBytes,
  encodeBackupFrame,
  decodeBackupFrame,
} from "../../tools/cloudflare-backup-codec.ts";

it("rejects unknown codecs, malformed payloads, oversized frames and unsupported values without data disclosure", () => {
  const secret = "private-backup-data";
  for (const frame of [
    secret,
    JSON.stringify({ schemaVersion: 1, codec: "future", payload: secret }),
    JSON.stringify({ schemaVersion: 1, codec, payload: secret }),
    encodeBackupFrame({ private: secret }),
    "a".repeat(frameBytes + 1),
  ])
    assert.throws(
      () => decodeBackupFrame(frame, Schema.String),
      (error: Error) => {
        assert.equal(error.message.includes(secret), false);
        return true;
      },
    );
  assert.throws(() => encodeBackupFrame({ [secret]: () => secret }), /cannot be encoded/);
  assert.throws(() => encodeBackupFrame("東京".repeat(frameBytes / 4)), /byte budget/);
});

it("round-trips native SQL blobs and typed KV data between fresh Durable Objects", async () => {
  const scratch = await fs.mkdtemp(path.join(tmpdir(), "backup-codec-"));
  let mf: Miniflare | undefined;
  try {
    const runtime = await startBackupRuntime(scratch);
    mf = runtime.mf;
    const { bindings, guarded } = runtime;
    const source = bindings.STORE.getByName("source");
    const target = bindings.STORE.getByName("target");
    await source.seed();
    const frame = await source.capture();
    await target.restore(frame);
    const Snapshot = Schema.Struct({
      rows: Schema.Array(Schema.Struct({ id: Schema.Int, body: Schema.instanceOf(ArrayBuffer) })),
      kv: Schema.Array(Schema.Tuple([Schema.String, Schema.Unknown])),
    });
    const captured = decodeBackupFrame(frame, Snapshot);
    const snapshot = decodeBackupFrame(await target.capture(), Snapshot);
    assert.deepEqual(snapshot, captured);
    assert.equal(snapshot.rows.length, 1);
    assert.equal(snapshot.kv.length, 3);
    assert.deepEqual(
      new Uint8Array(snapshot.rows[0]?.body ?? new ArrayBuffer(0)),
      new Uint8Array([0, 255, 128]),
    );
    assert.equal(new Map(snapshot.kv).get("HEAD:repo"), "refs/heads/main");
    assert.equal(new Map(snapshot.kv).get("snapshot-seq:repo"), 17);
    const typed = Schema.decodeUnknownSync(
      Schema.Struct({
        date: Schema.instanceOf(Date),
        map: Schema.instanceOf(Map),
        set: Schema.instanceOf(Set),
        binary: Schema.instanceOf(Uint8Array),
        graph: Schema.Unknown,
        alias: Schema.Unknown,
        negativeZero: Schema.Unknown,
        notANumber: Schema.Unknown,
      }),
    )(new Map(snapshot.kv).get("typed"));
    assert.equal(typed.date.getTime(), 0);
    assert.equal(typed.map.get("key"), 3n);
    assert.equal(typed.set.has("value"), true);
    assert.deepEqual(typed.binary, new Uint8Array([0, 255, 128]));
    assert.ok(Object.is(typed.negativeZero, -0));
    assert.equal(typed.notANumber, NaN);
    assert.equal(typed.graph, typed.alias);
    assert.ok(Predicate.isObject(typed.graph) && "self" in typed.graph);
    assert.equal(typed.graph.self, typed.graph);
    const kvTarget = guarded(bindings.STORE.getByName("kv-target"));
    const id = "snapshot-one";
    const first = await source.capturePage(id, null, 1);
    const firstPage = decodeBackupFrame(first, KvPage);
    assert.equal(firstPage.entries.length, 1);
    assert.ok(firstPage.next !== null);
    const second = await source.capturePage(id, firstPage.next, 1);
    const secondPage = decodeBackupFrame(second, KvPage);
    assert.ok(secondPage.next !== null);
    const third = await source.capturePage(id, secondPage.next, 1);
    await kvTarget.restorePage(first);
    await assert.rejects(kvTarget.finish(id), /not ready/);
    await assert.rejects(kvTarget.capturePage(id, null, 1), /unfinished/);
    await assert.rejects(kvTarget.restorePage(first), /must be empty/);
    await assert.rejects(kvTarget.restorePage(third), /out of sequence/);
    await assert.rejects(
      kvTarget.restorePage(encodeBackupFrame({ ...secondPage, snapshotId: "other" })),
      /out of sequence/,
    );
    await assert.rejects(
      kvTarget.restorePage(
        encodeBackupFrame({
          ...secondPage,
          entries: [...firstPage.entries, ...secondPage.entries],
        }),
      ),
      /replace existing/,
    );
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "kv-target" });
    await kvTarget.restorePage(second);
    await kvTarget.restorePage(third);
    await assert.rejects(kvTarget.finish("wrong-snapshot"), /not ready/);
    await assert.rejects(kvTarget.restorePage(third), /out of sequence/);
    await kvTarget.finish(id);
    const Entries = Schema.Array(Schema.Tuple([Schema.String, Schema.Unknown]));
    assert.deepEqual(decodeBackupFrame(await kvTarget.values(), Entries), snapshot.kv);
    await assert.rejects(kvTarget.finish(id), /Invalid KV restore progress/);
    const failing = guarded(bindings.STORE.getByName("kv-failure"));
    await assert.rejects(
      failing.restorePage(
        encodeBackupFrame({
          kind: "durable-kv",
          snapshotId: id,
          after: null,
          next: null,
          entries: [
            ["first", "must roll back"],
            ["invalid", undefined],
          ],
        }),
      ),
    );
    assert.deepEqual(
      decodeBackupFrame(await failing.values(), Entries),
      [],
      "a failed page rolls back its earlier writes and progress",
    );
    await assert.rejects(
      failing.restorePage(
        encodeBackupFrame({
          kind: "durable-kv",
          snapshotId: id,
          after: null,
          next: null,
          entries: [[restoreKey, "reserved"]],
        }),
      ),
      /reserved/,
    );
    const empty = guarded(bindings.STORE.getByName("kv-empty"));
    const emptyTarget = guarded(bindings.STORE.getByName("kv-empty-target"));
    await emptyTarget.restorePage(await empty.capturePage(id, null, 1));
    await emptyTarget.finish(id);
    assert.deepEqual(decodeBackupFrame(await emptyTarget.values(), Entries), []);
    const sqlSource = guarded(bindings.STORE.getByName("sql-source"));
    await sqlSource.invoke("sql-seed", "");
    const catalog = decodeBackupFrame(await sqlSource.invoke("sql-catalog", ""), SqlCatalog);
    assert.deepEqual(
      catalog.objects.map((object) => object.type),
      ["index", "table", "table", "trigger"],
    );
    assert.ok(catalog.objects.some((object) => object.name === "audit_rows"));
    const table = 'odd"table';
    const sqlPages = [];
    let after: string | null = null;
    do {
      const page: SqlPage = decodeBackupFrame(
        await sqlSource.invoke("sql-page", encodeBackupFrame([table, after, 1])),
        SqlPage,
      );
      sqlPages.push(page);
      after = page.next;
    } while (after !== null);
    assert.equal(sqlPages.length, 3);
    assert.deepEqual(
      sqlPages.flatMap((page) => page.rows.map((row) => row.rowid)),
      ["-2", "9007199254740993", "9223372036854775807"],
    );
    assert.deepEqual(
      sqlPages.flatMap((page) => page.rows.map((row) => row.cells[0])),
      [
        { type: "integer", value: "9223372036854775807" },
        { type: "integer", value: "-9223372036854775808" },
        { type: "real", value: 1.25 },
      ],
    );
    assert.deepEqual(sqlPages[0]?.rows[0]?.cells[1], {
      type: "blob",
      value: new Uint8Array([0, 255]).buffer,
    });
    assert.deepEqual(sqlPages[0]?.rows[0]?.cells[2], { type: "null", value: null });
    await assert.rejects(
      sqlSource.invoke("sql-page", encodeBackupFrame(["__cf_kv", null, 1])),
      /Unsupported/,
    );
    await assert.rejects(
      sqlSource.invoke("sql-page", encodeBackupFrame([table, "9223372036854775808", 1])),
      /range/,
    );
    const sqlTarget = guarded(bindings.STORE.getByName("sql-target"));
    const catalogFrame = encodeBackupFrame(catalog);
    const restorePage = (page: SqlPage) =>
      sqlTarget.invoke("sql-restore", encodeBackupFrame([id, encodeBackupFrame(page)]));
    await sqlTarget.invoke("sql-begin", encodeBackupFrame([id, catalogFrame]));
    await assert.rejects(
      sqlTarget.invoke("sql-begin", encodeBackupFrame([id, catalogFrame])),
      /rejected/,
    );
    await assert.rejects(sqlTarget.invoke("sql-catalog", ""), /unfinished/);
    await assert.rejects(sqlTarget.invoke("sql-finish", id), /rejected/);
    const auditPage = decodeBackupFrame(
      await sqlSource.invoke("sql-page", encodeBackupFrame(["audit", null, 1])),
      SqlPage,
    );
    await restorePage(auditPage);
    const firstSql = sqlPages[0];
    const secondSql = sqlPages[1];
    const thirdSql = sqlPages[2];
    assert.ok(firstSql && secondSql && thirdSql);
    await assert.rejects(restorePage(secondSql), /rejected/);
    await assert.rejects(
      sqlTarget.invoke("sql-restore", encodeBackupFrame(["other", encodeBackupFrame(firstSql)])),
      /rejected/,
    );
    // A late width failure must undo an earlier INSERT and keep the cursor unchanged.
    await assert.rejects(
      restorePage({ ...firstSql, rows: [...firstSql.rows, { rowid: "0", cells: [] }] }),
      /rejected/,
    );
    const failedRows = decodeBackupFrame(
      await sqlTarget.invoke("sql-page", encodeBackupFrame([table, null, 1])),
      SqlPage,
    );
    assert.deepEqual(failedRows.rows, []);
    await restorePage(firstSql);
    await assert.rejects(restorePage(firstSql), /rejected/);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "sql-target" });
    await restorePage(secondSql);
    await restorePage(thirdSql);
    await assert.rejects(restorePage(thirdSql), /rejected/);
    await sqlTarget.invoke("sql-finish", id);
    assert.deepEqual(
      decodeBackupFrame(await sqlTarget.invoke("sql-catalog", ""), SqlCatalog),
      catalog,
    );
    for (const page of sqlPages) {
      const restored = decodeBackupFrame(
        await sqlTarget.invoke("sql-page", encodeBackupFrame([table, page.after, 1])),
        SqlPage,
      );
      assert.deepEqual(restored, page);
    }
    assert.deepEqual(
      decodeBackupFrame(
        await sqlTarget.invoke("sql-page", encodeBackupFrame(["audit", null, 1])),
        SqlPage,
      ),
      auditPage,
      "restoration must not fire accounting triggers",
    );
    await sqlTarget.invoke("sql-insert", "");
    const auditAfter = decodeBackupFrame(
      await sqlTarget.invoke("sql-page", encodeBackupFrame(["audit", null, 1])),
      SqlPage,
    );
    assert.deepEqual(
      auditAfter.rows[0]?.cells[0],
      { type: "integer", value: "4" },
      "restored trigger must run for new writes",
    );
    const sqlEmpty = guarded(bindings.STORE.getByName("sql-empty"));
    const emptyCatalog = { kind: "durable-sql-catalog", objects: [] };
    await sqlEmpty.invoke("sql-begin", encodeBackupFrame([id, encodeBackupFrame(emptyCatalog)]));
    await sqlEmpty.invoke("sql-finish", id);
    assert.deepEqual(
      decodeBackupFrame(await sqlEmpty.invoke("sql-catalog", ""), SqlCatalog),
      emptyCatalog,
    );
    const ddlFailure = guarded(bindings.STORE.getByName("sql-ddl-failure"));
    const badCatalog = {
      kind: "durable-sql-catalog",
      objects: [
        { type: "table", name: "valid", table: "valid", sql: "CREATE TABLE valid (id INTEGER)" },
        { type: "table", name: "invalid", table: "invalid", sql: "CREATE TABLE invalid (" },
      ],
    };
    await assert.rejects(
      ddlFailure.invoke("sql-begin", encodeBackupFrame([id, encodeBackupFrame(badCatalog)])),
      /rejected/,
    );
    assert.deepEqual(
      decodeBackupFrame(await ddlFailure.invoke("sql-catalog", ""), SqlCatalog),
      emptyCatalog,
      "failed schema initialization must roll back earlier DDL",
    );
    // INTEGER PRIMARY KEY aliases rowid; verify that both survive exact read-back.
    const primarySource = guarded(source);
    const primaryTarget = guarded(bindings.STORE.getByName("sql-primary"));
    await primaryTarget.invoke(
      "sql-begin",
      encodeBackupFrame([id, await primarySource.invoke("sql-catalog", "")]),
    );
    const primaryFrame = await primarySource.invoke(
      "sql-page",
      encodeBackupFrame(["records", null, 1]),
    );
    await primaryTarget.invoke("sql-restore", encodeBackupFrame([id, primaryFrame]));
    await primaryTarget.invoke("sql-finish", id);
    assert.deepEqual(
      decodeBackupFrame(
        await primaryTarget.invoke("sql-page", encodeBackupFrame(["records", null, 1])),
        SqlPage,
      ),
      decodeBackupFrame(primaryFrame, SqlPage),
    );
    const application = guarded(bindings.STORE.getByName("application-source"));
    const recovered = guarded(bindings.STORE.getByName("application-restored"));
    await application.invoke("application-seed", "");
    const before = decodeBackupFrame(
      await application.invoke("application-source", ""),
      Schema.Unknown,
    );
    const applicationCatalogFrame = await application.invoke("sql-catalog", "");
    const applicationCatalog = decodeBackupFrame(applicationCatalogFrame, SqlCatalog);
    assert.deepEqual(
      applicationCatalog.objects
        .filter((object) => object.type === "table")
        .map((object) => object.name),
      [
        "git_capacity",
        "git_plus_provisioning",
        "git_ref_capacity",
        "reflog",
        "refs",
        "remotes",
        "shallow",
        "webhooks",
      ],
    );
    assert.equal(
      applicationCatalog.objects.filter((object) => object.type === "trigger").length,
      9,
    );
    const backupKey = await Effect.runPromise(Ssh.generate("native-backup-fixture"));
    const trustedSigner = await Effect.runPromise(Ssh.fingerprint(backupKey.publicKey));
    let capturedInventory = await Effect.runPromise(
      Inventory.startInventory("fixture-application", id),
    );
    await application.invoke("fence-start", id);
    assert.equal(await application.invoke("fence-wait", ""), "held");
    await recovered.invoke("fence-start", id);
    assert.equal(await recovered.invoke("fence-wait", ""), "held");
    await recovered.invoke(
      "import-start",
      encodeBackupFrame({
        installationId: "fixture-application",
        snapshotId: id,
        objectId: "application-source",
        signer: trustedSigner,
      }),
    );
    const transfer = async (kind: Position["kind"], payload: string) => {
      const position: Position = {
        installationId: "fixture-application",
        snapshotId: id,
        objectId: "application-source",
        kind,
        sequence: capturedInventory.frames,
      };
      const captured = await Effect.runPromise(
        Inventory.captureInventoryFrame(backupKey, capturedInventory, position, payload),
      );
      const receipt = decodeBackupFrame(
        await recovered.invoke("import-apply", captured.packet),
        Checkpoint,
      );
      assert.deepEqual(receipt.inventory, captured.state);
      capturedInventory = captured.state;
    };
    await transfer("sql-catalog", applicationCatalogFrame);
    for (const object of applicationCatalog.objects.filter((object) => object.type === "table")) {
      let cursor: string | null = null;
      do {
        const frame = await application.invoke(
          "sql-page",
          encodeBackupFrame([object.name, cursor, 1]),
        );
        const page: SqlPage = decodeBackupFrame(frame, SqlPage);
        await transfer("sql-page", frame);
        cursor = page.next;
      } while (cursor !== null);
    }
    let kvCursor: string | null = null;
    do {
      const frame = await application.capturePage(id, kvCursor, 1);
      const page: typeof KvPage.Type = decodeBackupFrame(frame, KvPage);
      await transfer("kv-page", frame);
      kvCursor = page.next;
    } while (kvCursor !== null);
    const copied = await copyR2Backup(bindings.SOURCE, bindings.RESTORED, 1);
    assert.equal(copied.objects, 7);
    const resources: Inventory.Resources = {
      namespaceInventorySha256: createHash("sha256")
        .update(JSON.stringify(["application-source"]))
        .digest("hex"),
      deploymentSha256: createHash("sha256")
        .update(await fs.readFile(path.join(scratch, "BackupCodec.worker.mjs")))
        .digest("hex"),
      r2: {
        objects: copied.objects,
        bytes: copied.bytes,
        contentInventorySha256: copied.contentInventorySha256,
        metadataInventorySha256: copied.metadataInventorySha256,
      },
    };
    const seal = await Effect.runPromise(
      Inventory.sealInventory(backupKey, capturedInventory, resources),
    );
    await assert.rejects(
      recovered.invoke(
        "import-finish",
        encodeBackupFrame([
          seal,
          { ...resources, r2: { ...resources.r2, objects: copied.objects + 1 } },
        ]),
      ),
    );
    await assert.rejects(recovered.invoke("sql-catalog", ""), /unfinished/);
    await assert.rejects(recovered.capturePage(id, null, 1), /checkpoint/);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "application-restored" });
    const applicationReceipt = decodeBackupFrame(
      await recovered.invoke("import-finish", encodeBackupFrame([seal, resources])),
      Checkpoint,
    );
    assert.deepEqual(applicationReceipt.inventory, capturedInventory);
    assert.equal(await recovered.invoke("fence-request", ""), "503");
    await recovered.invoke("fence-release", id);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "application-restored" });
    assert.deepEqual(
      decodeBackupFrame(await recovered.invoke("application-restored", ""), Schema.Unknown),
      before,
    );
    const archiveDirectory = path.join(scratch, "application-archive");
    const archiveSourceId = await application.invoke("object-id", "");
    const archivePlan = {
      installationId: "fixture-archive",
      snapshotId: id,
      objectId: archiveSourceId,
      signer: trustedSigner,
    };
    const archiveSource = {
      backup: (request: BackupRequest) => application.invoke("admin", encodeBackupFrame(request)),
    };
    const archived = await Effect.runPromise(
      captureObjectArchive({
        client: archiveSource,
        directory: archiveDirectory,
        plan: archivePlan,
        resources,
        key: backupKey,
      }),
    );
    assert.equal((await fs.stat(archiveDirectory)).mode & 0o077, 0);
    for (const file of await fs.readdir(archiveDirectory))
      assert.equal((await fs.stat(path.join(archiveDirectory, file))).mode & 0o077, 0);
    await assert.rejects(
      Effect.runPromise(
        captureObjectArchive({
          client: archiveSource,
          directory: archiveDirectory,
          plan: archivePlan,
          resources,
          key: backupKey,
        }),
      ),
    );
    const partialArchiveDirectory = path.join(scratch, "partial-archive");
    await assert.rejects(
      Effect.runPromise(
        captureObjectArchive({
          client: {
            backup: async (request: BackupRequest) => {
              if (request.command._tag === "SqlPage")
                throw new Error("private archive transport failure");
              return archiveSource.backup(request);
            },
          },
          directory: partialArchiveDirectory,
          plan: archivePlan,
          resources,
          key: backupKey,
        }),
      ),
      (error: Error) => !error.message.includes("private archive transport failure"),
    );
    await assert.rejects(fs.stat(path.join(partialArchiveDirectory, "complete.json")), {
      code: "ENOENT",
    });
    assert.equal(await application.invoke("fence-request", ""), "503");
    const archiveTarget = guarded(bindings.STORE.getByName("archive-application-target"));
    const archiveTargetId = await archiveTarget.invoke("object-id", "");
    for (const response of ["", "released", "draining"]) {
      const commands: string[] = [];
      const client = {
        backup: async (request: BackupRequest) => {
          commands.push(request.command._tag);
          return response;
        },
      };
      const rejectedDirectory = path.join(scratch, `unconfirmed-hold-${response || "empty"}`);
      await assert.rejects(
        Effect.runPromise(
          captureObjectArchive({
            client,
            directory: rejectedDirectory,
            plan: archivePlan,
            resources,
            key: backupKey,
          }),
        ),
      );
      assert.deepEqual(commands, ["Hold"], "capture cannot read data without confirmed hold");
      assert.deepEqual(await fs.readdir(rejectedDirectory), []);
      commands.length = 0;
      await assert.rejects(
        Effect.runPromise(
          restoreObjectArchive({
            client,
            directory: archiveDirectory,
            plan: archivePlan,
            resources,
            targetObjectId: archiveTargetId,
          }),
        ),
      );
      assert.deepEqual(commands, ["Hold"], "restore cannot start import without confirmed hold");
    }
    for (const field of ["plan", "lastPacket"] as const) {
      const target = guarded(bindings.STORE.getByName(`wrong-apply-${field}`));
      const targetId = await target.invoke("object-id", "");
      const commands: string[] = [];
      await assert.rejects(
        Effect.runPromise(
          restoreObjectArchive({
            client: {
              backup: async (request) => {
                commands.push(request.command._tag);
                const frame = await target.invoke("admin", encodeBackupFrame(request));
                if (request.command._tag !== "Apply") return frame;
                const receipt = decodeBackupFrame(frame, Checkpoint);
                return encodeBackupFrame(
                  field === "plan"
                    ? { ...receipt, plan: { ...receipt.plan, objectId: "wrong-source" } }
                    : { ...receipt, lastPacket: "0".repeat(64) },
                );
              },
            },
            directory: archiveDirectory,
            plan: archivePlan,
            resources,
            targetObjectId: targetId,
          }),
        ),
      );
      assert.deepEqual(commands, ["Hold", "Start", "Apply"]);
      assert.equal(await target.invoke("fence-request", ""), "503");
    }
    let archiveCalls = 0;
    let applied = 0;
    let loseAcknowledgement = true;
    const archiveClient = {
      backup: async (request: BackupRequest) => {
        archiveCalls += 1;
        const result = await archiveTarget.invoke("admin", encodeBackupFrame(request));
        if (request.command._tag === "Apply") {
          applied += 1;
          if (loseAcknowledgement && applied === 3) throw new Error("private lost acknowledgement");
        }
        return result;
      },
    };
    const restoreArchive = () =>
      Effect.runPromise(
        restoreObjectArchive({
          client: archiveClient,
          directory: archiveDirectory,
          plan: archivePlan,
          resources,
          targetObjectId: archiveTargetId,
        }),
      );
    const firstPacketPath = path.join(archiveDirectory, "000000000000.packet");
    const originalPacket = await fs.readFile(firstPacketPath, "utf8");
    await fs.writeFile(firstPacketPath, originalPacket.replace('"sql-catalog"', '"kv-page"'));
    await assert.rejects(restoreArchive());
    assert.equal(archiveCalls, 0, "tampered archive must fail before touching the target");
    await fs.writeFile(firstPacketPath, originalPacket.slice(0, -1));
    await assert.rejects(restoreArchive());
    await fs.writeFile(firstPacketPath, Buffer.alloc(packetBytes + 1));
    await assert.rejects(restoreArchive());
    const savedPacketPath = path.join(scratch, "saved-packet");
    await fs.writeFile(savedPacketPath, originalPacket);
    await fs.unlink(firstPacketPath);
    await fs.symlink(savedPacketPath, firstPacketPath);
    await assert.rejects(restoreArchive());
    await fs.unlink(firstPacketPath);
    await fs.writeFile(firstPacketPath, originalPacket, { mode: 0o600 });
    const completionPath = path.join(archiveDirectory, "complete.json");
    const savedCompletionPath = path.join(scratch, "saved-completion");
    await fs.rename(completionPath, savedCompletionPath);
    await assert.rejects(restoreArchive());
    await fs.rename(savedCompletionPath, completionPath);
    assert.equal(
      archiveCalls,
      0,
      "incomplete, oversized or linked archives must not touch the target",
    );
    await assert.rejects(
      Effect.runPromise(
        restoreObjectArchive({
          client: archiveClient,
          directory: archiveDirectory,
          plan: archivePlan,
          resources: { ...resources, r2: { ...resources.r2, objects: copied.objects + 1 } },
          targetObjectId: archiveTargetId,
        }),
      ),
    );
    assert.equal(archiveCalls, 0);
    await assert.rejects(restoreArchive());
    assert.equal(applied, 3);
    const partialReceipt = decodeBackupFrame(
      await archiveTarget.invoke("import-status", ""),
      Checkpoint,
    );
    assert.equal(partialReceipt.inventory.frames, 3);
    assert.equal(await archiveTarget.invoke("fence-request", ""), "503");
    await mf.unsafeEvictDurableObject("backup-codec", "Store", {
      name: "archive-application-target",
    });
    const alternateDirectory = path.join(scratch, "alternate-archive");
    await fs.mkdir(alternateDirectory);
    let alternateInventory = await Effect.runPromise(
      Inventory.startInventory(archivePlan.installationId, id),
    );
    const PacketPayload = Schema.fromJsonString(
      Schema.Struct({
        payload: Schema.String,
        position: Schema.Struct({ kind: Schema.Literals(["sql-catalog", "sql-page", "kv-page"]) }),
      }),
    );
    for (let sequence = 0; sequence < archived.inventory.frames; sequence++) {
      const filename = `${String(sequence).padStart(12, "0")}.packet`;
      const original = Schema.decodeSync(PacketPayload)(
        await fs.readFile(path.join(archiveDirectory, filename), "utf8"),
      );
      const replacement = await Effect.runPromise(
        Inventory.captureInventoryFrame(
          backupKey,
          alternateInventory,
          {
            installationId: archivePlan.installationId,
            snapshotId: id,
            objectId: archiveSourceId,
            kind: original.position.kind,
            sequence,
          },
          sequence === 0
            ? encodeBackupFrame({ kind: "durable-sql-catalog", objects: [] })
            : original.payload,
        ),
      );
      await fs.writeFile(path.join(alternateDirectory, filename), replacement.packet);
      alternateInventory = replacement.state;
    }
    const alternateSeal = await Effect.runPromise(
      Inventory.sealInventory(backupKey, alternateInventory, resources),
    );
    await fs.writeFile(
      path.join(alternateDirectory, "complete.json"),
      encodeBackupFrame({ ...archived, inventory: alternateInventory, seal: alternateSeal }),
    );
    await assert.rejects(
      Effect.runPromise(
        restoreObjectArchive({
          client: archiveClient,
          directory: alternateDirectory,
          plan: archivePlan,
          resources,
          targetObjectId: archiveTargetId,
        }),
      ),
    );
    assert.equal(
      applied,
      3,
      "a different authentic archive must not extend an existing checkpoint",
    );
    assert.deepEqual(
      decodeBackupFrame(await archiveTarget.invoke("import-status", ""), Checkpoint),
      partialReceipt,
    );
    loseAcknowledgement = false;
    const archiveReceipt = await restoreArchive();
    assert.equal(
      applied,
      archived.inventory.frames,
      "resume must verify and skip the committed prefix",
    );
    assert.deepEqual(decodeBackupFrame(archiveReceipt, Checkpoint).inventory, archived.inventory);
    assert.equal(await restoreArchive(), archiveReceipt);
    assert.equal(applied, archived.inventory.frames, "completed retries must not reapply pages");
    for (const field of ["start-plan", "finish-plan", "phase", "inventory", "seal"] as const) {
      const commands: string[] = [];
      await assert.rejects(
        Effect.runPromise(
          restoreObjectArchive({
            client: {
              backup: async (request) => {
                commands.push(request.command._tag);
                const frame = await archiveClient.backup(request);
                const stage = field === "start-plan" ? "Start" : "Finish";
                if (request.command._tag !== stage) return frame;
                const receipt = decodeBackupFrame(frame, Checkpoint);
                switch (field) {
                  case "start-plan":
                  case "finish-plan":
                    return encodeBackupFrame({
                      ...receipt,
                      plan: { ...receipt.plan, objectId: "wrong-source" },
                    });
                  case "phase":
                    return encodeBackupFrame({ ...receipt, phase: "kv" });
                  case "inventory":
                    return encodeBackupFrame({
                      ...receipt,
                      inventory: { ...receipt.inventory, frames: 0 },
                    });
                  case "seal":
                    return encodeBackupFrame({ ...receipt, seal: "0".repeat(64) });
                }
              },
            },
            directory: archiveDirectory,
            plan: archivePlan,
            resources,
            targetObjectId: archiveTargetId,
          }),
        ),
      );
      assert.deepEqual(
        commands,
        field === "start-plan" ? ["Hold", "Start"] : ["Hold", "Start", "Finish"],
      );
      assert.equal(await archiveTarget.invoke("fence-request", ""), "503");
    }
    await archiveClient.backup({
      objectId: archiveTargetId,
      snapshotId: id,
      command: { _tag: "Retire", receipt: archiveReceipt },
    });
    await archiveClient.backup({
      objectId: archiveTargetId,
      snapshotId: id,
      command: { _tag: "Release" },
    });
    assert.deepEqual(
      decodeBackupFrame(await archiveTarget.invoke("application-restored", ""), Schema.Unknown),
      before,
    );
    const changed = decodeBackupFrame(
      await recovered.invoke("application-mutate", ""),
      Schema.Struct({
        metadata: Schema.Array(Schema.Struct({ rows: Schema.Int, bytes: Schema.Int })),
        refs: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
        snapshot: Schema.Int,
        published: Schema.Struct({ refs: Schema.Array(Schema.Struct({ name: Schema.String })) }),
        journal: Schema.Struct({ seq: Schema.Int }),
        packs: Schema.Array(Schema.Struct({ name: Schema.String })),
        lfs: Schema.Struct({
          uncertainUploadBlocked: Schema.Literal(true),
          bytes: Schema.Int,
          head: Schema.Struct({ oid: Schema.String, size: Schema.Int }),
          lock: Schema.Null,
        }),
      }),
    );
    assert.equal(changed.refs.length, 2);
    assert.equal(changed.snapshot, 25);
    assert.equal(changed.journal.seq, changed.snapshot);
    assert.equal(changed.published.refs.length, 2);
    assert.equal(changed.packs.length, 1);
    assert.equal(changed.lfs.bytes, 9 * 1024 ** 2);
    assert.equal(changed.lfs.head.size, changed.lfs.bytes);
    assert.equal(
      changed.metadata[0]?.rows,
      5,
      "restored ref triggers account for the new ref and reflog",
    );
    assert.deepEqual(
      decodeBackupFrame(await application.invoke("application-source", ""), Schema.Unknown),
      before,
      "writing to the recovered installation must not change its source",
    );
    const importing = guarded(bindings.STORE.getByName("durable-sql-import"));
    const plan = {
      installationId: "fixture-sql",
      snapshotId: id,
      objectId: "sql-source",
      signer: trustedSigner,
    };
    const planFrame = encodeBackupFrame(plan);
    await assert.rejects(importing.invoke("import-start", planFrame));
    await importing.invoke("fence-start", id);
    assert.equal(await importing.invoke("fence-wait", ""), "held");
    const initial = await importing.invoke("import-start", planFrame);
    assert.equal(await importing.invoke("import-start", planFrame), initial);
    await assert.rejects(
      importing.invoke("import-start", encodeBackupFrame({ ...plan, objectId: "wrong" })),
    );
    let importInventory = await Effect.runPromise(
      Inventory.startInventory(plan.installationId, id),
    );
    const packets: string[] = [];
    for (const payload of [
      catalogFrame,
      encodeBackupFrame(auditPage),
      ...sqlPages.map(encodeBackupFrame),
      first,
      second,
      third,
    ]) {
      const captured = await Effect.runPromise(
        Inventory.captureInventoryFrame(
          backupKey,
          importInventory,
          {
            installationId: plan.installationId,
            snapshotId: id,
            objectId: plan.objectId,
            kind:
              importInventory.frames >= 5
                ? "kv-page"
                : importInventory.frames === 0
                  ? "sql-catalog"
                  : "sql-page",
            sequence: importInventory.frames,
          },
          payload,
        ),
      );
      packets.push(captured.packet);
      importInventory = captured.state;
    }
    for (const [index, packet] of packets.entries()) {
      const beforeCheckpoint = await importing.invoke("import-status", "");
      const beforeValues = await importing.values();
      if (index === 5) {
        const current = decodeBackupFrame(beforeCheckpoint, Checkpoint);
        const prematureSeal = await Effect.runPromise(
          Inventory.sealInventory(backupKey, current.inventory, resources),
        );
        await assert.rejects(
          importing.invoke("import-finish", encodeBackupFrame([prematureSeal, resources])),
        );
        for (const invalidPage of [
          ...[fenceKey, checkpointKey, legacyCheckpointKey, restoreKey].map((key) => ({
            ...firstPage,
            entries: [[key, "reserved"]],
            next: null,
          })),
          { ...firstPage, snapshotId: "wrong" },
          {
            ...firstPage,
            entries: [
              ["b", 1],
              ["a", 2],
            ],
            next: null,
          },
          {
            ...firstPage,
            entries: [
              ["a", 1],
              ["b", undefined],
            ],
            next: null,
          },
        ]) {
          const bad = await Effect.runPromise(
            Inventory.captureInventoryFrame(
              backupKey,
              current.inventory,
              {
                installationId: plan.installationId,
                snapshotId: id,
                objectId: plan.objectId,
                kind: "kv-page",
                sequence: current.inventory.frames,
              },
              encodeBackupFrame(invalidPage),
            ),
          );
          await assert.rejects(importing.invoke("import-apply", bad.packet));
          assert.equal(await importing.invoke("import-status", ""), beforeCheckpoint);
          assert.equal(await importing.values(), beforeValues);
        }
      }
      await importing.invoke("import-fail", "on");
      await assert.rejects(importing.invoke("import-apply", packet));
      assert.equal(await importing.invoke("import-status", ""), beforeCheckpoint);
      assert.equal(await importing.values(), beforeValues);
      if (index === 0)
        assert.deepEqual(
          decodeBackupFrame(await importing.invoke("sql-catalog", ""), SqlCatalog).objects,
          [],
        );
      else if (index === 2)
        assert.deepEqual(
          decodeBackupFrame(
            await importing.invoke("sql-page", encodeBackupFrame([table, null, 1])),
            SqlPage,
          ).rows,
          [],
        );
      await importing.invoke("import-fail", "off");
      if (index === 1) {
        await importing.invoke("fence-sync-fail", "on");
        await assert.rejects(importing.invoke("import-apply", packet));
        assert.equal(
          decodeBackupFrame(await importing.invoke("import-status", ""), Checkpoint).inventory
            .frames,
          index + 1,
        );
        await importing.invoke("fence-sync-fail", "off");
      }
      const [one, duplicate] = await Promise.all([
        importing.invoke("import-apply", packet),
        importing.invoke("import-apply", packet),
      ]);
      assert.equal(one, duplicate);
      assert.equal(decodeBackupFrame(one, Checkpoint).inventory.frames, index + 1);
      await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "durable-sql-import" });
      assert.equal(await importing.invoke("import-apply", packet), one);
    }
    const extraPage = await Effect.runPromise(
      Inventory.captureInventoryFrame(
        backupKey,
        importInventory,
        {
          installationId: plan.installationId,
          snapshotId: id,
          objectId: plan.objectId,
          kind: "kv-page",
          sequence: importInventory.frames,
        },
        encodeBackupFrame({
          kind: "durable-kv",
          snapshotId: id,
          after: null,
          next: null,
          entries: [],
        }),
      ),
    );
    await assert.rejects(importing.invoke("import-apply", extraPage.packet));
    const firstPacket = packets[0];
    assert.ok(firstPacket);
    await assert.rejects(importing.invoke("import-apply", firstPacket));
    const importSeal = await Effect.runPromise(
      Inventory.sealInventory(backupKey, importInventory, resources),
    );
    const finishFrame = encodeBackupFrame([importSeal, resources]);
    await assert.rejects(
      importing.invoke(
        "import-finish",
        encodeBackupFrame([
          importSeal,
          { ...resources, r2: { ...resources.r2, objects: resources.r2.objects + 1 } },
        ]),
      ),
    );
    await importing.invoke("import-fail", "on");
    await assert.rejects(importing.invoke("import-finish", finishFrame));
    await assert.rejects(importing.invoke("sql-catalog", ""), /unfinished/);
    await importing.invoke("import-fail", "off");
    const finished = await importing.invoke("import-finish", finishFrame);
    assert.equal(decodeBackupFrame(finished, Checkpoint).phase, "complete");
    assert.deepEqual(decodeBackupFrame(finished, Checkpoint).inventory, importInventory);
    for (const page of [auditPage, ...sqlPages])
      assert.deepEqual(
        decodeBackupFrame(
          await importing.invoke("sql-page", encodeBackupFrame([page.table, page.after, 1])),
          SqlPage,
        ),
        page,
      );
    assert.equal(await importing.invoke("sql-catalog", ""), catalogFrame);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "durable-sql-import" });
    assert.equal(await importing.invoke("import-finish", finishFrame), finished);
    assert.equal(await importing.invoke("fence-request", ""), "503");
    assert.deepEqual(
      decodeBackupFrame(await importing.values(), Entries).filter(
        ([key]) => key !== fenceKey && key !== checkpointKey,
      ),
      snapshot.kv,
    );
    await assert.rejects(importing.capturePage(id, null, 1), /checkpoint/);
    await kvTarget.invoke("fence-start", id);
    assert.equal(await kvTarget.invoke("fence-wait", ""), "held");
    assert.equal(await kvTarget.capturePage(id, null, 1), first);
    assert.equal(await kvTarget.capturePage(id, firstPage.next, 1), second);
    assert.equal(await kvTarget.capturePage(id, secondPage.next, 1), third);
    await assert.rejects(kvTarget.capturePage("wrong", null, 1), /fence mismatch/);
    await empty.invoke("fence-start", id);
    assert.equal(await empty.invoke("fence-wait", ""), "held");
    assert.deepEqual(decodeBackupFrame(await empty.capturePage(id, null, 1), KvPage).entries, []);
    const emptyImport = guarded(bindings.STORE.getByName("empty-object-import"));
    await emptyImport.invoke("fence-start", id);
    assert.equal(await emptyImport.invoke("fence-wait", ""), "held");
    await emptyImport.invoke("import-start", planFrame);
    let emptyInventory = await Effect.runPromise(Inventory.startInventory(plan.installationId, id));
    const emptyPayloads: ReadonlyArray<readonly [Position["kind"], string]> = [
      ["sql-catalog", encodeBackupFrame(emptyCatalog)],
      ["kv-page", await empty.capturePage(id, null, 1)],
    ];
    for (const [kind, payload] of emptyPayloads) {
      const captured = await Effect.runPromise(
        Inventory.captureInventoryFrame(
          backupKey,
          emptyInventory,
          {
            installationId: plan.installationId,
            snapshotId: id,
            objectId: plan.objectId,
            kind,
            sequence: emptyInventory.frames,
          },
          payload,
        ),
      );
      await emptyImport.invoke("import-apply", captured.packet);
      emptyInventory = captured.state;
    }
    const emptySeal = await Effect.runPromise(
      Inventory.sealInventory(backupKey, emptyInventory, resources),
    );
    const emptyReceipt = decodeBackupFrame(
      await emptyImport.invoke("import-finish", encodeBackupFrame([emptySeal, resources])),
      Checkpoint,
    );
    assert.equal(emptyReceipt.kv?.keys, 0);
    assert.equal(emptyReceipt.kv?.complete, true);
    assert.equal(emptyReceipt.phase, "complete");
    const adminTarget = guarded(bindings.STORE.getByName("empty-object-import"));
    const adminId = await adminTarget.invoke("object-id", "");
    const administer = (command: BackupRequest["command"], snapshotId = id, objectId = adminId) =>
      adminTarget.invoke("admin", encodeBackupFrame({ objectId, snapshotId, command }));
    await assert.rejects(administer({ _tag: "Status" }, id, "0".repeat(64)));
    await assert.rejects(administer({ _tag: "Status" }, "wrong"));
    const exactReceipt = await administer({ _tag: "Status" });
    await assert.rejects(administer({ _tag: "Release" }));
    await assert.rejects(administer({ _tag: "AbandonCapture" }));
    await assert.rejects(administer({ _tag: "Retire", receipt: initial }));
    await assert.rejects(administer({ _tag: "Retire", receipt: finished }));
    await assert.rejects(administer({ _tag: "KvPage", after: null, limit: 1 }));
    assert.equal(await administer({ _tag: "Retire", receipt: exactReceipt }), "retired");
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "empty-object-import" });
    assert.equal(await administer({ _tag: "Retire", receipt: exactReceipt }), "retired");
    assert.deepEqual(
      decodeBackupFrame(await administer({ _tag: "Catalog" }), SqlCatalog),
      emptyCatalog,
    );
    assert.deepEqual(
      decodeBackupFrame(await administer({ _tag: "KvPage", after: null, limit: 1 }), KvPage)
        .entries,
      [],
    );
    assert.equal(await adminTarget.invoke("fence-request", ""), "503");
    assert.equal(await administer({ _tag: "Release" }), "released");
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "empty-object-import" });
    assert.equal(
      await administer({ _tag: "Release" }),
      "released",
      "lost release acknowledgement can be retried after eviction",
    );
    assert.equal(await adminTarget.invoke("fence-request", ""), "204");
    // A delayed retry must never remove a fence belonging to a later snapshot.
    assert.equal(await administer({ _tag: "Hold" }, "later-snapshot"), "held");
    await assert.rejects(administer({ _tag: "Release" }));
    await assert.rejects(administer({ _tag: "AbandonCapture" }));
    assert.equal(await adminTarget.invoke("fence-request", ""), "503");
    assert.equal(await administer({ _tag: "Release" }, "later-snapshot"), "released");
    await assert.rejects(administer({ _tag: "Catalog" }));
    assert.equal(await administer({ _tag: "Hold" }), "held");
    await administer({ _tag: "Start", plan });
    await assert.rejects(administer({ _tag: "Release" }));
    await assert.rejects(administer({ _tag: "AbandonCapture" }));
    const emptyCatalogPacket = await Effect.runPromise(
      Inventory.captureInventoryFrame(
        backupKey,
        await Effect.runPromise(Inventory.startInventory(plan.installationId, id)),
        {
          installationId: plan.installationId,
          snapshotId: id,
          objectId: plan.objectId,
          kind: "sql-catalog",
          sequence: 0,
        },
        encodeBackupFrame(emptyCatalog),
      ),
    );
    await administer({ _tag: "Apply", packet: emptyCatalogPacket.packet });
    await assert.rejects(administer({ _tag: "Release" }));
    await assert.rejects(administer({ _tag: "AbandonCapture" }));
    const emptyKvPacket = await Effect.runPromise(
      Inventory.captureInventoryFrame(
        backupKey,
        emptyCatalogPacket.state,
        {
          installationId: plan.installationId,
          snapshotId: id,
          objectId: plan.objectId,
          kind: "kv-page",
          sequence: 1,
        },
        await empty.capturePage(id, null, 1),
      ),
    );
    await administer({ _tag: "Apply", packet: emptyKvPacket.packet });
    const adminReceipt = await administer({ _tag: "Finish", packet: emptySeal, resources });
    assert.equal(adminReceipt, exactReceipt);
    await administer({ _tag: "Retire", receipt: adminReceipt });
    await administer({ _tag: "Release" });
    const artifacts = guarded(bindings.STORE.getByName("artifacts-source"));
    const artifactsRestored = guarded(bindings.STORE.getByName("artifacts-restored"));
    const credentials = await artifacts.invoke("artifacts-seed", "");
    const plaintext = Object.values(
      decodeBackupFrame(
        credentials,
        Schema.Struct({
          read: Schema.String,
          write: Schema.String,
          revoked: Schema.String,
          expired: Schema.String,
        }),
      ),
    );
    const Authority = Schema.Struct({
      read: Schema.Literal("read"),
      write: Schema.Literal("write"),
      revoked: Schema.Null,
      expired: Schema.Null,
      wrongRepository: Schema.Null,
      repositories: Schema.Struct({
        repos: Schema.Array(
          Schema.Struct({ name: Schema.String, initializing: Schema.NullOr(Schema.String) }),
        ),
      }),
    });
    const authorityBefore = await artifacts.invoke("artifacts-read", credentials);
    const originalAuthority = decodeBackupFrame(authorityBefore, Authority);
    assert.equal(
      originalAuthority.repositories.repos.find((repo) => repo.name === "pending-fork")
        ?.initializing,
      "fork",
    );
    const artifactsCatalogFrame = await artifacts.invoke("sql-catalog", "");
    const artifactsCatalog = decodeBackupFrame(artifactsCatalogFrame, SqlCatalog);
    assert.deepEqual(
      artifactsCatalog.objects
        .filter((object) => object.type === "table")
        .map((object) => object.name),
      ["repos", "tokens"],
    );
    await artifactsRestored.invoke("sql-begin", encodeBackupFrame([id, artifactsCatalogFrame]));
    for (const object of artifactsCatalog.objects.filter((object) => object.type === "table")) {
      let cursor: string | null = null;
      do {
        const frame = await artifacts.invoke(
          "sql-page",
          encodeBackupFrame([object.name, cursor, 1]),
        );
        for (const token of plaintext)
          assert.equal(
            frame.includes(token),
            false,
            "SQL backup must contain token digests, not plaintext credentials",
          );
        const page: SqlPage = decodeBackupFrame(frame, SqlPage);
        await artifactsRestored.invoke("sql-restore", encodeBackupFrame([id, frame]));
        cursor = page.next;
      } while (cursor !== null);
    }
    await artifactsRestored.invoke("sql-finish", id);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "artifacts-restored" });
    const authorityRestored = await artifactsRestored.invoke("artifacts-read", credentials);
    decodeBackupFrame(authorityRestored, Authority);
    assert.deepEqual(
      decodeBackupFrame(authorityRestored, Schema.Unknown),
      decodeBackupFrame(authorityBefore, Schema.Unknown),
    );
    const revokedAfter = decodeBackupFrame(
      await artifactsRestored.invoke("artifacts-mutate", credentials),
      Schema.Struct({
        read: Schema.Literal("read"),
        write: Schema.Null,
        revoked: Schema.Null,
        expired: Schema.Null,
      }),
    );
    assert.equal(revokedAfter.write, null);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "artifacts-restored" });
    decodeBackupFrame(
      await artifactsRestored.invoke("artifacts-read", credentials),
      Schema.Struct({
        read: Schema.Literal("read"),
        write: Schema.Null,
        revoked: Schema.Null,
        expired: Schema.Null,
      }),
    );
    assert.deepEqual(
      decodeBackupFrame(await artifacts.invoke("artifacts-read", credentials), Schema.Unknown),
      decodeBackupFrame(authorityBefore, Schema.Unknown),
    );
    const artifactsId = await artifacts.invoke("object-id", "");
    const namespaceInventory = path.join(scratch, "namespace-inventory");
    const scope = {
      installationId: "fixture-namespace",
      snapshotId: id,
      account: "a".repeat(32),
      namespace: "b".repeat(32),
      deploymentSha256: resources.deploymentSha256,
    };
    await inventoryCloudflareObjects(
      scope.account,
      scope.namespace,
      namespaceInventory,
      "fixture-only",
      async () =>
        Response.json({
          success: true,
          result: [
            { id: archiveSourceId, hasStoredData: true },
            { id: artifactsId, hasStoredData: true },
          ],
          result_info: {},
        }),
    );
    const namespaceSources = new Map([
      [archiveSourceId, application],
      [artifactsId, artifacts],
    ]);
    const namespaceClient = {
      backup: async (request: BackupRequest) => {
        const source = namespaceSources.get(request.objectId);
        assert.ok(source);
        return source.invoke("admin", encodeBackupFrame(request));
      },
    };
    const failedHoldDirectory = path.join(scratch, "namespace-hold-failure");
    await assert.rejects(
      Effect.runPromise(
        captureNamespaceArchive({
          scope,
          inventoryDirectory: namespaceInventory,
          directory: failedHoldDirectory,
          sourceBucket: bindings.SOURCE,
          destinationBucket: bindings.HOLD_FAILURE,
          key: backupKey,
          client: {
            backup: (request: BackupRequest) =>
              request.objectId === artifactsId && request.command._tag === "Hold"
                ? Promise.reject(new Error("private hold failure"))
                : namespaceClient.backup(request),
          },
        }),
      ),
    );
    assert.equal(
      (await bindings.HOLD_FAILURE.list()).objects.length,
      0,
      "R2 copy must wait for every object hold",
    );
    await assert.rejects(fs.stat(path.join(failedHoldDirectory, "complete.packet")), {
      code: "ENOENT",
    });
    const failedPageDirectory = path.join(scratch, "namespace-page-failure");
    await assert.rejects(
      Effect.runPromise(
        captureNamespaceArchive({
          scope,
          inventoryDirectory: namespaceInventory,
          directory: failedPageDirectory,
          sourceBucket: bindings.SOURCE,
          destinationBucket: bindings.PAGE_FAILURE,
          key: backupKey,
          client: {
            backup: (request: BackupRequest) =>
              request.command._tag === "SqlPage"
                ? Promise.reject(new Error("private page failure"))
                : namespaceClient.backup(request),
          },
        }),
      ),
    );
    assert.equal((await bindings.PAGE_FAILURE.list()).objects.length, 7);
    await assert.rejects(fs.stat(path.join(failedPageDirectory, "complete.packet")), {
      code: "ENOENT",
    });
    for (const source of namespaceSources.values())
      assert.equal(await source.invoke("fence-request", ""), "503");
    const namespaceDirectory = path.join(scratch, "namespace-archive");
    const namespaceCompletion = await Effect.runPromise(
      captureNamespaceArchive({
        scope,
        inventoryDirectory: namespaceInventory,
        directory: namespaceDirectory,
        client: namespaceClient,
        sourceBucket: bindings.SOURCE,
        destinationBucket: bindings.ARCHIVE,
        key: backupKey,
      }),
    );
    assert.equal(namespaceCompletion.objects, 2);
    assert.equal(namespaceCompletion.resources.r2.objects, 7);
    const index = await fs.readFile(path.join(namespaceDirectory, "index.ndjson"), "utf8");
    assert.equal(
      createHash("sha256").update(index).digest("hex"),
      namespaceCompletion.objectsSha256,
    );
    assert.equal(index.trimEnd().split("\n").length, 2);
    const namespacePacket = await fs.readFile(
      path.join(namespaceDirectory, "complete.packet"),
      "utf8",
    );
    const namespacePayload = await Effect.runPromise(
      verifyBackupFrame(
        namespacePacket,
        {
          installationId: scope.installationId,
          snapshotId: id,
          objectId: "__namespace__",
          kind: "inventory",
          sequence: 2,
        },
        trustedSigner,
      ),
    );
    assert.deepEqual(decodeBackupFrame(namespacePayload, NamespaceCompletion), namespaceCompletion);
    const verifyNamespace = () =>
      Effect.runPromise(
        verifyNamespaceArchive({ directory: namespaceDirectory, scope, signer: trustedSigner }),
      );
    assert.deepEqual(await verifyNamespace(), namespaceCompletion);
    const expectedFile = path.join(scratch, "trusted-expected.json");
    const expected = { scope, signer: trustedSigner };
    await fs.writeFile(expectedFile, JSON.stringify(expected));
    const verifyCommand = (trustFile = expectedFile) =>
      promisify(execFile)(
        process.execPath,
        ["tools/verify-cloudflare-backup.ts", namespaceDirectory, trustFile],
        {
          timeout: 30_000,
          env: { ENABLE_CONFORMANCE: "1", ALLOW_ANONYMOUS_WRITES: "1", CLOUDFLARE_AI_TOKEN: "" },
        },
      );
    const report = JSON.parse((await verifyCommand()).stdout);
    assert.equal(report.kind, "namespace-archive-verification");
    assert.equal(report.signer, trustedSigner);
    assert.deepEqual(report.completion, namespaceCompletion);
    assert.deepEqual(report.notVerified, [
      "R2 bucket contents",
      "installation-wide isolation",
      "application recovery",
    ]);
    const rejectCommand = async (trustFile = expectedFile) => {
      await assert.rejects(verifyCommand(trustFile), (error: Error) => {
        assert.ok("stderr" in error && Predicate.isString(error.stderr));
        assert.match(error.stderr, /offline archive verification failed/);
        assert.equal(error.stderr.includes("private-trust-value"), false);
        assert.ok("stdout" in error);
        assert.equal(error.stdout, "");
        return true;
      });
    };
    for (const wrong of [
      { ...expected, signer: "SHA256:" + "A".repeat(43) },
      { ...expected, scope: { ...scope, snapshotId: "wrong-snapshot" } },
      { ...expected, scope: { ...scope, deploymentSha256: "0".repeat(64) } },
    ]) {
      await fs.writeFile(expectedFile, JSON.stringify(wrong));
      await rejectCommand();
    }
    await fs.writeFile(expectedFile, '{"private-trust-value":');
    await rejectCommand();
    await fs.writeFile(expectedFile, "private-trust-value".repeat(2000));
    await rejectCommand();
    await fs.writeFile(expectedFile, JSON.stringify(expected));
    const trustAlias = path.join(scratch, "trust-alias.json");
    await fs.symlink(expectedFile, trustAlias);
    await rejectCommand(trustAlias);
    const selfTrusted = path.join(namespaceDirectory, "self-trusted.json");
    await fs.writeFile(selfTrusted, JSON.stringify(expected));
    await rejectCommand(selfTrusted);
    await fs.unlink(selfTrusted);

    await assert.rejects(
      Effect.runPromise(
        verifyNamespaceArchive({
          directory: namespaceDirectory,
          scope: { ...scope, deploymentSha256: "0".repeat(64) },
          signer: trustedSigner,
        }),
      ),
    );
    const indexPath = path.join(namespaceDirectory, "index.ndjson");
    const namespaceCompletionPath = path.join(namespaceDirectory, "complete.packet");
    const indexLines = index.trimEnd().split("\n");
    const firstIndex = indexLines[0];
    const secondIndex = indexLines[1];
    assert.ok(firstIndex && secondIndex);
    for (const altered of [
      secondIndex + "\n" + firstIndex + "\n",
      firstIndex + "\n",
      firstIndex + "\n" + firstIndex + "\n",
      index.replace(/"sealSha256":"[a-f0-9]{64}"/, '"sealSha256":"' + "0".repeat(64) + '"'),
    ]) {
      await fs.writeFile(indexPath, altered);
      await assert.rejects(verifyNamespace());
    }
    const duplicatedIndex = firstIndex + "\n" + firstIndex + "\n";
    await fs.writeFile(indexPath, duplicatedIndex);
    const signedDuplicate = await Effect.runPromise(
      signBackupFrame(
        backupKey,
        {
          installationId: scope.installationId,
          snapshotId: id,
          objectId: "__namespace__",
          kind: "inventory",
          sequence: 2,
        },
        encodeBackupFrame({
          ...namespaceCompletion,
          objectsSha256: createHash("sha256").update(duplicatedIndex).digest("hex"),
        }),
      ),
    );
    await fs.writeFile(namespaceCompletionPath, signedDuplicate);
    await assert.rejects(
      verifyNamespace(),
      "a valid signature cannot substitute duplicate objects for the verified discovery list",
    );
    await fs.writeFile(indexPath, index);
    await fs.writeFile(namespaceCompletionPath, namespacePacket.slice(0, -1));
    await assert.rejects(verifyNamespace());
    await fs.writeFile(namespaceCompletionPath, namespacePacket);
    const savedNamespaceCompletion = path.join(scratch, "namespace-completion-saved");
    await fs.rename(namespaceCompletionPath, savedNamespaceCompletion);
    await fs.symlink(savedNamespaceCompletion, namespaceCompletionPath);
    await assert.rejects(verifyNamespace());
    await fs.unlink(namespaceCompletionPath);
    await fs.rename(savedNamespaceCompletion, namespaceCompletionPath);
    const objectPacketPath = path.join(
      namespaceDirectory,
      "objects",
      archiveSourceId,
      "000000000000.packet",
    );
    const savedObjectPacket = path.join(scratch, "namespace-object-packet-saved");
    await fs.rename(objectPacketPath, savedObjectPacket);
    await assert.rejects(verifyNamespace());
    await rejectCommand();
    await fs.rename(savedObjectPacket, objectPacketPath);
    assert.deepEqual(await verifyNamespace(), namespaceCompletion);
    const namespaceApplication = guarded(
      bindings.STORE.getByName("namespace-application-restored"),
    );
    const namespaceArtifacts = guarded(bindings.STORE.getByName("namespace-artifacts-restored"));
    const appTargetId = await namespaceApplication.invoke("object-id", "");
    const artifactsTargetId = await namespaceArtifacts.invoke("object-id", "");
    const targets = new Map([
      [appTargetId, namespaceApplication],
      [artifactsTargetId, namespaceArtifacts],
    ]);
    const mappingFile = path.join(scratch, "restore-mapping.ndjson");
    const mapping = [
      { sourceId: archiveSourceId, targetId: appTargetId },
      { sourceId: artifactsId, targetId: artifactsTargetId },
    ];
    const writeMapping = (rows: typeof mapping) =>
      fs.writeFile(mappingFile, rows.map((row) => JSON.stringify(row) + "\n").join(""));
    await writeMapping(mapping);
    let calls = 0;
    const client = {
      backup: async (request: BackupRequest) => {
        calls++;
        const target = targets.get(request.objectId);
        assert.ok(target);
        return target.invoke("admin", encodeBackupFrame(request));
      },
    };
    const options = {
      directory: namespaceDirectory,
      scope,
      signer: trustedSigner,
      mappingFile,
      journalDirectory: path.join(scratch, "restore-journal"),
      target: { account: "c".repeat(32), namespace: "d".repeat(32), bucket: "namespace-restored" },
      client,
      archiveBucket: bindings.ARCHIVE,
      targetBucket: bindings.ARCHIVE_RESTORED,
    };
    const restoreNamespace = () => Effect.runPromise(restoreNamespaceArchive(options));
    await writeMapping(mapping.map((row) => ({ ...row, targetId: appTargetId })));
    await assert.rejects(restoreNamespace());
    assert.equal(calls, 0, "duplicate targets rejected before any RPC");
    await writeMapping(mapping);
    let concurrentChecked = false;
    await assert.rejects(
      Effect.runPromise(
        restoreNamespaceArchive({
          ...options,
          client: {
            backup: async (request) => {
              if (!concurrentChecked) {
                concurrentChecked = true;
                await assert.rejects(restoreNamespace());
                assert.equal(
                  calls,
                  0,
                  "exclusive journal lock rejects concurrent restore before RPC",
                );
              }
              if (request.objectId === artifactsTargetId) throw new Error("lost hold");
              return client.backup(request);
            },
          },
        }),
      ),
    );
    assert.ok(concurrentChecked);
    assert.equal((await bindings.ARCHIVE_RESTORED.list()).objects.length, 0);
    assert.equal(await namespaceApplication.invoke("fence-request", ""), "503");
    const preflightCommands: string[] = [];
    await assert.rejects(
      Effect.runPromise(
        restoreNamespaceArchive({
          ...options,
          client: {
            backup: async (request) => {
              preflightCommands.push(request.command._tag);
              const frame = await client.backup(request);
              if (request.command._tag !== "Start") return frame;
              const receipt = decodeBackupFrame(frame, Checkpoint);
              return encodeBackupFrame({
                ...receipt,
                plan: { ...receipt.plan, objectId: "wrong-source" },
              });
            },
          },
        }),
      ),
    );
    assert.deepEqual(preflightCommands, ["Hold", "Hold", "Start"]);
    assert.equal(
      (await bindings.ARCHIVE_RESTORED.list()).objects.length,
      0,
      "a mismatched Start receipt must prevent all R2 writes",
    );
    let lostApply = false;
    await assert.rejects(
      Effect.runPromise(
        restoreNamespaceArchive({
          ...options,
          client: {
            backup: async (request) => {
              const result = await client.backup(request);
              if (
                !lostApply &&
                request.objectId === artifactsTargetId &&
                request.command._tag === "Apply"
              ) {
                lostApply = true;
                throw new Error("lost successful apply acknowledgement");
              }
              return result;
            },
          },
        }),
      ),
    );
    assert.ok(lostApply);
    const journalFile = path.join(options.journalDirectory, "restore.sqlite");
    const inspectJournal = () => {
      const db = new DatabaseSync(journalFile, { readOnly: true });
      try {
        return {
          phase: db.prepare("SELECT phase FROM meta").get()?.phase,
          receipts: db.prepare("SELECT receipt FROM objects ORDER BY sequence").all(),
        };
      } finally {
        db.close();
      }
    };
    const interrupted = inspectJournal();
    assert.equal(interrupted.phase, "restoring");
    assert.equal(interrupted.receipts.filter((row) => row.receipt !== null).length, 1);
    const beforePrematureRelease = calls;
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...options,
          verificationSha256: "e".repeat(64),
        }),
      ),
    );
    assert.equal(calls, beforePrematureRelease);
    const callsBeforeInvalid = calls;
    await writeMapping([
      { sourceId: archiveSourceId, targetId: artifactsTargetId },
      { sourceId: artifactsId, targetId: appTargetId },
    ]);
    await assert.rejects(restoreNamespace());
    await writeMapping(mapping);
    await assert.rejects(
      Effect.runPromise(
        restoreNamespaceArchive({
          ...options,
          target: { ...options.target, bucket: "changed-target" },
        }),
      ),
    );
    assert.equal(calls, callsBeforeInvalid);
    assert.deepEqual(inspectJournal(), interrupted);
    for (const name of ["namespace-application-restored", "namespace-artifacts-restored"])
      await mf.unsafeEvictDurableObject("backup-codec", "Store", { name });
    const versions = (await bindings.ARCHIVE_RESTORED.list()).objects.map(({ key, version }) => [
      key,
      version,
    ]);
    assert.equal((await restoreNamespace()).phase, "ready");
    assert.equal((await restoreNamespace()).phase, "ready");
    assert.deepEqual(
      (await bindings.ARCHIVE_RESTORED.list()).objects.map(({ key, version }) => [key, version]),
      versions,
    );
    assert.equal(inspectJournal().phase, "ready");
    assert.equal(inspectJournal().receipts.filter((row) => row.receipt !== null).length, 2);
    assert.equal((await fs.stat(options.journalDirectory)).mode & 0o777, 0o700);
    assert.equal((await fs.stat(journalFile)).mode & 0o777, 0o600);
    await mf.unsafeEvictDurableObject("backup-codec", "Store", {
      name: "namespace-application-restored",
    });
    await mf.unsafeEvictDurableObject("backup-codec", "Store", {
      name: "namespace-artifacts-restored",
    });
    assert.deepEqual(
      decodeBackupFrame(
        await namespaceApplication.invoke("application-archive-restored", ""),
        Schema.Unknown,
      ),
      before,
    );
    assert.deepEqual(
      decodeBackupFrame(
        await namespaceArtifacts.invoke("artifacts-read", credentials),
        Schema.Unknown,
      ),
      decodeBackupFrame(authorityBefore, Schema.Unknown),
    );
    for (const target of [namespaceApplication, namespaceArtifacts])
      assert.equal(
        await target.invoke("fence-request", ""),
        "503",
        "all restored objects stay fenced through recovery verification",
      );
    const releaseOptions = {
      ...options,
      verificationSha256: createHash("sha256")
        .update(encodeBackupFrame([before, authorityBefore]))
        .digest("hex"),
    };
    const releaseNamespace = () => Effect.runPromise(releaseNamespaceArchive(releaseOptions));
    const beforeRelease = calls;
    await writeMapping(mapping.map((row) => ({ ...row, targetId: appTargetId })));
    await assert.rejects(releaseNamespace());
    await writeMapping(mapping);
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...releaseOptions,
          target: { ...options.target, bucket: "wrong-release-target" },
        }),
      ),
    );
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...releaseOptions,
          verificationSha256: "",
        }),
      ),
    );
    await bindings.ARCHIVE_RESTORED.put("unexpected-before-release", "must block release");
    await assert.rejects(releaseNamespace());
    await bindings.ARCHIVE_RESTORED.delete("unexpected-before-release");
    assert.equal(calls, beforeRelease, "invalid release inputs and mismatched R2 fail before RPC");
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...releaseOptions,
          client: {
            backup: async (request) => {
              assert.equal(
                request.command._tag,
                "Status",
                "all receipts must verify before retirement",
              );
              if (request.objectId === artifactsTargetId) return initial;
              return client.backup(request);
            },
          },
        }),
      ),
    );
    assert.equal(inspectJournal().phase, "ready");
    let lostRetire = false;
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...releaseOptions,
          client: {
            backup: async (request) => {
              assert.notEqual(
                request.command._tag,
                "Release",
                "failed retirement must leave every fence held",
              );
              const response = await client.backup(request);
              if (request.command._tag === "Retire") {
                lostRetire = true;
                throw new Error("lost successful retirement acknowledgement");
              }
              return response;
            },
          },
        }),
      ),
    );
    assert.ok(lostRetire);
    assert.equal(inspectJournal().phase, "releasing");
    for (const target of targets.values())
      assert.equal(await target.invoke("fence-request", ""), "503");
    const beforeRejectedRetry = calls;
    for (const phases of [
      ["released", "pending"],
      ["pending", "retired"],
      ["retired", "released"],
    ] as const) {
      const database = new DatabaseSync(journalFile);
      try {
        database.prepare("UPDATE release_objects SET phase = ? WHERE sequence = 0").run(phases[0]);
        database.prepare("UPDATE release_objects SET phase = ? WHERE sequence = 1").run(phases[1]);
      } finally {
        database.close();
      }
      await assert.rejects(releaseNamespace());
      assert.equal(calls, beforeRejectedRetry, "impossible release progress fails before RPC");
    }
    const resetProgress = new DatabaseSync(journalFile);
    try {
      resetProgress.exec("UPDATE release_objects SET phase = 'pending'");
    } finally {
      resetProgress.close();
    }
    await assert.rejects(restoreNamespace());
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...releaseOptions,
          verificationSha256: "f".repeat(64),
        }),
      ),
    );
    assert.equal(
      calls,
      beforeRejectedRetry,
      "release intent forbids restart as import or changed verification",
    );
    await mf.unsafeEvictDurableObject("backup-codec", "Store", {
      name: "namespace-application-restored",
    });
    let retiredOnRetry = 0;
    let lostRelease = false;
    await assert.rejects(
      Effect.runPromise(
        releaseNamespaceArchive({
          ...releaseOptions,
          client: {
            backup: async (request) => {
              if (request.command._tag === "Retire") retiredOnRetry++;
              if (request.command._tag === "Release")
                assert.equal(retiredOnRetry, 2, "all retirements precede first release");
              const response = await client.backup(request);
              if (request.command._tag === "Release") {
                lostRelease = true;
                throw new Error("lost successful release acknowledgement");
              }
              return response;
            },
          },
        }),
      ),
    );
    assert.ok(lostRelease);
    assert.equal(await namespaceApplication.invoke("fence-request", ""), "204");
    assert.equal(await namespaceArtifacts.invoke("fence-request", ""), "503");
    for (const name of ["namespace-application-restored", "namespace-artifacts-restored"])
      await mf.unsafeEvictDurableObject("backup-codec", "Store", { name });
    assert.equal(
      (
        await Effect.runPromise(
          releaseNamespaceArchive({
            ...releaseOptions,
            client: {
              backup: async (request) => {
                assert.equal(
                  request.command._tag,
                  "Release",
                  "durable retirement progress prevents replay after reopening",
                );
                return client.backup(request);
              },
            },
          }),
        )
      ).phase,
      "released",
    );
    assert.equal(inspectJournal().phase, "released");
    for (const target of targets.values())
      assert.equal(await target.invoke("fence-request", ""), "204");
    const terminalCalls = calls;
    assert.equal((await releaseNamespace()).phase, "released");
    assert.equal(calls, terminalCalls, "terminal retry never contacts live targets");

    for (const source of namespaceSources.values())
      assert.equal(await source.invoke("fence-request", ""), "503");
    const fenceRaw = bindings.STORE.getByName("backup-fence");
    const fence = guarded(fenceRaw);
    assert.equal(await fence.invoke("fence-request", ""), "204");
    await fence.invoke("fence-enter", "");
    await fence.invoke("fence-start", id);
    const Fence = Schema.Struct({ phase: Schema.String, snapshotId: Schema.String });
    assert.equal(
      decodeBackupFrame(await fence.invoke("fence-status", ""), Fence).phase,
      "draining",
    );
    assert.equal(await fence.invoke("fence-request", ""), "503");
    // Admitted requests may still register their detached work while draining.
    await fence.invoke("fence-followup", "");
    await fence.invoke("fence-done", "");
    assert.equal(
      decodeBackupFrame(await fence.invoke("fence-status", ""), Fence).phase,
      "draining",
    );
    await fence.invoke("fence-done", "");
    assert.equal(await fence.invoke("fence-wait", ""), "held");
    await assert.rejects(fence.invoke("fence-release", "wrong-snapshot"));
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "backup-fence" });
    assert.equal(await fence.invoke("fence-request", ""), "503");
    await fence.invoke("fence-start", "wrong-snapshot");
    assert.equal(await fence.invoke("fence-wait", ""), "rejected");
    await fence.invoke("fence-release", id);
    assert.equal(await fence.invoke("fence-request", ""), "204");
    const streaming = await fenceRaw.fetch("https://fixture/fence-stream");
    assert.ok(streaming.body);
    await fence.invoke("fence-start", id);
    assert.equal(
      decodeBackupFrame(await fence.invoke("fence-status", ""), Fence).phase,
      "draining",
    );
    await streaming.body.cancel();
    assert.equal(await fence.invoke("fence-wait", ""), "held");
    await fence.invoke("fence-release", id);
    await fence.invoke("fence-sync-fail", "on");
    await fence.invoke("fence-start", id);
    assert.equal(await fence.invoke("fence-wait", ""), "rejected");
    assert.equal(await fence.invoke("fence-request", ""), "503");
    await fence.invoke("fence-sync-fail", "off");
    await fence.invoke("fence-start", id);
    assert.equal(await fence.invoke("fence-wait", ""), "held");
    await fence.invoke("fence-sync-fail", "on");
    await assert.rejects(fence.invoke("fence-release", id), (error: Error) => {
      assert.equal(error.message.includes("private flush"), false);
      return true;
    });
    assert.equal(await fence.invoke("fence-request", ""), "503");
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "backup-fence" });
    assert.equal(await fence.invoke("fence-request", ""), "503");
    await fence.invoke("fence-release", id);
    await fence.invoke("fence-corrupt", "");
    assert.equal(await fence.invoke("fence-request", ""), "503");
  } finally {
    await mf?.dispose();
    await fs.rm(scratch, { recursive: true, force: true });
  }
});
