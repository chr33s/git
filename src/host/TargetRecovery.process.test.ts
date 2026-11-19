import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { startBackupRuntime } from "../testing/BackupRuntime.ts";
import { openBackupProcess } from "../testing/BackupProcess.ts";
import * as Ssh from "../crypto/SshSignature.ts";
import type { Request } from "./CloudflareBackupAdmin.ts";
import { decodeBackupFrame, encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";
import { captureNamespaceArchive } from "../../tools/cloudflare-namespace-archive.ts";
import { openRestoreJournal } from "../../tools/cloudflare-restore-journal.ts";
import { checkpointKey, fenceKey } from "../../tools/cloudflare-backup-control.ts";

const Snapshot = Schema.Struct({
  rows: Schema.Array(Schema.Unknown),
  kv: Schema.Array(Schema.Tuple([Schema.String, Schema.Unknown])),
});
const data = (frame: string) => {
  const snapshot = decodeBackupFrame(frame, Snapshot);
  return {
    ...snapshot,
    kv: snapshot.kv.filter(([key]) => key !== fenceKey && key !== checkpointKey),
  };
};

for (const boundary of [
  "restore-during-r2-put",
  "restore-after-r2-put",
  "restore-after-apply",
  "restore-after-finish",
  "restore-after-receipt",
  "release-after-retire",
  "release-after-retirement-record",
  "release-after-release",
  "release-after-release-record",
] as const) {
  it(`resumes the target coordinator after SIGKILL ${boundary}`, async () => {
    const scratch = await fs.mkdtemp(path.join(tmpdir(), "target-coordinator-crash-"));
    const runtime = await startBackupRuntime(scratch);
    let process: Awaited<ReturnType<typeof openBackupProcess>> | undefined;
    try {
      const { bindings, guarded, mf } = runtime;
      const sourceNames = ["source-one", "source-two"];
      const targetNames = ["target-one", "target-two"];
      const sources = new Map<string, ReturnType<typeof guarded>>();
      const targets = new Map<string, ReturnType<typeof guarded>>();
      const expected: ReturnType<typeof data>[] = [];
      const mapping: { sourceId: string; targetId: string }[] = [];
      for (let index = 0; index < 2; index++) {
        const sourceName = sourceNames[index];
        const targetName = targetNames[index];
        assert.ok(sourceName && targetName);
        const raw = bindings.STORE.getByName(sourceName);
        await raw.seed();
        const source = guarded(raw);
        const target = guarded(bindings.STORE.getByName(targetName));
        const sourceId = await source.invoke("object-id", "");
        const targetId = await target.invoke("object-id", "");
        sources.set(sourceId, source);
        targets.set(targetId, target);
        mapping.push({ sourceId, targetId });
        expected.push(data(await source.invoke("snapshot", "")));
      }
      const dispatch = (objects: typeof sources) => ({
        backup: (request: Request) => {
          const object = objects.get(request.objectId);
          assert.ok(object);
          return object.invoke("admin", encodeBackupFrame(request));
        },
      });
      const payload = new Uint8Array(1024 * 1024).fill(137);
      await bindings.SOURCE.put("payload", payload, {
        httpMetadata: { contentType: "application/octet-stream", cacheControl: "private" },
        customMetadata: { fixture: "process-death" },
      });
      const scope = {
        installationId: "target-crash-fixture",
        snapshotId: boundary,
        account: "a".repeat(32),
        namespace: "b".repeat(32),
        deploymentSha256: "c".repeat(64),
      };
      const inventoryDirectory = path.join(scratch, "inventory");
      await inventoryCloudflareObjects(
        scope.account,
        scope.namespace,
        inventoryDirectory,
        "fixture-only",
        async () =>
          Response.json({
            success: true,
            result: [...sources.keys()].map((id) => ({ id, hasStoredData: true })),
            result_info: {},
          }),
      );
      const key = await Effect.runPromise(Ssh.generate("target-process-fixture"));
      const signer = await Effect.runPromise(Ssh.fingerprint(key.publicKey));
      const directory = path.join(scratch, "archive");
      await Effect.runPromise(
        captureNamespaceArchive({
          directory,
          inventoryDirectory,
          scope,
          key,
          client: dispatch(sources),
          sourceBucket: bindings.SOURCE,
          destinationBucket: bindings.ARCHIVE,
        }),
      );
      const mappingFile = path.join(scratch, "mapping.ndjson");
      await fs.writeFile(mappingFile, mapping.map((row) => JSON.stringify(row) + "\n").join(""));
      const first = mapping[0];
      const second = mapping[1];
      assert.ok(first && second);
      let interrupt = false;
      let firstFinished = false;
      const calls: string[] = [];
      let bucketReads = 0;
      process = await openBackupProcess({
        client: dispatch(targets),
        archiveBucket: bindings.ARCHIVE,
        targetBucket: bindings.ARCHIVE_RESTORED,
        putTarget: (key, body, length, options) =>
          bindings.STORE.getByName("stream-bridge").putRestored(key, body, length, options ?? {}),
        onBucket: () => {
          bucketReads++;
        },
        interruptBucket: (key, point) =>
          interrupt &&
          key === "payload" &&
          ((boundary === "restore-during-r2-put" && point === "during-put") ||
            (boundary === "restore-after-r2-put" && point === "after-put")),
        interrupt: (request, point) => {
          const tag = request.command._tag;
          if (point === "before") calls.push(tag);
          if (point === "after" && tag === "Finish" && request.objectId === first.targetId)
            firstFinished = true;
          if (!interrupt) return false;
          switch (boundary) {
            case "restore-during-r2-put":
            case "restore-after-r2-put":
              return false;
            case "restore-after-apply":
              return point === "after" && tag === "Apply";
            case "restore-after-finish":
              return point === "after" && tag === "Finish";
            case "restore-after-receipt":
              return (
                firstFinished &&
                point === "before" &&
                tag === "Hold" &&
                request.objectId === second.targetId
              );
            case "release-after-retire":
              return point === "after" && tag === "Retire";
            case "release-after-retirement-record":
              return point === "before" && tag === "Retire" && request.objectId === second.targetId;
            case "release-after-release":
              return point === "after" && tag === "Release";
            case "release-after-release-record":
              return (
                point === "before" && tag === "Release" && request.objectId === second.targetId
              );
          }
        },
      });
      const child = process;
      const journalDirectory = path.join(scratch, "journal");
      const options = {
        directory,
        scope,
        signer,
        mappingFile,
        journalDirectory,
        target: { account: "d".repeat(32), namespace: "e".repeat(32), bucket: "recovered" },
      };
      const checkData = async () => {
        const recovered = await bindings.ARCHIVE_RESTORED.get("payload");
        assert.ok(recovered);
        assert.deepEqual(new Uint8Array(await recovered.arrayBuffer()), payload);
        assert.deepEqual(recovered.customMetadata, { fixture: "process-death" });
        assert.deepEqual(recovered.httpMetadata, {
          contentType: "application/octet-stream",
          cacheControl: "private",
        });
        let index = 0;
        for (const target of targets.values()) {
          assert.equal(await target.invoke("fence-request", ""), "503");
          assert.deepEqual(data(await target.invoke("snapshot", "")), expected[index++]);
        }
      };
      const reportFile = path.join(scratch, "verification.json");
      const release = async () =>
        child.run("target-release", {
          ...options,
          verificationSha256: createHash("sha256")
            .update(await fs.readFile(reportFile))
            .digest("hex"),
        });
      const restoring = boundary.startsWith("restore-");
      if (!restoring) {
        assert.deepEqual(await child.run("restore", options), { code: 0, signal: null });
        await checkData();
        await fs.writeFile(reportFile, JSON.stringify({ typedKvAndSqlMatched: true, objects: 2 }));
      }
      interrupt = true;
      assert.deepEqual(await (restoring ? child.run("restore", options) : release()), {
        code: null,
        signal: "SIGKILL",
      });
      const priorObject = await bindings.ARCHIVE_RESTORED.head("payload");
      if (boundary === "restore-during-r2-put") assert.equal(priorObject, null);
      if (boundary === "restore-after-r2-put") assert.ok(priorObject);
      const journal = await openRestoreJournal(journalDirectory);
      try {
        assert.equal(journal.prepare("PRAGMA integrity_check").get()?.integrity_check, "ok");
        assert.equal(
          journal.prepare("SELECT phase FROM meta").get()?.phase,
          restoring ? "restoring" : "releasing",
        );
        const receipts = journal.prepare("SELECT receipt FROM objects ORDER BY sequence").all();
        assert.equal(
          receipts.filter((row) => row.receipt !== null).length,
          restoring ? (boundary === "restore-after-receipt" ? 1 : 0) : 2,
        );
        if (!restoring) {
          const phases = journal
            .prepare("SELECT phase FROM release_objects ORDER BY sequence")
            .all()
            .map((row) => row.phase);
          assert.deepEqual(
            phases,
            boundary === "release-after-retire"
              ? ["pending", "pending"]
              : boundary === "release-after-retirement-record"
                ? ["retired", "pending"]
                : boundary === "release-after-release"
                  ? ["retired", "retired"]
                  : ["released", "retired"],
          );
        }
      } finally {
        journal.close();
      }
      for (const [id, target] of targets)
        assert.equal(
          await target.invoke("fence-request", ""),
          id === first.targetId &&
            (boundary === "release-after-release" || boundary === "release-after-release-record")
            ? "204"
            : "503",
        );
      for (const name of targetNames)
        await mf.unsafeEvictDurableObject("backup-codec", "Store", { name });
      interrupt = false;
      calls.length = 0;
      if (restoring) {
        assert.deepEqual(await child.run("restore", options), { code: 0, signal: null });
        await checkData();
        if (priorObject !== null)
          assert.equal(
            (await bindings.ARCHIVE_RESTORED.head("payload"))?.version,
            priorObject.version,
            "retry verifies an acknowledged or unacknowledged existing key without replacing it",
          );
        await fs.writeFile(reportFile, JSON.stringify({ typedKvAndSqlMatched: true, objects: 2 }));
        calls.length = 0;
      }
      assert.deepEqual(await release(), { code: 0, signal: null });
      assert.ok(calls.every((tag) => tag === "Status" || tag === "Retire" || tag === "Release"));
      if (boundary === "release-after-release-record") assert.deepEqual(calls, ["Release"]);
      for (const target of targets.values())
        assert.equal(await target.invoke("fence-request", ""), "204");
      for (const source of sources.values())
        assert.equal(await source.invoke("fence-request", ""), "503");
      await dispatch(targets).backup({
        objectId: first.targetId,
        snapshotId: "later-snapshot",
        command: { _tag: "Hold" },
      });
      calls.length = 0;
      const reads = bucketReads;
      assert.deepEqual(await release(), { code: 0, signal: null });
      assert.deepEqual(calls, []);
      assert.equal(bucketReads, reads);
      const heldAgain = targets.get(first.targetId);
      assert.ok(heldAgain);
      assert.equal(await heldAgain.invoke("fence-request", ""), "503");
    } finally {
      await process?.close();
      await runtime.mf.dispose();
      await fs.rm(scratch, { recursive: true, force: true });
    }
  });
}
