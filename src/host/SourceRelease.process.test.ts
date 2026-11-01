import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { startBackupRuntime } from "../testing/BackupRuntime.ts";
import * as Ssh from "../crypto/SshSignature.ts";
import type { Request } from "./CloudflareBackupAdmin.ts";
import { openBackupProcess } from "../testing/BackupProcess.ts";
import { encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";
import { captureNamespaceArchive } from "../../tools/cloudflare-namespace-archive.ts";
import { openRestoreJournal } from "../../tools/cloudflare-restore-journal.ts";

for (const mode of ["release", "abandon"] as const)
  for (const boundary of ["before-release", "after-release", "after-record"] as const) {
    it(`resumes the source ${mode} coordinator after SIGKILL ${boundary}`, async () => {
      const scratch = await fs.mkdtemp(path.join(tmpdir(), "source-coordinator-crash-"));
      const runtime = await startBackupRuntime(scratch);
      let process: Awaited<ReturnType<typeof openBackupProcess>> | undefined;
      try {
        const { bindings, guarded, mf } = runtime;
        const first = guarded(bindings.STORE.getByName("first-source"));
        const second = guarded(bindings.STORE.getByName("second-source"));
        const firstId = await first.invoke("object-id", "");
        const secondId = await second.invoke("object-id", "");
        const sources = new Map([
          [firstId, first],
          [secondId, second],
        ]);
        const client = {
          backup: (request: Request) => {
            const source = sources.get(request.objectId);
            assert.ok(source);
            return source.invoke("admin", encodeBackupFrame(request));
          },
        };
        const scope = {
          installationId: "source-crash-fixture",
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
              result: [...sources.keys()].map((id) => ({ id, hasStoredData: false })),
              result_info: {},
            }),
        );
        const key = await Effect.runPromise(Ssh.generate("source-process-fixture"));
        const signer = await Effect.runPromise(Ssh.fingerprint(key.publicKey));
        const directory = path.join(scratch, "archive");
        const completion = await Effect.runPromise(
          captureNamespaceArchive({
            directory,
            inventoryDirectory,
            scope,
            key,
            client,
            sourceBucket: bindings.SOURCE,
            destinationBucket: bindings.ARCHIVE,
          }),
        );
        assert.equal(completion.resources.r2.objects, 0);
        let interrupt = true;
        let calls = 0;
        let bucketReads = 0;
        process = await openBackupProcess({
          client,
          archiveBucket: bindings.ARCHIVE,
          onBucket: () => {
            bucketReads++;
          },
          interrupt: (request, point) => {
            assert.equal(request.command._tag, mode === "release" ? "Release" : "AbandonCapture");
            if (point === "before") calls++;
            return (
              interrupt &&
              ((boundary === "before-release" && point === "before") ||
                (boundary === "after-release" && point === "after") ||
                (boundary === "after-record" &&
                  point === "before" &&
                  request.objectId === secondId))
            );
          },
        });
        const journalDirectory = path.join(scratch, "journal");
        const child = process;
        const run = () =>
          mode === "release"
            ? child.run("source-release", { directory, scope, signer, journalDirectory })
            : child.run("source-abandon", {
                directory,
                scope,
                signer,
                journalDirectory,
                decisionSha256: "d".repeat(64),
              });
        assert.deepEqual(await run(), { code: null, signal: "SIGKILL" });
        const journal = await openRestoreJournal(journalDirectory);
        try {
          assert.equal(journal.prepare("PRAGMA integrity_check").get()?.integrity_check, "ok");
          assert.equal(journal.prepare("SELECT phase FROM meta").get()?.phase, "source-releasing");
          const rows = journal.prepare("SELECT phase FROM release_objects ORDER BY sequence").all();
          assert.deepEqual(
            rows.map((row) => row.phase),
            boundary === "after-record" ? ["released", "pending"] : ["pending", "pending"],
          );
        } finally {
          journal.close();
        }
        assert.equal(
          await first.invoke("fence-request", ""),
          boundary === "before-release" ? "503" : "204",
        );
        assert.equal(await second.invoke("fence-request", ""), "503");
        for (const name of ["first-source", "second-source"])
          await mf.unsafeEvictDurableObject("backup-codec", "Store", { name });
        interrupt = false;
        assert.deepEqual(await run(), { code: 0, signal: null });
        assert.equal(
          calls,
          3,
          "retry skips committed progress and repeats only unacknowledged release",
        );
        assert.equal(
          bucketReads,
          mode === "release" ? 2 : 0,
          "only completed-capture release verifies R2",
        );
        for (const source of sources.values())
          assert.equal(await source.invoke("fence-request", ""), "204");
        await client.backup({
          objectId: firstId,
          snapshotId: "later-snapshot",
          command: { _tag: "Hold" },
        });
        assert.deepEqual(await run(), { code: 0, signal: null });
        assert.equal(calls, 3);
        assert.equal(
          bucketReads,
          mode === "release" ? 2 : 0,
          "terminal retry does not contact R2 or source objects",
        );
        assert.equal(await first.invoke("fence-request", ""), "503");
        await assert.rejects(
          mode === "release"
            ? child.run("source-abandon", {
                directory,
                scope,
                signer,
                journalDirectory,
                decisionSha256: "d".repeat(64),
              })
            : child.run("source-release", { directory, scope, signer, journalDirectory }),
        );
        assert.equal(calls, 3, "release and abandonment cannot reuse each other's journal");
        assert.equal(bucketReads, mode === "release" ? 2 : 0);
      } finally {
        await process?.close();
        await runtime.mf.dispose();
        await fs.rm(scratch, { recursive: true, force: true });
      }
    });
  }
