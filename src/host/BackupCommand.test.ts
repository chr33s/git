import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { tmpdir } from "node:os";
import { execFileSync, spawnSync } from "node:child_process";
import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as Ssh from "../crypto/SshSignature.ts";
import { startBackupRuntime } from "../testing/BackupRuntime.ts";
import { runBackupCommand } from "../../tools/backup-cloudflare.ts";
import type { Connection } from "../../tools/cloudflare-backup-connection.ts";
import type { Request } from "./CloudflareBackupAdmin.ts";
import { decodeBackupFrame, encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";

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

it("runs separate operator commands with one scoped connection and offline terminal retries", async () => {
  const scratch = await fs.mkdtemp(path.join(tmpdir(), "backup-command-"));
  const runtime = await startBackupRuntime(scratch);
  try {
    const { bindings, guarded } = runtime;
    const raw = bindings.STORE.getByName("source");
    await raw.seed();
    const source = guarded(raw);
    const target = guarded(bindings.STORE.getByName("target"));
    const sourceId = await source.invoke("object-id", "");
    const targetId = await target.invoke("object-id", "");
    await bindings.SOURCE.put("payload", new Uint8Array([0, 255, 42]));
    const scope = {
      installationId: "command-test",
      snapshotId: "snapshot",
      account: "a".repeat(32),
      namespace: "b".repeat(32),
      deploymentSha256: "c".repeat(64),
    };
    const connection = {
      account: scope.account,
      namespace: scope.namespace,
      worker: "source",
      bucket: "source",
      archiveBucket: "archive",
    };
    const recoveryConnection = {
      ...connection,
      namespace: "d".repeat(32),
      worker: "target",
      bucket: "target",
    };
    await inventoryCloudflareObjects(
      scope.account,
      scope.namespace,
      path.join(scratch, "inventory"),
      "fixture",
      async () =>
        Response.json({
          success: true,
          result: [{ id: sourceId, hasStoredData: true }],
          result_info: {},
        }),
    );
    const keyFile = path.join(scratch, "key");
    execFileSync("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-f", keyFile]);
    const key = await Effect.runPromise(
      Effect.fromResult(Ssh.parsePrivateKey(await fs.readFile(keyFile, "utf8"))),
    );
    const signer = await Effect.runPromise(Ssh.fingerprint(key.publicKey));
    const common = { schemaVersion: 1, scope, signer, directory: "archive" };
    const planFile = path.join(scratch, "plan.json");
    let opened = 0;
    let closed = 0;
    const connect = (config: Connection) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          opened++;
          const original = config.namespace === scope.namespace;
          return {
            client: {
              backup: (request: Request) => {
                assert.equal(request.objectId, original ? sourceId : targetId);
                return (original ? source : target).invoke("admin", encodeBackupFrame(request));
              },
            },
            bucket: original ? bindings.SOURCE : bindings.RESTORED,
            archiveBucket: bindings.ARCHIVE,
          };
        }),
        () =>
          Effect.sync(() => {
            closed++;
          }),
      );
    const capture = {
      ...common,
      connection,
      command: "capture",
      inventoryDirectory: "inventory",
      keyFile: "key",
    };
    await fs.writeFile(planFile, JSON.stringify(capture), { mode: 0o600 });
    await Effect.runPromise(runBackupCommand("capture", planFile, connect));
    assert.equal(opened, 1);
    assert.equal(closed, opened);
    assert.equal(await source.invoke("fence-request", ""), "503");
    await fs.writeFile(
      path.join(scratch, "mapping.ndjson"),
      JSON.stringify({ sourceId, targetId }) + "\n",
      { mode: 0o600 },
    );
    const restore = {
      ...common,
      connection: recoveryConnection,
      mappingFile: "mapping.ndjson",
      journalDirectory: "target-journal",
      command: "restore",
    };
    await fs.writeFile(planFile, JSON.stringify(restore));
    await Effect.runPromise(runBackupCommand("restore", planFile, connect));
    assert.equal(opened, 2);
    assert.equal(closed, opened);
    assert.equal(await target.invoke("fence-request", ""), "503");
    assert.deepEqual(
      data(await target.invoke("snapshot", "")),
      data(await source.invoke("snapshot", "")),
    );
    const payload = await bindings.RESTORED.get("payload");
    assert.ok(payload);
    assert.deepEqual(new Uint8Array(await payload.arrayBuffer()), new Uint8Array([0, 255, 42]));
    const release = {
      ...restore,
      command: "release-targets",
      verificationFile: "verification.json",
    };
    await fs.writeFile(
      path.join(scratch, "verification.json"),
      JSON.stringify({ valuesMatch: true, r2Match: true }),
      { mode: 0o600 },
    );
    await fs.writeFile(planFile, JSON.stringify(release));
    await Effect.runPromise(runBackupCommand("release-targets", planFile, connect));
    assert.equal(opened, 3);
    assert.equal(closed, opened);
    assert.equal(await target.invoke("fence-request", ""), "204");
    assert.equal(await source.invoke("fence-request", ""), "503");
    await target.invoke(
      "admin",
      encodeBackupFrame({ objectId: targetId, snapshotId: "later", command: { _tag: "Hold" } }),
    );
    await Effect.runPromise(runBackupCommand("release-targets", planFile, connect));
    assert.equal(opened, 3);
    assert.equal(await target.invoke("fence-request", ""), "503");
    await fs.writeFile(
      planFile,
      JSON.stringify({
        ...common,
        connection,
        command: "release-sources",
        journalDirectory: "source-journal",
      }),
    );
    await Effect.runPromise(runBackupCommand("release-sources", planFile, connect));
    assert.equal(opened, 4);
    assert.equal(closed, opened);
    assert.equal(await source.invoke("fence-request", ""), "204");
    await source.invoke(
      "admin",
      encodeBackupFrame({ objectId: sourceId, snapshotId: "later", command: { _tag: "Hold" } }),
    );
    await Effect.runPromise(runBackupCommand("release-sources", planFile, connect));
    assert.equal(opened, 4);
    assert.equal(await source.invoke("fence-request", ""), "503");

    // Local validation must not acquire a remote proxy, even with valid archive bytes.
    for (const plan of [
      { ...capture, connection: recoveryConnection },
      { ...capture, signer: "SHA256:" + "A".repeat(43) },
      { ...restore, journalDirectory: "archive/journal" },
      { ...restore, mappingFile: "archive/mapping.ndjson" },
      { ...release, verificationFile: "archive/report" },
      { ...release, verificationFile: "missing-report" },
    ]) {
      await fs.writeFile(planFile, JSON.stringify(plan));
      await assert.rejects(Effect.runPromise(runBackupCommand(plan.command, planFile, connect)));
      assert.equal(opened, 4);
    }
    await fs.writeFile(planFile, JSON.stringify(capture));
    await fs.chmod(planFile, 0o644);
    await assert.rejects(Effect.runPromise(runBackupCommand("capture", planFile, connect)));
    assert.equal(opened, 4);
    await fs.chmod(planFile, 0o600);
    await assert.rejects(Effect.runPromise(runBackupCommand("restore", planFile, connect)));
    for (const contents of ["{", "x".repeat(64 * 1024 + 1)]) {
      await fs.writeFile(planFile, contents);
      await assert.rejects(Effect.runPromise(runBackupCommand("capture", planFile, connect)));
    }
    await fs.writeFile(planFile, JSON.stringify(capture));
    const link = path.join(scratch, "linked-plan.json");
    await fs.symlink(planFile, link);
    await assert.rejects(Effect.runPromise(runBackupCommand("capture", link, connect)));
    await fs.writeFile(path.join(scratch, "verification.json"), " ");
    await fs.writeFile(planFile, JSON.stringify(release));
    await assert.rejects(Effect.runPromise(runBackupCommand("release-targets", planFile, connect)));
    assert.equal(opened, 4);

    // A remote failure after acquisition must dispose the connection too.
    await fs.writeFile(planFile, JSON.stringify({ ...capture, directory: "failed-archive" }));
    const failing = (config: Connection) =>
      connect(config).pipe(
        Effect.map((held) => ({
          ...held,
          client: {
            backup: async () => {
              throw new Error("injected transport failure");
            },
          },
        })),
      );
    await assert.rejects(Effect.runPromise(runBackupCommand("capture", planFile, failing)));
    assert.equal(opened, 5);
    assert.equal(closed, opened);
    const laterScope = { ...scope, snapshotId: "later" };
    await fs.writeFile(
      planFile,
      JSON.stringify({ ...capture, scope: laterScope, directory: "failed-later" }),
    );
    await assert.rejects(Effect.runPromise(runBackupCommand("capture", planFile, failing)));
    assert.equal(opened, 6);
    const abandonment = {
      ...common,
      connection,
      scope: laterScope,
      directory: "failed-later",
      command: "abandon-sources",
      journalDirectory: "abandon-journal",
      decisionFile: "decision.txt",
    };
    await fs.writeFile(planFile, JSON.stringify(abandonment));
    await fs.writeFile(path.join(scratch, "decision.txt"), " ", { mode: 0o600 });
    await assert.rejects(Effect.runPromise(runBackupCommand("abandon-sources", planFile, connect)));
    assert.equal(opened, 6);
    await fs.writeFile(
      path.join(scratch, "decision.txt"),
      "Abandon this failed capture; retain partial data for inspection.",
    );
    const abandoned = await Effect.runPromise(
      runBackupCommand("abandon-sources", planFile, connect),
    );
    assert.deepEqual(abandoned, {
      schemaVersion: 1,
      command: "abandon-sources",
      result: { phase: "source-abandoned", objects: 1 },
    });
    assert.equal(opened, 7);
    assert.equal(closed, opened);
    assert.equal(await source.invoke("fence-request", ""), "204");
    await source.invoke(
      "admin",
      encodeBackupFrame({ objectId: sourceId, snapshotId: "newer", command: { _tag: "Hold" } }),
    );
    await Effect.runPromise(runBackupCommand("abandon-sources", planFile, connect));
    assert.equal(opened, 7);
    assert.equal(await source.invoke("fence-request", ""), "503");
  } finally {
    await runtime.mf.dispose();
    await fs.rm(scratch, { recursive: true, force: true });
  }
});

it("redacts CLI failures without emitting a success report", () => {
  const child = spawnSync(
    process.execPath,
    ["tools/backup-cloudflare.ts", "capture", "/missing/private-secret-plan.json"],
    { encoding: "utf8" },
  );
  assert.equal(child.status, 1);
  assert.equal(child.stdout, "");
  assert.match(child.stderr, /backup command failed/);
  assert.doesNotMatch(child.stderr, /private-secret/);
});
