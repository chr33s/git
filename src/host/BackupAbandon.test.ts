import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { tmpdir } from "node:os";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { startBackupRuntime } from "../testing/BackupRuntime.ts";
import * as Ssh from "../crypto/SshSignature.ts";
import type { Request } from "./CloudflareBackupAdmin.ts";
import { encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";
import { captureNamespaceArchive } from "../../tools/cloudflare-namespace-archive.ts";
import {
  abandonCapturedSources,
  releaseCapturedSources,
} from "../../tools/cloudflare-source-release.ts";

it("abandons held, draining and untouched sources without losing data or newer holds", async () => {
  const scratch = await fs.mkdtemp(path.join(tmpdir(), "backup-abandon-"));
  const runtime = await startBackupRuntime(scratch);
  try {
    const { bindings, guarded, mf } = runtime;
    const names = ["held", "draining", "untouched"];
    const sources = new Map<string, ReturnType<typeof guarded>>();
    for (const name of names) {
      const object = guarded(bindings.STORE.getByName(name));
      sources.set(await object.invoke("object-id", ""), object);
    }
    const entries = [...sources];
    const first = entries[0];
    const second = entries[1];
    assert.ok(first && second);
    const client = {
      backup: (request: Request) => {
        const object = sources.get(request.objectId);
        assert.ok(object);
        return object.invoke("admin", encodeBackupFrame(request));
      },
    };
    const scope = {
      installationId: "abandon-fixture",
      snapshotId: "failed-capture",
      account: "a".repeat(32),
      namespace: "b".repeat(32),
      deploymentSha256: "c".repeat(64),
    };
    const inventoryDirectory = path.join(scratch, "inventory");
    await inventoryCloudflareObjects(
      scope.account,
      scope.namespace,
      inventoryDirectory,
      "fixture",
      async () =>
        Response.json({
          success: true,
          result: [...sources.keys()].map((id) => ({ id, hasStoredData: false })),
          result_info: {},
        }),
    );
    const key = await Effect.runPromise(Ssh.generate("abandon-fixture"));
    const signer = await Effect.runPromise(Ssh.fingerprint(key.publicKey));
    const directory = path.join(scratch, "archive");
    await bindings.ARCHIVE.put("retained-partial-data", "do not delete");
    await second[1].invoke("fence-enter", "");
    await second[1].invoke("fence-sync-fail", "on");
    await assert.rejects(
      Effect.runPromise(
        captureNamespaceArchive({
          directory,
          inventoryDirectory,
          scope,
          key,
          client,
          sourceBucket: bindings.SOURCE,
          destinationBucket: bindings.ARCHIVE,
        }),
      ),
    );
    await second[1].invoke("fence-sync-fail", "off");
    const intentBefore = await fs.readFile(path.join(directory, "capture-intent.packet"));
    const journalDirectory = path.join(scratch, "journal");
    const options = { directory, scope, signer, journalDirectory, decisionSha256: "d".repeat(64) };
    let calls = 0;
    let lost = true;
    let notifyDrain = () => {};
    const drainStarted = new Promise<void>((resolve) => {
      notifyDrain = resolve;
    });
    const releasing = {
      backup: async (request: Request) => {
        assert.equal(request.command._tag, "AbandonCapture");
        calls++;
        if (request.objectId === second[0]) notifyDrain();
        const response = await client.backup(request);
        if (lost) {
          lost = false;
          throw new Error("lost abandonment acknowledgement");
        }
        return response;
      },
    };
    await assert.rejects(
      Effect.runPromise(
        releaseCapturedSources({ ...options, client: releasing, archiveBucket: bindings.ARCHIVE }),
      ),
    );
    assert.equal(calls, 0, "normal release still requires a completed backup");
    await assert.rejects(
      Effect.runPromise(abandonCapturedSources({ ...options, client: releasing })),
    );
    assert.equal(await first[1].invoke("fence-request", ""), "204");
    await mf.unsafeEvictDurableObject("backup-codec", "Store", { name: "held" });
    await assert.rejects(
      Effect.runPromise(
        abandonCapturedSources({ ...options, decisionSha256: "e".repeat(64), client: releasing }),
      ),
    );
    assert.equal(calls, 1, "changed decision cannot reuse an in-progress journal");
    const retry = Effect.runPromise(abandonCapturedSources({ ...options, client: releasing }));
    await drainStarted;
    await second[1].invoke("fence-followup", "");
    await second[1].invoke("fence-done", "");
    assert.equal(await second[1].invoke("fence-request", ""), "503");
    await second[1].invoke("fence-done", "");
    assert.deepEqual(await retry, { phase: "source-abandoned", objects: 3 });
    assert.equal(calls, 4, "lost acknowledgement is retried, untouched source is harmless");
    for (const object of sources.values())
      assert.equal(await object.invoke("fence-request", ""), "204");
    assert.equal(
      await (await bindings.ARCHIVE.get("retained-partial-data"))?.text(),
      "do not delete",
    );
    assert.deepEqual(
      await fs.readFile(path.join(directory, "capture-intent.packet")),
      intentBefore,
    );
    await client.backup({ objectId: first[0], snapshotId: "newer", command: { _tag: "Hold" } });
    await Effect.runPromise(abandonCapturedSources({ ...options, client: releasing }));
    assert.equal(calls, 4);
    assert.equal(await first[1].invoke("fence-request", ""), "503");
    await assert.rejects(
      client.backup({
        objectId: first[0],
        snapshotId: scope.snapshotId,
        command: { _tag: "AbandonCapture" },
      }),
    );
    assert.equal(await first[1].invoke("fence-request", ""), "503");
    await first[1].invoke("fence-sync-fail", "on");
    await assert.rejects(
      client.backup({
        objectId: first[0],
        snapshotId: "newer",
        command: { _tag: "AbandonCapture" },
      }),
    );
    assert.equal(await first[1].invoke("fence-request", ""), "503");
    await first[1].invoke("fence-sync-fail", "off");
    assert.equal(
      await client.backup({
        objectId: first[0],
        snapshotId: "newer",
        command: { _tag: "AbandonCapture" },
      }),
      "abandoned",
    );
  } finally {
    await runtime.mf.dispose();
    await fs.rm(scratch, { recursive: true, force: true });
  }
});
