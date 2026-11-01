/** Local-only rehearsal of the actual deployed DO's private RPC contract. */
import assert from "node:assert/strict";
import type { DurableObjectId } from "@cloudflare/workers-types";
import { Effect, Schema } from "effect";
import { getPlatformProxy } from "@alchemy.run/cloudflare-runtime/core/platform-proxy";
import { DurableObjectNamespace, Service } from "@alchemy.run/cloudflare-runtime/core/bindings";
import type { Request } from "../src/host/CloudflareBackupAdmin.ts";
import { generate, fingerprint } from "../src/crypto/SshSignature.ts";
import { Checkpoint } from "./cloudflare-object-import.ts";
import { decodeBackupFrame } from "./cloudflare-backup-codec.ts";
import { SqlCatalog } from "./cloudflare-sql-backup.ts";
import { KvPage } from "./cloudflare-kv-backup.ts";
import * as Inventory from "./cloudflare-backup-inventory.ts";
import type { Position } from "./cloudflare-backup-auth.ts";

interface Stub {
  backup(request: Request): Promise<string>;
  fetch(request: string): Promise<Response>;
}
interface Bindings {
  REPOS: { getByName(name: string): Stub; idFromName(name: string): Promise<DurableObjectId> };
  OPERATOR: {
    backup(request: Request): Promise<string>;
    fetch(request: string, init?: RequestInit): Promise<Response>;
  };
}
export const verifyBackupAdministration = async (
  directory: string,
  scriptName: string,
  uniqueKey: string,
) => {
  const proxy = await getPlatformProxy<Bindings>({
    name: `backup-operator-${crypto.randomUUID()}`,
    persist: { directory },
    bindings: [
      DurableObjectNamespace.local({ binding: "REPOS", className: "Repo", scriptName, uniqueKey }),
      Service.local({ binding: "OPERATOR", scriptName }),
    ],
  });
  try {
    const stub = proxy.env.REPOS.getByName("backup-administration-proof");
    const objectId = (await proxy.env.REPOS.idFromName("backup-administration-proof")).toString();
    assert.match(objectId, /^[a-f0-9]{64}$/);
    const snapshotId = "local-administration-proof";
    const command = (command: Request["command"]) =>
      proxy.env.OPERATOR.backup({ objectId, snapshotId, command });
    await assert.rejects(async () =>
      Schema.decodeUnknownSync(Schema.String)(
        await proxy.env.OPERATOR.backup({
          objectId: "malformed",
          snapshotId,
          command: { _tag: "Hold" },
        }),
      ),
    );
    assert.equal(await command({ _tag: "Hold" }), "held");
    const publicAttempt = await proxy.env.OPERATOR.fetch("https://fixture/backup", {
      method: "POST",
      body: JSON.stringify({ objectId, snapshotId, command: { _tag: "Release" } }),
    });
    assert.ok(publicAttempt.status >= 400, "HTTP must not expose private backup RPC");
    await publicAttempt.body?.cancel();
    const blocked = await stub.fetch(
      "https://fixture/backup-administration-proof/info/refs?service=git-upload-pack",
    );
    assert.equal(blocked.status, 503);
    await blocked.body?.cancel();
    const catalog = await command({ _tag: "Catalog" });
    assert.deepEqual(decodeBackupFrame(catalog, SqlCatalog).objects, []);
    const kv = await command({ _tag: "KvPage", after: null, limit: 1 });
    assert.deepEqual(decodeBackupFrame(kv, KvPage).entries, []);
    const key = await Effect.runPromise(generate("local-backup-operator"));
    const signer = await Effect.runPromise(fingerprint(key.publicKey));
    const installationId = "local-backup-operator";
    await command({ _tag: "Start", plan: { objectId, snapshotId, installationId, signer } });
    await assert.rejects(async () =>
      Schema.decodeUnknownSync(Schema.String)(await command({ _tag: "AbandonCapture" })),
    );
    let inventory = await Effect.runPromise(Inventory.startInventory(installationId, snapshotId));
    const frames: ReadonlyArray<readonly [Position["kind"], string]> = [
      ["sql-catalog", catalog],
      ["kv-page", kv],
    ];
    for (const [kind, payload] of frames) {
      const captured = await Effect.runPromise(
        Inventory.captureInventoryFrame(
          key,
          inventory,
          { objectId, installationId, snapshotId, kind, sequence: inventory.frames },
          payload,
        ),
      );
      const receipt = await command({ _tag: "Apply", packet: captured.packet });
      assert.deepEqual(decodeBackupFrame(receipt, Checkpoint).inventory, captured.state);
      assert.equal(await command({ _tag: "Apply", packet: captured.packet }), receipt);
      inventory = captured.state;
    }
    // This is an empty-object protocol rehearsal, not evidence of installation/R2 recovery.
    const resources: Inventory.Resources = {
      namespaceInventorySha256: "0".repeat(64),
      deploymentSha256: "0".repeat(64),
      r2: {
        objects: 0,
        bytes: 0,
        contentInventorySha256: "0".repeat(64),
        metadataInventorySha256: "0".repeat(64),
      },
    };
    const packet = await Effect.runPromise(Inventory.sealInventory(key, inventory, resources));
    const receipt = await command({ _tag: "Finish", packet, resources });
    assert.equal(decodeBackupFrame(receipt, Checkpoint).phase, "complete");
    assert.equal(await command({ _tag: "Status" }), receipt);
    assert.equal(await command({ _tag: "Retire", receipt }), "retired");
    assert.equal(await command({ _tag: "Retire", receipt }), "retired");
    assert.equal(await command({ _tag: "Release" }), "released");
    assert.equal(await command({ _tag: "Release" }), "released");
    const nextSnapshot = `${snapshotId}-next`;
    assert.equal(
      await stub.backup({ objectId, snapshotId: nextSnapshot, command: { _tag: "Hold" } }),
      "held",
    );
    // The Alchemy bridge can encode a failed RPC as a non-string envelope.
    // Apply the same string boundary as the archive client before accepting it.
    await assert.rejects(async () =>
      Schema.decodeUnknownSync(Schema.String)(await command({ _tag: "Release" })),
    );
    await assert.rejects(async () =>
      Schema.decodeUnknownSync(Schema.String)(await command({ _tag: "AbandonCapture" })),
    );
    const stillHeld = await stub.fetch(
      "https://fixture/backup-administration-proof/info/refs?service=git-upload-pack",
    );
    assert.equal(stillHeld.status, 503);
    await stillHeld.body?.cancel();
    for (let attempt = 0; attempt < 2; attempt++)
      assert.equal(
        await proxy.env.OPERATOR.backup({
          objectId,
          snapshotId: nextSnapshot,
          command: { _tag: "AbandonCapture" },
        }),
        "abandoned",
      );
  } finally {
    await proxy.dispose();
  }
};
