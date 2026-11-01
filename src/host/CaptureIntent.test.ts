import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { tmpdir } from "node:os";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import * as Ssh from "../crypto/SshSignature.ts";
import {
  captureNamespaceArchive,
  verifyCaptureIntent,
  verifyNamespaceArchive,
} from "../../tools/cloudflare-namespace-archive.ts";
import { inventoryCloudflareObjects } from "../../tools/inventory-cloudflare-objects.ts";

for (const failure of ["second-hold", "r2-copy"] as const)
  it(`retains authenticated capture intent before holds when capture fails at ${failure}`, async () => {
    const scratch = await fs.mkdtemp(path.join(tmpdir(), "capture-intent-"));
    try {
      const scope = {
        installationId: "intent-fixture",
        snapshotId: "snapshot",
        account: "a".repeat(32),
        namespace: "b".repeat(32),
        deploymentSha256: "c".repeat(64),
      };
      const ids = ["1".repeat(64), "2".repeat(64)] as const;
      const inventoryDirectory = path.join(scratch, "inventory");
      const directory = path.join(scratch, "archive");
      await inventoryCloudflareObjects(
        scope.account,
        scope.namespace,
        inventoryDirectory,
        "fixture",
        async () =>
          Response.json({
            success: true,
            result: ids.map((id) => ({ id, hasStoredData: true })),
            result_info: {},
          }),
      );
      const key = await Effect.runPromise(Ssh.generate("capture-intent"));
      const signer = await Effect.runPromise(Ssh.fingerprint(key.publicKey));
      const expected = { directory, scope, signer };
      let holds = 0;
      let bucketCalls = 0;
      const unavailableBucket = {
        list: async () => {
          bucketCalls++;
          throw new Error("copy unavailable");
        },
        get: async () => {
          throw new Error("unexpected body read");
        },
        put: async () => {
          throw new Error("unexpected body write");
        },
      };
      await assert.rejects(
        Effect.runPromise(
          captureNamespaceArchive({
            directory,
            inventoryDirectory,
            scope,
            key,
            sourceBucket: unavailableBucket,
            destinationBucket: unavailableBucket,
            client: {
              backup: async (request) => {
                assert.equal(request.command._tag, "Hold");
                assert.equal(request.objectId, ids[holds]);
                const intent = await Effect.runPromise(verifyCaptureIntent(expected));
                assert.equal(intent.objects, 2);
                assert.deepEqual(intent.scope, scope);
                assert.equal(
                  (await fs.stat(path.join(directory, "capture-intent.packet"))).mode & 0o777,
                  0o600,
                );
                holds++;
                if (failure === "second-hold" && holds === 2) throw new Error("hold unavailable");
                return "held";
              },
            },
          }),
        ),
      );
      assert.equal(holds, 2);
      assert.equal(bucketCalls, failure === "r2-copy" ? 1 : 0);
      assert.equal((await Effect.runPromise(verifyCaptureIntent(expected))).objects, 2);
      await assert.rejects(Effect.runPromise(verifyNamespaceArchive(expected)));
      await assert.rejects(fs.access(path.join(directory, "complete.packet")));
      await assert.rejects(
        Effect.runPromise(
          verifyCaptureIntent({
            ...expected,
            scope: { ...scope, deploymentSha256: "d".repeat(64) },
          }),
        ),
      );
      await assert.rejects(
        Effect.runPromise(
          verifyCaptureIntent({ ...expected, scope: { ...scope, snapshotId: "later" } }),
        ),
      );
      const otherKey = await Effect.runPromise(Ssh.generate("other"));
      const otherSigner = await Effect.runPromise(Ssh.fingerprint(otherKey.publicKey));
      await assert.rejects(
        Effect.runPromise(verifyCaptureIntent({ ...expected, signer: otherSigner })),
      );
      const records = path.join(directory, "namespace", "objects.ndjson");
      const original = await fs.readFile(records, "utf8");
      await fs.writeFile(records, original.replace(ids[0], "3".repeat(64)));
      await assert.rejects(Effect.runPromise(verifyCaptureIntent(expected)));
      await fs.writeFile(records, original);
      const packet = path.join(directory, "capture-intent.packet");
      await fs.truncate(packet, 64);
      await assert.rejects(Effect.runPromise(verifyCaptureIntent(expected)));
      assert.equal(holds, 2, "verification never performs remote operations");
    } finally {
      await fs.rm(scratch, { recursive: true, force: true });
    }
  });
