import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Result, Schema } from "effect";
import * as Ssh from "../crypto/SshSignature.ts";
import * as Inventory from "../../tools/cloudflare-backup-inventory.ts";
import type { Position } from "../../tools/cloudflare-backup-auth.ts";
import { encodeBackupFrame } from "../../tools/cloudflare-backup-codec.ts";

const resources = {
  namespaceInventorySha256: "1".repeat(64),
  deploymentSha256: "2".repeat(64),
  r2: {
    objects: 3,
    bytes: 100,
    contentInventorySha256: "3".repeat(64),
    metadataInventorySha256: "5".repeat(64),
  },
} satisfies Inventory.Resources;
const position = (sequence: number): Schema.Schema.Type<typeof Position> => ({
  installationId: "installation",
  snapshotId: "snapshot",
  objectId: "object",
  kind: "kv-page",
  sequence,
});

it.effect("authenticates the complete ordered inventory and independently checked resources", () =>
  Effect.gen(function* () {
    const key = yield* Ssh.generate("inventory-fixture");
    const trusted = yield* Ssh.fingerprint(key.publicKey);
    const start = yield* Inventory.startInventory("installation", "snapshot");
    const first = yield* Inventory.captureInventoryFrame(
      key,
      start,
      position(0),
      encodeBackupFrame({ first: "private" }),
    );
    const second = yield* Inventory.captureInventoryFrame(
      key,
      first.state,
      position(1),
      encodeBackupFrame({ second: true }),
    );
    const seal = yield* Inventory.sealInventory(key, second.state, resources);
    const restoredFirst = yield* Inventory.restoreInventoryFrame(
      first.packet,
      start,
      position(0),
      trusted,
    );
    const restoredSecond = yield* Inventory.restoreInventoryFrame(
      second.packet,
      restoredFirst.state,
      position(1),
      trusted,
    );
    assert.deepEqual(restoredSecond.state, second.state);
    assert.equal(
      (yield* Inventory.verifyInventory(seal, restoredSecond.state, resources, trusted)).state
        .frames,
      2,
    );
    const omitted = yield* Inventory.restoreInventoryFrame(
      second.packet,
      start,
      position(1),
      trusted,
    );
    const reordered = yield* Inventory.restoreInventoryFrame(
      first.packet,
      omitted.state,
      position(0),
      trusted,
    );
    const repeated = yield* Inventory.restoreInventoryFrame(
      first.packet,
      restoredFirst.state,
      position(0),
      trusted,
    );
    for (const state of [
      start,
      restoredFirst.state,
      omitted.state,
      reordered.state,
      repeated.state,
    ])
      assert.ok(
        Result.isFailure(
          yield* Inventory.verifyInventory(seal, state, resources, trusted).pipe(Effect.result),
        ),
      );
    for (const changed of [
      { ...resources, namespaceInventorySha256: "4".repeat(64) },
      { ...resources, deploymentSha256: "4".repeat(64) },
      { ...resources, r2: { ...resources.r2, objects: 4 } },
      { ...resources, r2: { ...resources.r2, bytes: 101 } },
      { ...resources, r2: { ...resources.r2, contentInventorySha256: "4".repeat(64) } },
      { ...resources, r2: { ...resources.r2, metadataInventorySha256: "4".repeat(64) } },
    ])
      assert.ok(
        Result.isFailure(
          yield* Inventory.verifyInventory(seal, second.state, changed, trusted).pipe(
            Effect.result,
          ),
        ),
      );
  }),
);

it.effect(
  "binds empty inventories to their snapshot and refuses overflow or inventory-as-data",
  () =>
    Effect.gen(function* () {
      const key = yield* Ssh.generate("inventory-fixture");
      const trusted = yield* Ssh.fingerprint(key.publicKey);
      const start = yield* Inventory.startInventory("installation", "snapshot");
      const seal = yield* Inventory.sealInventory(key, start, resources);
      assert.equal(
        (yield* Inventory.verifyInventory(seal, start, resources, trusted)).state.frames,
        0,
      );
      const other = yield* Inventory.startInventory("installation", "other");
      assert.notEqual(start.root, other.root);
      assert.ok(
        Result.isFailure(
          yield* Inventory.verifyInventory(seal, other, resources, trusted).pipe(Effect.result),
        ),
      );
      for (const state of [
        { ...start, frames: Number.MAX_SAFE_INTEGER },
        { ...start, bytes: Number.MAX_SAFE_INTEGER },
      ])
        assert.ok(
          Result.isFailure(
            yield* Inventory.captureInventoryFrame(key, state, position(0), "payload").pipe(
              Effect.result,
            ),
          ),
        );
      assert.ok(
        Result.isFailure(
          yield* Inventory.captureInventoryFrame(
            key,
            start,
            { ...position(0), kind: "inventory" },
            "payload",
          ).pipe(Effect.result),
        ),
      );
      assert.ok(
        Result.isFailure(
          yield* Inventory.captureInventoryFrame(
            key,
            start,
            { ...position(0), snapshotId: "other" },
            "payload",
          ).pipe(Effect.result),
        ),
      );
    }),
);
