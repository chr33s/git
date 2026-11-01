/** Private, single-object SQL/KV import session. The caller binds the plan to the actual DO identity. */
import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Effect, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import * as Ssh from "../src/crypto/SshSignature.ts";
import { bytesToHex } from "../src/git/Format.ts";
import { fenceKey, FenceMarker, checkpointKey } from "./cloudflare-backup-control.ts";
export { checkpointKey } from "./cloudflare-backup-control.ts";
import { KvProgress, importKvPage } from "./cloudflare-kv-backup.ts";
import { Position, packetBytes } from "./cloudflare-backup-auth.ts";
import * as Inventory from "./cloudflare-backup-inventory.ts";
import { decodeBackupFrame, encodeBackupFrame } from "./cloudflare-backup-codec.ts";
import {
  beginSqlRestore,
  restoreSqlPage,
  finishSqlRestore,
  sqlRestoreComplete,
} from "./cloudflare-sql-restore.ts";
import { captureSqlCatalog, SqlCatalog } from "./cloudflare-sql-backup.ts";

export const Plan = Schema.Struct({
  installationId: Position.fields.installationId,
  snapshotId: Position.fields.snapshotId,
  objectId: Position.fields.objectId,
  signer: Schema.String.pipe(Schema.refine(Ssh.isFingerprint)),
});
export interface Plan extends Schema.Schema.Type<typeof Plan> {}
const Hash = Schema.NullOr(Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)));
export const Checkpoint = Schema.Struct({
  version: Schema.Literal(1),
  plan: Plan,
  inventory: Inventory.State,
  phase: Schema.Literals(["sql", "kv", "complete"]),
  kv: Schema.NullOr(KvProgress),
  lastPacket: Hash,
  seal: Hash,
});
export interface Checkpoint extends Schema.Schema.Type<typeof Checkpoint> {}
type Storage = Pick<DurableObjectStorage, "kv" | "sql" | "transactionSync" | "sync">;
const invalid = () => new Invalid({ field: "backup", reason: "Object import checkpoint rejected" });
const read = (storage: Storage) => {
  const raw = storage.kv.get(checkpointKey);
  const frame = Schema.decodeUnknownSync(Schema.String)(raw);
  return { frame, checkpoint: decodeBackupFrame(frame, Checkpoint) };
};
const held = (storage: Storage, snapshotId: string) => {
  const marker = Schema.decodeUnknownSync(FenceMarker)(storage.kv.get(fenceKey));
  if (marker.phase !== "held" || marker.snapshotId !== snapshotId) throw invalid();
};
const digest = (packet: string) =>
  Effect.tryPromise({
    try: async () => {
      // Same envelope ceiling as the authenticated transport, before allocation/hashing.
      if (packet.length > packetBytes) throw invalid();
      const bytes = new TextEncoder().encode(packet);
      if (bytes.byteLength > packetBytes) throw invalid();
      return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
    },
    catch: invalid,
  });
const flushed = (storage: Storage) =>
  Effect.tryPromise({ try: () => storage.sync(), catch: invalid });

export const startObjectImport = Effect.fn("Backup.startObjectImport")(function* (
  storage: Storage,
  plan: Plan,
) {
  const checked = yield* Schema.decodeEffect(Plan)(plan).pipe(Effect.mapError(invalid));
  const inventory = yield* Inventory.startInventory(checked.installationId, checked.snapshotId);
  const checkpoint: Checkpoint = {
    version: 1,
    plan: checked,
    inventory,
    phase: "sql",
    kv: null,
    lastPacket: null,
    seal: null,
  };
  const result = yield* Effect.try({
    try: () =>
      storage.transactionSync(() => {
        held(storage, checked.snapshotId);
        if (storage.kv.get(checkpointKey) !== undefined) {
          const existing = read(storage).checkpoint;
          if (encodeBackupFrame(existing.plan) !== encodeBackupFrame(checked)) throw invalid();
          return existing;
        }
        for (const [key] of storage.kv.list({ limit: 2 })) if (key !== fenceKey) throw invalid();
        if (decodeBackupFrame(captureSqlCatalog(storage.sql), SqlCatalog).objects.length !== 0)
          throw invalid();
        storage.kv.put(checkpointKey, encodeBackupFrame(checkpoint));
        return checkpoint;
      }),
    catch: invalid,
  });
  yield* flushed(storage);
  return result;
});

export const readObjectImport = Effect.fn("Backup.readObjectImport")((storage: Storage) =>
  Effect.try({ try: () => read(storage).checkpoint, catch: invalid }),
);

export const applyObjectImport = Effect.fn("Backup.applyObjectImport")(function* (
  storage: Storage,
  packet: string,
) {
  const previous = yield* Effect.try({ try: () => read(storage), catch: invalid });
  const hash = yield* digest(packet);
  const current = previous.checkpoint;
  if (current.phase === "complete") return yield* invalid();
  if (current.lastPacket === hash) {
    yield* Effect.try({ try: () => held(storage, current.plan.snapshotId), catch: invalid });
    yield* flushed(storage);
    return current;
  }
  const position: Position = {
    ...current.plan,
    kind:
      current.phase === "kv"
        ? "kv-page"
        : current.inventory.frames === 0
          ? "sql-catalog"
          : "sql-page",
    sequence: current.inventory.frames,
  };
  const verified = yield* Inventory.restoreInventoryFrame(
    packet,
    current.inventory,
    position,
    current.plan.signer,
  );
  const result = yield* Effect.try({
    try: () =>
      storage.transactionSync(() => {
        held(storage, current.plan.snapshotId);
        const latest = read(storage);
        if (latest.frame !== previous.frame) {
          if (latest.checkpoint.phase !== "complete" && latest.checkpoint.lastPacket === hash)
            return latest.checkpoint;
          throw invalid();
        }
        let kv = current.kv;
        if (position.kind === "kv-page")
          kv = importKvPage(storage.kv, current.plan.snapshotId, verified.payload, kv);
        else if (position.kind === "sql-catalog")
          beginSqlRestore(storage, current.plan.snapshotId, verified.payload);
        else restoreSqlPage(storage, current.plan.snapshotId, verified.payload);
        const phase = sqlRestoreComplete(storage, current.plan.snapshotId) ? "kv" : "sql";
        const next: Checkpoint = {
          ...current,
          inventory: verified.state,
          lastPacket: hash,
          phase,
          kv,
        };
        storage.kv.put(checkpointKey, encodeBackupFrame(next));
        return next;
      }),
    catch: invalid,
  });
  yield* flushed(storage);
  return result;
});

/** Object completion only. Leave the fence held until the installation coordinator verifies all objects and R2. */
export const finishObjectImport = Effect.fn("Backup.finishObjectImport")(function* (
  storage: Storage,
  packet: string,
  resources: Inventory.Resources,
) {
  const previous = yield* Effect.try({ try: () => read(storage), catch: invalid });
  const current = previous.checkpoint;
  const hash = yield* digest(packet);
  yield* Inventory.verifyInventory(packet, current.inventory, resources, current.plan.signer);
  const result = yield* Effect.try({
    try: () =>
      storage.transactionSync(() => {
        held(storage, current.plan.snapshotId);
        const latest = read(storage);
        if (latest.checkpoint.phase === "complete" && latest.checkpoint.seal === hash)
          return latest.checkpoint;
        if (latest.frame !== previous.frame || current.phase !== "kv" || !current.kv?.complete)
          throw invalid();
        finishSqlRestore(storage, current.plan.snapshotId);
        const next: Checkpoint = { ...current, phase: "complete", seal: hash };
        storage.kv.put(checkpointKey, encodeBackupFrame(next));
        return next;
      }),
    catch: invalid,
  });
  yield* flushed(storage);
  return result;
});

/** Private coordinator acknowledgement: preserve the fence while retiring an exact completed receipt. */
export const retireObjectImport = Effect.fn("Backup.retireObjectImport")(function* (
  storage: Storage,
  snapshotId: string,
  receipt: string,
) {
  yield* Effect.try({
    try: () =>
      storage.transactionSync(() => {
        held(storage, snapshotId);
        const expected = decodeBackupFrame(receipt, Checkpoint);
        if (expected.phase !== "complete" || expected.plan.snapshotId !== snapshotId)
          throw invalid();
        // Missing is an idempotent retry after successful deletion or a lost flush acknowledgement.
        if (storage.kv.get(checkpointKey) === undefined) return;
        const current = read(storage);
        if (current.frame !== receipt || current.checkpoint.phase !== "complete") throw invalid();
        storage.kv.delete(checkpointKey);
      }),
    catch: invalid,
  });
  yield* flushed(storage);
});
