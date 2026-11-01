/** Private disk transport for one object. Installation coordination owns isolation, R2 and release. */
import * as fs from "node:fs/promises";
import { createHash } from "node:crypto";
import { readBackupFile as read } from "./cloudflare-backup-read.ts";
import * as path from "node:path";
import { Effect, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import type { Request } from "../src/host/CloudflareBackupAdmin.ts";
import { fingerprint, type PrivateKey } from "../src/crypto/SshSignature.ts";
import { Plan, Checkpoint } from "./cloudflare-object-import.ts";
import * as Inventory from "./cloudflare-backup-inventory.ts";
import { Position, packetBytes } from "./cloudflare-backup-auth.ts";
import { decodeBackupFrame, encodeBackupFrame } from "./cloudflare-backup-codec.ts";
import { SqlCatalog, SqlPage } from "./cloudflare-sql-backup.ts";
import { KvPage } from "./cloudflare-kv-backup.ts";

export interface Client {
  readonly backup: (request: Request) => Promise<string>;
}
export interface Capture {
  readonly client: Client;
  readonly directory: string;
  readonly plan: Plan;
  readonly resources: Inventory.Resources;
  readonly key: PrivateKey;
}
export interface Verify {
  readonly directory: string;
  readonly plan: Plan;
  readonly resources: Inventory.Resources;
}
export interface Restore extends Verify {
  readonly client: Client;
  readonly targetObjectId: string;
}
const Archive = Schema.Struct({
  version: Schema.Literal(1),
  plan: Plan,
  inventory: Inventory.State,
  seal: Schema.String,
});
export interface Archive extends Schema.Schema.Type<typeof Archive> {}
const Header = Schema.fromJsonString(Schema.Struct({ position: Position }));
const maxFrames = 1_000_000;
const nameOf = (sequence: number) => `${String(sequence).padStart(12, "0")}.packet`;
const invalid = () => new Invalid({ field: "backup", reason: "object archive rejected" });
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: invalid });
const rpc = Effect.fn("Backup.archiveRpc")(function* (
  client: Client,
  objectId: string,
  snapshotId: string,
  command: Request["command"],
) {
  const value = yield* io(() => client.backup({ objectId, snapshotId, command }));
  // Raw Alchemy RPC failures are envelopes, not thrown exceptions.
  return yield* Schema.decodeUnknownEffect(Schema.String)(value).pipe(Effect.mapError(invalid));
});
const write = async (file: string, value: string) => {
  const output = await fs.open(file, "wx", 0o600);
  try {
    await output.writeFile(value, "utf8");
    await output.sync();
  } finally {
    await output.close();
  }
};
const syncDirectory = async (directory: string) => {
  const handle = await fs.open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
};
const decoded = <A, I>(frame: string, schema: Schema.Codec<A, I>) =>
  Effect.try({ try: () => decodeBackupFrame(frame, schema), catch: invalid });
const sameInventory = (left: Inventory.State, right: Inventory.State) =>
  encodeBackupFrame(left) === encodeBackupFrame(right);

/** Writes completion last and leaves the source fenced, including on failure. */
export const captureObjectArchive = Effect.fn("Backup.captureObjectArchive")(function* (
  options: Capture,
) {
  const plan = yield* Schema.decodeEffect(Plan)(options.plan).pipe(Effect.mapError(invalid));
  yield* Schema.decodeEffect(Inventory.Resources)(options.resources).pipe(Effect.mapError(invalid));
  if ((yield* fingerprint(options.key.publicKey)) !== plan.signer) return yield* invalid();
  yield* io(() => fs.mkdir(options.directory, { mode: 0o700 }));
  if ((yield* rpc(options.client, plan.objectId, plan.snapshotId, { _tag: "Hold" })) !== "held")
    return yield* invalid();
  let inventory = yield* Inventory.startInventory(plan.installationId, plan.snapshotId);
  const append = Effect.fn("Backup.archiveAppend")(function* (
    kind: Position["kind"],
    payload: string,
  ) {
    if (inventory.frames >= maxFrames) return yield* invalid();
    const captured = yield* Inventory.captureInventoryFrame(
      options.key,
      inventory,
      {
        installationId: plan.installationId,
        snapshotId: plan.snapshotId,
        objectId: plan.objectId,
        kind,
        sequence: inventory.frames,
      },
      payload,
    );
    yield* io(() => write(path.join(options.directory, nameOf(inventory.frames)), captured.packet));
    inventory = captured.state;
  });
  const catalogFrame = yield* rpc(options.client, plan.objectId, plan.snapshotId, {
    _tag: "Catalog",
  });
  const catalog = yield* decoded(catalogFrame, SqlCatalog);
  yield* append("sql-catalog", catalogFrame);
  for (const table of catalog.objects.filter((object) => object.type === "table")) {
    let after: string | null = null;
    do {
      const frame: string = yield* rpc(options.client, plan.objectId, plan.snapshotId, {
        _tag: "SqlPage",
        table: table.name,
        after,
        limit: 8,
      });
      const page: SqlPage = yield* decoded(frame, SqlPage);
      if (
        page.table !== table.name ||
        page.after !== after ||
        (page.next !== null && page.next === after)
      )
        return yield* invalid();
      yield* append("sql-page", frame);
      after = page.next;
    } while (after !== null);
  }
  let after: string | null = null;
  do {
    const frame: string = yield* rpc(options.client, plan.objectId, plan.snapshotId, {
      _tag: "KvPage",
      after,
      limit: 8,
    });
    const page: KvPage = yield* decoded(frame, KvPage);
    if (
      page.snapshotId !== plan.snapshotId ||
      page.after !== after ||
      (page.next !== null && page.next === after)
    )
      return yield* invalid();
    yield* append("kv-page", frame);
    after = page.next;
  } while (after !== null);
  const seal = yield* Inventory.sealInventory(options.key, inventory, options.resources);
  const archive: Archive = { version: 1, plan, inventory, seal };
  yield* io(() => syncDirectory(options.directory));
  yield* io(() => write(path.join(options.directory, "complete.json"), encodeBackupFrame(archive)));
  yield* io(() => syncDirectory(options.directory));
  yield* io(() => syncDirectory(path.dirname(path.resolve(options.directory))));
  return archive;
});

const verifyPacket = Effect.fn("Backup.verifyArchivePacket")(function* (
  packet: string,
  plan: Plan,
  state: Inventory.State,
) {
  const header = yield* Schema.decodeEffect(Header)(packet).pipe(Effect.mapError(invalid));
  if (header.position.kind === "inventory") return yield* invalid();
  return yield* Inventory.restoreInventoryFrame(
    packet,
    state,
    {
      installationId: plan.installationId,
      snapshotId: plan.snapshotId,
      objectId: plan.objectId,
      kind: header.position.kind,
      sequence: state.frames,
    },
    plan.signer,
  );
});

/** Authenticate every archive packet and its resource summary without contacting a target. */
export const verifyObjectArchive = Effect.fn("Backup.verifyObjectArchive")(function* (
  options: Verify,
) {
  const plan = yield* Schema.decodeEffect(Plan)(options.plan).pipe(Effect.mapError(invalid));
  const archive = yield* decoded(
    yield* io(() => read(path.join(options.directory, "complete.json"), 64 * 1024)),
    Archive,
  );
  if (
    encodeBackupFrame(archive.plan) !== encodeBackupFrame(plan) ||
    archive.inventory.frames < 2 ||
    archive.inventory.frames > maxFrames
  )
    return yield* invalid();
  let inventory = yield* Inventory.startInventory(plan.installationId, plan.snapshotId);
  for (let sequence = 0; sequence < archive.inventory.frames; sequence++) {
    const packet = yield* io(() =>
      read(path.join(options.directory, nameOf(sequence)), packetBytes),
    );
    inventory = (yield* verifyPacket(packet, plan, inventory)).state;
  }
  if (!sameInventory(inventory, archive.inventory)) return yield* invalid();
  yield* Inventory.verifyInventory(archive.seal, inventory, options.resources, plan.signer);
  return archive;
});

/** Preflight authenticates the archive; replay verifies the target's durable prefix. */
export const restoreObjectArchive = Effect.fn("Backup.restoreObjectArchive")(function* (
  options: Restore,
) {
  const archive = yield* verifyObjectArchive(options);
  const plan = archive.plan;
  if (
    (yield* rpc(options.client, options.targetObjectId, plan.snapshotId, { _tag: "Hold" })) !==
    "held"
  )
    return yield* invalid();
  const existing = yield* decoded(
    yield* rpc(options.client, options.targetObjectId, plan.snapshotId, { _tag: "Start", plan }),
    Checkpoint,
  );
  if (
    encodeBackupFrame(existing.plan) !== encodeBackupFrame(plan) ||
    existing.inventory.frames > archive.inventory.frames
  )
    return yield* invalid();
  let inventory = yield* Inventory.startInventory(plan.installationId, plan.snapshotId);
  if (existing.inventory.frames === 0 && !sameInventory(inventory, existing.inventory))
    return yield* invalid();
  for (let sequence = 0; sequence < archive.inventory.frames; sequence++) {
    const packet = yield* io(() =>
      read(path.join(options.directory, nameOf(sequence)), packetBytes),
    );
    inventory = (yield* verifyPacket(packet, plan, inventory)).state;
    if (
      inventory.frames === existing.inventory.frames &&
      !sameInventory(inventory, existing.inventory)
    )
      return yield* invalid();
    if (sequence >= existing.inventory.frames) {
      const result = yield* decoded(
        yield* rpc(options.client, options.targetObjectId, plan.snapshotId, {
          _tag: "Apply",
          packet,
        }),
        Checkpoint,
      );
      if (
        encodeBackupFrame(result.plan) !== encodeBackupFrame(plan) ||
        !sameInventory(inventory, result.inventory) ||
        result.lastPacket !== createHash("sha256").update(packet).digest("hex")
      )
        return yield* invalid();
    }
  }
  if (!sameInventory(inventory, archive.inventory)) return yield* invalid();
  const receipt = yield* rpc(options.client, options.targetObjectId, plan.snapshotId, {
    _tag: "Finish",
    packet: archive.seal,
    resources: options.resources,
  });
  const completed = yield* decoded(receipt, Checkpoint);
  if (
    completed.phase !== "complete" ||
    encodeBackupFrame(completed.plan) !== encodeBackupFrame(plan) ||
    !sameInventory(completed.inventory, archive.inventory) ||
    completed.seal !== createHash("sha256").update(archive.seal).digest("hex")
  )
    return yield* invalid();
  return receipt;
});
