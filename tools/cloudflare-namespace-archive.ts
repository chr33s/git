/** Namespace capture coordinator. The operator must isolate other writers and namespace creation first. */
import * as fs from "node:fs/promises";
import { constants } from "node:fs";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { Effect, Schema, Stream } from "effect";
import { Invalid } from "../src/git/Error.ts";
import { fingerprint, type PrivateKey, type Fingerprint } from "../src/crypto/SshSignature.ts";
import {
  Position,
  signBackupFrame,
  verifyBackupFrame,
  packetBytes,
} from "./cloudflare-backup-auth.ts";
import { encodeBackupFrame, decodeBackupFrame } from "./cloudflare-backup-codec.ts";
import {
  captureObjectArchive,
  verifyObjectArchive,
  type Client,
} from "./cloudflare-object-archive.ts";
import { readBackupFile, readBackupLines } from "./cloudflare-backup-read.ts";
import * as Inventory from "./cloudflare-backup-inventory.ts";
import { copyR2Backup, type Source, type Destination } from "./copy-r2-backup.ts";
import {
  readCloudflareObjectInventory,
  verifyCloudflareObjectInventory,
} from "./inventory-cloudflare-objects.ts";

export const Scope = Schema.Struct({
  installationId: Position.fields.installationId,
  snapshotId: Position.fields.snapshotId,
  account: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
  namespace: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
  deploymentSha256: Inventory.Resources.fields.deploymentSha256,
});
export interface Scope extends Schema.Schema.Type<typeof Scope> {}
export interface Capture {
  readonly scope: Scope;
  readonly inventoryDirectory: string;
  readonly directory: string;
  readonly client: Client;
  readonly sourceBucket: Source;
  readonly destinationBucket: Destination;
  readonly key: PrivateKey;
}
export const Completion = Schema.Struct({
  version: Schema.Literal(1),
  scope: Scope,
  objects: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000_000 })),
  objectsSha256: Inventory.Resources.fields.deploymentSha256,
  resources: Inventory.Resources,
});
export interface Completion extends Schema.Schema.Type<typeof Completion> {}
export const CaptureIntent = Schema.Struct({
  version: Schema.Literal(1),
  scope: Scope,
  objects: Completion.fields.objects,
  inventorySha256: Inventory.Resources.fields.deploymentSha256,
});
export interface CaptureIntent extends Schema.Schema.Type<typeof CaptureIntent> {}
const intentPosition = (scope: Scope) => ({
  installationId: scope.installationId,
  snapshotId: scope.snapshotId,
  objectId: "__capture_intent__",
  kind: "inventory" as const,
  sequence: 0,
});
const invalid = () => new Invalid({ field: "backup", reason: "namespace archive rejected" });
const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: invalid });
const syncedDirectory = async (directory: string) => {
  const handle = await fs.open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
};
const records = (directory: string) =>
  Stream.fromAsyncIterable(readCloudflareObjectInventory(directory), invalid);

/** Never releases sources. A failed hold prevents every R2/object copy; later failure leaves an incomplete archive. */
export const captureNamespaceArchive = Effect.fn("Backup.captureNamespaceArchive")(function* (
  options: Capture,
) {
  const scope = yield* Schema.decodeEffect(Scope)(options.scope).pipe(Effect.mapError(invalid));
  const signer = yield* fingerprint(options.key.publicKey);
  const sourceInventory = yield* io(() =>
    verifyCloudflareObjectInventory(options.inventoryDirectory, scope.account, scope.namespace),
  );
  yield* io(() => fs.mkdir(options.directory, { mode: 0o700 }));
  const inventoryDirectory = path.join(options.directory, "namespace");
  const objectsDirectory = path.join(options.directory, "objects");
  yield* io(async () => {
    await fs.mkdir(inventoryDirectory, { mode: 0o700 });
    await fs.mkdir(objectsDirectory, { mode: 0o700 });
    for (const name of ["objects.ndjson", "inventory.json"]) {
      const destination = path.join(inventoryDirectory, name);
      await fs.copyFile(
        path.join(options.inventoryDirectory, name),
        destination,
        constants.COPYFILE_EXCL,
      );
      await fs.chmod(destination, 0o600);
      const handle = await fs.open(destination, "r");
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
    }
    await syncedDirectory(inventoryDirectory);
  });
  const copiedInventory = yield* io(() =>
    verifyCloudflareObjectInventory(inventoryDirectory, scope.account, scope.namespace),
  );
  if (copiedInventory.inventorySha256 !== sourceInventory.inventorySha256) return yield* invalid();
  // Authenticate and durably retain the source set before any hold can succeed.
  // This records capture intent only; it is never evidence of a completed backup.
  const intent: CaptureIntent = {
    version: 1,
    scope,
    objects: copiedInventory.objects,
    inventorySha256: copiedInventory.inventorySha256,
  };
  const intentPacket = yield* signBackupFrame(
    options.key,
    intentPosition(scope),
    encodeBackupFrame(intent),
  );
  yield* io(async () => {
    const file = await fs.open(path.join(options.directory, "capture-intent.packet"), "wx", 0o600);
    try {
      await file.writeFile(intentPacket);
      await file.sync();
    } finally {
      await file.close();
    }
    await syncedDirectory(options.directory);
    await syncedDirectory(path.dirname(path.resolve(options.directory)));
  });
  let held = 0;
  yield* Stream.runForEach(records(inventoryDirectory), (record) =>
    Effect.gen(function* () {
      const result = yield* io(() =>
        options.client.backup({
          objectId: record.id,
          snapshotId: scope.snapshotId,
          command: { _tag: "Hold" },
        }),
      );
      if (result !== "held") return yield* invalid();
      held += 1;
    }),
  );
  if (held !== copiedInventory.objects) return yield* invalid();
  // All listed DO writers have drained before any R2 copy or object export starts.
  const r2 = yield* io(() => copyR2Backup(options.sourceBucket, options.destinationBucket));
  const resources: Inventory.Resources = {
    namespaceInventorySha256: copiedInventory.inventorySha256,
    deploymentSha256: scope.deploymentSha256,
    r2: {
      objects: r2.objects,
      bytes: r2.bytes,
      contentInventorySha256: r2.contentInventorySha256,
      metadataInventorySha256: r2.metadataInventorySha256,
    },
  };
  const indexDigest = createHash("sha256");
  let objects = 0;
  yield* Effect.acquireUseRelease(
    io(() => fs.open(path.join(options.directory, "index.ndjson"), "wx", 0o600)),
    (index) =>
      Stream.runForEach(records(inventoryDirectory), (record) =>
        Effect.gen(function* () {
          const archive = yield* captureObjectArchive({
            directory: path.join(objectsDirectory, record.id),
            client: options.client,
            key: options.key,
            resources,
            plan: {
              installationId: scope.installationId,
              snapshotId: scope.snapshotId,
              objectId: record.id,
              signer,
            },
          });
          const line =
            JSON.stringify({
              objectId: record.id,
              hasStoredData: record.hasStoredData,
              inventory: archive.inventory,
              sealSha256: createHash("sha256").update(archive.seal).digest("hex"),
            }) + "\n";
          yield* io(() => index.writeFile(line));
          indexDigest.update(line);
          objects += 1;
        }),
      ).pipe(Effect.tap(() => io(() => index.sync()))),
    (index) => io(() => index.close()).pipe(Effect.orDie),
  );
  if (objects !== copiedInventory.objects) return yield* invalid();
  const checkedInventory = yield* io(() =>
    verifyCloudflareObjectInventory(inventoryDirectory, scope.account, scope.namespace),
  );
  if (checkedInventory.inventorySha256 !== copiedInventory.inventorySha256) return yield* invalid();
  const completion: Completion = {
    version: 1,
    scope,
    objects,
    objectsSha256: indexDigest.digest("hex"),
    resources,
  };
  const packet = yield* signBackupFrame(
    options.key,
    {
      installationId: scope.installationId,
      snapshotId: scope.snapshotId,
      objectId: "__namespace__",
      kind: "inventory",
      sequence: objects,
    },
    encodeBackupFrame(completion),
  );
  yield* io(async () => {
    await syncedDirectory(objectsDirectory);
    await syncedDirectory(options.directory);
    const output = await fs.open(path.join(options.directory, "complete.packet"), "wx", 0o600);
    try {
      await output.writeFile(packet);
      await output.sync();
    } finally {
      await output.close();
    }
    await syncedDirectory(options.directory);
    await syncedDirectory(path.dirname(path.resolve(options.directory)));
  });
  return completion;
});

export interface Verify {
  readonly directory: string;
  readonly scope: Scope;
  readonly signer: Fingerprint;
}
/** Verifies only the pre-hold source set; never establishes recoverability. */
export const verifyCaptureIntent = Effect.fn("Backup.verifyCaptureIntent")(function* (
  options: Verify,
) {
  const scope = yield* Schema.decodeEffect(Scope)(options.scope).pipe(Effect.mapError(invalid));
  const manifest = yield* io(() =>
    verifyCloudflareObjectInventory(
      path.join(options.directory, "namespace"),
      scope.account,
      scope.namespace,
    ),
  );
  const packet = yield* io(() =>
    readBackupFile(path.join(options.directory, "capture-intent.packet"), packetBytes),
  );
  const payload = yield* verifyBackupFrame(packet, intentPosition(scope), options.signer);
  const intent = yield* Effect.try({
    try: () => decodeBackupFrame(payload, CaptureIntent),
    catch: invalid,
  });
  if (
    encodeBackupFrame(intent.scope) !== encodeBackupFrame(scope) ||
    intent.objects !== manifest.objects ||
    intent.inventorySha256 !== manifest.inventorySha256
  )
    return yield* invalid();
  return intent;
});

const IndexEntry = Schema.Struct({
  objectId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  hasStoredData: Schema.Boolean,
  inventory: Inventory.State,
  sealSha256: Inventory.Resources.fields.deploymentSha256,
});

/** Authenticate all local namespace/object files. R2 contents still require independent readback verification. */
export const verifyNamespaceArchive = Effect.fn("Backup.verifyNamespaceArchive")(function* (
  options: Verify,
) {
  const scope = yield* Schema.decodeEffect(Scope)(options.scope).pipe(Effect.mapError(invalid));
  const inventoryDirectory = path.join(options.directory, "namespace");
  const manifest = yield* io(() =>
    verifyCloudflareObjectInventory(inventoryDirectory, scope.account, scope.namespace),
  );
  const packet = yield* io(() =>
    readBackupFile(path.join(options.directory, "complete.packet"), packetBytes),
  );
  const payload = yield* verifyBackupFrame(
    packet,
    {
      installationId: scope.installationId,
      snapshotId: scope.snapshotId,
      objectId: "__namespace__",
      kind: "inventory",
      sequence: manifest.objects,
    },
    options.signer,
  );
  const completion = yield* Effect.try({
    try: () => decodeBackupFrame(payload, Completion),
    catch: invalid,
  });
  if (
    encodeBackupFrame(completion.scope) !== encodeBackupFrame(scope) ||
    completion.objects !== manifest.objects ||
    completion.resources.namespaceInventorySha256 !== manifest.inventorySha256 ||
    completion.resources.deploymentSha256 !== scope.deploymentSha256
  )
    return yield* invalid();
  const indexDigest = createHash("sha256");
  let objects = 0;
  yield* Effect.acquireUseRelease(
    Effect.sync(() => readCloudflareObjectInventory(inventoryDirectory)),
    (rows) =>
      Stream.runForEach(
        Stream.fromAsyncIterable(
          readBackupLines(path.join(options.directory, "index.ndjson"), 4096),
          invalid,
        ),
        (line) =>
          Effect.gen(function* () {
            const entry = yield* Schema.decodeEffect(Schema.fromJsonString(IndexEntry))(line).pipe(
              Effect.mapError(invalid),
            );
            const expected = yield* io(() => rows.next());
            if (
              expected.done ||
              expected.value.id !== entry.objectId ||
              expected.value.hasStoredData !== entry.hasStoredData
            )
              return yield* invalid();
            indexDigest.update(line + "\n");
            const archive = yield* verifyObjectArchive({
              directory: path.join(options.directory, "objects", entry.objectId),
              resources: completion.resources,
              plan: {
                installationId: scope.installationId,
                snapshotId: scope.snapshotId,
                objectId: entry.objectId,
                signer: options.signer,
              },
            });
            if (
              encodeBackupFrame(archive.inventory) !== encodeBackupFrame(entry.inventory) ||
              createHash("sha256").update(archive.seal).digest("hex") !== entry.sealSha256
            )
              return yield* invalid();
            objects += 1;
          }),
      ).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            if (!(yield* io(() => rows.next())).done) return yield* invalid();
          }),
        ),
      ),
    (rows) => io(() => rows.return()).pipe(Effect.orDie),
  );
  if (objects !== completion.objects || indexDigest.digest("hex") !== completion.objectsSha256)
    return yield* invalid();
  return completion;
});
