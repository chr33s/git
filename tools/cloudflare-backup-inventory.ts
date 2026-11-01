/** Constant-space ordered frame inventory. Persist updates atomically with imported pages. */
import { Effect, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import { bytesToHex } from "../src/git/Format.ts";
import type { Fingerprint, PrivateKey } from "../src/crypto/SshSignature.ts";
import { Position, signBackupFrame, verifyBackupFrame } from "./cloudflare-backup-auth.ts";
import { decodeBackupFrame, encodeBackupFrame } from "./cloudflare-backup-codec.ts";

const Count = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const Identity = Schema.Struct({
  installationId: Position.fields.installationId,
  snapshotId: Position.fields.snapshotId,
});
export const State = Schema.Struct({
  ...Identity.fields,
  frames: Count,
  bytes: Count,
  root: Hash,
});
export interface State extends Schema.Schema.Type<typeof State> {}
export const Resources = Schema.Struct({
  namespaceInventorySha256: Hash,
  deploymentSha256: Hash,
  r2: Schema.Struct({
    objects: Count,
    bytes: Count,
    contentInventorySha256: Hash,
    metadataInventorySha256: Hash,
  }),
});
export interface Resources extends Schema.Schema.Type<typeof Resources> {}
const Summary = Schema.Struct({ version: Schema.Literal(1), state: State, resources: Resources });
const domain = "chr33s-git/backup-inventory/v1";
const encoder = new TextEncoder();
const invalid = () =>
  new Invalid({ field: "backup", reason: "backup inventory verification failed" });
const hash = (bytes: Uint8Array) =>
  Effect.tryPromise({
    try: async () =>
      bytesToHex(
        new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes).buffer)),
      ),
    catch: invalid,
  });
const finalPosition = (state: State): Position => ({
  installationId: state.installationId,
  snapshotId: state.snapshotId,
  objectId: "__inventory__",
  kind: "inventory",
  sequence: state.frames,
});
const identity = (state: State, resources: Resources) =>
  JSON.stringify([
    state.installationId,
    state.snapshotId,
    state.frames,
    state.bytes,
    state.root,
    resources.namespaceInventorySha256,
    resources.deploymentSha256,
    resources.r2.objects,
    resources.r2.bytes,
    resources.r2.contentInventorySha256,
    resources.r2.metadataInventorySha256,
  ]);

export const startInventory = Effect.fn("Backup.startInventory")(function* (
  installationId: string,
  snapshotId: string,
) {
  const identity = yield* Schema.decodeEffect(Identity)({ installationId, snapshotId }).pipe(
    Effect.mapError(invalid),
  );
  const root = yield* hash(
    encoder.encode(JSON.stringify([domain, "start", identity.installationId, identity.snapshotId])),
  );
  return yield* Schema.decodeEffect(State)({
    installationId,
    snapshotId,
    frames: 0,
    bytes: 0,
    root,
  }).pipe(Effect.mapError(invalid));
});
const advance = Effect.fn("Backup.advanceInventory")(function* (
  state: State,
  position: Position,
  payload: string,
) {
  const checked = yield* Schema.decodeEffect(State)(state).pipe(Effect.mapError(invalid));
  if (
    position.installationId !== checked.installationId ||
    position.snapshotId !== checked.snapshotId ||
    position.kind === "inventory"
  )
    return yield* invalid();
  const bytes = encoder.encode(payload);
  const digest = yield* hash(bytes);
  const root = yield* hash(
    encoder.encode(
      JSON.stringify([
        domain,
        "frame",
        checked.root,
        checked.frames,
        position.objectId,
        position.kind,
        position.sequence,
        bytes.byteLength,
        digest,
      ]),
    ),
  );
  return yield* Schema.decodeEffect(State)({
    ...checked,
    root,
    frames: checked.frames + 1,
    bytes: checked.bytes + bytes.byteLength,
  }).pipe(Effect.mapError(invalid));
});
export const captureInventoryFrame = Effect.fn("Backup.captureInventoryFrame")(function* (
  key: PrivateKey,
  state: State,
  position: Position,
  payload: string,
) {
  const packet = yield* signBackupFrame(key, position, payload);
  return { packet, state: yield* advance(state, position, payload) };
});
export const restoreInventoryFrame = Effect.fn("Backup.restoreInventoryFrame")(function* (
  packet: string,
  state: State,
  expected: Position,
  trustedSigner: Fingerprint,
) {
  const payload = yield* verifyBackupFrame(packet, expected, trustedSigner);
  return { payload, state: yield* advance(state, expected, payload) };
});

export const sealInventory = Effect.fn("Backup.sealInventory")(function* (
  key: PrivateKey,
  state: State,
  resources: Resources,
) {
  const summary = yield* Schema.decodeEffect(Summary)({ version: 1, state, resources }).pipe(
    Effect.mapError(invalid),
  );
  const payload = yield* Effect.try({ try: () => encodeBackupFrame(summary), catch: invalid });
  return yield* signBackupFrame(key, finalPosition(summary.state), payload);
});
/** Resources must describe independently verified inventory/deployment/R2 data, not copied manifest claims. */
export const verifyInventory = Effect.fn("Backup.verifyInventory")(function* (
  packet: string,
  state: State,
  resources: Resources,
  trustedSigner: Fingerprint,
) {
  const expected = yield* Schema.decodeEffect(Summary)({ version: 1, state, resources }).pipe(
    Effect.mapError(invalid),
  );
  const payload = yield* verifyBackupFrame(packet, finalPosition(expected.state), trustedSigner);
  const actual = yield* Effect.try({
    try: () => decodeBackupFrame(payload, Summary),
    catch: invalid,
  });
  if (identity(actual.state, actual.resources) !== identity(expected.state, expected.resources))
    return yield* invalid();
  return actual;
});
