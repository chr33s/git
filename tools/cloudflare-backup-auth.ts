/** Authenticated pages, not a complete archive: the coordinator must verify the final inventory. */
import { Effect, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import * as Ssh from "../src/crypto/SshSignature.ts";
import { bytesToHex } from "../src/git/Format.ts";
import { frameBytes } from "./cloudflare-backup-codec.ts";

export const namespace = "chr33s-git/backup/v1";
const Id = Schema.NonEmptyString.check(Schema.isMaxLength(128));
export const Position = Schema.Struct({
  installationId: Id,
  snapshotId: Id,
  objectId: Id,
  kind: Schema.Literals(["sql-catalog", "sql-page", "kv-page", "inventory"]),
  sequence: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
    Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
});
export interface Position extends Schema.Schema.Type<typeof Position> {}
const Packet = Schema.Struct({
  version: Schema.Literal(1),
  position: Position,
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  signature: Schema.String.check(Schema.isMaxLength(8192)),
  payload: Schema.String,
});
const encoder = new TextEncoder();
export const packetBytes = frameBytes * 2 + 16 * 1024;
const invalid = () =>
  new Invalid({ field: "backup", reason: "backup frame authentication failed" });
const bounded = (value: string, limit: number) => {
  if (value.length > limit) throw invalid();
  const bytes = encoder.encode(value);
  if (bytes.byteLength > limit) throw invalid();
  return bytes;
};
// A fixed-order tuple avoids dependence on JSON object property order.
const preimage = (position: Position, sha256: string) =>
  encoder.encode(
    JSON.stringify([
      namespace,
      1,
      position.installationId,
      position.snapshotId,
      position.objectId,
      position.kind,
      position.sequence,
      sha256,
    ]),
  );
const digest = (payload: string) =>
  Effect.tryPromise({
    try: async () =>
      bytesToHex(
        new Uint8Array(await crypto.subtle.digest("SHA-256", bounded(payload, frameBytes))),
      ),
    catch: invalid,
  });

export const signBackupFrame = Effect.fn("Backup.signFrame")(function* (
  key: Ssh.PrivateKey,
  position: Position,
  payload: string,
) {
  const checked = yield* Schema.decodeEffect(Position)(position).pipe(Effect.mapError(invalid));
  const sha256 = yield* digest(payload);
  const signature = yield* Ssh.sign(key, preimage(checked, sha256), namespace);
  return yield* Effect.try({
    try: () => {
      const packet = JSON.stringify({ version: 1, position: checked, sha256, signature, payload });
      bounded(packet, packetBytes);
      return packet;
    },
    catch: invalid,
  });
});

/** expected and trustedSigner must come from the operator's trusted restore plan, not the packet. */
export const verifyBackupFrame = Effect.fn("Backup.verifyFrame")(function* (
  packet: string,
  expected: Position,
  trustedSigner: Ssh.Fingerprint,
) {
  yield* Effect.try({ try: () => bounded(packet, packetBytes), catch: invalid });
  const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(Packet))(packet).pipe(
    Effect.mapError(invalid),
  );
  const position = yield* Schema.decodeEffect(Position)(expected).pipe(Effect.mapError(invalid));
  const sha256 = yield* digest(decoded.payload);
  const expectedBytes = preimage(position, sha256);
  if (
    decoded.sha256 !== sha256 ||
    new TextDecoder().decode(preimage(decoded.position, sha256)) !==
      new TextDecoder().decode(expectedBytes)
  )
    return yield* invalid();
  const signer = yield* Ssh.verify(decoded.signature, expectedBytes, namespace).pipe(
    Effect.mapError(invalid),
  );
  if (signer === null || (yield* Ssh.fingerprint(signer)) !== trustedSigner)
    return yield* invalid();
  return decoded.payload;
});
