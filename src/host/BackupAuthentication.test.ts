import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Result, Schema } from "effect";
import * as Ssh from "../crypto/SshSignature.ts";
import {
  Position,
  namespace,
  signBackupFrame,
  verifyBackupFrame,
} from "../../tools/cloudflare-backup-auth.ts";
import { encodeBackupFrame, frameBytes } from "../../tools/cloudflare-backup-codec.ts";

const position: Position = {
  installationId: "installation-one",
  snapshotId: "snapshot-one",
  objectId: "object-one",
  kind: "sql-catalog",
  sequence: 0,
};
const Packet = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Int,
    position: Position,
    sha256: Schema.String,
    signature: Schema.String,
    payload: Schema.String,
  }),
);
const payload = encodeBackupFrame({ sql: "CREATE TABLE private_table (secret TEXT)" });

it.effect("authenticates backup contents and every expected restore coordinate before use", () =>
  Effect.gen(function* () {
    const key = yield* Ssh.generate("backup-fixture");
    const trusted = yield* Ssh.fingerprint(key.publicKey);
    const signed = yield* signBackupFrame(key, position, payload);
    assert.equal(yield* verifyBackupFrame(signed, position, trusted), payload);
    const decoded = yield* Schema.decodeEffect(Packet)(signed);
    const variants = [
      { ...decoded, version: 2 },
      { ...decoded, payload: `${payload}private-tampering` },
      { ...decoded, sha256: "0".repeat(64) },
      { ...decoded, signature: "private-invalid-signature" },
      ...[
        { ...position, installationId: "other" },
        { ...position, snapshotId: "other" },
        { ...position, objectId: "other" },
        { ...position, kind: "kv-page" },
        { ...position, sequence: 1 },
      ].map((changed) => ({ ...decoded, position: changed })),
    ];
    for (const packet of variants) {
      let used = false;
      const result = yield* verifyBackupFrame(JSON.stringify(packet), position, trusted).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            used = true;
          }),
        ),
        Effect.result,
      );
      assert.ok(Result.isFailure(result));
      assert.equal(used, false);
      assert.equal(result.failure.reason, "backup frame authentication failed");
    }
    // An authentic packet from the same archive is not valid at another position.
    const replay = yield* verifyBackupFrame(signed, { ...position, sequence: 1 }, trusted).pipe(
      Effect.result,
    );
    assert.ok(Result.isFailure(replay));
  }),
);

it.effect("requires an independently trusted signing key and the backup SSHSIG namespace", () =>
  Effect.gen(function* () {
    const trustedKey = yield* Ssh.generate("backup-fixture");
    const outsider = yield* Ssh.generate("backup-fixture");
    const trusted = yield* Ssh.fingerprint(trustedKey.publicKey);
    const foreign = yield* signBackupFrame(outsider, position, payload);
    assert.ok(
      Result.isFailure(yield* verifyBackupFrame(foreign, position, trusted).pipe(Effect.result)),
    );
    const signed = yield* signBackupFrame(trustedKey, position, payload);
    const decoded = yield* Schema.decodeEffect(Packet)(signed);
    const hubSignature = yield* Ssh.sign(
      trustedKey,
      new TextEncoder().encode(payload),
      Ssh.NAMESPACE,
    );
    assert.notEqual(namespace, Ssh.NAMESPACE);
    assert.ok(
      Result.isFailure(
        yield* verifyBackupFrame(
          JSON.stringify({ ...decoded, signature: hubSignature }),
          position,
          trusted,
        ).pipe(Effect.result),
      ),
    );
  }),
);

it.effect("bounds payloads and malformed signed envelopes without disclosing private data", () =>
  Effect.gen(function* () {
    const key = yield* Ssh.generate("backup-fixture");
    const trusted = yield* Ssh.fingerprint(key.publicKey);
    for (const input of ["x".repeat(frameBytes + 1), "東京".repeat(frameBytes / 3)])
      assert.ok(Result.isFailure(yield* signBackupFrame(key, position, input).pipe(Effect.result)));
    for (const input of ["private-malformed-json", "x".repeat(frameBytes * 2 + 16385)]) {
      const result = yield* verifyBackupFrame(input, position, trusted).pipe(Effect.result);
      assert.ok(Result.isFailure(result));
      assert.equal(result.failure.reason, "backup frame authentication failed");
    }
  }),
);
