import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import { concatBytes } from "../git/Format.ts";
import {
  decodeArmored,
  encodeArmored,
  fingerprint,
  formatPublicKey,
  generate,
  isFingerprint,
  NAMESPACE,
  parsePrivateKey,
  parsePublicKey,
  type PrivateKey,
  sign,
  verify,
} from "./SshSignature.ts";

const encoder = new TextEncoder();

const expectSuccess = <A, E>(result: Result.Result<A, E>): A => {
  if (Result.isFailure(result)) {
    throw new Error(`expected success, got failure: ${JSON.stringify(result.failure)}`);
  }
  return result.success;
};

const expectFailure = <A, E>(result: Result.Result<A, E>): E => {
  if (Result.isSuccess(result)) {
    throw new Error(`expected failure, got success: ${JSON.stringify(result.success)}`);
  }
  return result.failure;
};

const hexToBytes = (hex: string): Uint8Array => {
  const out = new Uint8Array(hex.length / 2);
  for (let index = 0; index < out.length; index++) {
    out[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return out;
};

const string = (payload: Uint8Array): Uint8Array => {
  const out = new Uint8Array(4 + payload.length);
  new DataView(out.buffer).setUint32(0, payload.length);
  out.set(payload, 4);
  return out;
};

const text = (value: string): Uint8Array => string(encoder.encode(value));

const uint32 = (value: number): Uint8Array => {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, value);
  return out;
};

/**
 * RFC 8032 §7.1 TEST 1 — the most widely reproduced Ed25519 vector there is.
 *
 * It is here for one reason: it is the only *external* statement in this
 * suite about which public key a given seed belongs to. Everything else could
 * be self-consistent and wrong together; a signature made from this seed that
 * verifies under this point proves the PKCS#8 wrapper this module builds maps
 * the seed to the key the standard says it does.
 */
const RFC8032 = {
  seed: hexToBytes("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"),
  point: hexToBytes("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"),
};

/**
 * An OpenSSH private key file, assembled here rather than by the module under
 * test — a parser checked against bytes its own writer produced would agree
 * with itself about a format it had got wrong.
 */
const opensshPrivateKey = (seed: Uint8Array, point: Uint8Array, comment: string): string => {
  const publicBlob = concatBytes([text("ssh-ed25519"), string(point)]);
  const check = uint32(0x01020304);
  const unpadded = concatBytes([
    check,
    check,
    text("ssh-ed25519"),
    string(point),
    string(concatBytes([seed, point])),
    text(comment),
  ]);
  // The private section is padded to the cipher's block size with 1, 2, 3…
  const padding: number[] = [];
  for (let index = 1; (unpadded.length + padding.length) % 8 !== 0; index++) padding.push(index);

  const body = concatBytes([
    encoder.encode("openssh-key-v1\0"),
    text("none"),
    text("none"),
    text(""),
    uint32(1),
    string(publicBlob),
    string(concatBytes([unpadded, new Uint8Array(padding)])),
  ]);

  const base64 = Buffer.from(body).toString("base64");
  const lines = base64.match(/.{1,70}/g) ?? [];
  return `-----BEGIN OPENSSH PRIVATE KEY-----\n${lines.join("\n")}\n-----END OPENSSH PRIVATE KEY-----\n`;
};

const rfcKey = (): PrivateKey =>
  expectSuccess(parsePrivateKey(opensshPrivateKey(RFC8032.seed, RFC8032.point, "rfc8032@test")));

describe("SshSignature", () => {
  describe("public keys", () => {
    it("round-trips an authorized_keys line", async () => {
      const key = await Effect.runPromise(generate("alice@example.com"));
      const line = formatPublicKey(key.publicKey);
      assert.match(line, /^ssh-ed25519 [A-Za-z0-9+/=]+ alice@example\.com$/);

      const parsed = expectSuccess(parsePublicKey(line));
      assert.equal(parsed.algorithm, "ssh-ed25519");
      assert.equal(parsed.comment, "alice@example.com");
      assert.deepEqual(parsed.point, key.publicKey.point);
    });

    it("keeps a comment containing spaces", () => {
      const line = formatPublicKey(rfcKey().publicKey).replace("rfc8032@test", "my laptop key");
      assert.equal(expectSuccess(parsePublicKey(line)).comment, "my laptop key");
    });

    it("refuses a key whose body disagrees with its label", () => {
      const line = formatPublicKey(rfcKey().publicKey);
      const swapped = line.replace("ssh-ed25519", "sk-ssh-ed25519@openssh.com");
      assert.match(expectFailure(parsePublicKey(swapped)).reason, /line says/);
    });

    it("refuses key types this version cannot verify", () => {
      const failure = expectFailure(parsePublicKey("ssh-rsa AAAAB3NzaC1yc2E= bob"));
      assert.match(failure.reason, /unsupported key type 'ssh-rsa'/);
    });

    it("refuses a body that is not base64", () => {
      assert.match(expectFailure(parsePublicKey("ssh-ed25519 not!base64")).reason, /base64/);
    });

    it("fingerprints in OpenSSH's spelling", async () => {
      const printed = await Effect.runPromise(fingerprint(rfcKey().publicKey));
      assert.ok(isFingerprint(printed), `not a fingerprint: ${printed}`);
      assert.ok(!printed.includes("="), "padding must be stripped");
      // Stable across runs: the same key must name the same subject forever,
      // or every membership record that points at it goes dangling.
      const again = await Effect.runPromise(
        fingerprint(expectSuccess(parsePublicKey(formatPublicKey(rfcKey().publicKey)))),
      );
      assert.equal(again, printed);
    });
  });

  describe("private keys", () => {
    it("reads an unencrypted OpenSSH key, comment and all", () => {
      const key = rfcKey();
      assert.deepEqual(key.seed, RFC8032.seed);
      assert.deepEqual(key.publicKey.point, RFC8032.point);
      assert.equal(key.publicKey.comment, "rfc8032@test");
    });

    it("says so when the key is passphrase-protected", () => {
      const key = opensshPrivateKey(RFC8032.seed, RFC8032.point, "x");
      const encrypted = key.replace(
        Buffer.from(concatBytes([text("none"), text("none")]))
          .toString("base64")
          .slice(0, 8),
        Buffer.from(concatBytes([text("aes2"), text("bcry")]))
          .toString("base64")
          .slice(0, 8),
      );
      const failure = expectFailure(parsePrivateKey(encrypted));
      assert.match(failure.reason, /passphrase|encrypted|openssh-key-v1/);
    });

    it("refuses armor it does not recognise", () => {
      assert.match(expectFailure(parsePrivateKey("not a key")).reason, /armor/);
    });
  });

  describe("signing", () => {
    /**
     * The external check. `sign` derives its signing key from the seed alone;
     * `verify` checks against the point alone. They only agree if the PKCS#8
     * wrapper maps RFC 8032's seed to RFC 8032's public key.
     */
    it("signs under the key the standard pairs with the seed", async () => {
      const key = rfcKey();
      const message = encoder.encode("the quick brown fox");
      const armored = await Effect.runPromise(sign(key, message, NAMESPACE));

      const signer = await Effect.runPromise(verify(armored, message, NAMESPACE));
      assert.notEqual(signer, null, "the RFC 8032 seed must verify under the RFC 8032 point");
      assert.deepEqual(signer?.point, RFC8032.point);
    });

    it("writes armor with the header, footer and 70-column body", async () => {
      const key = await Effect.runPromise(generate("t"));
      const armored = await Effect.runPromise(sign(key, encoder.encode("x"), NAMESPACE));

      const lines = armored.trimEnd().split("\n");
      assert.equal(lines.at(0), "-----BEGIN SSH SIGNATURE-----");
      assert.equal(lines.at(-1), "-----END SSH SIGNATURE-----");
      for (const line of lines.slice(1, -1)) assert.ok(line.length <= 70, `long line: ${line}`);

      const decoded = expectSuccess(decodeArmored(armored));
      assert.equal(decoded.namespace, NAMESPACE);
      assert.equal(decoded.hashAlgorithm, "sha512");
      assert.equal(encodeArmored(decoded), armored);
    });

    it("rejects a message that changed after signing", async () => {
      const key = await Effect.runPromise(generate("t"));
      const armored = await Effect.runPromise(sign(key, encoder.encode("pay alice 1"), NAMESPACE));

      const signer = await Effect.runPromise(
        verify(armored, encoder.encode("pay alice 2"), NAMESPACE),
      );
      assert.equal(signer, null);
    });

    it("rejects a signature made for another namespace", async () => {
      const key = await Effect.runPromise(generate("t"));
      const message = encoder.encode("grant bob push");
      const armored = await Effect.runPromise(sign(key, message, "other-application"));

      const failure = await Effect.runPromise(
        verify(armored, message, NAMESPACE).pipe(Effect.flip),
      );
      assert.match(failure.reason, /namespace/);
    });

    it("rejects a signature whose body was swapped for another key's", async () => {
      const [alice, bob] = await Effect.runPromise(
        Effect.all([generate("alice"), generate("bob")]),
      );
      const message = encoder.encode("approve sha1:abc");
      const signed = expectSuccess(
        decodeArmored(await Effect.runPromise(sign(alice, message, NAMESPACE))),
      );

      // Alice's signature, presented as Bob's: the point verifies nothing.
      const forged = encodeArmored({ ...signed, publicKey: bob.publicKey });
      assert.equal(await Effect.runPromise(verify(forged, message, NAMESPACE)), null);
    });

    it("reports malformed armor as a failure, not as a bad signature", async () => {
      const failure = await Effect.runPromise(
        verify(
          "-----BEGIN SSH SIGNATURE-----\nnope\n-----END SSH SIGNATURE-----",
          new Uint8Array(),
          NAMESPACE,
        ).pipe(Effect.flip),
      );
      assert.equal(failure._tag, "Invalid");
    });

    it("rejects a truncated signature body", async () => {
      const key = await Effect.runPromise(generate("t"));
      const armored = await Effect.runPromise(sign(key, encoder.encode("x"), NAMESPACE));
      const signed = expectSuccess(decodeArmored(armored));

      const short = encodeArmored({ ...signed, signature: signed.signature.subarray(0, 20) });
      const failure = await Effect.runPromise(
        verify(short, encoder.encode("x"), NAMESPACE).pipe(Effect.flip),
      );
      assert.equal(failure._tag, "Invalid");
    });
  });
});
