import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import { concatBytes } from "../git/Format.ts";
import {
  decodeArmored,
  encodeArmored,
  fingerprint,
  formatPublicKey,
  fromSeed,
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
const opensshPrivateKey = (
  seed: Uint8Array,
  point: Uint8Array,
  comment: string,
  /** What the outer blob advertises, when a test wants it to disagree. */
  advertised: Uint8Array = point,
  /** The point repeated inside the 64-byte key material, likewise. */
  paired: Uint8Array = point,
): string => {
  const publicBlob = concatBytes([text("ssh-ed25519"), string(advertised)]);
  const check = uint32(0x01020304);
  const unpadded = concatBytes([
    check,
    check,
    text("ssh-ed25519"),
    string(point),
    string(concatBytes([seed, paired])),
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
  it.effect("takes a 32-byte seed however wide the PKCS#8 export is", () =>
    Effect.promise(async () => {
      // Neither end of the buffer is the answer. "Everything after the fixed
      // prefix" is right only for the 48-byte v1 export; a v2 one carries the
      // public key too, leaving a seed wider than 32 bytes that WebCrypto then
      // refuses as a defect nothing can catch. "The last 32 bytes" is worse: in
      // that encoding the trailing bytes are the *public* point, and signing
      // would produce signatures no verifier accepts. The field's own header is
      // what is stable, so that is what is looked for.
      const key = await Effect.runPromise(generate("wide@example.com"));
      assert.equal(key.seed.length, 32);

      const bytes = new TextEncoder().encode("something to sign");
      const armored = await Effect.runPromise(sign(key, bytes, NAMESPACE));
      // The round trip is the assertion that matters: a seed read from the wrong
      // place still signs, and the signature verifies against nothing.
      const back = await Effect.runPromise(verify(armored, bytes, NAMESPACE));
      assert.notEqual(back, null, "a key that signs must verify");
      assert.deepEqual([...(back?.point ?? [])], [...key.publicKey.point]);
    }),
  );

  describe("public keys", () => {
    it.effect("round-trips an authorized_keys line", () =>
      Effect.promise(async () => {
        const key = await Effect.runPromise(generate("alice@example.com"));
        const line = formatPublicKey(key.publicKey);
        assert.match(line, /^ssh-ed25519 [A-Za-z0-9+/=]+ alice@example\.com$/);

        const parsed = expectSuccess(parsePublicKey(line));
        assert.equal(parsed.algorithm, "ssh-ed25519");
        assert.equal(parsed.comment, "alice@example.com");
        assert.deepEqual(parsed.point, key.publicKey.point);
      }),
    );

    it.effect("keeps a comment containing spaces", () =>
      Effect.sync(() => {
        const line = formatPublicKey(rfcKey().publicKey).replace("rfc8032@test", "my laptop key");
        assert.equal(expectSuccess(parsePublicKey(line)).comment, "my laptop key");
      }),
    );

    it.effect("refuses a key whose body disagrees with its label", () =>
      Effect.sync(() => {
        const line = formatPublicKey(rfcKey().publicKey);
        const swapped = line.replace("ssh-ed25519", "sk-ssh-ed25519@openssh.com");
        assert.match(expectFailure(parsePublicKey(swapped)).reason, /line says/);
      }),
    );

    it.effect("refuses key types this version cannot verify", () =>
      Effect.sync(() => {
        const failure = expectFailure(parsePublicKey("ssh-rsa AAAAB3NzaC1yc2E= bob"));
        assert.match(failure.reason, /unsupported key type 'ssh-rsa'/);
      }),
    );

    it.effect("refuses a body that is not base64", () =>
      Effect.sync(() => {
        assert.match(expectFailure(parsePublicKey("ssh-ed25519 not!base64")).reason, /base64/);
      }),
    );

    it.effect("fingerprints in OpenSSH's spelling", () =>
      Effect.promise(async () => {
        const printed = await Effect.runPromise(fingerprint(rfcKey().publicKey));
        assert.ok(isFingerprint(printed), `not a fingerprint: ${printed}`);
        assert.ok(!printed.includes("="), "padding must be stripped");
        // Stable across runs: the same key must name the same subject forever,
        // or every membership record that points at it goes dangling.
        const again = await Effect.runPromise(
          fingerprint(expectSuccess(parsePublicKey(formatPublicKey(rfcKey().publicKey)))),
        );
        assert.equal(again, printed);
      }),
    );
  });

  describe("private keys", () => {
    it.effect("reads an unencrypted OpenSSH key, comment and all", () =>
      Effect.sync(() => {
        const key = rfcKey();
        assert.deepEqual(key.seed, RFC8032.seed);
        assert.deepEqual(key.publicKey.point, RFC8032.point);
        assert.equal(key.publicKey.comment, "rfc8032@test");
      }),
    );

    it.effect("refuses a key whose halves disagree about the public point", () =>
      Effect.sync(() => {
        // The private section repeats the type and the point, and the 64-byte
        // key material repeats the point again. Read and discarded, a key whose
        // halves disagree loaded happily and then signed with one seed while
        // advertising another key: every signature verified nowhere, and the
        // failure surfaced as "the grant did not take effect", pointing at the
        // trust log rather than at the key file. The key *type* was already held
        // to exactly this rule one field earlier.
        const other = new Uint8Array(RFC8032.point);
        other[0] = other[0]! ^ 0xff;

        const advertised = parsePrivateKey(
          opensshPrivateKey(RFC8032.seed, RFC8032.point, "mismatch@test", other),
        );
        const paired = parsePrivateKey(
          opensshPrivateKey(RFC8032.seed, RFC8032.point, "mismatch@test", RFC8032.point, other),
        );

        assert.match(expectFailure(advertised).reason, /disagrees about the public key/);
        assert.match(expectFailure(paired).reason, /does not match the public key/);
      }),
    );

    it.effect("says so when the key is passphrase-protected", () =>
      Effect.sync(() => {
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
      }),
    );

    it.effect("refuses armor it does not recognise", () =>
      Effect.sync(() => {
        assert.match(expectFailure(parsePrivateKey("not a key")).reason, /armor/);
      }),
    );
  });

  describe("signing", () => {
    /**
     * The external check. `sign` derives its signing key from the seed alone;
     * `verify` checks against the point alone. They only agree if the PKCS#8
     * wrapper maps RFC 8032's seed to RFC 8032's public key.
     */
    it.effect("signs under the key the standard pairs with the seed", () =>
      Effect.promise(async () => {
        const key = rfcKey();
        const message = encoder.encode("the quick brown fox");
        const armored = await Effect.runPromise(sign(key, message, NAMESPACE));

        const signer = await Effect.runPromise(verify(armored, message, NAMESPACE));
        assert.notEqual(signer, null, "the RFC 8032 seed must verify under the RFC 8032 point");
        assert.deepEqual(signer?.point, RFC8032.point);
      }),
    );

    it.effect("writes armor with the header, footer and 70-column body", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("rejects a message that changed after signing", () =>
      Effect.promise(async () => {
        const key = await Effect.runPromise(generate("t"));
        const armored = await Effect.runPromise(
          sign(key, encoder.encode("pay alice 1"), NAMESPACE),
        );

        const signer = await Effect.runPromise(
          verify(armored, encoder.encode("pay alice 2"), NAMESPACE),
        );
        assert.equal(signer, null);
      }),
    );

    it.effect("rejects a signature made for another namespace", () =>
      Effect.promise(async () => {
        const key = await Effect.runPromise(generate("t"));
        const message = encoder.encode("grant bob push");
        const armored = await Effect.runPromise(sign(key, message, "other-application"));

        const failure = await Effect.runPromise(
          verify(armored, message, NAMESPACE).pipe(Effect.flip),
        );
        assert.match(failure.reason, /namespace/);
      }),
    );

    it.effect("rejects a signature whose body was swapped for another key's", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("reports malformed armor as a failure, not as a bad signature", () =>
      Effect.promise(async () => {
        const failure = await Effect.runPromise(
          verify(
            "-----BEGIN SSH SIGNATURE-----\nnope\n-----END SSH SIGNATURE-----",
            new Uint8Array(),
            NAMESPACE,
          ).pipe(Effect.flip),
        );
        assert.equal(failure._tag, "Invalid");
      }),
    );

    it.effect("rejects a truncated signature body", () =>
      Effect.promise(async () => {
        const key = await Effect.runPromise(generate("t"));
        const armored = await Effect.runPromise(sign(key, encoder.encode("x"), NAMESPACE));
        const signed = expectSuccess(decodeArmored(armored));

        const short = encodeArmored({ ...signed, signature: signed.signature.subarray(0, 20) });
        const failure = await Effect.runPromise(
          verify(short, encoder.encode("x"), NAMESPACE).pipe(Effect.flip),
        );
        assert.equal(failure._tag, "Invalid");
      }),
    );
  });

  describe("derivation", () => {
    it.effect("re-derives exactly the public key the seed was generated with", () =>
      Effect.promise(async () => {
        // The property key storage repair rests on: the seed alone determines
        // the public half, so two halves that disagree can always be settled
        // in the seed's favour — and a signature made with the derived key
        // verifies, which is the whole point of the repair.
        const original = await Effect.runPromise(generate("original@example.com"));
        const derived = await Effect.runPromise(fromSeed(original.seed, "rebuilt"));
        assert.deepEqual([...derived.publicKey.point], [...original.publicKey.point]);
        assert.equal(
          await Effect.runPromise(fingerprint(derived.publicKey)),
          await Effect.runPromise(fingerprint(original.publicKey)),
        );

        const armored = await Effect.runPromise(sign(derived, encoder.encode("m"), NAMESPACE));
        const verified = await Effect.runPromise(verify(armored, encoder.encode("m"), NAMESPACE));
        assert.notEqual(verified, null);

        // And a *different* seed derives a different key — the mismatch the
        // storage check exists to catch, not to paper over.
        const other = await Effect.runPromise(generate("other@example.com"));
        const stranger = await Effect.runPromise(fromSeed(other.seed, "rebuilt"));
        assert.notDeepEqual([...stranger.publicKey.point], [...original.publicKey.point]);
      }),
    );

    it.effect("refuses a seed that is not thirty-two bytes", () =>
      Effect.promise(async () => {
        const failure = await Effect.runPromise(
          fromSeed(new Uint8Array(16), "short").pipe(Effect.flip),
        );
        assert.equal(failure._tag, "Invalid");
      }),
    );
  });
});
