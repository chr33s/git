/**
 * SSH signatures — the one cryptographic primitive the hub trusts.
 *
 * This is OpenSSH's `SSHSIG` format, the same bytes `ssh-keygen -Y sign`
 * writes and `ssh-keygen -Y verify` reads, so a signature produced here is
 * verifiable by tooling that has never heard of this project, and a key a
 * user already has is a key that already works. `SshSignature.interop.test.ts`
 * checks both directions against the real binary.
 *
 * Pure below, effectful above, the same seam `git/Format.ts` draws: framing
 * and key parsing are synchronous and return `Result`; anything that reaches
 * for Web Crypto returns an `Effect`. No `node:*` — this runs in a Durable
 * Object, a browser tab and the CLI alike.
 *
 * Ed25519 only. `sk-ssh-ed25519@openssh.com` verifies (a hardware key's
 * signature is an Ed25519 signature over a longer preimage) but cannot sign,
 * because the private half is in the authenticator by construction.
 */
import { Effect, Result } from "effect";

import { Invalid } from "../git/Error.ts";
import { concatBytes } from "../git/Format.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const invalid = (field: string, reason: string) => Result.fail(new Invalid({ field, reason }));

/**
 * The signature namespace this project signs under.
 *
 * SSHSIG namespaces exist so a signature made for one application cannot be
 * replayed into another — a signature over a git commit is not a signature
 * over a membership grant, and `ssh-keygen -Y verify` enforces the namespace
 * it is given.
 */
export const NAMESPACE = "chr33s-git/hub/v1";

/** Key types this version accepts. `sk-` is verify-only. */
export type KeyAlgorithm = "ssh-ed25519" | "sk-ssh-ed25519@openssh.com";

const ED25519 = "ssh-ed25519";
const SK_ED25519 = "sk-ssh-ed25519@openssh.com";

const algorithmOf = (name: string): KeyAlgorithm | null =>
  name === ED25519 ? ED25519 : name === SK_ED25519 ? SK_ED25519 : null;

/**
 * A key fingerprint, in the form OpenSSH prints: `SHA256:` and unpadded
 * base64 of the SHA-256 of the public-key blob.
 *
 * Branded because it is the subject of every membership record and the name
 * of every signer, and a bare `string` in that position would accept a
 * comment, a URL or a public key just as happily.
 */
export type Fingerprint = string & { readonly Fingerprint: unique symbol };

export const isFingerprint = (value: string): value is Fingerprint =>
  /^SHA256:[A-Za-z0-9+/]{43}$/.test(value);

export interface PublicKey {
  readonly algorithm: KeyAlgorithm;
  /** The SSH wire blob verbatim: what the fingerprint is taken over. */
  readonly blob: Uint8Array;
  /** The Ed25519 point, thirty-two bytes. */
  readonly point: Uint8Array;
  /** A security key's application (`ssh:`); `null` for a software key. */
  readonly application: string | null;
  readonly comment: string;
}

export interface PrivateKey {
  readonly publicKey: PublicKey;
  /**
   * The Ed25519 seed, thirty-two bytes.
   *
   * Held in memory only. Nothing in this module serializes it, and nothing
   * above this module accepts it — signing takes the whole `PrivateKey` so
   * the seed has no separate life as a value that could be logged.
   */
  readonly seed: Uint8Array;
}

// -- SSH wire encoding (RFC 4251) --------------------------------------------

/**
 * One `string`: a big-endian uint32 length and that many bytes.
 *
 * Returns the payload and the offset after it, or `null` when the buffer is
 * too short — every caller is a parser that turns `null` into its own
 * message, which reads better than one shared "truncated" for six fields.
 */
const readString = (bytes: Uint8Array, offset: number): readonly [Uint8Array, number] | null => {
  if (offset + 4 > bytes.length) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const length = view.getUint32(offset);
  const start = offset + 4;
  const end = start + length;
  // `length` is unsigned and `end` cannot wrap in a double, so one bound check
  // is enough to know the slice is inside the buffer.
  if (end > bytes.length) return null;
  return [bytes.subarray(start, end), end];
};

const writeString = (payload: Uint8Array): Uint8Array => {
  const out = new Uint8Array(4 + payload.length);
  new DataView(out.buffer).setUint32(0, payload.length);
  out.set(payload, 4);
  return out;
};

const writeText = (text: string): Uint8Array => writeString(encoder.encode(text));

// -- base64 -------------------------------------------------------------------

/**
 * `btoa`/`atob` rather than a hand-rolled codec: both hosts and the browser
 * have them, and every payload here is a key or a signature — hundreds of
 * bytes, not a pack.
 */
const toBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

const fromBase64 = (text: string): Uint8Array | null => {
  try {
    // Armor arrives wrapped, and an `authorized_keys` line may carry stray
    // whitespace; `atob` accepts neither.
    const binary = atob(text.replace(/\s+/g, ""));
    const out = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index++) out[index] = binary.charCodeAt(index);
    return out;
  } catch {
    return null;
  }
};

// -- public keys ---------------------------------------------------------------

/**
 * The blob's own view of what it is, checked against the line's first field.
 *
 * A key whose blob disagrees with its label is not a key with a typo: the
 * label is what a reader sorts on and the blob is what gets verified, so
 * accepting the pair would let a `ssh-ed25519` line carry something else.
 */
const parseBlob = (blob: Uint8Array, comment: string): Result.Result<PublicKey, Invalid> => {
  const first = readString(blob, 0);
  if (first === null) return invalid("publicKey", "truncated key blob");
  const algorithm = algorithmOf(decoder.decode(first[0]));
  if (algorithm === null) {
    return invalid("publicKey", `unsupported key type '${decoder.decode(first[0])}'`);
  }

  const point = readString(blob, first[1]);
  if (point === null) return invalid("publicKey", "key blob missing its point");
  if (point[0].length !== 32) {
    return invalid("publicKey", `expected a 32-byte Ed25519 point, got ${point[0].length}`);
  }

  if (algorithm === ED25519) {
    if (point[1] !== blob.length) return invalid("publicKey", "trailing bytes after key blob");
    return Result.succeed({
      algorithm,
      blob,
      point: point[0],
      application: null,
      comment,
    });
  }

  const application = readString(blob, point[1]);
  if (application === null) return invalid("publicKey", "security key missing its application");
  if (application[1] !== blob.length) return invalid("publicKey", "trailing bytes after key blob");
  return Result.succeed({
    algorithm,
    blob,
    point: point[0],
    application: decoder.decode(application[0]),
    comment,
  });
};

/** One `authorized_keys` line: `<type> <base64 blob> [comment]`. */
export const parsePublicKey = (line: string): Result.Result<PublicKey, Invalid> => {
  const fields = line.trim().split(/\s+/);
  const [label, encoded] = fields;
  if (label === undefined || encoded === undefined) {
    return invalid("publicKey", "expected '<type> <base64> [comment]'");
  }
  if (algorithmOf(label) === null) {
    return invalid("publicKey", `unsupported key type '${label}'`);
  }

  const blob = fromBase64(encoded);
  if (blob === null) return invalid("publicKey", "key body is not base64");

  const parsed = parseBlob(blob, fields.slice(2).join(" "));
  if (Result.isFailure(parsed)) return parsed;
  if (parsed.success.algorithm !== label) {
    return invalid(
      "publicKey",
      `key body says '${parsed.success.algorithm}', line says '${label}'`,
    );
  }
  return parsed;
};

/** The `authorized_keys` spelling, comment included when there is one. */
export const formatPublicKey = (key: PublicKey): string => {
  const line = `${key.algorithm} ${toBase64(key.blob)}`;
  return key.comment === "" ? line : `${line} ${key.comment}`;
};

/**
 * `SHA256:<unpadded base64>` over the key blob — what OpenSSH prints and what
 * every membership record names a subject by.
 */
export const fingerprint = (key: PublicKey): Effect.Effect<Fingerprint> =>
  Effect.promise(async () => {
    const digest = await crypto.subtle.digest("SHA-256", key.blob.slice().buffer);
    // SAFETY: a SHA-256 digest is thirty-two bytes, whose base64 is
    // forty-four characters ending in one `=` — exactly the forty-three
    // characters `isFingerprint` names once the padding is dropped.
    return `SHA256:${toBase64(new Uint8Array(digest)).replace(/=+$/, "")}` as Fingerprint;
  });

// -- SSHSIG --------------------------------------------------------------------

const MAGIC = encoder.encode("SSHSIG");
const SIG_VERSION = 1;
const ARMOR_BEGIN = "-----BEGIN SSH SIGNATURE-----";
const ARMOR_END = "-----END SSH SIGNATURE-----";

export type HashAlgorithm = "sha256" | "sha512";

const hashNameOf = (name: string): HashAlgorithm | null =>
  name === "sha256" ? "sha256" : name === "sha512" ? "sha512" : null;

const webCryptoHash = (algorithm: HashAlgorithm): "SHA-256" | "SHA-512" =>
  algorithm === "sha256" ? "SHA-256" : "SHA-512";

export interface Signature {
  readonly publicKey: PublicKey;
  readonly namespace: string;
  readonly hashAlgorithm: HashAlgorithm;
  /** The algorithm-specific inner signature blob, as it appears on the wire. */
  readonly signature: Uint8Array;
}

/**
 * What the key actually signs.
 *
 * The message is hashed first and only the digest goes in, which is what lets
 * `ssh-keygen -Y sign` sign a file it never holds in memory whole.
 */
const signedData = (
  namespace: string,
  hashAlgorithm: HashAlgorithm,
  messageDigest: Uint8Array,
): Uint8Array =>
  concatBytes([
    MAGIC,
    writeText(namespace),
    writeText(""),
    writeText(hashAlgorithm),
    writeString(messageDigest),
  ]);

const encodeBlob = (signature: Signature): Uint8Array => {
  const version = new Uint8Array(4);
  new DataView(version.buffer).setUint32(0, SIG_VERSION);
  return concatBytes([
    MAGIC,
    version,
    writeString(signature.publicKey.blob),
    writeText(signature.namespace),
    writeText(""),
    writeText(signature.hashAlgorithm),
    writeString(signature.signature),
  ]);
};

const decodeBlob = (blob: Uint8Array): Result.Result<Signature, Invalid> => {
  if (blob.length < MAGIC.length + 4) return invalid("signature", "truncated signature");
  for (let index = 0; index < MAGIC.length; index++) {
    if (blob[index] !== MAGIC[index]) return invalid("signature", "not an SSHSIG signature");
  }

  const view = new DataView(blob.buffer, blob.byteOffset, blob.byteLength);
  const version = view.getUint32(MAGIC.length);
  if (version !== SIG_VERSION) {
    return invalid("signature", `unsupported signature version ${version}`);
  }

  const keyBlob = readString(blob, MAGIC.length + 4);
  if (keyBlob === null) return invalid("signature", "missing public key");
  const namespace = readString(blob, keyBlob[1]);
  if (namespace === null) return invalid("signature", "missing namespace");
  const reserved = readString(blob, namespace[1]);
  if (reserved === null) return invalid("signature", "missing reserved field");
  const hashName = readString(blob, reserved[1]);
  if (hashName === null) return invalid("signature", "missing hash algorithm");
  const inner = readString(blob, hashName[1]);
  if (inner === null) return invalid("signature", "missing signature body");
  if (inner[1] !== blob.length) return invalid("signature", "trailing bytes after signature");

  const hashAlgorithm = hashNameOf(decoder.decode(hashName[0]));
  if (hashAlgorithm === null) {
    return invalid("signature", `unsupported hash '${decoder.decode(hashName[0])}'`);
  }

  const publicKey = parseBlob(keyBlob[0], "");
  if (Result.isFailure(publicKey)) return Result.fail(publicKey.failure);

  return Result.succeed({
    publicKey: publicKey.success,
    namespace: decoder.decode(namespace[0]),
    hashAlgorithm,
    signature: inner[0],
  });
};

/** PEM-shaped armor, wrapped at seventy columns the way `ssh-keygen` wraps it. */
export const encodeArmored = (signature: Signature): string => {
  const body = toBase64(encodeBlob(signature));
  const lines: string[] = [];
  for (let index = 0; index < body.length; index += 70) lines.push(body.slice(index, index + 70));
  return `${ARMOR_BEGIN}\n${lines.join("\n")}\n${ARMOR_END}\n`;
};

export const decodeArmored = (armored: string): Result.Result<Signature, Invalid> => {
  const begin = armored.indexOf(ARMOR_BEGIN);
  const end = armored.indexOf(ARMOR_END);
  if (begin === -1 || end === -1 || end < begin) {
    return invalid("signature", "missing SSH SIGNATURE armor");
  }
  const body = fromBase64(armored.slice(begin + ARMOR_BEGIN.length, end));
  if (body === null) return invalid("signature", "signature body is not base64");
  return decodeBlob(body);
};

// -- verify / sign --------------------------------------------------------------

const digest = (algorithm: HashAlgorithm, message: Uint8Array): Promise<ArrayBuffer> =>
  crypto.subtle.digest(webCryptoHash(algorithm), message.slice().buffer);

/**
 * The Ed25519 point as a Web Crypto key.
 *
 * A point that is not on the curve is a key that cannot verify anything, and
 * `importKey` is where that is discovered — so this reports `null` rather
 * than failing, and the caller turns it into the same "did not verify" every
 * other bad signature gets.
 */
const importPoint = async (point: Uint8Array): Promise<CryptoKey | null> => {
  try {
    return await crypto.subtle.importKey("raw", point.slice().buffer, { name: "Ed25519" }, false, [
      "verify",
    ]);
  } catch {
    return null;
  }
};

/**
 * The inner signature blob: `string(algorithm) || string(64-byte signature)`,
 * plus the authenticator's flags and counter for a security key.
 */
interface InnerSignature {
  readonly raw: Uint8Array;
  readonly flags: number;
  readonly counter: number;
}

const decodeInner = (
  algorithm: KeyAlgorithm,
  inner: Uint8Array,
): Result.Result<InnerSignature, Invalid> => {
  const label = readString(inner, 0);
  if (label === null) return invalid("signature", "truncated signature body");
  if (decoder.decode(label[0]) !== algorithm) {
    return invalid(
      "signature",
      `signature is '${decoder.decode(label[0])}' but the key is '${algorithm}'`,
    );
  }

  const raw = readString(inner, label[1]);
  if (raw === null) return invalid("signature", "missing signature bytes");
  if (raw[0].length !== 64) {
    return invalid("signature", `expected 64 signature bytes, got ${raw[0].length}`);
  }

  if (algorithm === ED25519) {
    if (raw[1] !== inner.length) return invalid("signature", "trailing bytes in signature body");
    return Result.succeed({ raw: raw[0], flags: 0, counter: 0 });
  }

  // A security key adds the authenticator's own state, and it is signed:
  // dropping it here would verify a signature over data the token never saw.
  if (raw[1] + 5 !== inner.length) {
    return invalid("signature", "security key signature missing flags and counter");
  }
  const view = new DataView(inner.buffer, inner.byteOffset, inner.byteLength);
  return Result.succeed({
    raw: raw[0],
    flags: view.getUint8(raw[1]),
    counter: view.getUint32(raw[1] + 1),
  });
};

/**
 * The bytes the key's signature actually covers.
 *
 * A software key signs the SSHSIG blob. A security key signs the WebAuthn
 * preimage — `SHA256(application) || flags || counter || SHA256(blob)` —
 * because the token hashes what the host hands it and knows nothing about
 * SSHSIG.
 */
const preimage = async (
  key: PublicKey,
  inner: InnerSignature,
  data: Uint8Array,
): Promise<Uint8Array> => {
  if (key.algorithm === ED25519) return data;

  const application = new Uint8Array(
    await crypto.subtle.digest("SHA-256", encoder.encode(key.application ?? "").buffer),
  );
  const dataDigest = new Uint8Array(await crypto.subtle.digest("SHA-256", data.slice().buffer));
  const middle = new Uint8Array(5);
  const view = new DataView(middle.buffer);
  view.setUint8(0, inner.flags);
  view.setUint32(1, inner.counter);
  return concatBytes([application, middle, dataDigest]);
};

/**
 * Check a signature, and report which key made it.
 *
 * Three outcomes, deliberately: `Invalid` when the armor or its framing does
 * not parse — a caller has been handed something that is not a signature at
 * all — `null` when the bytes are a signature that does not verify, and the
 * key when it does. Returning the key is what makes the caller's next step
 * ("is this signer a member?") a lookup rather than a second parse.
 *
 * The namespace is an argument rather than a default: verifying against the
 * namespace the caller expected is the check, and a signature made for
 * another application must not pass here.
 */
export const verify = Effect.fn("SshSignature.verify")(function* (
  armored: string,
  message: Uint8Array,
  namespace: string,
) {
  const parsed = decodeArmored(armored);
  if (Result.isFailure(parsed)) return yield* parsed.failure;
  const signature = parsed.success;

  if (signature.namespace !== namespace) {
    return yield* new Invalid({
      field: "signature",
      reason: `signature namespace '${signature.namespace}' is not '${namespace}'`,
    });
  }

  const inner = decodeInner(signature.publicKey.algorithm, signature.signature);
  if (Result.isFailure(inner)) return yield* inner.failure;

  const verified = yield* Effect.promise(async () => {
    const key = await importPoint(signature.publicKey.point);
    if (key === null) return false;
    const messageDigest = new Uint8Array(await digest(signature.hashAlgorithm, message));
    const data = signedData(signature.namespace, signature.hashAlgorithm, messageDigest);
    const covered = await preimage(signature.publicKey, inner.success, data);
    return crypto.subtle.verify(
      { name: "Ed25519" },
      key,
      inner.success.raw.slice().buffer,
      covered.slice().buffer,
    );
  });

  return verified ? signature.publicKey : null;
});

/**
 * The fixed PKCS#8 wrapper for an Ed25519 seed.
 *
 * Web Crypto imports private keys as PKCS#8 and OpenSSH stores them as a bare
 * seed, and for Ed25519 the difference is exactly this sixteen-byte prefix:
 * SEQUENCE, version 0, AlgorithmIdentifier `1.3.101.112`, OCTET STRING.
 */
const PKCS8_ED25519_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
]);

/** An Ed25519 private key is a 32-byte seed, whatever wraps it. */
const SEED_BYTES = 32;

/**
 * The seed inside a PKCS#8 export, wherever the encoder put it.
 *
 * Neither end of the buffer is the answer. "Everything after the fixed
 * prefix" is right only for the 48-byte v1 export, and a v2 one — which
 * carries the public key as well, and which RFC 5958 permits — leaves a seed
 * wider than 32 bytes that WebCrypto then refuses, as a defect nothing can
 * catch. "The last 32 bytes" is worse: in that same v2 encoding the trailing
 * bytes are the *public* point, so signing would produce signatures no
 * verifier accepts.
 *
 * What is stable in both is the field itself: an OCTET STRING of length 0x22
 * whose contents are an OCTET STRING of length 0x20. That four-byte header is
 * what this looks for, and the 32 bytes after it are the seed.
 */
const seedOf = (pkcs8: Uint8Array): Uint8Array => {
  for (let at = 0; at + 4 + SEED_BYTES <= pkcs8.length; at++) {
    if (pkcs8[at] !== 0x04 || pkcs8[at + 1] !== 0x22) continue;
    if (pkcs8[at + 2] !== 0x04 || pkcs8[at + 3] !== 0x20) continue;
    return pkcs8.subarray(at + 4, at + 4 + SEED_BYTES);
  }
  // The fixed v1 layout, as the fallback rather than the assumption.
  return pkcs8.subarray(PKCS8_ED25519_PREFIX.length, PKCS8_ED25519_PREFIX.length + SEED_BYTES);
};

/**
 * Sign a message, producing the armor `ssh-keygen -Y verify` accepts.
 *
 * `sha512` because that is what `ssh-keygen` defaults to, and a signature
 * that differs from the default in a field nobody looks at is a signature
 * somebody will eventually fail to reproduce.
 */
export const sign = Effect.fn("SshSignature.sign")(function* (
  key: PrivateKey,
  message: Uint8Array,
  namespace: string,
) {
  const armored = yield* Effect.promise(async () => {
    const pkcs8 = concatBytes([PKCS8_ED25519_PREFIX, key.seed]);
    const signer = await crypto.subtle.importKey(
      "pkcs8",
      pkcs8.slice().buffer,
      { name: "Ed25519" },
      false,
      ["sign"],
    );
    const messageDigest = new Uint8Array(await digest("sha512", message));
    const data = signedData(namespace, "sha512", messageDigest);
    const raw = new Uint8Array(
      await crypto.subtle.sign({ name: "Ed25519" }, signer, data.slice().buffer),
    );
    return encodeArmored({
      publicKey: key.publicKey,
      namespace,
      hashAlgorithm: "sha512",
      signature: concatBytes([writeText(ED25519), writeString(raw)]),
    });
  });
  return armored;
});

// -- private keys ----------------------------------------------------------------

const PRIVATE_BEGIN = "-----BEGIN OPENSSH PRIVATE KEY-----";
const PRIVATE_END = "-----END OPENSSH PRIVATE KEY-----";
const PRIVATE_MAGIC = "openssh-key-v1\0";

/**
 * An unencrypted OpenSSH private key.
 *
 * Encrypted keys are refused rather than half-supported: the passphrase would
 * have to reach this module from somewhere, and every candidate for that
 * somewhere — a prompt below the domain, an environment variable, a config
 * file — is worse than telling the caller to use an agent. The message says
 * so, because "invalid key" would send someone looking for a corrupt file.
 */
export const parsePrivateKey = (pem: string): Result.Result<PrivateKey, Invalid> => {
  const begin = pem.indexOf(PRIVATE_BEGIN);
  const end = pem.indexOf(PRIVATE_END);
  if (begin === -1 || end === -1 || end < begin) {
    return invalid("privateKey", "missing OPENSSH PRIVATE KEY armor");
  }

  const body = fromBase64(pem.slice(begin + PRIVATE_BEGIN.length, end));
  if (body === null) return invalid("privateKey", "key body is not base64");

  const magic = encoder.encode(PRIVATE_MAGIC);
  if (body.length < magic.length) return invalid("privateKey", "truncated key");
  for (let index = 0; index < magic.length; index++) {
    if (body[index] !== magic[index]) return invalid("privateKey", "not an openssh-key-v1 key");
  }

  const cipher = readString(body, magic.length);
  if (cipher === null) return invalid("privateKey", "missing cipher name");
  if (decoder.decode(cipher[0]) !== "none") {
    return invalid(
      "privateKey",
      "the key is passphrase-protected; use an agent or decrypt it first",
    );
  }

  const kdf = readString(body, cipher[1]);
  if (kdf === null) return invalid("privateKey", "missing kdf name");
  const kdfOptions = readString(body, kdf[1]);
  if (kdfOptions === null) return invalid("privateKey", "missing kdf options");

  if (kdfOptions[1] + 4 > body.length) return invalid("privateKey", "missing key count");
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const count = view.getUint32(kdfOptions[1]);
  if (count !== 1) return invalid("privateKey", `expected one key, found ${count}`);

  const publicBlob = readString(body, kdfOptions[1] + 4);
  if (publicBlob === null) return invalid("privateKey", "missing public key");
  const secret = readString(body, publicBlob[1]);
  if (secret === null) return invalid("privateKey", "missing private section");

  const publicKey = parseBlob(publicBlob[0], "");
  if (Result.isFailure(publicKey)) return Result.fail(publicKey.failure);
  if (publicKey.success.algorithm !== ED25519) {
    return invalid("privateKey", `cannot sign with '${publicKey.success.algorithm}'`);
  }

  // The private section repeats the key type and the point, then carries the
  // seed and the point together as one 64-byte value, then the comment.
  const section = secret[0];
  if (section.length < 8) return invalid("privateKey", "truncated private section");
  const sectionView = new DataView(section.buffer, section.byteOffset, section.byteLength);
  if (sectionView.getUint32(0) !== sectionView.getUint32(4)) {
    return invalid("privateKey", "check bytes disagree; the key may be encrypted");
  }

  const type = readString(section, 8);
  if (type === null) return invalid("privateKey", "private section missing key type");
  if (decoder.decode(type[0]) !== ED25519) {
    return invalid("privateKey", "private section disagrees about the key type");
  }
  const point = readString(section, type[1]);
  if (point === null) return invalid("privateKey", "private section missing its point");
  const material = readString(section, point[1]);
  if (material === null) return invalid("privateKey", "private section missing key material");
  if (material[0].length !== 64) {
    return invalid("privateKey", `expected 64 bytes of key material, got ${material[0].length}`);
  }

  const comment = readString(section, material[1]);
  return Result.succeed({
    publicKey:
      comment === null
        ? publicKey.success
        : { ...publicKey.success, comment: decoder.decode(comment[0]) },
    seed: material[0].subarray(0, 32),
  });
};

/**
 * A fresh keypair, for callers that need one without a filesystem — the
 * tests, and any surface that would otherwise tell a user to go and run
 * `ssh-keygen` mid-flow.
 */
export const generate = (comment: string): Effect.Effect<PrivateKey> =>
  Effect.promise(async () => {
    const pair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
    const point = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
    const pkcs8 = new Uint8Array(await crypto.subtle.exportKey("pkcs8", pair.privateKey));
    return {
      publicKey: {
        algorithm: ED25519,
        blob: concatBytes([writeText(ED25519), writeString(point)]),
        point,
        application: null,
        comment,
      },
      seed: seedOf(pkcs8),
    };
  });
