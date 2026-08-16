/**
 * Repository identity: the genesis document and the `RepoID` it produces.
 *
 * A repository's name is a URL, and a URL is a claim anybody can make. Its
 * *identity* is this document — a set of root SSH keys and the threshold of
 * them that authority requires — and the SHA-256 of the exact bytes it is
 * stored as. That hash travels with the repository: move it between hosts,
 * clone it, restore it from a backup, and the identity is unchanged, because
 * nothing about it names a host.
 *
 * The bytes are the canonical form, not a re-serialization of the parsed
 * document. `JSON.stringify` of a decoded object would depend on key order,
 * number formatting and the decoder's own idea of what round-trips, and
 * "which bytes did we hash?" is not a question identity can afford to answer
 * approximately. So: written once, hashed as stored, and `load` hashes what it
 * read rather than what it understood.
 *
 * Signatures live *beside* the document, in `genesis.sig`, for the reason that
 * they cannot live inside it: a signature over the document would change the
 * document, and so change the `RepoID` it is a signature for.
 */
import { Effect, Result, Schema } from "effect";

import {
  type Fingerprint,
  fingerprint,
  NAMESPACE,
  parsePublicKey,
  type PrivateKey,
  type PublicKey,
  sign,
  verify,
} from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Record from "./Record.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Where the genesis lives. This ref is written once and never moves again. */
export const GENESIS_REF = "refs/meta/trust/genesis";

/**
 * The record name inside the commit: `genesis.json` holds the bytes the
 * `RepoID` is over, `genesis.sig` the signatures kept out of them.
 */
export const RECORD = "genesis";

/**
 * A repository's identity: `SHA256:` and unpadded base64, the same shape
 * OpenSSH prints a key fingerprint in.
 *
 * Branded separately from `Fingerprint` even though the two are the same
 * shape, because they answer different questions — "is this the same
 * repository?" and "is this the same key?" — and a function that takes one
 * must not silently accept the other.
 */
export type RepoId = string & { readonly RepoId: unique symbol };

export const isRepoId = (value: string): value is RepoId =>
  /^SHA256:[A-Za-z0-9+/]{43}$/.test(value);

/**
 * The object format the repository's git objects use.
 *
 * Both spellings decode, but only `sha1` is supported in this version: the
 * field exists so a future release can carry `sha256` without a format change,
 * and a repository that already declares it should fail with a message that
 * says so rather than one about an unknown field.
 */
export const ObjectFormat = Schema.Literals(["sha1", "sha256"]);

export const GenesisDocument = Schema.Struct({
  version: Schema.Literal(1),
  objectFormat: ObjectFormat,
  /** `authorized_keys` lines: the keys that hold root authority. */
  rootKeys: Schema.Array(Schema.String),
  /** How many distinct root keys an authority operation needs. */
  threshold: Schema.Int,
});

export type GenesisDocument = (typeof GenesisDocument)["Type"];

const decodeDocumentSchema = Schema.decodeUnknownEffect(GenesisDocument);

export interface RootKey {
  readonly key: PublicKey;
  readonly fingerprint: Fingerprint;
}

/**
 * A genesis that has been read, checked and fingerprinted — the value every
 * other part of the trust system takes, so that none of them has to remember
 * to validate one.
 */
export interface Genesis {
  readonly document: GenesisDocument;
  /** Exactly the bytes stored, and exactly the bytes `repoId` is over. */
  readonly bytes: Uint8Array;
  readonly repoId: RepoId;
  readonly roots: ReadonlyArray<RootKey>;
}

/**
 * The canonical serialization: fixed field order, two-space indent, trailing
 * newline.
 *
 * Field order is written out rather than left to the object literal so that
 * the canonical form is stated in one place a reader can check. The indent is
 * for the humans who will read this blob in a diff — it costs bytes in a file
 * written once per repository.
 */
export const encodeDocument = (document: GenesisDocument): Uint8Array =>
  encoder.encode(
    `${JSON.stringify(
      {
        version: document.version,
        objectFormat: document.objectFormat,
        rootKeys: document.rootKeys,
        threshold: document.threshold,
      },
      null,
      2,
    )}\n`,
  );

/** `SHA256:<unpadded base64>` over the stored bytes. */
export const repoIdOf = (bytes: Uint8Array): Effect.Effect<RepoId> =>
  Effect.promise(async () => {
    const hash = await crypto.subtle.digest("SHA-256", bytes.slice().buffer);
    let binary = "";
    for (const byte of new Uint8Array(hash)) binary += String.fromCharCode(byte);
    // SAFETY: a SHA-256 digest is thirty-two bytes, whose base64 is
    // forty-four characters ending in one `=`; dropping the padding leaves
    // the forty-three `isRepoId` names.
    return `SHA256:${btoa(binary).replace(/=+$/, "")}` as RepoId;
  });

/**
 * Structural rules, checked before a document is ever treated as authority.
 *
 * A threshold above the number of keys is the one that matters: it produces a
 * repository whose authority can never act, which is indistinguishable from a
 * repository whose authority has been destroyed, and it should be refused at
 * creation rather than discovered the first time somebody needs to add a
 * member.
 */
export const validateDocument = Effect.fn("Genesis.validateDocument")(function* (
  document: GenesisDocument,
) {
  if (document.objectFormat !== "sha1") {
    return yield* new Invalid({
      field: "objectFormat",
      reason: `'${document.objectFormat}' repositories are not supported in this version`,
    });
  }
  if (document.rootKeys.length === 0) {
    return yield* new Invalid({ field: "rootKeys", reason: "a repository needs a root key" });
  }
  if (document.threshold < 1) {
    return yield* new Invalid({ field: "threshold", reason: "threshold must be at least 1" });
  }
  if (document.threshold > document.rootKeys.length) {
    return yield* new Invalid({
      field: "threshold",
      reason: `threshold ${document.threshold} exceeds ${document.rootKeys.length} root keys`,
    });
  }

  const roots: RootKey[] = [];
  const seen = new Set<string>();
  for (const line of document.rootKeys) {
    const parsed = parsePublicKey(line);
    if (Result.isFailure(parsed)) {
      return yield* new Invalid({
        field: "rootKeys",
        reason: `bad root key: ${parsed.failure.reason}`,
      });
    }
    const print = yield* fingerprint(parsed.success);
    // The same key twice would let one holder satisfy a two-of-three quorum
    // on their own, which is the quorum not existing.
    if (seen.has(print)) {
      return yield* new Invalid({ field: "rootKeys", reason: `duplicate root key ${print}` });
    }
    seen.add(print);
    roots.push({ key: parsed.success, fingerprint: print });
  }
  return roots;
});

/**
 * Bytes to a checked `Genesis`.
 *
 * The hash is taken over the argument, not over a re-encoding of the parsed
 * document: a host that stored bytes this version would serialize differently
 * still has the identity it has always had, and changing that silently would
 * break every pinned `known_repos` entry pointing at it.
 */
export const load = Effect.fn("Genesis.load")(function* (bytes: Uint8Array) {
  // `JSON.parse` throws, and it throws while the argument to the decoder is
  // still being evaluated — before any Effect exists to carry the failure. It
  // has to be lifted here, or a corrupt genesis takes the fiber down as a
  // defect instead of being reported as the invalid document it is.
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "genesis", reason: "genesis is not valid JSON" }),
  });

  const parsed = yield* decodeDocumentSchema(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "genesis", reason: `malformed genesis: ${issue.message}` }),
    ),
  );

  const roots = yield* validateDocument(parsed);
  const repoId = yield* repoIdOf(bytes);
  return { document: parsed, bytes, repoId, roots };
});

/**
 * A new genesis from key lines and a threshold.
 *
 * Returns the loaded form, so a caller that has just created a repository
 * holds the same value a caller that has just read one does.
 */
export const create = Effect.fn("Genesis.create")(function* (
  rootKeys: ReadonlyArray<string>,
  threshold: number,
) {
  const document = {
    version: 1,
    objectFormat: "sha1",
    rootKeys,
    threshold,
  } satisfies GenesisDocument;
  const bytes = encodeDocument(document);
  const roots = yield* validateDocument(document);
  const repoId = yield* repoIdOf(bytes);
  return { document, bytes, repoId, roots };
});

// -- signatures -------------------------------------------------------------

/** One root holder's signature over the genesis bytes. */
export const signGenesis = (
  genesis: Genesis,
  key: PrivateKey,
): Effect.Effect<string, never, never> => sign(key, genesis.bytes, NAMESPACE);

/**
 * Which root keys have actually signed this genesis.
 *
 * Returns the distinct root signers rather than a yes/no, because the caller
 * that wants "is the quorum met?" and the caller that wants "who vouched for
 * this?" are both real, and the second cannot be recovered from the first.
 *
 * A signature from a key that is not a root is ignored rather than refused: it
 * proves something true about a key nobody asked about, and treating it as an
 * error would let anyone break a genesis by appending their own signature.
 */
export const rootSigners = Effect.fn("Genesis.rootSigners")(function* (
  genesis: Genesis,
  armored: ReadonlyArray<string>,
) {
  const roots = new Map(genesis.roots.map((root) => [root.fingerprint, root] as const));
  const signers = new Set<Fingerprint>();

  for (const signature of armored) {
    const signer = yield* verify(signature, genesis.bytes, NAMESPACE).pipe(
      // A malformed signature in the list is not a reason to refuse the
      // genesis: it cannot add authority, and the quorum check below is what
      // decides whether enough real ones are present.
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (signer === null) continue;
    const print = yield* fingerprint(signer);
    if (roots.has(print)) signers.add(print);
  }

  return [...signers];
});

/** Whether enough distinct root keys have signed to meet the threshold. */
export const quorumMet = (genesis: Genesis, signers: ReadonlyArray<Fingerprint>): boolean =>
  new Set(signers).size >= genesis.document.threshold;

// -- storage ----------------------------------------------------------------

/** The genesis as stored: the document, and whoever signed it. */
export interface StoredGenesis {
  readonly genesis: Genesis;
  readonly signatures: ReadonlyArray<string>;
  readonly commit: Oid;
}

/**
 * Write the genesis, once.
 *
 * `expected: null` is the whole point: this ref is created and never moves
 * again, so a second call fails with a `RefConflict` rather than replacing the
 * identity of a repository that already has one. A host that could rewrite
 * this ref could rename a repository into another repository's identity.
 */
export const writeGenesis = Effect.fn("Genesis.write")(function* (
  genesis: Genesis,
  signatures: ReadonlyArray<string>,
) {
  const repository = yield* Repository;

  const commit = yield* Record.write({
    name: RECORD,
    payload: genesis.bytes,
    signatures,
    parents: [],
    message: `genesis ${genesis.repoId}\n`,
  });

  yield* repository.setRef({ name: GENESIS_REF, to: commit, expected: null });
  return commit;
});

/**
 * The repository's identity, or `null` when it has none.
 *
 * `null` rather than a failure because "this repository is not hub-enabled" is
 * an ordinary answer — every stock git repository gives it, and the client
 * asks precisely to find out.
 */
/**
 * Read the genesis, distinguishing "there is none" from "we could not tell".
 *
 * `null` means the ref does not exist — an ordinary git repository, and an
 * ordinary answer. Every other outcome is a failure, and callers must treat it
 * as one: a storage fault read as "not hub-enabled" would serve a private
 * repository to anybody, which is the worst possible way to fail.
 */
export const readGenesis = Effect.fn("Genesis.read")(function* () {
  const repository = yield* Repository;

  const commit = yield* repository.resolve(GENESIS_REF);
  if (commit === null) return null;

  const record = yield* Record.read(commit, RECORD);
  const genesis = yield* load(record.payload);
  return { genesis, signatures: record.signatures, commit };
});
