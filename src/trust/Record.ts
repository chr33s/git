/**
 * A signed JSON document, stored as a git commit.
 *
 * Trust events and hub events are the same shape underneath — a payload whose
 * exact bytes are what signatures cover, the signatures themselves, and a
 * commit placing it in a hash-linked history — so that shape is written once
 * here and both namespaces use it.
 *
 * ```text
 * commit ── parents: the records this one follows
 *   └── tree
 *         ├── <name>.json   the payload; signatures are over these bytes
 *         └── <name>.sig    armored SSHSIG signatures, JSON array
 * ```
 *
 * Signatures sit beside the payload rather than inside it for the same reason
 * they do in the genesis: a signature written into the document changes the
 * document it signs. Keeping them in a sibling blob also means a record can
 * gain a signature — a second root approving a quorum operation — without the
 * payload's bytes, and so its identity, changing.
 *
 * Content-addressing does the deduplication: two replicas that write the same
 * payload with the same parents produce the same commit oid, so a record that
 * arrives twice is one object.
 */
import { Effect, Schema } from "effect";

import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import type { Signature } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Regular file, non-executable: what every blob written here is. */
const BLOB_MODE = "100644";

/**
 * The identity on trust and hub commits.
 *
 * Fixed rather than the signing user's, because the commit is a container and
 * the signature inside it is the authorship claim. A git identity here would
 * be a second, weaker answer to "who wrote this?" — unsigned, unverified, and
 * the one `git log` shows.
 */
export const IDENTITY = { name: "chr33s-git", email: "hub@localhost" } as const;

export const identityAt = (at: Date): Signature => ({
  name: IDENTITY.name,
  email: IDENTITY.email,
  at,
  offset: 0,
});

export interface Record {
  /** The payload's exact bytes — what every signature covers. */
  readonly payload: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

const SignatureList = Schema.Array(Schema.String);
const decodeSignatureList = Schema.decodeUnknownEffect(SignatureList);

/**
 * Canonical JSON for a payload: two-space indent and a trailing newline.
 *
 * The caller supplies an already-ordered object, because field order is part
 * of the canonical form and only the payload's own module knows its order.
 */
export const encodePayload = (text: string): Uint8Array => encoder.encode(`${text}\n`);

export const encodeSignatures = (armored: ReadonlyArray<string>): Uint8Array =>
  encoder.encode(`${JSON.stringify(armored, null, 2)}\n`);

/**
 * Write one record and return the commit that holds it.
 *
 * The commit is *not* attached to a ref here: which ref, and under what
 * compare-and-swap, is the caller's decision — an append to a trust log and an
 * append to a pull request's event DAG differ in exactly that.
 */
export const write = Effect.fn("trust.Record.write")(function* (input: {
  readonly name: string;
  readonly payload: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
  readonly parents: ReadonlyArray<Oid>;
  readonly message: string;
  readonly at?: Date;
}) {
  const repository = yield* Repository;

  const payload = yield* repository.writeBlob(input.payload);
  const signatures = yield* repository.writeBlob(encodeSignatures(input.signatures));
  const tree = yield* repository.writeTree([
    { mode: BLOB_MODE, name: `${input.name}.json`, oid: payload },
    { mode: BLOB_MODE, name: `${input.name}.sig`, oid: signatures },
  ]);

  return yield* repository.commitTree({
    tree,
    parents: input.parents,
    message: input.message,
    author: identityAt(input.at ?? new Date()),
  });
});

/**
 * Read the record a commit carries.
 *
 * A commit under one of these refs that has no payload is not an empty record
 * — it is a join commit (`hub/Event.ts`), or something that does not belong
 * here at all — so the absence is reported rather than smoothed over.
 */
export const read = Effect.fn("trust.Record.read")(function* (commit: Oid, name: string) {
  const repository = yield* Repository;

  const info = yield* repository.readCommit(commit);
  const payloadEntry = yield* repository.findPath(info.tree, `${name}.json`);
  if (payloadEntry === null) {
    return yield* new Invalid({ field: "record", reason: `${commit} carries no ${name}.json` });
  }

  const payload = yield* repository.readBlob(payloadEntry.oid);
  const signatureEntry = yield* repository.findPath(info.tree, `${name}.sig`);
  if (signatureEntry === null) return { payload, signatures: [] };

  // Lifted rather than piped: `JSON.parse` throws before the decoder's effect
  // exists, so a corrupt blob would be a defect rather than a failure.
  const raw = yield* repository.readBlob(signatureEntry.oid);
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(raw)),
    catch: () => new Invalid({ field: "record", reason: `${commit} has unparseable signatures` }),
  });

  const signatures = yield* decodeSignatureList(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({
          field: "record",
          reason: `${commit} has malformed signatures: ${issue.message}`,
        }),
    ),
  );

  return { payload, signatures };
});

/** Whether a commit carries a record of this name, without reading it. */
export const carries = Effect.fn("trust.Record.carries")(function* (commit: Oid, name: string) {
  const repository = yield* Repository;
  const info = yield* repository.readCommit(commit);
  return (yield* repository.findPath(info.tree, `${name}.json`)) !== null;
});

export type RecordError = Invalid | ObjectNotFound | StorageFailure;
