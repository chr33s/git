/**
 * The trust log: membership as a hash-linked history, not a bag of refs.
 *
 * An earlier design gave every grant and revocation its own immutable ref and
 * synchronized the set by union. That converges, but it has a failure nobody
 * can see: *omission is invisible*. A replica that withholds one revocation
 * ref presents a trust state in which the revoked key is still a member, and
 * no verifier holding only a set of refs can tell the set is incomplete.
 *
 * A log fixes exactly that. Every record names its predecessors, so the state
 * has a frontier, and two replicas can tell whether one has seen everything
 * the other has. Withholding stops being silent — it becomes a stale head,
 * which `Checkpoint.ts` puts a bound on.
 *
 * ```text
 * refs/meta/trust/genesis ── the identity, written once
 *              ▲
 *              │ first record's parent
 * refs/meta/trust/log ────── grant → revoke → root-change → …
 * ```
 *
 * What this does *not* claim: freshness. A replica can serve a consistent but
 * old view, up to the age a verifier is willing to accept. That is inherent to
 * anything verifiable offline, and it is written down rather than papered over.
 */
import { Effect, Schema } from "effect";

import { NAMESPACE, type PrivateKey, sign } from "../crypto/SshSignature.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "./Certificate.ts";
import { GENESIS_REF } from "./Genesis.ts";
import * as Record from "./Record.ts";

/** The log's head. Append-only: it never moves to something not descended from it. */
export const LOG_REF = "refs/meta/trust/log";

/** The record name inside each commit — `entry.json` and `entry.sig`. */
export const RECORD = "entry";

export interface Entry {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly payload: Certificate.TrustPayload;
  /**
   * The payload's stored bytes.
   *
   * Carried beside the decoded form because signatures are over these, and
   * verifying against a re-encoding would check that this version can
   * reproduce the bytes rather than that the signer signed them.
   */
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

/**
 * A UUIDv7: milliseconds big-endian, then randomness.
 *
 * Time-ordered so that ids sort roughly by creation, which is what makes them
 * usable as a deterministic tie-break between records that are causally
 * concurrent — and it is a *tie-break*, never a clock anyone trusts.
 */
export const newId = (at: Date = new Date()): string => {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);

  const milliseconds = at.getTime();
  // 48 bits of timestamp; `>>>` would truncate at 32, so the high half is
  // divided out rather than shifted.
  const high = Math.floor(milliseconds / 2 ** 32);
  const low = milliseconds >>> 0;
  bytes[0] = (high >>> 8) & 0xff;
  bytes[1] = high & 0xff;
  bytes[2] = (low >>> 24) & 0xff;
  bytes[3] = (low >>> 16) & 0xff;
  bytes[4] = (low >>> 8) & 0xff;
  bytes[5] = low & 0xff;
  // Version 7, variant 10.
  bytes[6] = 0x70 | (bytes[6]! & 0x0f);
  bytes[8] = 0x80 | (bytes[8]! & 0x3f);

  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
};

/**
 * Where the next record hangs from, and what the ref must currently be.
 *
 * These are the same value except on the very first append, and the exception
 * is the whole reason they are two fields: the first record's *parent* is the
 * genesis commit, so the chain reaches identity, but the *ref* does not exist
 * yet, so the compare-and-swap has to be "must not exist" rather than "must be
 * the genesis". Conflating them makes the first append of every repository
 * fail with a conflict against a ref nobody has written.
 */
const base = Effect.fn("trust.Log.base")(function* () {
  const repository = yield* Repository;

  const head = yield* repository.resolve(LOG_REF);
  if (head !== null) return { parent: head, expected: head };

  const genesis = yield* repository.resolve(GENESIS_REF);
  if (genesis === null) {
    return yield* new Invalid({
      field: "trust",
      reason: "this repository has no genesis; run `hub init` before granting membership",
    });
  }
  return { parent: genesis, expected: null };
});

/**
 * Append a record and move the log head.
 *
 * The compare-and-swap is on the head this record was built against, so a
 * concurrent append loses and retries rather than overwriting. Retrying is
 * safe here in a way it is not everywhere: adding a record to a log does not
 * change what the record says, and the projection is what resolves two
 * records that arrived at once.
 */
export const append = Effect.fn("trust.Log.append")(
  function* (
    payload: Certificate.TrustPayload,
    bytes: Uint8Array,
    signatures: ReadonlyArray<string>,
  ) {
    const repository = yield* Repository;

    const { expected, parent } = yield* base();
    const commit = yield* Record.write({
      name: RECORD,
      payload: bytes,
      signatures,
      parents: [parent],
      message: `${payload.type} ${payload.id}\n`,
    });

    yield* repository.setRef({ name: LOG_REF, to: commit, expected });
    return commit;
  },
  Effect.retry({ times: 3, while: (error) => error._tag === "RefConflict" }),
);

/**
 * Sign a payload and append it in one step.
 *
 * The bytes are encoded once and both signed and stored, rather than encoded
 * for signing and again for writing. Two encodings that agree today are two
 * that can drift, and the failure would be signatures that verify nowhere.
 *
 * Several keys because a quorum operation needs several: a root change is
 * signed by the threshold, not by whoever typed the command.
 */
export const issue = Effect.fn("trust.Log.issue")(function* (
  payload: Certificate.TrustPayload,
  keys: ReadonlyArray<PrivateKey>,
) {
  const bytes = Certificate.encode(payload);
  const signatures = yield* Effect.forEach(keys, (key) => sign(key, bytes, NAMESPACE));
  return yield* append(payload, bytes, signatures);
});

/**
 * Join two divergent heads.
 *
 * The result carries no payload — it is a synchronization artifact, and
 * `entries` walks through it without folding anything. Recording the join as a
 * commit rather than picking a winner is what keeps both sides' records in the
 * history: a log that resolved divergence by choosing one head would drop the
 * revocations on the other.
 */
export const join = Effect.fn("trust.Log.join")(function* (heads: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  if (heads.length < 2) {
    return yield* new Invalid({ field: "heads", reason: "a join needs two heads or more" });
  }

  const tree = yield* repository.writeTree([]);
  return yield* repository.commitTree({
    tree,
    parents: heads,
    message: "join\n",
    author: Record.identityAt(new Date()),
  });
});

/**
 * Every record in the log, oldest first, parents before children.
 *
 * Concurrent records — neither an ancestor of the other — are ordered by their
 * commit oid. Any total order over a partial one is arbitrary; what matters is
 * that every replica picks the *same* arbitrary one, so two hosts folding the
 * same history reach the same state.
 *
 * The whole log is read into memory. Membership changes are the rarest events
 * a repository has, and the alternative — a projection that streams — buys
 * nothing until a repository has more grants than commits.
 */
export const entries = Effect.fn("trust.Log.entries")(function* () {
  const repository = yield* Repository;

  const head = yield* repository.resolve(LOG_REF);
  if (head === null) return [];

  const genesis = yield* repository.resolve(GENESIS_REF);

  // Reachable set, stopping at the genesis: the genesis is the chain's anchor,
  // not a record, and walking past it would read the whole source history if
  // anything ever pointed the log at a branch.
  const parents = new Map<Oid, ReadonlyArray<Oid>>();
  const pending: Oid[] = [head];
  while (pending.length > 0) {
    const oid = pending.pop()!;
    if (parents.has(oid) || oid === genesis) continue;
    const commit = yield* repository.readCommit(oid);
    parents.set(oid, commit.parents);
    for (const parent of commit.parents) {
      if (parent !== genesis && !parents.has(parent)) pending.push(parent);
    }
  }

  // Kahn's algorithm over the reachable set, ties broken by oid.
  const remaining = new Map<Oid, number>();
  const children = new Map<Oid, Oid[]>();
  for (const [oid, ancestors] of parents) {
    const inside = ancestors.filter((parent) => parents.has(parent));
    remaining.set(oid, inside.length);
    for (const parent of inside) {
      const list = children.get(parent) ?? [];
      list.push(oid);
      children.set(parent, list);
    }
  }

  const ready = [...remaining]
    .filter(([, count]) => count === 0)
    .map(([oid]) => oid)
    .sort();
  const ordered: Oid[] = [];
  while (ready.length > 0) {
    const oid = ready.shift()!;
    ordered.push(oid);
    for (const child of children.get(oid) ?? []) {
      const count = remaining.get(child)! - 1;
      remaining.set(child, count);
      if (count === 0) {
        // Insert in sorted position so the order stays deterministic rather
        // than depending on which parent happened to finish last.
        const at = ready.findIndex((candidate) => candidate > child);
        if (at === -1) ready.push(child);
        else ready.splice(at, 0, child);
      }
    }
  }

  const records: Entry[] = [];
  for (const oid of ordered) {
    const carries = yield* Record.carries(oid, RECORD);
    // Join commits carry nothing; they are structure, not statements.
    if (!carries) continue;

    const record = yield* Record.read(oid, RECORD);
    const payload = yield* Certificate.decode(record.payload);
    records.push({
      commit: oid,
      parents: parents.get(oid) ?? [],
      payload,
      bytes: record.payload,
      signatures: record.signatures,
    });
  }
  return records;
});

/**
 * Every commit an entry can reach, itself included.
 *
 * This is what makes revocation ordering deterministic instead of
 * wall-clock-dependent: "had the author already seen this revocation?" is
 * answered by ancestry, which every replica computes the same way, rather than
 * by comparing timestamps that anybody can write.
 */
export const ancestry = Effect.fn("trust.Log.ancestry")(function* (from: Oid) {
  const repository = yield* Repository;

  const seen = new Set<Oid>();
  const pending: Oid[] = [from];
  while (pending.length > 0) {
    const oid = pending.pop()!;
    if (seen.has(oid)) continue;
    seen.add(oid);

    const commit = yield* repository
      .readCommit(oid)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    // A history this replica has not fetched yet is not an error here: the
    // caller is asking what we can see, and quarantine is what handles the rest.
    if (commit === null) continue;
    for (const parent of commit.parents) pending.push(parent);
  }
  return seen;
});

export const RecordName = Schema.Literal(RECORD);

export type LogError = Invalid | ObjectNotFound | StorageFailure;
