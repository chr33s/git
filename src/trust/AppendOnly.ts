/**
 * The machinery every signed append-only log in a repository shares.
 *
 * Two of them exist — the trust log a repository's membership is folded from,
 * and the social log a principal's statements are — and they are the same
 * structure over different payloads: a chain of record-carrying commits rooted
 * at the genesis, joined rather than rewound, bounded by a ceiling so a fold
 * on the push path cannot be made unbounded by whoever writes to it.
 *
 * Written out once per log, that structure existed twice, and the pair had to
 * be kept in step by hand — including the parts that are load-bearing rather
 * than incidental. `isCommit` narrows its tree read to an absence, because
 * swallowing a storage failure there reports an empty log, and an empty trust
 * log re-authorizes every revoked key; the walks are bounded by the genesis
 * and by the namespace test, because an oid an attacker names would otherwise
 * walk the repository's whole source history on every authorization check.
 * Each of those is one decision, and it belongs in one place.
 *
 * What a log supplies is in `LogDefinition`: where it lives, what its records are
 * called, how to decode one, and which service carries its ceiling. Everything
 * a log does *with* a decoded record — verifying, ordering, projecting — stays
 * with that log, because that is where the two genuinely differ.
 */
import { Effect } from "effect";

import * as Dag from "../git/Dag.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { GENESIS_REF } from "./Genesis.ts";
import * as Record from "./Record.ts";

export interface Entry<P> {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly payload: P;
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

export interface LogDefinition<P> {
  /** This log's name in a trace span, so the two stay distinguishable. */
  readonly name: string;
  /** The one ref of this namespace that moves. */
  readonly ref: string;
  /** The record file each commit of this log carries. */
  readonly record: string;
  /** `Invalid.field` for a refusal about this log. */
  readonly field: string;
  /** What to say when the repository has no genesis to root the log at. */
  readonly noGenesis: string;
  readonly decode: (bytes: Uint8Array) => Effect.Effect<P, Invalid>;
  /** This log's own ceiling service, read per call so a test can set it. */
  readonly ceiling: Effect.Effect<number>;
}

/**
 * Where the next record attaches, and what the head must still be to accept it.
 *
 * `readRef`, not `resolve`: the same value is the parent *and* the
 * compare-and-swap, and a resolved oid is not what the store compares against.
 */
export const base = <P>(log: LogDefinition<P>) =>
  Effect.gen(function* () {
    const repository = yield* Repository;

    const head = yield* repository.readRef(log.ref);
    if (head !== null) return { parent: head, expected: head };

    const genesis = yield* repository.resolve(GENESIS_REF);
    if (genesis === null) {
      return yield* new Invalid({ field: log.field, reason: log.noGenesis });
    }
    return { parent: genesis, expected: null };
  });

/**
 * Whether a commit is one of this log's own.
 *
 * The tree, not only the commit. `fetchRepository` applies refs without a
 * connectivity check, so a replica can hold a commit whose tree object never
 * arrived — and this walk is what every protected-branch push, collection and
 * deepening fetch runs first. Read as a failure, one missing tree took all of
 * them out; read as "not part of this history", it is one commit the walk
 * steps over.
 *
 * And an absence is the *only* thing tolerated. A missing tree is one commit
 * the walk steps over; a store that failed to answer is not. `orElseSucceed`
 * swallowed both, so a transient read error read as "not part of this
 * history" — and this test is the boundary of the history, so the whole ref
 * went empty. Narrowed to the same absence the reads above tolerate, a failure
 * propagates and every caller turns it into its own conservative answer
 * instead of a silently empty one.
 *
 * Swallowed, one blip on a join's tree emptied the trust log: no members and
 * no revocations, cached under an unchanged head, so every revoked key was
 * authorized again and a private repository reported itself as public. The
 * social log fails the same way, which is the reason this is one function and
 * not two.
 */
export const isCommitOf = <P>(log: LogDefinition<P>) =>
  Effect.fn(`${log.name}.isCommit`)(function* (commit: Oid) {
    const repository = yield* Repository;
    const info = yield* repository
      .readCommit(commit)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (info === null) return false;

    const found = yield* repository
      .findPath(info.tree, `${log.record}.json`)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (found !== null) return true;
    // A join carries an empty tree; anything else is not part of this history.
    const tree = yield* repository
      .readTree(info.tree)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    return tree?.length === 0;
  });

/**
 * Whether this head is still inside what a fold of this log will walk.
 *
 * Bounded as the walk runs, not after it: bounding the result means reading
 * the whole log before refusing it, which is the cost the ceiling exists to
 * refuse.
 */
export const withinCeilingOf = <P>(log: LogDefinition<P>) => {
  const isCommit = isCommitOf(log);
  return Effect.fn(`${log.name}.withinCeiling`)(function* (head: Oid) {
    const repository = yield* Repository;
    const genesis = yield* repository.resolve(GENESIS_REF);
    return yield* Dag.reachable(
      head,
      genesis,
      (commit) => isCommit(commit),
      yield* log.ceiling,
    ).pipe(
      Effect.as(true),
      Effect.catchTag("Invalid", () => Effect.succeed(false)),
    );
  });
};

/**
 * Every decodable record on this log, in deterministic topological order.
 *
 * A record this version cannot read is skipped, exactly as one that fails its
 * authority check is. Propagating would be permanent: the log is append-only,
 * so one malformed record pushed by any member would fail every projection —
 * and with it every request — with no way to rewind the ref.
 *
 * The whole walked DAG rides along, joins included. A caller deciding between
 * two records claiming one id needs to know which of them the rest of the log
 * descends from, and a parent map built from record-carrying commits alone
 * would break at every join.
 */
export const entriesOf = <P>(log: LogDefinition<P>) => {
  const isCommit = isCommitOf(log);
  return Effect.fn(`${log.name}.entries`)(function* () {
    const repository = yield* Repository;

    const head = yield* repository.resolve(log.ref);
    if (head === null) {
      return {
        records: [],
        parents: new Map<Oid, ReadonlyArray<Oid>>(),
      };
    }

    // The genesis is the chain's anchor, not a record: bounding the walk there
    // is what stops it reading the whole source history if anything ever points
    // the log at a branch.
    const genesis = yield* repository.resolve(GENESIS_REF);
    const parents = yield* Dag.reachable(head, genesis, (commit) => isCommit(commit));

    const records: Array<Entry<P>> = [];
    for (const oid of Dag.topological(parents)) {
      // Join commits carry nothing; they are structure, not statements.
      if (!(yield* Record.carries(oid, log.record))) continue;

      const record = yield* Record.read(oid, log.record).pipe(
        Effect.catchTags({
          ObjectNotFound: () => Effect.succeed(null),
          Invalid: () => Effect.succeed(null),
        }),
      );
      if (record === null) continue;

      const payload = yield* log.decode(record.payload).pipe(Effect.orElseSucceed(() => null));
      if (payload === null) continue;

      records.push({
        commit: oid,
        parents: parents.get(oid) ?? [],
        payload,
        bytes: record.payload,
        signatures: record.signatures,
      });
    }
    return { records, parents };
  });
};

/**
 * Whether a commit is part of this replica's log.
 *
 * Asked before an event's self-declared head is believed. Ancestry alone
 * cannot answer it: a walk from a fabricated oid, or from the tip of `main`,
 * reaches no record of this log and so matches no revocation — which looks
 * exactly like an event that genuinely predates one. Naming any oid at all
 * would otherwise be a way out of every forward-only revocation.
 */
export const containsOf = <P>(log: LogDefinition<P>) => {
  const isCommit = isCommitOf(log);
  return Effect.fn(`${log.name}.contains`)(function* (commit: Oid) {
    const repository = yield* Repository;

    const head = yield* repository.resolve(log.ref);
    if (head === null) return false;

    const genesis = yield* repository.resolve(GENESIS_REF);
    const reachable = yield* Dag.reachable(head, genesis, (oid) => isCommit(oid));
    return reachable.has(commit);
  });
};

/**
 * Every record this one can reach, itself included.
 *
 * What makes ordering deterministic instead of wall-clock-dependent: "had the
 * author already seen this?" is answered by ancestry, which every replica
 * computes the same way, rather than by comparing timestamps anybody can write.
 *
 * Bounded by the genesis and by the namespace test, for the same reason
 * `entries` is, and by the ceiling besides: `from` is an oid an *event*
 * declared, so it is chosen by whoever wrote the event, and a branch of
 * commits carrying a record passes the namespace test. Unbounded, one such
 * branch is walked again on every push that reads that event.
 */
export const ancestryOf = <P>(log: LogDefinition<P>) => {
  const isCommit = isCommitOf(log);
  return Effect.fn(`${log.name}.ancestry`)(function* (from: Oid) {
    const repository = yield* Repository;

    const genesis = yield* repository.resolve(GENESIS_REF);
    const reachable = yield* Dag.reachable(
      from,
      genesis,
      (oid) => isCommit(oid),
      yield* log.ceiling,
    );
    return new Set(reachable.keys());
  });
};
