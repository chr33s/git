/**
 * Pulling hub and trust state from another replica.
 *
 * Source branches and hub state need opposite things from a fetch, which is
 * why this is not just `fetchRepository` with more refspecs:
 *
 *   - a **branch** that diverged must not be merged behind anyone's back. The
 *     fetch reports it and stops, because inventing a merge is how a mirror
 *     silently rewrites what somebody pushed.
 *   - a **hub or trust ref** that diverged *must* be merged, and can be safely:
 *     both sides are append-only histories of signed statements, so the union
 *     of them is the answer and a join commit is how the union is recorded.
 *     Choosing a side instead would drop whatever was said on the other — a
 *     revocation, say.
 *
 * Trust first, always. A hub event is validated against the membership graph,
 * so fetching events before the grants that authorize them makes them look
 * unauthorized. They are not rejected forever when that happens — the
 * projection re-judges every event each time it runs, so an event whose grant
 * arrives later starts counting then. That is what "quarantine" amounts to
 * here: no separate holding area, just a fold that is a pure function of what
 * has arrived so far.
 */
import { Effect } from "effect";

import { fetchRepository, type FetchStores } from "../client/Fetch.ts";
import type { Invalid, ObjectNotFound, PackCorrupt, StorageFailure } from "../git/Error.ts";
import * as Refspec from "../git/Refspec.ts";
import * as Policy from "./Policy.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as SocialLog from "../social/Log.ts";
import * as Log from "../trust/Log.ts";

export interface Divergence {
  readonly ref: string;
  readonly ours: Oid;
  readonly theirs: Oid;
  /** The join that reconciled it, for an append-only ref. */
  readonly joined: Oid | null;
  /**
   * Whether this needs a person.
   *
   * `ours !== theirs` is not the question: a replica that pushed last is ahead
   * of its peer, which is the ordinary state of things and not something to
   * report. This is true only for a branch that genuinely went two ways, which
   * automatic synchronization refuses to resolve.
   */
  readonly diverged: boolean;
}

export interface Outcome {
  readonly fetched: ReadonlyArray<string>;
  /**
   * Refs the remote moved somewhere this repository cannot follow.
   *
   * For a branch that is the end of it. For an append-only ref it is the
   * beginning: `reconcile` is what turns one of these into a join.
   */
  readonly rejected: ReadonlyArray<{ readonly name: string; readonly oid: Oid }>;
}

/** Ordered state passes: authority first, then social evidence, then consumers. */
export const stateFetchPasses: ReadonlyArray<ReadonlyArray<Refspec.Refspec>> = [
  [
    { force: false, source: "refs/meta/trust/*", destination: "refs/meta/trust/*" },
    { force: false, source: "refs/meta/policy", destination: "refs/meta/policy" },
  ],
  [{ force: false, source: Refspec.SOCIAL_LOG, destination: Refspec.SOCIAL_LOG }],
  [{ force: false, source: "refs/hub/*", destination: "refs/hub/*" }],
];

/**
 * Fetch trust, then hub, from one remote.
 *
 * Two passes rather than one refspec list, because the ordering is the point:
 * the second pass is validated against what the first brought in.
 */
export const pull = Effect.fn("Replication.pull")(function* (input: {
  readonly url: string;
  readonly stores: FetchStores;
  readonly token?: string | undefined;
  /** Source branches as well, when a caller wants a full mirror. */
  readonly includeSource?: boolean;
}) {
  const fetched: string[] = [];
  const rejected: Array<{ readonly name: string; readonly oid: Oid }> = [];

  // Trust *and* the branch rules, in that order and before anything else: a
  // replica holding the membership but not the rules answers `OPEN` to every
  // question the policy boundary asks, so it would let through exactly the
  // pushes the origin protects.
  for (const refspecs of stateFetchPasses) {
    const pass = yield* fetchRepository({
      url: input.url,
      stores: input.stores,
      token: input.token,
      refspecs,
    });
    fetched.push(...pass.refs.map((update) => update.name));
    rejected.push(...pass.rejected);
  }

  if (input.includeSource === true) {
    const source = yield* fetchRepository({
      url: input.url,
      stores: input.stores,
      token: input.token,
      refspecs: Refspec.DEFAULT_FETCH,
    });
    fetched.push(...source.refs.map((update) => update.name));
    rejected.push(...source.rejected);
  }

  return { fetched, rejected } satisfies Outcome;
});

/**
 * Reconcile an append-only ref that this replica and another both moved.
 *
 * A fetch refuses such a ref rather than overwriting it, which is right for a
 * branch and only half an answer for a history that is meant to grow: the
 * remaining half is to keep both sides, and that is a join.
 *
 * Refuses to touch anything that is not append-only. A branch that diverged is
 * a decision for a person.
 */
export const reconcile = Effect.fn("Replication.reconcile")(function* (ref: string, theirs: Oid) {
  const repository = yield* Repository;

  // What the ref holds, not what it resolves to: `ours` is the expected value
  // of every swap below, and a symbolic ref's resolved oid is one the store
  // will never agree with.
  const ours = yield* repository.readRef(ref);
  if (ours === null) {
    yield* repository.setRef({ name: ref, to: theirs, expected: null });
    return { ref, ours: theirs, theirs, joined: null, diverged: false } satisfies Divergence;
  }
  if (ours === theirs) {
    return { ref, ours, theirs, joined: null, diverged: false } satisfies Divergence;
  }

  // The rules file is not append-only and is not a branch either: it is one
  // blob the repository publishes about itself, and a replica that keeps its
  // own copy keeps enforcing rules the source has already superseded — a
  // protected branch that was unprotected, or a required check that was
  // dropped. It is fetched for exactly that reason, so it is taken as read
  // rather than reported as a divergence nobody will act on. Writing it needs
  // `policy.write` at the boundary; arriving by replication is the source
  // saying what it now requires.
  if (ref === Policy.RULES_REF) {
    yield* repository.setRef({ name: ref, to: theirs, expected: ours });
    return { ref, ours, theirs, joined: null, diverged: false } satisfies Divergence;
  }

  if (!Refspec.isAppendOnly(ref)) {
    // Reported, never resolved: automatic synchronization must not invent a
    // merge, a rebase or a force push on a branch.
    return { ref, ours, theirs, joined: null, diverged: true } satisfies Divergence;
  }

  // Already ahead of them, or already behind: a fast-forward either way, and
  // no join to record — and *not* a divergence, which is what a caller reports
  // to a person. Being ahead of a peer is the ordinary state of the replica
  // that pushed last, and announcing it as a stuck trust log is a false alarm
  // in the one flow whose purpose is surfacing a real one.
  if (yield* ancestorOf(theirs, ours)) {
    return { ref, ours, theirs, joined: null, diverged: false } satisfies Divergence;
  }
  if (yield* ancestorOf(ours, theirs)) {
    yield* repository.setRef({ name: ref, to: theirs, expected: ours });
    return { ref, ours, theirs, joined: null, diverged: false } satisfies Divergence;
  }

  // Which ref this is decides where the join goes. Sending anything that is
  // merely append-only down the trust-log path would repoint the membership
  // log at whatever had diverged — `refs/hub/index`, a nested
  // `refs/hub/pr/a/b` — and wipe the projection every capability check reads.
  const pr = Event.prOf(ref);
  const joined =
    pr !== null ? yield* Event.join(pr, [ours, theirs]) : yield* joinInto(ref, [ours, theirs]);

  return { ref, ours, theirs, joined, diverged: false } satisfies Divergence;
});

const ancestorOf = Effect.fn("Replication.ancestorOf")(function* (ancestor: Oid, descendant: Oid) {
  const repository = yield* Repository;
  return yield* repository
    .isAncestor(ancestor, descendant)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)));
});

/**
 * A join on a ref that is not a pull request's — the trust log, or any other
 * append-only ref a future version adds.
 *
 * The commit shape is the trust log's, because a join is the same object
 * everywhere: a commit with both heads as parents and nothing in its tree.
 * What differs is only which ref it lands on, so that is the argument.
 */
const joinInto = Effect.fn("Replication.joinInto")(function* (
  ref: string,
  heads: ReadonlyArray<Oid>,
) {
  const repository = yield* Repository;
  const commit = ref === Refspec.SOCIAL_LOG ? yield* SocialLog.join(heads) : yield* Log.join(heads);
  yield* repository.setRef({ name: ref, to: commit, expected: heads[0] ?? null });
  return commit;
});

export type ReplicationError = Invalid | ObjectNotFound | PackCorrupt | StorageFailure;
