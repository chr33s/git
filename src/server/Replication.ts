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
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Log from "../trust/Log.ts";

export interface Divergence {
  readonly ref: string;
  readonly ours: Oid;
  readonly theirs: Oid;
  /** The join that reconciled it, for an append-only ref. */
  readonly joined: Oid | null;
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

  const trust = yield* fetchRepository({
    url: input.url,
    stores: input.stores,
    token: input.token,
    refspecs: [{ force: false, source: "refs/meta/trust/*", destination: "refs/meta/trust/*" }],
  });
  fetched.push(...trust.refs.map((update) => update.name));
  rejected.push(...trust.rejected);

  const hub = yield* fetchRepository({
    url: input.url,
    stores: input.stores,
    token: input.token,
    refspecs: [{ force: false, source: "refs/hub/*", destination: "refs/hub/*" }],
  });
  fetched.push(...hub.refs.map((update) => update.name));
  rejected.push(...hub.rejected);

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

  const ours = yield* repository.resolve(ref);
  if (ours === null) {
    yield* repository.setRef({ name: ref, to: theirs, expected: null });
    return { ref, ours: theirs, theirs, joined: null } satisfies Divergence;
  }
  if (ours === theirs) return { ref, ours, theirs, joined: null } satisfies Divergence;

  if (!Refspec.isAppendOnly(ref)) {
    // Reported, never resolved: automatic synchronization must not invent a
    // merge, a rebase or a force push on a branch.
    return { ref, ours, theirs, joined: null } satisfies Divergence;
  }

  // Already ahead of them, or already behind: a fast-forward either way, and
  // no join to record.
  if (yield* ancestorOf(theirs, ours)) return { ref, ours, theirs, joined: null };
  if (yield* ancestorOf(ours, theirs)) {
    yield* repository.setRef({ name: ref, to: theirs, expected: ours });
    return { ref, ours, theirs, joined: null } satisfies Divergence;
  }

  const pr = Event.prOf(ref);
  const joined =
    pr === null ? yield* joinTrust([ours, theirs]) : yield* Event.join(pr, [ours, theirs]);

  return { ref, ours, theirs, joined } satisfies Divergence;
});

const ancestorOf = Effect.fn("Replication.ancestorOf")(function* (ancestor: Oid, descendant: Oid) {
  const repository = yield* Repository;
  return yield* repository
    .isAncestor(ancestor, descendant)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)));
});

/** A join on the trust log, which owns its own ref rather than a PR's. */
const joinTrust = Effect.fn("Replication.joinTrust")(function* (heads: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  const commit = yield* Log.join(heads);
  yield* repository.setRef({ name: Log.LOG_REF, to: commit, expected: heads[0] ?? null });
  return commit;
});

export type ReplicationError = Invalid | ObjectNotFound | PackCorrupt | StorageFailure;
