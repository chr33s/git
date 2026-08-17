/**
 * Walking an append-only commit DAG in an order every replica agrees on.
 *
 * Two of these exist — the trust log and each pull request's event history —
 * and they need exactly the same thing: every commit reachable from a head,
 * ordered so that a commit comes after everything it descends from. Writing
 * that twice would be writing the tie-break twice, and a tie-break that
 * differs between two folds is two hosts disagreeing about state.
 *
 * The order is deterministic, not canonical. Concurrent commits — neither an
 * ancestor of the other — have no true order, so they are sorted by oid.
 * Which arbitrary order hardly matters; that every replica picks the *same*
 * arbitrary one is the whole point.
 */
import { Effect } from "effect";

import { Invalid, type ObjectNotFound, type StorageFailure } from "./Error.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";

/** Each commit in the walked set, mapped to the parents it named. */
export type Parents = ReadonlyMap<Oid, ReadonlyArray<Oid>>;

/**
 * Every commit reachable from `head`, stopping before `boundary`.
 *
 * The boundary is what keeps the trust log from walking into the repository's
 * source history: its first record's parent is the genesis commit, and
 * following that further would read every commit the repository has.
 */
export const reachable = Effect.fn("Dag.reachable")(function* (
  head: Oid,
  boundary?: Oid | null,
  /**
   * Whether a commit belongs to the history being walked.
   *
   * The other half of bounding. A named boundary commit stops a chain that
   * ends where you expect; this stops one that does not — a hub event whose
   * parent is a source commit, say, which would otherwise pull the entire
   * repository into a walk meant to cover one pull request.
   */
  belongs?: (commit: Oid) => Effect.Effect<boolean, ObjectNotFound | StorageFailure, Repository>,
  /**
   * How many commits this walk may read before giving up.
   *
   * Checked as the walk runs rather than against what it produced. A caller
   * that bounds the *result* has already paid for the whole history by the
   * time it can refuse it — which on the receive-pack path is the cost the
   * bound existed to refuse, taken anyway, once per push.
   */
  limit?: number,
) {
  const repository = yield* Repository;

  const parents = new Map<Oid, ReadonlyArray<Oid>>();
  /**
   * Commits `belongs` turned away, remembered.
   *
   * Answering the question again per in-edge is the same read twice, and —
   * because the answer was never recorded — those reads counted toward
   * nothing: one pushed commit listing a hundred thousand fabricated parents
   * cost a hundred thousand object reads without ever reaching the limit.
   */
  const outside = new Set<Oid>();
  const pending: Oid[] = [head];
  while (pending.length > 0) {
    const oid = pending.pop()!;
    if (parents.has(oid) || outside.has(oid) || oid === boundary) continue;
    // Before the work, and counting the work already refused. Checked after
    // `belongs`, the walk pays for every commit it declines to keep.
    if (limit !== undefined && parents.size + outside.size >= limit) {
      return yield* new Invalid({
        field: "history",
        reason: `this history reaches more than ${limit} commits`,
      });
    }
    if (belongs !== undefined && !(yield* belongs(oid))) {
      outside.add(oid);
      continue;
    }

    const commit = yield* repository.readCommit(oid);
    parents.set(oid, commit.parents);
    for (const parent of commit.parents) {
      if (parent !== boundary && !parents.has(parent)) pending.push(parent);
    }
  }
  return parents;
});

/**
 * Parents before children, ties broken by oid.
 *
 * Kahn's algorithm, with the ready set kept sorted rather than used as a
 * stack: taking whichever commit happened to finish last would make the order
 * depend on the shape of the walk instead of on the history.
 *
 * Parents outside the set are ignored, which is what makes a bounded walk
 * usable — the boundary commit is a parent nothing in the set contains.
 */
export const topological = (parents: Parents): ReadonlyArray<Oid> => {
  const remaining = new Map<Oid, number>();
  const children = new Map<Oid, Oid[]>();

  for (const [oid, named] of parents) {
    const inside = named.filter((parent) => parents.has(parent));
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
      if (count > 0) continue;
      // Inserted in sorted position, so the order stays a property of the
      // history rather than of which parent resolved last.
      const at = ready.findIndex((candidate) => candidate > child);
      if (at === -1) ready.push(child);
      else ready.splice(at, 0, child);
    }
  }
  return ordered;
};

/**
 * Everything `from` can reach, itself included.
 *
 * A commit this replica has not fetched is skipped rather than failing: the
 * caller is asking what *we* can see — "had this author already seen that
 * revocation?" — and a partial answer to that is the honest one. Events whose
 * history has not arrived are handled by quarantine, not by this walk.
 */
export const ancestry = Effect.fn("Dag.ancestry")(function* (from: Oid) {
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
    if (commit === null) continue;
    for (const parent of commit.parents) pending.push(parent);
  }
  return seen;
});

export type DagError = ObjectNotFound | StorageFailure;
