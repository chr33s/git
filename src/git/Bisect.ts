/**
 * Bisect: which commit first broke it.
 *
 * `git bisect` is usually described as a stateful session — mark good, mark
 * bad, get handed a checkout, repeat. The session is not the interesting part
 * and it does not survive a server, where each request arrives with no memory
 * of the last. What is interesting is the choice: given the commits known good
 * and the one known bad, which single commit should be tested next?
 *
 * So this is a pure function of the known state, and the caller keeps the
 * state. A CLI can hold it in a file, an API caller in a request body, and
 * both get the same answer because it is the same computation.
 *
 * The answer is not "the middle of the list". History is a graph, and the
 * candidate worth testing is the one whose result rules out the most either
 * way — the commit that best halves the remaining set, which for a linear
 * history is the middle and for a merge-heavy one is not. That is what
 * `git rev-list --bisect` computes, and the tests hold this to it.
 *
 * Where two commits halve the set equally well — seven suspects split three
 * and four either way — there is no better one, and this may name the other
 * one git would. The tests say so precisely: they compare against
 * `--bisect-all`, which reports every candidate's distance, and require the
 * chosen commit to be of maximal distance rather than to be git's pick.
 */
import { Effect } from "effect";

import { Invalid } from "./Error.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";

export interface BisectStep {
  /**
   * `test` — try `commit` and report back.
   * `found` — `commit` is the first bad one; there is nothing left to narrow.
   *
   * There is no empty case. The bad commit is always one of its own suspects,
   * so a search that has not failed outright has at least one candidate, and
   * a third state would only be a branch no caller could ever reach.
   */
  readonly kind: "test" | "found";
  readonly commit: Oid;
  /** Commits still under suspicion, `commit` included. */
  readonly remaining: number;
  /** Tests still needed in the worst case, once this one is answered. */
  readonly steps: number;
}

/**
 * Every commit reachable from `from`, and each one's parents.
 *
 * Not `Repository.log`, and not because of the walk it does — `log` follows
 * every parent now. What it also does is pay for `git log`'s output order: a
 * date-sorted frontier and a tie-break that re-walks commits sharing a
 * timestamp. Reachability has no order to get right, so a plain traversal
 * collecting parents is both sufficient and cheaper on exactly the histories
 * bisect exists for.
 */
const reachableFrom = Effect.fn("Bisect.reachableFrom")(function* (roots: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  const parentsOf = new Map<Oid, ReadonlyArray<Oid>>();
  const pending = [...roots];

  while (pending.length > 0) {
    const oid = pending.pop()!;
    if (parentsOf.has(oid)) continue;
    const parents = (yield* repository.readCommit(oid)).parents;
    parentsOf.set(oid, parents);
    for (const parent of parents) {
      if (!parentsOf.has(parent)) pending.push(parent);
    }
  }

  return parentsOf;
});

/**
 * The next commit to test.
 *
 * `bad` is a commit known to have the problem; `good` are commits known not
 * to. The suspects are what `bad` can reach and no `good` commit can — git
 * spells the same set `git rev-list good..bad`.
 */
export const next = Effect.fn("Bisect.next")(function* (input: {
  readonly bad: Oid;
  readonly good: ReadonlyArray<Oid>;
}) {
  const known = new Set((yield* reachableFrom(input.good)).keys());

  if (known.has(input.bad)) {
    return yield* new Invalid({
      field: "bad",
      reason: "the bad commit is reachable from a good one, so one of them is mislabelled",
    });
  }

  const graph = yield* reachableFrom([input.bad]);
  const inRange = (oid: Oid) => graph.has(oid) && !known.has(oid);

  const suspects = [...graph.keys()].filter(inRange);
  if (suspects.length === 1) {
    return { kind: "found", commit: suspects[0]!, remaining: 1, steps: 0 } satisfies BisectStep;
  }

  /**
   * Suspects with every parent before its children.
   *
   * A depth-first post-order over the parent edges gives exactly that, and
   * the order is what lets each commit's ancestry be built from its parents'
   * in one pass instead of re-walking the graph beneath every candidate.
   */
  const ordered: Array<Oid> = [];
  const started = new Set<Oid>();
  const finished = new Set<Oid>();
  const stack: Array<Oid> = [input.bad];

  while (stack.length > 0) {
    const oid = stack[stack.length - 1]!;
    if (finished.has(oid)) {
      stack.pop();
      continue;
    }
    if (started.has(oid)) {
      stack.pop();
      finished.add(oid);
      ordered.push(oid);
      continue;
    }
    started.add(oid);
    for (const parent of graph.get(oid) ?? []) {
      if (inRange(parent) && !finished.has(parent)) stack.push(parent);
    }
  }

  /** How many suspects each one can reach, itself included. */
  const ancestry = new Map<Oid, Set<Oid>>();
  for (const oid of ordered) {
    const own = new Set<Oid>([oid]);
    for (const parent of graph.get(oid) ?? []) {
      if (!inRange(parent)) continue;
      for (const reached of ancestry.get(parent) ?? []) own.add(reached);
    }
    ancestry.set(oid, own);
  }

  // Testing a commit resolves it and everything on one side of it: bad means
  // the fault is at or below it, good means it is above. The best candidate
  // is the one whose two sides are closest to equal, so whichever answer
  // comes back, the most suspects are eliminated.
  let best = suspects[0]!;
  let bestScore = -1;
  for (const oid of suspects) {
    const below = ancestry.get(oid)?.size ?? 1;
    const score = Math.min(below, suspects.length - below);
    if (score > bestScore) {
      bestScore = score;
      best = oid;
    }
  }

  return {
    kind: "test",
    commit: best,
    remaining: suspects.length,
    // The worst case after this answer: the larger of the two sides.
    steps: Math.ceil(Math.log2(Math.max(2, suspects.length - bestScore))),
  } satisfies BisectStep;
});
