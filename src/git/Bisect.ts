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
import { commitAt, Repository } from "./Repository.ts";
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
    const parents = (yield* repository.readHistoryCommit(oid)).parents;
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
  const repository = yield* Repository;
  const bad = yield* commitAt(repository, input.bad);
  const good = yield* Effect.forEach(input.good, (oid) => commitAt(repository, oid));
  const known = new Set((yield* reachableFrom(good)).keys());

  if (known.has(bad)) {
    return yield* new Invalid({
      field: "bad",
      reason: "the bad commit is reachable from a good one, so one of them is mislabelled",
    });
  }

  const graph = yield* reachableFrom([bad]);
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
  const stack: Array<Oid> = [bad];

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

  /**
   * Linear chains inherit their parent's count. Only merges need a union
   * walk, deduplicated with one reusable visitation array. A bitmap for every
   * commit cost n²/8 bytes even on a straight line: 200 MB for 40,000 suspects,
   * more than a Worker's entire memory budget before counting the graph.
   */
  const index = new Map<Oid, number>(ordered.map((oid, at) => [oid, at]));
  const parents = ordered.map((oid) =>
    (graph.get(oid) ?? []).flatMap((parent) => {
      const at = index.get(parent);
      return at === undefined ? [] : [at];
    }),
  );
  const counts = new Uint32Array(ordered.length);
  const visited = new Uint32Array(ordered.length);
  const pending: number[] = [];

  for (let at = 0; at < ordered.length; at++) {
    const preceding = parents[at]!;
    if (preceding.length <= 1) {
      counts[at] = 1 + (preceding[0] === undefined ? 0 : counts[preceding[0]]!);
      continue;
    }

    let count = 0;
    const generation = at + 1;
    pending.push(at);
    while (pending.length > 0) {
      const current = pending.pop()!;
      if (visited[current] === generation) continue;
      visited[current] = generation;
      count++;
      for (const parent of parents[current]!) pending.push(parent);
    }
    counts[at] = count;
  }

  // Testing a commit resolves it and everything on one side of it: bad means
  // the fault is at or below it, good means it is above. The best candidate
  // is the one whose two sides are closest to equal, so whichever answer
  // comes back, the most suspects are eliminated.
  let best = suspects[0]!;
  let bestScore = -1;
  for (const oid of suspects) {
    const below = counts[index.get(oid)!]!;
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
