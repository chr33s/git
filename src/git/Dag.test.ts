/**
 * The bounded walk both append-only histories are read through.
 *
 * The trust log and each pull request's event history are walked here, on the
 * synchronous receive-pack path, over commits whose parent lists are written
 * by whoever pushed them. What is tested here is the accounting: which reads
 * the walk pays for, and whether the bound it was given can actually stop it.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import * as Dag from "./Dag.ts";
import { EMPTY_TREE_OID, type Signature } from "./Format.ts";
import { stores } from "./Memory.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { ObjectStore, type Oid } from "./Store.ts";

const author: Signature = {
  name: "Author",
  email: "author@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ),
  );

/** Oids that name nothing, which is what a pushed parent list may be full of. */
const absent = (count: number): ReadonlyArray<Oid> =>
  // SAFETY: forty lowercase hex characters, which is what `Oid` brands.
  Array.from({ length: count }, (_, at) => `${at}`.padStart(40, "a") as Oid);

describe("Dag.reachable", () => {
  it("pays for a commit it turns away exactly once, and counts it", async () => {
    // A commit outside the history was never recorded, so the question was
    // asked again for every edge naming it — and, because the answer was not
    // kept, none of those reads counted toward the limit. One pushed commit
    // listing a hundred thousand fabricated parents cost a hundred thousand
    // object reads on the receive-pack path without ever reaching the ceiling
    // that was supposed to refuse it.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;

        const strangers = absent(6);
        const head = yield* repository.commitTree({
          tree: EMPTY_TREE_OID,
          parents: strangers,
          message: "head\n",
          author,
        });

        const asked: Oid[] = [];
        const belongs = (commit: Oid) =>
          Effect.gen(function* () {
            asked.push(commit);
            return (
              (yield* repository
                .readCommit(commit)
                .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)))) !== null
            );
          });

        const unbounded = yield* Dag.reachable(head, null, belongs);
        const asks = asked.length;
        const bounded = yield* Dag.reachable(head, null, belongs, 3).pipe(
          Effect.as(null),
          Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
        );
        return { walked: [...unbounded.keys()], asks, bounded };
      }),
    );

    assert.deepEqual(outcome.walked.length, 1, "only the commit the repository holds is kept");
    assert.equal(outcome.asks, 7, "each stranger is asked about once, and the head once");
    assert.match(outcome.bounded ?? "", /more than 3 commits/);
  });

  it("stops at the boundary rather than walking past it", async () => {
    const walked = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const first = yield* repository.commitTree({
          tree: EMPTY_TREE_OID,
          parents: [],
          message: "first\n",
          author,
        });
        const second = yield* repository.commitTree({
          tree: EMPTY_TREE_OID,
          parents: [first],
          message: "second\n",
          author,
        });
        return [...(yield* Dag.reachable(second, first)).keys()];
      }),
    );

    assert.deepEqual(walked.length, 1);
  });
});
