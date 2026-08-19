/**
 * History for one path — `git log -- <path>`.
 *
 * Filtering a log by path is not "walk the commits and keep the ones that
 * touched the file". Almost every merge touches almost nothing, and a naive
 * filter would report a merge whenever either side had changed the path,
 * burying the two or three commits that actually did the work under every
 * merge that carried them forward.
 *
 * git's answer is history simplification, and it is worth stating because it
 * is the whole algorithm: a commit is *treesame* to a parent when the path
 * has the same content in both. Walking back from the tip —
 *
 *   - treesame to some parent → the path did not change here. Do not report
 *     it, and follow only that parent: the other side's history of this path
 *     is already represented by the side that matches.
 *   - treesame to no parent → the path changed here. Report it, and follow
 *     every parent.
 *
 * The first rule is what collapses a merge-heavy history down to the commits
 * a reader is looking for, and it is why this walks the graph itself rather
 * than reusing `Repository.log`, which follows first parents only.
 */
import { Effect, Stream } from "effect";

import type { ObjectNotFound, StorageFailure } from "./Error.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";

export interface PathChange {
  readonly oid: Oid;
  readonly message: string;
  /** The path's blob at this commit, or `null` where it does not exist. */
  readonly blob: Oid | null;
}

/**
 * Commits that changed `path`, newest first.
 *
 * A rename is a different path and is reported as a deletion here, the same
 * way `git log -- <path>` does without `--follow`.
 */
export const forPath = (
  from: Oid,
  path: string,
  options?: { readonly limit?: number },
): Stream.Stream<PathChange, ObjectNotFound | StorageFailure, Repository> =>
  Stream.unfold(
    { pending: [from], seen: new Set<Oid>() },
    Effect.fn("History.step")(function* (state: {
      readonly pending: ReadonlyArray<Oid>;
      readonly seen: Set<Oid>;
    }) {
      const repository = yield* Repository;

      /** The blob a commit has at `path`, or `null` if it has none. */
      const blobAt = Effect.fn("History.blobAt")(function* (oid: Oid) {
        const commit = yield* repository.readCommit(oid);
        const found = yield* repository.findPath(commit.tree, path);
        return { commit, blob: found?.oid ?? null };
      });

      let pending = [...state.pending];

      while (pending.length > 0) {
        const oid = pending.shift()!;
        if (state.seen.has(oid)) continue;
        state.seen.add(oid);

        const here = yield* blobAt(oid);

        // A root commit has no parent to be treesame to, so it is reported
        // exactly when the path exists in it.
        if (here.commit.parents.length === 0) {
          if (here.blob === null) continue;
          return [
            { oid, message: here.commit.message, blob: here.blob },
            { pending, seen: state.seen },
          ] as const;
        }

        const parents = yield* Effect.forEach(here.commit.parents, (parent) =>
          blobAt(parent).pipe(Effect.map((read) => ({ oid: parent, blob: read.blob }))),
        );

        const treesame = parents.find((parent) => parent.blob === here.blob);
        if (treesame !== undefined) {
          pending = [...pending, treesame.oid];
          continue;
        }

        pending = [...pending, ...parents.map((parent) => parent.oid)];
        return [
          { oid, message: here.commit.message, blob: here.blob },
          { pending, seen: state.seen },
        ] as const;
      }

      return undefined;
    }),
  ).pipe(
    options?.limit === undefined
      ? (self: Stream.Stream<PathChange, ObjectNotFound | StorageFailure, Repository>) => self
      : Stream.take(options.limit),
  );
