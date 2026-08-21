/** Deterministic earliest-unique-commit lineage keys for repository discovery. */
import { Effect } from "effect";

import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";

export type Lineage = `sha1:${Oid}`;

export const isLineage = (value: string): value is Lineage => /^sha1:[0-9a-f]{40}$/.test(value);

const MAX_COMMITS = 1_000_000;

const history = Effect.fn("social.Lineage.history")(function* (head: Oid, ceiling: number) {
  const repository = yield* Repository;
  const parents = new Map<Oid, ReadonlyArray<Oid>>();
  const pending = [head];
  while (pending.length > 0) {
    const commit = pending.pop();
    if (commit === undefined || parents.has(commit)) continue;
    if (parents.size >= ceiling) {
      return yield* new Invalid({
        field: "lineage",
        reason: `lineage history exceeds the ${ceiling}-commit ceiling`,
      });
    }
    const record = yield* repository.readCommit(commit);
    parents.set(commit, record.parents);
    for (const parent of record.parents) pending.push(parent);
  }
  return parents;
});

/**
 * Root commit for an origin, or the first commit not reachable from an
 * upstream for a permanent fork. Concurrent boundary candidates are broken
 * by oid, so every full clone of the same graph chooses the same key.
 */
export const earliestUnique = Effect.fn("social.Lineage.earliestUnique")(function* (input: {
  readonly head: Oid;
  readonly upstream?: Oid;
  readonly ceiling?: number;
}) {
  const ceiling = Math.max(1, Math.min(input.ceiling ?? MAX_COMMITS, MAX_COMMITS));
  const local = yield* history(input.head, ceiling);
  const upstream =
    input.upstream === undefined
      ? new Map<Oid, ReadonlyArray<Oid>>()
      : yield* history(input.upstream, ceiling);
  const unique = new Set([...local.keys()].filter((commit) => !upstream.has(commit)));
  if (unique.size === 0) {
    return yield* new Invalid({
      field: "lineage",
      reason: "the selected revision has no commit unique from its upstream",
    });
  }

  const boundary = [...unique]
    .filter((commit) => !(local.get(commit) ?? []).some((parent) => unique.has(parent)))
    .sort();
  const first = boundary[0];
  if (first === undefined) {
    return yield* new Invalid({ field: "lineage", reason: "could not find a lineage boundary" });
  }
  const lineage = `sha1:${first}`;
  if (!isLineage(lineage)) {
    return yield* new Invalid({ field: "lineage", reason: "computed a malformed lineage key" });
  }
  return lineage;
});

export type LineageError = Invalid | ObjectNotFound | StorageFailure;
