/** Receive-pack's default: existing shallow roots stay, new ones require refusal. */
import { Effect } from "effect";

import { Repository } from "../git/Repository.ts";
import type { Oid, RefUpdate } from "../git/Store.ts";

/** Capture the receiver's history before the incoming pack can add objects. */
export const prepare = Effect.fn("Shallow.prepare")(function* (declared: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  const known = new Set((yield* repository.refs).map(([, oid]) => oid));
  const head = yield* repository.resolve("HEAD");
  if (head !== null) known.add(head);
  const roots = [...known];
  for (const oid of yield* repository.shallow) known.add(oid);

  // Trees and blobs have no commit history; an annotated tag leads to its
  // target. This follows declared boundaries, not general object connectivity.
  const parentsOf = Effect.fn("Shallow.parentsOf")((oid: Oid) =>
    repository.readHistoryCommit(oid).pipe(
      Effect.map((commit) => commit.parents),
      Effect.catchTag("ObjectNotFound", () =>
        repository.readTag(oid).pipe(
          Effect.map((tag) => [tag.object]),
          Effect.catchTag("ObjectNotFound", () => Effect.succeed([])),
        ),
      ),
    ),
  );

  const required = new Set(declared.filter((oid) => !known.has(oid)));
  const unresolved = new Set<Oid>();
  for (const oid of required) {
    // A boundary absent before unpacking is new regardless of which parent
    // objects the pack brings. Existing loose objects alone do not establish
    // history either: failed pushes may have left those behind.
    if (yield* repository.contains(oid)) unresolved.add(oid);
  }
  const pending = [...roots];
  const visited = new Set<Oid>();
  while (pending.length > 0 && unresolved.size > 0) {
    const oid = pending.pop()!;
    if (visited.has(oid)) continue;
    visited.add(oid);
    known.add(oid);
    unresolved.delete(oid);
    required.delete(oid);
    for (const parent of yield* parentsOf(oid)) pending.push(parent);
  }

  return Effect.fn("Shallow.refusals")(function* (updates: ReadonlyArray<RefUpdate>) {
    const refused = new Set<string>();
    if (required.size === 0) return refused;
    for (const update of updates) {
      if (update.value === null) continue;
      const pending = [update.value];
      const seen = new Set<Oid>();
      while (pending.length > 0) {
        const oid = pending.pop()!;
        if (required.has(oid)) {
          refused.add(update.name);
          break;
        }
        if (known.has(oid) || seen.has(oid)) continue;
        seen.add(oid);
        for (const parent of yield* parentsOf(oid)) pending.push(parent);
      }
    }
    return refused;
  });
});
