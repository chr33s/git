/** Bounded source-tree comparison; unchanged subtrees are never walked. */
import { compareUtf8 } from "../text.ts";
import { Effect } from "effect";
import { isTree } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";

interface Pair {
  readonly before: Oid | null;
  readonly after: Oid | null;
  readonly prefix: string;
}
export const changed = Effect.fn("knowledge.OutcomePaths.changed")(function* (
  before: Oid | null,
  after: Oid,
  maximum = 256,
) {
  const repository = yield* Repository;
  const pending: Array<Pair> = [{ before, after, prefix: "" }];
  const paths: Array<string> = [];
  let compared = 0;
  let complete = true;
  while (pending.length > 0 && paths.length < maximum && compared < maximum * 4) {
    const pair = pending.pop()!;
    if (pair.before === pair.after) continue;
    compared += 1;
    const old = new Map(
      (pair.before === null ? [] : yield* repository.readTree(pair.before)).map((entry) => [
        entry.name,
        entry,
      ]),
    );
    const fresh = new Map(
      (pair.after === null ? [] : yield* repository.readTree(pair.after)).map((entry) => [
        entry.name,
        entry,
      ]),
    );
    const names = [...new Set([...old.keys(), ...fresh.keys()])].sort(compareUtf8);
    for (const name of names) {
      if (paths.length >= maximum) {
        complete = false;
        break;
      }
      const previous = old.get(name);
      const current = fresh.get(name);
      if (previous?.oid === current?.oid && previous?.mode === current?.mode) continue;
      const pathname = pair.prefix + name;
      const oldTree = previous !== undefined && isTree(previous.mode);
      const newTree = current !== undefined && isTree(current.mode);
      if (oldTree || newTree)
        pending.push({
          before: oldTree ? previous.oid : null,
          after: newTree ? current.oid : null,
          prefix: `${pathname}/`,
        });
      if ((previous !== undefined && !oldTree) || (current !== undefined && !newTree))
        paths.push(pathname);
    }
  }
  return {
    paths: [...new Set(paths)].sort(compareUtf8),
    complete: complete && pending.length === 0,
  };
});
