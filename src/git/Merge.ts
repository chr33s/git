/**
 * Three-way merge — text, and the tree walk over it.
 *
 * diff3: match each side against the base, and the lines *both* sides matched
 * are the only places a region can safely begin or end. Between two of those
 * sync points sits one region seen three ways — base, ours, theirs — and the
 * whole merge is the four-case decision made on each of them.
 *
 * A conflict is a value, not a failure: an unmergeable region is a normal
 * outcome that the caller writes to a file with markers in it, and the caller
 * still wants the regions that did merge.
 *
 * `mergeTrees` at the bottom is the same decision lifted to whole trees, and
 * it lives here — once — because both of its callers depend on its subtlest
 * part being identical: `Repository.merge` and `Rebase.replayTree` differ
 * only in which commit supplies the base, and two copies of the treesame
 * rules and the conflict taxonomy would drift apart precisely where a drift
 * is hardest to notice.
 */
import { Effect } from "effect";

import { isBinary, lcs, splitLines } from "./Diff.ts";
import type { ObjectNotFound, StorageFailure } from "./Error.ts";
import type { Oid } from "./Store.ts";

export type MergeRegion =
  | { readonly ok: true; readonly lines: ReadonlyArray<string> }
  | {
      readonly ok: false;
      readonly base: ReadonlyArray<string>;
      readonly ours: ReadonlyArray<string>;
      readonly theirs: ReadonlyArray<string>;
    };

export type Strategy = "recursive" | "ours" | "theirs";

export interface TextMerge {
  readonly content: string;
  readonly conflicted: boolean;
}

export interface MergeInput {
  readonly base: string;
  readonly ours: string;
  readonly theirs: string;
  readonly labels?: {
    readonly base?: string;
    readonly ours?: string;
    readonly theirs?: string;
  };
  /** Default "recursive": conflicts survive as markers instead of being picked. */
  readonly strategy?: Strategy;
}

const same = (a: ReadonlyArray<string>, b: ReadonlyArray<string>): boolean =>
  a.length === b.length && a.every((line, index) => line === b[index]);

export const diff3 = (
  base: ReadonlyArray<string>,
  ours: ReadonlyArray<string>,
  theirs: ReadonlyArray<string>,
): ReadonlyArray<MergeRegion> => {
  const ourMatch = new Map(lcs(base, ours));
  const theirMatch = new Map(lcs(base, theirs));

  // Base lines both sides kept, in order. LCS pairs ascend in both
  // coordinates, so these ascend on all three axes and a single cursor walks
  // them.
  const syncs: Array<readonly [number, number, number]> = [];
  for (const [baseIndex, ourIndex] of ourMatch) {
    const theirIndex = theirMatch.get(baseIndex);
    if (theirIndex !== undefined) syncs.push([baseIndex, ourIndex, theirIndex]);
  }

  const regions: Array<MergeRegion> = [];
  let resolved: Array<string> = [];
  const flush = () => {
    if (resolved.length === 0) return;
    regions.push({ ok: true, lines: resolved });
    resolved = [];
  };

  let cursor = 0;
  let baseIndex = 0;
  let ourIndex = 0;
  let theirIndex = 0;

  while (baseIndex < base.length || ourIndex < ours.length || theirIndex < theirs.length) {
    // A stable run: all three files walking the same lines in step.
    let run = 0;
    while (
      baseIndex + run < base.length &&
      ourMatch.get(baseIndex + run) === ourIndex + run &&
      theirMatch.get(baseIndex + run) === theirIndex + run
    ) {
      run++;
    }
    if (run > 0) {
      for (const line of base.slice(baseIndex, baseIndex + run)) resolved.push(line);
      baseIndex += run;
      ourIndex += run;
      theirIndex += run;
      continue;
    }

    while (cursor < syncs.length) {
      const point = syncs[cursor];
      if (point === undefined || point[0] >= baseIndex) break;
      cursor++;
    }
    const [nextBase, nextOurs, nextTheirs] = syncs[cursor] ?? [
      base.length,
      ours.length,
      theirs.length,
    ];

    const region = {
      base: base.slice(baseIndex, nextBase),
      ours: ours.slice(ourIndex, nextOurs),
      theirs: theirs.slice(theirIndex, nextTheirs),
    };
    const take = same(region.ours, region.base)
      ? region.theirs
      : same(region.theirs, region.base) || same(region.ours, region.theirs)
        ? region.ours
        : undefined;

    if (take === undefined) {
      flush();
      regions.push({ ok: false, ...region });
    } else {
      for (const line of take) resolved.push(line);
    }

    baseIndex = nextBase;
    ourIndex = nextOurs;
    theirIndex = nextTheirs;
  }

  flush();
  return regions;
};

/**
 * The merged text, newline-terminated. The merge is line-oriented, so a file
 * that arrived without a final newline gets one — git's own merge drivers do
 * the same rather than let the marker lines run into content.
 */
export const mergeText = (input: MergeInput): TextMerge => {
  const strategy = input.strategy ?? "recursive";
  const oursLabel = input.labels?.ours ?? "ours";
  const baseLabel = input.labels?.base ?? "base";
  const theirsLabel = input.labels?.theirs ?? "theirs";

  const regions = diff3(splitLines(input.base), splitLines(input.ours), splitLines(input.theirs));

  const out: Array<string> = [];
  let conflicted = false;

  for (const region of regions) {
    if (region.ok) {
      for (const line of region.lines) out.push(line);
      continue;
    }
    if (strategy === "ours" || strategy === "theirs") {
      for (const line of strategy === "ours" ? region.ours : region.theirs) out.push(line);
      continue;
    }

    conflicted = true;
    out.push(`<<<<<<< ${oursLabel}`);
    for (const line of region.ours) out.push(line);
    out.push(`||||||| ${baseLabel}`);
    for (const line of region.base) out.push(line);
    out.push("=======");
    for (const line of region.theirs) out.push(line);
    out.push(`>>>>>>> ${theirsLabel}`);
  }

  return { content: out.length === 0 ? "" : `${out.join("\n")}\n`, conflicted };
};

/** One side's version of a path: the blob and the mode it carries. */
export interface TreeSideFile {
  readonly oid: Oid;
  readonly mode: string;
}

export interface TreeConflict {
  readonly path: string;
  /** Why it could not be resolved, in the vocabulary git uses for it. */
  readonly reason: "content" | "add/add" | "modify/delete" | "binary";
}

export interface TreeChange {
  readonly path: string;
  /** `null` removes the path. */
  readonly content: Uint8Array | null;
  readonly mode?: string;
}

const treeEncoder = new TextEncoder();
const treeDecoder = new TextDecoder();

/**
 * The three-way decision over whole trees: what changes to apply on top of
 * `ours`, and which paths a human has to settle.
 *
 * Every rule reads off one question — which sides moved since the base?
 * Neither or both-to-the-same: nothing to decide. One: that side stands,
 * which is the rule that keeps a merge from reverting the other branch's
 * work. Both: merge the text, or report why not.
 *
 * A `content` conflict still contributes its marker text to `changes`. A
 * caller recording the merge writes those markers into the tree — a
 * conflicted merge that wrote nothing would leave nothing to resolve — and a
 * caller that refuses to write on conflict discards `changes` whole, so the
 * markers cost it nothing.
 */
export const mergeTrees = Effect.fn("Merge.mergeTrees")(function* (input: {
  readonly base: ReadonlyMap<string, TreeSideFile>;
  readonly ours: ReadonlyMap<string, TreeSideFile>;
  readonly theirs: ReadonlyMap<string, TreeSideFile>;
  readonly strategy?: Strategy;
  readonly read: (oid: Oid) => Effect.Effect<Uint8Array, ObjectNotFound | StorageFailure>;
}) {
  const strategy = input.strategy ?? "recursive";
  const conflicts: TreeConflict[] = [];
  const changes: TreeChange[] = [];

  for (const path of new Set([
    ...input.ours.keys(),
    ...input.theirs.keys(),
    ...input.base.keys(),
  ])) {
    const inBase = input.base.get(path);
    const mine = input.ours.get(path);
    const yours = input.theirs.get(path);

    // Same on both sides: no decision to make.
    if (mine?.oid === yours?.oid && mine?.mode === yours?.mode) continue;

    // Only they moved, so their version stands — including standing deleted.
    if (inBase?.oid === mine?.oid && inBase?.mode === mine?.mode) {
      changes.push(
        yours === undefined
          ? { path, content: null }
          : { path, content: yield* input.read(yours.oid), mode: yours.mode },
      );
      continue;
    }

    // Only we moved; ours is already the tree the changes apply to.
    if (inBase?.oid === yours?.oid && inBase?.mode === yours?.mode) continue;

    // Both moved, one of them to nothing. A path absent from the base and
    // from one side is unchanged on that side and was handled above, so the
    // base has it and this is an edit against a delete — no content to merge.
    if (mine === undefined || yours === undefined) {
      if (strategy === "ours") {
        if (mine === undefined) changes.push({ path, content: null });
        continue;
      }
      if (strategy === "theirs") {
        changes.push(
          yours === undefined
            ? { path, content: null }
            : { path, content: yield* input.read(yours.oid), mode: yours.mode },
        );
        continue;
      }
      conflicts.push({ path, reason: "modify/delete" });
      continue;
    }

    const ourBytes = yield* input.read(mine.oid);
    const theirBytes = yield* input.read(yours.oid);

    if (strategy === "ours") continue;
    if (strategy === "theirs") {
      changes.push({ path, content: theirBytes, mode: yours.mode });
      continue;
    }

    // Conflict markers only make sense in text; a binary file has to be
    // chosen by a human, so it is reported and ours is left in place.
    if (isBinary(ourBytes) || isBinary(theirBytes)) {
      conflicts.push({ path, reason: "binary" });
      continue;
    }

    const baseBytes = inBase === undefined ? new Uint8Array(0) : yield* input.read(inBase.oid);
    const merged = mergeText({
      base: treeDecoder.decode(baseBytes),
      ours: treeDecoder.decode(ourBytes),
      theirs: treeDecoder.decode(theirBytes),
    });

    if (merged.conflicted) {
      conflicts.push({ path, reason: inBase === undefined ? "add/add" : "content" });
    }
    changes.push({ path, content: treeEncoder.encode(merged.content), mode: mine.mode });
  }

  return { changes, conflicts };
});
