/**
 * Three-way merge.
 *
 * diff3: match each side against the base, and the lines *both* sides matched
 * are the only places a region can safely begin or end. Between two of those
 * sync points sits one region seen three ways — base, ours, theirs — and the
 * whole merge is the four-case decision made on each of them.
 *
 * A conflict is a value, not a failure: an unmergeable region is a normal
 * outcome that the caller writes to a file with markers in it, and the caller
 * still wants the regions that did merge.
 */
import { lcs, splitLines } from "./Diff.ts";

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
