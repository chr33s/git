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
import { isGitlink, sameMode } from "./Format.ts";
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

  // A file that ended without a newline still does: `splitLines` drops the
  // distinction, so it is carried here from the inputs. Appending one changes
  // every byte-for-byte comparison downstream of the merge.
  //
  // Which input to carry it from is the same three-way question the content
  // answers, so it is answered the same way: the side that changed the
  // terminator decides, and if both changed it they agree. An `||` over all
  // three — what this was — restores a newline both sides deliberately
  // removed, because the base still had one.
  //
  // A conflict that reaches the end of the file ends with a marker line, and
  // a marker without its newline would run into whatever follows it.
  const ourEnd = input.ours.endsWith("\n");
  const theirEnd = input.theirs.endsWith("\n");
  const baseEnd = input.base.endsWith("\n");
  const lastRegion = regions.at(-1);
  const terminated =
    (conflicted && lastRegion?.ok === false) ||
    (ourEnd === theirEnd ? ourEnd : ourEnd === baseEnd ? theirEnd : ourEnd);
  const joined = out.join("\n");
  return {
    content: out.length === 0 ? "" : terminated ? `${joined}\n` : joined,
    conflicted,
  };
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
  /** `null` removes the path, unless `oid` names what belongs there. */
  readonly content: Uint8Array | null;
  /** The entry's object when it has no content here — a gitlink. */
  readonly oid?: Oid;
  readonly mode?: string;
}

const treeEncoder = new TextEncoder();
/**
 * `ignoreBOM` keeps a leading EF BB BF as a character instead of eating it.
 *
 * A decoder that swallows the BOM never encodes it back, so the round-trip
 * below came up three bytes short and every Windows-authored file in the
 * repository was reported as binary and refused a merge — and had the guard
 * passed it anyway, the merge would have written the file back with its BOM
 * removed.
 */
const treeDecoder = new TextDecoder("utf-8", { ignoreBOM: true });

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
    if (mine?.oid === yours?.oid && sameMode(mine?.mode, yours?.mode)) continue;

    /**
     * One side's entry as a change, reading its bytes only if it has any.
     *
     * A gitlink names a commit in another repository. Reading it fails —
     * the object is not here — so it travels as the oid it already is, and
     * the tree writer puts that oid back rather than hashing a blob.
     */
    const taking = (side: TreeSideFile) =>
      isGitlink(side.mode)
        ? Effect.succeed({ path, content: null, oid: side.oid, mode: side.mode } as TreeChange)
        : Effect.map(input.read(side.oid), (content): TreeChange => ({
            path,
            content,
            mode: side.mode,
          }));

    // Only they moved, so their version stands — including standing deleted.
    if (inBase?.oid === mine?.oid && sameMode(inBase?.mode, mine?.mode)) {
      changes.push(yours === undefined ? { path, content: null } : yield* taking(yours));
      continue;
    }

    // Only we moved; ours is already the tree the changes apply to.
    if (inBase?.oid === yours?.oid && sameMode(inBase?.mode, yours?.mode)) continue;

    // Both moved, one of them to nothing. A path absent from the base and
    // from one side is unchanged on that side and was handled above, so the
    // base has it and this is an edit against a delete — no content to merge.
    if (mine === undefined || yours === undefined) {
      if (strategy === "ours") {
        if (mine === undefined) changes.push({ path, content: null });
        continue;
      }
      if (strategy === "theirs") {
        changes.push(yours === undefined ? { path, content: null } : yield* taking(yours));
        continue;
      }
      conflicts.push({ path, reason: "modify/delete" });
      continue;
    }

    // Both sides moved a submodule, and there is no content here to merge:
    // which commit the submodule should be at is a question only the person
    // who owns both repositories can answer. Ours stays put, as with a binary.
    if (isGitlink(mine.mode) || isGitlink(yours.mode)) {
      if (strategy === "theirs") changes.push(yield* taking(yours));
      else if (strategy !== "ours") conflicts.push({ path, reason: "binary" });
      continue;
    }

    // Decided before the reads, not after: a strategy that picks a side
    // already knows the answer, and reading both blobs to throw both away is
    // two object reads per changed path across the whole tree.
    if (strategy === "ours") continue;
    if (strategy === "theirs") {
      changes.push(yield* taking(yours));
      continue;
    }

    const ourBytes = yield* input.read(mine.oid);
    const theirBytes = yield* input.read(yours.oid);

    // Conflict markers only make sense in text; a binary file has to be
    // chosen by a human, so it is reported and ours is left in place.
    if (isBinary(ourBytes) || isBinary(theirBytes)) {
      conflicts.push({ path, reason: "binary" });
      continue;
    }

    // And "text" here means text this can round-trip. A Latin-1 `.po` or
    // `.properties` file has no NUL, so it passes the binary check, but
    // decoding replaces every high byte with U+FFFD and the merge would
    // write that back — renaming nothing and corrupting everything. Better
    // reported as a conflict a person resolves than silently rewritten.
    const decodable = (bytes: Uint8Array) => {
      // Byte for byte: an invalid three-byte run decodes to one U+FFFD, which
      // re-encodes to exactly three bytes — so comparing lengths called that
      // content clean and the merge wrote the replacement characters back.
      const round = treeEncoder.encode(treeDecoder.decode(bytes));
      if (round.length !== bytes.length) return false;
      for (let at = 0; at < bytes.length; at++) {
        if (round[at] !== bytes[at]) return false;
      }
      return true;
    };
    if (!decodable(ourBytes) || !decodable(theirBytes)) {
      conflicts.push({ path, reason: "binary" });
      continue;
    }

    const baseBytes = inBase === undefined ? new Uint8Array(0) : yield* input.read(inBase.oid);
    const merged = mergeText({
      base: treeDecoder.decode(baseBytes),
      ours: treeDecoder.decode(ourBytes),
      theirs: treeDecoder.decode(theirBytes),
    });

    // The mode merges the same way the content does. Always taking ours drops
    // a `chmod +x` made only on the incoming side — the merge reports success
    // and the result is a script that will not run — and hides a genuine
    // mode/mode disagreement by resolving it silently.
    const mode = sameMode(yours.mode, mine.mode)
      ? mine.mode
      : inBase !== undefined && sameMode(mine.mode, inBase.mode)
        ? yours.mode
        : mine.mode;
    const modeClash =
      !sameMode(yours.mode, mine.mode) &&
      (inBase === undefined ||
        (!sameMode(mine.mode, inBase.mode) && !sameMode(yours.mode, inBase.mode)));

    // One entry per path, whatever went wrong with it. A file whose content
    // *and* mode both disagreed was listed twice, so a caller counting
    // conflicts counted one file as two and a caller resolving them by path
    // met the same path a second time with nothing left to do.
    if (merged.conflicted || modeClash) {
      conflicts.push({
        path,
        reason: merged.conflicted && inBase === undefined ? "add/add" : "content",
      });
    }
    changes.push({ path, content: treeEncoder.encode(merged.content), mode });
  }

  return { changes, conflicts };
});
