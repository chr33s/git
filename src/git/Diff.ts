/**
 * Line diffing: the matcher, the hunks it groups into, and unified output.
 *
 * Pure string work with no I/O, so plain functions like `Format.ts` — and
 * nothing here returns `Result`, because there is no input a caller could get
 * wrong: any two strings have a diff.
 *
 * The matcher is Myers' O(ND) algorithm, not the textbook O(NM) LCS table. D
 * is the edit distance, so a small change to a large file costs almost
 * nothing, and the table's quadratic *memory* is what makes the naive version
 * unusable on real files rather than its time.
 */

export interface Hunk {
  /** 0-based index into `before`; `unified` is what adds git's 1. */
  readonly oldStart: number;
  readonly oldLines: ReadonlyArray<string>;
  readonly newStart: number;
  readonly newLines: ReadonlyArray<string>;
}

export interface UnifiedOptions {
  readonly context?: number;
  readonly beforeName?: string;
  readonly afterName?: string;
}

/**
 * Walk the saved rows back from the end point, collecting the diagonal moves —
 * those are the matched pairs. Row `d` is the frontier *before* round `d` ran,
 * which is exactly what decides where round `d` came from.
 */
const backtrack = (
  trace: ReadonlyArray<Int32Array>,
  n: number,
  m: number,
): ReadonlyArray<readonly [number, number]> => {
  const pairs: Array<readonly [number, number]> = [];
  let x = n;
  let y = m;
  let d = trace.length;

  for (const row of trace.toReversed()) {
    d--;
    // Row `d` holds diagonals -d..d at offset d; anything outside is untouched.
    const reach = (k: number) => row[k + d] ?? 0;
    const k = x - y;
    const down = k === -d || (k !== d && reach(k - 1) < reach(k + 1));
    const previousK = down ? k + 1 : k - 1;
    const previousX = reach(previousK);
    const previousY = previousX - previousK;

    while (x > previousX && y > previousY) {
      x--;
      y--;
      pairs.push([x, y]);
    }
    x = previousX;
    y = previousY;
  }

  return pairs.reverse();
};

/**
 * How far the search will go before giving up.
 *
 * The trace costs O(D²) where D is the edit distance, so the bound has to be
 * on D — not on the file size. Two 5,000-line files differing by one line have
 * D = 2 and diff exactly; two that share nothing have D ≈ n + m and stop here.
 * Bounding `n + m` up front, as an earlier version did, called every large
 * file a rewrite and turned ordinary merges into whole-file conflicts.
 */
const MAX_EDITS = 2_000;

/** The Myers trace, or `null` when the edit distance ran past the bound. */
const myers = (
  a: ReadonlyArray<string>,
  b: ReadonlyArray<string>,
): ReadonlyArray<readonly [number, number]> | null => {
  const n = a.length;
  const m = b.length;
  if (n === 0 || m === 0) return [];

  const max = n + m;
  const frontier = new Int32Array(2 * max + 1);
  // Each round is saved trimmed to the diagonals it could reach, which keeps
  // the trace O(D²) rather than O(D(N+M)).
  const trace: Array<Int32Array> = [];

  // Bounded by the edit distance: past `MAX_EDITS` the two sides share little
  // enough that "rewritten" is the honest answer, and the trace stops growing.
  for (let d = 0; d <= Math.min(max, MAX_EDITS); d++) {
    trace.push(frontier.slice(max - d, max + d + 1));

    for (let k = -d; k <= d; k += 2) {
      const left = frontier[max + k - 1] ?? 0;
      const right = frontier[max + k + 1] ?? 0;
      const down = k === -d || (k !== d && left < right);
      let x = down ? right : left + 1;
      let y = x - k;

      // The snake: equal lines are free, and taking all of them is what makes
      // the algorithm proportional to the edit distance.
      while (x < n && y < m && a[x] === b[y]) {
        x++;
        y++;
      }

      frontier[max + k] = x;
      if (x >= n && y >= m) return backtrack(trace, n, m);
    }
  }

  return null;
};

/**
 * Matched index pairs, ascending in both coordinates.
 *
 * The equal lines at the two ends are matched directly, before the search
 * runs. That is not only faster — it is what keeps the bound above from
 * costing correctness. Returning no matches at all when the search ran long,
 * which is what this used to do, told `diff3` the two files had nothing in
 * common: a 100-line append to a 4,000-line file came back as a whole-file
 * rewrite, and a merge of two edits at opposite ends of one became a
 * whole-file conflict where git merges it cleanly. Now the search giving up
 * costs the detail in the middle and nothing else.
 */
export const lcs = (
  a: ReadonlyArray<string>,
  b: ReadonlyArray<string>,
): ReadonlyArray<readonly [number, number]> => {
  if (a.length === 0 || b.length === 0) return [];

  const shortest = Math.min(a.length, b.length);
  let head = 0;
  while (head < shortest && a[head] === b[head]) head++;

  let tail = 0;
  while (tail < shortest - head && a[a.length - 1 - tail] === b[b.length - 1 - tail]) tail++;

  const pairs: Array<readonly [number, number]> = [];
  for (let index = 0; index < head; index++) pairs.push([index, index]);

  const middle = myers(a.slice(head, a.length - tail), b.slice(head, b.length - tail));
  if (middle !== null) {
    for (const [x, y] of middle) pairs.push([x + head, y + head]);
  }

  for (let index = 0; index < tail; index++) {
    pairs.push([a.length - tail + index, b.length - tail + index]);
  }

  return pairs;
};

/** The changed regions only — no context, and no entry for equal lines. */
export const diffLines = (
  before: ReadonlyArray<string>,
  after: ReadonlyArray<string>,
): ReadonlyArray<Hunk> => {
  const hunks: Array<Hunk> = [];
  let oldIndex = 0;
  let newIndex = 0;

  const change = (oldEnd: number, newEnd: number) => {
    if (oldEnd === oldIndex && newEnd === newIndex) return;
    hunks.push({
      oldStart: oldIndex,
      oldLines: before.slice(oldIndex, oldEnd),
      newStart: newIndex,
      newLines: after.slice(newIndex, newEnd),
    });
  };

  for (const [oldMatch, newMatch] of lcs(before, after)) {
    change(oldMatch, newMatch);
    oldIndex = oldMatch + 1;
    newIndex = newMatch + 1;
  }
  change(before.length, after.length);

  return hunks;
};

/**
 * Lines without their terminators, with a trailing newline read as a
 * terminator and not a separator: `"a\nb\n"` and `"a\nb"` both give
 * `["a", "b"]`, and `""` gives `[]` rather than `[""]`. The difference between
 * those two is recovered by `unified` from the raw text, which is where git's
 * "\ No newline at end of file" comes from. A `\r` stays on the line it ends,
 * because git does not strip it either.
 */
export const splitLines = (text: string): ReadonlyArray<string> => {
  if (text === "") return [];
  const lines = text.split("\n");
  if (lines[lines.length - 1] === "") lines.pop();
  return lines;
};

/** `-l,c` — git omits the count for a single line and points 0-length ranges at the line before. */
const range = (start: number, count: number): string => {
  if (count === 1) return `${start + 1}`;
  return `${count === 0 ? start : start + 1},${count}`;
};

const NO_NEWLINE = "\\ No newline at end of file";

/**
 * Put the terminators back before matching, so a file ending without a newline
 * genuinely differs from one ending with it — the distinction `splitLines`
 * drops, and the only reason git has a "\ No newline" line at all.
 */
const terminated = (lines: ReadonlyArray<string>, missing: boolean): ReadonlyArray<string> =>
  lines.map((line, index) => (missing && index === lines.length - 1 ? line : `${line}\n`));

export const unified = (before: string, after: string, options?: UnifiedOptions): string => {
  const context = options?.context ?? 3;
  const beforeLines = splitLines(before);
  const afterLines = splitLines(after);
  const missingBefore = before !== "" && !before.endsWith("\n");
  const missingAfter = after !== "" && !after.endsWith("\n");
  const hunks = diffLines(
    terminated(beforeLines, missingBefore),
    terminated(afterLines, missingAfter),
  );
  if (hunks.length === 0) return "";

  const beforeName = options?.beforeName ?? "file";
  const afterName = options?.afterName ?? beforeName;
  const out = [`--- a/${beforeName}`, `+++ b/${afterName}`];

  // Two changes closer than twice the context share a hunk, because their
  // context runs would otherwise overlap and print lines twice.
  const groups: Array<{
    readonly hunks: Array<Hunk>;
    readonly oldFrom: number;
    readonly newFrom: number;
    oldTo: number;
    newTo: number;
  }> = [];
  let group: (typeof groups)[number] | undefined;
  for (const hunk of hunks) {
    if (group === undefined || hunk.oldStart - group.oldTo > context * 2) {
      group = {
        hunks: [],
        oldFrom: hunk.oldStart,
        newFrom: hunk.newStart,
        oldTo: hunk.oldStart,
        newTo: hunk.newStart,
      };
      groups.push(group);
    }
    group.hunks.push(hunk);
    group.oldTo = hunk.oldStart + hunk.oldLines.length;
    group.newTo = hunk.newStart + hunk.newLines.length;
  }

  for (const entry of groups) {
    // The runs on either side of a change have the same length in both files,
    // so one `min` keeps the two ranges aligned.
    const lead = Math.min(context, entry.oldFrom, entry.newFrom);
    const trail = Math.min(
      context,
      beforeLines.length - entry.oldTo,
      afterLines.length - entry.newTo,
    );
    const oldStart = entry.oldFrom - lead;
    const newStart = entry.newFrom - lead;
    const oldEnd = entry.oldTo + trail;
    const newEnd = entry.newTo + trail;

    out.push(`@@ -${range(oldStart, oldEnd - oldStart)} +${range(newStart, newEnd - newStart)} @@`);

    let oldIndex = oldStart;
    let newIndex = newStart;
    // The hunks index the terminated copies, so the text printed comes back
    // out of the split lines at those indices.
    for (const hunk of entry.hunks) {
      for (const line of beforeLines.slice(oldIndex, hunk.oldStart)) out.push(` ${line}`);
      newIndex += hunk.oldStart - oldIndex;
      oldIndex = hunk.oldStart + hunk.oldLines.length;

      for (const line of beforeLines.slice(hunk.oldStart, oldIndex)) out.push(`-${line}`);
      if (hunk.oldLines.length > 0 && oldIndex === beforeLines.length && missingBefore) {
        out.push(NO_NEWLINE);
      }

      newIndex += hunk.newLines.length;
      for (const line of afterLines.slice(hunk.newStart, newIndex)) out.push(`+${line}`);
      if (hunk.newLines.length > 0 && newIndex === afterLines.length && missingAfter) {
        out.push(NO_NEWLINE);
      }
    }

    for (const line of beforeLines.slice(oldIndex, oldEnd)) out.push(` ${line}`);
    // A shared last line can only be marked when both files lack the newline;
    // if just one does, the lines are not really equal and git says so with a
    // change we cannot see, having split the terminator off.
    if (trail > 0 && oldEnd === beforeLines.length && missingBefore && missingAfter) {
      out.push(NO_NEWLINE);
    }
  }

  return `${out.join("\n")}\n`;
};

/** git's own heuristic (`buffer_is_binary`): a NUL byte in the first 8000. */
export const isBinary = (bytes: Uint8Array): boolean => bytes.subarray(0, 8000).includes(0);

/**
 * 0..1, for rename detection. Common lines are counted against the totals of
 * both files, so the score is symmetric — a rename scores the same whichever
 * way round it is inspected — and reaches 1 only when the files are
 * line-for-line identical.
 */
export const similarity = (a: string, b: string): number => {
  const left = splitLines(a);
  const right = splitLines(b);
  const total = left.length + right.length;
  if (total === 0) return 1;
  return (2 * lcs(left, right).length) / total;
};
