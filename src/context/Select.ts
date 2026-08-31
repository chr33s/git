/**
 * The default selector: one replaceable answer to "what is worth exposing?".
 *
 * Everything here is outside the protocol on purpose (docs/context-pack.md
 * §12). Lexical search over the view is what this repository already has an
 * index for, and a better selector — symbol graphs, embeddings, a model
 * choosing for itself — is a drop-in replacement precisely because nothing in
 * `Pack.ts`, `Render.ts` or `Exposure.ts` knows this file exists. What a
 * verifier needs is that the items resolve; how they were chosen is a
 * diagnostic.
 *
 * The one thing it is careful about is the boundary. Ranges are snapped to
 * codepoint boundaries before they are recorded, because a range that cuts a
 * multi-byte character makes the evidence unrenderable as text (§5.1); and
 * omission diagnostics are reported per path only when the caller says its
 * reader may see the whole view, since naming a filtered path is naming
 * repository structure (§6.1).
 */
import { Effect } from "effect";

import { isBinary } from "../git/Diff.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { isGitlink } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Pack from "./Pack.ts";

/** Recorded as `selector` so a reader can tell which implementation chose. */
export const NAME = "repo-context";
export const VERSION = "1.0.0";

/** How many items, and how many evidence bytes, one pack may be built with. */
export const MAX_ITEMS = 32;
export const MAX_BYTES = 64 * 1024;

/**
 * The most evidence a caller may ask for, whatever the default is.
 *
 * The render is retained on an append-only ref this version cannot delete and
 * replicates to every clone, and nothing else bounds it — `Render` caps a
 * segment and a segment count, `Trace.MAX_PAYLOAD` covers only `event.json`.
 * So the ceiling belongs where the budget is chosen.
 */
export const MAX_EVIDENCE = 4 * 1024 * 1024;

/** How large a blob this selector will read at all. */
const MAX_FILE_BYTES = 1024 * 1024;

/**
 * How many omissions a pack will name individually before counting them.
 *
 * Omissions are non-exhaustive by definition (§6.1), so a cap costs nothing a
 * reader is entitled to — and without one the diagnostics can be larger than
 * the evidence they are diagnostics for.
 */
const MAX_NAMED_OMISSIONS = 64;

/** Lines of context kept either side of a match, in the recorded range. */
const CONTEXT_LINES = 4;

/**
 * Files whose contents carry repository-derived instruction authority here.
 *
 * A short, literal list rather than a pattern, because §7 makes this a claim a
 * producer has to be able to back: `authority.path` must resolve under
 * `view.tree` to the very blob the item names, and a rule that guessed would
 * be a rule producing claims that do not verify.
 */
const INSTRUCTIONS = ["AGENTS.md", "CLAUDE.md"];

export interface Options {
  readonly task: string;
  readonly view: Pack.View;
  readonly maxItems?: number;
  readonly maxBytes?: number;
  /**
   * Whether an omission may name the path it is about.
   *
   * `aggregate` is the setting for an invocation that cannot see the whole
   * view: a count and a reason say that something was left out without saying
   * what, which is what §6.1 requires when naming it would reveal structure
   * the reader has no access to.
   */
  readonly diagnostics?: "path" | "aggregate";
}

/**
 * The terms a task is searched for.
 *
 * Deliberately dull: split on non-word characters, drop what is too short to
 * discriminate, and keep the order the operator typed so the ranking is
 * reproducible. A stemmer or a synonym list would make the *selection* better
 * and the *explanation* worse, and the explanation is what this surface is for.
 */
export const terms = (task: string): ReadonlyArray<string> => {
  const seen = new Set<string>();
  for (const word of task.split(/[^\p{L}\p{N}_]+/u)) {
    const term = word.toLowerCase();
    if (term.length < 3 || seen.has(term)) continue;
    seen.add(term);
  }
  return [...seen].slice(0, 16);
};

/** Byte offset of the start of each 1-based line, plus the end of the blob. */
const lineOffsets = (bytes: Uint8Array): ReadonlyArray<number> => {
  const offsets: Array<number> = [0];
  for (const [index, byte] of bytes.entries()) if (byte === 0x0a) offsets.push(index + 1);
  return offsets;
};

interface Candidate {
  readonly path: string;
  /** How many distinct task terms hit this file. */
  readonly terms: number;
  readonly matches: number;
  readonly lines: ReadonlyArray<number>;
}

/**
 * Rank the view's files against a task.
 *
 * Distinct terms first, then match count, then path. Matching two of the
 * operator's words in one file is a better signal than matching one of them
 * twice, and the path tiebreak is what makes two runs over one tree produce
 * the same pack.
 */
/**
 * Whether a text refers to a submodule at this path, rather than merely
 * containing its name.
 *
 * A bare `includes` was recording unbacked claims in a signed, immutable pack.
 * For a submodule at `lib`, any selected file carrying `#include <stdlib.h>`,
 * the word `library`, or a path like `src/lib/x.ts` made the pack assert that
 * the exposed evidence referenced that submodule commit, when nothing did —
 * and `Pack.checkItem` verifies mode and oid, so the claim verifies and is
 * permanent on a ref nothing can rewind. It also cost an item slot, so the
 * spurious gitlink crowded out evidence somebody asked for.
 *
 * A path boundary is what separates the two: the match may not continue a path
 * segment on either side. A following `/` is a reference *into* the submodule
 * and counts; a preceding one means the name is a segment of some other path
 * and does not.
 */
const PATH_CHARACTER = /[A-Za-z0-9_./-]/;
const refers = (text: string, path: string): boolean => {
  for (let at = text.indexOf(path); at !== -1; at = text.indexOf(path, at + 1)) {
    // The whole path-shaped token around the match, and then the relative
    // prefixes a path may legitimately carry stripped off the front of it. The
    // path has to be what is left: `../vendor/policy-engine/index.ts` refers to
    // the submodule at `vendor/policy-engine`, and `src/lib/x.ts` does not
    // refer to one at `lib` — the name is a segment of somebody else's path.
    let start = at;
    while (start > 0 && PATH_CHARACTER.test(text.charAt(start - 1))) start -= 1;
    while (text.startsWith("./", start) || text.startsWith("../", start)) {
      start += text.startsWith("../", start) ? 3 : 2;
    }
    if (start !== at) continue;

    // And it has to end at a boundary too. A following `/` is a reference
    // *into* the submodule and counts; anything else a path segment can
    // continue with means this is a longer name that begins the same way, of
    // which `library` and `stdlib.h` are the ones that actually turned up.
    const after = text.charAt(at + path.length);
    if (after === "/" || !PATH_CHARACTER.test(after)) return true;
  }
  return false;
};

const candidates = Effect.fn("context.Select.candidates")(function* (
  tree: Oid,
  words: ReadonlyArray<string>,
) {
  const repository = yield* Repository;
  const found = new Map<string, { terms: Set<string>; matches: number; lines: Set<number> }>();
  // What the search itself could not look at. A blob it marked too large is a
  // file that was considered and not read, which is precisely what §6.1's
  // vocabulary is for — and a pack that said nothing about it understated what
  // was left out. `truncated` is the other half: the walk stops at the match
  // cap, so files past it were never reached at all.
  const skipped = new Set<string>();
  let truncated = false;

  for (const word of words) {
    const result = yield* repository
      .search({ ref: tree, pattern: word, fixed: true, ignoreCase: true })
      // A term the index cannot serve is one term, not a failed selection: the
      // pack is allowed to be worse, and is not allowed to be a failure the
      // operator has to decode.
      .pipe(Effect.catchTag("Invalid", () => Effect.succeed(null)));
    if (result === null) continue;
    for (const path of result.skipped) skipped.add(path);
    truncated ||= result.truncated;

    for (const match of result.matches) {
      const entry = found.get(match.path) ?? { terms: new Set(), matches: 0, lines: new Set() };
      entry.terms.add(word);
      entry.matches += 1;
      entry.lines.add(match.line);
      found.set(match.path, entry);
    }
  }

  const ranked = [...found]
    .map(([path, entry]): Candidate => ({
      path,
      terms: entry.terms.size,
      matches: entry.matches,
      lines: [...entry.lines].sort((left, right) => left - right),
    }))
    .sort(
      (left, right) =>
        right.terms - left.terms ||
        right.matches - left.matches ||
        // Code-unit order, not `localeCompare`: collation depends on the
        // host's ICU build, so two hosts ranked equal candidates differently
        // and produced different pack bytes — and a pack's bytes are its
        // identity. `Format.encodeTree` compares bytes for the same reason.
        (left.path < right.path ? -1 : left.path > right.path ? 1 : 0),
    );
  return { ranked, skipped: [...skipped].sort(), truncated } as const;
});

/**
 * The byte range covering a file's matches, with a little context around them.
 *
 * One range rather than one item per match: a pack with eleven items for one
 * file is a pack nobody reads, and the ranges would overlap anyway. Snapped to
 * codepoint boundaries last, so a file whose matches sit inside a multi-byte
 * character still yields evidence a text renderer can hand over intact.
 */
const rangeOf = (
  bytes: Uint8Array,
  lines: ReadonlyArray<number>,
  budget: number,
): readonly [number, number] | null => {
  if (bytes.length <= budget) return null;

  const offsets = lineOffsets(bytes);
  const first = Math.max(1, (lines[0] ?? 1) - CONTEXT_LINES);
  const last = Math.min(offsets.length, (lines.at(-1) ?? 1) + CONTEXT_LINES);

  // The context lines are dropped before the matched line is, because a range
  // that holds none of the matches is not what `reason: "search"` claims. The
  // tail was truncated to the budget with the leading four lines still in
  // front of it, so a tight budget on a file of long lines yielded a window
  // that stopped before the first match — signed permanently as search
  // evidence containing no search term.
  const matched = offsets[(lines[0] ?? 1) - 1] ?? 0;
  const after = offsets[lines[0] ?? 1] ?? bytes.length;
  const wanted = offsets[first - 1] ?? 0;
  // Room for the leading context *and* the matched line, not just the
  // context. Compared against the context alone, a budget exactly equal to it
  // took the `wanted` branch and `end` then collapsed to `matched`: the range
  // held the four lines before the match and stopped there — signed
  // permanently as `reason: "search"` evidence containing no search term,
  // which is the failure this line exists to prevent.
  const start = matched - wanted + (after - matched) <= budget ? wanted : matched;
  const end = Math.min(offsets[last] ?? bytes.length, start + budget);
  // A range that ends where it starts is not evidence (§5.1); a file whose
  // matched line is longer than the budget still has to hand over something.
  const snapped = Pack.snap(bytes, start, Math.max(start + 1, end));
  return snapped[0] === 0 && snapped[1] === bytes.length ? null : snapped;
};

/**
 * Build a pack for a task against one view.
 *
 * The view is an argument rather than something captured here, because a pack
 * and the exposure that carries it must resolve against the *same* snapshot:
 * capturing again inside the selector would let the work tree move between the
 * two and produce a pack whose items belong to a tree the record does not
 * retain.
 */
export const select = Effect.fn("context.Select.select")(function* (options: Options) {
  const repository = yield* Repository;
  const maxItems = options.maxItems ?? MAX_ITEMS;
  const maxBytes = options.maxBytes ?? MAX_BYTES;
  const namesPaths = (options.diagnostics ?? "path") === "path";

  const tree = Pack.unqualify(options.view.tree);
  if (tree === null) {
    return yield* new Invalid({
      field: "view",
      reason: `'${options.view.tree}' is not an object id`,
    });
  }

  const files = yield* repository.listFiles(tree);
  const blobs = new Map(files.filter((file) => !isGitlink(file.mode)).map((f) => [f.path, f]));

  const items: Array<Pack.Item> = [];
  const named: Array<Pack.Omission> = [];
  /**
   * How many paths each reason accounted for, for the entries not named.
   *
   * Counted per reason rather than totalled, because the reason is the whole
   * content of an aggregate diagnostic: rolling `budget` and `unavailable`
   * into one `filtered` count states, in a signed record, that a content
   * filter removed things a budget removed.
   */
  const counted = new Map<string, number>();
  /** Reasons whose extent is not known; recorded without a count (§6.1). */
  const unbounded = new Set<string>();
  const omit = (path: string, reason: string) => {
    // Bounded like the items are. Once the budget is spent every remaining
    // candidate is an omission, and `candidates()` returns one entry per file
    // matching any task term — a common word on a large repository is
    // thousands of paths, which is a pack that outgrows `MAX_PAYLOAD` after
    // the selection work is already done.
    if (namesPaths && named.length < MAX_NAMED_OMISSIONS) {
      named.push({ path, reason });
      return;
    }
    counted.set(reason, (counted.get(reason) ?? 0) + 1);
  };

  // The standing instructions first, and always: they are the one selection a
  // task cannot outrank, because an agent told to ignore them was still told
  // them and a pack that left them out would describe an exposure that did not
  // happen.
  //
  // Bounded on both sides, because each side alone is its own bug. Uncounted,
  // a two-megabyte `AGENTS.md` went into the pack and the render whole while
  // the search still received its full budget — a pack asked for a kilobyte
  // carrying two megabytes. Counted without a share of their own, a seventy-
  // kilobyte one ate a sixty-four-kilobyte budget outright and the pack came
  // back with the instructions and nothing else, every task-relevant file
  // omitted as `budget`. So instructions may take at most half, and one that
  // does not fit is recorded as an omission rather than as evidence — which
  // is the honest account of what this selector did with it.
  const chosen = new Set<string>();
  const share = Math.max(1, Math.floor(maxBytes / 2));
  let instructed = 0;
  for (const path of INSTRUCTIONS) {
    const file = blobs.get(path);
    if (file === undefined) continue;
    // Claimed however it turns out, so the candidate loop below does not
    // reconsider it. Left unclaimed, one signed pack said both things about
    // one path: an omission recording that `AGENTS.md` was cut for budget,
    // and an item recording it as `implementation / search` — with the §7
    // `authority` annotation dropped and only a truncated range exposed, so
    // the instruction claim the record exists to back was silently lost.
    chosen.add(path);
    if (items.length >= maxItems) {
      omit(path, "budget");
      continue;
    }

    const bytes = yield* repository
      .readBlob(file.oid)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (bytes === null) {
      omit(path, "unavailable");
      continue;
    }
    // Not text, or larger than this selector reads at all: an omission rather
    // than evidence, because `render` would otherwise frame bytes it never
    // looked at.
    if (bytes.length > MAX_FILE_BYTES || isBinary(bytes)) {
      omit(path, "filtered");
      continue;
    }
    if (instructed + bytes.length > share) {
      omit(path, "budget");
      continue;
    }

    instructed += bytes.length;
    items.push({
      kind: "blob",
      path,
      blob: Pack.qualify(file.oid),
      role: "instruction",
      reason: "instruction",
      authority: { source: "repository-instructions", root: options.view.tree, path },
    });
  }

  let spent = instructed;

  const searched = yield* candidates(tree, terms(options.task));
  // The search stopped at its match cap, so every file past the cut-off went
  // unreached. Recorded with no count, because the count is exactly what is
  // not known — a `1` here would have claimed one path was left out.
  if (searched.truncated) unbounded.add("other");

  for (const candidate of searched.ranked) {
    if (chosen.has(candidate.path)) continue;
    const file = blobs.get(candidate.path);
    if (file === undefined) continue;

    if (items.length >= maxItems || spent >= maxBytes) {
      omit(candidate.path, "budget");
      continue;
    }

    const bytes = yield* repository
      .readBlob(file.oid)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    // Reported, never substituted for: §5.3 makes an unavailable object a
    // condition to state rather than a gap to fill with different bytes.
    if (bytes === null) {
      omit(candidate.path, "unavailable");
      continue;
    }
    if (bytes.length > MAX_FILE_BYTES || isBinary(bytes)) {
      omit(candidate.path, "filtered");
      continue;
    }

    const range = rangeOf(bytes, candidate.lines, maxBytes - spent);
    spent += range === null ? bytes.length : range[1] - range[0];
    chosen.add(candidate.path);
    const blob = Pack.qualify(file.oid);
    const path = candidate.path;
    items.push(
      range === null
        ? { kind: "blob", path, blob, role: "implementation", reason: "search" }
        : { kind: "blob", path, blob, range, role: "implementation", reason: "search" },
    );
  }

  // And what the index refused to read, last of all. `search` reports every
  // blob in the tree over its own file cap, whether or not it matched a task
  // term — the size check happens before the match — so these are files that
  // were never candidates for this task at all. Named first, they took the 64
  // named slots in tree order: on a repository with that many large files,
  // every omission a reader was actually asking about (`budget`,
  // `unavailable`, a candidate this selector filtered) collapsed into an
  // anonymous count, while the signed pack named 64 paths nobody had asked
  // about — which is also the structure disclosure `diagnostics: "aggregate"`
  // exists to avoid.
  //
  // Still recorded, because a pack that said nothing about them would
  // understate what was left out; just after the candidates, so they take
  // whatever room is left rather than the room. Anything already accounted for
  // is skipped: an instruction file over this selector's own cap is also over
  // the index's, and was otherwise omitted twice under one reason.
  for (const path of searched.skipped) {
    if (chosen.has(path)) continue;
    chosen.add(path);
    omit(path, "filtered");
  }

  // Submodules the selected evidence actually points at. A gitlink proves only
  // that this repository named that commit — nothing from inside the submodule
  // was retrieved — so it earns its place by being referenced, not by existing.
  //
  // Skipped entirely when the view holds no submodules, which is every
  // repository without one: the scan below re-reads every selected blob and
  // decodes it, a second full copy of the evidence budget bought for nothing.
  // Byte order, not `listFiles`' `localeCompare`: when a view holds more
  // referenced submodules than the item budget, the cut keeps a prefix — and a
  // locale-dependent prefix means two hosts emit different pack bytes for the
  // same task and tree, which are the exposure's identity. `candidates` was
  // given a byte comparator for exactly this; the gitlink pass inherited the
  // other one.
  const gitlinks = files
    .filter((file) => isGitlink(file.mode))
    .sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
  const decoder = new TextDecoder();
  const mentions: Array<string> = [];
  for (const item of gitlinks.length === 0 ? [] : items) {
    if (item.kind !== "blob") continue;
    const bytes = yield* Pack.evidence(options.view, item).pipe(
      Effect.catchTags({
        Invalid: () => Effect.succeed(null),
        ObjectNotFound: () => Effect.succeed(null),
      }),
    );
    if (bytes !== null) mentions.push(decoder.decode(bytes));
  }
  const mentioned = mentions.join("\n");

  for (const file of gitlinks) {
    const asked = refers(options.task, file.path);
    if (!asked && !refers(mentioned, file.path)) continue;
    // Recorded, and every one of them — this was the only cut in the selector
    // that went unwritten, and it `break`s, so with `--max-items 1` and an
    // `AGENTS.md` present a submodule the task names by hand vanished in
    // silence while every search candidate was correctly accounted for. The
    // signed pack read as though no submodule had been considered.
    if (items.length >= maxItems) {
      omit(file.path, "budget");
      continue;
    }
    items.push({
      kind: "gitlink",
      path: file.path,
      commit: Pack.qualify(file.oid),
      role: "dependency",
      // `reference` rather than `import`: what was established is that the
      // path is named in the evidence, not that anything imports it, and the
      // vocabulary has a word for each.
      reason: asked ? "explicit" : "reference",
    });
  }

  const omissions: Array<Pack.Omission> = [...named];
  for (const [reason, count] of [...counted].sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  )) {
    omissions.push({ reason, count });
  }
  for (const reason of [...unbounded].sort()) omissions.push({ reason });

  const pack: Pack.Pack = {
    version: Pack.VERSION,
    view: options.view,
    selector: { name: NAME, version: VERSION },
    items,
  };
  return omissions.length === 0 ? pack : { ...pack, omissions };
});

/**
 * One segment per pack item, plus the task, as a harness would hand them over.
 *
 * A render is the harness's to build — the invocation is the harness's — but a
 * producer with no render has nothing to commit to, and a `context for` that
 * could not demonstrate the whole chain would leave §8 untested in practice.
 * Placement records where each segment sat and nothing about its authority:
 * repository instructions go in `system` because that is where a harness puts
 * them, not because being there makes them binding (§7).
 */
export const render = Effect.fn("context.Select.render")(function* (pack: Pack.Pack, task: string) {
  const encoder = new TextEncoder();
  const segments: Array<{
    readonly placement: string;
    readonly mediaType: string;
    readonly body: Uint8Array;
  }> = [];

  for (const item of pack.items) {
    if (item.kind === "gitlink") {
      segments.push({
        placement: "developer",
        mediaType: "text/plain; charset=utf-8",
        body: encoder.encode(`${item.path} @ ${item.commit}\n`),
      });
      continue;
    }
    segments.push({
      placement: item.role === "instruction" ? "system" : "developer",
      mediaType: "text/plain; charset=utf-8",
      body: yield* Pack.evidence(pack.view, item),
    });
  }

  segments.push({
    placement: "user",
    mediaType: "text/plain; charset=utf-8",
    body: encoder.encode(task),
  });
  return segments;
});

export type SelectError = Invalid | ObjectNotFound | StorageFailure;
