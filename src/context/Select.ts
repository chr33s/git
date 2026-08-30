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

/** How large a blob this selector will read at all. */
const MAX_FILE_BYTES = 1024 * 1024;

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
const candidates = Effect.fn("context.Select.candidates")(function* (
  tree: Oid,
  words: ReadonlyArray<string>,
) {
  const repository = yield* Repository;
  const found = new Map<string, { terms: Set<string>; matches: number; lines: Set<number> }>();

  for (const word of words) {
    const result = yield* repository
      .search({ ref: tree, pattern: word, fixed: true, ignoreCase: true })
      // A term the index cannot serve is one term, not a failed selection: the
      // pack is allowed to be worse, and is not allowed to be a failure the
      // operator has to decode.
      .pipe(Effect.catchTag("Invalid", () => Effect.succeed(null)));
    if (result === null) continue;

    for (const match of result.matches) {
      const entry = found.get(match.path) ?? { terms: new Set(), matches: 0, lines: new Set() };
      entry.terms.add(word);
      entry.matches += 1;
      entry.lines.add(match.line);
      found.set(match.path, entry);
    }
  }

  return [...found]
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
        left.path.localeCompare(right.path),
    );
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

  const start = offsets[first - 1] ?? 0;
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
  const named = (options.diagnostics ?? "path") === "path";

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
  const omissions: Array<Pack.Omission> = [];
  let aggregate = 0;
  const omit = (path: string, reason: string) => {
    if (named) omissions.push({ path, reason });
    else aggregate += 1;
  };

  // The standing instructions first, and always. They are the one selection a
  // task cannot outrank: an agent told to ignore them was still told them, and
  // a pack that left them out would describe an exposure that did not happen.
  const chosen = new Set<string>();
  for (const path of INSTRUCTIONS) {
    const file = blobs.get(path);
    if (file === undefined) continue;
    chosen.add(path);
    items.push({
      kind: "blob",
      path,
      blob: Pack.qualify(file.oid),
      role: "instruction",
      reason: "instruction",
      authority: { source: "repository-instructions", root: options.view.tree, path },
    });
  }

  let spent = 0;
  for (const candidate of yield* candidates(tree, terms(options.task))) {
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

  // Submodules the selected evidence actually points at. A gitlink proves only
  // that this repository named that commit — nothing from inside the submodule
  // was retrieved — so it earns its place by being referenced, not by existing.
  const decoder = new TextDecoder();
  const mentions: Array<string> = [];
  for (const item of items) {
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

  for (const file of files) {
    if (!isGitlink(file.mode)) continue;
    if (items.length >= maxItems) break;
    const referenced = options.task.includes(file.path) || mentioned.includes(file.path);
    if (!referenced) continue;
    items.push({
      kind: "gitlink",
      path: file.path,
      commit: Pack.qualify(file.oid),
      role: "dependency",
      reason: options.task.includes(file.path) ? "explicit" : "import",
    });
  }

  if (aggregate > 0) omissions.push({ reason: "filtered", count: aggregate });

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
