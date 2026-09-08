/**
 * Task-specific recall: finding the Concept that is relevant *now*, and the
 * current repository evidence that makes it readable.
 *
 * ```text
 * task terms
 *    ↓
 * Concept metadata, then bounded bodies      ← lexical, deliberately dull
 *    ↓
 * eligibility under this view and trust state
 *    ↓
 * selection group: the Concept + its declared dependencies
 * ```
 *
 * Two invariants shape everything here. **The startup note is not the corpus**
 * (INV-09): a rare learning that never fit into Memory has to be findable at
 * the moment it matters, so this searches the bundle in the view rather than
 * the note. And **a Concept travels with its support** (§9.2): recalling prose
 * that summarizes a test file, without the current bytes of that file, is how
 * stale prose comes to stand in for implementation. A group that does not fit
 * the budget is omitted whole, with a diagnostic, rather than admitted as
 * apparently-supported prose.
 *
 * The ranking is lexical and replaceable. It exists to be explainable, not to
 * be clever: BM25, embeddings and graph expansion are all out of scope, and
 * §1 makes a better selector something that has to demonstrate improvement at
 * a comparable budget before it earns the complexity.
 */
import { Effect } from "effect";

import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import * as Pack from "../context/Pack.ts";
import type { Projection } from "../trust/Projection.ts";
import { terms } from "../text.ts";
import * as Check from "./Check.ts";
import * as Concept from "./Concept.ts";

/** How much of a Concept body the lexical pass reads. */
export const MAX_BODY_SCAN = 64 * 1024;

/** How many Concept groups one recall will offer a selector. */
export const MAX_GROUPS = 8;

/** The exclusion reason for a Concept that matched and ranked past the cut. */
export const OVERFLOW = "recall-limit";

/**
 * Exclusion reasons that say this replica lacks an object, not that a policy
 * refused one: a selector reporting them must say `unavailable`, since a
 * fetch would restore what a filter would not (§5.3).
 */
export const UNAVAILABLE: ReadonlyArray<string> = [
  "citation-unavailable",
  "evidence-unavailable",
  "evidence-unknown",
];

export interface Group {
  readonly checked: Check.Checked;
  /** Distinct task terms this Concept matched; the whole of the ranking. */
  readonly terms: number;
  readonly matches: number;
  /** The Concept blob, then the current bytes of what it declares (§9.2). */
  readonly items: ReadonlyArray<Pack.Item>;
}

export interface Recalled {
  readonly groups: ReadonlyArray<Group>;
  /** Concepts that matched and are not eligible for automatic recall. */
  readonly excluded: ReadonlyArray<{
    readonly path: string;
    readonly reasons: ReadonlyArray<string>;
  }>;
  readonly complete: boolean;
}

export interface Options {
  readonly view: Pack.View;
  readonly task: string;
  readonly bundle?: string | undefined;
  readonly evaluationTime?: Date | undefined;
  readonly repo?: string | null | undefined;
  readonly trust?: Projection | null | undefined;
  readonly maxGroups?: number | undefined;
  /** The view's files, where the caller listed them already; see `Check.Input`. */
  readonly files?: ReadonlyArray<Check.Listed> | undefined;
  /**
   * Whether a lifecycle-excluded Concept may still be offered.
   *
   * Never for automatic recall. Explicit historical inspection is a different
   * call with a different label (§7).
   */
  readonly includeIneligible?: boolean;
}

/** The terms a task is searched for: the same rule the source search uses. */
export { terms };

/** What a Concept offers the lexical pass, metadata first (§9.1). */
const haystack = (concept: Concept.Concept) => {
  const metadata = [
    concept.id,
    concept.title ?? "",
    concept.type,
    concept.description ?? "",
    ...concept.sources.map((source) => source.id),
  ]
    .join("\n")
    .toLowerCase();
  return { metadata, body: concept.body.slice(0, MAX_BODY_SCAN).toLowerCase() };
};

/** How many distinct task terms a Concept matches, and how strongly. */
const scored = (concept: Concept.Concept, words: ReadonlyArray<string>) => {
  const { body, metadata } = haystack(concept);
  let hits = 0;
  let matches = 0;
  for (const word of words) {
    const inMetadata = metadata.includes(word);
    const inBody = body.includes(word);
    if (!inMetadata && !inBody) continue;
    hits += 1;
    // Metadata counts double: §9.1 searches metadata first, and a term in a
    // title is a better signal than the same term buried in prose.
    matches += (inMetadata ? 2 : 0) + (inBody ? 1 : 0);
  }
  return { hits, matches };
};

/**
 * Concepts worth putting in front of this task, with their support.
 *
 * Eligibility is asked of the *same* view, trust snapshot and clock the caller
 * is building a pack against — never cached by blob oid alone, because
 * reachability, redaction, policy and time all move without a byte of the
 * Concept changing (§9.1).
 */
export const search = Effect.fn("knowledge.Recall.search")(function* (
  options: Options,
): Effect.fn.Return<Recalled, Invalid | ObjectNotFound | StorageFailure, Repository> {
  const repository = yield* Repository;
  const words = terms(options.task);
  const bundle = (options.bundle ?? Concept.BUNDLE).replace(/\/+$/u, "");
  const evaluationTime = options.evaluationTime ?? new Date();
  const judged = {
    view: options.view,
    bundle,
    evaluationTime,
    repo: options.repo ?? null,
    trust: options.trust ?? null,
    files: options.files,
  };

  // Lexical first, verification second. The check verifies every citation's
  // signature against the trust log, and running it over the whole bundle to
  // then keep the three Concepts that mention the task was the cost of a
  // full `knowledge check` on every `context for --knowledge`. Listed and
  // parsed here — structure only, which is a blob read each — and the check
  // is asked about what matched.
  const tree = Pack.unqualify(options.view.tree);
  const listed =
    tree === null
      ? null
      : yield* Check.list(tree, bundle, options.files).pipe(
          Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
        );
  if (listed === null) {
    // An unreadable view is the check's to describe; nothing here matched.
    const report = yield* Check.check(judged);
    return { groups: [], excluded: [], complete: report.complete };
  }

  // The same ceiling the check applies, so the corpus this searched is the
  // corpus a check would have reported — a bounded host says "partial" the
  // same way from both surfaces.
  const scanned = listed.slice(0, Check.LIMITS.concepts);
  const matched: Array<string> = [];
  const scores = new Map<string, { hits: number; matches: number }>();
  for (const file of scanned) {
    const bytes = yield* repository
      .readBlob(file.oid)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    const parsed = bytes === null ? null : Concept.parse({ bundle, path: file.path, bytes });
    if (parsed === null || !parsed.ok) {
      // Only when the task points at it. A Concept that did not parse has no
      // metadata to match, and naming every one of them for every task let
      // an unrelated malformed bundle fill the pack's named-omission slots
      // before the source search had run. What the check says about it is
      // still the check's to say, so it goes through the same pass.
      const name = `${file.path}\n${Concept.idOf(bundle, file.path) ?? ""}`.toLowerCase();
      if (words.some((word) => name.includes(word))) matched.push(file.path);
      continue;
    }
    const score = scored(parsed.concept, words);
    if (score.hits === 0) continue;
    matched.push(file.path);
    scores.set(file.path, score);
  }

  const excluded: Array<{ path: string; reasons: ReadonlyArray<string> }> = [];
  const ranked: Array<Group> = [];
  // Nothing matched, nothing to verify — and no report to wait for.
  const report =
    matched.length === 0
      ? null
      : yield* Check.check({ ...judged, paths: matched, limits: { concepts: matched.length } });

  for (const checked of report?.concepts ?? []) {
    if (checked.concept === null) {
      excluded.push({ path: checked.path, reasons: checked.eligibility.reasons });
      continue;
    }
    const score = scores.get(checked.path);
    if (score === undefined) continue;

    if (!checked.eligibility.recall && options.includeIneligible !== true) {
      // Named as excluded rather than dropped: "why did I not get the Concept
      // I know exists?" is the question a recall diagnostic exists to answer.
      excluded.push({ path: checked.path, reasons: checked.eligibility.reasons });
      continue;
    }

    const items: Array<Pack.Item> = [
      {
        kind: "blob",
        path: checked.path,
        blob: checked.blob,
        role: "knowledge",
        // The pack vocabulary's own word for "this is recalled knowledge";
        // role and reason stay descriptive, and neither grants authority.
        reason: "memory",
      },
    ];

    // The support the Concept itself declares, at the version this view holds
    // — never the version it cited. A changed dependency is reported by the
    // check; the pack carries what is actually there now (§9.2). The check
    // resolved each path against the tree already: `unchanged` means the
    // recorded oid is the current one, and `changed` names what is there.
    let usable = true;
    // One item per path: a Concept may declare one file more than once (two
    // ranges on it), and a pack that listed it twice would charge and render
    // the same bytes twice.
    const declared = new Set<string>([checked.path]);
    for (const dependency of checked.repositoryEvidence) {
      const current =
        dependency.state === "unchanged"
          ? Pack.unqualify(dependency.recorded)
          : dependency.state === "changed" && dependency.found !== undefined
            ? Pack.unqualify(dependency.found)
            : null;
      if (current === null) {
        usable = false;
        break;
      }
      if (declared.has(dependency.path)) continue;
      declared.add(dependency.path);
      items.push(
        dependency.kind === "gitlink"
          ? {
              kind: "gitlink",
              path: dependency.path,
              commit: Pack.qualify(current),
              role: "dependency",
              reason: "reference",
            }
          : {
              kind: "blob",
              path: dependency.path,
              blob: Pack.qualify(current),
              role: "implementation",
              reason: "reference",
            },
      );
    }
    if (!usable) {
      excluded.push({ path: checked.path, reasons: ["evidence-unavailable"] });
      continue;
    }

    ranked.push({ checked, terms: score.hits, matches: score.matches, items });
  }

  ranked.sort(
    (left, right) =>
      right.terms - left.terms ||
      right.matches - left.matches ||
      // Byte order on the path, so two hosts rank equal candidates the same
      // way and produce the same pack bytes.
      (left.checked.path < right.checked.path
        ? -1
        : left.checked.path > right.checked.path
          ? 1
          : 0),
  );

  const limit = options.maxGroups ?? MAX_GROUPS;
  // What ranked past the cut is named too. Dropped silently, the pack recorded
  // no omission for a Concept that matched, so "why did I not get it?" had no
  // answer for exactly the case the limit caused.
  for (const group of ranked.slice(limit)) {
    excluded.push({ path: group.checked.path, reasons: [OVERFLOW] });
  }
  return {
    groups: ranked.slice(0, limit),
    excluded,
    // A partial corpus is a partial search: saying it was complete would
    // claim the rare Concept is not there rather than not reached.
    complete:
      scanned.length === listed.length && (report?.complete ?? true) && ranked.length <= limit,
  };
});
