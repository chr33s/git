/**
 * Repository memory: what agents have learned about this repository.
 *
 * Sessions capture learnings one session at a time and nothing compounds
 * them. This does — the machine-maintained sibling of a hand-written
 * `CLAUDE.md`, read at session start beside the standing instructions.
 *
 * A **projection cache, not a record**. It is rebuilt from the Concepts and
 * sessions it cites rather than merged into, which is what makes it
 * disposable: a stale or missing memory costs context, never correctness, and
 * a redacted session's lessons leave on the next distillation rather than
 * surviving their source. Verification is citation checking, never signature
 * checking — nothing is signed here, because what it says is not evidence of
 * anything its sources do not already say themselves.
 *
 * ```text
 * collect   every bounded, addressable candidate + completeness
 *    ↓
 * assess    which are eligible now, and why the rest are not
 *    ↓
 * select    the bounded startup subset + what was omitted
 *    ↓
 * render    the exact UTF-8 bytes
 *    ↓
 * persist   a note commit, a no-op, or an explicit failure
 * ```
 *
 * Those stages are separate because they fail differently
 * (docs/context-pack.knowledge.md §8.1). Collection can be partial on a bounded
 * host; eligibility moves when trust, redaction or the clock moves without a
 * single source byte changing; selection is a budget; and persistence can lose
 * a compare-and-swap. Folded together, every one of those came out as "the
 * memory is what it is".
 *
 * Because every future session reads it, this is the highest-value injection
 * target in the system. The rule that projected records are data applies
 * doubly: memory is cited, not obeyed — and the bytes in the note are never
 * what gets injected. §8.4: automatic recall re-derives, because a matching
 * stamp string in a note somebody else wrote proves nothing about how its
 * prose was derived.
 */
import { Effect } from "effect";

import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { hashObject } from "../git/Format.ts";
import { qualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import type { Oid } from "../git/Store.ts";
import type { View } from "../context/Pack.ts";
import * as Check from "../knowledge/Check.ts";
import * as Concept from "../knowledge/Concept.ts";
import type { Projection } from "../trust/Projection.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import * as Session from "./Session.ts";
import * as Tombstone from "./Tombstone.ts";

/** Where memory lives: a note on the one commit every replica shares. */
export const MEMORY_REF = "refs/notes/hub/memory";

/**
 * How large the note may be.
 *
 * Small enough to ride into a context window whole, which is the only reason
 * it exists. The cap is also what forces eviction, and eviction is the point:
 * what fewest distinct sessions have seen is what falls off, on a stable
 * identity tiebreak — a UUID is not read as recency (§8.3). Counted over the *complete*
 * rendered document — headings, provenance, separators, the stamp line — since
 * a cap that measured only the claims would let the framing overrun it (§8.3).
 */
export const MAX_MEMORY = 16 * 1024;

/** The projection version, which keys the cache and rides in the stamp. */
export const PROJECTION_VERSION = "gitplus-memory/1";

const BLOB_MODE = "100644";
const decoder = new TextDecoder();
const encoder = new TextEncoder();

const identity = {
  name: "chr33s-git",
  email: "chr33s-git@localhost",
  at: new Date(0),
  offset: 0,
};

/**
 * One thing learned, and exactly what says so.
 *
 * `records` is the canonical half: qualified record commit oids, which survive
 * a display id being reused and are what a reader can actually go and verify
 * (INV-04). `cites` keeps the session ids beside them because that is what a
 * person reads — supplementary, never a replacement (§8.1).
 */
export interface Entry {
  readonly kind: string;
  readonly text: string;
  /** Distinct sessions that observed this; never repeats within one session. */
  readonly observations: number;
  readonly cites: ReadonlyArray<string>;
  readonly records: ReadonlyArray<string>;
  /** Where this entry came from; a Concept also names its exact bytes. */
  readonly origin: "concept" | "session";
  readonly source?: { readonly path: string; readonly blob: string };
}

/** A candidate and what the assessment made of it. */
export interface Candidate {
  readonly entry: Entry;
  readonly eligible: boolean;
  readonly reasons: ReadonlyArray<string>;
}

export interface Collected {
  readonly candidates: ReadonlyArray<Candidate>;
  readonly sessions: number;
  /**
   * Whether the whole corpus was walked.
   *
   * A bounded host may stop, and then it must say so rather than let a partial
   * walk read as "this is everything there is" (§8.1).
   */
  readonly complete: boolean;
  readonly stamp: string;
}

export interface Selected {
  readonly entries: ReadonlyArray<Entry>;
  readonly omitted: number;
}

export interface Limits {
  readonly sessions?: number;
  readonly concepts?: number;
  readonly bytes?: number;
}

export const LIMITS = { sessions: 512, concepts: 256 } as const;

export interface Options {
  /** The view Concepts are read from; absent means sessions only. */
  readonly view?: View | null | undefined;
  readonly bundle?: string | undefined;
  readonly repo?: string | null | undefined;
  readonly trust?: Projection | null | undefined;
  readonly evaluationTime?: Date | undefined;
  readonly limits?: Limits | undefined;
}

/**
 * The note as it stands, or `null`.
 *
 * Anchored to the genesis commit — the one object every replica of this
 * repository is guaranteed to hold — which gives the cache a stable address
 * without inventing a ref class for it.
 *
 * A *historical* read. What this returns is whatever bytes are on the ref,
 * including a note written on another branch, by another host, or before a
 * redaction: fine to show a person with that label, and never the thing to
 * inject. `derive` is the automatic path (§8.4, §12.4).
 */
export const read = Effect.fn("hub.Memory.read")(function* () {
  const repository = yield* Repository;

  const anchor = yield* repository.resolve(GENESIS_REF);
  const head = yield* repository.resolve(MEMORY_REF);
  if (anchor === null || head === null) return null;

  const info = yield* repository.readCommit(head);
  const entry = yield* repository.findPath(info.tree, anchor);
  if (entry === null) return null;

  return decoder.decode(yield* repository.readBlob(entry.oid));
});

// -- render ---------------------------------------------------------------------

/**
 * The stamp line, which says what a note was derived from.
 *
 * Plain text at the end of the document, so the note stays readable with
 * `git notes show` and a human never has to decode a header to read a
 * sentence (§8.4). It is a cache key and nothing more: it is unsigned, so a
 * matching stamp is not evidence that the prose above it was derived honestly,
 * which is why `derive` re-derives rather than trusting one.
 */
export const STAMP_PREFIX = "<!-- gitplus-memory/1 ";

export const stampOf = (input: {
  /**
   * The check's own input stamp, composed rather than re-derived.
   *
   * Two spellings of "what the Concept check read" would drift the moment
   * the checker learned a new input; this one is the checker's, verbatim.
   */
  readonly check: string;
  readonly sessionsHead: string | null;
  readonly recheckAt: string | null;
}): string =>
  [
    `projection=${PROJECTION_VERSION}`,
    input.check,
    // A digest over the walked sessions *and their heads* (§8.4): a count and
    // the newest id moved when a session was added, and stood still when a
    // learning was appended to one that already existed.
    `sessions=${input.sessionsHead ?? "none"}`,
    // The earliest deadline any selected entry declares. Without it a note
    // built before a `stale_after` and read after it looks current (§8.4).
    `recheck=${input.recheckAt ?? "none"}`,
  ].join(" ");

/** The stamp a note carries, for a reader deciding whether to re-derive. */
export const stampIn = (text: string): string | null => {
  for (const line of text.split("\n")) {
    if (line.startsWith(STAMP_PREFIX) && line.endsWith("-->")) {
      return line.slice(STAMP_PREFIX.length, -3).trim();
    }
  }
  return null;
};

/**
 * A note's text as an agent reads it.
 *
 * Deterministic in the strict sense: the same entries and the same stamp
 * produce the same bytes on every host, so repeating a distillation costs no
 * commit (§8.3). Every claim keeps its provenance on the line under it —
 * dropping an entry's citations while keeping its sentence is the one trade
 * this is not allowed to make. A long list is bounded by `CITATIONS` with its
 * remainder counted, never cut through an identifier.
 */
export const render = (entries: ReadonlyArray<Entry>, sessions: number, stamp?: string): string => {
  const lines = [`# Repository memory, distilled from ${sessions} session(s)`, ""];
  for (const entry of entries) lines.push(...linesFor(entry));
  if (stamp !== undefined) {
    lines.push("", `${STAMP_PREFIX}${stamp} -->`);
  }
  return `${lines.join("\n")}\n`;
};

/**
 * How many citations one entry lists in the note.
 *
 * Every record oid rides in the `Entry`, and the note's provenance line is
 * where the ceiling met them: at roughly 85 bytes per observation, an entry
 * seen in a few hundred sessions could not fit an *empty* note, and well
 * short of that the best-corroborated entries — the ones the ranking puts
 * first — were the ones the budget skipped. So the line names this many and
 * states how many more it holds. A stated count is not a truncated
 * identifier: every citation listed is whole (§8.3), and the entry itself
 * keeps them all.
 */
export const CITATIONS = 8;

const cited = (ids: ReadonlyArray<string>): string =>
  ids.length <= CITATIONS
    ? ids.join(", ")
    : `${ids.slice(0, CITATIONS).join(", ")} +${ids.length - CITATIONS} more`;

const linesFor = (entry: Entry): ReadonlyArray<string> => {
  const provenance =
    entry.origin === "concept"
      ? `  [concept ${entry.source?.path ?? "?"} @ ${entry.source?.blob ?? "?"}${
          entry.records.length === 0 ? "" : `; records ${cited(entry.records)}`
        }]`
      : `  [${entry.observations} observation(s); sessions ${cited(entry.cites)}; records ${cited(entry.records)}]`;
  return [`- ${entry.kind}: ${entry.text}`, provenance];
};

// -- collect --------------------------------------------------------------------

/**
 * Every candidate this repository can offer, with completeness stated.
 *
 * Concepts and session learnings are collected the same way and kept apart in
 * the entry: a Concept summary is curated publication with exact bytes behind
 * it, and a session note is an observation. Neither is promoted to the other.
 *
 * Candidates outside the startup budget are still returned, because §8.1 makes
 * them available to task-specific retrieval — the startup note is not the
 * search corpus (INV-09).
 */
export const collect = Effect.fn("hub.Memory.collect")(function* (options: Options = {}) {
  const repository = yield* Repository;
  const bundle = options.bundle ?? Concept.BUNDLE;
  const evaluationTime = options.evaluationTime ?? new Date();
  const sessionLimit = options.limits?.sessions ?? LIMITS.sessions;
  const conceptLimit = options.limits?.concepts ?? LIMITS.concepts;

  const candidates: Array<Candidate> = [];
  let complete = true;
  let recheckAt: number | null = null;

  // Concepts first: curated publication, checked against the same view and
  // trust snapshot the caller is asking about.
  let checkStamp: string | null = null;
  if (options.view != null) {
    const report = yield* Check.check({
      view: options.view,
      bundle,
      evaluationTime,
      repo: options.repo ?? null,
      trust: options.trust ?? null,
      limits: { concepts: conceptLimit },
    });
    complete &&= report.complete;
    checkStamp = report.inputStamp;

    for (const checked of report.concepts) {
      if (checked.concept === null) {
        candidates.push({
          entry: {
            kind: "concept",
            text: checked.path,
            observations: 0,
            cites: [],
            records: [],
            origin: "concept",
            source: { path: checked.path, blob: checked.blob },
          },
          eligible: false,
          reasons: checked.eligibility.reasons,
        });
        continue;
      }
      const text = Concept.summarize(checked.concept);
      if (text === "") {
        // Named rather than dropped: a Concept with nothing to say is still a
        // Concept the author will ask about when it is not in the note.
        candidates.push({
          entry: {
            kind: checked.concept.type.toLowerCase(),
            text: checked.path,
            observations: 0,
            cites: [],
            records: [],
            origin: "concept",
            source: { path: checked.path, blob: checked.blob },
          },
          eligible: false,
          reasons: [...checked.eligibility.reasons, "no-summary"],
        });
        continue;
      }
      if (checked.temporal.state === "within-deadline") {
        const deadline = Concept.instantOf(checked.temporal.deadline);
        if (deadline.state === "instant") {
          recheckAt = recheckAt === null ? deadline.at : Math.min(recheckAt, deadline.at);
        }
      }
      candidates.push({
        entry: {
          kind: checked.concept.type.toLowerCase(),
          text,
          observations: 0,
          cites: [],
          // Only accepted citations ride along: an entry must not carry a
          // provenance claim the check did not establish.
          records: checked.citations
            .filter((citation) => citation.state === "accepted")
            .map((citation) => citation.record),
          origin: "concept",
          source: { path: checked.path, blob: checked.blob },
        },
        eligible: checked.eligibility.startup,
        reasons: checked.eligibility.reasons,
      });
    }
  }

  // Then session learnings, which are observations rather than publications.
  // The newest, when the walk is bounded. `Session.sessions` sorts ascending
  // and UUIDv7 orders by time, so a prefix kept the *oldest* sessions: the
  // learning the stop hook had just recorded was never folded, while the stamp
  // cited the newest id as covered.
  const all = yield* Session.sessions();
  const walked = all.slice(-sessionLimit);
  if (walked.length < all.length) complete = false;

  const found = new Map<
    string,
    { kind: string; text: string; cites: Array<string>; records: Array<string> }
  >();
  /** Each walked session and the head it was read at, for the stamp. */
  const heads: Array<string> = [];
  for (const session of walked) {
    // Tombstones first. A redacted record keeps its commit and its place in
    // the chain until the next collection, so a fold that read whatever
    // decoded went on serving a removed learning from every session start —
    // which is exactly the "an old note still contains its text" resurrection
    // INV-07 forbids.
    // One walk, read twice: the tombstones are events on the same ref, and
    // folding them from a second walk took every session ref twice.
    const walked = yield* Session.entries(session);
    heads.push(`${session} ${walked.head ?? "none"}`);
    const removed =
      options.repo == null
        ? new Set<string>()
        : yield* Tombstone.removals(walked.events, options.repo, options.trust ?? null);

    for (const { commit, payload } of walked.events) {
      if (payload.type !== "session.produced" || payload.note === null) continue;
      if (removed.has(qualify(commit)) || removed.has(commit)) continue;

      // `kind: text` where a note offers one, so the kinds worth keeping —
      // convention, gotcha, decision, friction — survive the fold without this
      // having to guess at them.
      const split = payload.note.indexOf(":");
      const labelled = split > 0 && split < 24;
      const kind = labelled ? payload.note.slice(0, split).trim() : "note";
      const text = (labelled ? payload.note.slice(split + 1) : payload.note).trim();
      if (text === "") continue;

      const key = `${kind} ${text}`;
      const held = found.get(key);
      if (held === undefined) {
        found.set(key, { kind, text, cites: [session], records: [qualify(commit)] });
        continue;
      }
      // One observation per session, however many times that session said it:
      // repetition inside one run is not corroboration (§7, M-03). The record
      // oids all ride along, because each is a distinct thing somebody signed.
      if (!held.cites.includes(session)) held.cites.push(session);
      if (!held.records.includes(qualify(commit))) held.records.push(qualify(commit));
    }
  }

  for (const entry of found.values()) {
    candidates.push({
      entry: {
        kind: entry.kind,
        text: entry.text,
        observations: entry.cites.length,
        // Newest first, which UUIDv7 makes the greatest id — so the ones the
        // note lists under `CITATIONS` are the most recent.
        cites: [...entry.cites].sort((left, right) => (left < right ? 1 : left > right ? -1 : 0)),
        records: [...entry.records].sort((left, right) =>
          left < right ? -1 : left > right ? 1 : 0,
        ),
        origin: "session",
      },
      eligible: entry.records.length > 0,
      reasons: entry.records.length > 0 ? [] : ["no-record"],
    });
  }

  // The corpus size and a digest over what was read: byte-ordered `id head`
  // lines, hashed as a blob so the stamp stays one token. §8.4 permits a
  // digest over a canonical enumeration; this one moves when a session is
  // added, walked or appended to, and stands still otherwise.
  const digest =
    all.length === 0
      ? null
      : yield* hashObject({
          type: "blob",
          data: encoder.encode(
            [...heads].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0)).join("\n"),
          ),
        });
  return {
    candidates,
    // What was actually folded, not what exists: the count goes in the note's
    // heading, and a bounded walk that printed the corpus size would overstate
    // what it read. `complete` says whether the two are the same.
    sessions: walked.length,
    complete,
    stamp: stampOf({
      check:
        checkStamp ??
        Check.stampOf({
          view: null,
          bundle,
          repo: options.repo ?? null,
          trustHead: yield* repository.resolve(TRUST_LOG),
        }),
      sessionsHead: digest === null ? null : `${all.length}:${digest}`,
      recheckAt: recheckAt === null ? null : new Date(recheckAt).toISOString(),
    }),
  } satisfies Collected;
});

/** The candidates automatic recall may use, and why the rest may not (§8.1). */
export const assess = (collected: Collected) => ({
  eligible: collected.candidates.filter((candidate) => candidate.eligible).map((c) => c.entry),
  excluded: collected.candidates.filter((candidate) => !candidate.eligible),
});

/**
 * The bounded startup subset.
 *
 * Concept-backed summaries first, then session observations by how often they
 * were seen, with a stable identity tiebreaker. That is a retrieval heuristic
 * and not a credibility ranking (§8.3) — the order decides what fits, not what
 * is true.
 *
 * A candidate that does not fit is skipped and the next one is considered
 * (M-02), rather than the whole selection stopping at the first oversized
 * entry. Sizes are measured per entry against a budget rather than by
 * re-rendering the document once per candidate.
 */
export const select = (
  entries: ReadonlyArray<Entry>,
  sessions: number,
  budget = MAX_MEMORY,
  stamp?: string,
): Selected => {
  const ordered = [...entries].sort(
    (left, right) =>
      Number(right.origin === "concept") - Number(left.origin === "concept") ||
      right.observations - left.observations ||
      // Byte order on a stable identity, never locale collation: two hosts
      // must fill the same budget with the same entries (§8.3, INV-14).
      compare(identityOf(left), identityOf(right)),
  );

  const framing = encoder.encode(render([], sessions, stamp)).length;
  const kept: Array<Entry> = [];
  let spent = framing;
  let omitted = 0;
  for (const entry of ordered) {
    const size = encoder.encode(`${linesFor(entry).join("\n")}\n`).length;
    if (spent + size > budget) {
      omitted += 1;
      continue;
    }
    spent += size;
    kept.push(entry);
  }
  return { entries: kept, omitted };
};

const identityOf = (entry: Entry): string =>
  entry.origin === "concept"
    ? `${entry.source?.path ?? ""} ${entry.source?.blob ?? ""}`
    : `${entry.records[0] ?? ""} ${entry.kind} ${entry.text}`;

const compare = (left: string, right: string): number => (left < right ? -1 : left > right ? 1 : 0);

export interface Derived {
  readonly text: string;
  readonly entries: ReadonlyArray<Entry>;
  readonly omitted: number;
  readonly excluded: ReadonlyArray<Candidate>;
  readonly sessions: number;
  readonly complete: boolean;
  readonly stamp: string;
}

/**
 * The whole pipeline, which is what automatic injection reads.
 *
 * Re-derived every time rather than served from the note. §8.4: a read after a
 * revocation, a redaction, a Concept edit, a policy change or a passed deadline
 * must not serve an obsolete entry merely because no distillation has run —
 * and an unsigned stamp on a note somebody else wrote cannot authorize
 * injection either. The note is where this is *kept*, not where it is trusted
 * from.
 */
export const derive = Effect.fn("hub.Memory.derive")(function* (options: Options = {}) {
  const collected = yield* collect(options);
  const assessed = assess(collected);
  const selected = select(
    assessed.eligible,
    collected.sessions,
    options.limits?.bytes ?? MAX_MEMORY,
    collected.stamp,
  );
  return {
    text: render(selected.entries, collected.sessions, collected.stamp),
    entries: selected.entries,
    omitted: selected.omitted,
    excluded: assessed.excluded,
    sessions: collected.sessions,
    complete: collected.complete,
    stamp: collected.stamp,
  } satisfies Derived;
});

export type Persisted =
  | { readonly state: "written"; readonly commit: Oid }
  | { readonly state: "unchanged" }
  | { readonly state: "no-anchor" }
  | { readonly state: "conflict"; readonly reason: string };

/**
 * Write the note, replacing whatever it held.
 *
 * Four outcomes because they are four different things to act on (§8.3).
 * Identical inputs render identical bytes, so a repeated distillation is
 * `unchanged` and costs no commit; the swap is compare-and-swap on the head
 * this derivation was built against, so a concurrent writer is a reported
 * conflict rather than a silent last-writer-wins.
 */
export const write = Effect.fn("hub.Memory.write")(function* (
  text: string,
): Effect.fn.Return<Persisted, Invalid | ObjectNotFound | StorageFailure, Repository> {
  const repository = yield* Repository;

  // A store that cannot be read fails as one: swallowed to `null`, a transient
  // read failure became `no-anchor` on a repository with a genesis, or a
  // parentless commit and a compare-and-swap against nothing — reported as a
  // conflict with a writer that never existed.
  const anchor = yield* repository.resolve(GENESIS_REF);
  if (anchor === null) return { state: "no-anchor" };

  const head = yield* repository.readRef(MEMORY_REF);
  if (head !== null) {
    const held = yield* read().pipe(Effect.orElseSucceed(() => null));
    if (held === text) return { state: "unchanged" };
  }

  const blob = yield* repository.writeBlob(encoder.encode(text));
  const tree = yield* repository.writeTree([{ mode: BLOB_MODE, name: anchor, oid: blob }]);
  const commit = yield* repository.commitTree({
    tree,
    // Kept as history, so yesterday's memory is still readable after today's
    // rewrite: the cache is disposable, and the record of what it used to say
    // costs one commit.
    parents: head === null ? [] : [head],
    message: "memory\n",
    author: identity,
  });

  // `RefConflict` alone: a read-only store or a full disk reported as a
  // conflict tells an operator to retry a write that cannot succeed.
  const swapped = yield* repository.setRef({ name: MEMORY_REF, to: commit, expected: head }).pipe(
    Effect.as(true),
    Effect.catchTag("RefConflict", () => Effect.succeed(false)),
  );

  return swapped
    ? { state: "written", commit }
    : { state: "conflict", reason: "the memory ref moved while this note was being written" };
});
