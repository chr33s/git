/**
 * Repository memory: what agents have learned about this repository.
 *
 * Sessions capture learnings one session at a time and nothing compounds
 * them. This does — the machine-maintained sibling of a hand-written
 * `CLAUDE.md`, read at session start beside the standing instructions.
 *
 * A **projection cache, not a record**. It is rebuilt from the sessions it
 * cites rather than merged into, which is what makes it disposable: a stale or
 * missing memory costs context, never correctness, and a redacted session's
 * lessons leave on the next distillation rather than surviving their source.
 * Verification is citation checking, never signature checking — nothing is
 * signed here, because what it says is not evidence of anything the sessions
 * do not already say themselves.
 *
 * Because every future session reads it, this is the highest-value injection
 * target in the system. The rule that projected records are data applies
 * doubly: memory is cited, not obeyed.
 */
import { Effect } from "effect";

import { Repository } from "../git/Repository.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import * as Session from "./Session.ts";

/** Where memory lives: a note on the one commit every replica shares. */
export const MEMORY_REF = "refs/notes/hub/memory";

/**
 * How large the note may be.
 *
 * Small enough to ride into a context window whole, which is the only reason
 * it exists. The cap is also what forces eviction, and eviction is the point:
 * a convention three refactors old should age out.
 */
export const MAX_MEMORY = 16 * 1024;

const BLOB_MODE = "100644";
const decoder = new TextDecoder();
const encoder = new TextEncoder();

const identity = {
  name: "chr33s-git",
  email: "chr33s-git@localhost",
  at: new Date(0),
  offset: 0,
};

/** One thing learned, and what says so. */
export interface Entry {
  readonly kind: string;
  readonly text: string;
  readonly observations: number;
  /** The sessions this was read from; an entry without them is not written. */
  readonly cites: ReadonlyArray<string>;
}

/**
 * The note as it stands, or `null`.
 *
 * Anchored to the genesis commit — the one object every replica of this
 * repository is guaranteed to hold — which gives the cache a stable address
 * without inventing a ref class for it.
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

/** A note's text as an agent reads it. */
export const render = (entries: ReadonlyArray<Entry>, sessions: number): string => {
  const lines = [`# Repository memory, distilled from ${sessions} session(s)`, ""];
  for (const entry of entries) {
    const cites = entry.cites.slice(0, 3).join(", ");
    lines.push(
      `- ${entry.kind}: ${entry.text}`,
      `  [${entry.observations} observation(s); sessions ${cites}]`,
    );
  }
  return `${lines.join("\n")}\n`;
};

/**
 * What the sessions say, folded into what a reader should know.
 *
 * Rebuilt rather than merged. The sessions are the record and this is a view
 * of them, so regenerating costs a walk and buys the two properties that
 * matter: it cannot drift from what it cites, and anything whose source was
 * redacted simply stops appearing.
 *
 * Ordered by how often a thing was observed and then by how recently, which is
 * also the eviction rule the cap enforces: what is rarely seen and long unseen
 * is what falls off the end.
 */
export const distill = Effect.fn("hub.Memory.distill")(function* () {
  const found = new Map<string, { kind: string; text: string; cites: Array<string> }>();
  const all = yield* Session.sessions();

  for (const session of all) {
    for (const { payload } of (yield* Session.entries(session)).events) {
      if (payload.type !== "session.produced" || payload.note === null) continue;

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
      if (held === undefined) found.set(key, { kind, text, cites: [session] });
      else if (!held.cites.includes(session)) held.cites.push(session);
    }
  }

  const entries = [...found.values()]
    .map((entry) => ({
      kind: entry.kind,
      text: entry.text,
      observations: entry.cites.length,
      // Newest first, which UUIDv7 makes the greatest id.
      cites: [...entry.cites].sort((left, right) => right.localeCompare(left)),
    }))
    .sort(
      (left, right) =>
        right.observations - left.observations ||
        (right.cites[0] ?? "").localeCompare(left.cites[0] ?? ""),
    );

  // Filled to the cap and no further: what is left out is what was seen least
  // and longest ago, which is the entry a reader would miss least.
  const kept: Array<Entry> = [];
  for (const entry of entries) {
    if (render([...kept, entry], all.length).length > MAX_MEMORY) break;
    kept.push(entry);
  }

  return { entries: kept, dropped: entries.length - kept.length, sessions: all.length };
});

/** Write the note, replacing whatever it held. */
export const write = Effect.fn("hub.Memory.write")(function* (text: string) {
  const repository = yield* Repository;

  const anchor = yield* repository.resolve(GENESIS_REF);
  if (anchor === null) return null;

  const blob = yield* repository.writeBlob(encoder.encode(text));
  const tree = yield* repository.writeTree([{ mode: BLOB_MODE, name: anchor, oid: blob }]);
  const head = yield* repository.readRef(MEMORY_REF);

  const commit = yield* repository.commitTree({
    tree,
    // Kept as history, so yesterday's memory is still readable after today's
    // rewrite: the cache is disposable, and the record of what it used to say
    // costs one commit.
    parents: head === null ? [] : [head],
    message: "memory\n",
    author: identity,
  });
  yield* repository.setRef({ name: MEMORY_REF, to: commit, expected: head });
  return commit;
});
