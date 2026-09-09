/**
 * Constraints a session's own record suggests somebody might want to anchor.
 *
 * §27 is explicit about what this may and may not do: a distillation process
 * MAY suggest candidate anchored notes, and MUST NOT publish them as
 * authoritative constraints without an explicit signed event. So nothing here
 * writes. It reads what sessions said they learned, works out which files
 * those sessions actually changed, and hands both to a person — who decides
 * whether any of it is a constraint at all.
 *
 * The distinction §27 draws is the whole reason this cannot be automatic. A
 * session note says what happened:
 *
 *     "Refactoring the parser exposed a Windows path issue."
 *
 * An anchored note says what must keep being true:
 *
 *     "Drive-letter comparison must remain case-insensitive."
 *
 * Only the second is worth carrying forward, and only an author can tell which
 * one they wrote. What this produces is a shortlist and the command to run.
 */
import { Effect } from "effect";

import { isOid, type Oid } from "../git/Store.ts";
import { Repository } from "../git/Repository.ts";
import { changedPaths } from "./NoteAudit.ts";
import * as Notes from "./NoteProjection.ts";
import * as Session from "./Session.ts";

export interface Candidate {
  /** `convention`, `gotcha`, … where the observation labelled itself. */
  readonly kind: string;
  readonly text: string;
  readonly session: string;
  /** Paths the session's own commits changed, newest commit first. */
  readonly paths: ReadonlyArray<string>;
}

/**
 * How many paths one session may suggest before it suggests nothing useful.
 *
 * A session that touched two files points at something; one that touched two
 * hundred is a refactor, and pairing its observation with every file it swept
 * is noise a reader has to wade through to find the two that mattered.
 */
export const MAX_PATHS = 12;

const commitOf = (named: string): Oid | null => {
  const bare = named.startsWith("sha1:") ? named.slice(5) : named;
  return isOid(bare) ? bare : null;
};

/** What one commit changed against its first parent, or all of a root commit. */
const touched = Effect.fn("hub.NoteCandidate.touched")(function* (commit: Oid) {
  const repository = yield* Repository;
  const info = yield* repository
    .readCommit(commit)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  // A replica that never received the commit simply contributes no paths: a
  // suggestion list is not worth failing a command over.
  if (info === null) return [];

  const parent = info.parents[0];
  if (parent === undefined) {
    return (yield* repository.listFiles(info.tree)).map((file) => file.path);
  }
  return [...(yield* changedPaths(parent, commit))];
});

/**
 * Session observations that no anchored note already says, with somewhere to
 * put them.
 *
 * An observation whose text a note already carries is dropped: this is a
 * shortlist of what has *not* been captured, and re-suggesting what somebody
 * already wrote down is how a shortlist stops being read.
 */
export const candidates = Effect.fn("hub.NoteCandidate.candidates")(function* () {
  const written = new Set((yield* Notes.all()).map((note) => note.text.trim()));
  const found: Candidate[] = [];

  for (const session of yield* Session.sessions()) {
    const observations: Array<{ kind: string; text: string }> = [];
    const commits: string[] = [];

    for (const { payload } of (yield* Session.entries(session)).events) {
      if (payload.type !== "session.produced") continue;
      commits.push(...payload.commits);
      if (payload.note === null) continue;
      // `kind: text` where the note offers one, exactly as `Memory.distill`
      // reads it — the two are looking at the same records.
      const split = payload.note.indexOf(":");
      const labelled = split > 0 && split < 24;
      const kind = labelled ? payload.note.slice(0, split).trim() : "note";
      const text = (labelled ? payload.note.slice(split + 1) : payload.note).trim();
      if (text !== "" && !written.has(text)) observations.push({ kind, text });
    }
    if (observations.length === 0) continue;

    const paths = new Set<string>();
    for (const named of commits) {
      const oid = commitOf(named);
      if (oid === null) continue;
      for (const path of yield* touched(oid)) paths.add(path);
    }
    // Nowhere to anchor it, so there is nothing to suggest. The observation
    // still lives in the session record and in repository memory.
    if (paths.size === 0 || paths.size > MAX_PATHS) continue;

    const where = [...paths].sort();
    for (const observation of observations) {
      found.push({ ...observation, session, paths: where });
    }
  }
  return found;
});
