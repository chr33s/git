/**
 * Compare confirmed note baselines against the source a repository now holds.
 *
 * "Now" is deliberately not one place. A work tree answers what an author is
 * about to change; a revision answers what a pull request proposes. Both are
 * the same comparison against the same recorded baseline, so the source is a
 * port with two implementations rather than two audits that drift apart.
 *
 * Nothing here writes. An audit that could advance a baseline would make
 * observation into approval, which is the one thing this feature exists to
 * refuse — a drifted note keeps reporting drift until a signed `confirm`,
 * `replace` or `retire` says otherwise.
 */
import { Effect, Stream } from "effect";

import { isBinary, similarity } from "../git/Diff.ts";
import type { ObjectNotFound, StorageFailure } from "../git/Error.ts";
import * as History from "../git/History.ts";
import { Repository, treeAt } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { WorkTree } from "../git/Work.ts";
import { AnchorResolver, type Fingerprint } from "./Anchor.ts";
import { covers } from "./Note.ts";
import type { Projection } from "./NoteProjection.ts";

export { covers };

export type DriftStatus =
  | "fresh"
  | "content-changed"
  | "contract-changed"
  | "anchor-missing"
  | "source-missing"
  | "unverifiable"
  | "rebaseline-required"
  | "conflicted";

export interface AuditResult {
  readonly id: string;
  readonly path: string;
  readonly anchor: string;
  readonly text: string;
  readonly status: DriftStatus;
  readonly baseline: Fingerprint | null;
  readonly current: Fingerprint | null;
  readonly active: boolean;
  readonly pinned: boolean;
  /** The path the note was written against, when a rename was followed. */
  readonly pathMovedFrom: string | null;
}

export const actionable = (status: DriftStatus): boolean =>
  status === "content-changed" ||
  status === "contract-changed" ||
  status === "anchor-missing" ||
  status === "source-missing" ||
  status === "conflicted";

/**
 * Where an audit reads source from.
 *
 * `at` is the commit whose history rename following may consult. A source
 * with no commit behind it — a fresh checkout with no HEAD — simply cannot
 * follow renames, which is a missing answer rather than a wrong one.
 */
export interface Source {
  readonly at: Oid | null;
  readonly read: (
    path: string,
  ) => Effect.Effect<Uint8Array | null, ObjectNotFound | StorageFailure>;
  readonly list: Effect.Effect<ReadonlyArray<string>, ObjectNotFound | StorageFailure>;
}

/** What the author is about to change: files on disk, HEAD behind them. */
export const workTree = Effect.fn("hub.NoteAudit.workTree")(function* () {
  const work = yield* WorkTree;
  const repository = yield* Repository;
  const head = yield* repository.resolve(yield* repository.head);
  return {
    at: head,
    read: (path) =>
      Effect.flatMap(work.stat(path), (stat) =>
        stat === null ? Effect.succeed(null) : work.read(path),
      ),
    // `list` takes the tracked entries so it can tell a tracked directory from
    // a gitlink it must not descend into. HEAD's tree is what this source is
    // already reading against, so it is the tracked set the audit means.
    list: Effect.flatMap(
      head === null
        ? Effect.succeed<ReadonlyArray<{ readonly path: string; readonly mode: number }>>([])
        : Effect.map(
            Effect.flatMap(treeAt(repository, head), (tree) => repository.listFiles(tree)),
            (files) =>
              files.map((file) => ({ path: file.path, mode: Number.parseInt(file.mode, 8) })),
          ),
      (tracked) => work.list(tracked),
    ),
  } satisfies Source;
});

/** What a revision proposes: one commit's tree, read straight from objects. */
export const revision = Effect.fn("hub.NoteAudit.revision")(function* (commit: Oid) {
  const repository = yield* Repository;
  const tree = yield* treeAt(repository, commit);
  return {
    at: commit,
    read: (path) =>
      Effect.flatMap(repository.findPath(tree, path), (entry) =>
        entry === null ? Effect.succeed(null) : repository.readBlob(entry.oid),
      ),
    list: Effect.map(repository.listFiles(tree), (files) => files.map((file) => file.path)),
  } satisfies Source;
});

/**
 * git's own default rename threshold. Half the lines in common is what
 * `git diff -M` calls the same file moved, and agreeing with it keeps a note
 * following exactly the renames a reviewer already sees reported as renames.
 */
const RENAME_SIMILARITY = 0.5;

/**
 * How many candidate paths a single rename inference will read.
 *
 * A note written long ago can have thousands of files added beneath it since,
 * and reading all of them to answer "where did one file go" would make the
 * cheap read path — `why` before an edit — pay for the rare one. Past the cap
 * the answer is `source-missing`, which is the same answer ambiguity gives:
 * the path is gone and a human has to say what that means.
 */
const RENAME_SCAN_LIMIT = 200;

const decoder = new TextDecoder();

/**
 * The path a vanished file most likely became, or `null`.
 *
 * Candidates are paths the source holds now and the commit where the note's
 * path last existed did not — the set a rename could have landed in. An exact
 * content match settles it; otherwise the single best similarity above git's
 * threshold does, and a tie settles nothing.
 */
const followRename = Effect.fn("hub.NoteAudit.followRename")(function* (
  source: Source,
  path: string,
) {
  if (source.at === null) return null;
  const repository = yield* Repository;

  const changes = yield* Stream.runCollect(History.forPath(source.at, path, { limit: 4 }));
  const existed = changes.find((change) => change.blob !== null);
  if (existed === undefined || existed.blob === null) return null;

  const before = yield* repository.readBlob(existed.blob);
  if (isBinary(before)) return null;

  const held = yield* repository.readCommit(existed.oid);
  const known = new Set((yield* repository.listFiles(held.tree)).map((file) => file.path));
  const appeared = (yield* source.list).filter((candidate) => !known.has(candidate));

  // A file that moved keeps its name far more often than it keeps nothing
  // else, so the same basename is both the likeliest answer and the one that
  // costs a handful of reads instead of the whole scan.
  const name = path.slice(path.lastIndexOf("/") + 1);
  const named = appeared.filter(
    (candidate) => candidate.endsWith(`/${name}`) || candidate === name,
  );
  const candidates = named.length > 0 ? named : appeared;
  if (candidates.length === 0 || candidates.length > RENAME_SCAN_LIMIT) return null;

  const text = decoder.decode(before);
  let best: { readonly path: string; readonly score: number } | null = null;
  let tied = false;
  for (const candidate of candidates) {
    const content = yield* source.read(candidate);
    if (content === null || isBinary(content)) continue;
    const score = similarity(text, decoder.decode(content));
    if (score < RENAME_SIMILARITY) continue;
    if (best === null || score > best.score) {
      best = { path: candidate, score };
      tied = false;
    } else if (score === best.score) {
      tied = true;
    }
  }
  return best === null || tied ? null : best.path;
});

const result = (
  note: Projection,
  status: DriftStatus,
  current: Fingerprint | null,
  moved?: { readonly path: string; readonly from: string },
): AuditResult => ({
  id: note.id,
  path: moved?.path ?? note.path,
  anchor: note.anchor,
  text: note.text,
  status,
  baseline: note.baseline,
  current,
  active: note.active,
  pinned: note.pinned,
  pathMovedFrom: moved?.from ?? null,
});

export const audit = Effect.fn("hub.NoteAudit.audit")(function* (note: Projection, source: Source) {
  if (note.state === "conflicted") return result(note, "conflicted", null);

  let path = note.path;
  let moved: { readonly path: string; readonly from: string } | undefined;
  let content = yield* source.read(path);
  if (content === null) {
    const followed = yield* followRename(source, path);
    if (followed === null) return result(note, "source-missing", null);
    content = yield* source.read(followed);
    if (content === null) return result(note, "source-missing", null);
    moved = { path: followed, from: path };
    path = followed;
  }

  const resolver = yield* AnchorResolver;
  const resolved = yield* resolver.resolve(path, content, note.anchor);

  if (resolved._tag === "Unsupported") return result(note, "unverifiable", null, moved);
  if (resolved._tag === "Missing" || resolved._tag === "Ambiguous") {
    return result(note, "anchor-missing", null, moved);
  }

  const current = resolved.fingerprint;
  const baseline = note.baseline;
  // A null baseline stays null in authoritative state. Reporting fresh here
  // says what this read could compare without turning the read into a
  // confirmation; creation normally records the same fingerprint anyway.
  if (baseline === null) return result(note, "fresh", current, moved);
  if (baseline.resolver !== current.resolver || baseline.normalization !== current.normalization) {
    return result(note, "rebaseline-required", current, moved);
  }
  if (
    baseline.signatureHash !== null &&
    current.signatureHash !== null &&
    baseline.signatureHash !== current.signatureHash
  ) {
    return result(note, "contract-changed", current, moved);
  }
  if (baseline.contentHash !== current.contentHash) {
    return result(note, "content-changed", current, moved);
  }
  return result(note, "fresh", current, moved);
});

/**
 * The paths a source holds now, for a caller narrowing a selection.
 *
 * A note whose file was renamed still describes that file under its new name,
 * and the new name is what an agent asks about before editing it — so a
 * selection scoped by the *stored* path alone answered nothing for exactly
 * that query, and the rename following below it could never fire. Passing this
 * to `NoteIndex.select` keeps a note whose stored path is gone in the running,
 * and `auditAll` decides on the followed path whether it belongs.
 */
export const present = Effect.fn("hub.NoteAudit.present")(function* (
  source: Source,
  query?: string,
) {
  // An unscoped read selects every note anyway, so the listing would buy
  // nothing — and `git+ why` with no argument is the read taken at session
  // start, which is the wrong place to spend a directory walk.
  if (query === undefined || query === "") return undefined;
  const paths: ReadonlySet<string> = new Set(yield* source.list);
  return paths;
});

export const auditAll = Effect.fn("hub.NoteAudit.auditAll")(function* (
  notes: ReadonlyArray<Projection>,
  source: Source,
  query?: string,
) {
  const results: AuditResult[] = [];
  for (const note of notes) {
    if (!note.active) continue;
    const audited = yield* audit(note, source);
    // Judged on the path the audit landed on, which is the note's own path
    // unless a rename moved it. The selection let a moved note through on the
    // chance that it moved into scope; this is where that chance is settled.
    if (!covers(query, audited.path)) continue;
    results.push(audited);
  }
  return results;
});

/**
 * Paths whose content differs between two revisions.
 *
 * What a pull request touched, which is the set §25 wants a merge check
 * scoped to: unrelated historical drift is a real thing to fix, but blocking
 * an independent change on it is not what a required check is for.
 */
export const changedPaths = Effect.fn("hub.NoteAudit.changedPaths")(function* (
  base: Oid,
  head: Oid,
) {
  const repository = yield* Repository;
  const before = new Map(
    (yield* repository.listFiles(yield* treeAt(repository, base))).map((file) => [
      file.path,
      file.oid,
    ]),
  );
  const after = new Map(
    (yield* repository.listFiles(yield* treeAt(repository, head))).map((file) => [
      file.path,
      file.oid,
    ]),
  );
  const changed = new Set<string>();
  for (const [path, oid] of after) if (before.get(path) !== oid) changed.add(path);
  for (const path of before.keys()) if (!after.has(path)) changed.add(path);
  return changed;
});

/**
 * Audit the notes a change between two revisions could have invalidated.
 *
 * A note whose path the change never touched is not re-read: its baseline
 * cannot have moved relative to a source neither side altered.
 */
export const auditRange = Effect.fn("hub.NoteAudit.auditRange")(function* (
  notes: ReadonlyArray<Projection>,
  base: Oid,
  head: Oid,
  query?: string,
) {
  const changed = yield* changedPaths(base, head);
  const source = yield* revision(head);
  const results: AuditResult[] = [];
  for (const note of notes) {
    if (!note.active) continue;
    // The stored path, and it needs no relaxing for a rename: a range that
    // renamed the note's file changed the old path too — it holds nothing at
    // `head` — so a moved note is already in `changed` under the name the
    // projection knows it by.
    if (!changed.has(note.path)) continue;
    const audited = yield* audit(note, source);
    if (!covers(query, audited.path)) continue;
    results.push(audited);
  }
  return results;
});
