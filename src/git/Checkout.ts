/**
 * Working-tree operations: the three-way disagreement git calls `status`, and
 * the commands that resolve it.
 *
 * A path can differ between HEAD, the index and the disk, and every porcelain
 * verb here is a way of moving one of those toward another:
 *
 *   add       disk  -> index
 *   restore   index -> disk, or HEAD -> index
 *   commit    index -> HEAD
 *   checkout  HEAD  -> index and disk
 *
 * Saying it that way is the point: `status` computes both differences once,
 * and the verbs are then small.
 *
 * The index caches each file's size and mtime, so a status over an unchanged
 * tree hashes nothing — the difference between reading every byte in the
 * repository and reading none of it.
 */
import { Effect } from "effect";

import { Invalid } from "./Error.ts";
import { hashObject, isGitlink } from "./Format.ts";
import { addEntry, type IndexEntry, removeEntry } from "./Index.ts";
import { Repository } from "./Repository.ts";
import { isOid, type Oid } from "./Store.ts";
import {
  entryFor,
  type FileStat,
  IndexStore,
  modeString,
  REGULAR,
  unchanged,
  validatePath,
  WorkTree,
} from "./Work.ts";

export type Change = "added" | "modified" | "deleted";

export interface Status {
  /** Index against HEAD: what a commit would record. */
  readonly staged: ReadonlyArray<{ readonly path: string; readonly change: Change }>;
  /** Disk against the index: what a commit would miss. */
  readonly unstaged: ReadonlyArray<{ readonly path: string; readonly change: Change }>;
  /** On disk, in neither. */
  readonly untracked: ReadonlyArray<string>;
  readonly branch: string;
}

/**
 * A stat for a path that is not on disk.
 *
 * Restoring the index does not require the file to exist — unstaging a `git
 * rm` is precisely the case where it does not — and the stat cache is an
 * optimisation, so zeroes here only mean "hash it next time".
 */
const blank = (mode: number) => ({
  mode,
  size: 0,
  mtimeSeconds: 0,
  mtimeNanos: 0,
  ctimeSeconds: 0,
  ctimeNanos: 0,
  device: 0,
  inode: 0,
  uid: 0,
  gid: 0,
});

/**
 * A tree's mode against an index entry's, as numbers.
 *
 * The index holds a number and a tree holds a string, and the string may be
 * zero-padded — git's own `zeroPaddedFilemode`. Spelling the index mode and
 * comparing the text called `040000` and `40000` two different modes, so
 * every file under such a directory showed up as staged-modified forever and
 * `checkout` refused to run.
 */
const sameMode = (tree: string, index: number): boolean => Number.parseInt(tree, 8) === index;

/** HEAD's tree as a path -> entry map, or empty on an unborn branch. */
const headFiles = Effect.gen(function* () {
  const repository = yield* Repository;
  const head = yield* repository.head;
  const tip = yield* repository.resolve(head);
  if (tip === null) return new Map<string, { oid: Oid; mode: string }>();

  const commit = yield* repository.readCommit(tip);
  const files = yield* repository.listFiles(commit.tree);
  return new Map(files.map((file) => [file.path, { oid: file.oid, mode: file.mode }]));
});

export const status = Effect.fn("Checkout.status")(function* () {
  const repository = yield* Repository;
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  const head = yield* headFiles;
  const entries = yield* index.load;
  const staged = new Map(entries.map((entry) => [entry.path, entry]));
  const onDisk = yield* work.list;

  const stagedChanges: Array<{ path: string; change: Change }> = [];
  for (const [path, entry] of staged) {
    const committed = head.get(path);
    if (committed === undefined) stagedChanges.push({ path, change: "added" });
    else if (committed.oid !== entry.oid || !sameMode(committed.mode, entry.mode)) {
      stagedChanges.push({ path, change: "modified" });
    }
  }
  for (const path of head.keys()) {
    if (!staged.has(path)) stagedChanges.push({ path, change: "deleted" });
  }

  const unstaged: Array<{ path: string; change: Change }> = [];
  const untracked: string[] = [];

  for (const path of onDisk) {
    const entry = staged.get(path);
    if (entry === undefined) {
      untracked.push(path);
      continue;
    }

    const stat = yield* work.stat(path);
    if (stat === null) continue;
    // The stat cache: identical size, mtime and mode means identical
    // content, and the file is not read at all.
    if (unchanged(entry, stat)) continue;

    const content = yield* work.read(path);
    // Hashed, not written: `status` is a question, and answering it by storing
    // every modified file would leave the object store holding a blob for work
    // nobody has staged — garbage that only a `gc` can find its way back out of.
    const oid = yield* hashObject({ type: "blob", data: content });
    if (oid !== entry.oid || modeString(stat.mode) !== modeString(entry.mode)) {
      unstaged.push({ path, change: "modified" });
    }
  }

  const present = new Set(onDisk);
  for (const [path, entry] of staged) {
    // A gitlink has no file of its own on disk — the submodule's work tree is
    // that other repository's business — so "absent from `work.list`" is its
    // normal state, not a deletion. Reporting it as one made every checkout
    // of a repository with a submodule refuse for a dirty work tree.
    if (entry.mode === 0o160000) continue;
    if (!present.has(path)) unstaged.push({ path, change: "deleted" });
  }

  const sort = <A extends { path: string }>(items: Array<A>) =>
    items.sort((left, right) => left.path.localeCompare(right.path));

  return {
    staged: sort(stagedChanges),
    unstaged: sort(unstaged),
    untracked: untracked.sort(),
    branch: yield* repository.head,
  } satisfies Status;
});

/** Stage paths as they are on disk. A directory stages everything under it. */
export const add = Effect.fn("Checkout.add")(function* (paths: ReadonlyArray<string>) {
  const repository = yield* Repository;
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  const onDisk = yield* work.list;
  let entries = yield* index.load;
  const staged: string[] = [];

  for (const requested of paths) {
    const normalized = requested === "." ? "" : yield* validatePath(requested);
    // A path may name a file or a directory; git takes both, and a caller
    // typing `src` means everything under it.
    const matches =
      normalized === ""
        ? onDisk
        : onDisk.filter((path) => path === normalized || path.startsWith(`${normalized}/`));

    if (matches.length === 0) {
      return yield* new Invalid({ field: "path", reason: `nothing matches '${requested}'` });
    }

    for (const path of matches) {
      const stat = yield* work.stat(path);
      if (stat === null) continue;
      const oid = yield* repository.writeBlob(yield* work.read(path));
      entries = addEntry(entries, entryFor(path, oid, stat));
      staged.push(path);
    }
  }

  yield* index.save(entries);
  return staged.sort();
});

/** Unstage and, unless `cached`, delete from disk — `git rm`. */
export const remove = Effect.fn("Checkout.remove")(function* (
  paths: ReadonlyArray<string>,
  options?: { readonly cached?: boolean },
) {
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  let entries = yield* index.load;
  const removed: string[] = [];

  for (const requested of paths) {
    const normalized = yield* validatePath(requested);
    const matches = entries
      .map((entry) => entry.path)
      .filter((path) => path === normalized || path.startsWith(`${normalized}/`));

    if (matches.length === 0) {
      return yield* new Invalid({ field: "path", reason: `'${requested}' is not tracked` });
    }

    for (const path of matches) {
      entries = removeEntry(entries, path);
      if (options?.cached !== true) yield* work.remove(path);
      removed.push(path);
    }
  }

  yield* index.save(entries);
  return removed.sort();
});

/** Move a tracked path, staging both halves — `git mv`. */
export const move = Effect.fn("Checkout.move")(function* (from: string, to: string) {
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  const source = yield* validatePath(from);
  const target = yield* validatePath(to);

  const entries = yield* index.load;
  const entry = entries.find((candidate) => candidate.path === source);
  if (entry === undefined) {
    return yield* new Invalid({ field: "from", reason: `'${from}' is not tracked` });
  }
  if (entries.some((candidate) => candidate.path === target)) {
    return yield* new Invalid({ field: "to", reason: `'${to}' already exists` });
  }

  const stat = yield* work.stat(source);
  const content = yield* work.read(source);
  yield* work.write(target, content, stat?.mode ?? REGULAR);
  yield* work.remove(source);

  const moved = yield* work.stat(target);
  // A work tree that cannot stat what it just wrote still gets a real entry:
  // zeroed stat fields only mean the cache is cold, so the next `status`
  // re-hashes the file instead of trusting the cache.
  const cold = {
    size: content.length,
    mtimeSeconds: 0,
    mtimeNanos: 0,
    ctimeSeconds: 0,
    ctimeNanos: 0,
    device: 0,
    inode: 0,
    uid: 0,
    gid: 0,
    mode: entry.mode,
  } satisfies FileStat;
  const next = addEntry(
    removeEntry(entries, source),
    entryFor(target, entry.oid, moved ?? { ...(stat ?? cold), mode: entry.mode }),
  );
  yield* index.save(next);

  return { from: source, to: target };
});

/**
 * Put a path back: from the index onto disk, or from a commit into the index.
 *
 * `git restore` splits these with `--staged`/`--worktree` and so does this,
 * because they are genuinely different operations that happen to share a verb.
 */
export const restore = Effect.fn("Checkout.restore")(function* (
  paths: ReadonlyArray<string>,
  options?: { readonly staged?: boolean; readonly worktree?: boolean; readonly source?: string },
) {
  const repository = yield* Repository;
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  // `staged` alone means index-only; anything else touches the work tree
  // unless the caller said not to. `||` here made `worktree: false` a no-op
  // for every caller that did not also pass `staged`.
  const toIndex = options?.staged === true;
  const toWorktree = options?.worktree ?? !toIndex;

  let entries = yield* index.load;
  const restored: string[] = [];

  // `--source` means "take the content from that commit" rather than from
  // whatever is staged. Restoring the *index* has no other sensible source:
  // taking the oid out of the index this call exists to rewrite makes
  // `restore --staged` — the documented way to unstage — a silent no-op.
  const from = options?.source ?? (toIndex ? "HEAD" : undefined);
  const source =
    from === undefined
      ? null
      : yield* Effect.gen(function* () {
          const oid = yield* repository.resolve(from);
          if (oid === null) {
            // An unborn branch has no HEAD to restore from. That is an error
            // when the caller named the source and merely nothing to take
            // when the default supplied it — `restore --staged` on a
            // repository without commits should not fail.
            if (options?.source === undefined) return null;
            return yield* new Invalid({ field: "source", reason: `unknown '${from}'` });
          }
          const commit = yield* repository.readCommit(oid);
          const files = yield* repository.listFiles(commit.tree);
          return new Map(files.map((file) => [file.path, file]));
        });

  // Every path first, as `checkout` does: a bad one late in the batch would
  // otherwise abort after earlier paths had already been written or removed,
  // with `index.save` never reached.
  const wanted = yield* Effect.forEach(paths, (requested) =>
    validatePath(requested).pipe(Effect.map((path) => ({ path, requested }))),
  );

  for (const { path, requested } of wanted) {
    const fromSource = source?.get(path);
    const entry = entries.find((candidate) => candidate.path === path);

    // Restoring the index from a source that does not hold the path is how a
    // newly added file is unstaged — the entry goes away rather than being
    // written back from the index it was supposed to be rewritten from.
    if (toIndex && fromSource === undefined) {
      if (entry === undefined) {
        return yield* new Invalid({ field: "path", reason: `'${requested}' is not tracked` });
      }
      entries = removeEntry(entries, path);
      if (toWorktree) yield* work.remove(path);
      restored.push(path);
      continue;
    }

    const oid = fromSource?.oid ?? entry?.oid;
    if (oid === undefined) {
      return yield* new Invalid({ field: "path", reason: `'${requested}' is not tracked` });
    }
    const mode =
      fromSource === undefined ? (entry?.mode ?? REGULAR) : Number.parseInt(fromSource.mode, 8);

    // A gitlink has no bytes here to restore; the index entry is the whole of
    // what this repository records about it.
    if (toWorktree && mode !== 0o160000) {
      yield* work.write(path, yield* repository.readBlob(oid), mode);
    }
    if (toIndex) {
      // The file need not be on disk for the index to be restored: unstaging
      // a `git rm` is exactly the case where it is not, and skipping the
      // write there reports success while changing nothing.
      const stat = yield* work.stat(path);
      entries = addEntry(
        entries,
        entryFor(path, oid, stat === null ? blank(mode) : { ...stat, mode }),
      );
    }
    restored.push(path);
  }

  if (toIndex) yield* index.save(entries);
  return restored.sort();
});

/**
 * Replace the index and the work tree with a commit's tree, and point HEAD at
 * it — `git checkout` / `git switch`.
 *
 * Refuses when the work tree has changes that are not staged, because
 * overwriting them is the one thing a version control system must never do
 * silently. `force` is the way to say it anyway.
 */
export const checkout = Effect.fn("Checkout.checkout")(function* (
  target: string,
  options?: { readonly create?: boolean; readonly force?: boolean },
) {
  const repository = yield* Repository;
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  const ref = target.startsWith("refs/") ? target : `refs/heads/${target}`;

  // The branch is created *after* the refusals below, not before: a
  // `checkout -b` that is refused for a dirty work tree would otherwise
  // leave the branch behind, and the retry then fails because it exists.
  const create = options?.create === true;
  const tip = create
    ? yield* repository.resolve(yield* repository.head)
    : yield* repository.resolve(ref);
  if (tip === null) {
    return yield* new Invalid({ field: "target", reason: `unknown branch '${target}'` });
  }

  const commit = yield* repository.readCommit(tip);
  const wanted = yield* repository.listFiles(commit.tree);
  const wantedPaths = new Set(wanted.map((file) => file.path));

  if (options?.force !== true) {
    const current = yield* status();
    if (current.unstaged.length > 0) {
      return yield* new Invalid({
        field: "worktree",
        reason: `${current.unstaged.length} unstaged change(s) would be overwritten`,
      });
    }

    // An untracked file the target tree also has is content this repository
    // has never seen: overwriting it loses work that was never hashed, and
    // git refuses for exactly that reason. A staged addition the target does
    // not have would be deleted from disk *and* from the index, so it is the
    // same loss with an extra step.
    const clobbered = current.untracked.filter((path) => wantedPaths.has(path));
    if (clobbered.length > 0) {
      return yield* new Invalid({
        field: "worktree",
        reason: `untracked file(s) would be overwritten: ${clobbered.slice(0, 3).join(", ")}`,
      });
    }

    // Every staged change, not only additions: the index is rebuilt from the
    // target tree, so a staged *modification* is discarded just as completely
    // as a staged new file is deleted — and neither was ever committed.
    const staged = current.staged.map((entry) => entry.path);
    if (staged.length > 0) {
      return yield* new Invalid({
        field: "worktree",
        reason: `${staged.length} staged change(s) would be lost: ${staged.slice(0, 3).join(", ")}`,
      });
    }
  }

  // Anything the old index tracked and the new tree does not is removed;
  // untracked files are left alone, which is what makes a checkout safe.
  // Every path is validated first: these come from a tree and an index — a
  // clone's, so from whoever wrote them — and `..`, a leading `.git` or a
  // path that descends through a symlink written earlier in this same loop
  // would all land outside the checkout.
  const tracked = yield* index.load;
  // Every path first, before a single file moves: validating inside the loops
  // would abort a checkout that had already deleted the old tree, leaving a
  // work tree, an index and a HEAD that disagree.
  for (const file of wanted) yield* validatePath(file.path);
  for (const entry of tracked) {
    if (!wantedPaths.has(entry.path)) yield* validatePath(entry.path);
  }

  // Before a single file moves, not after the work tree has been rewritten:
  // creating the branch is the last thing here that can fail on its own —
  // `refs/heads/<name>` already existing is a `RefConflict` — and failing it
  // afterwards aborted with the old tree already deleted from disk and the
  // index never saved.
  if (create) {
    yield* repository.branch({ name: target.replace(/^refs\/heads\//, ""), base: tip });
  }

  for (const entry of tracked) {
    if (!wantedPaths.has(entry.path)) yield* work.remove(entry.path);
  }

  let entries: ReadonlyArray<IndexEntry> = [];
  for (const file of wanted) {
    const mode = Number.parseInt(file.mode, 8);
    // A gitlink is a commit in another repository: there is nothing to write
    // to disk, and reading it as a blob fails on an object this repository
    // does not have. It still belongs in the index, because the index is what
    // the next commit's tree is built from — and an entry missing from there
    // is a submodule deleted from history with no error and no conflict.
    if (isGitlink(file.mode)) {
      entries = addEntry(entries, entryFor(file.path, file.oid, blank(mode)));
      continue;
    }
    yield* work.write(file.path, yield* repository.readBlob(file.oid), mode);
    const stat = yield* work.stat(file.path);
    if (stat !== null)
      entries = addEntry(entries, entryFor(file.path, file.oid, { ...stat, mode }));
  }

  yield* index.save(entries);
  yield* repository.setHead(ref);

  return { ref, oid: tip, files: wanted.length };
});

/**
 * Commit what is staged.
 *
 * The tree comes from the index rather than from the caller, which is the
 * whole difference between this and `Repository.commit` — and the reason the
 * index exists at all.
 */
export const commit = Effect.fn("Checkout.commit")(function* (input: {
  readonly message: string;
  readonly author: import("./Format.ts").Signature;
  readonly expected?: Oid | null;
}) {
  const repository = yield* Repository;
  const index = yield* IndexStore;

  const entries = yield* index.load;
  if (entries.length === 0) {
    return yield* new Invalid({ field: "index", reason: "nothing staged" });
  }

  const branch = yield* repository.head;
  // A detached HEAD holds a commit, not the name of one. Passing it on would
  // create `refs/heads/<40-hex>` with no parent — a root commit under a
  // branch spelled as a sha, while HEAD never moves and the work looks lost.
  //
  // Refused before the tree is written, not after: writing first left every
  // tree of the refused commit in the object store, reachable from nothing
  // and collectable only by a gc.
  if (isOid(branch)) {
    return yield* new Invalid({
      field: "head",
      reason: "HEAD is detached; check out a branch before committing",
    });
  }

  // The index already names every blob, so the tree is built from oids
  // rather than by reading the content back out to write it again.
  const tree = yield* repository.writePaths(
    entries.map((entry) => ({
      path: entry.path,
      oid: entry.oid,
      mode: modeString(entry.mode),
    })),
  );

  const request = { branch, tree, message: input.message, author: input.author };
  const oid = yield* input.expected === undefined
    ? repository.commit(request)
    : repository.commit({ ...request, expected: input.expected });

  return { oid, tree, files: entries.length };
});
