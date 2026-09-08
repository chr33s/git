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
 * The index caches each file's size, mtime and ctime, so a status over an unchanged
 * tree hashes nothing — the difference between reading every byte in the
 * repository and reading none of it.
 */
import { Context, Effect } from "effect";

import { Invalid } from "./Error.ts";
import { hashObject, isGitlink } from "./Format.ts";
import { addEntry, type IndexEntry, removeEntry } from "./Index.ts";
import { MergeState } from "./MergeState.ts";
import { commitAt, Repository, treeAt } from "./Repository.ts";
import { isOid, type Oid } from "./Store.ts";
import {
  entryFor,
  EXECUTABLE,
  IndexStore,
  modeString,
  REGULAR,
  unchanged,
  validatePath,
  WorkTree,
} from "./Work.ts";

export type Change = "added" | "modified" | "deleted";

/** Keep the index reservation from the first read through the last mutation. */
const withIndexLock = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const context = yield* Effect.context<R | IndexStore>();
    const index = Context.get(context, IndexStore);
    return yield* index.withLock((access) =>
      effect.pipe(
        Effect.provideContext(
          Context.add(context, IndexStore, IndexStore.of({ ...index, ...access })),
        ),
      ),
    );
  });

export interface Status {
  /** Conflicts remain unresolved until their index stages are replaced. */
  readonly unmerged: ReadonlyArray<{ readonly path: string; readonly status: UnmergedStatus }>;
  /** Index against HEAD: what a commit would record. */
  readonly staged: ReadonlyArray<{ readonly path: string; readonly change: Change }>;
  /** Disk against the index: what a commit would miss. */
  readonly unstaged: ReadonlyArray<{ readonly path: string; readonly change: Change }>;
  /** On disk, in neither. */
  readonly untracked: ReadonlyArray<string>;
  readonly branch: string;
}

export type UnmergedStatus = "DD" | "AU" | "UD" | "UA" | "DU" | "AA" | "UU";

const conflictCodes: ReadonlyMap<number, UnmergedStatus> = new Map([
  [1, "DD"],
  [2, "AU"],
  [3, "UD"],
  [4, "UA"],
  [5, "DU"],
  [6, "AA"],
  [7, "UU"],
]);

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

/** Disabling filemode ignores permission changes, never changes in file type. */
const diskMode = (mode: number, indexed: number | undefined, trust: boolean): number => {
  if (trust || (mode !== REGULAR && mode !== EXECUTABLE)) return mode;
  return indexed === REGULAR || indexed === EXECUTABLE ? indexed : REGULAR;
};

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
  const trustExecutableBit = yield* work.trustExecutableBit;

  const head = yield* headFiles;
  const entries = yield* index.load;
  const staged = new Map(
    entries.filter((entry) => entry.stage === 0).map((entry) => [entry.path, entry]),
  );
  const conflicts = new Map<string, number>();
  for (const entry of entries) {
    if (entry.stage !== 0)
      conflicts.set(entry.path, (conflicts.get(entry.path) ?? 0) | (1 << (entry.stage - 1)));
  }
  const unmerged: Array<{ path: string; status: UnmergedStatus }> = [];
  for (const [path, mask] of conflicts) {
    const code = conflictCodes.get(mask);
    if (code === undefined)
      return yield* new Invalid({
        field: "index",
        reason: `invalid conflict stages for '${path}'`,
      });
    unmerged.push({ path, status: code });
  }
  const onDisk = yield* work.list(entries);

  const stagedChanges: Array<{ path: string; change: Change }> = [];
  for (const [path, entry] of staged) {
    const committed = head.get(path);
    if (committed === undefined) stagedChanges.push({ path, change: "added" });
    else if (committed.oid !== entry.oid || !sameMode(committed.mode, entry.mode)) {
      stagedChanges.push({ path, change: "modified" });
    }
  }
  for (const path of head.keys()) {
    if (!staged.has(path) && !conflicts.has(path)) stagedChanges.push({ path, change: "deleted" });
  }

  const unstaged: Array<{ path: string; change: Change }> = [];
  const untracked = onDisk.filter((path) => !staged.has(path) && !conflicts.has(path));

  // Ignore rules filter untracked discovery, never already indexed paths.
  for (const [path, entry] of staged) {
    if (entry.mode === 0o160000) {
      const gitlink = yield* work.gitlink(path);
      if (gitlink !== null && gitlink.oid !== entry.oid) {
        unstaged.push({ path, change: "modified" });
      }
      continue;
    }

    const stat = yield* work.stat(path);
    if (stat === null) {
      unstaged.push({ path, change: "deleted" });
      continue;
    }
    // Timestamp-preserving copies can change bytes without changing size or
    // mtime. The cached ctime must match too before skipping the read.
    if (unchanged(entry, stat)) continue;

    const content = yield* work.read(path);
    // Hashed, not written: `status` is a question, and answering it by storing
    // every modified file would leave the object store holding a blob for work
    // nobody has staged — garbage that only a `gc` can find its way back out of.
    const oid = yield* hashObject({ type: "blob", data: content });
    if (oid !== entry.oid || diskMode(stat.mode, entry.mode, trustExecutableBit) !== entry.mode) {
      unstaged.push({ path, change: "modified" });
    }
  }

  const sort = <A extends { path: string }>(items: Array<A>) =>
    items.sort((left, right) => left.path.localeCompare(right.path));

  return {
    unmerged: sort(unmerged),
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
  const trustExecutableBit = yield* work.trustExecutableBit;

  let entries = yield* index.load;
  const onDisk = yield* work.list(entries);
  const staged: string[] = [];
  const candidates = [...new Set([...onDisk, ...entries.map((entry) => entry.path)])];

  for (const requested of paths) {
    const normalized = requested === "." ? "" : yield* validatePath(requested);
    // A path may name a file or a directory; git takes both, and a caller
    // typing `src` means everything under it.
    const matches =
      normalized === ""
        ? candidates
        : candidates.filter((path) => path === normalized || path.startsWith(`${normalized}/`));

    if (matches.length === 0 && normalized !== "") {
      return yield* new Invalid({ field: "path", reason: `nothing matches '${requested}'` });
    }

    for (const path of matches) {
      const gitlink = yield* work.gitlink(path);
      if (gitlink !== null) {
        if (gitlink.oid === null) {
          return yield* new Invalid({
            field: "path",
            reason: `'${path}' does not have a commit checked out`,
          });
        }
        entries = addEntry(removeEntry(entries, path), entryFor(path, gitlink.oid, gitlink.stat));
        staged.push(path);
        continue;
      }
      const stat = yield* work.stat(path);
      if (stat === null) {
        if (entries.some((entry) => entry.path === path && entry.mode === 0o160000)) continue;
        entries = removeEntry(entries, path);
        staged.push(path);
        continue;
      }
      const oid = yield* repository.writeBlob(yield* work.read(path));
      const indexed = (
        entries.find((entry) => entry.path === path && entry.stage === 0) ??
        entries.find((entry) => entry.path === path && entry.stage === 2)
      )?.mode;
      entries = addEntry(
        removeEntry(entries, path),
        entryFor(path, oid, {
          ...stat,
          mode: diskMode(stat.mode, indexed, trustExecutableBit),
        }),
      );
      staged.push(path);
    }
  }

  yield* index.save(entries);
  return staged.sort();
}, withIndexLock);

/** Unstage and, unless `cached`, delete from disk — `git rm`. */
export const remove = Effect.fn("Checkout.remove")(function* (
  paths: ReadonlyArray<string>,
  options?: { readonly cached?: boolean; readonly force?: boolean },
) {
  const work = yield* WorkTree;
  const index = yield* IndexStore;
  const trustExecutableBit = yield* work.trustExecutableBit;

  let entries = yield* index.load;
  const removed = new Set<string>();
  const head = yield* headFiles;

  for (const requested of paths) {
    const normalized = yield* validatePath(requested);
    const matches = entries.filter(
      (entry) => entry.path === normalized || entry.path.startsWith(`${normalized}/`),
    );
    if (matches.length === 0)
      return yield* new Invalid({ field: "path", reason: `'${requested}' is not tracked` });
    for (const entry of matches) {
      if (options?.force !== true) {
        const original = head.get(entry.path);
        const matchesHead = original?.oid === entry.oid && sameMode(original.mode, entry.mode);
        const stat = yield* work.stat(entry.path);
        const matchesDisk =
          stat !== null &&
          diskMode(stat.mode, entry.mode, trustExecutableBit) === entry.mode &&
          (yield* hashObject({ type: "blob", data: yield* work.read(entry.path) })) === entry.oid;
        const safe =
          entry.stage === 0 &&
          (options?.cached === true
            ? matchesHead || matchesDisk
            : matchesHead && (stat === null || matchesDisk));
        if (!safe)
          return yield* new Invalid({
            field: "worktree",
            reason: `'${entry.path}' has uncommitted changes; use force to remove it`,
          });
      }
      removed.add(entry.path);
    }
  }
  for (const path of removed) {
    entries = removeEntry(entries, path);
    if (options?.cached !== true) yield* work.remove(path);
  }

  yield* index.save(entries);
  return [...removed].sort();
}, withIndexLock);

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
  if (entry.stage !== 0) {
    return yield* new Invalid({ field: "from", reason: `'${from}' is unmerged` });
  }
  if (
    entries.some((candidate) => candidate.path === target) ||
    (yield* work.stat(target)) !== null
  ) {
    return yield* new Invalid({ field: "to", reason: `'${to}' already exists` });
  }

  const stat = yield* work.stat(source);
  const content = yield* work.read(source);
  yield* work.write(target, content, stat?.mode ?? REGULAR);
  yield* work.remove(source);

  // The moved bytes may differ from the staged blob: force the next status to hash them.
  const next = addEntry(
    removeEntry(entries, source),
    entryFor(target, entry.oid, blank(entry.mode)),
  );
  yield* index.save(next);

  return { from: source, to: target };
}, withIndexLock);

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
          const oid = isOid(from) ? from : yield* repository.resolve(from);
          if (oid === null) {
            // An unborn branch has no HEAD to restore from. That is an error
            // when the caller named the source and merely nothing to take
            // when the default supplied it — `restore --staged` on a
            // repository without commits should not fail.
            if (options?.source === undefined) return null;
            return yield* new Invalid({ field: "source", reason: `unknown '${from}'` });
          }
          const files = yield* repository.listFiles(yield* treeAt(repository, oid));
          return new Map(
            files.map((file) => [
              file.path,
              { oid: file.oid, mode: Number.parseInt(file.mode, 8) },
            ]),
          );
        });

  // Resolve every requested path before changing either destination. A late
  // unmatched path must not fail after earlier local edits were overwritten.
  const wanted = yield* Effect.forEach(paths, (requested) =>
    Effect.gen(function* () {
      const path = yield* validatePath(requested);
      const entry = entries.find((candidate) => candidate.path === path);
      if (from === undefined && entry !== undefined && entry.stage !== 0) {
        return yield* new Invalid({ field: "index", reason: `'${requested}' is unmerged` });
      }
      const target = from === undefined ? entry : source?.get(path);
      if (entry === undefined && target === undefined) {
        return yield* new Invalid({ field: "path", reason: `'${requested}' is not tracked` });
      }
      return { path, target };
    }),
  );

  for (const { path, target } of wanted) {
    // Absence in the chosen source means deletion from the requested
    // destinations. Falling back to the index would restore different bytes.
    if (target === undefined) {
      if (toIndex) entries = removeEntry(entries, path);
      if (toWorktree) yield* work.remove(path);
      restored.push(path);
      continue;
    }

    const { oid, mode } = target;

    // A gitlink has no bytes here to restore; the index entry is the whole of
    // what this repository records about it.
    if (toWorktree && mode !== 0o160000) {
      yield* work.write(path, yield* repository.readBlob(oid), mode);
    }
    if (toIndex) {
      // The file need not be on disk for the index to be restored: unstaging
      // a `git rm` is exactly the case where it is not, and skipping the
      // write there reports success while changing nothing.
      entries = addEntry(removeEntry(entries, path), entryFor(path, oid, blank(mode)));
    }
    restored.push(path);
  }

  if (toIndex) yield* index.save(entries);
  return restored.sort();
}, withIndexLock);

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
  const merge = yield* MergeState;

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
    if (current.unmerged.length > 0) {
      return yield* new Invalid({
        field: "index",
        reason: "resolve the unmerged paths before switching branches",
      });
    }
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
    const wantedGitlinks = new Set(
      wanted.filter((file) => isGitlink(file.mode)).map((file) => file.path),
    );
    const wantedDirectories = new Set<string>();
    for (const path of wantedPaths) {
      for (let slash = path.indexOf("/"); slash !== -1; slash = path.indexOf("/", slash + 1)) {
        wantedDirectories.add(path.slice(0, slash));
      }
    }
    const clobbered: string[] = [];
    for (const path of current.untracked) {
      let overlaps = wantedPaths.has(path) || wantedDirectories.has(path);
      for (let slash = path.indexOf("/"); slash !== -1; slash = path.indexOf("/", slash + 1)) {
        const parent = path.slice(0, slash);
        if (wantedPaths.has(parent) && !wantedGitlinks.has(parent)) {
          overlaps = true;
          break;
        }
      }
      if (!overlaps) continue;
      // Restoring a gitlink only updates the outer index; the nested checkout stays untouched.
      if (wantedGitlinks.has(path) && (yield* work.gitlink(path)) !== null) continue;
      clobbered.push(path);
    }
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

  yield* work.prepareWrites(
    wanted.filter((file) => !isGitlink(file.mode)).map((file) => file.path),
  );

  for (const entry of tracked) {
    // Non-recursive checkout leaves nested repositories on disk when their gitlink disappears.
    if (!wantedPaths.has(entry.path) && entry.mode !== 0o160000) yield* work.remove(entry.path);
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

  // Complete publication and merge-state cleanup before cancellation releases the index lock.
  yield* Effect.gen(function* () {
    yield* index.save(entries);
    yield* repository.setHead(ref);
    yield* merge.clear;
  }).pipe(Effect.uninterruptible);

  return { ref, oid: tip, files: wanted.length };
}, withIndexLock);

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
  if (entries.some((entry) => entry.stage !== 0)) {
    return yield* new Invalid({ field: "index", reason: "unresolved merge conflicts" });
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

  const merge = yield* MergeState;
  const mergeParents = yield* Effect.forEach(yield* merge.heads, (oid) =>
    commitAt(repository, oid),
  );
  const previous = yield* repository.resolve(branch);

  // The index already names every blob, so the tree is built from oids
  // rather than by reading the content back out to write it again.
  const tree = yield* repository.writePaths(
    entries.map((entry) => ({
      path: entry.path,
      oid: entry.oid,
      mode: modeString(entry.mode),
    })),
  );

  if (
    mergeParents.length === 0 &&
    ((previous === null && entries.length === 0) ||
      (previous !== null && (yield* repository.readCommit(previous)).tree === tree))
  ) {
    return yield* new Invalid({ field: "index", reason: "nothing staged" });
  }
  const oid = yield* Effect.gen(function* () {
    const committed = yield* repository.commit({
      branch,
      tree,
      message: input.message,
      author: input.author,
      mergeParents,
      expected: input.expected === undefined ? previous : input.expected,
    });
    if (mergeParents.length > 0) yield* merge.clear;
    return committed;
  }).pipe(Effect.uninterruptible);

  return { oid, tree, files: entries.length };
}, withIndexLock);
