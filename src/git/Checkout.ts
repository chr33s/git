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
import { addEntry, type IndexEntry, removeEntry } from "./Index.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";
import {
  entryFor,
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
    else if (committed.oid !== entry.oid || committed.mode !== modeString(entry.mode)) {
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
    const oid = yield* repository.writeBlob(content);
    if (oid !== entry.oid || modeString(stat.mode) !== modeString(entry.mode)) {
      unstaged.push({ path, change: "modified" });
    }
  }

  const present = new Set(onDisk);
  for (const path of staged.keys()) {
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
  const next = addEntry(
    removeEntry(entries, source),
    entryFor(target, entry.oid, moved ?? { ...(stat ?? ({} as never)), mode: entry.mode }),
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

  const toWorktree = options?.worktree !== false || options.staged !== true;
  const toIndex = options?.staged === true;

  let entries = yield* index.load;
  const restored: string[] = [];

  // `--source` means "take the content from that commit" rather than from
  // whatever is staged.
  const source =
    options?.source === undefined
      ? null
      : yield* Effect.gen(function* () {
          const oid = yield* repository.resolve(options.source!);
          if (oid === null) {
            return yield* new Invalid({ field: "source", reason: `unknown '${options.source}'` });
          }
          const commit = yield* repository.readCommit(oid);
          const files = yield* repository.listFiles(commit.tree);
          return new Map(files.map((file) => [file.path, file]));
        });

  for (const requested of paths) {
    const path = yield* validatePath(requested);

    const fromSource = source?.get(path);
    const entry = entries.find((candidate) => candidate.path === path);
    const oid = fromSource?.oid ?? entry?.oid;
    if (oid === undefined) {
      return yield* new Invalid({ field: "path", reason: `'${requested}' is not tracked` });
    }
    const mode = fromSource === undefined ? (entry?.mode ?? REGULAR) : Number.parseInt(fromSource.mode, 8);

    if (toWorktree) {
      yield* work.write(path, yield* repository.readBlob(oid), mode);
    }
    if (toIndex) {
      const stat = yield* work.stat(path);
      if (stat !== null) entries = addEntry(entries, entryFor(path, oid, { ...stat, mode }));
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

  if (options?.force !== true) {
    const current = yield* status();
    if (current.unstaged.length > 0) {
      return yield* new Invalid({
        field: "worktree",
        reason: `${current.unstaged.length} unstaged change(s) would be overwritten`,
      });
    }
  }

  const ref = target.startsWith("refs/") ? target : `refs/heads/${target}`;

  if (options?.create === true) {
    const head = yield* repository.head;
    yield* repository.branch({ name: target.replace(/^refs\/heads\//, ""), base: head });
  }

  const tip = yield* repository.resolve(ref);
  if (tip === null) {
    return yield* new Invalid({ field: "target", reason: `unknown branch '${target}'` });
  }

  const commit = yield* repository.readCommit(tip);
  const wanted = yield* repository.listFiles(commit.tree);
  const wantedPaths = new Set(wanted.map((file) => file.path));

  // Anything the old index tracked and the new tree does not is removed;
  // untracked files are left alone, which is what makes a checkout safe.
  for (const entry of yield* index.load) {
    if (!wantedPaths.has(entry.path)) yield* work.remove(entry.path);
  }

  let entries: ReadonlyArray<IndexEntry> = [];
  for (const file of wanted) {
    const mode = Number.parseInt(file.mode, 8);
    yield* work.write(file.path, yield* repository.readBlob(file.oid), mode);
    const stat = yield* work.stat(file.path);
    if (stat !== null) entries = addEntry(entries, entryFor(file.path, file.oid, { ...stat, mode }));
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

  // The index already names every blob, so the tree is built from oids
  // rather than by reading the content back out to write it again.
  const tree = yield* repository.writePaths(
    entries.map((entry) => ({
      path: entry.path,
      oid: entry.oid,
      mode: modeString(entry.mode),
    })),
  );

  const branch = yield* repository.head;
  const oid = yield* repository.commit({
    branch,
    tree,
    message: input.message,
    author: input.author,
    ...(input.expected === undefined ? {} : { expected: input.expected }),
  });

  return { oid, tree, files: entries.length };
});
