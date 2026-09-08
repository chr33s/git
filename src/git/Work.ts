/**
 * The working tree, and the index that mediates it.
 *
 * Everything else here serves bare repositories, where a commit is built from
 * a tree the caller already has. A work tree is the other half of git: files
 * on disk that are edited, an index that records what has been staged, and
 * three states a path can disagree across — HEAD, index, disk.
 *
 * `WorkTree` describes the files on disk and `IndexStore` holds what has been
 * staged. `MergeState` separately records a merge waiting to be committed.
 * A server has none of these; a CLI has all three, and a browser can keep an
 * index without a filesystem worktree.
 *
 * `git/Index.ts` is the codec underneath `IndexStore` — git's own `DIRC` v2
 * format, so a repository this writes can be handed to the `git` binary and
 * back without either noticing.
 */
import { Context, Effect, Layer, Semaphore } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "./Error.ts";
import { type IndexEntry } from "./Index.ts";
import type { Oid } from "./Store.ts";

/** What `stat` tells us, and what the index caches to avoid re-hashing. */
export interface FileStat {
  readonly size: number;
  readonly mtimeSeconds: number;
  readonly mtimeNanos: number;
  readonly ctimeSeconds: number;
  readonly ctimeNanos: number;
  readonly device: number;
  readonly inode: number;
  readonly uid: number;
  readonly gid: number;
  /** git's mode: 0o100644, 0o100755, or 0o120000 for a symlink. */
  readonly mode: number;
}

export class WorkTree extends Context.Service<
  WorkTree,
  {
    /** Whether executable-bit changes on regular files are meaningful. */
    readonly trustExecutableBit: Effect.Effect<boolean, StorageFailure>;
    /** Unignored paths; indexed paths distinguish ordinary directories from gitlinks. */
    readonly list: (
      tracked: ReadonlyArray<{ readonly path: string; readonly mode: number }>,
    ) => Effect.Effect<ReadonlyArray<string>, StorageFailure>;
    readonly read: (path: string) => Effect.Effect<Uint8Array, ObjectNotFound | StorageFailure>;
    readonly stat: (path: string) => Effect.Effect<FileStat | null, StorageFailure>;
    /** An embedded repository's checked-out commit; a null OID means unborn HEAD. */
    readonly gitlink: (
      path: string,
    ) => Effect.Effect<
      { readonly oid: Oid | null; readonly stat: FileStat } | null,
      StorageFailure
    >;
    readonly write: (
      path: string,
      content: Uint8Array,
      mode: number,
    ) => Effect.Effect<void, StorageFailure>;
    /** Remove file/directory obstructions after checkout has approved overwriting them. */
    readonly prepareWrites: (paths: ReadonlyArray<string>) => Effect.Effect<void, StorageFailure>;
    readonly remove: (path: string) => Effect.Effect<void, StorageFailure>;
  }
>()("git/WorkTree") {}

export interface IndexAccess {
  readonly load: Effect.Effect<ReadonlyArray<IndexEntry>, StorageFailure>;
  readonly save: (entries: ReadonlyArray<IndexEntry>) => Effect.Effect<void, StorageFailure>;
}

export class IndexStore extends Context.Service<
  IndexStore,
  IndexAccess & {
    /** The callback owns exclusive access until it completes, fails or is interrupted. */
    readonly withLock: <A, E>(
      use: (access: IndexAccess) => Effect.Effect<A, E>,
    ) => Effect.Effect<A, E | StorageFailure>;
  }
>()("git/IndexStore") {}

/** An index in memory: enough for a browser, and for tests. */
export const indexMemory = Layer.sync(IndexStore, () => {
  let entries: ReadonlyArray<IndexEntry> = [];
  const semaphore = Semaphore.makeUnsafe(1);
  const access: IndexAccess = {
    load: Effect.sync(() => entries),
    save: (next) =>
      Effect.sync(() => {
        entries = next;
      }),
  };
  const withLock = Effect.fn("IndexStore.withLock")(
    <A, E>(use: (access: IndexAccess) => Effect.Effect<A, E>) =>
      semaphore.withPermit(Effect.suspend(() => use(access))),
  );
  return IndexStore.of({
    load: access.load,
    save: (next) => withLock((locked) => locked.save(next)),
    withLock,
  });
});

/** A work tree in memory, keyed by path. */
export const workTreeMemory = Layer.sync(WorkTree, () => {
  const files = new Map<string, { content: Uint8Array; mode: number }>();
  let clock = 1_700_000_000;

  const statOf = (file: { content: Uint8Array; mode: number }, at: number): FileStat => ({
    size: file.content.length,
    mtimeSeconds: at,
    mtimeNanos: 0,
    ctimeSeconds: at,
    ctimeNanos: 0,
    device: 0,
    inode: 0,
    uid: 0,
    gid: 0,
    mode: file.mode,
  });

  const times = new Map<string, number>();

  return WorkTree.of({
    trustExecutableBit: Effect.succeed(true),
    list: () => Effect.sync(() => [...files.keys()].sort()),
    gitlink: () => Effect.succeed(null),
    prepareWrites: Effect.fn("WorkTree.prepareWrites")((paths) =>
      Effect.sync(() => {
        for (const target of paths) {
          for (const file of files.keys()) {
            if (file.startsWith(`${target}/`) || target.startsWith(`${file}/`)) {
              files.delete(file);
              times.delete(file);
            }
          }
        }
      }),
    ),
    read: (path) => {
      const file = files.get(path);
      return file === undefined
        ? Effect.fail(new ObjectNotFound({ oid: path }))
        : Effect.succeed(file.content);
    },
    stat: (path) =>
      Effect.sync(() => {
        const file = files.get(path);
        return file === undefined ? null : statOf(file, times.get(path) ?? 0);
      }),
    write: (path, content, mode) =>
      Effect.sync(() => {
        files.set(path, { content, mode });
        times.set(path, clock++);
      }),
    remove: (path) =>
      Effect.sync(() => {
        files.delete(path);
        times.delete(path);
      }),
  });
});

/** Paths are relative, slash-separated, and may not escape the tree. */
export const validatePath = (path: string): Effect.Effect<string, Invalid> =>
  Effect.suspend(() => {
    const segments = path.split("/").filter((segment) => segment !== "");
    if (segments.length === 0) {
      return Effect.fail(new Invalid({ field: "path", reason: `empty path '${path}'` }));
    }
    if (segments.some((segment) => segment === "." || segment === "..")) {
      return Effect.fail(
        new Invalid({ field: "path", reason: `path escapes the tree: '${path}'` }),
      );
    }
    // Any segment, any case: `.GIT/hooks/post-checkout` is the same file on a
    // case-insensitive filesystem, and `a/.git/config` is one git refuses too.
    // `Repository.segmentsOf` applies exactly this rule when a tree is
    // written; this is the half that reads one back onto a work tree.
    if (segments.some((segment) => segment.toLowerCase() === ".git")) {
      return Effect.fail(new Invalid({ field: "path", reason: "the repository is not content" }));
    }
    return Effect.succeed(segments.join("/"));
  });

/** git's file modes, and the only ones a work tree produces. */
export const REGULAR = 0o100644;
export const EXECUTABLE = 0o100755;
export const SYMLINK = 0o120000;

export const modeString = (mode: number): string => mode.toString(8).padStart(6, "0");

/**
 * An index entry for a path as it is on disk right now.
 *
 * The stat fields are carried so `status` can skip re-hashing a file whose
 * size, mtime and ctime are unchanged — which is the difference between a status
 * that reads every byte in the tree and one that reads almost none.
 */
export const entryFor = (path: string, oid: Oid, stat: FileStat): IndexEntry => ({
  path,
  oid,
  mode: stat.mode,
  size: stat.size,
  mtimeSeconds: stat.mtimeSeconds,
  mtimeNanos: stat.mtimeNanos,
  ctimeSeconds: stat.ctimeSeconds,
  ctimeNanos: stat.ctimeNanos,
  device: stat.device,
  inode: stat.inode,
  uid: stat.uid,
  gid: stat.gid,
  stage: 0,
  assumeValid: false,
});

/** Whether the index's cached stat still describes the file on disk. */
export const unchanged = (entry: IndexEntry, stat: FileStat): boolean =>
  entry.size === stat.size &&
  entry.mtimeSeconds === stat.mtimeSeconds &&
  entry.mtimeNanos === stat.mtimeNanos &&
  entry.ctimeSeconds === stat.ctimeSeconds &&
  entry.ctimeNanos === stat.ctimeNanos &&
  entry.mode === stat.mode;

export type { IndexEntry };
export { StorageFailure };
