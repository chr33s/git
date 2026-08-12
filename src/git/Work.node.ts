/**
 * The work tree and index on a real filesystem.
 *
 * Its own module for `node:fs`, like `Subscribers.node.ts` and `Lfs.node.ts`.
 *
 * The layout is git's: the checkout lives in a directory and the repository is
 * `.git` inside it, so a tree written here is one the `git` binary can be
 * pointed at without conversion — which is the only way to know the index
 * codec and the mode handling are right.
 */
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { ObjectNotFound, StorageFailure } from "./Error.ts";
import { decodeIndex, encodeIndex } from "./Index.ts";
import {
  EXECUTABLE,
  type FileStat,
  IndexStore,
  REGULAR,
  SYMLINK,
  WorkTree,
} from "./Work.ts";

const failed = (operation: string, target: string) => (cause: unknown) =>
  new StorageFailure({ operation, path: target, cause });

/**
 * git records three modes and infers them from the filesystem: the owner
 * execute bit, and whether the entry is a link. Everything else it stores as
 * a plain file, which is why a mode round-trips even across filesystems that
 * do not keep permissions.
 */
const modeOf = (stat: fs.Stats): number => {
  if (stat.isSymbolicLink()) return SYMLINK;
  return (stat.mode & 0o111) === 0 ? REGULAR : EXECUTABLE;
};

const statOf = (stat: fs.Stats): FileStat => ({
  size: stat.size,
  mtimeSeconds: Math.floor(stat.mtimeMs / 1000),
  mtimeNanos: Math.floor((stat.mtimeMs % 1000) * 1e6),
  ctimeSeconds: Math.floor(stat.ctimeMs / 1000),
  ctimeNanos: Math.floor((stat.ctimeMs % 1000) * 1e6),
  device: stat.dev,
  inode: stat.ino,
  uid: stat.uid,
  gid: stat.gid,
  mode: modeOf(stat),
});

export interface WorkTreeOptions {
  /** The checkout. */
  readonly root: string;
  /** Directory names never walked into; `.git` is always one. */
  readonly ignore?: ReadonlyArray<string>;
}

export const workTree = (options: WorkTreeOptions): Layer.Layer<WorkTree> =>
  Layer.sync(WorkTree, () => {
    const ignored = new Set([".git", ...(options.ignore ?? [])]);
    const resolve = (relative: string) => path.join(options.root, relative);

    const walk = async (prefix: string): Promise<string[]> => {
      const directory = prefix === "" ? options.root : path.join(options.root, prefix);
      if (!fs.existsSync(directory)) return [];

      const found: string[] = [];
      for (const entry of await fsp.readdir(directory, { withFileTypes: true })) {
        if (ignored.has(entry.name)) continue;
        const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
        if (entry.isDirectory()) found.push(...(await walk(relative)));
        else found.push(relative);
      }
      return found;
    };

    return WorkTree.of({
      list: Effect.tryPromise({
        try: async () => (await walk("")).sort(),
        catch: failed("work.list", options.root),
      }),

      read: (relative) =>
        Effect.tryPromise({
          try: async () => {
            const target = resolve(relative);
            const stat = await fsp.lstat(target);
            // A symlink's content is its target, which is what git stores.
            return stat.isSymbolicLink()
              ? new TextEncoder().encode(await fsp.readlink(target))
              : new Uint8Array(await fsp.readFile(target));
          },
          catch: () => new ObjectNotFound({ oid: relative }),
        }),

      stat: (relative) =>
        Effect.tryPromise({
          try: async () => {
            try {
              return statOf(await fsp.lstat(resolve(relative)));
            } catch {
              return null;
            }
          },
          catch: failed("work.stat", relative),
        }),

      write: (relative, content, mode) =>
        Effect.tryPromise({
          try: async () => {
            const target = resolve(relative);
            await fsp.mkdir(path.dirname(target), { recursive: true });
            // Replacing a file with a link, or the reverse, needs the old one
            // gone first — `writeFile` would follow the link and overwrite
            // whatever it points at.
            await fsp.rm(target, { force: true });

            if (mode === SYMLINK) {
              await fsp.symlink(new TextDecoder().decode(content), target);
              return;
            }
            await fsp.writeFile(target, content);
            await fsp.chmod(target, mode === EXECUTABLE ? 0o755 : 0o644);
          },
          catch: failed("work.write", relative),
        }),

      remove: (relative) =>
        Effect.tryPromise({
          try: async () => {
            await fsp.rm(resolve(relative), { force: true });
            // git leaves no empty directories behind, so neither does this;
            // the walk up stops at the first one that is not empty.
            let directory = path.dirname(resolve(relative));
            while (directory !== options.root && directory.startsWith(options.root)) {
              const remaining = await fsp.readdir(directory).catch(() => ["stop"]);
              if (remaining.length > 0) break;
              await fsp.rmdir(directory);
              directory = path.dirname(directory);
            }
          },
          catch: failed("work.remove", relative),
        }),
    });
  });

/**
 * The index as `.git/index`, in git's own format.
 *
 * Temp-and-rename, like every other write here: a half-written index is the
 * one file that makes a repository unusable to both implementations at once.
 */
export const indexFile = (gitDirectory: string): Layer.Layer<IndexStore> =>
  Layer.sync(IndexStore, () => {
    const location = path.join(gitDirectory, "index");

    return IndexStore.of({
      load: Effect.tryPromise({
        try: async () => {
          if (!fs.existsSync(location)) return [];
          const decoded = decodeIndex(new Uint8Array(await fsp.readFile(location)));
          if (decoded._tag === "Failure") throw decoded.failure;
          return decoded.success;
        },
        catch: failed("index.load", location),
      }),

      save: (entries) =>
        Effect.tryPromise({
          try: async () => {
            await fsp.mkdir(gitDirectory, { recursive: true });
            const temporary = `${location}.${crypto.randomUUID()}.tmp`;
            await fsp.writeFile(temporary, encodeIndex(entries));
            await fsp.rename(temporary, location);
          },
          catch: failed("index.save", location),
        }),
    });
  });

/** Both, for a checkout at `root` whose repository is `root/.git`. */
export const workspace = (root: string) =>
  Layer.mergeAll(workTree({ root }), indexFile(path.join(root, ".git")));
