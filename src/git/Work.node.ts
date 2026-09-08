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
import * as IgnoreConfig from "./IgnoreConfig.node.ts";
import { directoryRules, rootRules, type IgnoreRules } from "./Ignore.node.ts";
import { mergeState } from "./MergeState.node.ts";
import { refStore } from "./Node.ts";
import { RefStore } from "./Store.ts";
import {
  EXECUTABLE,
  type FileStat,
  type IndexAccess,
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

/** Locate a nested checkout without descending into its content. */
const nestedGitDirectory = async (directory: string): Promise<string | null> => {
  try {
    if (!(await fsp.lstat(directory)).isDirectory()) return null;
    const marker = path.join(directory, ".git");
    const stat = await fsp.lstat(marker);
    if (stat.isDirectory()) return marker;
    if (!stat.isFile()) return null;
    const match = /^gitdir:\s*(.+)\s*$/m.exec(await fsp.readFile(marker, "utf8"));
    if (match?.[1] === undefined) throw new Error(`invalid git directory file: ${marker}`);
    return path.resolve(directory, match[1]);
  } catch (cause) {
    if (
      cause instanceof Error &&
      "code" in cause &&
      (cause.code === "ENOENT" || cause.code === "ENOTDIR")
    )
      return null;
    throw cause;
  }
};

export interface WorkTreeOptions {
  /** The checkout. */
  readonly root: string;
  /** Checkout-specific Git directory, for repository exclusions and linked worktrees. */
  readonly gitDirectory?: string;
  /** Directory names never walked into; `.git` is always one. */
  readonly ignore?: ReadonlyArray<string>;
}

export const workTree = (options: WorkTreeOptions): Layer.Layer<WorkTree, StorageFailure> =>
  Layer.effect(
    WorkTree,
    Effect.gen(function* () {
      const ignoreEnvironment = yield* IgnoreConfig.environment().pipe(
        Effect.mapError(failed("work.ignoreConfig", options.root)),
      );
      const ignored = new Set([".git", ...(options.ignore ?? [])]);
      const resolve = (relative: string) => path.join(options.root, relative);

      /**
       * The path a relative name denotes, once it is known to be inside the
       * checkout.
       *
       * `validatePath` refuses `..` and a leading `.git` in the *name*, but a
       * name can also be innocent and still land outside: a tree may hold a
       * symlink `link -> /etc` and then an entry `link/passwd`, and every
       * `fs` call would follow the link. Resolving the parent and requiring it
       * to stay under the root is what closes that — and it belongs here, in
       * the one place every operation resolves through, rather than on the
       * write path alone: `remove` following that same link deletes outside
       * the checkout, and `read` follows it out to answer with someone else's
       * file.
       */
      const contained = async (relative: string, create = false): Promise<string> => {
        const target = resolve(relative);
        const parent = path.dirname(target);

        /**
         * A path with its symlinks resolved, whether or not it exists yet.
         *
         * The links live in the part that exists, so that part is resolved and
         * the remainder — which cannot be a symlink, because it is not there —
         * is appended verbatim. Resolving only the existing prefix and then
         * comparing prefixes is the whole check: an earlier version compared
         * the *nearest existing ancestor* of each side and accepted a match in
         * either direction, so a symlink pointing at `..` resolved to an
         * ancestor of the checkout and passed. That is an arbitrary write.
         */
        const settled = async (from: string): Promise<string> => {
          const absolute = path.resolve(from);
          let at = absolute;
          while (!fs.existsSync(at) && at !== path.dirname(at)) at = path.dirname(at);
          const real = await fsp.realpath(at);
          return at === absolute ? real : path.join(real, path.relative(at, absolute));
        };

        // Decided before anything is created: `mkdir -p` follows a symlink as
        // readily as a write does, so checking after it would already have made
        // directories on the far side of the link.
        const base = await settled(options.root);
        const real = await settled(parent);
        if (real !== base && !real.startsWith(base + path.sep)) {
          throw new Error(`path escapes the work tree: '${relative}'`);
        }

        if (create) await fsp.mkdir(parent, { recursive: true });
        return target;
      };

      const walk = async (
        prefix: string,
        inherited: IgnoreRules,
        directories: ReadonlySet<string>,
        gitlinks: ReadonlySet<string>,
      ): Promise<string[]> => {
        const directory = prefix === "" ? options.root : path.join(options.root, prefix);
        if (!fs.existsSync(directory)) return [];
        if (
          prefix !== "" &&
          (gitlinks.has(prefix) ||
            (!directories.has(prefix) && (await nestedGitDirectory(directory)) !== null))
        )
          return [prefix];
        const rules = await directoryRules(options.root, prefix, inherited);

        const found: string[] = [];
        for (const entry of await fsp.readdir(directory, { withFileTypes: true })) {
          if (ignored.has(entry.name)) continue;
          const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
          if (rules.matcher.ignores(relative + (entry.isDirectory() ? "/" : ""))) continue;
          if (entry.isDirectory())
            found.push(...(await walk(relative, rules, directories, gitlinks)));
          else found.push(relative);
        }
        return found;
      };

      return WorkTree.of({
        trustExecutableBit: Effect.tryPromise({
          try: async () =>
            (
              await IgnoreConfig.forRepository(
                options.gitDirectory ?? path.join(options.root, ".git"),
                ignoreEnvironment,
              )
            ).trustExecutableBit,
          catch: failed("work.filemode", options.root),
        }),
        gitlink: Effect.fn("WorkTree.gitlink")(function* (relative: string) {
          const location = yield* Effect.tryPromise({
            try: async () => {
              const target = await contained(relative);
              const gitDirectory = await nestedGitDirectory(target);
              if (gitDirectory === null) return null;
              let common = gitDirectory;
              try {
                common = path.resolve(
                  gitDirectory,
                  (await fsp.readFile(path.join(gitDirectory, "commondir"), "utf8")).trim(),
                );
              } catch (cause) {
                if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT"))
                  throw cause;
              }
              return {
                gitDirectory,
                common,
                stat: { ...statOf(await fsp.lstat(target)), mode: 0o160000 },
              };
            },
            catch: failed("work.gitlink", relative),
          });
          if (location === null) return null;
          const oid = yield* Effect.flatMap(RefStore, (refs) => refs.resolve("HEAD")).pipe(
            Effect.provide(refStore(location.common, location.gitDirectory)),
          );
          return { oid, stat: location.stat };
        }),
        list: Effect.fn("WorkTree.list")((tracked) =>
          Effect.tryPromise({
            try: async () => {
              const directories = new Set<string>();
              const gitlinks = new Set<string>();
              for (const entry of tracked) {
                if (entry.mode === 0o160000) gitlinks.add(entry.path);
                const parts = entry.path.split("/");
                for (let length = 1; length < parts.length; length++)
                  directories.add(parts.slice(0, length).join("/"));
              }
              return (
                await walk(
                  "",
                  await rootRules(
                    options.gitDirectory ?? path.join(options.root, ".git"),
                    options.root,
                    ignoreEnvironment,
                  ),
                  directories,
                  gitlinks,
                )
              ).sort();
            },
            catch: failed("work.list", options.root),
          }),
        ),

        read: (relative) =>
          Effect.tryPromise({
            try: async () => {
              const target = await contained(relative);
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
                const stat = await fsp.lstat(await contained(relative));
                return stat.isFile() || stat.isSymbolicLink() ? statOf(stat) : null;
              } catch {
                return null;
              }
            },
            catch: failed("work.stat", relative),
          }),

        prepareWrites: Effect.fn("WorkTree.prepareWrites")((paths) =>
          Effect.tryPromise({
            try: async () => {
              const removals = new Set<string>();
              const stat = async (target: string) => {
                try {
                  return await fsp.lstat(target);
                } catch (cause) {
                  if (
                    cause instanceof Error &&
                    "code" in cause &&
                    (cause.code === "ENOENT" || cause.code === "ENOTDIR")
                  )
                    return null;
                  throw cause;
                }
              };
              for (const relative of paths) {
                const target = await contained(relative);
                let blockedParent = false;
                for (
                  let slash = relative.indexOf("/");
                  slash !== -1;
                  slash = relative.indexOf("/", slash + 1)
                ) {
                  const parent = await contained(relative.slice(0, slash));
                  const entry = await stat(parent);
                  if (entry !== null && !entry.isDirectory()) {
                    removals.add(parent);
                    blockedParent = true;
                    break;
                  }
                }
                if (!blockedParent && (await stat(target))?.isDirectory()) removals.add(target);
              }
              // Discover every obstruction before beginning removal; writes happen afterwards.
              for (const target of removals) await fsp.rm(target, { recursive: true, force: true });
            },
            catch: failed("work.prepareWrites", options.root),
          }).pipe(Effect.uninterruptible),
        ),

        write: (relative, content, mode) =>
          Effect.tryPromise({
            try: async () => {
              const target = await contained(relative, true);
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
          }).pipe(Effect.uninterruptible),

        remove: (relative) =>
          Effect.tryPromise({
            try: async () => {
              await fsp.rm(await contained(relative), { force: true });
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
          }).pipe(Effect.uninterruptible),
      });
    }),
  );

/**
 * The index as `.git/index`, in git's own format.
 *
 * Reserve Git's index.lock across the complete read/edit sequence. Each save
 * publishes a complete file while retaining that reservation until the caller
 * finishes its other mutations, such as moving HEAD or clearing merge state.
 */
export const indexFile = (gitDirectory: string): Layer.Layer<IndexStore> =>
  Layer.sync(IndexStore, () => {
    const location = path.join(gitDirectory, "index");
    const lock = `${location}.lock`;

    const access: IndexAccess = {
      load: Effect.tryPromise({
        try: async () => {
          let bytes: Uint8Array;
          try {
            bytes = new Uint8Array(await fsp.readFile(location));
          } catch (cause) {
            if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return [];
            throw cause;
          }
          const decoded = decodeIndex(bytes);
          if (decoded._tag === "Failure") throw decoded.failure;
          return decoded.success;
        },
        catch: failed("index.load", location),
      }),

      save: (entries) =>
        Effect.tryPromise({
          try: async () => {
            const temporary = `${location}.${crypto.randomUUID()}.tmp`;
            try {
              await fsp.writeFile(temporary, encodeIndex(entries), { flag: "wx" });
              await fsp.rename(temporary, location);
            } finally {
              await fsp.rm(temporary, { force: true });
            }
          },
          catch: failed("index.save", location),
        }).pipe(Effect.uninterruptible),
    };
    const withLock = Effect.fn("IndexStore.withLock")(
      <A, E>(use: (access: IndexAccess) => Effect.Effect<A, E>) =>
        Effect.acquireUseRelease(
          Effect.try({
            try: () => {
              fs.mkdirSync(gitDirectory, { recursive: true });
              const descriptor = fs.openSync(lock, "wx", 0o600);
              try {
                fs.closeSync(descriptor);
              } catch (cause) {
                fs.rmSync(lock, { force: true });
                throw cause;
              }
            },
            catch: failed("index.lock", lock),
          }),
          () => use(access),
          () =>
            Effect.try({
              try: () => fs.rmSync(lock, { force: true }),
              catch: failed("index.unlock", lock),
            }),
        ),
    );
    return IndexStore.of({
      load: access.load,
      save: (entries) => withLock((locked) => locked.save(entries)),
      withLock,
    });
  });

/** Files, index and pending merge state for one checkout. */
export const workspace = (root: string, gitDirectory = path.join(root, ".git")) =>
  Layer.mergeAll(
    workTree({ root, gitDirectory }),
    indexFile(gitDirectory),
    mergeState(gitDirectory),
  );
