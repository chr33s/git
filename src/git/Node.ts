/**
 * Filesystem backend.
 *
 * The second implementation of the ports, and the one that proves they are
 * ports: it passes the same contract suite as `Memory.ts` without either the
 * suite or `Repository` knowing which is loaded.
 *
 * Layout is git's own, so a repository written here can be inspected with the
 * real `git` binary:
 *
 *   <root>/objects/ab/cdef…   loose objects, zlib-deflated
 *   <root>/refs/heads/main    one file per ref
 *   <root>/HEAD               symbolic ref
 *
 * Atomicity comes from `rename(2)`, which is atomic within a filesystem: a ref
 * update writes a temp file and renames it over the target. That is the same
 * guarantee `RefStore.apply` promises on Workers via the DO input gate, which
 * is why the port can demand it of every backend instead of leaving it
 * optional.
 */
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { deflateSync, inflateSync } from "node:zlib";

import { Effect, Layer, Stream } from "effect";

import { ObjectNotFound, StorageFailure } from "./Error.ts";
import {
  decodeObject,
  encodeObject,
  encodeReflogLine,
  hashObject,
  parseReflogLine,
} from "./Format.ts";
import { packed, type PackHandle, PackStore } from "./Packed.ts";
import {
  checkHeadTarget,
  checkRefAddress,
  checkRefNames,
  isOid,
  ObjectStore,
  type Oid,
  type ReflogEntry,
  RefStore,
  tracedRefStore,
  type RefUpdate,
  type RefUpdateResult,
} from "./Store.ts";

const failure = (operation: string, target: string) => (cause: unknown) =>
  new StorageFailure({ operation, path: target, cause });

/**
 * The repositories that read this one's objects.
 *
 * Two sources, because there are two ways to become a borrower. This server
 * records its own forks in `objects/info/borrowers` — cheap, exact, and
 * written where the fork is made. But `git clone --shared`/`--reference`
 * writes only the *child's* `alternates` and leaves no trace in the parent,
 * and a backup that dropped the borrowers file looks the same. So the
 * siblings are read too: a repository beside this one whose alternates name
 * this object store is borrowing from it, whoever set that up.
 *
 * The scan is one small read per sibling and happens once per `gc`, not per
 * object. A repository with no siblings — a bare path outside any root — sees
 * an empty directory listing and pays nothing.
 */
const borrowersOf = (root: string): ReadonlyArray<string> => {
  const recorded = readLines(path.join(root, "objects", "info", "borrowers"));
  const mine = path.resolve(root, "objects");

  const found = new Set(recorded);
  try {
    const siblings = path.dirname(root);
    for (const entry of readdirSync(siblings, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const candidate = path.join(siblings, entry.name);
      if (path.resolve(candidate) === path.resolve(root)) continue;
      if (alternatesOf(candidate).some((directory) => path.resolve(directory) === mine)) {
        found.add(entry.name);
      }
    }
  } catch {
    // No parent directory to read, or no permission: the recorded list is
    // still an answer, and a scan that cannot run must not fail a gc.
  }

  return [...found];
};

/** A small list file, or nothing at all — never a throw from a sync context. */
const readLines = (file: string): ReadonlyArray<string> => {
  try {
    if (!existsSync(file)) return [];
    return readFileSync(file, "utf8")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
  } catch {
    return [];
  }
};

/**
 * How much reflog a ref keeps. git expires by age; a bare server has nobody to
 * run `reflog expire`, so the bound here is on the file — reached only by a
 * ref that has moved thousands of times, and never by an ordinary branch.
 */
const REFLOG_MAX_BYTES = 256 * 1024;

/** Objects are immutable, so a write is temp-file plus rename, no locking. */
export const packStore = (root: string) => Layer.sync(PackStore, () => filePacks(root));

export const objectStore = (root: string) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const packs = yield* PackStore;
      const objectsDir = path.join(root, "objects");
      const pathFor = (oid: Oid) => path.join(objectsDir, oid.slice(0, 2), oid.slice(2));

      /**
       * Read-through only: writes, deletes and `list` stay with this
       * repository, so collecting garbage here can never touch the objects
       * another repository lent it.
       */

      const alternates = rememberAlternates(root);

      const borrowed = (oid: Oid): string | null => {
        for (const directory of alternates()) {
          const candidate = path.join(directory, oid.slice(0, 2), oid.slice(2));
          if (existsSync(candidate)) return candidate;
        }
        return null;
      };

      const read = (oid: Oid) =>
        Effect.tryPromise({
          // This repository's own copy first: a fork that has received its own
          // pushes would otherwise stat the parent once per object it already
          // holds, on the event-loop thread.
          try: () =>
            fs.readFile(existsSync(pathFor(oid)) ? pathFor(oid) : (borrowed(oid) ?? pathFor(oid))),
          catch: () => new ObjectNotFound({ oid }),
        }).pipe(
          Effect.flatMap((deflated) =>
            Effect.try({
              try: () => inflateSync(deflated),
              catch: failure("read", pathFor(oid)),
            }),
          ),
          Effect.flatMap((bytes) =>
            Effect.fromResult(decodeObject(new Uint8Array(bytes))).pipe(
              Effect.mapError(() => new ObjectNotFound({ oid })),
            ),
          ),
        );

      const loose: ObjectStore["Service"] = {
        read,
        readStream: (oid) =>
          read(oid).pipe(
            Effect.map(
              (object) =>
                Stream.fromIterable([object.data]) as Stream.Stream<Uint8Array, StorageFailure>,
            ),
          ),
        write: (object) =>
          Effect.gen(function* () {
            const oid = yield* hashObject(object);
            const target = pathFor(oid);

            // Already there: objects are content-addressed, so a rewrite would
            // be identical bytes and only costs IO.
            if (existsSync(target)) return oid;

            yield* Effect.tryPromise({
              try: async () => {
                await fs.mkdir(path.dirname(target), { recursive: true });
                const temporary = `${target}.${crypto.randomUUID()}.tmp`;
                await fs.writeFile(temporary, deflateSync(encodeObject(object)));
                await fs.rename(temporary, target);
              },
              catch: failure("write", target),
            });

            return oid;
          }),
        has: (oid) => Effect.sync(() => existsSync(pathFor(oid)) || borrowed(oid) !== null),
        delete: (oid) =>
          Effect.tryPromise({
            try: () => fs.rm(pathFor(oid), { force: true }),
            catch: failure("delete", pathFor(oid)),
          }),

        /**
         * What this repository lends and borrows.
         *
         * `gc` asks once, before it deletes anything: a borrower reads these
         * objects through `alternates` and keeps no copy, and its refs are
         * invisible from here, so collecting on this side would destroy
         * history it still advertises. Optional on the port — a backend that
         * cannot share objects has nothing to answer.
         */
        shared: Effect.sync(() => ({
          borrowers: borrowersOf(root),
          alternates: alternates(),
        })),
        list: Stream.unwrap(
          Effect.tryPromise({
            try: async () => {
              if (!existsSync(objectsDir)) return [];
              const oids: Oid[] = [];
              for (const prefix of await fs.readdir(objectsDir)) {
                // `objects/` also holds `pack/` and `info/`; only the two-hex
                // fanout directories hold loose objects, and a caller that was
                // handed `packpack-<sha>.idx` as an oid would go looking for
                // an object of that name.
                if (!/^[0-9a-f]{2}$/.test(prefix)) continue;
                for (const rest of await fs.readdir(path.join(objectsDir, prefix))) {
                  const oid = `${prefix}${rest}`;
                  if (isOid(oid)) oids.push(oid);
                }
              }
              return oids;
            },
            catch: failure("list", objectsDir),
          }).pipe(Effect.map(Stream.fromIterable)),
        ) as Stream.Stream<Oid, StorageFailure>,
      };

      return ObjectStore.of(packed(loose, packs, "Node"));
    }),
  );

/** One `objects/info/alternates` file, as the directories it names. */
const alternatesIn = (objectsDir: string): ReadonlyArray<string> => {
  const file = path.join(objectsDir, "info", "alternates");
  try {
    if (!existsSync(file)) return [];
    return readFileSync(file, "utf8")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0 && !line.startsWith("#"))
      .map((line) => (path.isAbsolute(line) ? line : path.resolve(objectsDir, line)));
  } catch {
    // Unreadable is "lends nothing", not a crash: this is consulted from
    // `Effect.sync` and from inside a `tryPromise` whose catch means "no such
    // object", so a throw here would surface as a defect or as a missing
    // object rather than as the storage error the port declares.
    return [];
  }
};

/**
 * Every object directory this repository reads through, `alternates` followed
 * to the end — git resolves them transitively, so a fork of a fork reaches its
 * grandparent's objects, and stopping at one hop loses that history.
 */
export const alternatesOf = (root: string): ReadonlyArray<string> => {
  const found: string[] = [];
  const seen = new Set<string>();
  const queue = [...alternatesIn(path.join(root, "objects"))];

  while (queue.length > 0) {
    const directory = queue.shift()!;
    if (seen.has(directory)) continue;
    seen.add(directory);
    found.push(directory);
    // A cycle is a misconfiguration, not a reason to loop forever.
    queue.push(...alternatesIn(directory));
  }

  return found;
};

/**
 * The same, re-read only when the file changes.
 *
 * The read path asks whenever an object is not here, so re-reading and
 * re-resolving per object would be a syscall storm on a fork. Keying the memo
 * on the file's mtime rather than on elapsed time is what makes a fork created
 * after this store was built visible to it — and what stops a directory that
 * was dropped and recreated under the same name from serving the previous
 * occupant's objects.
 */
const rememberAlternates = (root: string): (() => ReadonlyArray<string>) => {
  const file = path.join(root, "objects", "info", "alternates");
  let stamp: number | null = null;
  let dirs: ReadonlyArray<string> = [];

  return () => {
    let now: number | null = null;
    try {
      now = existsSync(file) ? statSync(file).mtimeMs : null;
    } catch {
      now = null;
    }
    if (now === stamp) return dirs;
    stamp = now;
    dirs = now === null ? [] : alternatesOf(root);
    return dirs;
  };
};

/**
 * Packs on disk, in git's own `objects/pack` layout.
 *
 * A `.pack` is read through `read(2)` at an offset rather than loaded, which
 * is what keeps a repository with a gigabyte pack usable from a process that
 * does not have a gigabyte.
 */
export const filePacks = (root: string): PackStore["Service"] => {
  const directory = path.join(root, "objects", "pack");
  const alternates = rememberAlternates(root);

  /**
   * Handles, re-read when a pack directory changes.
   *
   * `locate` lists on every read that misses the loose objects, and listing
   * here means reading every `.idx` off disk and verifying its checksum — so
   * a checkout of a packed repository paid that per object. The stamp is the
   * directories' mtimes, which is what a repack changes when it writes the
   * new pack and deletes the old ones, so a stale answer is not possible.
   */
  const cache = new Map<string, { stamp: string; handles: ReadonlyArray<PackHandle> }>();

  const stampOf = (directories: ReadonlyArray<string>): string =>
    directories
      .map((from) => {
        try {
          return existsSync(from) ? `${from}:${statSync(from).mtimeMs}` : `${from}:-`;
        } catch {
          return `${from}:?`;
        }
      })
      .join("|");

  /** Every readable `.pack`/`.idx` pair in these directories. */
  const handlesIn = (directories: ReadonlyArray<string>) =>
    Effect.tryPromise({
      try: async () => {
        const key = directories.join("|");
        const stamp = stampOf(directories);
        const cached = cache.get(key);
        if (cached !== undefined && cached.stamp === stamp) return cached.handles;
        const handles = await readHandles(directories);
        cache.set(key, { handles, stamp });
        return handles;
      },
      catch: failure("packs.list", directory),
    });

  const readHandles = async (directories: ReadonlyArray<string>): Promise<PackHandle[]> =>
    await (async () => {
      const found: Array<{ readonly directory: string; readonly name: string }> = [];
      for (const from of directories) {
        if (!existsSync(from)) continue;
        for (const entry of await fs.readdir(from)) {
          if (entry.endsWith(".idx")) found.push({ directory: from, name: entry.slice(0, -4) });
        }
      }

      const handles: PackHandle[] = [];
      for (const { directory: packDirectory, name } of found) {
        const packPath = path.join(packDirectory, `${name}.pack`);
        // An `.idx` without its `.pack` is a half-finished write, not a
        // pack; skipping it beats failing every read in the repository.
        if (!existsSync(packPath)) continue;

        const index = new Uint8Array(await fs.readFile(path.join(packDirectory, `${name}.idx`)));
        const size = (await fs.stat(packPath)).size;
        handles.push({
          name,
          index,
          source: {
            size,
            read: async (offset, length) => {
              const handle = await fs.open(packPath, "r");
              try {
                const buffer = Buffer.alloc(Math.max(0, Math.min(length, size - offset)));
                await handle.read(buffer, 0, buffer.length, offset);
                return new Uint8Array(buffer);
              } finally {
                await handle.close();
              }
            },
          },
        });
      }
      return handles;
    })();

  return {
    list: handlesIn([directory]),

    /**
     * The parent's packs, for reads only.
     *
     * A parent that has been repacked holds the lent history in a pack rather
     * than loose, so a fall-through that looked only for loose objects would
     * go blind the first time the parent ran `gc --repack`. Kept out of `list`
     * because that is what `gc` and `fsck` enumerate: folding these in makes a
     * fork report its parent's objects as its own and try to collect them.
     */
    borrowed: Effect.suspend(() =>
      handlesIn(alternates().map((alternate) => path.join(alternate, "pack"))),
    ),

    write: ({ index, name, pack }) =>
      Effect.tryPromise({
        try: async () => {
          await fs.mkdir(directory, { recursive: true });
          // The pack lands before the index that points into it: an index
          // without a pack is skipped above, but a pack without an index is
          // simply not consulted, and neither state loses an object.
          await fs.writeFile(path.join(directory, `${name}.pack`), pack);
          await fs.writeFile(path.join(directory, `${name}.idx`), index);
        },
        catch: failure("packs.write", directory),
      }),

    delete: (name) =>
      Effect.tryPromise({
        try: async () => {
          // Index first: it is what makes the pack visible, so removing it
          // first means a reader never sees an index whose pack is gone.
          await fs.rm(path.join(directory, `${name}.idx`), { force: true });
          await fs.rm(path.join(directory, `${name}.pack`), { force: true });
        },
        catch: failure("packs.delete", directory),
      }),
  };
};

/**
 * Refs on disk.
 *
 * The batch is checked before anything is written, so an atomic batch with one
 * stale entry writes nothing. Serializing concurrent batches is the host's job
 * (the readme's "One Durable Object per repository"), exactly as the DO input
 * gate does it on Workers; this layer assumes it is not racing itself.
 */
export const refStore = (root: string) =>
  Layer.effect(
    RefStore,
    Effect.sync(() => {
      const pathFor = (name: string) => path.join(root, name);
      const headPath = path.join(root, "HEAD");

      const readFile = (target: string) =>
        Effect.tryPromise({
          try: async () => {
            // A directory where a ref would be is not a ref — it is
            // `refs/heads/x` while `refs/heads/x/y` exists, which is the
            // shape a push has to be told about rather than crash on.
            if (!existsSync(target) || (await fs.stat(target)).isDirectory()) return null;
            return (await fs.readFile(target, "utf8")).trim();
          },
          catch: failure("read", target),
        });

      const writeAtomic = (target: string, contents: string) =>
        Effect.tryPromise({
          try: async () => {
            await fs.mkdir(path.dirname(target), { recursive: true });
            const temporary = `${target}.${crypto.randomUUID()}.tmp`;
            await fs.writeFile(temporary, contents);
            // rename(2) is atomic within a filesystem: a concurrent reader sees
            // either the old ref or the new one, never a half-written file.
            await fs.rename(temporary, target);
          },
          catch: failure("write", target),
        });

      /**
       * `packed-refs`, which git writes and this store must therefore read.
       *
       * `git gc` (and `git pack-refs`) moves loose refs into this one file and
       * removes them from `refs/`. A store that only walked the directory
       * would report a repository with a full history as having no refs at
       * all — and everything downstream, `gc` above all, would believe it.
       *
       * Writes stay loose: a loose ref shadows the packed entry, which is what
       * git itself does, so nothing here has to rewrite the file.
       */
      const packedRefsPath = path.join(root, "packed-refs");

      /**
       * Re-read only when the file changes, for the same reason the pack
       * handles above are.
       *
       * Every loose miss lands here, and `resolve` follows up to eight links
       * per name — so an advertisement or a push touching N refs read and
       * re-parsed this whole file O(N) times on the request path, which on a
       * repository whose refs `git pack-refs` has collected is *every* ref.
       *
       * The stamp is the file's mtime and size together, which leaves one gap
       * worth naming: a rewrite by another process that changes no byte count
       * and lands inside the filesystem's mtime resolution is not seen. Size
       * closes the common case of a ref being added or removed, and every
       * writer *here* clears the memo outright rather than trusting the stamp
       * — so what is left is a `git gc` from outside moving a ref to a
       * same-length oid in the same tick, on a filesystem coarse enough to
       * round them together. The pack handles above take the same bet.
       */
      let packedStamp: string | null = null;
      let packedMemo: ReadonlyMap<string, Oid> = new Map();

      const packedStampOf = (): string | null => {
        try {
          if (!existsSync(packedRefsPath)) return null;
          const stats = statSync(packedRefsPath);
          return `${stats.mtimeMs}:${stats.size}`;
        } catch {
          return null;
        }
      };

      /** Dropped when this store rewrites the file, so no stamp is involved. */
      const forgetPackedRefs = () => {
        packedStamp = null;
        packedMemo = new Map();
      };

      const packedRefs = Effect.suspend(() => {
        const stamp = packedStampOf();
        if (stamp !== null && stamp === packedStamp) return Effect.succeed(packedMemo);
        return Effect.tryPromise({
          try: async () => {
            const packed = new Map<string, Oid>();
            if (!existsSync(packedRefsPath)) return packed;

            for (const line of (await fs.readFile(packedRefsPath, "utf8")).split("\n")) {
              // `#` is the header, `^<oid>` the previous line's peeled target —
              // which is a tag's commit, not a ref of its own.
              if (line.length === 0 || line.startsWith("#") || line.startsWith("^")) continue;
              const [value = "", name = ""] = line.split(" ");
              if (name !== "" && isOid(value)) packed.set(name, value);
            }
            return packed;
          },
          catch: failure("read", packedRefsPath),
        }).pipe(
          Effect.map((packed): ReadonlyMap<string, Oid> => {
            // Stamped from before the read, so a write that landed *during* it
            // leaves a stamp that will not match and is re-read next time.
            packedStamp = stamp;
            packedMemo = packed;
            return packed;
          }),
        );
      });

      /**
       * A name that may be joined onto the root.
       *
       * `apply` and `setHead` are guarded, but every reader joins too — and
       * `reflog` takes its name straight from `?ref=`, so without this a
       * read-scoped token on one repository reads another's logs by asking
       * for `../../other/logs/refs/heads/main`.
       */
      const addressable = (name: string) => name === "HEAD" || checkRefAddress(name) === null;

      /** The file's text, whatever it holds — `resolve` needs the `ref: ` form. */
      const readRaw = (name: string) =>
        Effect.gen(function* () {
          if (!addressable(name)) return null;
          const loose = yield* readFile(pathFor(name));
          return loose ?? (yield* packedRefs).get(name) ?? null;
        });

      const read = (name: string) =>
        readRaw(name).pipe(
          // The port's contract is an oid: `apply` compares this against
          // `expected` and writes it into a commit's parent header, so a
          // symbolic ref's text must not come back branded as one.
          Effect.map((value) => (value !== null && isOid(value) ? (value as Oid) : null)),
        );

      /**
       * Drop one ref from `packed-refs`, leaving the rest of the file alone.
       *
       * A `^<oid>` line belongs to the ref line above it — it is that tag's
       * peeled target — so it goes with the ref it annotates and stays with
       * every other.
       */
      const removePacked = async (name: string): Promise<void> => {
        const target = packedRefsPath;
        if (!existsSync(target)) return;

        const lines = (await fs.readFile(target, "utf8")).split("\n");
        const kept: string[] = [];
        let dropping = false;
        for (const line of lines) {
          if (line.startsWith("^")) {
            if (!dropping) kept.push(line);
            continue;
          }
          dropping = line.split(" ")[1] === name;
          if (!dropping && line.length > 0) kept.push(line);
        }

        if (kept.length === lines.filter((line) => line.length > 0).length) return;
        const temporary = `${target}.${crypto.randomUUID()}.tmp`;
        await fs.writeFile(temporary, kept.length === 0 ? "" : `${kept.join("\n")}\n`);
        await fs.rename(temporary, target);
        forgetPackedRefs();
      };

      const head = readFile(headPath).pipe(
        Effect.map((value) => (value === null ? "refs/heads/main" : value.replace(/^ref:\s*/, ""))),
      );

      const appendReflog = (update: RefUpdate, from: Oid | null, at: Date) =>
        Effect.tryPromise({
          try: async () => {
            const target = path.join(root, "logs", update.name);
            await fs.mkdir(path.dirname(target), { recursive: true });
            const line = encodeReflogLine({
              from,
              to: update.value,
              at,
              message: update.reason ?? "update",
            });

            // Appending is the cheap path; a log that has grown past the cap
            // is rewritten with only its newest entries, so a busy ref cannot
            // turn its history into an unbounded file.
            const size = existsSync(target) ? (await fs.stat(target)).size : 0;
            if (size < REFLOG_MAX_BYTES) {
              await fs.appendFile(target, line);
              return;
            }

            // Trimmed in bytes, because bytes are what triggered it: keeping a
            // fixed number of *entries* would leave a log of long messages
            // above the cap forever, and keeping a fixed number of characters
            // would do the same for any message that is not ASCII.
            const existing = await fs.readFile(target);
            const tail = existing.subarray(-Math.floor(REFLOG_MAX_BYTES / 2));
            // From the first whole entry in the tail: a slice at a byte offset
            // can land inside a line, or inside a character.
            const start = tail.indexOf(0x0a) + 1;
            const temporary = `${target}.${crypto.randomUUID()}.tmp`;
            await fs.writeFile(temporary, Buffer.concat([tail.subarray(start), Buffer.from(line)]));
            // Renamed rather than written in place: a crash mid-rewrite would
            // otherwise leave the log truncated at whatever landed.
            await fs.rename(temporary, target);
          },
          catch: failure("reflog", update.name),
        });

      return RefStore.of(
        tracedRefStore("Node", {
          read,
          resolve: (name) =>
            Effect.gen(function* () {
              let current = name;
              // Bounded, because a `ref:` chain on disk can be a cycle — git
              // gives up after five; the extra hops cost nothing.
              for (let depth = 0; depth < 8; depth++) {
                // A detached HEAD holds the commit itself rather than a ref to
                // one — `git checkout <sha>`, a rebase or a bisect in progress
                // all leave one — and resolving it to null would drop that
                // commit from every caller that asks what HEAD reaches, `gc`
                // included.
                if (isOid(current)) return current;
                if (current === "HEAD") {
                  current = yield* head;
                  continue;
                }
                const value = yield* readRaw(current);
                if (value === null) return null;
                // Any ref may be symbolic, not only HEAD: `refs/remotes/origin/HEAD`
                // is one in every repository git clones.
                if (value.startsWith("ref: ")) {
                  current = value.slice("ref: ".length).trim();
                  continue;
                }
                // A file holding something that is not an oid is not a ref
                // this store can answer with; saying so beats handing back
                // whatever text it contained branded as an `Oid`.
                return isOid(value) ? value : null;
              }
              return null;
            }),
          list: (prefix) =>
            Effect.gen(function* () {
              // Packed first, loose over the top: a loose ref is the newer of
              // the two whenever both exist, exactly as git resolves it.
              const found = new Map<string, Oid>(yield* packedRefs);

              /** `refs/heads/x` -> `refs/heads/y`, for the symbolic ones. */
              const symbolic = new Map<string, string>();

              yield* Effect.tryPromise({
                try: async () => {
                  const base = path.join(root, "refs");
                  if (!existsSync(base)) return;

                  const walk = async (dir: string): Promise<void> => {
                    for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
                      const full = path.join(dir, entry.name);
                      if (entry.isDirectory()) {
                        await walk(full);
                        continue;
                      }
                      // A half-written ref, not a ref: `writeAtomic` names its
                      // temp files `<ref>.<uuid>.tmp`. Matching bare `.tmp`
                      // would hide `refs/heads/foo.tmp`, which is a name git
                      // and this store both accept — and a ref that is written
                      // but never listed is a branch gc collects the history
                      // of while the push that created it reports success.
                      if (/\.[0-9a-f-]{36}\.tmp$/.test(entry.name)) continue;

                      const name = path.relative(root, full).split(path.sep).join("/");
                      const value = (await fs.readFile(full, "utf8")).trim();
                      // `refs/remotes/origin/HEAD` is symbolic in every mirror
                      // git clones; handing its `ref: …` text back as an `Oid`
                      // would put that text in the advertisement.
                      if (value.startsWith("ref: ")) {
                        symbolic.set(name, value.slice("ref: ".length).trim());
                      } else if (isOid(value)) found.set(name, value);
                    }
                  };
                  await walk(base);
                },
                catch: failure("list", root),
              });

              for (const [name, target] of symbolic) {
                const value = found.get(target);
                if (value !== undefined) found.set(name, value);
              }

              return [...found]
                .filter(([name]) => prefix === undefined || name.startsWith(prefix))
                .map(([name, value]) => [name, value] as const);
            }),
          apply: (updates, options) =>
            Effect.gen(function* () {
              // Before any name is joined onto `root`: `pathFor` would happily
              // resolve `refs/../../etc/passwd` outside the repository.
              yield* checkRefNames(updates);

              const at = new Date();
              const results: RefUpdateResult[] = [];
              const pending: Array<{ from: Oid | null; update: RefUpdate }> = [];

              // Check everything first: an atomic batch that fails must not have
              // written anything, and rename(2) cannot be undone.
              for (const update of updates) {
                const actual = yield* read(update.name);
                const matches = update.expected === undefined || update.expected === actual;
                results.push({
                  name: update.name,
                  applied: matches,
                  current: matches ? update.value : actual,
                });
                if (matches) pending.push({ from: actual, update });
              }

              if (options?.atomic === true && results.some((result) => !result.applied)) {
                return yield* Effect.forEach(results, (result) =>
                  read(result.name).pipe(
                    Effect.map((current) => ({ name: result.name, applied: false, current })),
                  ),
                );
              }

              /** One update, as a ref write: `null` deletes. */
              const put = (name: string, value: Oid | null) =>
                value === null
                  ? Effect.tryPromise({
                      try: async () => {
                        await fs.rm(pathFor(name), { force: true });
                        // The loose file is only half of a ref that git has
                        // packed: leaving the `packed-refs` entry would let
                        // the next read resurrect a branch just deleted, and
                        // report the deletion as having worked.
                        await removePacked(name);
                      },
                      catch: failure("delete", pathFor(name)),
                    })
                  : writeAtomic(pathFor(name), `${value}\n`);

              /** What has been written, newest last, for an atomic undo. */
              const done: Array<{ from: Oid | null; update: RefUpdate }> = [];

              for (const { from, update } of pending) {
                const written = yield* put(update.name, update.value).pipe(
                  Effect.as(true),
                  // One ref that cannot be written — `refs/heads/x` where
                  // `refs/heads/x/y` is a directory, a full disk — must not
                  // report the refs that *were* written as untouched.
                  Effect.catchTag("StorageFailure", () => Effect.succeed(false)),
                );

                if (!written) {
                  // `atomic` is a promise about the batch, not about this ref.
                  // rename(2) cannot be undone, so the refs already moved are
                  // put back where they were and the whole batch reports as
                  // unapplied — which is what the caller asked for.
                  if (options?.atomic === true) {
                    for (const undo of done.reverse()) {
                      yield* put(undo.update.name, undo.from).pipe(Effect.ignore);
                    }
                    return yield* Effect.forEach(results, (result) =>
                      read(result.name).pipe(
                        Effect.map((current) => ({
                          name: result.name,
                          applied: false,
                          current,
                          // The batch failed because one ref could not be
                          // written, not because anyone else moved these.
                          reason: "cannot lock ref",
                        })),
                      ),
                    );
                  }

                  const index = results.findIndex((result) => result.name === update.name);
                  if (index !== -1) {
                    results[index] = {
                      name: update.name,
                      applied: false,
                      current: yield* read(update.name),
                      reason: "cannot lock ref",
                    };
                  }
                  continue;
                }

                done.push({ from, update });
              }

              // Once every write in the batch has landed, not as each one does.
              // An atomic batch that rolls back puts the refs themselves back,
              // but a line already appended here cannot be taken out of the
              // log — so `logs/refs/heads/x` recorded a move that was undone,
              // and `Maintenance.gc` reads reflog entries as roots, pinning
              // those rolled-back commits for the whole grace window.
              //
              // A reflog is the record of a move that has already happened:
              // failing the update because the record could not be written
              // would report a ref as untouched while it sits at its new
              // value. `fsck` is where an unwritable `logs/` is diagnosed.
              for (const { from, update } of done) {
                yield* appendReflog(update, from, at).pipe(Effect.ignore);
              }

              return results;
            }),
          head,
          setHead: (target) =>
            checkHeadTarget(target).pipe(Effect.andThen(writeAtomic(headPath, `ref: ${target}\n`))),
          reflog: (name) =>
            Effect.tryPromise({
              try: async () => {
                if (!addressable(name)) return [] as ReflogEntry[];
                const target = path.join(root, "logs", name);
                if (!existsSync(target)) return [] as ReflogEntry[];
                return (await fs.readFile(target, "utf8"))
                  .split("\n")
                  .map(parseReflogLine)
                  .filter((entry): entry is ReflogEntry => entry !== null);
              },
              catch: failure("reflog", name),
            }),
          logged: Effect.tryPromise({
            try: async () => {
              const base = path.join(root, "logs");
              if (!existsSync(base)) return [] as string[];

              const names: string[] = [];
              const walk = async (dir: string): Promise<void> => {
                for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
                  const full = path.join(dir, entry.name);
                  if (entry.isDirectory()) await walk(full);
                  else names.push(path.relative(base, full).split(path.sep).join("/"));
                }
              };
              await walk(base);
              return names;
            },
            catch: failure("reflog.list", root),
          }),
        }),
      );
    }),
  );

/** Both stores over one directory. */
export const stores = (root: string) =>
  Layer.mergeAll(objectStore(root), refStore(root)).pipe(Layer.provideMerge(packStore(root)));
