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
import { existsSync, readdirSync, readFileSync, realpathSync, statSync } from "node:fs";
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
import { inflate as nativeInflate } from "./Inflate.zlib.ts";
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
export const borrowersOf = (root: string): ReadonlyArray<string> => {
  // Resolved before the dirname is taken: `path.dirname(".")` is `"."`, so a
  // repository opened by a relative path — which is what `gc .` from inside
  // one gives — would look for its siblings inside itself. The listing below
  // survives that by finding nothing; the borrower check does not, because
  // "not at that path" is exactly the answer that releases a fork's objects.
  const siblingRoot = path.dirname(path.resolve(root));

  /**
   * The directory as the filesystem knows it, or as written when it cannot
   * say.
   *
   * `alternates` is written by whoever made the fork — `git clone --shared`
   * writes the path it was given — so the same object directory reaches here
   * spelled through a symlink, through `..`, or through a mount point, and
   * comparing the strings makes a live borrower look like one reading
   * somewhere else. That is a miss in the sibling scan below and a release in
   * `collected`, which is a `gc` that deletes what the fork reads.
   */
  const canonical = (target: string): string => {
    try {
      return realpathSync.native(target);
    } catch {
      return path.resolve(target);
    }
  };

  const mine = canonical(path.join(root, "objects"));

  /** There, and a directory. Anything else — including "could not look". */
  const present = (target: string): boolean => {
    try {
      return statSync(target).isDirectory();
    } catch {
      return false;
    }
  };

  /** Whether a repository has an `alternates` file at all — `undefined` where the question could not be put. */
  const borrows = (repository: string): boolean | undefined => {
    try {
      readFileSync(path.join(repository, "objects", "info", "alternates"));
      return true;
    } catch (error) {
      const code = error instanceof Error && "code" in error ? error.code : undefined;
      return code === "ENOENT" ? false : undefined;
    }
  };

  /**
   * Whether a recorded borrower demonstrably does not borrow.
   *
   * Demonstrably, and nothing weaker: a line here is the only thing standing
   * between `gc` and objects a fork reads through `alternates`, so it is
   * released on evidence rather than on the absence of evidence. The evidence
   * is a repository standing at the borrower's name with no `alternates` at
   * all — reading through nobody, so through nobody transitively either. That
   * is what a name reused by an unrelated repository looks like, and it is
   * the whole of what this releases.
   *
   * Deliberately not "its alternates name somewhere else": git resolves them
   * transitively, so somewhere else can still arrive here through a hop this
   * would have to follow — and following it means trusting every read along
   * the way, where an unreadable link in the chain reads as "does not borrow"
   * and collects a live fork's objects.
   *
   * Nor is not being at `<siblings>/<name>` evidence. That is also what a
   * borrower under another layout looks like, and what a mount that is not up
   * yet looks like. The cost of keeping a line instead is a `gc` this
   * repository refuses until the fork's own delete is retried, or until
   * somebody removes the line — recoverable, which the other direction is not.
   */
  const collected = (name: string): boolean => {
    if (name !== path.basename(name) || name === "." || name === "..") return false;
    const at = path.join(siblingRoot, name);
    // Bare and not, because `git clone --shared` — the case the recorded list
    // exists alongside — makes a working tree with its git directory in
    // `.git`, and asking only the bare spelling answers ENOENT for it.
    const answers = [borrows(at), borrows(path.join(at, ".git"))];
    if (answers.some((answer) => answer !== false)) return false;
    // Neither spelling has one, so nothing is read through from there — but
    // only where the repository itself is, so that a directory this cannot
    // look at keeps its line too.
    return present(at);
  };

  const recorded = readLines(path.join(root, "objects", "info", "borrowers")).filter(
    (name) => !collected(name),
  );

  const found = new Set(recorded);
  const here = canonical(root);
  try {
    const siblings = siblingRoot;
    for (const entry of readdirSync(siblings, { withFileTypes: true })) {
      // A plain file is not a repository, and asking the filesystem again
      // about one would be a syscall per entry for nothing.
      if (entry.isFile()) continue;
      const candidate = path.join(siblings, entry.name);
      // Everything else is asked rather than assumed. `readdir` describes the
      // entry, so a repository kept elsewhere and linked into place here — an
      // ordinary way to put one on another disk — reads as "not a directory";
      // and where the kernel did not fill in the type at all, which is what
      // XFS without `ftype`, FUSE and some network mounts do, every entry
      // reads as neither a directory nor a link. Skipping those is a scan
      // that finds no borrowers at all, and a `gc` that deletes what they
      // read.
      if (!entry.isDirectory() && !present(candidate)) continue;
      // Both spellings again: a `git clone --shared` working tree keeps its
      // git directory in `.git`, and that is the borrower with nothing on the
      // lender's side to record it.
      const borrows = [candidate, path.join(candidate, ".git")].some((repository) =>
        alternatesOf(repository).some((directory) => canonical(directory) === mine),
      );
      // Asked here rather than at the top of the loop, where it was a
      // `realpath` for every entry in the namespace and this runs once per
      // sibling on a delete. Only a repository that reads through this store
      // can be this one under another spelling, and reaching this at all
      // takes an `alternates` file naming its own objects.
      if (borrows && canonical(candidate) !== here) found.add(entry.name);
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
          read(oid).pipe(Effect.map((object) => Stream.fromIterable([object.data]))),
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
                  // Temp files from in-flight writes live beside the objects;
                  // anything that is not forty hex characters is not an object.
                  const oid = `${prefix}${rest}`;
                  if (isOid(oid)) oids.push(oid);
                }
              }
              return oids;
            },
            catch: failure("list", objectsDir),
          }).pipe(Effect.map(Stream.fromIterable)),
        ),
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
 * Open `.pack` descriptors, shared by every pack store in the process.
 *
 * A pack read is one `pread(2)`, but it used to be an `open`/`read`/`close`
 * triple: reading 200 commits out of a packed repository opened the same file
 * 399 times. Holding the descriptor instead costs one `open` for the whole
 * walk — worth ~11% of `log -n 200`, and far more to a host answering a clone,
 * which reads every object in the pack.
 *
 * The pool is capped because a host is long-lived and descriptors are not:
 * one store per repository, each holding its packs open, is a file descriptor
 * limit waiting to be hit. Past the cap the least recently used file is
 * retired, and a later read re-opens it.
 *
 * Retiring waits for readers. A descriptor closed under an in-flight
 * `read(2)` is an `EBADF` on a good day, and node makes the sloppier version
 * fatal: a `FileHandle` collected without an explicit close throws
 * `ERR_INVALID_STATE` and takes the process with it.
 */
const OPEN_PACKS = 64;

interface OpenPack {
  readonly key: string;
  readonly file: Promise<fs.FileHandle>;
  /** Which file this descriptor is on, to notice being handed a new one. */
  readonly inode: Promise<{ readonly dev: number; readonly ino: number } | null>;
  /** Reads in flight; a retired file closes when this reaches zero. */
  readers: number;
  retired: boolean;
  /** Resolves once the descriptor is actually closed, not merely retired. */
  readonly closed: Promise<void>;
  readonly settle: () => void;
}

const openPacks = new Map<string, OpenPack>();

/**
 * Packs that have left the map but whose descriptor is not closed yet.
 *
 * "Retired" and "closed" are not the same moment, and callers who are about
 * to unlink a file need the second one. Without this a pack the cap evicted a
 * tick ago, or one another caller is already retiring, would answer
 * `retirePack` instantly while its descriptor was still open — on Windows,
 * the unlink that follows fails.
 *
 * A set of records rather than a map by path: the same pack can have an old
 * descriptor closing while a new one is already pooled, and a caller that
 * wants the file closable has to wait for every one of them.
 */
interface ClosingPack {
  readonly key: string;
  readonly closed: Promise<void>;
}

const closingPacks = new Set<ClosingPack>();

/** Mark a retired pack as on its way out, until its descriptor settles. */
const beginClosing = (open: OpenPack): void => {
  const record: ClosingPack = { key: open.key, closed: open.closed };
  closingPacks.add(record);
  void open.closed.then(() => {
    closingPacks.delete(record);
    // A descriptor actually gone is a slot actually free — which is not the
    // same moment as the entry leaving the map.
    offerSlot();
  });
};

/**
 * Descriptors this process is holding, retired or not.
 *
 * The cap is on open files, and a retired pack is still an open file until
 * its `close` lands. Counting only the map would let a burst admit new opens
 * faster than the event loop retires old ones, which is the `EMFILE` the
 * waiting below exists to prevent.
 */
const heldPacks = (): number => openPacks.size + closingPacks.size + reservedSlots;

const closePack = async (open: OpenPack): Promise<void> => {
  try {
    await (await open.file).close();
  } catch {
    // A pack that cannot be closed is a pack that is already gone; the read
    // that needs it will say so with a better message than this would.
  } finally {
    open.settle();
  }
};

/**
 * Retire least-recently-used files until the pool is down to `limit`.
 *
 * Only idle ones. Evicting a file with reads in flight sheds the map entry
 * but not the descriptor, and the next read of that pack opens a second one —
 * so a host with more hot packs than the cap would hold *more* descriptors
 * than the cap, not fewer, and thrash back to an open per read. The map is
 * allowed over its cap instead, by however many reads are in flight, which
 * is what a `pread` apiece bounds to something small and short-lived.
 */
const retirePacks = (limit: number): void => {
  for (const [key, open] of openPacks) {
    if (openPacks.size <= limit) return;
    if (open.readers > 0) continue;
    openPacks.delete(key);
    open.retired = true;
    beginClosing(open);
    void closePack(open);
  }
};

/**
 * Callers waiting for a descriptor to come free.
 *
 * Skipping busy entries is what keeps a read from being closed out from under
 * — but on its own it turns the cap into a suggestion: a host answering a
 * hundred repositories at once would open a hundred descriptors, which is how
 * a process meets `EMFILE`. So a read that needs a new file when every slot is
 * taken waits here instead, and the next read to finish wakes it. It cannot
 * deadlock: waking depends only on reads that already hold their descriptor,
 * and a read is one `pread` that opens nothing further.
 */
const packSlot: Array<() => void> = [];

/**
 * Slots handed to a waiter that has not woken up yet.
 *
 * Waking a waiter is a microtask; a read arriving in the meantime runs
 * synchronously and would take the descriptor the waiter was just given, put
 * the waiter back at the end of the queue, and — with reads arriving steadily
 * — leave it there. Counting the handover as held is what makes the queue a
 * queue rather than a suggestion.
 */
let reservedSlots = 0;

/** Wake one waiter, once there is actually room for the file it wants. */
const offerSlot = (): void => {
  if (heldPacks() >= OPEN_PACKS) return;
  const next = packSlot.shift();
  if (next === undefined) return;
  reservedSlots += 1;
  next();
};

/**
 * Wait for room for one more descriptor.
 *
 * Room for *one more*, not room to be at the cap: evicting down to the cap
 * leaves a full pool, which is what the caller is already stuck behind.
 */
const freeSlot = async (): Promise<boolean> => {
  retirePacks(OPEN_PACKS - 1);
  if (heldPacks() < OPEN_PACKS) return false;
  await new Promise<void>((resolve) => packSlot.push(resolve));
  retirePacks(OPEN_PACKS - 1);
  // The reservation stays with the caller — released where the descriptor it
  // was reserved for is opened, not here. Dropping it on the way out would
  // leave it free for the length of a microtask, which is all a read arriving
  // synchronously needs to take it and put this caller back in the queue.
  return true;
};

/**
 * The pool's key for a pack.
 *
 * One file has several spellings. A store built on a relative root — which is
 * what `--root .` gives the CLI — reaches its own packs relatively, while an
 * alternate is resolved absolute before it is followed; a repository can be
 * reached through a symlink; and on Windows or macOS the same path differs
 * only in case. Keyed as they come, one pack takes two descriptors and, worse,
 * a `delete` retires one spelling and unlinks under the other — which is the
 * failure `retiringRemove` exists to prevent.
 *
 * `realpath.native` is what answers all three, and it is a syscall, so the
 * answers are kept. The memo is a cache and nothing more: dropped wholesale
 * once it outgrows the pool it serves.
 */
const canonicalPacks = new Map<string, string>();
const canonicalDirectories = new Map<string, string>();

/**
 * A bounded memo, least recently used out.
 *
 * Clearing it wholesale would put a blocking `realpath` back on the read path
 * for any working set larger than the bound — the opposite of what the pool
 * is for — so the hot spellings stay and the cold ones go.
 */
const remember = (memo: Map<string, string>, key: string, value: string): string => {
  // Deleted before it is set: `Map.set` on a key that is already there keeps
  // its old position, which would make this queue by first insertion and
  // evict the paths asked for most.
  memo.delete(key);
  if (memo.size >= OPEN_PACKS * 16) {
    const oldest = memo.keys().next();
    if (oldest.done !== true) memo.delete(oldest.value);
  }
  memo.set(key, value);
  return value;
};

/** The memo's answer, refreshed as most recently used. */
const recall = (memo: Map<string, string>, key: string): string | undefined => {
  const known = memo.get(key);
  if (known === undefined) return undefined;
  memo.delete(key);
  memo.set(key, known);
  return known;
};

/**
 * The best canonical spelling still obtainable for a path that is gone.
 *
 * What it resolved to while it was here, and failing that as much of the path
 * as still resolves: the memo is bounded, and one listing of a large root is
 * enough to evict an entry between the read that pooled a descriptor and the
 * retire that should close it. The ancestors carrying the symlinks outlive
 * the tail — it is the pack that was just unlinked — so walking up arrives at
 * the same answer `realpath` would have given while it was there. `resolve`
 * alone, which is where this used to stop, matches no pooled key under a
 * symlinked or relative root, and a descriptor that matches nothing is one
 * that never closes.
 */
const lastKnown = (memo: Map<string, string>, resolved: string): string => {
  const known = recall(memo, resolved);
  if (known !== undefined) return known;

  const missing: string[] = [];
  let head = resolved;
  for (;;) {
    const parent = path.dirname(head);
    if (parent === head) return resolved;
    missing.push(path.basename(head));
    head = parent;
    try {
      return path.join(realpathSync.native(head), ...missing.slice().reverse());
    } catch {
      // Keep walking up.
    }
  }
};

const packKey = (packPath: string): string => {
  const resolved = path.resolve(packPath);
  try {
    const canonical = realpathSync.native(resolved);
    // Remembered for when it is gone, not consulted while it is here: a memo
    // that answers first goes stale the moment anything is relinked, and a
    // stale key matches no prefix and retires nothing.
    remember(canonicalPacks, resolved, canonical);
    return canonical;
  } catch {
    return lastKnown(canonicalPacks, resolved);
  }
};

/**
 * The pool's key prefix for everything under a directory.
 *
 * Canonical, exactly like the keys — a prefix built with `path.resolve` alone
 * matches nothing at all under a symlinked or differently-cased root, which
 * would silently turn every retire and every staleness check into a no-op.
 *
 * The answer is remembered because these are asked for after the directory is
 * gone as much as before: retiring after an unlink is the supported order,
 * and by then `realpath` has nothing left to resolve.
 */
const packPrefix = (directory: string): string => {
  const resolved = path.resolve(directory);
  try {
    return remember(canonicalDirectories, resolved, realpathSync.native(resolved)) + path.sep;
  } catch {
    // Gone — and retiring after the unlink is the supported order, so this is
    // the ordinary path rather than the exceptional one.
    return lastKnown(canonicalDirectories, resolved) + path.sep;
  }
};

/**
 * Run `use` against a pack's descriptor, opening or reusing as needed.
 *
 * `key` is canonical already — `readHandles` resolves each pack once when it
 * lists them, and a `realpath` per read is exactly the blocking syscall this
 * pool exists to remove.
 */
const withPack = async <A>(key: string, use: (file: fs.FileHandle) => Promise<A>): Promise<A> => {
  let open = openPacks.get(key);
  let reserved = false;
  // A reservation this caller already holds is not something to wait behind.
  while (open === undefined && heldPacks() - (reserved ? 1 : 0) >= OPEN_PACKS) {
    // Every slot taken and this pack is not one of them: wait for one rather
    // than opening past the cap. Re-read afterwards — the pack may have been
    // pooled by whoever we were waiting on.
    reserved = (await freeSlot()) || reserved;
    open = openPacks.get(key);
  }

  const opening = open === undefined;

  if (open === undefined) {
    const closed = Promise.withResolvers<void>();
    const file = fs.open(key, "r");
    const entry: OpenPack = {
      key,
      file,
      inode: file
        .then(async (handle) => {
          const stat = await handle.stat();
          return { dev: stat.dev, ino: stat.ino };
        })
        .catch(() => null),
      readers: 0,
      retired: false,
      closed: closed.promise,
      settle: closed.resolve,
    };
    // A failed open must not be remembered: a pack deleted between the
    // listing and the read would otherwise replay that rejection to every
    // later read of the same path, including after a repack writes it back.
    void entry.file.catch(() => {
      if (openPacks.get(key) === entry) openPacks.delete(key);
    });
    open = entry;
    openPacks.set(key, entry);
  } else {
    // Re-insert to mark it most recently used: a Map iterates in insertion
    // order, which is the whole LRU.
    openPacks.delete(key);
    openPacks.set(key, open);
  }

  // Claimed before the pool is asked to shrink: an entry with no readers yet
  // is an entry the cap is free to evict and close, and this one is about to
  // be read. That is only reachable when everything older is busy, which is
  // exactly the case that made the pool worth having.
  open.readers += 1;

  if (reserved) {
    // Spent on the descriptor just opened, or given back if this caller found
    // the pack already pooled while it waited.
    reservedSlots -= 1;
    if (!opening) offerSlot();
  }

  try {
    return await use(await open.file);
  } finally {
    open.readers -= 1;
    if (open.retired && open.readers === 0) void closePack(open);
    // Idle again: whatever this entry was holding, someone may be waiting for
    // a slot the cap can now give them — and a waiter needs room for one
    // more, not merely a pool trimmed back to full.
    // Only for a waiter: admission already keeps the pool inside its cap, so
    // trimming *to* the cap would never evict anything. Making room is the
    // only trim that means something here.
    if (packSlot.length > 0) retirePacks(OPEN_PACKS - 1);
    offerSlot();
  }
};

/**
 * Retire one pack, for when the file itself is going away.
 *
 * A descriptor held against an unlinked file keeps its blocks allocated, so a
 * `gc` that repacks a gigabyte would free nothing until the process exited.
 *
 * This waits for the descriptor to be closed, not merely marked: a read in
 * flight defers the close, and callers use this to make the file closable
 * before they unlink it — which on Windows is the difference between a
 * removed pack and a failed `gc`.
 */
const retirePack = async (packPath: string): Promise<void> => {
  const key = packKey(packPath);
  // Descriptors already on their way out count, and there can be more than
  // one: gone from the map is not gone from the process, and a pack the cap
  // evicted can have been re-pooled since by a read that arrived after.
  const settling = [...closingPacks]
    .filter((record) => record.key === key)
    .map((record) => record.closed);

  const open = openPacks.get(key);
  if (open !== undefined) {
    openPacks.delete(key);
    open.retired = true;
    beginClosing(open);
    settling.push(open.readers === 0 ? closePack(open) : open.closed);
  }

  await Promise.all(settling);
};

/**
 * Retire every pack under a repository, for when the directory itself goes.
 *
 * `packs.delete` is the hook for a pack that is named; this is the one for a
 * recursive `fs.rm`, which unlinks packs without naming any of them. Without
 * it a dropped repository leaves the pool holding descriptors on files that
 * no longer have a name, and their blocks with them, for as long as the host
 * runs — the same leak `delete` avoids, arrived at from the other direction.
 */
/** Whether a pooled key names nothing on disk — `ENOENT` and nothing weaker. */
const gone = (key: string): boolean => {
  try {
    statSync(key);
    return false;
  } catch (error) {
    return error instanceof Error && "code" in error && error.code === "ENOENT";
  }
};

export const retirePacksUnder = async (root: string): Promise<void> => {
  // Two prefixes because `objects` can be a symlink out of the tree — an old
  // and still-supported way to put a repository's objects on another disk.
  // Canonicalizing only the root would then match none of its packs, and the
  // retire would quietly do nothing.
  const prefixes = [packPrefix(root), packPrefix(path.join(root, "objects", "pack"))];
  const keys = new Set([...openPacks.keys(), ...[...closingPacks].map((it) => it.key)]);
  await Promise.all(
    [...keys]
      // Or a key whose file is not there any more, whatever it is called. The
      // prefixes are reconstructed after the fact, and once the memo that
      // remembers what a path resolved to has evicted its entry, a symlink
      // removed on the way down cannot be resolved again — the prefix is then
      // a spelling no pooled key has, and the descriptor on a deleted pack
      // would be held for the life of the process. A key that no longer names
      // a file cannot be wanted by a later read, and `retirePack` still waits
      // for the readers it has.
      .filter((key) => prefixes.some((prefix) => key.startsWith(prefix)) || gone(key))
      .map((key) => retirePack(key)),
  );
};

/**
 * Remove something this process may be holding a pack open under.
 *
 * Three steps because two are not enough. The retire before makes the file
 * closable, which POSIX does not need and Windows does — it will not unlink a
 * file this process still has open. But a read in flight can re-pool the pack
 * between that retire and the remove, so the remove can still fail there;
 * hence the second attempt, and the retire after, which collects whatever was
 * re-pooled in the window on either platform.
 */
const retiringRemove = async (
  retire: () => Promise<void>,
  remove: () => Promise<void>,
): Promise<void> => {
  await retire();
  try {
    await remove();
  } catch {
    await retire();
    await remove();
  } finally {
    await retire();
  }
};

/** `retiringRemove` for a whole repository, for callers outside this file. */
export const retirePacksAndRemove = (root: string, remove: () => Promise<void>): Promise<void> =>
  retiringRemove(() => retirePacksUnder(root), remove);

/**
 * Retire pooled packs in these directories that a fresh listing no longer
 * names — the repack that happened somewhere else.
 *
 * `delete` covers a pack this process removes. Nothing covers `git repack -ad`
 * in another process, or another `chr33s-git gc`, which is a supported thing
 * to do to a repository this host is serving: the listing is re-read when the
 * directory's mtime changes, but the pool would go on holding the old pack's
 * descriptor, and the space the repack was asked to reclaim would never come
 * back. The LRU is no answer — it only turns over when a new pack is opened,
 * which a host with a handful of repositories never reaches.
 */
const retireVanished = async (
  directories: ReadonlyArray<string>,
  live: ReadonlyMap<string, { readonly dev: number; readonly ino: number }>,
): Promise<void> => {
  const prefixes = directories.map((directory) => packPrefix(directory));
  const mine = [...openPacks.keys()].filter((key) =>
    prefixes.some((prefix) => key.startsWith(prefix)),
  );

  const stale: string[] = [];
  for (const key of mine) {
    const current = live.get(key);
    if (current === undefined) {
      stale.push(key);
      continue;
    }
    // Same name, different file. A pack restored from a backup, or moved into
    // place by anything that is not this process, leaves the descriptor on an
    // unlinked inode while the freshly read `.idx` describes the new one —
    // offsets into the wrong bytes. Open-per-read could not get this wrong,
    // so the pool has to check.
    const inode = await openPacks.get(key)?.inode;
    if (inode != null && (inode.dev !== current.dev || inode.ino !== current.ino)) stale.push(key);
  }

  await Promise.all(stale.map((key) => retirePack(key)));
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
      const live = new Map<string, { readonly dev: number; readonly ino: number }>();
      for (const { directory: packDirectory, name } of found) {
        const packPath = path.join(packDirectory, `${name}.pack`);
        // An `.idx` without its `.pack` is a half-finished write, not a
        // pack; skipping it beats failing every read in the repository.
        // Counting it as live would also keep a descriptor on a pack that has
        // already been unlinked out from under a leftover index.
        if (!existsSync(packPath)) continue;

        const index = new Uint8Array(await fs.readFile(path.join(packDirectory, `${name}.idx`)));
        const stat = await fs.stat(packPath);
        const size = stat.size;
        // Canonicalized once, here, and handed to every read of this pack:
        // `realpath` is a blocking syscall and a listing is a far better
        // place for it than the read path the pool exists to keep cheap.
        const canonical = packKey(packPath);
        live.set(canonical, { dev: stat.dev, ino: stat.ino });
        handles.push({
          name,
          index,
          source: {
            size,
            read: async (offset, length) =>
              await withPack(canonical, async (file) => {
                const buffer = Buffer.alloc(Math.max(0, Math.min(length, size - offset)));
                await file.read(buffer, 0, buffer.length, offset);
                return new Uint8Array(buffer);
              }),
          },
        });
      }
      // This listing is the only moment the process learns that a pack it may
      // be holding open is gone — a repack in another process leaves no other
      // trace here.
      await retireVanished(directories, live);

      return handles;
    })();

  return {
    list: handlesIn([directory]),
    // Node has zlib, and reading a pack is where a repository spends its
    // time; `Inflate.ts` is the browser's decoder, not this one's.
    inflate: nativeInflate,

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
          // The pack first, because of how each half fails. A listing is
          // built from `.idx` names, so a `.pack` left behind by a removal
          // that could not finish — Windows, a pack still open elsewhere — is
          // a file nothing can name again, and its space never comes back:
          // the whole pack, stranded. The other way round strands an `.idx`,
          // which is the small half and the harmless one — every listing
          // skips it, since this writer lands the pack first and an index
          // without one indexes nothing.
          //
          // Neither is collected on its own. `gc` deletes the packs a repack
          // supersedes, which it learns from `packs.list`, and a stray `.idx`
          // is not in that list any more than a stray `.pack` is. What this
          // ordering buys is which file is left, not that it is cleaned up.
          const packPath = path.join(directory, `${name}.pack`);
          await retiringRemove(
            () => retirePack(packPath),
            () => fs.rm(packPath, { force: true }),
          );
          // Forgiven, deliberately: with the pack gone this index names
          // nothing, every listing skips it, and failing here would fail a
          // `gc` that has already written its new pack — leaving the objects
          // it superseded behind for the sake of a file that costs kilobytes.
          await fs.rm(path.join(directory, `${name}.idx`), { force: true }).catch(() => undefined);
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
          Effect.map((value) => (value !== null && isOid(value) ? value : null)),
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
                if (!addressable(name)) return [];
                const target = path.join(root, "logs", name);
                if (!existsSync(target)) return [];
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
              if (!existsSync(base)) return [];

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
