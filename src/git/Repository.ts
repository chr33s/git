/**
 * Repository operations.
 *
 * Every failure is in the type — a signature says that `commit` can lose a
 * race — ref updates go through one compare-and-swap path, and the hooks run
 * inside the same pipeline as the ref update instead of beside it.
 *
 * Storage reads go through this service rather than the stores directly — that
 * is what keeps `ObjectStore`/`RefStore` out of the HTTP handlers' requirements
 * later on. See the readme's "Do not reach past the domain".
 */
import { Context, Effect, Layer, Option, Result, Schedule, Stream } from "effect";
import {
  type HookRejected,
  Invalid,
  ObjectNotFound,
  type PackCorrupt,
  RefConflict,
  type StorageFailure,
} from "./Error.ts";
import {
  type CommitInfo,
  EMPTY_TREE_OID,
  encodeCommit,
  encodeTag,
  encodeTree,
  bytesToHex,
  hashObject,
  parseCommit,
  parseTag,
  parseTree,
  type Signature,
  type TagInfo,
  type TreeEntry,
} from "./Format.ts";
import { mergeTrees, type Strategy as MergeStrategy } from "./Merge.ts";
import * as Pack from "./Pack.ts";
import { PackStore } from "./Packed.ts";
import { buildPackIndex } from "./PackIndex.ts";
import {
  isOid,
  ObjectStore,
  type Oid,
  type RawObject,
  type ReflogEntry,
  RefStore,
  type RefUpdate,
} from "./Store.ts";

export interface Commit extends CommitInfo {
  readonly oid: Oid;
}

export interface ReceiveResult {
  readonly ref: string;
  readonly from: Oid | null;
  readonly to: Oid | null;
  readonly ok: boolean;
  readonly reason?: string;
}

/** One path's worth of change against a tree. */
export interface FileChange {
  /** Slash-separated, relative to the tree root. */
  readonly path: string;
  /** `null` removes the path. */
  readonly content: Uint8Array | null;
  /** git file mode; `100644` unless said otherwise. */
  readonly mode?: string;
}

/** The modes a blob may carry. Directories are written by `writeFiles` itself. */
const FILE_MODES = new Set(["100644", "100755", "120000", "160000"]);
const TREE_MODE = "40000";

/** What upload-pack asks for, shallow options included. */
export interface FetchRequest {
  readonly wants: ReadonlyArray<Oid>;
  readonly haves: ReadonlyArray<Oid>;
  /** Boundaries the client already records; their parents are absent there. */
  readonly clientShallow?: ReadonlyArray<Oid>;
  /** `deepen <n>`: commits this far from a want keep their parents hidden. */
  readonly depth?: number;
  /** `deepen-since`: nothing committed before this. */
  readonly since?: Date;
  /** `deepen-not <ref>`: nothing reachable from these. */
  readonly notRefs?: ReadonlyArray<string>;
}

export interface FsckProblem {
  readonly oid: Oid;
  readonly problem: string;
}

export interface FsckReport {
  readonly checked: number;
  readonly problems: ReadonlyArray<FsckProblem>;
  /** Refs pointing at objects the store does not hold. */
  readonly danglingRefs: ReadonlyArray<{ readonly ref: string; readonly oid: Oid }>;
}

export interface MergeConflict {
  readonly path: string;
  /** Why it could not be resolved, in the vocabulary git uses for it. */
  readonly reason: "content" | "add/add" | "modify/delete" | "binary";
}

export interface MergeOutcome {
  readonly kind: "up-to-date" | "fast-forward" | "merged" | "conflicted";
  /** The commit the merge produced, or `null` when it conflicted. */
  readonly commit: Oid | null;
  readonly tree: Oid | null;
  readonly base: Oid | null;
  readonly conflicts: ReadonlyArray<MergeConflict>;
}

export interface TreeFile {
  readonly path: string;
  readonly mode: string;
  readonly oid: Oid;
}

export interface GcReport {
  readonly scanned: number;
  readonly reachable: number;
  readonly removed: ReadonlyArray<Oid>;
  /** The pack a repack wrote, and how many objects went into it. */
  readonly packed?: { readonly name: string; readonly objects: number };
}

export interface FetchPlan {
  /** New boundaries the client must record. */
  readonly shallow: ReadonlyArray<Oid>;
  /** Boundaries it may forget, because their parents are in this pack. */
  readonly unshallow: ReadonlyArray<Oid>;
  readonly oids: ReadonlyArray<Oid>;
}

/**
 * The tree an object names: a tree outright, a commit's tree, or a tag peeled
 * to one.
 *
 * A module-level helper rather than a 36th service method: the revision half
 * stays with the caller — the CLI disambiguates short names, the API resolves
 * refs — but what an oid *means* as a tree is one question, and both edges
 * were answering it with their own copy.
 */
export const treeAt = (
  repository: Repository["Service"],
  oid: Oid,
): Effect.Effect<Oid, ObjectNotFound | StorageFailure> =>
  Effect.gen(function* () {
    const object = yield* repository.readObject(oid);
    if (object.type === "tree") return oid;
    if (object.type === "tag") {
      return (yield* repository.readCommit((yield* repository.readTag(oid)).object)).tree;
    }
    return (yield* repository.readCommit(oid)).tree;
  });

export class Repository extends Context.Service<
  Repository,
  {
    readonly refs: Effect.Effect<ReadonlyArray<readonly [string, Oid]>, StorageFailure>;
    readonly resolve: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    readonly head: Effect.Effect<string, StorageFailure>;
    /** Point HEAD at a ref — what a checkout does last. */
    readonly setHead: (ref: string) => Effect.Effect<void, StorageFailure>;

    readonly readCommit: (oid: Oid) => Effect.Effect<CommitInfo, ObjectNotFound | StorageFailure>;
    readonly readTree: (
      oid: Oid,
    ) => Effect.Effect<ReadonlyArray<TreeEntry>, ObjectNotFound | StorageFailure>;
    readonly readBlob: (oid: Oid) => Effect.Effect<Uint8Array, ObjectNotFound | StorageFailure>;
    /** Any object, type included — what a caller holding only an oid needs. */
    readonly readObject: (oid: Oid) => Effect.Effect<RawObject, ObjectNotFound | StorageFailure>;

    readonly writeTree: (entries: ReadonlyArray<TreeEntry>) => Effect.Effect<Oid, StorageFailure>;
    readonly writeBlob: (data: Uint8Array) => Effect.Effect<Oid, StorageFailure>;

    /**
     * A tree with `changes` applied to `base`, written bottom-up.
     *
     * The unit a caller actually has is a path, not a tree object — this is
     * what lets an API create content without a staging area, and only the
     * directories on a changed path are read.
     */
    readonly writeFiles: (input: {
      /** Absent starts from the empty tree. */
      readonly base?: Oid;
      readonly changes: ReadonlyArray<FileChange>;
    }) => Effect.Effect<Oid, ObjectNotFound | StorageFailure | Invalid>;

    /**
     * Nested trees from a flat list of paths whose blobs already exist.
     *
     * `writeFiles` is the same shape for content a caller is holding; this is
     * for content already in the store — an index, where every entry is a
     * path and an oid and re-reading the bytes to write them back would be
     * the only cost.
     */
    readonly writePaths: (
      entries: ReadonlyArray<{
        readonly path: string;
        readonly oid: Oid;
        readonly mode: string;
      }>,
    ) => Effect.Effect<Oid, StorageFailure | Invalid>;

    readonly commit: (input: {
      readonly branch: string;
      readonly tree: Oid;
      readonly message: string;
      readonly author: Signature;
      readonly committer?: Signature;
      /** `undefined` = whatever the branch is now; `null` = must not exist. */
      readonly expected?: Oid | null;
    }) => Effect.Effect<Oid, RefConflict | ObjectNotFound | StorageFailure | Invalid>;

    /**
     * History from `from`, newest first, as `git log` orders it: every
     * parent followed, each commit emitted once.
     *
     * Commits that share a committer date and sit on different branches have
     * no right order — git falls back to its own queue discipline, and this
     * falls back to the oid, so the output is at least stable run to run.
     * Where dates differ, which is every real repository, the two agree.
     *
     * `firstParent` follows only each commit's first parent — `git log
     * --first-parent`. It is the right walk for replaying a branch onto
     * another, and the wrong one for anything that must not miss a commit
     * that arrived by a merge.
     */
    readonly log: (
      from: Oid,
      options?: { readonly limit?: number; readonly firstParent?: boolean },
    ) => Stream.Stream<Commit, ObjectNotFound | StorageFailure>;

    readonly branch: (input: {
      readonly name: string;
      readonly base: string;
    }) => Effect.Effect<Oid, RefConflict | Invalid | StorageFailure>;

    /** receive-pack's ref phase: hooks, then one all-or-nothing ref update. */
    readonly receive: (
      updates: ReadonlyArray<RefUpdate>,
      options?: { readonly atomic?: boolean },
    ) => Effect.Effect<ReadonlyArray<ReceiveResult>, HookRejected | StorageFailure | Invalid>;

    readonly contains: (oid: Oid) => Effect.Effect<boolean, StorageFailure>;

    /** receive-pack's object phase: ingest a packfile into the store. */
    readonly unpack: <E>(
      pack: Stream.Stream<Uint8Array, E>,
    ) => Effect.Effect<ReadonlyArray<Oid>, PackCorrupt | ObjectNotFound | StorageFailure>;

    /**
     * upload-pack's answer: a pack of everything reachable from `wants` that
     * is not reachable from `haves`. Lazy — objects are read and deflated as
     * the consumer pulls.
     */
    readonly packOf: (
      wants: ReadonlyArray<Oid>,
      haves: ReadonlyArray<Oid>,
    ) => Stream.Stream<Uint8Array, ObjectNotFound | StorageFailure>;

    /**
     * The same answer with shallow options honoured, and the boundary lines
     * the client needs before the pack. One walk produces both.
     */
    readonly fetch: (
      request: FetchRequest,
    ) => Effect.Effect<FetchPlan, ObjectNotFound | StorageFailure>;

    /** A pack of exactly these objects, in this order. */
    readonly packOids: (
      oids: ReadonlyArray<Oid>,
    ) => Stream.Stream<Uint8Array, ObjectNotFound | StorageFailure>;

    readonly readTag: (oid: Oid) => Effect.Effect<TagInfo, ObjectNotFound | StorageFailure>;

    /**
     * A tag ref, annotated when a message is given and lightweight otherwise —
     * the same distinction `git tag` makes, and the reason the return says
     * which object the ref points at as well as what it names.
     */
    readonly tag: (input: {
      readonly name: string;
      /** Anything resolvable: a ref, or an oid. */
      readonly target: string;
      readonly message?: string;
      readonly tagger?: Signature;
      /** Move a tag that already exists. */
      readonly force?: boolean;
    }) => Effect.Effect<
      { readonly ref: string; readonly oid: Oid; readonly target: Oid },
      RefConflict | ObjectNotFound | Invalid | StorageFailure
    >;

    readonly deleteTag: (name: string) => Effect.Effect<boolean, StorageFailure | Invalid>;

    /** `false` when the ref was not there to begin with. */
    readonly deleteRef: (name: string) => Effect.Effect<boolean, StorageFailure | Invalid>;

    /**
     * Point a ref at a commit. `expected` turns it into a compare-and-swap,
     * which is the difference between moving a branch and losing someone
     * else's push.
     */
    readonly setRef: (input: {
      readonly name: string;
      /** A ref or an oid. */
      readonly to: string;
      readonly expected?: Oid | null;
    }) => Effect.Effect<
      { readonly ref: string; readonly oid: Oid; readonly previous: Oid | null },
      RefConflict | ObjectNotFound | Invalid | StorageFailure
    >;

    /**
     * Every object read back and checked against its own name.
     *
     * The storage contract proves the store keeps what it was given; this
     * proves what it kept is still a git object — a different question, and
     * the only one that catches corruption underneath the port.
     */
    readonly fsck: Effect.Effect<FsckReport, StorageFailure>;

    /**
     * Delete what no ref can reach.
     *
     * Safe without a grace period only because every host serializes requests
     * per repository — the Durable Object input gate, a mutex on node — so a
     * collection cannot interleave with the window in a push between writing
     * objects and moving the ref that makes them reachable. A host that
     * dropped that property would need one.
     *
     * Fails rather than collects when the reachability walk cannot finish: a
     * partial answer would name live objects as garbage, and `fsck` is where
     * a repository in that state gets diagnosed.
     */
    readonly gc: (options?: {
      readonly dryRun?: boolean;
      /**
       * Also write everything reachable into one pack and drop the loose
       * copies. This is what turns a repository from one stored entry per
       * object into a handful — the difference between a filesystem (or an
       * R2 bucket) holding a million keys and holding three.
       */
      readonly repack?: boolean;
    }) => Effect.Effect<GcReport, ObjectNotFound | StorageFailure>;

    /**
     * The best common ancestor of two commits — the base a three-way merge
     * is "three-way" about. `null` when the histories are unrelated.
     */
    readonly mergeBase: (
      left: Oid,
      right: Oid,
    ) => Effect.Effect<Oid | null, ObjectNotFound | StorageFailure>;

    /** Whether `descendant` can reach `ancestor`; a fast-forward is this. */
    readonly isAncestor: (
      ancestor: Oid,
      descendant: Oid,
    ) => Effect.Effect<boolean, ObjectNotFound | StorageFailure>;

    /** Every path under a tree, depth-first, with the blob each names. */
    readonly listFiles: (
      tree: Oid,
      options?: { readonly prefix?: string },
    ) => Effect.Effect<ReadonlyArray<TreeFile>, ObjectNotFound | StorageFailure>;

    /** One path's entry, or `null` when the tree has no such path. */
    readonly findPath: (
      tree: Oid,
      path: string,
    ) => Effect.Effect<TreeEntry | null, ObjectNotFound | StorageFailure>;

    /** The reflog for a ref, newest last, as the stores recorded it. */
    readonly reflog: (name: string) => Effect.Effect<ReadonlyArray<ReflogEntry>, StorageFailure>;

    /** A commit object with any number of parents — a merge has two. */
    readonly commitTree: (input: {
      readonly tree: Oid;
      readonly parents: ReadonlyArray<Oid>;
      readonly message: string;
      readonly author: Signature;
      readonly committer?: Signature;
    }) => Effect.Effect<Oid, StorageFailure>;

    /**
     * Three-way merge of two commits.
     *
     * Reports rather than throws: a conflict is an outcome a caller acts on,
     * not a failure. When `into` is given and the merge succeeds, that ref is
     * moved with a compare-and-swap, so a merge that raced another push loses
     * cleanly instead of overwriting it.
     */
    readonly merge: (input: {
      /** Refs or oids. */
      readonly ours: string;
      readonly theirs: string;
      readonly author: Signature;
      readonly message?: string;
      readonly strategy?: MergeStrategy;
      /** The ref to move on success; absent computes the merge and stops. */
      readonly into?: string;
      /** A fast-forward is the default when history allows one. */
      readonly noFastForward?: boolean;
    }) => Effect.Effect<MergeOutcome, RefConflict | ObjectNotFound | Invalid | StorageFailure>;
  }
>()("git/Repository") {}

/** Hooks as a service, rather than a mutable registry on the server instance. */
export class Hooks extends Context.Service<
  Hooks,
  {
    readonly preReceive: (updates: ReadonlyArray<RefUpdate>) => Effect.Effect<void, HookRejected>;
    readonly update: (update: RefUpdate) => Effect.Effect<void, HookRejected>;
    readonly postReceive: (results: ReadonlyArray<ReceiveResult>) => Effect.Effect<void>;
  }
>()("git/Hooks") {}

/** No-op hooks, which is what a server without policy wants. */
export const hooksNoop = Layer.succeed(Hooks, {
  preReceive: () => Effect.void,
  update: () => Effect.void,
  postReceive: () => Effect.void,
});

export const layer = Layer.effect(
  Repository,
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const refs = yield* RefStore;
    const hooks = yield* Hooks;
    const packs = yield* PackStore;

    const readTyped = <A>(
      oid: Oid,
      type: "commit" | "tree" | "tag",
      parse: (data: Uint8Array) => Result.Result<A, Invalid>,
    ) =>
      objects.read(oid).pipe(
        Effect.flatMap((object) =>
          object.type === type
            ? Effect.fromResult(parse(object.data)).pipe(
                // A malformed object is indistinguishable from a missing one as
                // far as a caller can act on it.
                Effect.mapError(() => new ObjectNotFound({ oid })),
              )
            : Effect.fail(new ObjectNotFound({ oid })),
        ),
      );

    const readCommit = (oid: Oid) => readTyped(oid, "commit", parseCommit);

    /** An annotated tag's target: the `object <oid>` header line. */
    const tagTarget = (data: Uint8Array): Oid | null => {
      const line = new TextDecoder().decode(data.subarray(0, 47));
      const target = line.startsWith("object ") ? line.slice(7, 47) : "";
      return isOid(target) ? target : null;
    };

    /**
     * Everything reachable from `roots`: commits pull in their tree and
     * parents, trees their entries (gitlinks excepted — those live in another
     * repository), tags their target. The empty tree is git's one virtual
     * object; a commit may reference it without any store holding it.
     */
    const walk = (
      roots: ReadonlyArray<Oid>,
      options: {
        readonly ignoreMissing: boolean;
        readonly skip?: ReadonlySet<Oid>;
        /**
         * Commits whose parents are not followed. A shallow client's history
         * stops at these, so walking past them would mark objects as "already
         * had" that the client has never seen.
         */
        readonly boundary?: ReadonlySet<Oid>;
      },
    ) =>
      Effect.gen(function* () {
        const seen = new Set<Oid>();
        const order: Oid[] = [];
        const stack = [...roots];

        while (stack.length > 0) {
          const oid = stack.pop()!;
          if (seen.has(oid) || options.skip?.has(oid)) continue;
          seen.add(oid);

          const tolerant = options.ignoreMissing || oid === EMPTY_TREE_OID;
          const object = yield* objects.read(oid).pipe(
            Effect.map((value): RawObject | null => value),
            Effect.catchTag("ObjectNotFound", (error) =>
              tolerant ? Effect.succeed(null) : Effect.fail(error),
            ),
          );
          if (object === null) continue;
          order.push(oid);

          switch (object.type) {
            case "commit": {
              const commit = yield* Effect.fromResult(parseCommit(object.data)).pipe(
                Effect.mapError(() => new ObjectNotFound({ oid })),
              );
              stack.push(commit.tree);
              if (options.boundary?.has(oid) !== true) stack.push(...commit.parents);
              break;
            }
            case "tree": {
              const entries = yield* Effect.fromResult(parseTree(object.data)).pipe(
                Effect.mapError(() => new ObjectNotFound({ oid })),
              );
              for (const entry of entries) if (entry.mode !== "160000") stack.push(entry.oid);
              break;
            }
            case "tag": {
              const target = tagTarget(object.data);
              if (target !== null) stack.push(target);
              break;
            }
            case "blob":
              break;
          }
        }

        return { order, seen };
      });

    const readTreeEntries = (oid: Oid) =>
      oid === EMPTY_TREE_OID
        ? Effect.succeed<ReadonlyArray<TreeEntry>>([])
        : readTyped(oid, "tree", parseTree);

    const writeTree = (entries: ReadonlyArray<TreeEntry>) =>
      objects.write({ type: "tree", data: encodeTree(entries) });

    /** `a/b/c.txt` -> `["a","b","c.txt"]`, or a failure naming the path. */
    const segmentsOf = (path: string) =>
      Effect.suspend(() => {
        const segments = path.split("/").filter((segment) => segment !== "");
        if (segments.length === 0) {
          return Effect.fail(new Invalid({ field: "path", reason: `empty path '${path}'` }));
        }
        if (segments.some((segment) => segment === "." || segment === "..")) {
          return Effect.fail(
            new Invalid({ field: "path", reason: `path escapes root: '${path}'` }),
          );
        }
        return Effect.succeed(segments);
      });

    /**
     * The write is bottom-up because a tree names its children by oid: a
     * directory cannot be written until every directory inside it has been.
     * Only directories on a changed path are ever read.
     */
    const writeFiles = Effect.fn("Repository.writeFiles")(function* (input: {
      readonly base?: Oid;
      readonly changes: ReadonlyArray<FileChange>;
    }) {
      /** Keyed by directory prefix; `""` is the root. */
      const directories = new Map<string, Map<string, TreeEntry>>();

      const load = (prefix: string, oid: Oid | null) =>
        Effect.gen(function* () {
          const cached = directories.get(prefix);
          if (cached !== undefined) return cached;

          const entries = new Map<string, TreeEntry>();
          if (oid !== null) {
            for (const entry of yield* readTreeEntries(oid)) entries.set(entry.name, entry);
          }
          directories.set(prefix, entries);
          return entries;
        });

      yield* load("", input.base ?? null);

      /** Walk to a directory, reading (or inventing) each level on the way. */
      const directoryFor = (segments: ReadonlyArray<string>) =>
        Effect.gen(function* () {
          let prefix = "";
          let current = directories.get("")!;
          for (const segment of segments) {
            const next = prefix === "" ? segment : `${prefix}/${segment}`;
            if (!directories.has(next)) {
              // A path may replace a file with a directory; then there is no
              // tree to read and the new directory starts empty.
              const entry = current.get(segment);
              yield* load(next, entry?.mode === TREE_MODE ? entry.oid : null);
            }
            current = directories.get(next)!;
            prefix = next;
          }
          return current;
        });

      for (const change of input.changes) {
        const segments = yield* segmentsOf(change.path);
        const name = segments.at(-1)!;
        const parent = yield* directoryFor(segments.slice(0, -1));

        if (change.content === null) {
          parent.delete(name);
          continue;
        }

        const mode = change.mode ?? "100644";
        if (!FILE_MODES.has(mode)) {
          return yield* new Invalid({ field: "mode", reason: `unsupported file mode '${mode}'` });
        }
        const oid = yield* objects.write({ type: "blob", data: change.content });
        parent.set(name, { mode, name, oid });
      }

      const depth = (prefix: string) => (prefix === "" ? 0 : prefix.split("/").length);
      const prefixes = [...directories.keys()].sort((left, right) => depth(right) - depth(left));

      let root = EMPTY_TREE_OID;
      for (const prefix of prefixes) {
        const entries = [...directories.get(prefix)!.values()];

        if (prefix === "") {
          root = yield* writeTree(entries);
          continue;
        }

        const slash = prefix.lastIndexOf("/");
        const parent = directories.get(slash === -1 ? "" : prefix.slice(0, slash))!;
        const name = slash === -1 ? prefix : prefix.slice(slash + 1);

        // git has no empty directories: one whose last entry went away is
        // dropped from its parent rather than written.
        if (entries.length === 0) parent.delete(name);
        else parent.set(name, { mode: TREE_MODE, name, oid: yield* writeTree(entries) });
      }

      return root;
    });

    const readBlobAt = (oid: Oid) =>
      objects
        .read(oid)
        .pipe(
          Effect.flatMap((object) =>
            object.type === "blob"
              ? Effect.succeed(object.data)
              : Effect.fail(new ObjectNotFound({ oid })),
          ),
        );

    const listFilesOf = Effect.fn("Repository.listFiles")(function* (
      tree: Oid,
      options?: { readonly prefix?: string },
    ) {
      const files: TreeFile[] = [];
      // Depth-first with an explicit stack rather than recursion: a deep tree
      // is data, and data should not size the call stack.
      const stack: Array<{ oid: Oid; prefix: string }> = [
        { oid: tree, prefix: options?.prefix ?? "" },
      ];

      while (stack.length > 0) {
        const { oid, prefix } = stack.pop()!;
        for (const entry of yield* readTreeEntries(oid)) {
          const path = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
          if (entry.mode === TREE_MODE) stack.push({ oid: entry.oid, prefix: path });
          else files.push({ path, mode: entry.mode, oid: entry.oid });
        }
      }

      return files.sort((left, right) => left.path.localeCompare(right.path));
    });

    /** Every commit reachable from `roots`, the commit graph only. */
    const ancestry = (roots: ReadonlyArray<Oid>) =>
      Effect.gen(function* () {
        const seen = new Set<Oid>();
        const stack = [...roots];
        while (stack.length > 0) {
          const oid = stack.pop()!;
          if (seen.has(oid)) continue;
          seen.add(oid);
          const commit = yield* readCommit(oid).pipe(
            Effect.map((value): CommitInfo | null => value),
            // A history that runs into a missing commit is a shallow clone's
            // normal shape, not a failure to walk.
            Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
          );
          if (commit !== null) stack.push(...commit.parents);
        }
        return seen;
      });

    /**
     * Candidates are common ancestors that no other common ancestor can
     * reach — without that filter every shared commit back to the root
     * qualifies, and the "base" of a three-way merge would be the wrong one.
     */
    const mergeBase = Effect.fn("Repository.mergeBase")(function* (left: Oid, right: Oid) {
      if (left === right) return left;

      const leftSide = yield* ancestry([left]);
      if (leftSide.has(right)) return right;
      const rightSide = yield* ancestry([right]);
      if (rightSide.has(left)) return left;

      const shared = [...rightSide].filter((oid) => leftSide.has(oid));
      if (shared.length === 0) return null;

      const candidates = new Set(shared);
      for (const oid of shared) {
        const commit = yield* readCommit(oid).pipe(
          Effect.map((value): CommitInfo | null => value),
          Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
        );
        if (commit === null) continue;
        // Anything strictly behind another candidate is not the best one.
        for (const older of yield* ancestry(commit.parents)) candidates.delete(older);
      }

      return candidates.values().next().value ?? shared[0] ?? null;
    });

    /**
     * Everything reachable into one pack, and the loose copies dropped.
     *
     * Order is the whole safety argument: the pack and its index are written
     * and verified readable *before* anything is deleted, so an interruption
     * leaves the repository with both copies rather than neither. `packed`
     * prefers loose objects for the same reason.
     *
     * The pack is built in memory. That is the honest cost of writing an
     * `.idx`, which cannot be finished until every offset is known — and
     * repacking is a maintenance call a host schedules, not something on the
     * request path.
     */
    const repack = Effect.fn("Repository.repack")(function* (reachable: ReadonlySet<Oid>) {
      const oids = [...reachable].filter((oid) => oid !== EMPTY_TREE_OID);
      if (oids.length === 0) return null;

      const entries: Pack.PackedEntry[] = [];
      const chunks = yield* Stream.runCollect(
        Pack.pack(oids, { onObject: (entry) => entries.push(entry) }).pipe(
          Stream.provideService(ObjectStore, objects),
        ),
      );

      const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
      const bytes = new Uint8Array(total);
      let at = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, at);
        at += chunk.length;
      }

      // The trailer is the pack's own name, which is how git names the pair.
      const checksum = bytes.subarray(total - 20);
      const name = `pack-${bytesToHex(checksum)}`;
      const index = buildPackIndex(entries, checksum);

      yield* packs.write({ name, pack: bytes, index });

      // Only now: the objects are in the pack and the pack is stored.
      yield* Effect.forEach(oids, (oid) => objects.delete(oid), { discard: true });

      return { name, objects: oids.length };
    });

    const writePaths = Effect.fn("Repository.writePaths")(function* (
      entries: ReadonlyArray<{ readonly path: string; readonly oid: Oid; readonly mode: string }>,
    ) {
      /** Directory prefix -> its entries, filled in as paths are placed. */
      const directories = new Map<string, Map<string, TreeEntry>>([["", new Map()]]);

      const directoryAt = (prefix: string) => {
        let existing = directories.get(prefix);
        if (existing !== undefined) return existing;
        existing = new Map();
        directories.set(prefix, existing);
        return existing;
      };

      for (const entry of entries) {
        const segments = entry.path.split("/").filter((segment) => segment !== "");
        const name = segments.at(-1);
        if (name === undefined) {
          return yield* new Invalid({ field: "path", reason: `empty path '${entry.path}'` });
        }
        if (!FILE_MODES.has(entry.mode)) {
          return yield* new Invalid({
            field: "mode",
            reason: `unsupported file mode '${entry.mode}'`,
          });
        }
        directoryAt(segments.slice(0, -1).join("/")).set(name, {
          mode: entry.mode,
          name,
          oid: entry.oid,
        });
      }

      const depth = (prefix: string) => (prefix === "" ? 0 : prefix.split("/").length);
      const prefixes = [...directories.keys()].sort((left, right) => depth(right) - depth(left));

      let root = EMPTY_TREE_OID;
      for (const prefix of prefixes) {
        const contents = [...directories.get(prefix)!.values()];
        if (prefix === "") {
          root = yield* writeTree(contents);
          continue;
        }
        const slash = prefix.lastIndexOf("/");
        const name = slash === -1 ? prefix : prefix.slice(slash + 1);
        const parent = directoryAt(slash === -1 ? "" : prefix.slice(0, slash));
        if (contents.length === 0) parent.delete(name);
        else parent.set(name, { mode: TREE_MODE, name, oid: yield* writeTree(contents) });
      }

      return root;
    });

    const closure = (
      wants: ReadonlyArray<Oid>,
      haves: ReadonlyArray<Oid>,
      clientShallow?: ReadonlySet<Oid>,
    ) =>
      Effect.gen(function* () {
        // What the client has is walked tolerantly: a `have` can reference
        // history this repository never saw, and that is not an error.
        const excluded = (yield* walk(haves, {
          ignoreMissing: true,
          ...(clientShallow === undefined ? {} : { boundary: clientShallow }),
        })).seen;
        return (yield* walk(wants, { ignoreMissing: false, skip: excluded })).order;
      });

    /**
     * The objects a set of commits needs, without descending into what the
     * client already has.
     *
     * Blobs are never read: a tree entry's mode says whether it is a subtree,
     * so the only objects opened here are the trees that have to be walked.
     */
    const objectsOf = (commits: ReadonlyArray<Oid>, skip: ReadonlySet<Oid>) =>
      Effect.gen(function* () {
        const seen = new Set<Oid>(skip);
        const order: Oid[] = [];
        const take = (oid: Oid): boolean => {
          if (seen.has(oid)) return false;
          seen.add(oid);
          order.push(oid);
          return true;
        };

        for (const oid of commits) {
          if (!take(oid)) continue;
          const commit = yield* readCommit(oid);

          const stack: Oid[] = [commit.tree];
          while (stack.length > 0) {
            const treeOid = stack.pop()!;
            if (treeOid === EMPTY_TREE_OID || !take(treeOid)) continue;
            for (const entry of yield* readTreeEntries(treeOid)) {
              // A gitlink names a commit in another repository entirely.
              if (entry.mode === "160000") continue;
              if (entry.mode === "40000") stack.push(entry.oid);
              else take(entry.oid);
            }
          }
        }

        return order;
      });

    /**
     * Which commits a fetch may send, and where history is cut.
     *
     * A shallow request is a walk with a stop condition — depth, age, or
     * another ref's history — and the commits whose parents the stop condition
     * removed are exactly the new boundary the client must record. Doing it in
     * one pass matters: the boundary lines go out before the pack, so a second
     * walk would be the same work twice.
     */
    const fetchPlan = Effect.fn("Repository.fetch")(function* (input: FetchRequest) {
      const clientShallow = new Set(input.clientShallow ?? []);
      const deepening =
        input.depth !== undefined || input.since !== undefined || (input.notRefs?.length ?? 0) > 0;

      if (!deepening) {
        const order = yield* closure(input.wants, input.haves, clientShallow);
        return { shallow: [], unshallow: [], oids: order };
      }

      // `deepen-not <ref>`: everything reachable from those refs stays put.
      const blocked = new Set<Oid>();
      for (const name of input.notRefs ?? []) {
        const oid = yield* refs.resolve(name);
        if (oid === null) continue;
        for (const seen of (yield* walk([oid], { ignoreMissing: true })).seen) blocked.add(seen);
      }

      const since = input.since?.getTime();
      const depths = new Map<Oid, number>();
      const boundary = new Set<Oid>();
      const order: Oid[] = [];
      const queue: Array<{ readonly oid: Oid; readonly depth: number }> = input.wants.map(
        (oid) => ({
          oid,
          depth: 1,
        }),
      );

      while (queue.length > 0) {
        const { depth, oid } = queue.shift()!;
        if (blocked.has(oid)) continue;

        const already = depths.get(oid);
        // A shallower route to the same commit can extend history past it, so
        // it is re-examined; an equal or deeper one cannot.
        if (already !== undefined && already <= depth) continue;
        if (already === undefined) order.push(oid);
        depths.set(oid, depth);

        const commit = yield* readCommit(oid);
        const atLimit = input.depth !== undefined && depth >= input.depth;

        let cut = false;
        for (const parent of commit.parents) {
          if (atLimit || blocked.has(parent)) {
            cut = true;
            continue;
          }
          if (since !== undefined) {
            const older = (yield* readCommit(parent)).committer.at.getTime() < since;
            if (older) {
              cut = true;
              continue;
            }
          }
          queue.push({ oid: parent, depth: depth + 1 });
        }

        if (cut) boundary.add(oid);
        else boundary.delete(oid);
      }

      const excluded = (yield* walk(input.haves, {
        ignoreMissing: true,
        boundary: clientShallow,
      })).seen;

      return {
        // Only boundaries the client does not already record are news to it.
        shallow: [...boundary].filter((oid) => !clientShallow.has(oid)),
        // A commit the client had as a boundary and whose parents are in this
        // pack is no longer one.
        unshallow: [...clientShallow].filter((oid) => depths.has(oid) && !boundary.has(oid)),
        oids: yield* objectsOf(order, excluded),
      };
    });

    return Repository.of({
      refs: refs.list("refs/"),
      resolve: refs.resolve,
      head: refs.head,
      setHead: refs.setHead,

      readCommit,
      readTree: readTreeEntries,
      readObject: objects.read,
      readBlob: (oid) =>
        objects
          .read(oid)
          .pipe(
            Effect.flatMap((object) =>
              object.type === "blob"
                ? Effect.succeed(object.data)
                : Effect.fail(new ObjectNotFound({ oid })),
            ),
          ),

      writeTree,
      writePaths,
      writeBlob: (data) => objects.write({ type: "blob", data }),
      writeFiles,

      commit: Effect.fn("Repository.commit")(
        function* ({ author, branch, committer, expected, message, tree }) {
          const ref = branch.startsWith("refs/") ? branch : `refs/heads/${branch}`;
          const parent = yield* refs.read(ref);

          const oid = yield* objects.write({
            type: "commit",
            data: encodeCommit({
              tree,
              parents: parent === null ? [] : [parent],
              author,
              committer: committer ?? author,
              message,
            }),
          });

          const [result] = yield* refs.apply([
            {
              name: ref,
              value: oid,
              expected: expected === undefined ? parent : expected,
              reason: `commit: ${message.split("\n")[0]}`,
            },
          ]);

          if (result === undefined || !result.applied) {
            return yield* new RefConflict({
              ref,
              expected: expected === undefined ? parent : expected,
              actual: result?.current ?? null,
            });
          }

          return oid;
        },
        // Optimistic concurrency: a caller that did not pin `expected` is
        // saying "append to the branch", so a lost race is retried rather than
        // surfaced. A caller that did pin one gets the conflict.
        Effect.retry({
          while: (error) => error._tag === "RefConflict",
          times: 3,
          schedule: Schedule.exponential("10 millis"),
        }),
      ),

      log: (from, options) => {
        if (options?.firstParent === true) {
          return Stream.paginate(from, (oid) =>
            readCommit(oid).pipe(
              Effect.map(
                (commit) =>
                  [[{ ...commit, oid }], Option.fromNullishOr(commit.parents[0])] as const,
              ),
            ),
          ).pipe(options.limit === undefined ? (self) => self : Stream.take(options.limit));
        }

        /**
         * A frontier ordered by committer date, not a queue.
         *
         * Following every parent means the walk reaches the same commit by
         * several routes and reaches old commits on one side before newer
         * ones on the other. Emitting in arrival order would interleave the
         * two sides by shape of the graph rather than by time, which is not
         * what `git log` shows; taking the newest pending commit each step
         * is. `seen` is what keeps a commit reachable twice from being
         * reported twice.
         */
        return Stream.paginate(
          { frontier: [from] as ReadonlyArray<Oid>, seen: new Set<Oid>() },
          (state) =>
            Effect.gen(function* () {
              let frontier = state.frontier.filter((oid) => !state.seen.has(oid));
              if (frontier.length === 0) {
                return [[] as ReadonlyArray<Commit>, Option.none<typeof state>()] as const;
              }

              const commits = yield* Effect.forEach(frontier, (oid) =>
                readCommit(oid).pipe(Effect.map((commit) => ({ ...commit, oid }))),
              );

              const latest = Math.max(...commits.map((commit) => commit.committer.at.getTime()));
              const tied = commits.filter((commit) => commit.committer.at.getTime() === latest);

              /**
               * Date order alone would sometimes print a parent above its
               * child, which `git log` never does. It only can when the two
               * share a timestamp — a parent is otherwise older — and then
               * every commit between them shares it too, so the disagreement
               * can be resolved by walking just the commits at this instant.
               */
              const reachesWithinTie = Effect.fn("Repository.log.reaches")(function* (
                start: Oid,
                target: Oid,
              ) {
                const pending = [start];
                const visited = new Set<Oid>();
                while (pending.length > 0) {
                  const oid = pending.pop()!;
                  if (oid === target) return true;
                  if (visited.has(oid) || state.seen.has(oid)) continue;
                  visited.add(oid);
                  const commit = yield* readCommit(oid);
                  if (commit.committer.at.getTime() !== latest) continue;
                  pending.push(...commit.parents);
                }
                return false;
              });

              const eligible: Array<Commit> = [];
              for (const candidate of tied) {
                let shadowed = false;
                for (const other of tied) {
                  if (other.oid === candidate.oid) continue;
                  if (yield* reachesWithinTie(other.oid, candidate.oid)) {
                    shadowed = true;
                    break;
                  }
                }
                if (!shadowed) eligible.push(candidate);
              }

              // Oid decides only between commits that are genuinely unordered,
              // so the output is stable run to run rather than merely valid.
              const newest = (eligible.length > 0 ? eligible : tied).reduce((best, candidate) =>
                candidate.oid > best.oid ? candidate : best,
              );

              state.seen.add(newest.oid);
              frontier = [
                ...frontier.filter((oid) => oid !== newest.oid),
                ...newest.parents.filter((oid) => !state.seen.has(oid)),
              ];

              return [
                [newest] as ReadonlyArray<Commit>,
                Option.some({ frontier, seen: state.seen }),
              ] as const;
            }),
        ).pipe(options?.limit === undefined ? (self) => self : Stream.take(options.limit));
      },

      branch: Effect.fn("Repository.branch")(function* ({ base, name }) {
        const from = yield* refs.resolve(base);
        if (from === null) {
          return yield* new Invalid({ field: "base", reason: `unknown ref '${base}'` });
        }

        const ref = `refs/heads/${name}`;
        const [result] = yield* refs
          .apply([{ name: ref, value: from, expected: null, reason: `branch: from ${base}` }])
          .pipe(Effect.catchTag("Invalid", (error) => Effect.fail(error)));

        if (result === undefined || !result.applied) {
          return yield* new RefConflict({
            ref,
            expected: null,
            actual: result?.current ?? null,
          });
        }

        return from;
      }),

      receive: Effect.fn("Repository.receive")(function* (updates, options) {
        yield* hooks.preReceive(updates);
        // Per-ref checks are independent, so they run together; one rejection
        // interrupts the rest.
        yield* Effect.forEach(updates, hooks.update, { concurrency: "unbounded" });

        const applied = yield* refs.apply(updates, options);
        const results = applied.map((result, index): ReceiveResult => ({
          ref: result.name,
          from: updates[index]?.expected ?? null,
          to: result.current,
          ok: result.applied,
          ...(result.applied ? {} : { reason: "ref moved" }),
        }));

        yield* hooks.postReceive(results);
        return results;
      }),

      contains: objects.has,

      // Traced: ingesting a pack is the expensive half of a push, and the
      // span is where its cost shows up.
      unpack: Effect.fn("Repository.unpack")(function* (pack) {
        return yield* Pack.unpack(pack).pipe(Effect.provideService(ObjectStore, objects));
      }),

      packOf: (wants, haves) =>
        Stream.unwrap(
          // The walk is the expensive half of a fetch; the deflate that
          // follows is per-object and streams.
          Effect.withSpan("Repository.packOf")(closure(wants, haves)).pipe(
            Effect.map((oids) => Pack.pack(oids).pipe(Stream.provideService(ObjectStore, objects))),
          ),
        ),

      fetch: fetchPlan,
      packOids: (oids) => Pack.pack(oids).pipe(Stream.provideService(ObjectStore, objects)),

      readTag: (oid) => readTyped(oid, "tag", parseTag),

      tag: Effect.fn("Repository.tag")(function* ({ force, message, name, tagger, target }) {
        if (name === "" || name.includes(" ") || name.startsWith("refs/")) {
          return yield* new Invalid({ field: "name", reason: `bad tag name '${name}'` });
        }

        const resolved = isOid(target) ? target : yield* refs.resolve(target);
        if (resolved === null) {
          return yield* new Invalid({ field: "target", reason: `unknown ref '${target}'` });
        }
        const object = yield* objects.read(resolved);

        // An annotated tag is an object of its own; a lightweight one is the
        // ref alone, pointing straight at the target.
        const oid =
          message === undefined
            ? resolved
            : yield* objects.write({
                type: "tag",
                data: encodeTag({
                  object: resolved,
                  type: object.type,
                  tag: name,
                  ...(tagger === undefined ? {} : { tagger }),
                  message,
                }),
              });

        const ref = `refs/tags/${name}`;
        const [result] = yield* refs.apply([
          {
            name: ref,
            value: oid,
            // A tag is meant to be stable, so replacing one is opt-in.
            ...(force === true ? {} : { expected: null }),
            reason: `tag: ${name}`,
          },
        ]);

        if (result === undefined || !result.applied) {
          return yield* new RefConflict({
            ref,
            expected: null,
            actual: result?.current ?? null,
          });
        }

        return { ref, oid, target: resolved };
      }),

      deleteTag: (name) =>
        refs
          .apply([{ name: `refs/tags/${name}`, value: null, reason: "tag: delete" }])
          .pipe(Effect.map(([result]) => result?.applied === true)),

      deleteRef: (name) =>
        refs
          .apply([{ name, value: null, reason: "delete" }])
          .pipe(Effect.map(([result]) => result?.applied === true)),

      setRef: Effect.fn("Repository.setRef")(function* ({ expected, name, to }) {
        const target = isOid(to) ? to : yield* refs.resolve(to);
        if (target === null) {
          return yield* new Invalid({ field: "to", reason: `unknown ref '${to}'` });
        }
        // Refuse to point a ref at something that is not there: a dangling
        // ref is a repository nobody can clone.
        if (!(yield* objects.has(target))) return yield* new ObjectNotFound({ oid: target });

        const previous = yield* refs.read(name);
        const [result] = yield* refs.apply([
          {
            name,
            value: target,
            ...(expected === undefined ? {} : { expected }),
            reason: `set: ${to}`,
          },
        ]);

        if (result === undefined || !result.applied) {
          return yield* new RefConflict({
            ref: name,
            expected: expected ?? null,
            actual: result?.current ?? null,
          });
        }

        return { ref: name, oid: target, previous };
      }),

      fsck: Effect.gen(function* () {
        const problems: FsckProblem[] = [];
        let checked = 0;

        yield* Stream.runForEach(objects.list, (oid) =>
          Effect.gen(function* () {
            checked++;
            const object = yield* objects.read(oid).pipe(
              Effect.map((value): RawObject | null => value),
              Effect.catchTag("ObjectNotFound", () => {
                problems.push({ oid, problem: "listed but unreadable" });
                return Effect.succeed(null);
              }),
            );
            if (object === null) return;

            // The name is the hash of the content: if they disagree, the
            // bytes changed underneath the store.
            const actual = yield* hashObject(object);
            if (actual !== oid) {
              problems.push({ oid, problem: `hash mismatch: content hashes to ${actual}` });
              return;
            }

            // A blob is bytes and has no structure to be wrong about; the
            // other three do, and the codec is the checker.
            const structure: Result.Result<unknown, Invalid> | null =
              object.type === "commit"
                ? parseCommit(object.data)
                : object.type === "tree"
                  ? parseTree(object.data)
                  : object.type === "tag"
                    ? parseTag(object.data)
                    : null;
            if (structure !== null && Result.isFailure(structure)) {
              problems.push({
                oid,
                problem: `malformed ${object.type}: ${structure.failure.reason}`,
              });
            }
          }),
        );

        // A ref pointing at nothing is the other half of integrity, and the
        // half a per-object walk cannot see.
        const danglingRefs: Array<{ ref: string; oid: Oid }> = [];
        for (const [ref, oid] of yield* refs.list("refs/")) {
          if (oid !== EMPTY_TREE_OID && !(yield* objects.has(oid))) danglingRefs.push({ ref, oid });
        }

        return { checked, problems, danglingRefs };
      }).pipe(Effect.withSpan("Repository.fsck")),

      listFiles: listFilesOf,

      findPath: Effect.fn("Repository.findPath")(function* (tree, path) {
        const segments = path.split("/").filter((segment) => segment !== "");
        let current = tree;

        for (let index = 0; index < segments.length; index++) {
          const entries = yield* readTreeEntries(current);
          const entry = entries.find((candidate) => candidate.name === segments[index]);
          if (entry === undefined) return null;
          if (index === segments.length - 1) return entry;
          if (entry.mode !== TREE_MODE) return null;
          current = entry.oid;
        }

        return null;
      }),

      reflog: refs.reflog,

      commitTree: ({ author, committer, message, parents, tree }) =>
        objects.write({
          type: "commit",
          data: encodeCommit({ tree, parents, author, committer: committer ?? author, message }),
        }),

      merge: Effect.fn("Repository.merge")(function* (input) {
        const resolveCommit = (name: string) =>
          Effect.gen(function* () {
            const oid = isOid(name) ? name : yield* refs.resolve(name);
            if (oid === null) {
              return yield* new Invalid({ field: "ref", reason: `unknown ref '${name}'` });
            }
            return oid;
          });

        const ours = yield* resolveCommit(input.ours);
        const theirs = yield* resolveCommit(input.theirs);
        const strategy = input.strategy ?? "recursive";

        const settled = (kind: MergeOutcome["kind"], commit: Oid, tree: Oid, base: Oid | null) =>
          Effect.gen(function* () {
            if (input.into !== undefined && kind !== "up-to-date") {
              const expected = isOid(input.into) ? undefined : yield* refs.read(input.into);
              const [result] = yield* refs.apply([
                {
                  name: input.into,
                  value: commit,
                  ...(expected === undefined ? {} : { expected }),
                  reason: `merge: ${input.theirs}`,
                },
              ]);
              if (result === undefined || !result.applied) {
                return yield* new RefConflict({
                  ref: input.into,
                  expected: expected ?? null,
                  actual: result?.current ?? null,
                });
              }
            }
            return { kind, commit, tree, base, conflicts: [] } satisfies MergeOutcome;
          });

        const base = yield* mergeBase(ours, theirs);

        // Already contained: there is nothing of theirs we do not have.
        if (base === theirs) {
          return {
            kind: "up-to-date",
            commit: ours,
            tree: (yield* readCommit(ours)).tree,
            base,
            conflicts: [],
          };
        }

        // Ours is an ancestor of theirs, so the merge is a ref move — unless
        // the caller wants the merge commit recorded anyway.
        if (base === ours && input.noFastForward !== true) {
          return yield* settled("fast-forward", theirs, (yield* readCommit(theirs)).tree, base);
        }

        const flatten = (tree: Oid) =>
          Effect.gen(function* () {
            const map = new Map<string, TreeFile>();
            for (const file of yield* listFilesOf(tree)) map.set(file.path, file);
            return map;
          });

        const baseFiles =
          base === null
            ? new Map<string, TreeFile>()
            : yield* flatten((yield* readCommit(base)).tree);
        const ourFiles = yield* flatten((yield* readCommit(ours)).tree);
        const theirFiles = yield* flatten((yield* readCommit(theirs)).tree);

        // The walk itself is `Merge.mergeTrees`, shared with `Rebase` — the
        // treesame rules and the conflict taxonomy exist exactly once.
        const { changes, conflicts } = yield* mergeTrees({
          base: baseFiles,
          ours: ourFiles,
          theirs: theirFiles,
          strategy,
          read: readBlobAt,
        });

        const tree = yield* writeFiles({
          base: (yield* readCommit(ours)).tree,
          changes,
        });

        if (conflicts.length > 0)
          return { kind: "conflicted", commit: null, tree, base, conflicts };

        const message = input.message ?? `Merge ${input.theirs} into ${input.into ?? input.ours}\n`;
        const commit = yield* objects.write({
          type: "commit",
          data: encodeCommit({
            tree,
            parents: [ours, theirs],
            author: input.author,
            committer: input.author,
            message,
          }),
        });

        return yield* settled("merged", commit, tree, base);
      }),

      mergeBase,
      isAncestor: (ancestor, descendant) =>
        ancestor === descendant
          ? Effect.succeed(true)
          : ancestry([descendant]).pipe(Effect.map((seen) => seen.has(ancestor))),

      gc: Effect.fn("Repository.gc")(function* (options) {
        const roots = (yield* refs.list("refs/")).map(([, oid]) => oid);
        const head = yield* refs.resolve("HEAD");
        if (head !== null) roots.push(head);

        // Tolerant: a ref pointing at a missing object is fsck's problem to
        // report, not a reason to refuse to collect everything else.
        const reachable = (yield* walk(roots, { ignoreMissing: true })).seen;

        const removed: Oid[] = [];
        let scanned = 0;
        yield* Stream.runForEach(objects.list, (oid) =>
          Effect.gen(function* () {
            scanned++;
            if (reachable.has(oid) || oid === EMPTY_TREE_OID) return;
            removed.push(oid);
            if (options?.dryRun !== true) yield* objects.delete(oid);
          }),
        );

        const report = { scanned, reachable: reachable.size, removed };
        if (options?.repack !== true || options.dryRun === true) return report;

        const written = yield* repack(reachable);
        return written === null ? report : { ...report, packed: written };
      }),
    });
  }),
);
