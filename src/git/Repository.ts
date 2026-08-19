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
 *
 * The file is one service interface and its one implementation, and its size
 * follows from the surface, not from algorithms accumulating here: the
 * three-way walk lives in `Merge.ts`, reachability and maintenance in
 * `Maintenance.ts`, and replay/bisect/path-history in their own modules. A
 * method whose body outgrows a screen is a candidate for the same move.
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
  isFileMode,
  isGitlink,
  isTree,
  parseCommit,
  parseTag,
  parseTree,
  type Signature,
  type TagInfo,
  type TreeEntry,
} from "./Format.ts";
import { mergeTrees, type Strategy as MergeStrategy } from "./Merge.ts";
import * as Maintenance from "./Maintenance.ts";
import { reachable } from "./Maintenance.ts";
import * as Pack from "./Pack.ts";
import { PackStore } from "./Packed.ts";
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
  /** `null` removes the path, unless `oid` names what to put there instead. */
  readonly content: Uint8Array | null;
  /**
   * The entry's object, when the caller already has it and there is nothing
   * to write. A gitlink is the case that needs it: the commit it names lives
   * in another repository, so there are no bytes to store — hashing `content`
   * would invent a blob and the submodule would come back as a text file.
   */
  readonly oid?: Oid;
  /** git file mode; `100644` unless said otherwise. */
  readonly mode?: string;
}

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
  /**
   * Objects a tree may name and the pack must not carry.
   *
   * Redaction's shape from this side. A tombstoned payload's blob is deleted
   * while the tree naming it survives — the hash chain depends on that tree —
   * so a walk that treats every named object as required fails the whole
   * fetch the moment anything is redacted. The caller supplies the set,
   * because "which absences are explained" is a question about tombstones and
   * this layer has never heard of one.
   */
  readonly exclude?: ReadonlySet<Oid>;
}

export type { FsckProblem, FsckReport, GcReport } from "./Maintenance.ts";
import type { FsckReport as FsckReportType, GcReport as GcReportType } from "./Maintenance.ts";

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
    /**
     * What the ref itself holds, without following a symbolic one.
     *
     * The pairing `setRef`'s and `receive`'s `expected` is checked against:
     * comparing a compare-and-swap against the *resolved* value of a symbolic
     * ref names an oid the store will never agree with, so every such write
     * would fail as a conflict against a value nobody wrote.
     */
    readonly readRef: (name: string) => Effect.Effect<Oid | null, StorageFailure>;
    readonly head: Effect.Effect<string, StorageFailure>;
    /** Point HEAD at a ref — what a checkout does last. */
    readonly setHead: (ref: string) => Effect.Effect<void, StorageFailure | Invalid>;

    readonly readCommit: (oid: Oid) => Effect.Effect<CommitInfo, ObjectNotFound | StorageFailure>;
    readonly readTree: (
      oid: Oid,
    ) => Effect.Effect<ReadonlyArray<TreeEntry>, ObjectNotFound | StorageFailure>;
    readonly readBlob: (oid: Oid) => Effect.Effect<Uint8Array, ObjectNotFound | StorageFailure>;
    /** Any object, type included — what a caller holding only an oid needs. */
    readonly readObject: (oid: Oid) => Effect.Effect<RawObject, ObjectNotFound | StorageFailure>;

    readonly writeTree: (
      entries: ReadonlyArray<TreeEntry>,
    ) => Effect.Effect<Oid, StorageFailure | Invalid>;
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

    /**
     * git's `ok_to_give_up`: can every want already reach a commit the client
     * has confirmed common? True is what lets negotiation say `ready` — a
     * pack cut at `common` misses nothing the client needs. False only ever
     * costs another round or a larger pack, so the walk is budgeted rather
     * than exhaustive: history deeper than the budget answers false.
     */
    readonly canServe: (
      wants: ReadonlyArray<Oid>,
      common: ReadonlyArray<Oid>,
    ) => Effect.Effect<boolean, StorageFailure>;

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
    readonly fsck: Effect.Effect<FsckReportType, StorageFailure>;

    /**
     * Delete what no ref, and no recent reflog entry, can reach.
     *
     * Safe without a grace period only because every host serializes requests
     * per repository for as long as one is being answered — the Durable Object
     * input gate, `host/Node.ts`'s per-repository gate, which holds until the
     * response body has been written and not merely until it exists. That is
     * what keeps a collection from interleaving with the window in a push
     * between writing objects and moving the ref that makes them reachable, or
     * with the lazy read of a pack still streaming to a clone.
     *
     * The reflog is the other half: a commit a ref was reset off is protected
     * for `reflogGrace`, so recovery has something to recover from.
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
       *
       * It is also the only way to collect an object that is already packed,
       * because deleting one from a pack means rewriting the pack.
       */
      readonly repack?: boolean;
      /**
       * How long an object only the reflog still names is protected, in
       * milliseconds. `0` collects those too — what purging a secret needs.
       */
      readonly reflogGrace?: number;
      /**
       * Objects that a ref may name and still not protect — the payload blobs
       * a redaction tombstone covers. Reachability would otherwise keep them
       * forever, because the tree that names them has to survive for the
       * hash chain to hold.
       */
      readonly exclude?: ReadonlySet<Oid>;
    }) => Effect.Effect<GcReportType, ObjectNotFound | StorageFailure | Invalid>;

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
      /**
       * What `into` held when the *caller* judged this write, if it judged one.
       *
       * Read at write time instead, the swap compares the ref's value against
       * itself and cannot fail — so a push landing while the merge was being
       * computed was silently overwritten, and a write the boundary had judged
       * a fast-forward became one that drops commits.
       */
      readonly expected?: Oid | null;
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

/**
 * Every one of these, in order, as one.
 *
 * `Hooks` is a single service, so a host that wants two things to happen after
 * a push — deliver a webhook *and* forward to a mirror — cannot provide two
 * layers and hope. Refusals short-circuit, because a refusal is an answer and
 * the rest of the chain has nothing to add to it; `postReceive` runs all of
 * them, because it cannot refuse anything and one silent receiver must not
 * cost the next one its notification.
 */
export const hooksAll = (all: ReadonlyArray<Hooks["Service"]>): Hooks["Service"] => ({
  preReceive: (updates) =>
    Effect.forEach(all, (hooks) => hooks.preReceive(updates), { discard: true }),
  update: (update) => Effect.forEach(all, (hooks) => hooks.update(update), { discard: true }),
  postReceive: (results) =>
    Effect.forEach(all, (hooks) => hooks.postReceive(results).pipe(Effect.ignoreCause), {
      discard: true,
    }),
});

/** No-op hooks, which is what a server without policy wants. */
export const hooksNoop = Layer.succeed(Hooks, {
  preReceive: () => Effect.void,
  update: () => Effect.void,
  postReceive: () => Effect.void,
});

/**
 * A `RefUpdate` under construction: the same contract, writable, so a caller
 * can decide field by field whether `expected` participates — the store reads
 * an absent `expected` as "don't care".
 */
type RefUpdateDraft = {
  name: string;
  value: Oid | null;
  expected?: Oid | null;
  reason?: string;
};

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
    const readTreeEntries = (oid: Oid) =>
      oid === EMPTY_TREE_OID
        ? Effect.succeed<ReadonlyArray<TreeEntry>>([])
        : readTyped(oid, "tree", parseTree);

    /**
     * The tree object itself, with no opinion about what is in it.
     *
     * The checks live in `writeTree` rather than here because they are checks
     * on what a *caller* asked for. A rewrite passes the whole directory —
     * every untouched sibling read back out of the base tree along with the
     * one entry that changed — so validating at this level would judge history
     * the caller never touched and cannot fix: one entry git itself tolerates,
     * say a `100664` that arrived through `receive-pack`, would fail every
     * later commit, merge and rebase that goes anywhere near its directory.
     */
    const storeTree = (entries: ReadonlyArray<TreeEntry>) =>
      objects.write({ type: "tree", data: encodeTree(entries) });

    const writeTree = (entries: ReadonlyArray<TreeEntry>) =>
      Effect.gen(function* () {
        // `writeFiles` validates the paths it is given; this is the other way
        // in — the JSON tree endpoint hands entries straight here — and a
        // tree naming `.git`, `..` or two entries with one name is one git
        // will fetch and then refuse to check out.
        const seen = new Set<string>();
        for (const entry of entries) {
          if (entry.name === "" || entry.name.includes("/")) {
            return yield* new Invalid({ field: "name", reason: `bad tree entry '${entry.name}'` });
          }
          if (entry.name === "." || entry.name === ".." || entry.name.toLowerCase() === ".git") {
            return yield* new Invalid({ field: "name", reason: `'${entry.name}' is not content` });
          }
          // Judged on the bytes that will be written, not on the decode: two
          // names whose bytes differ are two entries, even when both decode
          // to the same U+FFFD spelling.
          const key = entry.raw === undefined ? entry.name : String.fromCharCode(...entry.raw);
          if (seen.has(key)) {
            return yield* new Invalid({ field: "name", reason: `duplicate entry '${entry.name}'` });
          }
          seen.add(key);
          if (!isTree(entry.mode) && !isFileMode(entry.mode)) {
            return yield* new Invalid({
              field: "mode",
              reason: `unsupported mode '${entry.mode}'`,
            });
          }
        }
        return yield* storeTree(entries);
      });

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
        // The same rule `Work.ts` applies when a tree is read onto a work
        // tree: git refuses to check such an entry out, so a tree containing
        // one is a branch nobody can clone — `error: invalid path '.git/…'`
        // and no working tree — and only history rewriting fixes it.
        if (segments.some((segment) => segment.toLowerCase() === ".git")) {
          return Effect.fail(
            new Invalid({ field: "path", reason: "the repository is not content" }),
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

      /**
       * What a loaded entry is filed under.
       *
       * The decoded name is not a key: git names are bytes, and two entries
       * whose bytes are different but not valid UTF-8 both decode to the same
       * U+FFFD spelling — so keying by the decode makes one of them overwrite
       * the other, and an unrelated commit that rewrites the tree drops a
       * tracked file. The `raw` bytes `parseTree` kept are the identity; a
       * leading NUL keeps that key out of the space of real names, which
       * cannot contain one.
       */
      const keyOf = (entry: TreeEntry): string =>
        entry.raw === undefined ? entry.name : `\0${String.fromCharCode(...entry.raw)}`;

      /**
       * The key a caller's path segment addresses.
       *
       * A caller can only spell a name that survived decoding, so a change to
       * `caf<U+FFFD>.txt` means the entry whose raw bytes decode to it — and
       * without this, setting that path would leave the original in place and
       * write a second entry with the same encoded name, which is a tree git
       * reads as corrupt.
       */
      const keyIn = (entries: Map<string, TreeEntry>, name: string) =>
        Effect.suspend(() => {
          if (entries.has(name)) return Effect.succeed(name);
          const matches = [...entries].filter(([, entry]) => entry.name === name);
          if (matches.length === 0) return Effect.succeed(name);
          // Two entries whose bytes differ but whose decode does not: the
          // caller cannot say which they meant, and picking one writes their
          // content into an arbitrary file.
          if (matches.length > 1) {
            return Effect.fail(
              new Invalid({
                field: "path",
                reason: `'${name}' names ${matches.length} entries whose bytes differ`,
              }),
            );
          }
          return Effect.succeed(matches[0]![0]);
        });

      /** The bytes an existing entry was stored under, kept across a rewrite. */
      const identityOf = (entries: Map<string, TreeEntry>, key: string) => entries.get(key)?.raw;

      const load = (prefix: string, oid: Oid | null) =>
        Effect.gen(function* () {
          const cached = directories.get(prefix);
          if (cached !== undefined) return cached;

          const entries = new Map<string, TreeEntry>();
          if (oid !== null) {
            for (const entry of yield* readTreeEntries(oid)) entries.set(keyOf(entry), entry);
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
              const entry = current.get(yield* keyIn(current, segment));
              yield* load(next, entry !== undefined && isTree(entry.mode) ? entry.oid : null);
            }
            current = directories.get(next)!;
            prefix = next;
          }
          return current;
        });

      /**
       * A path that becomes a file — or goes away — is no longer a directory.
       *
       * Without this the bottom-up pass below still holds the tree that used
       * to live there and writes it back over the entry just set, so a change
       * replacing a directory with a file would be silently dropped.
       */
      const forgetDirectory = (prefix: string) => {
        for (const loaded of directories.keys()) {
          if (loaded === prefix || loaded.startsWith(`${prefix}/`)) directories.delete(loaded);
        }
      };

      for (const change of input.changes) {
        const segments = yield* segmentsOf(change.path);
        const name = segments.at(-1)!;
        const parent = yield* directoryFor(segments.slice(0, -1));
        const full = segments.join("/");

        if (change.content === null && change.oid === undefined) {
          parent.delete(yield* keyIn(parent, name));
          forgetDirectory(full);
          continue;
        }

        const mode = change.mode ?? "100644";
        // Normalised, like every other mode comparison: `0100644` is the same
        // file mode as `100644`, and a repository holding one — git's own
        // `zeroPaddedFilemode` — could be read and merged but never written
        // back, so every merge on it failed with "unsupported file mode".
        if (!isFileMode(mode)) {
          return yield* new Invalid({ field: "mode", reason: `unsupported file mode '${mode}'` });
        }
        // A gitlink carries a commit that lives in another repository: there
        // is nothing to write here, and the entry keeps the oid it came with.
        const oid =
          change.oid ??
          (yield* objects.write({ type: "blob", data: change.content ?? new Uint8Array(0) }));
        const key = yield* keyIn(parent, name);
        // The replacement inherits the bytes of what it replaces: writing it
        // back under the decoded spelling would rename the file — which is
        // exactly what `raw` exists to prevent.
        const raw = identityOf(parent, key);
        parent.delete(key);
        parent.set(key, raw === undefined ? { mode, name, oid } : { mode, name, oid, raw });
        forgetDirectory(full);
      }

      const depth = (prefix: string) => (prefix === "" ? 0 : prefix.split("/").length);
      const prefixes = [...directories.keys()].sort((left, right) => depth(right) - depth(left));

      let root = EMPTY_TREE_OID;
      for (const prefix of prefixes) {
        const entries = [...directories.get(prefix)!.values()];

        if (prefix === "") {
          // `storeTree`, not `writeTree`: every change above was checked as it
          // was applied — its path by `segmentsOf`, its mode by `isFileMode`,
          // its name against the others by the map it went into — and what is
          // left in here beside it is the base tree's own entries, which are
          // not this caller's to answer for.
          root = yield* storeTree(entries);
          continue;
        }

        const slash = prefix.lastIndexOf("/");
        const parent = directories.get(slash === -1 ? "" : prefix.slice(0, slash))!;
        const name = slash === -1 ? prefix : prefix.slice(slash + 1);

        // git has no empty directories: one whose last entry went away is
        // dropped from its parent rather than written. Only if the parent
        // still calls it a directory, though — a change may have written a
        // file over the name since, and that file is what the caller meant.
        const key = yield* keyIn(parent, name);
        if (entries.length === 0) {
          const existing = parent.get(key);
          if (existing !== undefined && isTree(existing.mode)) parent.delete(key);
        } else {
          // As above: a directory whose name is not valid UTF-8 keeps its
          // bytes, or every file beneath it moves to a renamed path.
          const raw = identityOf(parent, key);
          parent.delete(key);
          const entry = { mode: TREE_MODE, name, oid: yield* storeTree(entries) };
          parent.set(key, raw === undefined ? entry : { ...entry, raw });
        }
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
          if (isTree(entry.mode)) stack.push({ oid: entry.oid, prefix: path });
          // Gitlinks included, deliberately. They are not content — the commit
          // they name lives in another repository — but this list is what
          // checkout, merge and rebase rebuild a tree from, so an entry left
          // out of it is an entry deleted from the next commit, silently and
          // with no conflict. Callers that read bytes skip them by mode; the
          // one thing none of them may do is not know the entry is there.
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
          // Appended one at a time rather than spread. A commit object states
          // its own parents, and a client can write as many as it likes: past
          // the argument limit — which is around a hundred thousand — the
          // spread throws a `RangeError`, so one crafted commit took out every
          // walk that met it, `gc` and merge-base included.
          if (commit !== null) for (const parent of commit.parents) stack.push(parent);
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

      /**
       * Anything strictly behind another shared commit is not the best base.
       *
       * One walk over the shared set, not a full `ancestry()` per member: the
       * old shape read the whole history again for every commit the two sides
       * had in common, so a merge of two long-lived branches re-read the
       * object store thousands of times over.
       *
       * One pass is enough because `shared` is closed under parents — both
       * sides come from `ancestry()`, which follows every parent — so a
       * commit with any shared child is marked by this loop when that child
       * is visited, and no propagation step can add to it.
       */
      const sharedSet = new Set(shared);
      const behind = new Set<Oid>();
      for (const oid of shared) {
        const commit = yield* readCommit(oid).pipe(
          Effect.map((value): CommitInfo | null => value),
          Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
        );
        if (commit === null) continue;
        // Only the parents need marking: a parent that is itself shared is
        // visited by this same loop, so its own parents are marked there.
        for (const parent of commit.parents) {
          if (sharedSet.has(parent)) behind.add(parent);
        }
      }

      return shared.find((oid) => !behind.has(oid)) ?? shared[0] ?? null;
    });

    /**
     * Two phases under one budget, both reading commit frontiers with
     * bounded concurrency rather than one storage round-trip per commit.
     *
     * Phase one walks each want down to the confirmed common set — wants
     * share the budget but not the visited set, since a shared set would
     * let one want's early exit hide the path another want needed. A want
     * that forked *below* a common commit never meets the set itself, so
     * phase two expands `covered` down the common commits' own history —
     * an ancestor of common is reachable from the client's haves, so
     * bottoming out there covers the want just as well (git propagates its
     * COMMON flag the same way) — and retries only the wants phase one
     * missed. Budget exhaustion anywhere answers false, which costs
     * another round or a larger pack, never a wrong one.
     */
    const canServe = Effect.fn("Repository.canServe")(function* (
      wants: ReadonlyArray<Oid>,
      common: ReadonlyArray<Oid>,
    ) {
      if (common.length === 0) return false;
      const covered = new Set<Oid>(common);
      let budget = 4096;

      /** One frontier of tolerant reads, `null` where no commit was found. */
      const commitsOf = (frontier: ReadonlyArray<Oid>) =>
        Effect.forEach(
          frontier,
          (oid) =>
            readCommit(oid).pipe(
              Effect.map((value): CommitInfo | null => value),
              Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
              Effect.map((commit) => [oid, commit] as const),
            ),
          { concurrency: 16 },
        );

      const reaches = (want: Oid) =>
        Effect.gen(function* () {
          const seen = new Set<Oid>();
          let frontier: Oid[] = [want];
          while (frontier.length > 0) {
            const layer: Oid[] = [];
            for (const oid of frontier) {
              if (seen.has(oid)) continue;
              seen.add(oid);
              if (covered.has(oid)) return true;
              layer.push(oid);
            }
            budget -= layer.length;
            if (budget < 0) return false;

            const next: Oid[] = [];
            for (const [oid, commit] of yield* commitsOf(layer)) {
              if (commit !== null) {
                for (const parent of commit.parents) next.push(parent);
                continue;
              }
              // Not a commit: an annotated tag peels to its target.
              // Anything else has no history to reach common through, and
              // a dead end only delays `ready` — the safe direction.
              const tag = yield* readTyped(oid, "tag", parseTag).pipe(
                Effect.map((value): TagInfo | null => value),
                Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
              );
              if (tag !== null) next.push(tag.object);
            }
            frontier = next;
          }
          return false;
        });

      const expandCovered = Effect.gen(function* () {
        let frontier: Oid[] = [...common];
        while (frontier.length > 0) {
          budget -= frontier.length;
          if (budget < 0) return;
          const next: Oid[] = [];
          for (const [, commit] of yield* commitsOf(frontier)) {
            if (commit === null) continue;
            for (const parent of commit.parents) {
              if (covered.has(parent)) continue;
              covered.add(parent);
              next.push(parent);
            }
          }
          frontier = next;
        }
      });

      const missed: Oid[] = [];
      for (const want of wants) {
        if (!(yield* reaches(want))) missed.push(want);
      }
      if (missed.length === 0) return true;
      if (budget <= 0) return false;

      yield* expandCovered;
      for (const want of missed) {
        if (budget <= 0) return false;
        if (!(yield* reaches(want))) return false;
      }
      return true;
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
        // The same rules `writeFiles` applies: these paths come from an index,
        // and an index is a file on disk that something else may have written.
        const segments = yield* segmentsOf(entry.path);
        const name = segments.at(-1)!;
        if (!isFileMode(entry.mode)) {
          return yield* new Invalid({
            field: "mode",
            reason: `unsupported file mode '${entry.mode}'`,
          });
        }
        // Every level, not just the one holding the file: a directory with no
        // direct file child was never registered, so the bottom-up pass below
        // never wrote it and never linked it into its parent — and everything
        // beneath it vanished from the commit with no error.
        let prefix = "";
        for (const segment of segments.slice(0, -1)) {
          prefix = prefix === "" ? segment : `${prefix}/${segment}`;
          directoryAt(prefix);
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
      exclude?: ReadonlySet<Oid>,
    ) =>
      Effect.gen(function* () {
        // What the client has is walked tolerantly: a `have` can reference
        // history this repository never saw, and that is not an error.
        const excluded = new Set(
          (yield* reachable(
            objects,
            haves,
            clientShallow === undefined
              ? { ignoreMissing: true }
              : { ignoreMissing: true, boundary: clientShallow },
          )).seen,
        );
        // Redacted payloads join the set the walk steps over. Still strict
        // about everything else: a missing object nobody accounted for is
        // corruption, and answering a fetch as though it were not would hand
        // the client a pack it cannot check.
        for (const oid of exclude ?? []) excluded.add(oid);
        return (yield* reachable(objects, wants, { ignoreMissing: false, skip: excluded })).order;
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
              if (isGitlink(entry.mode)) continue;
              if (isTree(entry.mode)) stack.push(entry.oid);
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
        const order = yield* closure(input.wants, input.haves, clientShallow, input.exclude);
        return { shallow: [], unshallow: [], oids: order };
      }

      // `deepen-not <ref>`: everything reachable from those refs stays put.
      const blocked = new Set<Oid>();
      for (const name of input.notRefs ?? []) {
        const oid = yield* refs.resolve(name);
        if (oid === null) continue;
        for (const seen of (yield* reachable(objects, [oid], { ignoreMissing: true })).seen)
          blocked.add(seen);
      }

      /**
       * A `want` may name an annotated tag, and the walk below is over
       * commits — so tags are peeled here. The tag objects are kept: a client
       * that asked for a tag must receive the tag itself, not only its target.
       */
      const tagged: Oid[] = [];
      const wants: Oid[] = [];
      const unwalkable: Oid[] = [];
      for (const want of input.wants) {
        let current = want;
        let object = yield* objects.read(current);
        // Tags can point at tags; a chain that does not end is a corrupt
        // repository, not a reason to walk forever.
        for (let hop = 0; hop < 8 && object.type === "tag"; hop++) {
          tagged.push(current);
          const tag = yield* Effect.fromResult(parseTag(object.data)).pipe(
            Effect.mapError(() => new ObjectNotFound({ oid: current })),
          );
          current = tag.object;
          object = yield* objects.read(current);
        }
        // A tag on a tree or a blob has no history to deepen: it and what it
        // contains go in the pack whole.
        if (object.type === "commit") wants.push(current);
        else unwalkable.push(current);
      }

      const since = input.since?.getTime();
      const depths = new Map<Oid, number>();
      const boundary = new Set<Oid>();
      const order: Oid[] = [];
      const queue: Array<{ readonly oid: Oid; readonly depth: number }> = wants.map((oid) => ({
        oid,
        depth: 1,
      }));

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

      const excluded = new Set(
        (yield* reachable(objects, input.haves, {
          ignoreMissing: true,
          boundary: clientShallow,
        })).seen,
      );
      // The same absences the shallow path steps over. Dropping `exclude`
      // here meant a `deepen`/`deepen-since`/`deepen-not` fetch that reached a
      // redacted event's tree put the deleted blob into the plan, and the pack
      // writer then failed on an object the tombstone accounts for.
      for (const oid of input.exclude ?? []) excluded.add(oid);

      const loose =
        unwalkable.length === 0
          ? []
          : (yield* reachable(objects, unwalkable, { ignoreMissing: false, skip: excluded })).order;

      return {
        // Only boundaries the client does not already record are news to it.
        shallow: [...boundary].filter((oid) => !clientShallow.has(oid)),
        // A commit the client had as a boundary and whose parents are in this
        // pack is no longer one.
        unshallow: [...clientShallow].filter((oid) => depths.has(oid) && !boundary.has(oid)),
        oids: [...new Set([...tagged, ...loose, ...(yield* objectsOf(order, excluded))])].filter(
          (oid) => !excluded.has(oid),
        ),
      };
    });

    return Repository.of({
      refs: refs.list("refs/"),
      resolve: refs.resolve,
      readRef: refs.read,
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
          // A tree this repository does not hold makes a commit no clone can
          // read: the API takes `tree` from a caller, and `setRef` checks its
          // target for exactly this reason while `commit` did not.
          if (tree !== EMPTY_TREE_OID && !(yield* objects.has(tree))) {
            return yield* new ObjectNotFound({ oid: tree });
          }

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
        return Stream.paginate({ frontier: [from], seen: new Set<Oid>() }, (state) =>
          Effect.gen(function* () {
            let frontier = state.frontier.filter((oid) => !state.seen.has(oid));
            if (frontier.length === 0) {
              return [[], Option.none<typeof state>()] as const;
            }

            const commits = yield* Effect.forEach(frontier, (oid) =>
              readCommit(oid).pipe(Effect.map((commit) => ({ ...commit, oid }))),
            );

            /**
             * When a commit says it was made, with an unparseable date read
             * as the epoch.
             *
             * A commit object can carry anything a client wrote, and a
             * timestamp that will not parse is a `NaN` — which compares equal
             * to nothing, itself included. So `latest` became `NaN`, `tied`
             * came out empty, and the reduce below threw `Reduce of empty
             * array` on a repository whose only fault was one odd commit:
             * `git log` stopped working and nothing said which commit did it.
             * Oldest is the honest reading of a date nobody can read.
             */
            const when = (commit: { readonly committer: Signature }): number => {
              const at = commit.committer.at.getTime();
              return Number.isNaN(at) ? 0 : at;
            };

            // Folded rather than spread, for the reason `ancestry` appends
            // its parents one at a time: the frontier is as wide as the widest
            // commit in it, and `Math.max` handed a hundred thousand arguments
            // is a `RangeError` rather than a maximum.
            let latest = Number.NEGATIVE_INFINITY;
            for (const commit of commits) latest = Math.max(latest, when(commit));
            const tied = commits.filter((commit) => when(commit) === latest);

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
                if (when(commit) !== latest) continue;
                for (const parent of commit.parents) pending.push(parent);
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

            return [[newest], Option.some({ frontier, seen: state.seen })] as const;
          }),
        ).pipe(options?.limit === undefined ? (self) => self : Stream.take(options.limit));
      },

      branch: Effect.fn("Repository.branch")(function* ({ base, name }) {
        // A commit is as good a base as a ref name, and every other verb here
        // takes both. Without it `checkout -b` could not work at all: it
        // resolves the tip it is branching from and hands the oid over, so
        // the ref store looked for a ref literally named `<40 hex>`, found
        // none, and every `checkout -b` failed — after rewriting the work
        // tree, which is the half that made it look like something else.
        const from = isOid(base)
          ? (yield* objects.has(base))
            ? base
            : null
          : yield* refs.resolve(base);
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
        const results = applied.map((result, index): ReceiveResult => {
          const from = updates[index]?.expected ?? null;
          return result.applied
            ? { ref: result.name, from, to: result.current, ok: true }
            : {
                ref: result.name,
                from,
                to: result.current,
                ok: false,
                // The store's own reason when it has one — a ref it could not
                // write is not a ref somebody else moved, and a client that is
                // told the latter retries a push that cannot succeed.
                reason: result.reason ?? "ref moved",
              };
        });

        yield* hooks.postReceive(results);
        return results;
      }),

      contains: objects.has,
      canServe,

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
        let oid = resolved;
        if (message !== undefined) {
          const info: TagInfo =
            tagger === undefined
              ? { object: resolved, type: object.type, tag: name, message }
              : { object: resolved, type: object.type, tag: name, tagger, message };
          oid = yield* objects.write({ type: "tag", data: encodeTag(info) });
        }

        const ref = `refs/tags/${name}`;
        const update: RefUpdateDraft = { name: ref, value: oid, reason: `tag: ${name}` };
        // A tag is meant to be stable, so replacing one is opt-in.
        if (force !== true) update.expected = null;
        const [result] = yield* refs.apply([update]);

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
        const update: RefUpdateDraft = { name, value: target, reason: `set: ${to}` };
        if (expected !== undefined) update.expected = expected;
        const [result] = yield* refs.apply([update]);

        if (result === undefined || !result.applied) {
          return yield* new RefConflict({
            ref: name,
            expected: expected ?? null,
            actual: result?.current ?? null,
          });
        }

        return { ref: name, oid: target, previous };
      }),

      fsck: Maintenance.fsck({ objects, refs }),

      listFiles: listFilesOf,

      findPath: Effect.fn("Repository.findPath")(function* (tree, path) {
        const segments = path.split("/").filter((segment) => segment !== "");
        let current = tree;

        for (let index = 0; index < segments.length; index++) {
          const entries = yield* readTreeEntries(current);
          const entry = entries.find((candidate) => candidate.name === segments[index]);
          if (entry === undefined) return null;
          if (index === segments.length - 1) return entry;
          if (!isTree(entry.mode)) return null;
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
              // The caller's snapshot wins when it took one; see `expected`.
              const expected =
                input.expected !== undefined
                  ? input.expected
                  : isOid(input.into)
                    ? undefined
                    : yield* refs.read(input.into);
              const update: RefUpdateDraft = {
                name: input.into,
                value: commit,
                reason: `merge: ${input.theirs}`,
              };
              if (expected !== undefined) update.expected = expected;
              const [result] = yield* refs.apply([update]);
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

      gc: (options) => Maintenance.gc({ objects, packs, refs }, options),
    });
  }),
);
