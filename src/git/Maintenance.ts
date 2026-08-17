/**
 * Reachability, and the maintenance that stands on it: fsck, gc, repack.
 *
 * Everything here consumes the storage ports directly rather than
 * `Repository` — these are the operations that look *underneath* the domain
 * service, at the store as a set of objects, and asking the service for its
 * own integrity would presuppose the thing being checked.
 *
 * `reachable` is exported because it is the one traversal several answers
 * share: what a fetch must send, what gc may delete, what a shallow boundary
 * hides. One walk, so "reachable" means the same thing in every one of them.
 */
import { Effect, Result, Stream } from "effect";

import { Invalid, ObjectNotFound, StorageFailure } from "./Error.ts";
import {
  bytesToHex,
  concatBytes,
  EMPTY_TREE_OID,
  hashObject,
  isGitlink,
  parseCommit,
  parseTag,
  parseTree,
} from "./Format.ts";
import * as Pack from "./Pack.ts";
import { bufferSource, readAt } from "./PackFile.ts";
import { buildPackIndex, parsePackIndex } from "./PackIndex.ts";
import type { PackStore } from "./Packed.ts";
import * as Refspec from "./Refspec.ts";
import { isOid, ObjectStore, type Oid, type RawObject, type RefStore } from "./Store.ts";

/** The two ports every operation here reads; `repack` also needs the third. */
export interface Stores {
  readonly objects: ObjectStore["Service"];
  readonly refs: RefStore["Service"];
}

const encoder = new TextEncoder();

/** A tag's target, from the header alone — enough to keep walking. */
const tagTarget = (data: Uint8Array): Oid | null => {
  const line = new TextDecoder().decode(data.subarray(0, 47));
  const target = line.startsWith("object ") ? line.slice(7, 47) : "";
  return isOid(target) ? target : null;
};

/**
 * git's `pack_name_hash`, over a tree entry's name: whitespace skipped, and
 * each byte shifted in so the tail of the name dominates. Same-named files
 * across commits — the best delta pairs a repository has — land on the same
 * hash, which is all `deltaOrder` needs from it.
 */
const nameHash = (name: string): number => {
  let hash = 0;
  for (const byte of encoder.encode(name)) {
    if (byte === 0x20 || (byte >= 0x09 && byte <= 0x0d)) continue;
    hash = ((hash >>> 2) + ((byte << 24) >>> 0)) >>> 0;
  }
  return hash;
};

/**
 * What a classifying walk learned about the objects it read — enough for
 * `deltaOrder` to sort delta candidates without reading anything again.
 * An object the walk could not read is simply absent.
 */
export interface Classified {
  readonly kinds: ReadonlyMap<Oid, { readonly type: RawObject["type"]; readonly size: number }>;
  /** Tree-entry name hash per child oid, for the objects trees name. */
  readonly names: ReadonlyMap<Oid, number>;
}

/**
 * Everything reachable from `roots`: commits pull in their tree and parents,
 * trees their entries (gitlinks excepted — those live in another repository),
 * tags their target. The empty tree is git's one virtual object; a commit may
 * reference it without any store holding it.
 */
export const reachable = (
  objects: ObjectStore["Service"],
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
    /**
     * Also record type/size per object and a name hash per tree entry —
     * the walk reads every object anyway, so classification is free here
     * and spares repack a second full read pass. Off on the fetch path,
     * which runs this walk per request and never deltifies.
     */
    readonly classify?: boolean;
  },
) =>
  Effect.gen(function* () {
    const seen = new Set<Oid>();
    const order: Oid[] = [];
    const kinds = new Map<Oid, { type: RawObject["type"]; size: number }>();
    const names = new Map<Oid, number>();
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
      if (options.classify === true) {
        kinds.set(oid, { type: object.type, size: object.data.length });
      }

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
          // By mode, not by spelling: a gitlink written `0160000` compared
          // unequal here and its commit — an object in another repository —
          // was pushed onto the walk, so every clone of that repository
          // failed on an object it was never supposed to have.
          for (const entry of entries) {
            if (isGitlink(entry.mode)) continue;
            stack.push(entry.oid);
            if (options.classify === true) names.set(entry.oid, nameHash(entry.name));
          }
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

    return { order, seen, classified: { kinds, names } satisfies Classified };
  });

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

export const fsck = Effect.fn("Maintenance.fsck")(function* (stores: Stores) {
  const { objects, refs } = stores;
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

  return { checked, problems, danglingRefs } satisfies FsckReport;
});

/**
 * The emission order the delta window wants: same-type objects grouped,
 * trees and blobs sorted by the name their tree entry gives them and then
 * largest first — so successive versions of one file sit inside the window
 * with the biggest as the natural base. Commits and tags keep their walk
 * order, which already puts a commit next to its parent.
 *
 * A pure sort over what the classifying walk already read — ordering costs
 * no I/O of its own. The name is the entry's rather than the full path:
 * `pack_name_hash` weights the tail of a path, so the entry name carries
 * most of the signal without a root-down walk. Objects the walk could not
 * read (a dangling ref's target, tolerated by `ignoreMissing`) were never
 * classified and so are never ordered — which is what keeps a repack from
 * failing on damage that is fsck's to report.
 */
export const deltaOrder = (classified: Classified): ReadonlyArray<Oid> => {
  interface Sortable {
    readonly oid: Oid;
    readonly hash: number;
    readonly size: number;
  }
  const commits: Oid[] = [];
  const tags: Oid[] = [];
  const trees: Sortable[] = [];
  const blobs: Sortable[] = [];
  for (const [oid, kind] of classified.kinds) {
    if (kind.type === "commit") commits.push(oid);
    else if (kind.type === "tag") tags.push(oid);
    else {
      const sortable = { oid, hash: classified.names.get(oid) ?? 0, size: kind.size };
      (kind.type === "tree" ? trees : blobs).push(sortable);
    }
  }

  const grouped = (left: Sortable, right: Sortable) =>
    left.hash - right.hash || right.size - left.size || left.oid.localeCompare(right.oid);
  trees.sort(grouped);
  blobs.sort(grouped);

  return [...commits, ...tags, ...trees.map((tree) => tree.oid), ...blobs.map((blob) => blob.oid)];
};

/**
 * Everything reachable into one `.pack`/`.idx` pair, loose copies deleted.
 *
 * Order is the whole safety argument: the pack is written and every entry
 * verified readable — delta chains applied, bytes hashed back to the oid
 * the index claims — *before* anything is deleted, so an interruption or a
 * writer bug leaves the repository with both copies rather than neither.
 * `packed` prefers loose objects for the same reason.
 *
 * The pack is built in memory. That is the honest cost of writing an
 * `.idx`, which cannot be finished until every offset is known — and
 * repacking is a maintenance call a host schedules, not something on the
 * request path.
 */
const repack = Effect.fn("Maintenance.repack")(function* (
  objects: ObjectStore["Service"],
  packs: PackStore["Service"],
  classified: Classified,
  /** Packs the new one supersedes; deleting them is what collects their garbage. */
  superseded: ReadonlyArray<string>,
) {
  const ordered = deltaOrder(classified).filter((oid) => oid !== EMPTY_TREE_OID);
  if (ordered.length === 0) return null;

  const entries: Pack.PackedEntry[] = [];
  const chunks = yield* Stream.runCollect(
    // Deltified here and nowhere else: repack is background work whose
    // output is storage, so the window's CPU and pinned memory buy smaller
    // packs at rest without costing any request a first byte. `PackFile.ts`
    // resolves the ofs-deltas on every later read with no cross-read base
    // cache, so the chain cap is what bounds that read amplification —
    // hence 16 here rather than the writer's format-conventional 50.
    Pack.pack(ordered, {
      onObject: (entry) => entries.push(entry),
      deltify: { maxDepth: 16 },
    }).pipe(Stream.provideService(ObjectStore, objects)),
  );

  const bytes = concatBytes(chunks);
  const total = bytes.length;

  // The trailer is the pack's own name, which is how git names the pair.
  const checksum = bytes.subarray(total - 20);
  const name = `pack-${bytesToHex(checksum)}`;
  const index = buildPackIndex(entries, checksum);

  // The verification the deletion below leans on: resolve every entry back
  // out of the in-memory bytes and hash it. The loose copies these bytes
  // are about to replace are the last other copy of the repository, so the
  // delta writer is not trusted — it is checked, before the pack is even
  // stored. Pure CPU on bytes already held.
  const source = bufferSource(bytes);
  for (const entry of entries) {
    const unreadable = (cause: unknown) =>
      new StorageFailure({ operation: "repack verify", path: entry.oid, cause });
    const object = yield* Effect.tryPromise({
      try: () => readAt(source, entry.offset, () => Promise.resolve(null), 0, packs.inflate),
      catch: unreadable,
    });
    const actual = yield* hashObject(object);
    if (actual !== entry.oid) {
      return yield* unreadable(`entry resolves to ${actual}`);
    }
  }

  yield* packs.write({ name, pack: bytes, index });

  // Only now: the objects are in the pack, verified, and the pack is stored.
  // The old packs go first — everything reachable that was in them is in the
  // new one, and what was not reachable is the garbage this whole call is for.
  // Leaving them is how a repository grows a pack per gc and never collects an
  // object that was ever packed.
  yield* Effect.forEach(
    superseded.filter((old) => old !== name),
    (old) => packs.delete(old),
    { discard: true },
  );
  yield* Effect.forEach(ordered, (oid) => objects.delete(oid), { discard: true });

  return { name, objects: ordered.length };
});

export interface GcReport {
  /** Set when `repack` was asked for and could not be done, and why. */
  readonly repackSkipped?: string;
  readonly scanned: number;
  readonly reachable: number;
  /** Objects this call actually deleted. */
  readonly removed: ReadonlyArray<Oid>;
  /**
   * Unreachable objects that survive because a pack holds them. Deleting from
   * a pack means rewriting it, so `repack` is what collects these.
   */
  readonly retained: ReadonlyArray<Oid>;
  /** The pack a repack wrote, and how many objects went into it. */
  readonly packed?: { readonly name: string; readonly objects: number };
}

/**
 * How long an object the reflog alone names is protected. git's own default,
 * and the reason `git reflog expire --expire=now` exists: a reflog entry is
 * the record that makes "recover what I just reset away" possible, so
 * collecting what one names is a data loss the user cannot see coming.
 */
export const REFLOG_GRACE_MS = 90 * 24 * 60 * 60 * 1000;

export const gc = Effect.fn("Maintenance.gc")(function* (
  stores: Stores & { readonly packs: PackStore["Service"] },
  options?: {
    readonly dryRun?: boolean;
    readonly repack?: boolean;
    /** Milliseconds; `0` collects everything only the reflog still names. */
    readonly reflogGrace?: number;
    /**
     * Objects a ref may name and still not protect.
     *
     * Redaction's other half. A tombstoned payload blob is still referenced by
     * the tree of the commit that carried it — that structure has to survive,
     * or every later event's hash breaks — so reachability alone would protect
     * the content forever. Excluding it here is what lets a repack drop the
     * pack copy, which is the only way a packed object is ever removed.
     */
    readonly exclude?: ReadonlySet<Oid>;
  },
) {
  const { objects, packs, refs } = stores;

  // Asked once, before anything is deleted: a fork reads these objects
  // through git's `alternates` and keeps no copy of them, and its refs cannot
  // be seen from here — so collecting would destroy history it still
  // advertises. Answered as a refusal the caller can act on, not as a failure
  // part-way through a deletion loop.
  const shared = objects.shared === undefined ? null : yield* objects.shared;
  if (shared !== null && shared.borrowers.length > 0) {
    return yield* new Invalid({
      field: "gc",
      reason: `this repository lends its objects to ${shared.borrowers.join(", ")}; collect those first`,
    });
  }

  const named = yield* refs.list("refs/");
  /** Only refs gate the walk below; a reflog may name what a purge collected. */
  const refRoots = named.map(([, oid]) => oid);
  const roots = [...refRoots];
  /**
   * The same roots, less the hub's own.
   *
   * An exclusion says the *hub* must not keep a payload alive; it does not say
   * a branch may not. So the refs the exclusion is not about are walked a
   * second time without it, and this is that root set — kept in step with
   * `roots` rather than rebuilt from ref names, so it carries `HEAD` and the
   * reflog entries too.
   */
  const sourceRoots = named
    .filter(([name]) => !Refspec.hiddenFromAdvertisement(name))
    .map(([, oid]) => oid);
  const head = yield* refs.resolve("HEAD");
  if (head !== null) {
    refRoots.push(head);
    roots.push(head);
    sourceRoots.push(head);
  }

  // Where a ref has been, not only where it is. Without these, a reset or a
  // force-push destroys the commit it moved off the moment gc next runs, and
  // the reflog entry that was supposed to lead back to it dangles.
  const cutoff = Date.now() - (options?.reflogGrace ?? REFLOG_GRACE_MS);
  // Every ref that has a log, not only the refs that still exist: a branch
  // deleted by mistake is precisely the case this protection is for, and it
  // is gone from `list` the moment it is deleted.
  const logs = new Set([...named.map(([ref]) => ref), ...(yield* refs.logged), "HEAD"]);
  for (const name of logs) {
    for (const entry of yield* refs.reflog(name)) {
      const at = entry.at.getTime();
      // Strictly newer than the cutoff, so a grace of `0` protects nothing
      // even when the entry was written in this same millisecond. An entry
      // whose timestamp will not parse is treated as expired rather than as
      // infinitely young — otherwise one unreadable line pins a repository's
      // garbage forever and no grace setting can release it.
      if (Number.isNaN(at) || at <= cutoff) continue;
      if (entry.from !== null) roots.push(entry.from);
      if (entry.to !== null) roots.push(entry.to);
      if (Refspec.hiddenFromAdvertisement(name)) continue;
      if (entry.from !== null) sourceRoots.push(entry.from);
      if (entry.to !== null) sourceRoots.push(entry.to);
    }
  }

  // Tolerant: a ref pointing at a missing object is fsck's problem to
  // report, not a reason to refuse to collect everything else. Classified
  // only when a repack will consume it — classification is free inside the
  // walk but pointless without one.
  //
  // Borrowed objects are in this walk but not in this repository: packing them
  // here would copy the parent's whole history into the fork and undo the
  // sharing a fork exists for.
  const borrowing = (shared?.alternates.length ?? 0) > 0;
  const willRepack = options?.repack === true && options?.dryRun !== true && !borrowing;
  const walked = yield* reachable(objects, roots, {
    ignoreMissing: true,
    classify: willRepack,
    skip: options?.exclude,
  });

  // An exclusion says "the hub must not keep this alive", not "delete this".
  // Git dedupes by content, so a redacted event payload can be byte-identical
  // to a blob the source history also holds — post the comment, commit the
  // same bytes as a file, have the comment redacted — and a skip applied to
  // the *whole* walk then deleted an object `refs/heads/*` still reaches,
  // leaving the source history dangling. So the refs the exclusion is not
  // about are walked again without it, and what they reach survives.
  const source =
    (options?.exclude?.size ?? 0) === 0
      ? null
      : yield* reachable(objects, sourceRoots, { ignoreMissing: true, classify: willRepack });
  const keep = source === null ? walked.seen : new Set([...walked.seen, ...source.seen]);
  const classified =
    source === null
      ? walked.classified
      : {
          kinds: new Map([...source.classified.kinds, ...walked.classified.kinds]),
          names: new Map([...source.classified.names, ...walked.classified.names]),
        };

  // What a pack holds cannot be deleted object by object, so an unreachable
  // object that is packed is only reported as removed when a repack — which
  // drops the packs it supersedes — is going to run.
  const packedOids = new Set<Oid>();
  const handles = yield* packs.list;
  for (const handle of handles) {
    const parsed = parsePackIndex(handle.index);
    if (parsed._tag === "Failure") {
      return yield* new StorageFailure({
        operation: "packs.list",
        path: handle.name,
        cause: parsed.failure,
      });
    }
    for (const entry of parsed.success) packedOids.add(entry.oid);
  }

  /**
   * Roots that lead nowhere are not a licence to delete.
   *
   * A repository with roots whose objects the store cannot read back is broken
   * in a way `fsck` diagnoses, and every object in it would look unreachable —
   * so the walk would name the whole store as garbage. Refusing here, before
   * anything is deleted, is the difference between reporting a problem and
   * being the problem. An empty repository has no roots at all and still
   * collects normally.
   */
  // Reflog roots are deliberately not counted: a zero-grace purge leaves
  // entries naming objects it just collected, and treating those as evidence
  // of a broken store would wedge every later collection for the whole grace
  // window.
  if (refRoots.length > 0) {
    // The refs' *own* walk, not the one the reflog roots also fed: a single
    // readable reflog entry would otherwise disarm the guard for a store
    // whose every ref is unreadable, and gc would sweep what was salvageable.
    const fromRefs = yield* reachable(objects, refRoots, { ignoreMissing: true });
    if (fromRefs.order.length === 0) {
      return yield* new StorageFailure({
        operation: "gc",
        path: "refs",
        cause: `refs name ${refRoots.length} object(s) this store cannot read; run fsck`,
      });
    }
  }

  const unreachable: Oid[] = [];
  let scanned = 0;
  yield* Stream.runForEach(objects.list, (oid) =>
    Effect.gen(function* () {
      scanned++;
      if (keep.has(oid) || oid === EMPTY_TREE_OID) return;
      unreachable.push(oid);
      // Loose-only, by the port's contract; the pack copy goes with the pack.
      if (options?.dryRun !== true) yield* objects.delete(oid);
    }),
  );

  const written = !willRepack
    ? null
    : yield* repack(
        objects,
        packs,
        classified,
        handles.map((handle) => handle.name),
      );

  // What survives is decided by what actually happened, not by what was asked
  // for: an object inside a pack is gone only if that pack was superseded, and
  // a repack that wrote nothing superseded nothing. Reporting it either way
  // would tell a caller a secret was collected while it is still clonable.
  const collected = (oid: Oid) => !packedOids.has(oid) || written !== null;
  const counted: GcReport = {
    scanned,
    reachable: keep.size,
    removed: unreachable.filter(collected),
    retained: unreachable.filter((oid) => !collected(oid)),
  };
  // Said only when a repack was asked for and declined, so an ordinary gc
  // report does not carry an empty explanation of something that never came up.
  const report: GcReport =
    options?.repack === true && borrowing
      ? {
          ...counted,
          repackSkipped:
            "this repository borrows objects through alternates; packing them here would copy the history it shares",
        }
      : counted;
  return written === null ? report : ({ ...report, packed: written } satisfies GcReport);
});
