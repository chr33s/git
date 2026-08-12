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

import { Invalid, ObjectNotFound } from "./Error.ts";
import {
  bytesToHex,
  EMPTY_TREE_OID,
  hashObject,
  parseCommit,
  parseTag,
  parseTree,
} from "./Format.ts";
import * as Pack from "./Pack.ts";
import { buildPackIndex } from "./PackIndex.ts";
import type { PackStore } from "./Packed.ts";
import { isOid, ObjectStore, type Oid, type RawObject, type RefStore } from "./Store.ts";

/** The two ports every operation here reads; `repack` also needs the third. */
export interface Stores {
  readonly objects: ObjectStore["Service"];
  readonly refs: RefStore["Service"];
}

/** A tag's target, from the header alone — enough to keep walking. */
const tagTarget = (data: Uint8Array): Oid | null => {
  const line = new TextDecoder().decode(data.subarray(0, 47));
  const target = line.startsWith("object ") ? line.slice(7, 47) : "";
  return isOid(target) ? target : null;
};

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
 * Everything reachable into one `.pack`/`.idx` pair, loose copies deleted.
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
const repack = Effect.fn("Maintenance.repack")(function* (
  objects: ObjectStore["Service"],
  packs: PackStore["Service"],
  keep: ReadonlySet<Oid>,
) {
  const oids = [...keep].filter((oid) => oid !== EMPTY_TREE_OID);
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

export interface GcReport {
  readonly scanned: number;
  readonly reachable: number;
  readonly removed: ReadonlyArray<Oid>;
  /** The pack a repack wrote, and how many objects went into it. */
  readonly packed?: { readonly name: string; readonly objects: number };
}

export const gc = Effect.fn("Maintenance.gc")(function* (
  stores: Stores & { readonly packs: PackStore["Service"] },
  options?: { readonly dryRun?: boolean; readonly repack?: boolean },
) {
  const { objects, packs, refs } = stores;

  const roots = (yield* refs.list("refs/")).map(([, oid]) => oid);
  const head = yield* refs.resolve("HEAD");
  if (head !== null) roots.push(head);

  // Tolerant: a ref pointing at a missing object is fsck's problem to
  // report, not a reason to refuse to collect everything else.
  const keep = (yield* reachable(objects, roots, { ignoreMissing: true })).seen;

  const removed: Oid[] = [];
  let scanned = 0;
  yield* Stream.runForEach(objects.list, (oid) =>
    Effect.gen(function* () {
      scanned++;
      if (keep.has(oid) || oid === EMPTY_TREE_OID) return;
      removed.push(oid);
      if (options?.dryRun !== true) yield* objects.delete(oid);
    }),
  );

  const report: GcReport = { scanned, reachable: keep.size, removed };
  if (options?.repack !== true || options.dryRun === true) return report;

  const written = yield* repack(objects, packs, keep);
  return written === null ? report : ({ ...report, packed: written } satisfies GcReport);
});
