/**
 * Replaying commits: cherry-pick, and rebase as a sequence of cherry-picks.
 *
 * A cherry-pick is a three-way merge, not a patch application: the base is the
 * picked commit's *parent*, "ours" is the target, "theirs" is the commit. That
 * choice of base is the whole module. Merging the commit against `onto`
 * directly — the base `Repository.merge` would compute for the same two
 * commits — makes every file `onto` changed since the histories parted look
 * like a difference to resolve, and the pick silently reverts work nobody
 * asked it to touch. With the parent as the base, a path the commit left alone
 * is unchanged on "their" side and `onto`'s version stands untouched.
 *
 * Conflicts are values here, as they are in `Repository.merge`: a replay that
 * cannot be resolved stops the sequence and reports which commit stopped it.
 * Guessing a resolution is the one thing a rebase must never do, and carrying
 * on past a conflict would replay the rest against a tree the author never
 * wrote.
 */
import { Effect, Stream } from "effect";

import { Invalid, type ObjectNotFound, type RefConflict, type StorageFailure } from "./Error.ts";
import type { CommitInfo, Signature } from "./Format.ts";
import { mergeTrees } from "./Merge.ts";
import { type MergeConflict, Repository, type TreeFile } from "./Repository.ts";
import { isOid, type Oid } from "./Store.ts";

/** Everything replaying a commit can go wrong with, ref move included. */
export type RebaseError = Invalid | ObjectNotFound | RefConflict | StorageFailure;

export interface Replayed {
  readonly original: Oid;
  /**
   * The commit the replay produced, or `null` when it produced none: either it
   * conflicted — and `conflicts` says where — or there was nothing left to
   * apply, because `onto` already carries the change.
   */
  readonly replayed: Oid | null;
  readonly conflicts: ReadonlyArray<MergeConflict>;
}

export interface ReplayOutcome {
  readonly kind: "replayed" | "up-to-date" | "conflicted";
  /** The new tip when it succeeded; `null` when it conflicted. */
  readonly head: Oid | null;
  /** One entry per commit considered, in the order they were replayed. */
  readonly commits: ReadonlyArray<Replayed>;
}

const resolveCommit = Effect.fn("Rebase.resolveCommit")(function* (name: string) {
  const repository = yield* Repository;
  const oid = isOid(name) ? name : yield* repository.resolve(name);
  if (oid === null) return yield* new Invalid({ field: "ref", reason: `unknown ref '${name}'` });
  return oid;
});

const filesOf = Effect.fn("Rebase.filesOf")(function* (tree: Oid) {
  const repository = yield* Repository;
  const files = new Map<string, TreeFile>();
  for (const file of yield* repository.listFiles(tree)) files.set(file.path, file);
  return files;
});

/**
 * The tree `commit`'s change produces on top of `onto`, or `null` and the
 * conflicts that stopped it.
 */
const replayTree = Effect.fn("Rebase.replayTree")(function* (input: {
  readonly commit: CommitInfo;
  readonly onto: CommitInfo;
}) {
  const repository = yield* Repository;

  // The base is the commit's own first parent: what its author changed is
  // `parent -> commit`, and only a merge against that base carries that much
  // and no more. A root commit has no parent, so all of it is new.
  const parent = input.commit.parents[0];
  const baseFiles =
    parent === undefined
      ? new Map<string, TreeFile>()
      : yield* filesOf((yield* repository.readCommit(parent)).tree);
  const ourFiles = yield* filesOf(input.onto.tree);
  const theirFiles = yield* filesOf(input.commit.tree);

  // The walk is `Merge.mergeTrees` with the roles cast for a replay: `ours`
  // is `onto`, `theirs` is the commit, and the base above is what makes the
  // common case — a path the commit never touched — resolve to `onto`'s
  // version instead of being dragged back to the fork point.
  const { changes, conflicts } = yield* mergeTrees({
    base: baseFiles,
    ours: ourFiles,
    theirs: theirFiles,
    read: repository.readBlob,
  });

  // Nothing is written when a replay conflicts. A merge hands the caller a
  // tree with markers to resolve; a replay hands back the commit that failed,
  // and a tree of half-applied changes would only be reachable garbage.
  if (conflicts.length > 0) return { tree: null, conflicts };

  return {
    tree: yield* repository.writeFiles({ base: input.onto.tree, changes }),
    conflicts: [],
  };
});

const replayOne = Effect.fn("Rebase.replayOne")(function* (input: {
  readonly commit: Oid;
  readonly onto: Oid;
  readonly author?: Signature | undefined;
}) {
  const repository = yield* Repository;

  // Already contained: `onto` carries this change under this very name, and a
  // second copy of it is not what "replay" means.
  if (yield* repository.isAncestor(input.commit, input.onto)) {
    return { original: input.commit, replayed: null, conflicts: [] } satisfies Replayed;
  }

  const commit = yield* repository.readCommit(input.commit);
  const onto = yield* repository.readCommit(input.onto);
  const { conflicts, tree } = yield* replayTree({ commit, onto });
  if (tree === null)
    return { original: input.commit, replayed: null, conflicts } satisfies Replayed;

  // The change is already there by content, arrived by some other route — a
  // previous pick, an identical edit. git calls this an empty cherry-pick and
  // refuses to record it rather than writing a commit that changes nothing.
  if (tree === onto.tree) {
    return { original: input.commit, replayed: null, conflicts: [] } satisfies Replayed;
  }

  const replayed = yield* repository.commitTree({
    tree,
    parents: [input.onto],
    // Authorship travels with the change — it is the same change, by the same
    // person — while the committer is whoever replayed it, which is what
    // separates a rebased commit from the original it copies.
    author: commit.author,
    committer: input.author ?? commit.committer,
    message: commit.message,
  });

  return { original: input.commit, replayed, conflicts: [] } satisfies Replayed;
});

/** The outcome, and the ref move that makes it visible. */
const settle = Effect.fn("Rebase.settle")(function* (input: {
  readonly onto: Oid;
  readonly commits: ReadonlyArray<Replayed>;
  readonly into?: string | undefined;
  /** Where `into` stood when the replay began; the swap compares against it. */
  readonly expected?: Oid | null;
}) {
  const repository = yield* Repository;
  const head = input.commits.reduce<Oid>((last, entry) => entry.replayed ?? last, input.onto);

  if (input.commits.some((entry) => entry.conflicts.length > 0)) {
    // Whatever was replayed before the conflict stays in the object store,
    // unreachable, for `gc` — the caller gets the commits that succeeded in
    // `commits` and no ref pointing at a half-finished replay.
    const outcome: ReplayOutcome = { kind: "conflicted", head: null, commits: input.commits };
    return outcome;
  }

  if (head === input.onto) {
    // Nothing was replayed, so no ref moves: the caller asked to replay
    // commits, not to fast-forward a branch that is merely behind. That is
    // `Repository.merge`'s job and it already spells it.
    const outcome: ReplayOutcome = { kind: "up-to-date", head, commits: input.commits };
    return outcome;
  }

  if (input.into !== undefined) {
    // Compare-and-swap, as `Repository.merge` does: a replay that raced
    // another push loses cleanly instead of overwriting it. `expected` is
    // where the ref stood when the replay began — reading it here instead
    // would be comparing the value against itself, which no race can fail.
    const update = { name: input.into, to: head };
    yield* repository.setRef(
      input.expected === undefined ? update : { ...update, expected: input.expected },
    );
  }

  const outcome: ReplayOutcome = { kind: "replayed", head, commits: input.commits };
  return outcome;
});

/** Apply one commit's change onto `onto`, as `git cherry-pick` does. */
export const cherryPick = Effect.fn("Rebase.cherryPick")(function* (input: {
  /** A ref or an oid. */
  readonly commit: string;
  readonly onto: string;
  /** Who is doing the picking; defaults to the original commit's committer. */
  readonly author?: Signature;
  /** The ref to move on success; absent computes the replay and stops. */
  readonly into?: string;
  /**
   * What `into` held when the *caller* judged this write, if it judged one.
   *
   * The boundary decides whether a replay is a force push by asking what
   * `into` holds, and it asks before any of this runs. Read again here, the
   * swap compares against a value the boundary never saw — so a push landing
   * between the two turns a write that was judged a fast-forward into one that
   * drops commits, which is `source.force-push`'s to allow and not this
   * caller's. Handed in, the swap fails and the caller is told to look again.
   */
  readonly expected?: Oid | null;
}) {
  const repository = yield* Repository;
  const commit = yield* resolveCommit(input.commit);
  const onto = yield* resolveCommit(input.onto);
  // Read before the replay, as `rebase` does: a pick reads trees, merges them
  // and writes objects, and a push landing in that window is a real race. With
  // no `expected` the swap below is an unconditional write, so the pick would
  // silently overwrite whatever arrived.
  const intoWas =
    input.expected !== undefined
      ? input.expected
      : input.into === undefined
        ? null
        : yield* repository.resolve(input.into);

  const replayed = yield* replayOne({ commit, onto, author: input.author });

  return yield* settle(
    input.into === undefined
      ? { onto, commits: [replayed] }
      : { onto, commits: [replayed], into: input.into, expected: intoWas },
  );
});

/** Every commit `onto` already contains — the walk's stop condition. */
const ancestryOf = Effect.fn("Rebase.ancestry")(function* (onto: Oid) {
  const repository = yield* Repository;
  const seen = new Set<Oid>();
  const stack = [onto];
  while (stack.length > 0) {
    const oid = stack.pop()!;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const commit = yield* repository.readCommit(oid).pipe(
      Effect.map((value): { readonly parents: ReadonlyArray<Oid> } | null => value),
      Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
    );
    if (commit !== null) stack.push(...commit.parents);
  }
  return seen;
});

/** Replay `branch`'s commits that are not in `onto`, in order. */
export const rebase = Effect.fn("Rebase.rebase")(function* (input: {
  /** A ref or an oid. */
  readonly branch: string;
  readonly onto: string;
  /** The ref to move on success; absent computes the replay and stops. */
  readonly into?: string;
  /** What `into` held when the caller judged this write; see `cherryPick`. */
  readonly expected?: Oid | null;
}) {
  const repository = yield* Repository;
  const branch = yield* resolveCommit(input.branch);
  const onto = yield* resolveCommit(input.onto);
  // Read before the replay, which takes as long as the history is deep: this
  // is what the ref move at the end compares against.
  const intoWas =
    input.expected !== undefined
      ? input.expected
      : input.into === undefined
        ? null
        : yield* repository.resolve(input.into);

  // `onto..branch`, oldest first — the merge base is where the two histories
  // parted, so everything after it on `branch` is what `onto` lacks. A branch
  // already contained in `onto` yields nothing here and settles as up-to-date.
  //
  // `firstParent` is asked for rather than inherited: a merge commit's side
  // branch is not replayed on its own, which is how `git rebase` flattens
  // unless asked to preserve merges. `log` walks every parent by default, and
  // that walk would replay the side branch's commits individually here.
  const base = yield* repository.mergeBase(branch, onto);
  // Everything `onto` already contains, so the walk stops at the merge base
  // wherever it lies. `takeWhile(oid !== base)` alone would only stop if the
  // base sat on this first-parent chain — and it does not on any branch that
  // has merged its upstream, so the walk ran to the root commit and replayed
  // the entire history.
  const contained = base === null ? new Set<Oid>() : yield* ancestryOf(onto);
  const history = yield* Stream.runCollect(
    repository
      .log(branch, { firstParent: true })
      .pipe(Stream.takeWhile((commit) => !contained.has(commit.oid))),
  );

  const commits: Replayed[] = [];
  let head = onto;
  for (const commit of history.toReversed()) {
    const replayed = yield* replayOne({ commit: commit.oid, onto: head });
    commits.push(replayed);
    // Stop at the first conflict: the commits after it were written against
    // this one's result, and replaying them onto anything else is a guess.
    if (replayed.conflicts.length > 0) break;
    if (replayed.replayed !== null) head = replayed.replayed;
  }

  return yield* settle(
    input.into === undefined
      ? { onto, commits }
      : { onto, commits, into: input.into, expected: intoWas },
  );
});
