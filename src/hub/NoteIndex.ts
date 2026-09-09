/**
 * A disposable path index over the note namespace.
 *
 * `git+ why <path>` runs before every edit, and answering it from the refs
 * alone means folding every note's event DAG to discover that most of them are
 * about other files. §29 allows a cache for exactly this, on one condition:
 * correctness may not depend on it.
 *
 * That condition is what the stored ref oids are for. A note's projection is a
 * pure function of the bytes its ref reaches, so an index that records the oid
 * every note ref held when it was built is trustworthy for as long as those
 * oids still match — and the ref store answers that question without reading a
 * single object. A mismatch is not repaired here and not reported as an error;
 * it simply falls back to the full fold, which is the authoritative answer the
 * cache was standing in for.
 *
 * The oid check covers every state this module can itself produce, since the
 * only writer is `refresh` and it builds from the fold. It does not cover an
 * index somebody hand-edited to name the wrong path while leaving the oids
 * alone — and it does not need to: this cache lives in the same object store
 * as the note refs it caches, so anybody able to rewrite it can rewrite the
 * authoritative records beside it. `select` re-checks every hit against the
 * projection regardless, so the worst such an index can do is hide a note
 * from a query, never invent one.
 *
 * It stores every note's path, active or retired, rather than §29's "active
 * note ids". The narrower map would be sound — retiring a note moves its ref,
 * which invalidates the index — but it would answer only one of the two reads
 * that want it, and a listing that quietly drops retired notes is a worse
 * failure than a fold that costs slightly more.
 */
import { Effect, Schema } from "effect";

import { Repository } from "../git/Repository.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import { type Oid, storageOf } from "../git/Store.ts";
import * as Note from "./Note.ts";
import { covers } from "./Note.ts";
import { project, type Projection } from "./NoteProjection.ts";

/** Local by default, like `Memory`'s: no refspec replicates `refs/notes/*`. */
export const INDEX_REF = "refs/notes/hub/note-index";

const ENTRY = "notes.json";
const BLOB_MODE = "100644";
const decoder = new TextDecoder();
const encoder = new TextEncoder();

const identity = {
  name: "chr33s-git",
  email: "chr33s-git@localhost",
  at: new Date(0),
  offset: 0,
};

/** The stored shape. JSON has no map, so the wire keeps objects. */
const Stored = Schema.Struct({
  version: Schema.Literal(1),
  /** Note id → the ref oid this index was built from. */
  refs: Schema.Record(Schema.String, Schema.String),
  /** Repository-relative path → the notes anchored to it. */
  paths: Schema.Record(Schema.String, Schema.Array(Schema.String)),
});
interface Stored extends Schema.Schema.Type<typeof Stored> {}

/**
 * The same index in memory, keyed by `Map` rather than by object property.
 *
 * Paths and note ids are attacker-chosen strings, and a plain object answers
 * some of them from `Object.prototype`: `paths["constructor"] ??= []` finds an
 * inherited function, declines to assign, and throws on `.push` — taking down
 * every note mutation with it. `refs["__proto__"] = oid` is dropped silently
 * instead, which leaves the index permanently reading as stale. A `Map` has no
 * prototype keys and neither failure exists.
 */
export interface Index {
  readonly version: 1;
  readonly refs: ReadonlyMap<string, string>;
  readonly paths: ReadonlyMap<string, ReadonlyArray<string>>;
}

const decodeStored = Schema.decodeUnknownEffect(Stored);

const held = (stored: Stored): Index => ({
  version: stored.version,
  refs: new Map(Object.entries(stored.refs)),
  paths: new Map(Object.entries(stored.paths)),
});

const stored = (index: Index): Stored => ({
  version: index.version,
  refs: Object.fromEntries(index.refs),
  paths: Object.fromEntries([...index.paths].map(([path, ids]) => [path, [...ids]])),
});

/** What every note ref holds right now — the state an index is checked against. */
export const state = Effect.fn("hub.NoteIndex.state")(function* () {
  const repository = yield* Repository;
  const found = new Map<string, Oid>();
  for (const [name, oid] of yield* repository.refs) {
    const id = Note.noteOf(name);
    if (id !== null) found.set(id, oid);
  }
  return found;
});

const current = (index: Index, refs: ReadonlyMap<string, Oid>): boolean => {
  if (index.refs.size !== refs.size) return false;
  for (const [id, oid] of index.refs) if (refs.get(id) !== oid) return false;
  return true;
};

/** The stored index, or `null` where there is none this replica can read. */
export const read = Effect.fn("hub.NoteIndex.read")(function* () {
  const repository = yield* Repository;
  const head = yield* repository.resolve(INDEX_REF);
  if (head === null) return null;

  // A cache that will not read is a cache that is not there — and "will not
  // read" covers more than "will not parse". A ref whose commit, tree or blob
  // is gone, which is what an object store pruned or restored under a live ref
  // leaves behind, used to fail the whole read path: `GET /hub/notes`,
  // `/hub/why`, `/hub/notes/check` and `git+ why` all took the error, over a
  // file this module's own docstring says correctness may not depend on.
  const bytes = yield* Effect.gen(function* () {
    const info = yield* repository.readCommit(head);
    const entry = yield* repository.findPath(info.tree, ENTRY);
    return entry === null ? null : yield* repository.readBlob(entry.oid);
  }).pipe(
    Effect.catchTags({
      ObjectNotFound: () => Effect.succeed(null),
      StorageFailure: () => Effect.succeed(null),
    }),
  );
  if (bytes === null) return null;

  const parsed: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => null,
  }).pipe(Effect.orElseSucceed(() => null));
  if (parsed === null) return null;
  const decoded = yield* decodeStored(parsed).pipe(Effect.orElseSucceed(() => null));
  return decoded === null ? null : held(decoded);
});

/** Fold every note and record where each one points. */
export const build = Effect.fn("hub.NoteIndex.build")(function* () {
  const notes = yield* state();
  // Where each note sat last time, so a note whose ref has not moved does not
  // have to be folded again to be told the same answer. A `note add` on a
  // repository with a thousand notes moved one ref and re-walked all thousand
  // event DAGs, verifying every signature on each — once per mutation.
  const previous = yield* read();
  const before = new Map<string, string>();
  if (previous !== null) {
    for (const [path, ids] of previous.paths) {
      for (const id of ids) before.set(id, path);
    }
  }

  const paths = new Map<string, Array<string>>();
  const refs = new Map<string, string>();
  const at = (path: string): Array<string> => {
    const found = paths.get(path);
    if (found !== undefined) return found;
    const made: Array<string> = [];
    paths.set(path, made);
    return made;
  };
  for (const [id, oid] of notes) {
    // Recorded for every note ref, including one that projects to nothing:
    // the check compares against the ref store, so a ref left out of `refs`
    // would make a freshly built index read as stale on the very next call.
    refs.set(id, oid);
    // Reused only where the ref is byte-for-byte the one that produced it,
    // which is the same evidence `select` trusts the whole index on.
    const unmoved = previous?.refs.get(id) === oid ? before.get(id) : undefined;
    if (unmoved !== undefined) {
      at(unmoved).push(id);
      continue;
    }
    const note = yield* project(id);
    if (note !== null) at(note.path).push(id);
  }
  return { version: 1, refs, paths } satisfies Index;
});

/** Rebuild and store it. Never called on a read path. */
export const refresh = Effect.fn("hub.NoteIndex.refresh")(function* () {
  const repository = yield* Repository;
  const index = yield* build();

  const blob = yield* repository.writeBlob(encoder.encode(`${JSON.stringify(stored(index))}\n`));
  const tree = yield* repository.writeTree([{ mode: BLOB_MODE, name: ENTRY, oid: blob }]);
  const head = yield* repository.readRef(INDEX_REF);
  const commit = yield* repository.commitTree({
    tree,
    // No parent: this is a cache, not a record, and keeping its history would
    // pin every superseded copy out of reach of collection for good.
    parents: [],
    message: "note-index\n",
    author: identity,
  });
  yield* repository.setRef({ name: INDEX_REF, to: commit, expected: head });
  return index;
});

/**
 * The notes a query could be about, folded.
 *
 * With a current index only the matching notes are folded; without one the
 * answer is identical and costs the full walk. Nothing here writes, so a stale
 * index stays stale until a mutation refreshes it.
 */
/**
 * Projections already folded, one entry per repository.
 *
 * The stored index makes a *scoped* read cheap; this makes a repeated one
 * cheap, which is the shape the API sees. `GET /hub/why` is charged only
 * `repo.read`, so on a public repository an anonymous client can ask it in a
 * loop — and each ask re-walked every note's event DAG and re-verified every
 * signature on it. That is the same anonymous-driveable walk `Redaction`'s
 * memo exists to remove, so this is the same memo: keyed by the repository and
 * *validated* against the ref values the answer was folded from, so a moved
 * ref is a miss and a stale answer is impossible.
 *
 * Least-recently-used, and a whole repository at a time: evicting one note's
 * projection would leave an entry that has to be re-validated ref by ref,
 * which is the cost this removes.
 */
const MEMO = 64;
const folded = new Map<
  string,
  { readonly state: string; readonly notes: ReadonlyArray<Projection> }
>();

/** The ref values an answer was folded from, as one comparable string. */
const stamp = (held: ReadonlyMap<string, Oid>): string =>
  [...held]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([id, oid]) => `${id} ${oid}`)
    .join("\u0000");

const all = Effect.fn("hub.NoteIndex.all")(function* (held: ReadonlyMap<string, Oid>) {
  const repository = yield* Repository;
  // The repository's own identity, not the note refs: a mirror and its origin
  // under one host hold the same refs and need not be able to read the same
  // objects. See `Redaction`'s memo, which keys the same way for the same
  // reason.
  const identity = `${yield* storageOf()}\u0000${yield* repository.resolve(GENESIS_REF)}`;
  const state = stamp(held);

  const known = folded.get(identity);
  if (known !== undefined && known.state === state) {
    folded.delete(identity);
    folded.set(identity, known);
    return known.notes;
  }

  const notes: Projection[] = [];
  for (const id of [...held.keys()].sort()) {
    const note = yield* project(id);
    if (note !== null) notes.push(note);
  }
  folded.delete(identity);
  folded.set(identity, { state, notes });
  for (const oldest of folded.keys()) {
    if (folded.size <= MEMO) break;
    folded.delete(oldest);
  }
  return notes;
});

/**
 * Every note a scoped read must consider, which is more than the ones whose
 * stored path matches.
 *
 * `present` is the set of paths the source holds now, when a caller has one.
 * A note whose stored path is not among them may have been renamed into the
 * query's scope, and dropping it here is what left `git+ why <new-path>`
 * answering nothing for a constraint that had simply moved. The audit follows
 * the rename and settles it; this only has to avoid deciding first.
 */
export const select = Effect.fn("hub.NoteIndex.select")(function* (
  query?: string,
  present?: ReadonlySet<string>,
) {
  const held = yield* state();
  const index = yield* read();

  const wanted = (path: string) =>
    covers(query, path) || (present !== undefined && !present.has(path));

  // Nothing to narrow, or nothing to narrow *with*: fold the lot, through the
  // memo, so a repeated ask does not repeat the walk. The unscoped read is the
  // one an anonymous client can drive in a loop, and a stale index used to
  // send the scoped read down the same path without the memo.
  const scoped = query !== undefined && query !== "";
  if (!scoped || index === null || !current(index, held)) {
    const notes = yield* all(held);
    return scoped ? notes.filter((note) => wanted(note.path)) : notes;
  }

  const ids = [...index.paths.entries()]
    .filter(([path]) => wanted(path))
    .flatMap(([, notes]) => notes);

  const projected: Projection[] = [];
  for (const id of ids.sort()) {
    const note = yield* project(id);
    // Filtered again against the projection: the index says where a note was
    // anchored, and the fold says where it is anchored. Only the second is
    // authoritative, so a hit that no longer matches is dropped here.
    if (note !== null && wanted(note.path)) projected.push(note);
  }
  return projected;
});

/** Forget every memoised fold; for a test that needs to count the walks. */
export const forget = (): void => {
  folded.clear();
};
