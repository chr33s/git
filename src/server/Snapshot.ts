/**
 * The read path's published view of a repository: its refs, as one small
 * object stored beside the objects and packs they name.
 *
 * On the Cloudflare host every request funnels through the repository's
 * Durable Object — the single writer that makes ref transactions trivially
 * linearizable. That is the right door for writes and the wrong bottleneck
 * for reads: a clone needs the refs (here) and the objects (already in R2),
 * and nothing about serving either requires waking the writer. So the host
 * publishes this snapshot to R2 *synchronously on every mutating request,
 * before acknowledging it* — the same "never acknowledge until persisted"
 * discipline a write-ahead log keeps — and the front Worker serves anonymous
 * `git-upload-pack` traffic (the advertisement and the pack) statelessly
 * from R2 alone, falling back to the Durable Object whenever the snapshot is
 * absent, unreadable, restricted, or the request is anything but an
 * anonymous read. R2 reads are strongly consistent, so a push acknowledged
 * through the writer is visible to the very next stateless read.
 *
 * Authorization is decided when the snapshot is written, where the trust
 * state lives: `anonymousRead` is `Auth.anonymousReadAllowed` over the trust
 * projection, re-judged whenever the trust refs move and carried forward
 * otherwise. A repository that restricts reading publishes `false` and the
 * stateless path steps aside — the Durable Object's guard, which can issue
 * and judge challenges, remains the only door that ever sees a credential.
 *
 * One window is accepted and named: `gc` runs in the Durable Object and
 * tracks its own in-flight deliveries, but a stateless read it cannot see
 * could still be streaming objects a concurrent collection deletes. The same
 * window exists for any CDN-cached pack anywhere; a client that hits it
 * retries against a server that has already converged.
 */
import { DateTime, Effect, Layer, Result, Schema } from "effect";

import type { GitError, Invalid } from "../git/Error.ts";
import { StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid, type RefUpdateResult, RefStore } from "../git/Store.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { anonymousReadAllowed } from "./Auth.ts";
import * as Protocol from "./Protocol.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Where a repository's snapshot lives, beside its `objects/` and `pack/`. */
export const keyOf = (repo: string): string => `${repo}/meta/refs-snapshot.json`;

const SnapshotRef = Schema.Struct({ name: Schema.String, oid: Schema.String });

export const Published = Schema.Struct({
  version: Schema.Literal(1),
  publishedAt: Schema.String,
  /** What `HEAD` points at — the symref target, not an oid. */
  head: Schema.String,
  /** Whether a reader with no credential may clone, judged at publish time. */
  anonymousRead: Schema.Boolean,
  /** Every ref, oid-valued; the protocol's own hiding rules apply at serve time. */
  refs: Schema.Array(SnapshotRef),
});
export type Published = (typeof Published)["Type"];

const decodePublished = Schema.decodeUnknownResult(Published);

export const encode = (snapshot: Published): Uint8Array =>
  encoder.encode(`${JSON.stringify(snapshot, null, 2)}\n`);

/** `null` for bytes that are not a snapshot — the caller falls back, never throws. */
export const decode = (bytes: Uint8Array): Published | null => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(bytes));
  } catch {
    return null;
  }
  const result = decodePublished(parsed);
  return Result.isSuccess(result) ? result.success : null;
};

/** Whether two captures publish the same view; `publishedAt` says nothing. */
export const same = (left: Published, right: Published): boolean =>
  left.head === right.head &&
  left.anonymousRead === right.anonymousRead &&
  left.refs.length === right.refs.length &&
  left.refs.every(
    (ref, index) => right.refs[index]?.name === ref.name && right.refs[index]?.oid === ref.oid,
  );

const TRUST_PREFIX = "refs/meta/trust/";

const trustView = (refs: ReadonlyArray<{ readonly name: string; readonly oid: string }>): string =>
  refs
    .filter((ref) => ref.name.startsWith(TRUST_PREFIX))
    .map((ref) => `${ref.name} ${ref.oid}`)
    .join("\n");

/**
 * Whether a reader with no credential may clone this repository — the same
 * judgment the guard makes, asked of the trust state at publish time. A
 * repository with no identity is a plain public repository.
 */
const readability = Effect.fn("Snapshot.readability")(function* () {
  const stored = yield* readGenesis();
  if (stored === null) return true;
  const projection = yield* projectTrust(stored.genesis);
  return anonymousReadAllowed(projection);
});

/**
 * The current refs and `HEAD` as one publishable value.
 *
 * `previous` is the last capture this host published: when the trust refs
 * have not moved, its readability verdict is carried forward instead of
 * re-projecting the trust log on every ref write — the one part of a capture
 * whose cost grows with the repository's history rather than its ref count.
 */
export const capture = Effect.fn("Snapshot.capture")(function* (previous?: Published) {
  const repository = yield* Repository;
  const listed = yield* repository.refs;
  const refs = listed.map(([name, oid]) => ({ name, oid }));
  const head = yield* repository.head;
  const anonymousRead =
    previous !== undefined && trustView(previous.refs) === trustView(refs)
      ? previous.anonymousRead
      : yield* readability();
  return {
    version: 1 as const,
    publishedAt: DateTime.formatIso(yield* DateTime.now),
    head,
    anonymousRead,
    refs,
  };
});

const readOnly = (operation: string) =>
  Effect.fail(
    new StorageFailure({
      operation,
      path: "refs-snapshot",
      cause: "a snapshot is an observation; refs move through the repository's writer",
    }),
  );

/**
 * The snapshot as a `RefStore` — reads answered from the published value,
 * writes refused: the stateless path serves fetches and nothing else, and a
 * write that somehow reached it must fail loudly rather than fork history
 * outside the writer's transaction.
 */
export const refStore = (snapshot: Published): Layer.Layer<RefStore> => {
  const byName = new Map<string, Oid>();
  for (const ref of snapshot.refs) {
    if (isOid(ref.oid)) byName.set(ref.name, ref.oid);
  }
  return Layer.succeed(RefStore)({
    read: (name) => Effect.succeed(byName.get(name) ?? null),
    resolve: (name) => Effect.succeed(byName.get(name === "HEAD" ? snapshot.head : name) ?? null),
    list: (prefix) =>
      Effect.succeed(
        [...byName.entries()]
          .filter(([name]) => prefix === undefined || name.startsWith(prefix))
          .sort(([left], [right]) => (left < right ? -1 : 1))
          .map(([name, oid]) => [name, oid] as const),
      ),
    apply: (updates): Effect.Effect<ReadonlyArray<RefUpdateResult>, StorageFailure> =>
      readOnly(`apply ${updates.map((update) => update.name).join(" ")}`),
    head: Effect.succeed(snapshot.head),
    setHead: () => readOnly("setHead"),
    // Honest emptiness: the snapshot observes where refs are, not where they
    // have been; nothing on the read path asks.
    reflog: () => Effect.succeed([]),
    logged: Effect.succeed([]),
  });
};

/**
 * Whether this request is one the stateless path may answer: an anonymous
 * `git-upload-pack` conversation — the advertisement, or the pack request
 * itself (protocol v2's `ls-refs` arrives as the latter). Anything carrying
 * a credential goes to the writer, which can actually judge it; every write
 * already belongs there.
 */
export const readable = (request: Request): boolean => {
  if (request.headers.get("authorization") !== null) return false;
  const url = new URL(request.url);
  const segments = url.pathname.split("/").filter((segment) => segment !== "");
  const last = segments.at(-1);
  if (request.method === "GET" && last === "refs" && segments.at(-2) === "info") {
    return url.searchParams.get("service") === "git-upload-pack";
  }
  return request.method === "POST" && last === "git-upload-pack";
};

/**
 * Serve one read from the snapshot's view, or `null` for a request this path
 * does not answer. The caller provides `Repository` built over the snapshot
 * ref store and the shared object store — the same `Protocol` the writer
 * runs, over the same bytes, minus the writer.
 */
export const serve = (request: Request): Effect.Effect<Response | null, GitError, Repository> =>
  readable(request) ? Protocol.handle(request) : Effect.succeed(null);

// -- the journal ----------------------------------------------------------------

/**
 * The journal: every state the refs have been in, appended beside the
 * `latest` pointer the read path serves from.
 *
 * The latest snapshot answers "where are the refs now"; the journal answers
 * "where have they been" — which is the question every recovery starts
 * with. Each entry carries the *whole* refs view (a repository's ref list
 * is small; its history is what is large) plus the delta from the entry
 * before it, so an operator can read what a push did without diffing, and
 * `restore` can rebuild a ref store from any retained point without
 * replaying anything. Entries are sequenced with zero-padded keys so the
 * store lists them in order, and retention is a single keyed delete per
 * append — the entry that just fell off the window — never a listing.
 *
 * Objects are content-addressed and never rewritten, so a journal entry
 * whose objects still exist (retention inside the `gc` horizon) names a
 * fully working tree of states: refs from here, bytes from the store.
 */
export const RETAIN = 256;

const SEQ_WIDTH = 10;

export const journalKeyOf = (repo: string, seq: number): string =>
  `${repo}/meta/journal/${String(seq).padStart(SEQ_WIDTH, "0")}.json`;

/** One ref's movement between two captures; `null` is absence on that side. */
const Change = Schema.Struct({
  name: Schema.String,
  from: Schema.NullOr(Schema.String),
  to: Schema.NullOr(Schema.String),
});

export const JournalEntry = Schema.Struct({
  ...Published.fields,
  /** This entry's position in the journal — the suffix of its own key. */
  seq: Schema.Int,
  /** What moved since the previous entry, in the order refs list. */
  changes: Schema.Array(Change),
});
export type JournalEntry = (typeof JournalEntry)["Type"];

const decodeEntry = Schema.decodeUnknownResult(JournalEntry);

export const encodeJournal = (entry: JournalEntry): Uint8Array =>
  encoder.encode(`${JSON.stringify(entry, null, 2)}\n`);

/** `null` for bytes that are not a journal entry, like `decode` above. */
export const decodeJournal = (bytes: Uint8Array): JournalEntry | null => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(bytes));
  } catch {
    return null;
  }
  const result = decodeEntry(parsed);
  return Result.isSuccess(result) ? result.success : null;
};

/**
 * A capture as the journal's next entry: the same view, stamped with its
 * sequence and the movement since `previous` — additions, moves, and
 * deletions, each named once.
 */
export const entryOf = (
  seq: number,
  captured: Published,
  previous: Published | undefined,
): JournalEntry => {
  const before = new Map((previous?.refs ?? []).map((ref) => [ref.name, ref.oid]));
  const changes: Array<{ name: string; from: string | null; to: string | null }> = [];
  for (const ref of captured.refs) {
    const from = before.get(ref.name) ?? null;
    if (from !== ref.oid) changes.push({ name: ref.name, from, to: ref.oid });
    before.delete(ref.name);
  }
  for (const [name, from] of before) changes.push({ name, from, to: null });
  return { ...captured, seq, changes };
};

/**
 * Rebuild a ref store from one journal entry — refs and `HEAD`, exactly as
 * the entry observed them. The store this writes into is the *caller's*
 * choice, and an empty one is the honest starting point: restoration is a
 * statement about what was, not a merge with what is.
 */
export const restore = Effect.fn("Snapshot.restore")(function* (
  entry: JournalEntry,
): Effect.fn.Return<void, StorageFailure | Invalid, RefStore> {
  const refs = yield* RefStore;
  const updates = entry.refs.flatMap((ref) =>
    isOid(ref.oid) ? [{ name: ref.name, value: ref.oid, reason: "journal restore" }] : [],
  );
  yield* refs.apply(updates);
  yield* refs.setHead(entry.head);
});
