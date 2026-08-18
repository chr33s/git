/**
 * Which blobs a redaction tombstone has taken out of circulation.
 *
 * A tombstone is a statement, and deleting the loose copy of the blob it names
 * is only half of acting on it. Git stores objects twice over — loose, and
 * inside packs — and a pack cannot give up one object without being rewritten,
 * so `deleteObject` on a packed blob is a no-op that returns success. A
 * redaction that reported success while the payload stayed clonable would be
 * the worst possible shape for this feature: the operator believes the
 * credentials are gone, and they are still being served.
 *
 * So the removal is finished where pack rewriting already happens — in `gc`.
 * §21 and §27 of the spec say exactly this: a blob covered by a valid
 * tombstone is excluded from reachability protection and pruned. This module
 * computes "covered by a valid tombstone", and `gc` takes it as the set of
 * objects the walk must not protect.
 *
 * "Valid" is load-bearing and is why this reads the projection rather than the
 * raw events. A tombstone only counts if it decoded, if its signer held
 * `hub.redact`, and if that signer was authorized when they signed it —
 * otherwise anybody who can push would be able to delete another member's
 * review by naming it.
 */
import { Effect } from "effect";

import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { GENESIS_REF, type Genesis, readGenesis } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import {
  project as projectTrust,
  type Projection as TrustProjection,
} from "../trust/Projection.ts";
import * as Event from "./Event.ts";
import { project } from "./Projection.ts";

/**
 * Every payload blob a valid tombstone covers, across every pull request.
 *
 * Returned as oids rather than deleted here, because whether an object can
 * actually be removed depends on whether it is packed — a question `gc` owns
 * and this module has no business answering.
 */
export const blobs = Effect.fn("hub.Redaction.blobs")(function* (
  genesis: Genesis,
  trust: TrustProjection,
  only?: ReadonlyArray<string>,
) {
  const repository = yield* Repository;

  const found = new Set<Oid>();
  for (const pr of only ?? (yield* tombstoned(yield* Event.pullRequests()))) {
    // One pull request this host cannot fold is one pull request. `tombstoned`
    // above already treats an unwalkable history that way, and leaving this
    // one unguarded put the whole repository's collection behind a single
    // pull request — including the purge that is how a redacted payload
    // actually leaves the object store.
    const state = yield* project(genesis, trust, pr).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (state === null || state.redacted.size === 0) continue;

    // By commit, not by event id: an id belongs to its author here, and a
    // tombstone matched by bare id would take every same-id event with it —
    // which is how somebody holding only `hub.comment` could have an approval's
    // payload deleted along with their own duplicate.
    for (const entry of state.redacted) {
      // The blob by the name its own tree gives it. A tombstone whose target
      // has already been collected finds nothing, which is the state it was
      // asking for.
      const info = yield* repository
        .readCommit(entry)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
      if (info === null) continue;
      // The tree too. Refs are applied without a connectivity check, so a
      // replica can hold a commit whose tree never arrived — and this is `gc`,
      // which must not stop collecting a whole repository over one object it
      // was going to be told to drop anyway.
      const path = yield* repository
        .findPath(info.tree, `${Event.RECORD}.json`)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
      if (path !== null) found.add(path.oid);
    }
  }
  return found;
});

/**
 * The same set, read from the repository, for a caller that has no trust state
 * in hand.
 *
 * `gc` runs on every repository and most of them have no genesis, so "nothing
 * is redacted" is the ordinary answer rather than an error. Both `gc` entry
 * points want exactly this one line.
 */
export const excluded = Effect.fn("hub.Redaction.excluded")(function* () {
  const repository = yield* Repository;

  // The memo is consulted *before* the walk it exists to save. Keyed on
  // anything the walk produced, it saved only the fold — and the walk reads
  // every event of every pull request, on a path (a deepening fetch) an
  // anonymous reader can drive in a loop. Ref names and their oids are what a
  // ref store already has, and the answer is a pure function of them, so a
  // moved ref changes the key and a stale answer is not possible.
  const refs = yield* Event.pullRequests();
  const identity = yield* repository.resolve(GENESIS_REF);
  // Names as well as oids. A repository with no genesis keys under `null`
  // along with every other one, so the state is all that separates them — and
  // oids alone made a fork and its parent, whose pull requests point at the
  // same commits under different names, one entry.
  // The repository's own identity keys the entry. Left out, the key was the
  // hub and trust refs alone: two repositories on one host whose heads happen
  // to match — the same trust log under a *different* genesis, which is a
  // different membership and so a different answer — shared an entry, and one
  // of them collected with an exclusion set computed for the other.
  const state = [
    // The ceiling is part of the answer, not of the repository: a host that
    // will not walk a pull request that size reports nothing redacted in it,
    // so two hosts with the same refs and different ceilings hold two
    // different answers under what would otherwise be one key.
    `ceiling ${yield* Event.ceilingOf()}`,
    yield* repository.resolve(LOG_REF),
    ...(yield* Effect.forEach(refs, (pr) =>
      repository.resolve(Event.refOf(pr)).pipe(Effect.map((oid) => `${Event.refOf(pr)} ${oid}`)),
    )),
  ].join("\u0000");

  const known = memo.get(identity);
  if (known !== undefined && known.state === state) {
    // Re-inserted so iteration order is least-recently-used first.
    memo.delete(identity);
    memo.set(identity, known);
    return known.found;
  }

  // Walked before anything is folded, and the trust log is not folded at all
  // unless a tombstone turned up. A fold verifies a signature per record and
  // per event; any *valid* tombstone is first of all a decoded `event.redacted`
  // payload, so a walk that finds none rules the repository out without a
  // single verification. The filter can produce a false positive, which costs
  // one fold; it cannot produce a false negative, which is what would matter.
  const candidates = yield* tombstoned(refs);
  const stored = candidates.length === 0 ? null : yield* readGenesis();
  const found =
    stored === null
      ? new Set<Oid>()
      : yield* blobs(stored.genesis, yield* projectTrust(stored.genesis), candidates);

  memo.delete(identity);
  memo.set(identity, { state, found });
  while (memo.size > MEMO) {
    const oldest = memo.keys().next();
    if (oldest.done === true) break;
    memo.delete(oldest.value);
  }
  return found;
});

/**
 * Every payload blob a tombstone *names*, whether or not it still counts.
 *
 * A different question from `excluded`, and the one a fetch asks: "is this
 * absence explained?" rather than "may this be removed?". Authorization
 * decides whether a removal happens; it must not decide whether a removal that
 * already happened is explicable, because the removal is irreversible and the
 * answer moves. `Verify.authorize` judges expiry against the clock, and a
 * `compromised` revocation reaches backwards — so a tombstone valid on Monday
 * can be invalid on Friday, and with the strict set the bytes stay gone while
 * nothing accounts for them: every fetch of `refs/hub/*` fails from then on,
 * permanently, and two hosts whose memos turned over at different moments
 * disagree about the same request.
 */
export const covered = Effect.fn("hub.Redaction.covered")(function* () {
  const repository = yield* Repository;

  // Memoised for the same reason `excluded` is, and more urgently: this is the
  // set a *deepening fetch* takes, and a deepening fetch is a request an
  // anonymous reader makes. Unmemoised, every one of them walked every pull
  // request's event DAG twice — once to find the tombstones and once to read
  // them — which is a walk of the whole hub history per request, driveable in
  // a loop by anybody who can reach the repository.
  //
  // Sound because the answer is a pure function of the refs in the key: this
  // set asks only what a tombstone *names*, so no trust state and no clock
  // enters it, and a moved ref changes the key.
  const refs = yield* Event.pullRequests();
  const identity = yield* repository.resolve(GENESIS_REF);
  const state = [
    // As above: the ceiling decides which pull requests were walked at all.
    `ceiling ${yield* Event.ceilingOf()}`,
    ...(yield* Effect.forEach(refs, (pr) =>
      repository.resolve(Event.refOf(pr)).pipe(Effect.map((oid) => `${Event.refOf(pr)} ${oid}`)),
    )),
  ].join("\u0000");

  const known = names.get(identity);
  if (known !== undefined && known.state === state) {
    names.delete(identity);
    names.set(identity, known);
    return known.found;
  }

  const found = new Set<Oid>();
  for (const pr of yield* tombstoned(refs)) {
    const { events } = yield* Event.entries(pr);
    for (const entry of events) {
      if (entry.payload?.type !== "event.redacted" || entry.payload.pr !== pr) continue;
      const target = Event.unqualify(entry.payload.targetCommit);
      if (target === null) continue;
      const info = yield* repository
        .readCommit(target)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
      if (info === null) continue;
      // As above, and here it matters more: this set is what a *fetch* takes,
      // so a failure is every deepening fetch of the repository failing over a
      // tombstone whose target is exactly the object that did not arrive.
      const path = yield* repository
        .findPath(info.tree, `${Event.RECORD}.json`)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
      if (path !== null) found.add(path.oid);
    }
  }

  names.delete(identity);
  names.set(identity, { state, found });
  while (names.size > MEMO) {
    const oldest = names.keys().next();
    if (oldest.done === true) break;
    names.delete(oldest.value);
  }
  return found;
});

/**
 * What a tombstone covers *and this repository no longer holds*.
 *
 * The exclusion `gc` takes says "stop protecting this"; this one says "this is
 * not here, walk past it". They are not the same set, because git dedupes by
 * content: a redacted payload can be the very object a branch's tree names,
 * and `gc` keeps that one. Handing the whole set to a pack walk would drop an
 * object the pack genuinely needs, and the other end would rebuild a tree
 * pointing at nothing.
 *
 * Both ends of a transfer need it — the server serving a fetch, and a client
 * packing a hub ref for a push — so it lives beside the set it filters rather
 * than in either of them.
 */
export const absent = Effect.fn("hub.Redaction.absent")(function* () {
  const repository = yield* Repository;
  // Every payload a tombstone *names*, not only those a tombstone that still
  // counts names; see `covered`.
  const gone = new Set<Oid>();
  for (const oid of yield* covered()) if (!(yield* repository.contains(oid))) gone.add(oid);
  return gone;
});

/** The pull requests carrying anything that *might* be a tombstone. */
const tombstoned = Effect.fn("hub.Redaction.tombstoned")(function* (refs: ReadonlyArray<string>) {
  const found: string[] = [];
  for (const pr of refs) {
    // One pull request this cannot walk is one pull request, not a broken
    // repository. The fold's ceiling is enforced where a push crosses it, so a
    // history that arrived by replication may sit above it — and failing here
    // would take out `gc` for every repository on the host and every deepening
    // fetch of this one, on a walk that visits pull requests with nothing
    // redacted in them at all.
    const walked = yield* Event.entries(pr).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (walked === null) continue;
    if (walked.events.some((entry) => entry.payload?.type === "event.redacted")) found.push(pr);
  }
  return found;
});

/**
 * Answers already worked out, one per repository.
 *
 * Keyed by the repository and *validated* against the refs the answer was
 * worked out from, rather than keyed by those refs. Keyed by them, every hub
 * event minted a new entry: one busy repository filled the whole map with its
 * own history and evicted every other repository the host serves, reinstating
 * the walk-every-pull-request cost this exists to remove — the same defect
 * `Auth.folds` was reshaped to avoid, for the same reason. Nothing ever wants
 * two answers for one repository, so a moved ref replaces rather than adds.
 *
 * Least-recently-used, not first-in: a host with more repositories than the
 * bound serves them in some order, and evicting by insertion evicts whichever
 * happens to be oldest rather than whichever is idle.
 */
const MEMO = 256;
type Memo = Map<Oid | null, { readonly state: string; readonly found: ReadonlySet<Oid> }>;
const memo: Memo = new Map();

/**
 * The same, for `covered`.
 *
 * A separate map rather than a wider value, because the two are asked on
 * different paths — `excluded` by `gc`, `covered` by every deepening fetch —
 * and sharing one entry would make either question pay for the other's walk.
 */
const names: Memo = new Map();

export type RedactionError = Invalid | ObjectNotFound | StorageFailure;
