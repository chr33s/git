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
import { type Genesis, readGenesis } from "../trust/Genesis.ts";
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
  for (const pr of only ?? (yield* tombstoned())) {
    const state = yield* project(genesis, trust, pr);
    if (state.redacted.size === 0) continue;

    // By commit, not by event id: an id belongs to its author here, and a
    // tombstone matched by bare id would take every same-id event with it —
    // which is how somebody holding only `hub.comment` could have an approval's
    // payload deleted along with their own duplicate.
    for (const entry of state.redacted) {
      // The blob by the name its own tree gives it. A tombstone whose target
      // has already been collected finds nothing, which is the state it was
      // asking for.
      const info = yield* repository.readCommit(entry);
      const path = yield* repository.findPath(info.tree, `${Event.RECORD}.json`);
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

  // Walked before anything is folded, and the trust log is not folded at all
  // unless a tombstone turned up. A fold verifies a signature per record and
  // per event, and this runs on every collection and on every deepening fetch
  // — which anonymous readers can ask for repeatedly. Any *valid* tombstone is
  // first of all a decoded `event.redacted` payload, so a walk that finds none
  // rules the repository out without a single verification. The filter can
  // produce a false positive, which costs one fold; it cannot produce a false
  // negative, which is what would matter.
  const candidates = yield* tombstoned();
  if (candidates.length === 0) return new Set<Oid>();

  // And once found, remembered against the state it was computed from: the
  // answer is a pure function of the hub refs and the trust head, so a moved
  // ref changes the key and a stale one is not possible.
  const key = [
    yield* repository.resolve(LOG_REF),
    ...(yield* Effect.forEach(candidates, (pr) => repository.resolve(Event.refOf(pr)))),
  ].join("\u0000");
  const known = memo.get(key);
  if (known !== undefined) return known;

  const stored = yield* readGenesis();
  if (stored === null) return new Set<Oid>();
  const found = yield* blobs(stored.genesis, yield* projectTrust(stored.genesis), candidates);

  memo.set(key, found);
  while (memo.size > MEMO) {
    const oldest = memo.keys().next();
    if (oldest.done === true) break;
    memo.delete(oldest.value);
  }
  return found;
});

/** The pull requests carrying anything that *might* be a tombstone. */
const tombstoned = Effect.fn("hub.Redaction.tombstoned")(function* () {
  const found: string[] = [];
  for (const pr of yield* Event.pullRequests()) {
    const { events } = yield* Event.entries(pr);
    if (events.some((entry) => entry.payload?.type === "event.redacted")) found.push(pr);
  }
  return found;
});

/**
 * Answers already worked out, by the refs they were worked out from.
 *
 * Small, because it only ever holds repositories that have actually redacted
 * something and only one entry per state of each.
 */
const MEMO = 32;
const memo = new Map<string, ReadonlySet<Oid>>();

export type RedactionError = Invalid | ObjectNotFound | StorageFailure;
