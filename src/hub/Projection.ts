/**
 * A pull request's events folded into its current state.
 *
 * The events are the truth and this is a derivation, so it is disposable: any
 * replica holding the same events reaches the same state, and a cache of it
 * can be thrown away without losing anything.
 *
 * Two things make that "same state" claim hold rather than merely sound good.
 * The walk is deterministic (`git/Dag.ts`), and where determinism is not
 * enough — two authors updating the head at the same time, neither having seen
 * the other — the tie is broken by the greater event id. UUIDv7 makes that
 * approximately "the later one" while staying a pure function of the events,
 * which a timestamp comparison could never be.
 *
 * Verification happens here too. An event only counts if its signer was
 * authorized *when they made it*, which is why every event carries the trust
 * head it was written against: a review signed before its author could see
 * their own revocation is still a review this repository authorized, and one
 * signed afterwards is not. Events that fail are kept in `rejected` rather
 * than dropped — "why is my approval not counting?" is the first question
 * anybody asks.
 */
import { Effect } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import type { Oid } from "../git/Store.ts";
import type { Genesis } from "../trust/Genesis.ts";
import type { Projection as TrustProjection } from "../trust/Projection.ts";
import * as Log from "../trust/Log.ts";
import * as Verify from "../trust/Verify.ts";
import * as Event from "./Event.ts";

export interface Review {
  readonly id: string;
  readonly author: Fingerprint;
  /** The exact revision reviewed. An approval is of a revision, never of a PR. */
  readonly head: Oid;
  readonly decision: "approve" | "reject" | "comment";
  readonly body: string;
  readonly at: Date;
  readonly dismissed: boolean;
  /**
   * Whether the revision reviewed is still the one proposed.
   *
   * A stale approval is not an invalid one — it remains a true statement about
   * the revision it named — so it is marked rather than removed, and the merge
   * policy is what decides that stale approvals do not count.
   */
  readonly stale: boolean;
}

export interface Comment {
  readonly id: string;
  readonly author: Fingerprint;
  readonly body: string;
  readonly at: Date;
  readonly redacted: boolean;
}

export interface Thread {
  readonly id: string;
  readonly path: string | null;
  readonly side: "old" | "new" | null;
  readonly line: number | null;
  readonly head: Oid | null;
  readonly resolved: boolean;
  readonly comments: ReadonlyArray<Comment>;
}

export interface Check {
  readonly name: string;
  readonly provider: string;
  readonly head: Oid;
  readonly status: "started" | "success" | "failure" | "neutral";
  readonly url: string | null;
  readonly at: Date;
  readonly author: Fingerprint;
}

export interface Rejected {
  readonly commit: Oid;
  readonly reason: string;
}

export interface PullRequest {
  readonly id: string;
  readonly title: string;
  readonly description: string;
  readonly base: string;
  /** Derived from the event history; there is no mutable head ref. */
  readonly head: Oid | null;
  readonly state: "open" | "closed" | "merged";
  readonly author: Fingerprint | null;
  readonly mergeCommit: Oid | null;
  readonly reviews: ReadonlyArray<Review>;
  readonly threads: ReadonlyArray<Thread>;
  readonly checks: ReadonlyArray<Check>;
  /**
   * Which commits claimed each event id, among the events that counted.
   *
   * Exposed because a caller naming an id — `redact` is the one — has to
   * resolve it exactly as the fold does. Resolving it independently, over the
   * raw walk, meant a rejected event could be picked as the target while the
   * projection redacted a different one.
   */
  readonly claims: ReadonlyMap<string, ReadonlyArray<Oid>>;
  /**
   * Everybody who claimed to have opened this pull request.
   *
   * `author` alone is not enough to exclude self-approval: a contested opening
   * deliberately establishes no author, and an approval from either claimant
   * is still somebody approving their own proposal.
   */
  readonly openers: ReadonlySet<Fingerprint>;
  /**
   * The commits whose payloads a valid tombstone reached.
   *
   * Commits rather than event ids, because an id is scoped to its author here
   * and a bare one can be claimed twice; a commit names exactly one event, so
   * a consumer deleting blobs cannot be aimed at somebody else's.
   */
  readonly redacted: ReadonlySet<Oid>;
  readonly rejected: ReadonlyArray<Rejected>;
  readonly at: Date;
}

/**
 * Which head-setting event wins.
 *
 * Causality first: an event that descends from the current one replaces it.
 * Where neither descends from the other the two are genuinely concurrent, and
 * the greater id wins — arbitrary, but identically arbitrary everywhere.
 */
const supersedes = (
  candidate: { readonly commit: Oid; readonly id: string },
  current: { readonly commit: Oid; readonly id: string } | null,
  ancestors: ReadonlyMap<Oid, ReadonlySet<Oid>>,
): boolean => {
  if (current === null) return true;
  if (ancestors.get(candidate.commit)?.has(current.commit) === true) return true;
  if (ancestors.get(current.commit)?.has(candidate.commit) === true) return false;
  return candidate.id > current.id;
};

/**
 * Ancestor sets for every commit in the walked history.
 *
 * Built in topological order so each commit's set is the union of its parents'
 * — the parents are already done. Quadratic in the worst case, which is fine
 * for the size a pull request reaches and would not be for a source history;
 * this only ever walks `refs/hub/pr/<id>`.
 */
const ancestorSets = (
  parents: Dag.Parents,
  ordered: ReadonlyArray<Oid>,
): ReadonlyMap<Oid, ReadonlySet<Oid>> => {
  const sets = new Map<Oid, Set<Oid>>();
  for (const oid of ordered) {
    const set = new Set<Oid>();
    for (const parent of parents.get(oid) ?? []) {
      set.add(parent);
      for (const older of sets.get(parent) ?? []) set.add(older);
    }
    sets.set(oid, set);
  }
  return sets;
};

/** Which event opened a pull request, and whether anything competes to be it. */
interface Opening {
  readonly commit: Oid | null;
  readonly contested: boolean;
}

/**
 * An entry's key: its author and the id they chose.
 *
 * Composite because an id alone is chosen by whoever writes the event, so a
 * map keyed by it is a map any author can overwrite another author's entry in.
 * The `\0` cannot occur in a fingerprint or a UUID, so the two halves cannot
 * be made to run together.
 */
const keyOf = (signer: Fingerprint, id: string): string => `${signer}\u0000${id}`;

/**
 * The one entry an event id names, or nothing when it names none or several.
 *
 * Cross-references — a reply's thread, a dismissal's review, a tombstone's
 * target — are written as bare ids, because the author of the reference knows
 * the id and not who will have claimed it. Where two authors claim one id the
 * reference answers to neither: guessing would let a second claimant capture
 * the first one's replies, which is the eviction this keying exists to stop,
 * wearing another shape.
 */
const byEventId = <A extends { readonly id: string }>(
  entries: ReadonlyMap<string, A>,
  id: string,
): { readonly key: string; readonly value: A } | "ambiguous" | null => {
  let found: { readonly key: string; readonly value: A } | null = null;
  for (const [key, value] of entries) {
    if (value.id !== id) continue;
    if (found !== null) return "ambiguous";
    found = { key, value };
  }
  return found;
};

/**
 * The trust head an event recorded, as an oid.
 *
 * SAFETY: a trust head is a commit oid, and a value that is not one simply
 * fails to resolve — `Dag.ancestry` treats an unknown commit as unreachable
 * rather than as an error, so a malformed one denies rather than throws.
 */
const trustHeadOf = (value: string | null): Oid | null => value as Oid | null;

/**
 * "Is this trust-log commit at least as new as that one?", memoised per fold.
 *
 * `Log.ancestry` walks the log — a `readCommit` and a `findPath` per commit —
 * and the monotonicity check below asks about the same few heads over and over,
 * once per ancestor per event, on the write path. The answers are a pure
 * function of the log, which does not move while a projection is being built,
 * so one walk per distinct head is all that is ever needed.
 */
const trustReach = () => {
  const walked = new Map<Oid, ReadonlySet<Oid>>();

  /** The memo itself, which `Verify.authorize` takes so it walks the log once too. */
  const ancestry: Verify.Ancestry = Effect.fnUntraced(function* (head: Oid) {
    const seen = walked.get(head) ?? (yield* Log.ancestry(head));
    walked.set(head, seen);
    return seen;
  });

  /**
   * `null` — an event that recorded no trust head — reaches everything, which
   * is the same conservative reading `Verify.reaches` gives it: an author who
   * cannot show what they had seen is treated as having seen everything, and
   * here that means they cannot be behind their own ancestors either.
   */
  const reaches = Effect.fnUntraced(function* (head: Oid | null, target: Oid) {
    if (head === null || head === target) return true;
    return (yield* ancestry(head)).has(target);
  });

  /**
   * "Is this a trust-log commit at all?", memoised for the same reason.
   *
   * Asked once per accepted event, about the same handful of heads, and a
   * head cannot move while a projection is being built.
   */
  const held = new Map<Oid, boolean>();
  const contains = Effect.fnUntraced(function* (commit: Oid) {
    const known = held.get(commit) ?? (yield* Log.contains(commit));
    held.set(commit, known);
    return known;
  });

  return { ancestry, contains, reaches };
};

/**
 * Fold one pull request.
 *
 * `trust` is passed in rather than read here so that a caller projecting fifty
 * pull requests folds the membership log once.
 */
export const project = Effect.fn("hub.Projection.project")(function* (
  genesis: Genesis,
  trust: TrustProjection,
  pr: string,
) {
  const { events, parents } = yield* Event.entries(pr);
  const rejected: Rejected[] = [];

  /**
   * Which commit holds each event id, once it has earned the claim.
   *
   * Two commits claiming one id is a forgery or a corrupt replica, and the
   * question is which one to believe. Deciding it in `Event.entries` — before
   * any signature was checked — meant deciding it by commit order, whose
   * tie-break is the oid, which anybody who can write a hub ref can grind: a
   * member holding only `hub.comment` could re-use an approval's id, sort
   * first, and push the real approval out of the projection, taking a merge's
   * required approval with it.
   *
   * So the claim is recorded below, after `validate` and `authorize` have both
   * passed. An impostor without the capability the event type needs is refused
   * on its own merits and never reaches the map, and a second claim from
   * somebody who *does* hold it is refused as the duplicate it is.
   */
  const claimed = new Map<string, Oid>();

  // Ancestry over the *whole* DAG, join commits and all. Building it from the
  // payload-carrying events alone would cut every chain at the join where two
  // concurrent histories met, and `supersedes` would fall back to comparing
  // ids for events that are genuinely ordered.
  const ancestors = ancestorSets(parents, Dag.topological(parents));
  const { ancestry, contains: inTrustLog, reaches: reachesTrust } = trustReach();

  /**
   * The opening event, and whether anything is competing to be it.
   *
   * Every honest event in a pull request descends from its `pr.opened`: the
   * ref starts there and each append parents on the current head, joins
   * keeping both sides. A forgery cannot arrange that — it would have to be an
   * ancestor of commits that already exist — so where the history has any
   * depth at all, the genuine opening is the one the rest descends from.
   *
   * Where it has none, the two are structurally identical: a pull request
   * whose only event is its opening, plus a parentless impostor and a join,
   * offers nothing to tell them apart, and any deterministic tie-break is one
   * an attacker can grind. So a contested opening does not establish an
   * author at all, and every author-gated action then needs `hub.merge`. The
   * attacker gains nothing — they still cannot close the pull request or free
   * the branch behind it — and a `hub.merge` holder can still settle it.
   */
  /**
   * Everybody who has claimed to have opened this pull request.
   *
   * Usually one. More than one means the opening is contested, and an
   * approval from *any* claimant is still self-approval — so the exclusion
   * cannot be keyed on the single `author` a contested opening declines to
   * establish.
   */
  const openers = new Set<Fingerprint>();

  /** Which members signed each event, for the contested-opening ranking. */
  const members = new Map<Oid, ReadonlySet<Fingerprint>>();

  const opening = yield* Effect.gen(function* () {
    // Only openings that would actually be *accepted* compete. Computed over
    // the raw walk, an unsigned second `pr.opened` — pushable by any
    // `source.push` holder — contested the genuine one, and a contested
    // opening establishes no author: which locked the real author out of
    // their own pull request and, worse, re-enabled self-approval, since
    // `approvals` can only exclude an author it knows.
    const candidates: Oid[] = [];
    for (const entry of events) {
      if (entry.payload?.type !== "pr.opened") continue;
      const payload = entry.payload;
      if (payload.pr !== pr) continue;

      const valid = yield* Event.validate(payload, genesis.repoId).pipe(
        Effect.as(true),
        Effect.catchTag("Invalid", () => Effect.succeed(false)),
      );
      if (!valid) continue;

      const authorized = yield* Verify.authorize({
        projection: trust,
        bytes: entry.bytes,
        signatures: entry.signatures,
        capability: Event.capabilityFor(payload),
        made: { at: new Date(payload.issuedAt), trustHead: trustHeadOf(payload.trustHead) },
        seen: ancestry,
        contains: inTrustLog,
      });
      if (authorized.ok) {
        candidates.push(entry.commit);
        openers.add(authorized.principal.fingerprint);
      }
    }

    if (candidates.length <= 1) {
      return { commit: candidates[0] ?? null, contested: false } satisfies Opening;
    }

    // Who signed each event, memoised across candidates, and only members: a
    // fresh key costs nothing to generate, so counting *distinct signers* only
    // means anything if the keys have to be ones this repository granted.
    const signersOf = Effect.fnUntraced(function* (entry: (typeof events)[number]) {
      const cached = members.get(entry.commit);
      if (cached !== undefined) return cached;
      const found = new Set<Fingerprint>();
      for (const signer of yield* Verify.signers(entry.bytes, entry.signatures)) {
        if (trust.members.has(signer) || trust.former.has(signer)) found.add(signer);
      }
      members.set(entry.commit, found);
      return found;
    });

    /**
     * How much of the pull request stands behind an opening.
     *
     * Raw descendant count is manufactured, not earned: a forger grafts their
     * own opening and chains commits under it, and — since the count was taken
     * over the walked DAG rather than over events — the commits did not even
     * have to carry a payload. Winning handed them `base`, and a pull request
     * whose base no longer names its branch is one `Policy.protectedBranch`
     * skips, which freezes the branch behind an approved change.
     *
     * Counted over *events*, weighted by how many distinct members made them.
     * Every honest event in a pull request descends from its genuine opening,
     * and they come from the people taking part; a graft descends only from
     * what its author wrote, so displacing a real conversation costs one
     * member key per participant rather than one commit per event.
     */
    const rank = Effect.fnUntraced(function* (commit: Oid) {
      const signers = new Set<Fingerprint>();
      let count = 0;
      for (const entry of events) {
        if (entry.commit === commit || entry.payload === null) continue;
        if (ancestors.get(entry.commit)?.has(commit) !== true) continue;
        count++;
        for (const signer of yield* signersOf(entry)) signers.add(signer);
      }
      return [signers.size, count, commit] as const;
    });

    let best = yield* rank(candidates[0]!);
    for (const commit of candidates.slice(1)) {
      const candidate = yield* rank(commit);
      const better =
        candidate[0] !== best[0]
          ? candidate[0] > best[0]
          : candidate[1] !== best[1]
            ? candidate[1] > best[1]
            : candidate[2] < best[2];
      if (better) best = candidate;
    }

    // A *legitimate* second opening — the author retargeting their own pull
    // request — descends from the first. One that does not is a competing
    // claim to have started it.
    const contested = candidates.some(
      (commit) => commit !== best[2] && ancestors.get(commit)?.has(best[2]) !== true,
    );
    return { commit: best[2], contested } satisfies Opening;
  });

  let title = "";
  let description = "";
  let base = "";
  let state: PullRequest["state"] = "open";
  let author: Fingerprint | null = null;
  let mergeCommit: Oid | null = null;
  let at = new Date(0);
  let headSetter: { readonly commit: Oid; readonly id: string; readonly head: Oid } | null = null;
  /** The `pr.opened` event's key, so a tombstone over it blanks what it said. */
  let openedBy: string | null = null;

  const reviews = new Map<string, Review>();
  const dismissed = new Set<string>();
  const threads = new Map<string, Thread>();
  const checks = new Map<string, Check>();
  /** Which event last wrote each check, so a tombstone can blank its `url`. */
  const checkKeys = new Map<string, string>();
  /**
   * The *commits* a tombstone reached, not the ids it named.
   *
   * Ids are scoped to their author everywhere else in this fold, and a
   * tombstone that resolved by bare id would walk straight around that: a
   * member holding only `hub.comment` could post a comment re-using an
   * approval's id, redact their own comment, and take the approval's payload
   * with it — the blob deleted, the event unreadable, the approval gone. A
   * commit names exactly one event, so this is the one spelling that cannot
   * be aimed at somebody else's.
   */
  const redacted = new Set<Oid>();

  /** Bare id to the commits claiming it, for resolving a tombstone's target. */
  const byId = new Map<string, Oid[]>();

  /** The trust head each accepted event named, for the monotonicity check. */
  const heads = new Map<Oid, Oid>();

  /**
   * The newest trust head any accepted ancestor of this commit named.
   *
   * "Newest" among a set that is itself a chain: the trust log is append-only,
   * so of any two of its commits one reaches the other, and the one reaching
   * the rest is the one an event must be at least as new as.
   */
  const trustFloor = Effect.fnUntraced(function* (commit: Oid) {
    let floor: Oid | null = null;
    for (const ancestor of ancestors.get(commit) ?? []) {
      const named = heads.get(ancestor);
      if (named === undefined || named === floor) continue;
      if (floor === null || (yield* reachesTrust(named, floor))) floor = named;
    }
    return floor;
  });

  for (const entry of events) {
    const payload = entry.payload;

    // A redacted event: the commit and its place in the chain survive, the
    // content does not. It contributes nothing further — with no payload
    // there is no thread or review to build — and, importantly, it blanks
    // nothing else. The id in its commit message is unsigned, so treating it
    // as a tombstone would let anybody with `source.push` blank another
    // member's review by pushing a junk event that names it. Redactions come
    // from `event.redacted` payloads, which are signed and capability-checked.
    if (payload === null) continue;

    const invalid = yield* Event.validate(payload, genesis.repoId).pipe(
      Effect.as(null),
      Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
    );
    if (invalid !== null) {
      rejected.push({ commit: entry.commit, reason: invalid });
      continue;
    }
    if (payload.pr !== pr) {
      rejected.push({ commit: entry.commit, reason: `event belongs to ${payload.pr}` });
      continue;
    }

    // The trust head an event names is written by its own signer, and a
    // forward-only revocation is judged by whether that head already reached
    // it. Left unconstrained, a revoked member could name any pre-revocation
    // commit and have their capabilities recovered from `former`, which is the
    // revocation not applying at all. What they cannot do is rewrite the
    // events they are building on: the pull request's history is hash-linked,
    // so an event whose own ancestors were written against a *later* trust
    // head is claiming to have seen less than the conversation it is joining.
    //
    // Held to that floor rather than refused for falling short of it. Hub refs
    // and the trust log replicate as separate refs, so a client whose log lags
    // the conversation writes an older head honestly and often — and refusing
    // dropped those comments, reviews and approvals *permanently*, since the
    // floor comes from a history that only grows and re-folding after the log
    // caught up could not rescue them. Reading the event as having seen the
    // floor keeps the property intact — nobody escapes a revocation their own
    // ancestors had already seen — and costs an honest straggler nothing.
    const declared = trustHeadOf(payload.trustHead);
    const floor = yield* trustFloor(entry.commit);
    const behind = floor !== null && !(yield* reachesTrust(declared, floor));
    const effective = behind ? floor : declared;

    const authorized = yield* Verify.authorize({
      projection: trust,
      bytes: entry.bytes,
      signatures: entry.signatures,
      capability: Event.capabilityFor(payload),
      made: { at: new Date(payload.issuedAt), trustHead: effective },
      seen: ancestry,
      contains: inTrustLog,
    });
    if (!authorized.ok) {
      rejected.push({ commit: entry.commit, reason: authorized.reason });
      continue;
    }

    // Recorded only once the event counts, and only when the head it names is
    // a commit this replica's trust log actually holds. A fabricated one
    // passes its own floor check — it has no ancestors to be behind — and
    // would then become the floor for everything after it, which nothing can
    // reach: `Log.ancestry` of a commit nobody has is empty, so every later
    // event is refused forever on a ref that only grows.
    if (effective !== null && (yield* inTrustLog(effective))) {
      heads.set(entry.commit, effective);
    }

    const signer = authorized.principal.fingerprint;

    // The claim is per *author*, not per id, and that is the whole defence.
    // An id is chosen by whoever writes the event, so a global claim let the
    // first commit in topological order — a tie broken by the bare oid, which
    // anybody able to write a hub ref can grind — evict a stranger's event
    // that happened to share it. Scoped to the signer, a duplicate can only
    // ever displace its own author's earlier event, which is a mistake rather
    // than an attack, and every collection below is keyed the same way so that
    // no author can overwrite another's entry either.
    const mine = keyOf(signer, payload.id);
    const previous = claimed.get(mine);
    if (previous !== undefined) {
      rejected.push({
        commit: entry.commit,
        reason: `${signer} already used event id ${payload.id} in ${previous}`,
      });
      continue;
    }
    claimed.set(mine, entry.commit);
    byId.set(payload.id, [...(byId.get(payload.id) ?? []), entry.commit]);

    const issued = new Date(payload.issuedAt);
    if (issued > at) at = issued;

    /**
     * Whether this event's signer also holds a second capability.
     *
     * Several events cost one capability to *make* and a higher one to make
     * about somebody else's work: retargeting a pull request, closing it,
     * dismissing a review. `capabilityFor` sees only the payload, so the
     * author comparison and this second check are what the difference is made
     * of. Judged against the same trust head, so it is ordered like everything
     * else here.
     */
    const alsoHolds = (capability: string) =>
      Verify.authorize({
        projection: trust,
        bytes: entry.bytes,
        signatures: entry.signatures,
        capability,
        made: { at: issued, trustHead: effective },
        seen: ancestry,
        contains: inTrustLog,
      });

    switch (payload.type) {
      case "pr.opened": {
        // The opening event establishes the author; any *other* `pr.opened`
        // rewrites the title, description and base of a pull request somebody
        // else opened, and is held to the same rule as `pr.updated` below.
        // Which one opens it is decided by descent, above, and not by which
        // one this loop happens to reach first.
        const opens = entry.commit === opening.commit;
        if (!opens && signer !== author && !(yield* alsoHolds("hub.merge")).ok) {
          rejected.push({
            commit: entry.commit,
            reason: "re-opening somebody else's pull request needs hub.merge",
          });
          break;
        }

        // The winner supplies the content. Withholding it when the opening was
        // contested left `base` empty, which `protectedBranch` matches against
        // nothing — so a competing `pr.opened` froze the very pull request it
        // was pushed at, which is the denial the capability charges exist to
        // stop. Capturing the content instead requires *winning descent*, and
        // that means being an ancestor of the events the pull request already
        // has: impossible for a forgery on any pull request with activity, and
        // an approved one has activity by definition.
        title = payload.title;
        description = payload.description;
        base = payload.base;
        // Authorship is the part a contested opening still does not confer.
        if (!opening.contested) author ??= signer;
        openedBy = mine;
        const head = Event.unqualify(payload.head);
        if (
          head !== null &&
          supersedes({ commit: entry.commit, id: payload.id }, headSetter, ancestors)
        ) {
          headSetter = { commit: entry.commit, id: payload.id, head };
        }
        break;
      }

      case "pr.updated": {
        // Moving the head stales every approval of the revision it replaces.
        // Charged only `hub.create-pr`, that let any hub writer retarget
        // somebody else's approved pull request and, with it, block the
        // protected branch that pull request was the route to — the same
        // denial `pr.closed` is defended against.
        //
        // `author === null` is *not* a licence. An attacker can push a
        // parentless event and a join reaching it, and `Dag.topological`
        // orders parentless commits by oid — so grinding a low one folds it
        // before the `pr.opened` that establishes the author, and a guard
        // written as `author !== null && …` was inert exactly there.
        if (signer !== author && !(yield* alsoHolds("hub.merge")).ok) {
          rejected.push({
            commit: entry.commit,
            reason: "retargeting somebody else's pull request needs hub.merge",
          });
          break;
        }

        const head = Event.unqualify(payload.head);
        if (
          head !== null &&
          supersedes({ commit: entry.commit, id: payload.id }, headSetter, ancestors)
        ) {
          headSetter = { commit: entry.commit, id: payload.id, head };
        }
        break;
      }

      case "pr.closed":
      case "pr.reopened": {
        // Deciding a pull request's fate is not the same authority as opening
        // one. `hub.create-pr` is the lowest-privileged hub capability, and
        // charging closing to it let anybody holding it close somebody else's
        // approved pull request — after which `protectedBranch` skips it and
        // the branch it was approved for cannot be moved at all. Its own
        // author may always close it; anybody else needs `hub.merge`, which is
        // the capability for settling a pull request.
        // As in `pr.updated`: an event folded before the opening one has no
        // author to compare against, and treating that as permission let a
        // parentless `pr.closed` close a pull request its signer had no
        // authority over.
        if (signer !== author && !(yield* alsoHolds("hub.merge")).ok) {
          rejected.push({
            commit: entry.commit,
            reason: `${payload.type} by somebody other than the author needs hub.merge`,
          });
          break;
        }
        // A merge is final; closing or reopening after it would be a later
        // event undoing a state that has already landed in the branch.
        if (state === "merged") break;
        state = payload.type === "pr.closed" ? "closed" : "open";
        break;
      }

      case "pr.merged":
        state = "merged";
        mergeCommit = Event.unqualify(payload.mergeCommit);
        break;

      case "review.submitted": {
        const head = Event.unqualify(payload.head);
        if (head === null) break;
        reviews.set(mine, {
          id: payload.id,
          author: signer,
          head,
          decision: payload.decision,
          body: payload.body,
          at: issued,
          dismissed: false,
          stale: false,
        });
        break;
      }

      case "review.dismissed": {
        // By claimant, like every other cross-reference. Resolved by bare id
        // this dropped every review sharing it, so a `hub.review` holder could
        // nullify an `hub.approve` holder's approval by posting a review that
        // re-used its id and then dismissing that.
        const found = byEventId(reviews, payload.review);
        if (found === null || found === "ambiguous") {
          rejected.push({
            commit: entry.commit,
            reason:
              found === null ? `no review ${payload.review}` : `ambiguous review ${payload.review}`,
          });
          break;
        }
        // Nullifying somebody else's approval costs what making one costs.
        // Charged `hub.review`, the lower capability could cancel the higher
        // one's word — and a repository requiring two approvals could be held
        // at one by anybody who may review. A reviewer may always withdraw
        // their own.
        if (found.value.author !== signer && !(yield* alsoHolds("hub.approve")).ok) {
          rejected.push({
            commit: entry.commit,
            reason: "dismissing somebody else's review needs hub.approve",
          });
          break;
        }
        dismissed.add(found.key);
        break;
      }

      case "comment.created":
        threads.set(mine, {
          id: payload.id,
          path: payload.path,
          side: payload.side,
          line: payload.line,
          head: payload.head === null ? null : Event.unqualify(payload.head),
          resolved: false,
          comments: [
            { id: payload.id, author: signer, body: payload.body, at: issued, redacted: false },
          ],
        });
        break;

      case "comment.replied": {
        const found = byEventId(threads, payload.thread);
        // A reply to a thread that does not exist is not a thread: dropping it
        // is what keeps a projection from inventing one out of a typo. And a
        // reference that two threads answer to is a reference to neither —
        // guessing would let a second claimant capture the first one's replies.
        if (found === null || found === "ambiguous") {
          rejected.push({
            commit: entry.commit,
            reason:
              found === null ? `no thread ${payload.thread}` : `ambiguous thread ${payload.thread}`,
          });
          break;
        }
        threads.set(found.key, {
          ...found.value,
          comments: [
            ...found.value.comments,
            { id: payload.id, author: signer, body: payload.body, at: issued, redacted: false },
          ],
        });
        break;
      }

      case "comment.resolved":
      case "comment.reopened": {
        const found = byEventId(threads, payload.thread);
        // Said out loud, like every other cross-reference: a pull request held
        // shut by `requireResolvedThreads` needs somewhere to show why.
        if (found === null || found === "ambiguous") {
          rejected.push({
            commit: entry.commit,
            reason:
              found === null ? `no thread ${payload.thread}` : `ambiguous thread ${payload.thread}`,
          });
          break;
        }

        // Resolving a thread is what satisfies `requireResolvedThreads`, and
        // reopening one is what withholds it — so neither is something any
        // `hub.comment` holder may do to a thread that is not theirs. The
        // thread's own author may always settle it; anybody else needs
        // `hub.review`, the capability for judging somebody else's work.
        const opener = found.value.comments[0]?.author;
        if (opener !== signer && !(yield* alsoHolds("hub.review")).ok) {
          rejected.push({
            commit: entry.commit,
            reason: `${payload.type} on somebody else's thread needs hub.review`,
          });
          break;
        }

        threads.set(found.key, {
          ...found.value,
          resolved: payload.type === "comment.resolved",
        });
        break;
      }

      case "check.started":
      case "check.completed": {
        const head = Event.unqualify(payload.head);
        if (head === null) break;
        // Keyed by name and revision: a check re-run against the same head
        // replaces its own result, and one against a new head is a new answer.
        checkKeys.set(`${payload.name}@${head}`, mine);
        checks.set(`${payload.name}@${head}`, {
          name: payload.name,
          provider: payload.provider,
          head,
          status: payload.type === "check.started" ? "started" : payload.status,
          url: payload.type === "check.completed" ? payload.url : null,
          at: issued,
          author: signer,
        });
        break;
      }

      case "event.redacted": {
        // By commit, which is what the tombstone signs. Resolving the id
        // instead worked exactly once: the payload an id is read from is the
        // payload the tombstone deletes, so every later projection lost the
        // target and stopped excluding its blob — the removal undoing itself
        // the first time anything rebuilt the state.
        const targetCommit = Event.unqualify(payload.targetCommit);
        if (targetCommit === null || !parents.has(targetCommit)) {
          rejected.push({
            commit: entry.commit,
            reason: `${payload.targetCommit} is not an event of this pull request`,
          });
          break;
        }
        if (targetCommit === entry.commit) {
          rejected.push({ commit: entry.commit, reason: "a tombstone cannot remove itself" });
          break;
        }
        // Nor another tombstone. A tombstone is the *record* of a removal, and
        // removing it would take its target's blob back out of the excluded
        // set — so the redaction would quietly undo itself the next time
        // anything packed or collected. `PullRequest.redact` already refuses
        // to write one; the fold has to refuse to honour one, because a
        // replica sees the event and never the command.
        if (
          events.find((event) => event.commit === targetCommit)?.payload?.type === "event.redacted"
        ) {
          rejected.push({
            commit: entry.commit,
            reason: "a tombstone is the record of a removal and is not itself removable",
          });
          break;
        }
        redacted.add(targetCommit);
        break;
      }
    }
  }

  const head = headSetter?.head ?? null;

  /** Whether a tombstone reached the event this key names. */
  const blanked = (key: string): boolean => {
    const commit = claimed.get(key);
    return commit !== undefined && redacted.has(commit);
  };
  const openingRedacted = openedBy !== null && blanked(openedBy);

  return {
    id: pr,
    title: openingRedacted ? "" : title,
    description: openingRedacted ? "" : description,
    base,
    head,
    state,
    author,
    mergeCommit,
    reviews: [...reviews.entries()].map(([key, review]) => ({
      ...review,
      dismissed: dismissed.has(key),
      // Stale, not gone: the statement stays true about the revision it named.
      stale: head === null || review.head !== head,
      // A tombstone removes content wherever the content is. Blanking only
      // comments would leave a redacted review's prose, and a pull request's
      // title and description, readable on every replica that still holds the
      // blob — which is most of them, since the object is deleted locally.
      body: blanked(key) ? "" : review.body,
    })),
    threads: [...threads.entries()].map(([key, thread]) => {
      // Where a comment pointed is content too. A redacted inline comment that
      // still names a file and a line says most of what it said, on every
      // replica that holds the blob — which is the case redaction exists for.
      const opener = blanked(key);
      return {
        ...thread,
        path: opener ? null : thread.path,
        side: opener ? null : thread.side,
        line: opener ? null : thread.line,
        head: opener ? null : thread.head,
        comments: thread.comments.map((comment) =>
          blanked(keyOf(comment.author, comment.id))
            ? { ...comment, body: "", redacted: true }
            : comment,
        ),
      };
    }),
    checks: [...checks.entries()].map(([name, check]) => {
      const key = checkKeys.get(name);
      // A check's `url` points at somebody's build output, which is content a
      // tombstone is entitled to remove along with the rest.
      return key !== undefined && blanked(key) ? { ...check, url: null } : check;
    }),
    claims: byId,
    openers,
    redacted,
    rejected,
    at,
  } satisfies PullRequest;
});

/**
 * The approvals a merge policy may count.
 *
 * Not merely "approve" events: an approval of a revision that has since been
 * replaced says nothing about the revision being merged, a dismissed one has
 * been withdrawn, and a pull request's own author approving their own work is
 * not review at all — it is the thing review exists to be independent of.
 * All three are excluded here so that no caller has to remember to.
 */
export const approvals = (pullRequest: PullRequest): ReadonlyArray<Review> => {
  // One per approver, not one per event — counting events would let a single
  // member satisfy "two approvals required" alone — and it is each author's
  // *latest* word that counts, so a later "request changes" withdraws their
  // earlier approval rather than sitting beside it.
  const latest = new Map<Fingerprint, Review>();
  for (const review of pullRequest.reviews) {
    if (review.stale || review.dismissed) continue;
    // Self-approval satisfies nothing. Without this, one member holding
    // `hub.approve` opened a pull request for their own commit, approved it,
    // and cleared `requiredApprovals` on a protected branch alone. Every
    // claimed opener, not merely `author`: a contested opening establishes no
    // author, and an approval from either claimant is still their own.
    if (pullRequest.openers.has(review.author)) continue;
    const existing = latest.get(review.author);
    if (existing === undefined || existing.at <= review.at) latest.set(review.author, review);
  }
  return [...latest.values()].filter((review) => review.decision === "approve");
};

/** Whether every named check has succeeded against the current head. */
export const checksPassed = (
  pullRequest: PullRequest,
  required: ReadonlyArray<string>,
): boolean => {
  for (const name of required) {
    const check = pullRequest.checks.find(
      (candidate) => candidate.name === name && candidate.head === pullRequest.head,
    );
    if (check === undefined || check.status !== "success") return false;
  }
  return true;
};

export type ProjectionError = Invalid | ObjectNotFound | StorageFailure;
