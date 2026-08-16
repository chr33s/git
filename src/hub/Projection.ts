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
  /** Events whose payloads have been tombstoned. */
  readonly redacted: ReadonlySet<string>;
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
 * Whether one trust-log commit is at least as new as another.
 *
 * `null` — an event that recorded no trust head — reaches nothing, which is
 * the same conservative reading `Verify.reaches` gives it: an author who
 * cannot show what they had seen is treated as having seen everything, and
 * here that means they cannot be behind their own ancestors either.
 */
const reachesTrust = Effect.fn("hub.Projection.reachesTrust")(function* (
  head: Oid | null,
  target: Oid,
) {
  if (head === null) return true;
  if (head === target) return true;
  return (yield* Log.ancestry(head)).has(target);
});

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

  let title = "";
  let description = "";
  let base = "";
  let state: PullRequest["state"] = "open";
  let author: Fingerprint | null = null;
  let mergeCommit: Oid | null = null;
  let at = new Date(0);
  let headSetter: { readonly commit: Oid; readonly id: string; readonly head: Oid } | null = null;
  /** The `pr.opened` event, so a tombstone over it can blank what it said. */
  let openedBy: string | null = null;

  const reviews = new Map<string, Review>();
  const dismissed = new Set<string>();
  const threads = new Map<string, Thread>();
  const checks = new Map<string, Check>();
  const redacted = new Set<string>();

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
    const declared = trustHeadOf(payload.trustHead);
    const floor = yield* trustFloor(entry.commit);
    if (floor !== null && !(yield* reachesTrust(declared, floor))) {
      rejected.push({
        commit: entry.commit,
        reason: `trust head ${payload.trustHead ?? "(none)"} predates ${floor}, which an earlier event in this pull request already named`,
      });
      continue;
    }

    const authorized = yield* Verify.authorize({
      projection: trust,
      bytes: entry.bytes,
      signatures: entry.signatures,
      capability: Event.capabilityFor(payload),
      made: { at: new Date(payload.issuedAt), trustHead: declared },
    });
    if (!authorized.ok) {
      rejected.push({ commit: entry.commit, reason: authorized.reason });
      continue;
    }

    // Recorded only once the event counts, so a rejected one cannot raise the
    // floor for everything after it.
    if (declared !== null) heads.set(entry.commit, declared);

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

    const issued = new Date(payload.issuedAt);
    if (issued > at) at = issued;

    switch (payload.type) {
      case "pr.opened": {
        title = payload.title;
        description = payload.description;
        base = payload.base;
        author ??= signer;
        openedBy = payload.id;
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
        // A merge is final; closing after it would be a later event undoing a
        // state that has already landed in the branch.
        if (state !== "merged") state = "closed";
        break;

      case "pr.reopened":
        if (state !== "merged") state = "open";
        break;

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

      case "review.dismissed":
        dismissed.add(payload.review);
        break;

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
        if (found === null || found === "ambiguous") break;
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

      case "event.redacted":
        redacted.add(payload.target);
        break;
    }
  }

  const head = headSetter?.head ?? null;
  const openingRedacted = openedBy !== null && redacted.has(openedBy);

  return {
    id: pr,
    title: openingRedacted ? "" : title,
    description: openingRedacted ? "" : description,
    base,
    head,
    state,
    author,
    mergeCommit,
    reviews: [...reviews.values()].map((review) => ({
      ...review,
      dismissed: dismissed.has(review.id),
      // Stale, not gone: the statement stays true about the revision it named.
      stale: head === null || review.head !== head,
      // A tombstone removes content wherever the content is. Blanking only
      // comments would leave a redacted review's prose, and a pull request's
      // title and description, readable on every replica that still holds the
      // blob — which is most of them, since the object is deleted locally.
      body: redacted.has(review.id) ? "" : review.body,
    })),
    threads: [...threads.values()].map((thread) => ({
      ...thread,
      comments: thread.comments.map((comment) =>
        redacted.has(comment.id) ? { ...comment, body: "", redacted: true } : comment,
      ),
    })),
    checks: [...checks.values()],
    redacted,
    rejected,
    at,
  } satisfies PullRequest;
});

/**
 * The approvals a merge policy may count.
 *
 * Not merely "approve" events: an approval of a revision that has since been
 * replaced says nothing about the revision being merged, and a dismissed one
 * has been withdrawn. Both are excluded here so that no caller has to remember
 * to exclude them.
 */
export const approvals = (pullRequest: PullRequest): ReadonlyArray<Review> => {
  // One per approver, not one per event — counting events would let a single
  // member satisfy "two approvals required" alone — and it is each author's
  // *latest* word that counts, so a later "request changes" withdraws their
  // earlier approval rather than sitting beside it.
  const latest = new Map<Fingerprint, Review>();
  for (const review of pullRequest.reviews) {
    if (review.stale || review.dismissed) continue;
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
