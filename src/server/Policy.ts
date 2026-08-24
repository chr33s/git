/**
 * The one boundary a ref update crosses.
 *
 * Three questions, asked in order and answered by three different things:
 *
 * ```text
 * authentication  → who is this?          (Auth.ts, an SSH key)
 * membership      → what may they do?     (trust/Verify.ts, a capability)
 * policy          → is this transition allowed now?   (here)
 * ```
 *
 * The third is the one that needs the repository's *state*: whether the push
 * drops commits, whether the branch is protected, whether the revision has the
 * approvals and checks the branch requires. None of that is knowable from a
 * credential, which is why it cannot live in the guard.
 *
 * Two rules here are not configurable, because they are what the namespaces
 * mean rather than what an administrator prefers. `refs/meta/trust/genesis`
 * never moves — a host that could rewrite it could rename one repository into
 * another's identity. And `refs/hub/**` and the trust log only grow: an update
 * must contain what it replaces, so history can be added to and never edited.
 *
 * Every allowed update carries the value it was judged against, and `apply`
 * passes that as the compare-and-swap. Evaluating and then applying without it
 * is a race with a name: the approvals counted a moment ago were for a head
 * that has since moved.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import * as Dag from "../git/Dag.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import * as Refspec from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import { type Oid, type RefUpdate, storageOf } from "../git/Store.ts";
import { GENESIS_REF, type Genesis, readGenesis } from "../trust/Genesis.ts";
import {
  type Member,
  openWindow,
  project,
  type Projection as TrustProjection,
} from "../trust/Projection.ts";
import * as Event from "../hub/Event.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import * as Session from "../hub/Session.ts";
import * as Task from "../hub/Task.ts";
import * as Queue from "../hub/Queue.ts";
import * as SocialLog from "../social/Log.ts";
import * as Inbox from "../social/Inbox.ts";
import * as Statement from "../social/Statement.ts";
import {
  approvals,
  checksPassedAt,
  project as projectPr,
  type Review,
  type PullRequest as PullRequestState,
} from "../hub/Projection.ts";
import * as SocialProjection from "../social/Projection.ts";
import { externalReviews, type ExternalReview } from "../social/Review.ts";
import { permits } from "../trust/Certificate.ts";
import * as Log from "../trust/Log.ts";
import * as Record from "../trust/Record.ts";
import { principalOf, principalSubject, type PrincipalId } from "../trust/Principal.ts";
import * as Verify from "../trust/Verify.ts";
import * as Auth from "./Auth.ts";

/**
 * What a branch requires before it will move.
 *
 * Empty by default in every field: a repository that has said nothing about a
 * branch has not protected it, and inventing protection nobody asked for would
 * break every existing push.
 */
export interface ExternalReviewRule {
  /** Exact protected branch this opt-in applies to. */
  readonly branch: string;
  /** Repository-selected roots; a pusher cannot bring their own web. */
  readonly anchors: ReadonlyArray<PrincipalId>;
  readonly scope: "review";
  readonly maxDepth: number;
  readonly minPaths: number;
  readonly maxCount: number;
}

export interface Rules {
  /**
   * Full ref names, or prefixes with a trailing `*`.
   *
   * `refs/heads/main`, not `main`: the value is compared against the ref being
   * written, and a bare branch name would match nothing while looking exactly
   * like it protected something.
   */
  readonly protected: ReadonlyArray<string>;
  readonly requiredApprovals: number;
  readonly requiredChecks: ReadonlyArray<string>;
  readonly requireResolvedThreads: boolean;
  /** Whether a protected branch may only move through a pull request. */
  readonly requirePullRequest: boolean;
  /**
   * How stale a membership view may be before writes are refused, in seconds.
   *
   * A hash-linked log makes withholding visible but not impossible: a replica
   * can serve a consistent view that simply stops short of a revocation, and
   * nothing in the log itself says so. A checkpoint is a signed statement that
   * somebody with authority had seen a given frontier at a given time, so
   * requiring a recent one bounds how far behind a served view may be.
   *
   * `0` is unbounded, and is the default: a repository that has never
   * checkpointed would otherwise refuse every push the moment this shipped,
   * and a bound nobody asked for is a bound that breaks working repositories.
   */
  readonly maxTrustAgeSeconds: number;
  /**
   * Whether every commit a push introduces must name the session that made it.
   *
   * Off by default, and a real cost when on: it is a rule about *every* new
   * commit, so on a repository where people push directly it asks people to
   * publish session records too. Scoping it by signer would be kinder and is
   * deferred — an unsigned commit would become the way around it, and a rule
   * with a hole in it is worse than one an operator turned on knowingly.
   */
  readonly requireProvenance: boolean;
  /**
   * What a repository will accept being told it cost, per window.
   *
   * Observability and advisory restraint, not defence: usage is self-reported
   * by the signer, so this bounds what the repository *accepts* rather than
   * what anybody spent. The wallet is bounded where the tokens are actually
   * counted, which is not here. `0` is unbounded, and the default.
   */
  readonly maxUsageTokens: number;
  readonly usageWindowSeconds: number;
  /**
   * Whether a protected branch may also be advanced by a queue candidate.
   *
   * A candidate is a chain of merge commits, each merging one approved pull
   * request's head onto the step before it, ending at what the branch holds
   * now. What makes one safe to accept is that this boundary *re-derives* every
   * merge: a step counts only if its tree is exactly what `Repository.mergeTree`
   * produces for its two parents, so the chain's content is a pure function of
   * revisions that were reviewed, and whoever built it is trusted with nothing.
   *
   * Off by default. It is a second way onto a protected branch, and one an
   * operator should turn on knowingly rather than acquire in an upgrade — the
   * same reason `requireProvenance` is off. Left off, the walk is never
   * attempted and this costs a boolean read.
   *
   * What it buys is the composition itself: required checks on a candidate name
   * the *candidate*, so what a queue tests is the combination being landed
   * rather than each pull request alone — which is the only evidence that two
   * individually green changes work together.
   */
  readonly queueCandidates: boolean;
  /**
   * How many merge steps one candidate chain may carry.
   *
   * Every step costs a merge this boundary recomputes, synchronously, on the
   * receive-pack path, so how deep a chain may be is how much work one push can
   * ask for. Bounded here rather than only by whoever builds the chain, and
   * bounded above by `MAX_QUEUE_DEPTH` besides — a rules file is written by a
   * `policy.write` holder, and a number nobody thought about should not be able
   * to turn one push into an unbounded walk.
   */
  readonly queueDepth: number;
  /** Opt-in only; omitted and `null` both mean external reviews never count. */
  readonly externalReview?: ExternalReviewRule | null;
}

/** Re-exported where the boundary reads it; defined beside the guard. */
export const anonymousWrites = Auth.anonymousWrites;
export type AnonymousWrites = Auth.AnonymousWrites;

/**
 * How deep a candidate chain may be where a rules file does not say.
 *
 * Eight is a queue depth, not a history depth: it is how many approved pull
 * requests one landing may carry, and a queue that cannot land eight at once is
 * not the bottleneck a queue exists to remove.
 */
export const QUEUE_DEPTH = 8;

/**
 * And the most any rules file may ask for.
 *
 * The per-step cost is a merge recomputed on the synchronous push path, so this
 * is the same kind of bound as `Event.MAX_EVENTS`: what one request may make a
 * host do, decided by the host rather than by the document it is serving.
 */
export const MAX_QUEUE_DEPTH = 64;

export const OPEN: Rules = {
  protected: [],
  requiredApprovals: 0,
  requiredChecks: [],
  requireResolvedThreads: false,
  requirePullRequest: false,
  maxTrustAgeSeconds: 0,
  requireProvenance: false,
  maxUsageTokens: 0,
  usageWindowSeconds: 0,
  queueCandidates: false,
  queueDepth: QUEUE_DEPTH,
  externalReview: null,
};

/** Where a repository keeps its branch rules, if it has any. */
export const RULES_REF = "refs/meta/policy";
export const RULES_PATH = "policy.json";

const RulesDocument = Schema.Struct({
  version: Schema.Literal(1),
  protected: Schema.Array(Schema.String),
  requiredApprovals: Schema.Int,
  requiredChecks: Schema.Array(Schema.String),
  requireResolvedThreads: Schema.Boolean,
  requirePullRequest: Schema.Boolean,
  /** Optional, so a rules file written before this existed still decodes. */
  maxTrustAgeSeconds: Schema.optional(Schema.Int),
  /** Optional, so a rules file written before this existed still decodes. */
  requireProvenance: Schema.optional(Schema.Boolean),
  maxUsageTokens: Schema.optional(Schema.Int),
  usageWindowSeconds: Schema.optional(Schema.Int),
  /** Optional, so a rules file written before this existed still decodes. */
  queueCandidates: Schema.optional(Schema.Boolean),
  queueDepth: Schema.optional(Schema.Int),
  externalReview: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        branch: Schema.String,
        anchors: Schema.Array(Schema.String),
        scope: Schema.Literal("review"),
        maxDepth: Schema.Int,
        minPaths: Schema.Int,
        maxCount: Schema.Int,
      }),
    ),
  ),
});

const decodeRules = Schema.decodeUnknownEffect(RulesDocument);
const decoder = new TextDecoder();
const encoder = new TextEncoder();

export const encodeRules = (rules: Rules): Uint8Array =>
  encoder.encode(
    `${JSON.stringify(
      {
        version: 1,
        protected: rules.protected,
        requiredApprovals: rules.requiredApprovals,
        requiredChecks: rules.requiredChecks,
        requireResolvedThreads: rules.requireResolvedThreads,
        requirePullRequest: rules.requirePullRequest,
        maxTrustAgeSeconds: rules.maxTrustAgeSeconds,
        requireProvenance: rules.requireProvenance,
        maxUsageTokens: rules.maxUsageTokens,
        usageWindowSeconds: rules.usageWindowSeconds,
        queueCandidates: rules.queueCandidates,
        queueDepth: rules.queueDepth,
        externalReview:
          rules.externalReview === undefined || rules.externalReview === null
            ? null
            : {
                ...rules.externalReview,
                anchors: rules.externalReview.anchors.map(principalSubject),
              },
      },
      null,
      2,
    )}\n`,
  );

/**
 * The rules a repository has published, or none.
 *
 * A repository that has said nothing is `OPEN`, which is what every repository
 * predating this was — turning on protection nobody configured would break
 * pushes that have always worked.
 *
 * A policy file that *exists* and will not parse is a failure, not `OPEN`.
 * Reading a broken rules file as "no rules" would let anybody turn branch
 * protection off by corrupting it, which is the opposite of what a rules file
 * is for. Callers fail closed on that failure — with one exception, and it is
 * what keeps failing closed from being a one-way door: see `repairable`.
 */
export const rulesOf = Effect.fn("Policy.rulesOf")(function* () {
  const repository = yield* Repository;

  const commit = yield* repository.resolve(RULES_REF);
  if (commit === null) return OPEN;

  const info = yield* repository.readCommit(commit);
  const entry = yield* repository.findPath(info.tree, RULES_PATH);
  if (entry === null) return OPEN;

  // A policy file that will not parse leaves the repository's stated rules in
  // force by failing, not by quietly reverting to `OPEN`. Reading a *failure*
  // as "no protection" would turn branch protection off at the moment storage
  // was least trustworthy — the opposite of what the gates above do.
  const bytes = yield* repository.readBlob(entry.oid);
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "policy", reason: "policy is not valid JSON" }),
  });
  const loaded = yield* decodeRules(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "policy", reason: `malformed policy: ${issue.message}` }),
    ),
  );

  let externalReview: ExternalReviewRule | null = null;
  if (loaded.externalReview !== undefined && loaded.externalReview !== null) {
    const anchors: PrincipalId[] = [];
    for (const anchor of loaded.externalReview.anchors) {
      const parsed = principalOf(anchor);
      if (parsed === null) {
        return yield* new Invalid({
          field: "policy.externalReview.anchors",
          reason: `'${anchor}' is not a principal:<PrincipalID> subject`,
        });
      }
      anchors.push(parsed);
    }
    if (
      !loaded.externalReview.branch.startsWith("refs/heads/") ||
      loaded.externalReview.branch.includes("*") ||
      anchors.length === 0 ||
      loaded.externalReview.maxDepth < 0 ||
      loaded.externalReview.maxDepth > 16 ||
      loaded.externalReview.minPaths < 1 ||
      loaded.externalReview.maxCount < 0
    ) {
      return yield* new Invalid({
        field: "policy.externalReview",
        reason:
          "branch must be one exact refs/heads/* name, anchors must be non-empty, " +
          "maxDepth must be 0..16, minPaths at least 1, and maxCount non-negative",
      });
    }
    externalReview = { ...loaded.externalReview, anchors };
  }

  return {
    protected: loaded.protected,
    requiredApprovals: loaded.requiredApprovals,
    requiredChecks: loaded.requiredChecks,
    requireResolvedThreads: loaded.requireResolvedThreads,
    requirePullRequest: loaded.requirePullRequest,
    maxTrustAgeSeconds: loaded.maxTrustAgeSeconds ?? 0,
    requireProvenance: loaded.requireProvenance ?? false,
    maxUsageTokens: loaded.maxUsageTokens ?? 0,
    usageWindowSeconds: loaded.usageWindowSeconds ?? 0,
    queueCandidates: loaded.queueCandidates ?? false,
    queueDepth: clampDepth(loaded.queueDepth),
    externalReview,
  };
});

/**
 * External approvals that meet a repository-selected rooted confidence bar.
 *
 * The graph is taken rather than built: the caller needs the same projection to
 * ask whether the web is fresh, and building it here as well walked the whole
 * social web twice per pull request on the synchronous push path — for one
 * answer, from identical inputs the two call sites had to keep in step by hand.
 */
export const eligibleExternalApprovals = (input: {
  readonly rule: ExternalReviewRule;
  readonly graph: SocialProjection.Projection;
  readonly reviews: ReadonlyArray<ExternalReview>;
  readonly internal?: ReadonlyArray<Review>;
}): ReadonlyArray<ExternalReview> => {
  const graph = input.graph;
  const internal = new Set(
    (input.internal ?? []).map((review) => review.principal ?? review.author),
  );
  return input.reviews
    .filter(
      (review) =>
        !internal.has(review.principal) &&
        SocialProjection.confidence(
          graph,
          review.principal,
          input.rule.scope,
          input.rule.maxDepth,
        ) >= input.rule.minPaths,
    )
    .slice(0, input.rule.maxCount);
};

/**
 * How many approvals this pull request has, by every route the rules allow.
 *
 * One answer, for every caller who has to meet the same bar. The merge-queue
 * runner counted `approvals()` alone and the boundary counted those *plus* the
 * eligible external ones, so a repository whose bar is met partly by external
 * review had its runner refuse to build a candidate the boundary would have
 * landed: the entry sat unbuilt on every pass while the pull request itself was
 * mergeable. A runner stricter than the judge it is building for is a queue
 * that never drains, which is what the runner's own comment says must not
 * happen.
 *
 * A caller with no social web in context — the CLI runner on a replica that
 * holds none — counts the internal approvals and stops, which is the same
 * answer the boundary gives there.
 */
export const approvalsFor = Effect.fn("Policy.approvalsFor")(function* (input: {
  readonly genesis: Genesis;
  readonly pullRequest: PullRequestState;
  readonly id: string;
  readonly ref: string;
  readonly rules: Rules;
}) {
  const internal = approvals(input.pullRequest);
  const notStale: string | null = null;
  const configured = input.rules.externalReview ?? null;
  const rule = configured?.branch === input.ref ? configured : null;
  if (internal.length >= input.rules.requiredApprovals || rule === null || rule.maxCount <= 0) {
    return { count: internal.length, stale: notStale };
  }

  const web = yield* Effect.serviceOption(SocialProjection.SocialWeb);
  if (Option.isNone(web)) return { count: internal.length, stale: notStale };

  const logs = yield* web.value.logs;
  const external = yield* externalReviews(input.genesis, input.pullRequest, input.id, {
    maxIdentityAgeMs: input.rules.maxTrustAgeSeconds * 1000,
  });
  const graph = SocialProjection.project({
    roots: rule.anchors,
    logs,
    maxDepth: rule.maxDepth,
  });
  const eligible = eligibleExternalApprovals({
    rule,
    graph,
    reviews: external.reviews,
    internal,
  });
  const freshness =
    input.rules.maxTrustAgeSeconds <= 0
      ? ({ ok: true } as const)
      : SocialProjection.fresh(graph, input.rules.maxTrustAgeSeconds * 1000);
  // A stale web counts nothing, and is only worth *saying* when it had
  // something to count: a repository with no external approvals at all is
  // short of its bar for the ordinary reason, not because of the web.
  return freshness.ok
    ? { count: internal.length + eligible.length, stale: notStale }
    : {
        count: internal.length,
        stale: eligible.length > 0 ? freshness.reason : notStale,
      };
});

/**
 * The queue depth a repository actually gets, from what it asked for.
 *
 * Clamped rather than taken as written. What this number buys is work the host
 * does on the synchronous push path, so the document may ask for less than the
 * host's ceiling and never for more; a negative one reads as none, which is
 * what a chain of no steps already is.
 *
 * Exported because every door that accepts rules has to clamp them the same
 * way. The JSON verb echoed back whatever it was handed, so a caller writing
 * `queueDepth: 1000000` was told the repository now enforced that, while the
 * rules file it had just written would be read back clamped — two answers to
 * one question, from one repository.
 */
export const clampDepth = (asked: number | undefined): number =>
  Math.min(Math.max(asked ?? QUEUE_DEPTH, 0), MAX_QUEUE_DEPTH);

/**
 * Pull requests folded once for a batch of ref updates.
 *
 * A pull request's state cannot change while one push is being judged, so a
 * fold is reusable within it — and only within it, which is why the map is the
 * caller's rather than a module-level cache.
 */
export type FoldCache = Map<string, PullRequestState>;

/**
 * Which revisions each pull request has ever proposed, from its raw events.
 *
 * A pre-filter, not a decision: it says which pull requests are *worth*
 * folding, and the fold is what decides anything. Shared across one batch, for
 * the same reason `FoldCache` is.
 */
export type MentionCache = Map<string, ReadonlySet<string>>;

/**
 * Merges one batch has already re-derived, keyed by the two commits merged.
 *
 * A merge is a pure function of its two sides and of objects that never change,
 * so within one request the same pair has one answer. Shared for the reason
 * `FoldCache` is, and it matters more here: verifying a chain of depth D asks
 * about D merges, and a caller looking for the longest landable prefix asks
 * about the chain once per prefix — which without this is D(D+1)/2 full
 * three-way merges per pass, each one a merge-base walk and three whole-tree
 * flattens, repeated on every wake while a check is pending.
 *
 * The caller's, not a module-level cache, and deliberately: a merged tree is a
 * tree this wrote, and a memo outliving the request could hand back an oid that
 * a collection in between had swept.
 */
export type MergeCache = Map<string, MergeOf>;

interface MergeOf {
  readonly tree: Oid;
  readonly conflicts: ReadonlyArray<{ readonly path: string }>;
}

/**
 * The hub refs one batch has already been allowed to create.
 *
 * A push is judged in full before any of it is applied, so a bound read from
 * the store is the same number for every command in the batch. Anything that
 * bounds how many of something a repository may hold therefore has to count
 * what this push is adding as well as what it already holds. The caller's,
 * like the caches above, because it is state about one push and not about the
 * host.
 */
export type Openings = Set<string>;

/**
 * Every revision each pull request has proposed, kept across requests.
 *
 * `MentionCache` is per batch, which is the right lifetime for a decision and
 * the wrong one for a walk: this reads a pull request's whole event DAG, and
 * it is asked about *every* pull request the repository has on *every*
 * protected-branch push. Within a repository the answer is kept per pull
 * request, with the ref's value held as state to compare rather than keyed on,
 * so a moved ref is a miss, a stale answer is impossible, and an append
 * overwrites instead of adding.
 *
 * Two levels, because the question is asked one repository at a time and a
 * flat map is bounded by the wrong thing. One protected-branch push sweeps
 * every pull request the repository has, so a flat ceiling smaller than the
 * population a repository may reach — which the boundary lets reach
 * `MAX_PULL_REQUESTS` — is smaller than a single sweep: every push then misses
 * on every key and re-walks every event DAG synchronously, which is the cost
 * this exists to remove, arrived at through the memo itself. And a flat map is
 * shared by every repository the host serves, so one busy repository evicts the
 * rest.
 *
 * So both bounds are kept, and they bound different things. `ENTRIES` is the
 * hard cap on what the memo retains in total; it is the population bound
 * exactly, which is what makes one sweep always fit. `REPOSITORIES` bounds how
 * many hosts' worth of answers are kept at once. Whichever is exceeded, what is
 * evicted is a **whole repository**, least recently used — never an entry from
 * the repository being asked about, which is how a flat cap turned a sweep into
 * a sweep of misses. A single repository at the population bound can therefore
 * sit slightly over `ENTRIES` on its own, and nothing else is kept beside it.
 */
export const ENTRIES = Event.MAX_PULL_REQUESTS;
export const REPOSITORIES = 256;
type Mentions = Map<string, { readonly state: string; readonly heads: ReadonlySet<string> }>;
const mentions = new Map<string, Mentions>();

/** What the mention memo may keep; see `ENTRIES` and `REPOSITORIES`. */
export class MemoSize extends Context.Service<
  MemoSize,
  { readonly repositories: number; readonly entries: number }
>()("server/Policy/MemoSize") {}

export const memo = (repositories: number, entries = ENTRIES): Layer.Layer<MemoSize> =>
  Layer.succeed(MemoSize)({ repositories, entries });

const memoOf = Effect.fnUntraced(function* () {
  return Option.getOrElse(yield* Effect.serviceOption(MemoSize), () => ({
    repositories: REPOSITORIES,
    entries: ENTRIES,
  }));
});

/**
 * How many pull requests the mention memo is holding for one repository.
 *
 * Exported for the suite that guards the memo's shape: that its ceiling counts
 * pull requests rather than the revisions they propose, and that it counts
 * repositories rather than pull requests. Nothing reads it in production.
 */
export const mentionsHeld = (storage: string | null, genesis: Oid | null): number =>
  mentions.get(`${storage}\u0000${genesis}`)?.size ?? 0;

/** Whether a pull request's events ever named this revision as a head. */
const proposes = Effect.fn("Policy.proposes")(function* (pr: string, to: Oid, cache: MentionCache) {
  const repository = yield* Repository;

  const known = cache.get(pr);
  if (known !== undefined) return known.has(to);

  const at = yield* repository.resolve(Event.refOf(pr));
  // The head oid and the ceiling are *compared*, not keyed on. Keyed on the
  // head, every append left the answer for the head before it behind, so the
  // bound counted revisions rather than pull requests and a repository well
  // inside the population bound turned its own entry over on ordinary
  // activity. The ceiling has to be in there too: a host that will not walk a
  // pull request that size answers "it proposes nothing", and two hosts with
  // the same ref and different ceilings hold two different answers, one of
  // which refuses an approved protected-branch push.
  //
  // The repository is the outer key. This map outlives one request and one
  // repository, and a host serves many: two of them number their pull requests
  // from one, so `pr` alone is not a name — and a fork and its parent point at
  // the same commits under different refs, which the oid does not separate
  // either.
  //
  // The storage as well as the genesis, for the same reason: an origin and its
  // mirror under one host share the genesis oid, and right after a replication
  // the hub ref oids too — while what they can read need not agree, since refs
  // are applied without a connectivity check. Aliased, a cached empty answer
  // from the replica that cannot walk a pull request filtered the approved one
  // out of the origin's protected-branch push. See `Storage`.
  const identity = `${yield* storageOf()}\u0000${yield* repository.resolve(GENESIS_REF)}`;
  const state = `${at}\u0000${yield* Event.ceilingOf()}`;
  const kept = mentions.get(identity);
  const remembered = kept?.get(pr);
  if (kept !== undefined && remembered !== undefined && remembered.state === state) {
    // Re-inserted so iteration order is least-recently-used first.
    mentions.delete(identity);
    mentions.set(identity, kept);
    cache.set(pr, remembered.heads);
    return remembered.heads.has(to);
  }

  // A pull request this replica cannot walk proposes nothing it can act on.
  // The ceiling is enforced where a *push* crosses it, so a history that
  // arrived by replication may sit above it — and failing here would refuse
  // every push to every protected branch on that replica, permanently, which
  // is the denial the ceiling exists to prevent rather than a second way to
  // reach it. Skipped as one candidate, exactly as the fold below is.
  const walked = yield* Event.entries(pr).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  if (walked === null) {
    cache.set(pr, new Set());
    return false;
  }
  const { events } = walked;
  const heads = new Set<string>();
  for (const entry of events) {
    const payload = entry.payload;
    if (payload === null) continue;
    if (payload.type !== "pr.opened" && payload.type !== "pr.updated") continue;
    const head = Event.unqualify(payload.head);
    if (head !== null) heads.add(head);
  }
  cache.set(pr, heads);
  const held = kept ?? new Map();
  held.set(pr, { state, heads });
  mentions.delete(identity);
  mentions.set(identity, held);
  let total = [...mentions.values()].reduce((count, each) => count + each.size, 0);
  // Never down to nothing: one repository at the population bound is allowed
  // to exceed `ENTRIES` by itself, because the alternative is evicting the
  // sweep that is running.
  const bound = yield* memoOf();
  while (mentions.size > 1 && (mentions.size > bound.repositories || total > bound.entries)) {
    const oldest = mentions.keys().next();
    if (oldest.done === true) break;
    total -= mentions.get(oldest.value)?.size ?? 0;
    mentions.delete(oldest.value);
  }
  return heads.has(to);
});

export interface Allowed {
  readonly update: RefUpdate;
  /** What the ref was when the decision was made — the compare-and-swap. */
  readonly expected: Oid | null;
}

export type Decision =
  | { readonly ok: true; readonly allowed: Allowed }
  | { readonly ok: false; readonly ref: string; readonly reason: string };

/** The refusing half, named so a caller holding one need not re-narrow it. */
export type Refusal = Extract<Decision, { readonly ok: false }>;

const refused = (ref: string, reason: string): Refusal => ({ ok: false, ref, reason });

/**
 * Whether a protected branch asks anything of the *revision* arriving on it.
 *
 * A branch can be protected and still take a direct push: "no force-push, no
 * deletion" is protection that asks nothing of what is being pushed, and
 * `protectedBranch` returns early there. Exported because a caller deciding
 * whether a queue has anything to do on a branch has to ask the same question
 * — asked as "is it protected?" alone, a runner refused to build candidates
 * for a branch whose pushes the boundary would simply have allowed.
 */
export const needsReview = (rules: Rules): boolean =>
  rules.requirePullRequest ||
  rules.requiredApprovals > 0 ||
  rules.requiredChecks.length > 0 ||
  rules.requireResolvedThreads;

/**
 * Whether a rule covers this ref — or, for a caller naming a namespace,
 * anything in it.
 *
 * `gateWrite` is asked about `refs/tags/*` by the verbs that write a whole
 * namespace at once: a fetch writes every tag the remote has, and it does not
 * know their names until the negotiation is over. Compared as a literal name,
 * `refs/tags/*` matched a rule protecting `refs/tags/*` and missed one
 * protecting `refs/tags/v*`, so the narrower rule was the one with the hole in
 * it. Two patterns that overlap *anywhere* are treated as a match, which is the
 * conservative reading and the only one a namespace-wide write can be held to.
 */
export const isProtected = (rules: Rules, ref: string): boolean => {
  const asked = ref.endsWith("*") ? ref.slice(0, -1) : null;
  return rules.protected.some((pattern) => {
    if (!pattern.endsWith("*")) return asked === null ? ref === pattern : pattern.startsWith(asked);
    const prefix = pattern.slice(0, -1);
    return asked === null
      ? ref.startsWith(prefix)
      : // Both are prefixes: they overlap when either contains the other.
        prefix.startsWith(asked) || asked.startsWith(prefix);
  });
};

/**
 * How far a provenance check will walk a push.
 *
 * The rule is about every commit a push introduces, and a push chooses how
 * many that is. Bounded for the reason every other walk on this path is: an
 * unbounded one is a push that never returns rather than a push that is
 * refused, and the refusal says which bound it hit.
 */
const MAX_PROVENANCE = 4096;

/**
 * Whether every commit this push introduces names the session that made it.
 *
 * The session refs of the *same push* count. A branch and the record of what
 * produced it arrive in one receive-pack — that is the flow the rule is for —
 * so reading the session as it stands on disk would refuse exactly the push
 * that did everything right.
 */
const provenanceOf = Effect.fn("Policy.provenanceOf")(function* (input: {
  readonly update: RefUpdate;
  readonly current: Oid | null;
  readonly sessions: ReadonlyMap<string, Oid>;
}) {
  const repository = yield* Repository;
  const to = input.update.value;
  if (to === null) return null;

  // Bounded by everything this repository already reaches, not by the ref's
  // own previous value alone. A create has no previous value, so a walk
  // bounded that way went to the root and demanded a trailer on every commit
  // the branch was cut from — `git push origin HEAD:refs/heads/feature` was
  // refused on the first commit predating the rule, which is not what the rule
  // says (agents.md §9: what the push *introduces*).
  //
  // Every ref's tip is a boundary, which covers what a branch is actually cut
  // from: `main`, another topic, a tag. A commit reachable from an existing
  // ref without being a tip is still re-judged, and that residual is why this
  // is a bound and not a connectivity check — the alternative is walking every
  // ref's whole history on every push.
  const stop = new Set<Oid>();
  for (const [, value] of yield* repository.refs) stop.add(value);
  stop.delete(to);

  const introduced = yield* Dag.reachable(
    to,
    input.current,
    (commit) => Effect.succeed(!stop.has(commit)),
    MAX_PROVENANCE,
  ).pipe(
    // Every failure here is a refusal, not an error. Receive-pack checks only
    // that the tip object exists, so a commit naming a parent that never
    // arrived is a push this cannot judge — and every other walk in this file
    // catches the same thing rather than letting it out, because escaping
    // turns a refusable push into a 404 anybody holding `source.push` can ask
    // for on demand.
    Effect.catchTags({
      Invalid: () => Effect.succeed(null),
      ObjectNotFound: () => Effect.succeed(null),
      StorageFailure: () => Effect.succeed(null),
    }),
  );
  if (introduced === null) {
    return `${input.update.name} could not be walked for provenance; it introduces more than ${MAX_PROVENANCE} commits, or names an object this repository does not hold`;
  }

  // Walked once per session named, not once per commit: a push of fifty
  // commits from one session reads that session's events once.
  const produced = new Map<string, ReadonlySet<string>>();
  for (const commit of introduced.keys()) {
    const info = yield* repository.readCommit(commit).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        StorageFailure: () => Effect.succeed(null),
      }),
    );
    if (info === null) return `${commit} cannot be read, so its provenance cannot be judged`;

    const named = Session.trailerOf(info.message);
    if (!("session" in named)) {
      return `${commit} has ${named.reason}; this branch requires one`;
    }

    if (!produced.has(named.session)) {
      const head = input.sessions.get(named.session) ?? null;
      produced.set(
        named.session,
        head === null ? new Set<string>() : yield* Session.producedBy(head),
      );
    }
    if (produced.get(named.session)?.has(commit) !== true) {
      return `session ${named.session} does not say it produced ${commit}`;
    }
  }

  return null;
});

export interface Principal {
  /** `null` for an anonymous request, which may never write. */
  readonly member: Member | null;
  /** What the request may do — a delegated credential narrows this. */
  readonly capabilities: ReadonlyArray<string>;
}

const may = (principal: Principal, capability: string): boolean =>
  principal.member !== null && permits(principal.capabilities, capability);

/**
 * Judge one ref update.
 *
 * Returns the value the ref held at decision time, so the caller can apply it
 * under exactly that condition.
 */
export const evaluate = Effect.fn("Policy.evaluate")(function* (input: {
  readonly update: RefUpdate;
  readonly principal: Principal;
  readonly genesis: Genesis | null;
  readonly trust: TrustProjection | null;
  readonly rules: Rules;
  /**
   * Pull requests already folded, shared across one batch of updates.
   *
   * A push moving several protected branches otherwise folds every pull
   * request — a DAG walk and a signature verification per event — once per
   * ref. The map is the caller's, so nothing survives the request that built
   * it, and a fold cannot go stale inside one.
   */
  /**
   * Where each session named by this push stands, the push's own moves
   * included; see `provenanceOf`.
   */
  readonly sessions?: ReadonlyMap<string, Oid>;
  readonly folds?: FoldCache;
  /** As `folds`, for the walk that decides which pull requests to fold. */
  readonly mentions?: MentionCache;
  /** As `folds`, for the merges a candidate chain re-derives. */
  readonly merges?: MergeCache;
  /** As `folds`, for the bounds a batch could otherwise outrun; see `Openings`. */
  readonly opening?: Openings;
}) {
  const repository = yield* Repository;
  const { update } = input;
  // A batch of one when the caller did not bring a map, which is what every
  // direct caller outside `gate` is.
  const folds: FoldCache = input.folds ?? new Map();
  const mentions: MentionCache = input.mentions ?? new Map();
  const merges: MergeCache = input.merges ?? new Map();
  const opening: Openings = input.opening ?? new Set();
  const current = yield* repository.resolve(update.name);
  // Two readings of "what the ref is now", and they differ for a symbolic ref.
  // Reachability wants the commit it resolves to; the compare-and-swap wants
  // exactly what the store will compare against, which is the ref's own value
  // — `null` for a symref. Using the resolved oid as `expected` made every
  // gated write to a symbolic ref fail as a conflict against a value that was
  // never written there.
  const stored = yield* repository.readRef(update.name);
  // Walked once and handed to every rule that asks "does the ref already
  // reach this?". Asked per candidate, it was an unbounded ancestry walk per
  // commit the push adds — twice over, since two rules ask.
  //
  // Cached rather than computed: the rules that read it sit behind the
  // membership check and behind the refusals that need nothing from it, so
  // walking here meant a `source.delete` holder could spend a whole trust-log
  // walk per command on updates this refuses out of hand.
  const nothing: ReadonlySet<Oid> = new Set();
  const held = yield* Effect.cached(
    Refspec.isAppendOnly(update.name) ? alreadyHeld(update.name, current) : Effect.succeed(nothing),
  );

  // Identity is not a thing a push may edit. This is checked before anything
  // about membership, because a repository whose genesis can move has no
  // membership worth checking.
  if (update.name === Refspec.TRUST_GENESIS && current !== null) {
    return refused(update.name, "the genesis is written once and never moves");
  }

  // And the namespace holds only the two things it is for. The rules below
  // know `refs/meta/trust/log` and the genesis; every other name under
  // `refs/meta/trust/` is not append-only, so nothing here bounds it — and the
  // API refuses the whole namespace, so this door was the only one open. Such
  // a ref is hidden from the advertisement, copied to every mirror by the hub
  // refspec, which only adds refs, and roots collection: a permanent,
  // invisible pin on the object graph of every replica, created by anybody
  // holding `source.push`. Refused whether or not the repository has an
  // identity: the namespace means what it means either way.
  if (
    update.name.startsWith("refs/meta/trust/") &&
    update.name !== Refspec.TRUST_GENESIS &&
    !Refspec.isAppendOnly(update.name)
  ) {
    return refused(update.name, `${update.name} is not part of the trust namespace`);
  }
  if (update.name.startsWith("refs/social/") && update.name !== Refspec.SOCIAL_LOG) {
    return refused(update.name, `${update.name} is not part of the social namespace`);
  }
  if (update.name.startsWith("refs/quarantine/")) {
    return yield* quarantineRules({
      update,
      current,
      stored,
      principal: input.principal,
      enabled: input.genesis !== null && input.trust !== null,
    });
  }

  // Not hub-enabled: no genesis, so no members, so nobody holds `source.push`
  // — and §14 is unconditional that anonymous does not get it. A repository
  // with no identity is readable by anyone who can reach it, exactly as a
  // plain git repository has always been, and writable over the network by
  // nobody: there is no membership graph for it to be judged against, and
  // "no policy" must not read as "no protection". `hub init` is what gives a
  // repository something to say about who may write to it.
  //
  // That also stops a repository acquiring an identity by push. Whoever got
  // there first would own it, and its actual owner would be locked out of
  // their own repository by a stranger.
  if (input.genesis === null || input.trust === null) {
    // Identity is never acquired by push, whatever the host allows: whoever
    // got there first would own the repository, and its actual owner would be
    // locked out of it by a stranger.
    if (update.name.startsWith("refs/meta/trust/") || update.name.startsWith("refs/social/")) {
      return refused(
        update.name,
        "a repository's identity is established by `hub init`, not by a push",
      );
    }
    const open = yield* Effect.serviceOption(Auth.AnonymousWrites);
    if (!Option.getOrElse(open, () => false)) {
      return refused(
        update.name,
        "this repository has no membership to authorize a write; run `hub init` to give it one",
      );
    }
    const namespace = yield* namespaceRules(update, current, stored, held, opening);
    if (!namespace.ok || !isProtected(input.rules, update.name)) return namespace;

    // Branch protection still applies to a repository with no identity. The
    // *approval* half of it cannot — there is no membership for a review to
    // come from — but "this branch may not be deleted or force-pushed" asks
    // nothing of trust, and returning before the rules were consulted made a
    // published `refs/meta/policy` inert on exactly the repositories that have
    // no other protection at all. On an open repository the rules ref is
    // itself writable, so this guards a mistake rather than an adversary; that
    // is still the difference between a protected branch and no branch
    // protection whatsoever.
    if (update.value === null) {
      return refused(update.name, `${update.name} is protected and may not be deleted`);
    }
    if (current !== null && !(yield* contains(current, update.value))) {
      return refused(update.name, `${update.name} is protected and may not be force-pushed`);
    }
    return namespace;
  }

  if (input.principal.member === null) {
    return refused(update.name, "authentication required to write refs");
  }

  // These namespaces are written by the hub and by trust operations, not by
  // whoever may push source. Charged `source.push` alone, an ordinary
  // contributor could chain commits onto either — both read an empty tree as a
  // join, so none of it need be a statement — until the ref hit the ceiling a
  // fold will walk, and then, on a namespace with no way back, it refused
  // every later push: the revocation of the padder, the checkpoint that lifts
  // a staleness bound, the approval a protected branch was waiting on.
  //
  // *Some* capability of the right kind, not a particular one: which event or
  // record a commit carries is settled by the fold, which reads the payload
  // this boundary has not got. The point here is that the door belongs to
  // members of the hub rather than to everyone who may push.
  if (update.value !== null && Refspec.isAppendOnly(update.name)) {
    // The trust log's kind is `member.*`: what a trust record says is who may
    // do what, and the two capabilities that say it are `member.invite` and
    // `member.revoke`. Naming a `trust.*` prefix here charged a capability
    // that does not exist, which reads as tighter and is in fact a lockout —
    // `repo.admin` and nobody else could grow the log, so a `member.revoke`
    // holder could sign a revocation and never publish it.
    //
    // Task and session refs are charged their own capability rather than the
    // kind: their folds deliberately never re-judge a signer (which is what
    // makes two hosts agree about a task), so the boundary is the only place
    // `hub.task` and `hub.session` are ever asked for — charged "any hub.*",
    // a `hub.comment` holder could open, claim and close tasks. A redaction
    // holder still passes: a tombstone is the one event either namespace
    // accepts from a signer outside its own capability.
    //
    // A queue ref is charged `hub.queue` and nothing else, redaction included:
    // every field a queue record carries is an identifier, an object id or a
    // ref name, so the namespace has no tombstone to admit a redactor for. See
    // `hub/Queue.ts`.
    const needed = (name: string): { exact: ReadonlyArray<string> } | { prefix: string } =>
      name === Refspec.SOCIAL_LOG
        ? { exact: ["social.write"] }
        : Task.taskOf(name) !== null
          ? { exact: ["hub.task", "hub.redact"] }
          : Session.sessionOf(name) !== null
            ? { exact: ["hub.session", "hub.redact"] }
            : Queue.queueOf(name) !== null
              ? { exact: ["hub.queue"] }
              : name.startsWith("refs/hub/")
                ? { prefix: "hub." }
                : { prefix: "member." };
    const charge = needed(update.name);
    const passes =
      "exact" in charge
        ? charge.exact.some((capability) => permits(input.principal.capabilities, capability))
        : input.principal.capabilities.some(
            (held) => held.startsWith(charge.prefix) || held === "repo.admin",
          );
    if (!passes) {
      const wanted =
        "exact" in charge ? charge.exact.join(" or ") : `a ${charge.prefix}* capability`;
      return refused(update.name, `appending to ${update.name} needs ${wanted}`);
    }
  }

  const deleting = update.value === null;
  const forced = !deleting && current !== null && !(yield* contains(current, update.value!));

  if (deleting && !may(input.principal, "source.delete")) {
    return refused(update.name, "deleting a ref needs source.delete");
  }
  if (forced && !may(input.principal, "source.force-push")) {
    return refused(update.name, "this push drops commits and needs source.force-push");
  }
  if (!deleting && !may(input.principal, "source.push")) {
    return refused(update.name, "pushing needs source.push");
  }

  // The ref that decides what the rules are cannot be governed by them: any
  // `source.push` holder could otherwise rewrite the branch protection they
  // are subject to. `policy.write` exists for exactly this ref.
  if (update.name === RULES_REF && !may(input.principal, "policy.write")) {
    return refused(update.name, `writing ${RULES_REF} needs policy.write`);
  }

  const namespace = yield* namespaceRules(update, current, stored, held, opening);
  if (!namespace.ok) return namespace;

  // An event's trust head is written by its own signer, and the only thing the
  // fold holds it to is the floor its ancestors raise it to. A pull request
  // that has just been opened has no ancestors and so no floor, which made a
  // revocation have no effect on new pull requests at all: name a
  // pre-revocation head on the `pr.opened`, and it becomes the floor for an
  // approval signed by the revoked key against that same head — `reaches` sees
  // the revocation as unreachable, `former` supplies the capabilities it had,
  // and the approval satisfies a protected branch. Nothing in the events says
  // when they were written, but the boundary knows whose keys are revoked
  // *now*, and a signature arriving now from a key this repository has already
  // revoked is one it has no reason to take.
  if (
    (update.name.startsWith("refs/hub/") || update.name === Refspec.SOCIAL_LOG) &&
    update.value !== null
  ) {
    const refusal = yield* signedByRevoked(update.name, update.value, current, input.trust, held);
    if (refusal !== null) return refused(update.name, refusal);
  }

  // Asked of every branch this rule covers, before protection is considered:
  // provenance is about what a commit *is*, not about which branch it landed
  // on, and a repository that turned this on meant all of them.
  if (
    input.rules.requireProvenance &&
    update.name.startsWith("refs/heads/") &&
    update.value !== null
  ) {
    const missing = yield* provenanceOf({
      update,
      current,
      sessions: input.sessions ?? new Map(),
    });
    if (missing !== null) return refused(update.name, missing);
  }

  if (!isProtected(input.rules, update.name)) return namespace;

  if (deleting) return refused(update.name, `${update.name} is protected and may not be deleted`);
  if (forced)
    return refused(update.name, `${update.name} is protected and may not be force-pushed`);
  const protectedValue = update.value;
  if (protectedValue === null) {
    return refused(update.name, `${update.name} is protected and may not be deleted`);
  }

  const gate = yield* protectedBranch({
    ref: update.name,
    from: current,
    to: protectedValue,
    genesis: input.genesis,
    trust: input.trust,
    rules: input.rules,
    folds,
    mentions,
    merges,
  });
  return gate ?? namespace;
});

/**
 * The rules that come from the namespace rather than from configuration.
 *
 * Hub and trust refs only grow. A `+` in a refspec does not change that: it is
 * a preference of whoever wrote the config, and this is what the namespace
 * means.
 */
const namespaceRules = Effect.fn("Policy.namespaceRules")(function* (
  update: RefUpdate,
  current: Oid | null,
  stored: Oid | null,
  /** What the ref already reaches, walked at most once per push; see `alreadyHeld`. */
  held: Effect.Effect<ReadonlySet<Oid>, StorageFailure, Repository>,
  /** The hub refs this same batch has already been allowed to create; see `Openings`. */
  opening: Openings,
) {
  // The client's own old-oid wins when it declared one. A push that names the
  // value it believes the ref holds is asserting something, and replacing that
  // with whatever the ref holds *now* would make every stale push succeed —
  // which is the check receive-pack's old-oid exists to perform. Where the
  // client said nothing, the value read at decision time is the guarantee,
  // so the approvals counted a moment ago cannot be applied to a moved head.
  const expected = update.expected === undefined ? stored : update.expected;
  const allowed: Decision = { ok: true, allowed: { update, expected } };
  if (!Refspec.isAppendOnly(update.name)) return allowed;

  if (update.value === null) {
    return refused(update.name, `${update.name} is append-only and may not be deleted`);
  }

  // A ref in this namespace is a pull request, a session, a task or a queue,
  // and nothing else. `refs/hub/` as a whole is undeletable, so a name outside
  // those shapes is a permanent entry nothing counts, nothing folds and nothing
  // can ever remove — one more object graph pinned into every ref listing, every
  // advertisement, every collection root and every memo key for the life of
  // the repository.
  if (
    update.name.startsWith("refs/hub/") &&
    Event.prOf(update.name) === null &&
    Session.sessionOf(update.name) === null &&
    Task.taskOf(update.name) === null &&
    Queue.queueOf(update.name) === null
  ) {
    return refused(
      update.name,
      `${update.name} does not name a pull request, session, task or queue`,
    );
  }

  // And its value is a commit of this namespace's own kind. Nothing else here
  // asks: the ceiling walk steps over a foreign head and reports an empty
  // history, and the graft check steps over it too — so a `source.push`
  // holder could point a hub ref at any source commit at all, on a name that
  // can never be deleted, pinning whatever it reaches out of reach of `gc`
  // for good.
  const belongs = namespaceOf(update.name) ?? TRUST_NAMESPACE;
  if (!(yield* belongs.isCommit(update.value))) {
    return refused(update.name, `${update.value} is not part of ${update.name}'s history`);
  }

  // Asked before the ref exists as well as after. Returning early on a create
  // let the first push of `refs/hub/pr/<new-id>` bring a history of any size
  // at all — onto a namespace nothing can delete, so every later fold, every
  // protected-branch push and every collection paid for it forever.
  const oversized = yield* beyondCeiling(update.name, update.value);
  if (oversized !== null) return refused(update.name, oversized);

  // A *new* pull request, on a namespace whose entries are never removed. The
  // per-pull-request ceiling bounds one fold; this bounds how many folds a
  // protected-branch push, a collection and a deepening fetch each have to
  // make, which is otherwise chosen by anybody holding `hub.create-pr`.
  if (current === null && update.name.startsWith("refs/hub/")) {
    // The batch's own creates count. A push is judged in full before any of it
    // is applied, so every create in one receive-pack read the same pre-push
    // count and every one of them passed: the bound said 65 536 and one push
    // could open as many pull requests as it liked. `refs/hub/*` is
    // undeletable, so what that costs every later protected-branch push,
    // collection and deepening fetch is permanent.
    //
    // Counted per class. Sessions are opened far faster than pull requests —
    // one per agent run rather than one per proposal — so a shared bound would
    // let a fleet's ordinary week exhaust what a repository's pull requests
    // are allowed, and a session ref is exactly as undeletable as a pull
    // request's.
    const classOf = (name: string): "sessions" | "tasks" | "queues" | "pull requests" =>
      Session.sessionOf(name) !== null
        ? "sessions"
        : Task.taskOf(name) !== null
          ? "tasks"
          : Queue.queueOf(name) !== null
            ? "queues"
            : "pull requests";

    const kind = classOf(update.name);
    const held =
      kind === "sessions"
        ? yield* Session.sessions()
        : kind === "tasks"
          ? yield* Task.tasks()
          : kind === "queues"
            ? yield* Queue.queues()
            : yield* Event.pullRequests();
    const opened = [...opening].filter((name) => classOf(name) === kind).length;
    const count = held.length + opened;
    if (count >= (yield* Event.populationOf())) {
      return refused(update.name, `this repository already holds ${count} ${kind}`);
    }
  }

  // And it may not graft a second beginning onto the history, or an edge out
  // of it. Every event a hub ref carries is written onto the ref's current
  // head, so an append-only history has exactly one parentless commit: a pull
  // request's `pr.opened`, a trust log's first record. A push that brings a
  // *new* one is not adding to this history, it is adding another one beside
  // it — and a fold with two roots has to choose between them by something,
  // which on a pull request with no activity yet can only be the oid, which
  // whoever wrote the commit ground. That is how a `hub.create-pr` holder took
  // the authorship, the title and the base of a pull request they had no part
  // in opening. Asked before the ref exists as well as after: the create is
  // where a first push could otherwise hang an event off a source commit, or
  // bring several competing openings at once.
  // Containment first: an update that drops what the ref held is refused for
  // that, and reporting it as a graft would be true but unhelpful.
  if (current !== null && !(yield* contains(current, update.value))) {
    return refused(
      update.name,
      `${update.name} is append-only: the update must contain ${current}`,
    );
  }

  const grafted = yield* orphanBeyond(update.name, current, update.value, held);
  if (grafted !== null) {
    return refused(
      update.name,
      `${update.name} is append-only: ${grafted.commit} ${grafted.reason}`,
    );
  }

  // Counted once it has passed everything here, so a create this function goes
  // on to refuse does not consume a slot from the one beside it.
  if (current === null && update.name.startsWith("refs/hub/")) opening.add(update.name);

  return allowed;
});

const MAX_INBOX_PROPOSALS = 4096;

/** The only ref transition an unauthenticated receive-pack may perform. */
const quarantineRules = Effect.fn("Policy.quarantineRules")(function* (input: {
  readonly update: RefUpdate;
  readonly current: Oid | null;
  readonly stored: Oid | null;
  readonly principal: Principal;
  readonly enabled: boolean;
}) {
  const { update } = input;
  if (!input.enabled) {
    return refused(update.name, "an inbox belongs to an identified repository");
  }
  if (!update.name.startsWith(Inbox.PENDING_PREFIX)) {
    return refused(update.name, `${update.name} is not an inbox proposal ref`);
  }
  if (!input.principal.capabilities.includes(Auth.INBOX_SUBMIT)) {
    return refused(update.name, "the inbox may only be written by a quarantine operation");
  }
  const id = update.name.slice(Inbox.PENDING_PREFIX.length);
  if (!Inbox.isProposalId(id)) {
    return refused(update.name, "an inbox proposal ref must end in a UUIDv7");
  }
  if (update.value === null) {
    return refused(update.name, "an inbox proposal may not be deleted over receive-pack");
  }
  if (input.current !== null || input.stored !== null) {
    return refused(update.name, `inbox proposal '${id}' already exists`);
  }
  const repository = yield* Repository;
  if ((yield* repository.resolve(`${Inbox.ADOPTED_PREFIX}${id}`)) !== null) {
    return refused(update.name, `inbox proposal '${id}' has already been adopted`);
  }
  const commit = yield* repository.readCommit(update.value).pipe(
    Effect.as(true),
    Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)),
  );
  if (!commit) return refused(update.name, "an inbox proposal must point at a commit");

  let count = 0;
  for (const [name] of yield* repository.refs) {
    if (name.startsWith(Inbox.PENDING_PREFIX)) count++;
  }
  if (count >= MAX_INBOX_PROPOSALS) {
    return refused(update.name, `this inbox already holds ${count} pending proposals`);
  }
  const expected = update.expected === undefined ? input.stored : update.expected;
  return { ok: true, allowed: { update, expected } } satisfies Decision;
});

/**
 * One append-only namespace, as the rules below need to see it.
 *
 * The three of them — hub, trust, social — differ in their record name, their
 * fold ceiling, what counts as one of their commits, and whether their history
 * descends from the genesis. Every rule that walks such a ref needs some of
 * that, and each used to re-derive "which namespace is this" from the ref name
 * itself and reach for that namespace's helpers by hand, in six places here
 * and once more in `Replication`.
 *
 * Which made adding a namespace a change nobody could see the whole of, and
 * every miss silent in the dangerous direction: one omitted here skips the
 * revocation screening rather than failing, and one omitted in the ceiling
 * walks the ref unbounded on the synchronous push path.
 */
interface LogNamespace {
  /** Whether a commit belongs to this namespace's own history. */
  readonly isCommit: (commit: Oid) => Effect.Effect<boolean, StorageFailure, Repository>;
  readonly withinCeiling: (
    head: Oid,
  ) => Effect.Effect<boolean, ObjectNotFound | StorageFailure, Repository>;
  readonly ceilingOf: () => Effect.Effect<number>;
  readonly record: string;
  /**
   * Whether this namespace's history descends from the trust genesis.
   *
   * Hub refs do not: each is its own root. The trust and social logs both hang
   * off the genesis, which is the one legitimate edge out of their namespace.
   */
  readonly anchored: boolean;
  /** What this namespace holds, for the message a ceiling refusal carries. */
  readonly holds: string;
}

const HUB_NAMESPACE: LogNamespace = {
  isCommit: Event.isHubCommit,
  withinCeiling: Event.withinCeiling,
  ceilingOf: Event.ceilingOf,
  record: Event.RECORD,
  anchored: false,
  holds: "events",
};

const TRUST_NAMESPACE: LogNamespace = {
  isCommit: Log.isTrustCommit,
  withinCeiling: Log.withinCeiling,
  ceilingOf: Log.ceilingOf,
  record: Log.RECORD,
  anchored: true,
  holds: "records",
};

const SOCIAL_NAMESPACE: LogNamespace = {
  isCommit: SocialLog.isSocialCommit,
  withinCeiling: SocialLog.withinCeiling,
  ceilingOf: SocialLog.ceilingOf,
  record: SocialLog.RECORD,
  anchored: true,
  holds: "statements",
};

/**
 * Which append-only namespace a ref belongs to, or `null` for an ordinary ref.
 *
 * The one place the ref-name spellings are read. `Refspec.isAppendOnly` names
 * exactly these three, so a ref it accepts always answers here.
 */
const namespaceOf = (ref: string): LogNamespace | null =>
  ref.startsWith("refs/hub/")
    ? HUB_NAMESPACE
    : ref === Refspec.SOCIAL_LOG
      ? SOCIAL_NAMESPACE
      : ref === Refspec.TRUST_LOG
        ? TRUST_NAMESPACE
        : null;

/**
 * Why an append-only ref may not hold this value, or `null`.
 *
 * Both namespaces are folded on paths that cannot wait — a protected-branch
 * push, a membership check, a collection — and both are bounded in what a fold
 * will walk. The bound belongs here as well as at the fold: applied only
 * there, it converts a slow push into a ref that can never be read again, on a
 * namespace with no way to shorten it. So the ref is never allowed to get
 * there in the first place.
 */
const beyondCeiling = Effect.fn("Policy.beyondCeiling")(function* (name: string, to: Oid) {
  const namespace = namespaceOf(name);
  if (namespace === null) return null;
  return (yield* namespace.withinCeiling(to))
    ? null
    : `${name} would hold more ${namespace.holds} than a fold will walk`;
});

/**
 * Whether a commit is one this push is *adding* to a hub ref.
 *
 * A hub commit, and one the ref does not already reach. `Dag.reachable`'s
 * boundary cuts only the chain that runs through the named oid, and a join has
 * a second parent — so an ordinary reconciling push walked back to the root
 * and re-read every event already on the ref.
 */
const added = Effect.fnUntraced(function* (commit: Oid, held: ReadonlySet<Oid>) {
  if (held.has(commit)) return false;
  return yield* Event.isHubCommit(commit);
});

/**
 * Everything an append-only ref already reaches, walked once.
 *
 * Asked per candidate through `isAncestor`, this was an unbounded walk of the
 * ref's whole history *per commit the push adds* — and twice per push, since
 * two rules ask it. One bounded walk answers all of them: the namespace's own
 * commits, stopping at the ceiling a fold would stop at anyway, so the reads
 * are the ones the fold was going to make regardless.
 */
const alreadyHeld = Effect.fn("Policy.alreadyHeld")(function* (name: string, current: Oid | null) {
  const held = new Set<Oid>();
  if (current === null) return held;

  const repository = yield* Repository;
  const namespace = namespaceOf(name) ?? TRUST_NAMESPACE;
  const anchor = namespace.anchored ? yield* repository.resolve(Refspec.TRUST_GENESIS) : null;
  // Bounded by the *ceiling*, not by the namespace. Bounded by the namespace,
  // the walk stopped at a commit whose tree object never arrived — a state
  // every other walk here deliberately steps over, because refs are applied
  // without a connectivity check — and everything behind it dropped out of
  // the set, so the next ordinary reconciling push met an unaccounted parent
  // and was refused for good on a ref that cannot be rewound.
  const ceiling = yield* namespace.ceilingOf();

  // Walked here rather than through `Dag.reachable` for the same tolerance in
  // the other direction: a *commit* object that never arrived is still a
  // commit this ref reaches, so it is recorded and simply not descended
  // through. Failing on it — which is what an answer of "this ref holds
  // nothing I can name" amounted to — left the two rules that read this set
  // disagreeing about what to do with it, one waving a graft through and the
  // other refusing an ordinary join for good.
  const pending: Oid[] = [current];
  while (pending.length > 0 && held.size < ceiling) {
    const oid = pending.pop();
    if (oid === undefined) break;
    if (held.has(oid) || oid === anchor) continue;
    held.add(oid);

    const info = yield* repository
      .readCommit(oid)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (info === null) continue;
    for (const parent of info.parents) if (!held.has(parent)) pending.push(parent);
  }
  return held;
});

/**
 * A revoked signer on an event the push is adding, or `null`.
 *
 * Narrow on purpose. Refusing every event whose declared trust head predates a
 * revocation would catch the honest straggler too — somebody who signed a
 * comment minutes before a revocation landed — and on an append-only ref that
 * is a pull request they can never push again. What actually needs stopping is
 * a *new signature from a key this repository has already revoked*, which no
 * honest client produces: the key is gone from its holder's hands or should
 * be, and the events it signed before the revocation are already on the ref
 * and are not re-judged.
 *
 * Only what the push adds is walked, so an ordinary push reads one commit.
 */
const signedByRevoked = Effect.fn("Policy.signedByRevoked")(function* (
  ref: string,
  to: Oid,
  current: Oid | null,
  trust: TrustProjection,
  held: Effect.Effect<ReadonlySet<Oid>, StorageFailure, Repository>,
) {
  // Stopped at everything the ref already reaches, not at the tip alone. A
  // boundary of one oid only cuts the chain that runs through it, and a join
  // has a second parent — so an ordinary reconciling push walked back to the
  // root and re-judged every event already on the ref. One old comment from a
  // member revoked since then made that pull request refuse its own joins for
  // good, on a namespace that cannot be rewound.
  const social = ref === Refspec.SOCIAL_LOG;
  const reached = yield* held;
  const parents = yield* Dag.reachable(to, current, (commit) =>
    social
      ? reached.has(commit)
        ? Effect.succeed(false)
        : SocialLog.isSocialCommit(commit)
      : added(commit, reached),
  ).pipe(
    // An unreadable history is not a signature claim. It is refused, or passed
    // over, by the walks that own that question.
    Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
  );
  if (parents === null) return null;

  // Only hub and social refs reach here — see the guard at the call site — so
  // the namespace is one of those two, and its record name comes from the
  // table rather than from a second reading of the ref name.
  const recordName = (namespaceOf(ref) ?? HUB_NAMESPACE).record;
  for (const oid of parents.keys()) {
    if (!(yield* Record.carries(oid, recordName))) continue;
    const record = yield* Record.read(oid, recordName).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) continue;
    const signed = yield* Verify.signers(record.payload, record.signatures);
    const kind = social
      ? yield* Statement.decode(record.payload).pipe(
          Effect.map((payload) => payload.type),
          Effect.orElseSucceed(() => null),
        )
      : yield* Event.decode(record.payload).pipe(
          Effect.map((payload) => payload.type),
          Effect.orElseSucceed(() => null),
        );

    // Every event, without an exemption list. Three attempts at one — first
    // "grants authority", then "moves authority", then a list of families —
    // each sprang a leak, because almost everything here feeds a branch rule
    // by some path: `checks` is keyed by name and head, so a `check.started`
    // *replaces* a completed success and flips the branch's checks to failing;
    // a `comment.created` opens an unresolved thread and fails
    // `requireResolvedThreads`; `pr.closed` makes the pull request authorize
    // nothing at all. A rule that has to enumerate the safe cases is a rule
    // that will be wrong again.
    //
    // The cost is the one the tombstone gate below already carries and is
    // worth stating in the same breath: a history that is entirely honest and
    // entirely old — a replica seeded from elsewhere, a client that has been
    // offline — stops being *pushable* once one of its past participants is
    // revoked. Replication is not gated here, so it still arrives by fetch;
    // what it cannot do is arrive by push.
    for (const signer of signed) {
      if (openWindow(trust.revoked.get(signer)) !== null) {
        return `${signer} has been revoked and may not add a ${kind ?? (social ? "statement" : "event")}`;
      }
    }

    // And a tombstone needs the capability *now*. The fold's first pass asks
    // only whether its signer ever held `hub.redact`, because that set has to
    // be monotone — an answer that shrinks leaves the host which already
    // deleted a payload folding a history no replica agrees with. Monotone and
    // generous is the right trade there and the wrong one here: a member whose
    // `hub.redact` was narrowed away, keeping `source.push`, could push decoy
    // tombstones naming the ancestors of a real one, drop them out of the
    // first pass and with them the trust floor, and have a tombstone signed
    // against a stale head accepted — sending somebody else's payload to `gc`.
    // The boundary is where "now" is knowable, so it is where that is refused.
    //
    // The cost is deliberate and worth stating: a pull request whose history
    // already carries a once-valid tombstone stops being *pushable* to a host
    // that does not hold it, once its signer's `hub.redact` is narrowed away.
    // Replication is not gated here, so such a history still reaches a replica
    // by fetch; what it cannot do is arrive by push. The alternative is to
    // judge the tombstone by what its signer held at the head it declares,
    // which is the fold's question — and the fold's question is exactly what
    // the decoy attack above is built to answer wrongly.
    // Asked of the bytes rather than of the decoded pull-request payload: a
    // session and a task write the same tombstone inside their own envelopes,
    // which `Event.decode` reads as nothing at all — so this gate covered
    // `refs/hub/pr/*` and left the two namespaces whose records are prompts,
    // the ones most likely to need removing, ungated.
    if (social || !(yield* Tombstone.claims(record.payload))) continue;
    // Expiry as well as the capability. A permanent verdict does not consult
    // expiry — it cannot, or the answer would move on a wall clock and the
    // host that acted on it would fold a history no replica agrees with — so
    // the *only* place an expired redactor is turned away is here. Left out,
    // a relayed tombstone from a membership that lapsed years ago was
    // honoured and `gc` destroyed the payload it named.
    const holds = signed.some((signer) => {
      const member = trust.members.get(signer);
      if (member === undefined || !permits(member.capabilities, "hub.redact")) return false;
      return member.expiresAt === null || member.expiresAt.getTime() > Date.now();
    });
    if (!holds) return "a redaction needs an unexpired hub.redact";
  }
  return null;
});

/**
 * A parentless commit the update brings that the ref did not already have.
 *
 * Walked from the new value and stopped at the current one, so the ordinary
 * push — a handful of commits on the tip — costs a handful of reads. A root
 * found this way may still be one the ref already reached by another path, and
 * a redundant join over an old commit is a strange thing to write but not a
 * dishonest one, so it is checked against the current value before being
 * called new.
 */
const orphanBeyond = Effect.fn("Policy.orphanBeyond")(function* (
  name: string,
  current: Oid | null,
  to: Oid,
  held: Effect.Effect<ReadonlySet<Oid>, StorageFailure, Repository>,
) {
  // Bounded to the namespace's own commits, like every other walk of them. An
  // unbounded one is what a hub commit naming a *source* commit as a second
  // parent turns into: the whole repository history, walked synchronously on
  // the receive-pack path, from a push of one tiny commit. The ceiling check
  // above does not catch it — that walk is bounded, so it simply steps over
  // the foreign parent and finds nothing to refuse.
  // Stopped at everything the ref already reaches, and not at the tip alone:
  // the boundary oid cuts only the chain running through it, and a join has a
  // second parent, so an ordinary reconciling push walked back to the root.
  //
  // Asked on a *create* as well, which is where the same hole reopened: only
  // the tip was inspected there, and the ceiling walk is itself bounded to the
  // namespace, so a first push of `refs/hub/pr/<fresh-uuid>` could hang one
  // event commit off a source commit and pin everything behind it out of
  // reach of `gc` for good.
  const namespace = namespaceOf(name) ?? TRUST_NAMESPACE;
  const belongs = namespace.isCommit;
  // The commit a trust log hangs off is not a record and never will be: it is
  // the genesis, the one legitimate edge out of that namespace. On every push
  // but the log's first it is already reachable from `current`; on the first
  // there is nothing else for the first record to name.
  const repository = yield* Repository;
  const anchor = namespace.anchored ? yield* repository.resolve(Refspec.TRUST_GENESIS) : null;
  const reached = yield* held;
  const parents = yield* Dag.reachable(to, current, (commit) =>
    Effect.gen(function* () {
      if (reached.has(commit)) return false;
      return yield* belongs(commit);
    }),
  ).pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  // Unreadable is not "no second root": the objects arrived with this push, so
  // a missing one is a push this cannot judge rather than one it may wave on.
  if (parents === null) return { commit: to, reason: "could not be read" } as const;

  // Collected rather than reported as they are found: which of several roots
  // is named would otherwise depend on the walk's order, and the answer to
  // "is there more than one" does not.
  const roots: Oid[] = [];
  for (const [commit, of] of [...parents].sort(([left], [right]) => (left < right ? -1 : 1))) {
    // A root of *this* history, which is not the same as a commit with no
    // parents at all. The walk holds only what the push adds, so a parent is
    // an edge of this DAG when the walk kept it or when the ref already
    // reached it — and anything else, a fabricated oid or a commit from the
    // source history included, is not an edge at all. Tested against the raw
    // parent list, a graft that named any junk oid read as attached and
    // slipped through: it then out-ranked the genuine opening on descent,
    // supplied the base and the author, and the real `pr.opened` was refused
    // as "re-opening somebody else's pull request" — freezing the protected
    // branch behind an approved pull request the boundary could no longer see.
    let attached = false;
    for (const parent of of) {
      if (parents.has(parent) || parent === anchor) {
        attached = true;
        continue;
      }
      if (reached.has(parent)) {
        attached = true;
        continue;
      }
      // A parent this DAG does not have. `gc` treats every ref as a root, so a
      // source commit named as a hub commit's second parent is a whole object
      // graph pinned out of reach of collection through a name that can never
      // be deleted — a purged secret among them. The anchor a trust log hangs
      // off is the one legitimate outside edge, and it is already reachable
      // from `current` on every push that is not the log's first.
      return { commit: parent, reason: "is not part of this history" } as const;
    }
    if (!attached) roots.push(commit);
  }

  // One beginning, and only when there was none before. An existing ref
  // already has its own, so any root a push brings is a second; a create is
  // allowed exactly one, and a second is the same graft arriving a step
  // earlier — several competing parentless `pr.opened` commits in the very
  // push that makes the pull request.
  const allowance = current === null ? 1 : 0;
  const extra = roots[allowance];
  if (extra !== undefined) return { commit: extra, reason: "begins a second history" } as const;
  return null;
});

/** Whether `to` can reach `from` — a fast-forward, or a join that kept it. */
const contains = Effect.fn("Policy.contains")(function* (from: Oid, to: Oid) {
  const repository = yield* Repository;
  if (from === to) return true;
  return yield* repository
    .isAncestor(from, to)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)));
});

/**
 * Whether an open pull request for this branch authorizes landing a revision.
 *
 * The pull request is found by what it *proposes* rather than by anything the
 * push carries: a client that could name its own pull request could name one
 * that was approved for something else.
 *
 * Two revisions, because a queue candidate separates them. `revision` is what a
 * pull request must currently propose — the thing that was reviewed, which is
 * the only thing an approval is ever about. `checkAt` is what required checks
 * must have run against, which for a direct push is the same revision and for a
 * candidate is the candidate itself: what a queue exists to test is the
 * combination being landed, not each pull request in isolation. Everything else
 * — the fold, the base match, the open state, self-approval, staleness — is one
 * reading shared by both paths, so neither can drift from the other.
 */
const authorizes = Effect.fn("Policy.authorizes")(function* (input: {
  readonly ref: string;
  readonly revision: Oid;
  readonly checkAt: Oid;
  readonly genesis: Genesis;
  readonly trust: TrustProjection;
  readonly rules: Rules;
  /** Shared across one batch; see `FoldCache`. */
  readonly folds: FoldCache;
  /** Shared across one batch; see `MentionCache`. */
  readonly mentions: MentionCache;
}) {
  const { rules } = input;

  /** The nearest miss, reported only if nothing else satisfies the rules. */
  let shortfall: Refusal | null = null;

  for (const id of yield* Event.pullRequests()) {
    // Folded, not sniffed. An earlier version read the base straight out of
    // the first-parent root's payload to skip pull requests cheaply — but
    // nothing had checked that payload's signature, so appending a commit
    // whose first parent was a forged `pr.opened` naming another base made an
    // approved pull request invisible here and its protected branch
    // unpushable. The fold is the only reading of a pull request that has
    // verified anything, so it is the only one this may act on. The cache is
    // what keeps a batch moving several protected branches from paying for it
    // once per ref.
    // Ruled out by a walk before it is ruled out by a fold. Only a pull
    // request that has *proposed this exact revision* can authorize moving the
    // branch to it, and whether it ever did is visible in its event payloads
    // without verifying a signature — the expensive half. A forged payload can
    // add the string and buy itself one wasted fold; it cannot remove it from
    // an honest event, so the filter can never hide a real pull request.
    //
    // Without this, every protected-branch push folded every pull request the
    // repository had ever had, closed and merged included, at a signature
    // verification per event.
    const cached = input.folds.get(id);
    if (cached === undefined && !(yield* proposes(id, input.revision, input.mentions))) continue;

    // One pull request this cannot read is one candidate, not a refusal of the
    // push. A history that arrived by replication is not held to the ceiling
    // the boundary applies to a push, so a replica can hold a pull request
    // larger than it will fold — and failing here would let whoever grew it
    // freeze a branch on every replica that copied it, which is the denial the
    // ceiling exists to prevent rather than a second way to arrive at it.
    const pullRequest =
      cached ??
      (yield* projectPr(input.genesis, input.trust, id).pipe(
        Effect.catchTag("Invalid", () => Effect.succeed(null)),
      ));
    if (pullRequest === null) continue;
    input.folds.set(id, pullRequest);
    if (pullRequest.base !== input.ref || pullRequest.head === null) continue;
    // A closed or already-merged pull request authorizes nothing further.
    // "Descends from an approved head" is not enough on its own: every commit
    // made after a merge descends from it, so one merged pull request would
    // otherwise unlock direct pushes to the branch forever.
    if (pullRequest.state !== "open") continue;

    // The revision has to *be* the reviewed one. A merge commit that merely
    // names it as a parent proves nothing about content — a merge's tree is
    // unconstrained, so anybody who may push could wrap whatever they liked
    // around an approved head.
    //
    // So a protected branch is advanced by *pushing the approved revision onto
    // it*, and by nothing else: an arbitrary merge commit cannot land there,
    // and neither can the API's ref-moving verbs, which are refused on a
    // protected ref by `gateWrite` because they name a branch and not the
    // revision these rules are about. A queue candidate is not an exception to
    // that rule but an application of it — it is admitted only because its tree
    // is re-derived rather than trusted (see `candidateChain`), and each step is
    // still asked this question about the revision it merges.
    if (input.revision !== pullRequest.head) continue;

    // Why this one did not satisfy the rules, kept rather than returned. Two
    // pull requests can propose the same revision, and refusing on the first
    // that falls short would let an unapproved duplicate block the approved
    // one purely by sorting first.
    const counted = yield* approvalsFor({
      genesis: input.genesis,
      pullRequest,
      id,
      ref: input.ref,
      rules,
    });
    const approvalCount = counted.count;
    if (counted.stale !== null) {
      shortfall ??= refused(input.ref, `external-review web is stale: ${counted.stale}`);
    }
    if (approvalCount < rules.requiredApprovals) {
      shortfall ??= refused(
        input.ref,
        `${id} has ${approvalCount} approvals of its current revision, ` +
          `and ${rules.requiredApprovals} are required`,
      );
      continue;
    }
    if (!checksPassedAt(pullRequest, rules.requiredChecks, input.checkAt)) {
      shortfall ??= refused(
        input.ref,
        input.checkAt === input.revision
          ? `${id} has not passed ${rules.requiredChecks.join(", ")}`
          : `${id} has not passed ${rules.requiredChecks.join(", ")} against candidate ${input.checkAt}`,
      );
      continue;
    }
    if (rules.requireResolvedThreads && pullRequest.threads.some((thread) => !thread.resolved)) {
      shortfall ??= refused(input.ref, `${id} has unresolved review threads`);
      continue;
    }
    return { satisfied: true, shortfall: null } as const;
  }

  return { satisfied: false, shortfall } as const;
});

/**
 * What a candidate chain is, or why this push is not one.
 *
 * `absent` and `refused` are deliberately different answers. A push that is not
 * shaped like a chain at all falls back to the direct path's own refusal, which
 * is the one its author needs to read; a push that *is* a chain and fails a step
 * gets that step's reason, which is the one a queue needs to read.
 */
type Chain =
  | { readonly kind: "verified" }
  | { readonly kind: "absent" }
  | { readonly kind: "refused"; readonly reason: string };

/**
 * The other way a protected branch may move: a chain of re-derived merges.
 *
 * Walk first parents from the pushed tip back to what the branch holds. Every
 * step must be a two-parent merge of one approved pull request's head onto the
 * step before it, and — the rule the whole thing rests on — its tree must be
 * *exactly* what merging those two parents produces. That last check is what
 * makes accepting a merge commit safe here when hub.md §11 refuses one
 * everywhere else: the tree is not taken on trust, it is recomputed, so a
 * candidate can carry nothing beyond the composition of revisions that were
 * each reviewed for this branch. Whoever built the chain is authorized with
 * nothing; the boundary re-derives every claim it makes.
 *
 * The steps are checked cheap-first: a step's pull request and the branch's
 * rules are settled before any merge is recomputed, so a chain nobody approved
 * costs a fold rather than a tree walk per step.
 *
 * Determinism is what makes the check meaningful, and it comes from both sides
 * calling one function: whoever builds a candidate and whoever verifies it both
 * ask `Repository.mergeTree`, so they agree about the base and the merge by
 * construction. A replica that cannot read the objects computes no answer and
 * refuses, which is the safe direction.
 *
 * What is constrained is a candidate's **content and its ancestry** — its tree,
 * its two parents, and the chain they form — and deliberately not its commit
 * header. The message, author and committer are whatever the builder wrote,
 * exactly as they are on any merge commit a member with `source.push` makes on
 * a branch of their own: they are not code, nothing downstream parses them (a
 * repository requiring provenance takes no candidates at all — see
 * `cli/queue.ts`), and pinning them would bake one builder's conventions into
 * the boundary. That last part is the point rather than an oversight: a
 * candidate is a shape, not a tool's output, so a person can build one by hand
 * and land it with no queue running at all, and two implementations can
 * interoperate. `cli/queue.ts` makes its own candidates a pure function of what
 * they merge — which is what lets a check bound to one runner's candidate name
 * another's — but that is a convention among runners, not a rule this enforces.
 */
const candidateChain = Effect.fn("Policy.candidateChain")(function* (input: {
  readonly ref: string;
  /** What the branch holds now — the foot of the chain. */
  readonly from: Oid | null;
  readonly to: Oid;
  readonly genesis: Genesis;
  readonly trust: TrustProjection;
  readonly rules: Rules;
  readonly folds: FoldCache;
  readonly mentions: MentionCache;
  readonly merges: MergeCache;
}) {
  const repository = yield* Repository;
  const { rules } = input;
  const absent: Chain = { kind: "absent" };

  // A chain merges onto something. A branch being created has nothing to merge
  // onto, and a repository that has not turned this on has no chains at all.
  if (!rules.queueCandidates || rules.queueDepth <= 0 || input.from === null) return absent;
  const from = input.from;

  const read = (commit: Oid) =>
    repository.readCommit(commit).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        StorageFailure: () => Effect.succeed(null),
      }),
    );

  // First parents only, and it must arrive exactly at what the branch holds.
  // `evaluate` has already established that the tip contains `from`, but
  // containment says nothing about *how* — a chain is a specific shape, and
  // anything else is not one.
  const steps: Array<{
    readonly commit: Oid;
    readonly onto: Oid;
    readonly head: Oid;
    readonly tree: Oid;
  }> = [];
  let at = input.to;
  while (at !== from) {
    if (steps.length >= rules.queueDepth) {
      // Both readings, because the shape alone cannot tell them apart. A
      // well-formed chain past the ceiling and an ordinary integration branch
      // of nine merges look identical from here, and each wants a different
      // half of this sentence: one needs the ceiling named, the other needs to
      // be told it is not a candidate at all and never was.
      return {
        kind: "refused",
        reason: `${input.ref} may only be moved by an approved pull request; ${input.to} is not one, and it does not reach ${from} within ${String(rules.queueDepth)} merge steps to be a queue candidate either`,
      } satisfies Chain;
    }
    const info = yield* read(at);
    if (info === null) return absent;
    const [onto, head] = info.parents;
    if (info.parents.length !== 2 || onto === undefined || head === undefined) return absent;
    steps.push({ commit: at, onto, head, tree: info.tree });
    at = onto;
  }
  if (steps.length === 0) return absent;
  // Oldest first, so a refusal names the earliest step that fails rather than
  // the last one walked.
  steps.reverse();

  // What the walk has already put behind each step. Every earlier step's commit
  // sits on the first-parent spine below this one, and every earlier step's head
  // is a second parent of one of those commits, so `onto` reaches all of them by
  // construction — that is what the walk just established, and no history walk
  // can say more. A step merging one of them merges nothing: its base is its own
  // `theirs`, so the tree it has to hold is the tree `onto` already had.
  //
  // Worth the bookkeeping because that is the only chain a single approved pull
  // request can make deep. Every step's head must be some open request's current
  // head, so with one open request the sole shape that authorizes is that head
  // merged over and over — and re-deriving each of those the long way is a full
  // history walk per step, `queueDepth` of them on the push path, none of which
  // the pair cache can absorb because every step's `onto` is different.
  const contained = new Set<Oid>([from]);
  let ontoTree: Oid | null = null;

  for (const step of steps) {
    const authorized = yield* authorizes({
      ref: input.ref,
      revision: step.head,
      // The candidate, not the head: what a required check has to have run
      // against is the combination this step lands. A check that passed on the
      // pull request alone says nothing about it merged with everything queued
      // ahead of it, which is the failure a queue exists to catch.
      checkAt: step.commit,
      genesis: input.genesis,
      trust: input.trust,
      rules,
      folds: input.folds,
      mentions: input.mentions,
    });
    if (!authorized.satisfied) {
      return {
        kind: "refused",
        reason:
          authorized.shortfall?.reason ??
          `${step.commit} merges ${step.head}, which no open pull request for ${input.ref} proposes as its head`,
      } satisfies Chain;
    }

    // And now the expensive half, asked only of a step everything else allows —
    // and asked once per pair within a request, however many times a caller
    // walks the chain, and not at all of a step whose shape already answers it.
    const key = `${step.onto}\u0000${step.head}`;
    const held: Oid | null = contained.has(step.head)
      ? (ontoTree ?? (yield* read(step.onto))?.tree ?? null)
      : null;
    const merged: MergeOf | null =
      input.merges.get(key) ??
      (held !== null
        ? { tree: held, conflicts: [] }
        : yield* repository.mergeTree({ ours: step.onto, theirs: step.head }).pipe(
            Effect.catchTags({
              ObjectNotFound: () => Effect.succeed(null),
              StorageFailure: () => Effect.succeed(null),
              Invalid: () => Effect.succeed(null),
            }),
          ));
    if (merged === null) {
      return {
        kind: "refused",
        reason: `${step.commit} could not be re-merged from ${step.onto} and ${step.head}`,
      } satisfies Chain;
    }
    if (merged.conflicts.length > 0) {
      return {
        kind: "refused",
        reason: `${step.commit} merges ${step.head} into ${step.onto}, which conflicts in ${merged.conflicts
          .map((conflict) => conflict.path)
          .join(", ")}`,
      } satisfies Chain;
    }
    input.merges.set(key, { tree: merged.tree, conflicts: merged.conflicts });

    // The one rule everything else here exists to make safe.
    if (merged.tree !== step.tree) {
      return {
        kind: "refused",
        reason: `${step.commit} does not hold the merge of ${step.head} into ${step.onto}: its tree is ${step.tree}, and merging them gives ${merged.tree}`,
      } satisfies Chain;
    }

    contained.add(step.head);
    contained.add(step.commit);
    ontoTree = step.tree;
  }

  return { kind: "verified" } satisfies Chain;
});

/**
 * What a protected branch demands of the revision arriving on it.
 *
 * `null` means nothing was refused. Two ways to satisfy it, asked in that
 * order: the revision is itself an approved pull request's head, or it is a
 * chain of merges this boundary re-derives. The direct question is asked first
 * and costs nothing extra when it fails — a candidate's tip is a merge commit
 * no pull request ever proposed, so the pre-filter skips every fold.
 */
const protectedBranch = Effect.fn("Policy.protectedBranch")(function* (input: {
  readonly ref: string;
  readonly from: Oid | null;
  readonly to: Oid;
  readonly genesis: Genesis;
  readonly trust: TrustProjection;
  readonly rules: Rules;
  readonly folds: FoldCache;
  readonly mentions: MentionCache;
  readonly merges: MergeCache;
}) {
  if (!needsReview(input.rules)) return null;

  const direct = yield* authorizes({ ...input, revision: input.to, checkAt: input.to });
  if (direct.satisfied) return null;

  const chain = yield* candidateChain(input);
  if (chain.kind === "verified") return null;

  // The direct path's own shortfall wins where it has one. A pull request whose
  // head *is* a merge commit is a candidate chain by shape, so a push of it one
  // approval short was refused for the second parent naming no pull request —
  // true of the chain reading, and useless to somebody who has an approved pull
  // request for exactly this revision and needs to know it is short a review.
  // A chain's reason is the better answer only when nothing proposed the
  // revision at all, which is what an actual candidate looks like.
  if (direct.shortfall !== null) return direct.shortfall;
  if (chain.kind === "refused") return refused(input.ref, chain.reason);

  return refused(input.ref, `${input.ref} may only be moved by an approved pull request`);
});

/**
 * May this requester write this ref at all?
 *
 * For the JSON verbs that produce a ref's new value as part of doing the work
 * — commit, branch, tag, merge, cherry-pick, rebase — the resulting oid is not
 * known until afterwards, so the full `gate` cannot be asked first. What can be
 * asked, and is enough, is whether the ref is writable by this requester at
 * all: a protected branch is not, because the only thing that may move one is
 * an approved pull request, and none of these verbs is that.
 */
/**
 * Whether the requester may write at all, without naming a ref.
 *
 * The ref rules need the ref, and sometimes the objects behind it — a
 * fast-forward cannot be told from a force push until the pack is unpacked. So
 * receive-pack judges *after* the object phase, which means a caller the
 * boundary was always going to refuse had their pack persisted first. This is
 * the half of the answer available before a byte of the body is read: a
 * credential scoped to delete a branch is not one that may create or move one,
 * and that is knowable from the commands alone.
 *
 * `null` means nothing was refused; anything else is the reason.
 */
/**
 * Which commands the requester's signed envelope does not cover.
 *
 * A native client signs the refs it is moving and where to, and that promise
 * needs nothing from the pack to check — unlike the force-push rule, which
 * cannot tell a fast-forward from a rewrite until the objects are present. So
 * it is asked before the body is read: left to `gate`, a push naming refs the
 * envelope never covered had its whole pack unpacked and persisted first.
 */
export const uncovered = Effect.fn("Policy.uncovered")(function* (
  updates: ReadonlyArray<RefUpdate>,
) {
  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);
  if (who.envelope === null) return [];

  const refused: Array<{ readonly ref: string; readonly reason: string }> = [];
  for (const update of updates) {
    const covered = coveredByEnvelope(who.envelope, update);
    if (!covered.ok) refused.push({ ref: covered.ref, reason: covered.reason });
  }
  return refused;
});

export const mayWrite = Effect.fn("Policy.mayWrite")(function* (
  capability: string,
  writes: ReadonlyArray<RefUpdate> = [],
) {
  const stored = yield* readGenesis();
  if (stored === null) {
    const open = yield* Effect.serviceOption(Auth.AnonymousWrites);
    if (!Option.getOrElse(open, () => false)) {
      return "this repository has no membership to authorize a write; run `hub init` to give it one";
    }

    return null;
  }

  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);
  // Only for the refs the inbox is *for*. `quarantineRules` refuses everything
  // else, but it runs in the object phase — so waving the whole push through
  // here let an unauthenticated submitter have their entire pack unpacked and
  // persisted before a single ref was judged, which is the object half of the
  // write this pre-body check exists to refuse. A push naming any other ref is
  // one this door can already answer, and it answers it before the body.
  if (Auth.inboxOnly(who)) {
    return writes.every((update) => update.name.startsWith(Inbox.PENDING_PREFIX))
      ? null
      : "an inbox submission may only create an inbox proposal ref";
  }
  const principal = { member: who.principal, capabilities: who.capabilities };
  if (principal.member === null) return "authentication required to write refs";
  return may(principal, capability) ? null : `this needs ${capability}`;
});

/**
 * Whether a staleness bound has anything to say about this ref.
 *
 * It has nothing to say about the two refs that *lift* it. A checkpoint is how
 * a membership view stops being stale, and it lands on the trust log; the
 * bound itself lives in the rules file. Refusing those alongside everything
 * else made the flag a one-way door: the repository became unwritable over the
 * network and neither push that would recover it could be made, which is not
 * the bound `Verify.fresh` describes but a repository lost to it. Both are
 * charged their own capability elsewhere — a trust record needs a trust
 * capability, and `refs/meta/policy` needs `policy.write` — so exempting them
 * from *this* check opens nothing.
 */
/**
 * Whether this write is the one that can fix an unreadable rules file.
 *
 * Failing closed on a policy nobody can parse is right for every ref the
 * policy governs, and wrong for the policy itself: the rules are read before
 * any per-ref decision, so a single `policy.write` holder pushing a truncated
 * file made every receive-pack fail and every JSON verb answer "the
 * repository's policy could not be evaluated" — the corrective push included.
 * The door locked from the inside, and the key was filesystem access to the
 * host. The rules have nothing to say about their own file in any case: the
 * staleness bound already exempts it for the same reason.
 *
 * `open` is the repository that has no identity on a host that allows
 * anonymous writes — `serve --open`. It has no members, so there is nobody to
 * hold `policy.write`, and requiring the capability left exactly that
 * repository with no way back: the same anonymous client that may write the
 * rules could write an unparseable one and lock every write on the repository,
 * its own next attempt included. Whoever may publish the rules may repair
 * them, which is the same door either way.
 */
/**
 * The membership graph as it stands *now*, reusing the guard's fold when it can.
 *
 * The guard folded the log a moment ago to decide who this is, and the fold is
 * an Ed25519 verification per signature per record — so reusing it halves the
 * cost of every write. What makes reuse safe is the head, not the repository:
 * between the guard and this boundary sits the whole pack upload, and on a
 * host that queues per repository, the queue as well. A revocation that lands
 * in that window was invisible, so a member revoked while their push was in
 * flight was authorized by it — against `signedByRevoked`'s own premise, that
 * the boundary knows whose keys are revoked *now*.
 *
 * Re-folding when the head has moved costs one ref read to find out, and
 * `Auth.folded` is memoised by that same head, so the fold itself is usually
 * already done.
 */
const membership = Effect.fn("Policy.membership")(function* (
  genesis: Genesis,
  reached: TrustProjection,
) {
  const repository = yield* Repository;
  if (reached.repoId !== genesis.repoId) return yield* project(genesis);
  const head = yield* repository.resolve(Log.LOG_REF);
  return reached.head === head ? reached : yield* project(genesis);
});

/**
 * The requester as the membership standing *now* sees them.
 *
 * The guard decided who this is against the log as it stood when the request
 * arrived, and `membership` above re-folds when the log has moved since — but
 * the capabilities came from the guard either way. So a revocation that landed
 * between the two was applied to every namespace rule and to none of the
 * capability checks: the revoked member's own push, already in flight, was
 * judged by the grant they no longer had.
 *
 * Only ever narrows. Each capability the guard granted is re-asked of the
 * current projection — which is where revocation, expiry and a narrowed grant
 * all live — and a credential's own scoping survives because what is re-asked
 * is the scoped list rather than the member's full one.
 */
const standing = Effect.fn("Policy.standing")(function* (
  who: Auth.Authenticated,
  trust: TrustProjection | null,
) {
  const held = { member: who.principal, capabilities: who.capabilities };
  // Nothing to re-ask: an anonymous requester holds nothing that a membership
  // could take away, and an unchanged projection is the one the guard used.
  if (trust === null || who.signer === null || trust === who.projection) return held;

  const now = new Date();
  const capabilities: Array<string> = [];
  for (const capability of who.capabilities) {
    const authorized = yield* Verify.authorizeKey({
      projection: trust,
      signer: who.signer,
      capability,
      at: now,
    });
    if (authorized.ok) capabilities.push(capability);
  }
  const member = trust.members.get(who.signer) ?? null;
  return { member: capabilities.length === 0 ? null : member, capabilities };
});

const repairable = (ref: string, principal: Principal, open: boolean): boolean =>
  ref === RULES_REF && (open || may(principal, "policy.write"));

const boundApplies = (ref: string): boolean => ref !== RULES_REF && ref !== Refspec.TRUST_LOG;

/**
 * Whether the requester's *foreign* identity view is too old to judge by.
 *
 * Both doors ask it — the API's `gateWrite` and receive-pack's `gate` — and
 * both had their own copy, down to the message. Two copies of one bound is a
 * member refused over a push and admitted over the JSON API the moment either
 * is edited alone, which is the boundary consistency this module exists for.
 *
 * Fresh by definition when the bound is off, or when membership here came by a
 * direct key: there is no foreign view behind it whose age could be in
 * question. The repository's own projection is judged separately by each
 * caller, because they refuse on it differently.
 */
const identityFreshness = (
  who: Auth.Authenticated,
  rules: Rules,
): { readonly ok: true } | { readonly ok: false; readonly reason: string } => {
  if (rules.maxTrustAgeSeconds <= 0 || who.identity === undefined) return { ok: true };
  const fresh = Verify.fresh(who.identity.projection, rules.maxTrustAgeSeconds * 1000);
  return fresh.ok
    ? { ok: true }
    : { ok: false, reason: `the member's identity view is stale: ${fresh.reason}` };
};

export const gateWrite = Effect.fn("Policy.gateWrite")(function* (
  ref: string,
  /**
   * Whether the verb may rewrite history.
   *
   * `rebase` and `cherry-pick` with `into` move a branch somewhere that need
   * not contain what it held, which is a force push wearing another name — and
   * the smart-HTTP door charges `source.force-push` for exactly that.
   */
  rewrites = false,
) {
  // These hold whether or not the repository has an identity, because they are
  // what the namespaces mean rather than what a member may do. In particular a
  // repository with no genesis must not acquire one through an API call, or
  // whoever asked first would own somebody else's repository.
  if (ref.startsWith("refs/meta/trust/")) {
    return "a repository's identity is established by `hub init`, not over the API";
  }
  if (ref.startsWith("refs/social/")) {
    return `${ref} may only be appended to by a social operation`;
  }
  if (ref.startsWith("refs/quarantine/")) {
    return `${ref} may only be written by a quarantine operation`;
  }
  if (Refspec.isAppendOnly(ref)) return `${ref} may only be appended to by the hub`;

  const stored = yield* readGenesis();
  if (stored === null) {
    // The same answer `gate` gives, for the same reason. Allowing here while
    // receive-pack refused meant `git push` was blocked and unauthenticated
    // `POST /commit`, `/branch`, `/tags`, a merge or rebase with `into`, a
    // pull and commit-pack all still wrote refs — every door but the one that
    // was locked.
    const open = yield* Effect.serviceOption(Auth.AnonymousWrites);
    if (!Option.getOrElse(open, () => false)) {
      return "this repository has no membership to authorize a write; run `hub init` to give it one";
    }

    // And the branch rules the repository publishes, which `evaluate` already
    // applies on this path. Left out, the two doors disagreed about the same
    // file on the same repository: receive-pack, `reset` and a branch or tag
    // delete honoured `refs/meta/policy` while `commit`, `branch`,
    // `tagCreate`, a merge, rebase or cherry-pick with `into`, `fetch`,
    // `pull` and commit-pack ignored it. These verbs name a branch rather
    // than a revision, so what a protected branch has to say to them is that
    // it does not move this way at all.
    // And unreadable rules must not lock this repository either; see
    // `repairable`. It has no members, so there is nobody to hold
    // `policy.write` — whoever may publish the rules here may repair them.
    const published = yield* rulesOf().pipe(
      Effect.orElseSucceed(() =>
        repairable(ref, { member: null, capabilities: [] }, true) ? OPEN : null,
      ),
    );
    if (published === null) return "the repository's policy could not be evaluated";
    return isProtected(published, ref)
      ? `${ref} is protected and does not move by this route`
      : null;
  }

  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);
  const principal = { member: who.principal, capabilities: who.capabilities };

  if (principal.member === null) return "authentication required to write refs";
  if (!may(principal, "source.push")) return "pushing needs source.push";
  if (rewrites && !may(principal, "source.force-push")) {
    return "rewriting a branch needs source.force-push";
  }
  if (ref === RULES_REF && !may(principal, "policy.write")) {
    return `writing ${RULES_REF} needs policy.write`;
  }

  // Read after the capability checks and *before* it can lock the repair out;
  // see `repairable`. Everything below this point is a question the rules
  // answer, and the rules answer nothing about their own file.
  const rules = yield* rulesOf().pipe(
    Effect.orElseSucceed(() => (repairable(ref, principal, false) ? OPEN : null)),
  );
  if (rules === null) return "the repository's policy could not be evaluated";

  // A verb that makes the commit cannot also carry the record that names it:
  // the session has to say it produced a commit that does not exist until
  // this call creates it. So where a repository requires provenance, these
  // verbs do not move a branch at all — refused rather than exempt, because
  // an exemption is the way around the rule. Left only on receive-pack, the
  // two doors disagreed: `git push` was refused for a missing trailer while
  // `POST /commit` wrote the same change to the same branch.
  if (rules.requireProvenance && ref.startsWith("refs/heads/")) {
    return `${ref} requires provenance, which this route cannot supply; push the commit with the session record that names it`;
  }

  // The same staleness bound `gate` applies. Left only on receive-pack, a
  // repository that had asked for one still accepted `commit`, `branch`,
  // `tagCreate`, `merge`, `rebase`, `cherry-pick`, `pull` and commit-pack
  // against a membership view of any age — which is most of the ways a ref
  // moves.
  if (rules.maxTrustAgeSeconds > 0 && boundApplies(ref)) {
    const trust = yield* membership(stored.genesis, who.projection);
    const stale = Verify.fresh(trust, rules.maxTrustAgeSeconds * 1000);
    if (!stale.ok) return stale.reason;
    const foreign = identityFreshness(who, rules);
    if (!foreign.ok) return foreign.reason;
  }

  // Refused outright, not evaluated. Every caller of this gates by ref *name*
  // and before the write, so the revision the protected-branch rules are about
  // does not exist yet — and a rule that cannot be evaluated is one this must
  // not pretend to have satisfied. A protected branch moves through
  // receive-pack, where the revision arrives with the request.
  return isProtected(rules, ref)
    ? `${ref} is protected and may only be moved by pushing an approved revision`
    : null;
});

/**
 * Whether a signed envelope authorized this exact ref command.
 *
 * A request with no envelope is not refused here — a delegated credential
 * makes no claim about particular refs, and its containment is its scope and
 * its lifetime. What is refused is an envelope that named *other* refs: a
 * signature over "move topic" must not move main.
 */
const coveredByEnvelope = (
  envelope: Auth.Envelope | null,
  update: RefUpdate,
):
  | { readonly ok: true }
  | { readonly ok: false; readonly ref: string; readonly reason: string } => {
  if (envelope === null) return { ok: true };

  const signed = envelope.commands.find((entry) => entry.ref === update.name);
  if (signed === undefined) {
    return refusedCommand(update.name, "the signed request did not name this ref");
  }
  if ((signed.to ?? null) !== (update.value ?? null)) {
    return refusedCommand(update.name, "the signed request named a different revision");
  }
  // The old oid too, and for the reason the envelope exists. `from` is the
  // compare-and-swap the client signed for; `expected` is what the receive-pack
  // command line — which nothing signs — asked for. Comparing only `to` left
  // the two free to disagree: a signed "move `main` from A to B" was replayable
  // as an unconditional "set `main` to B", landing the push on a branch that
  // had moved on since the client looked, which is the exact race the
  // compare-and-swap was promised against.
  if ((signed.from ?? null) !== (update.expected ?? null)) {
    return refusedCommand(update.name, "the signed request named a different current revision");
  }
  return { ok: true };
};

const refusedCommand = (ref: string, reason: string) => ({ ok: false, ref, reason }) as const;

/**
 * The receive-pack entry point: judge a batch against everything the
 * repository currently knows.
 *
 * Gathers the three inputs itself — who the requester is (from the guard that
 * already ran), the trust state, and the branch rules — so that the protocol
 * handler has one call rather than a checklist it could get half right.
 *
 * Returns the updates to apply, each carrying the compare-and-swap it was
 * judged under, and the refusals to report back as `ng` lines.
 */
export const gate = Effect.fn("Policy.gate")(function* (
  updates: ReadonlyArray<RefUpdate>,
  atomic: boolean,
  /**
   * Whether to hold the updates to a signed envelope's ref commands.
   *
   * True for receive-pack, which is the conversation an envelope describes.
   * False for the JSON verbs: a client authenticated with an envelope for a
   * push has not made a claim about `reset`, and refusing it for saying
   * nothing would be reading silence as a denial.
   */
  bindEnvelope = true,
) {
  // A failure to read identity is not "this repository has none": treating it
  // that way would drop every rule below at the moment storage was least
  // trustworthy. `null` means the ref is genuinely absent.
  const stored = yield* readGenesis();
  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);

  // The guard folded the log a moment ago to decide who this is, and the fold
  // is an Ed25519 verification per signature per record. Reusing what it
  // reached — when it is the same repository's — halves the cost of every
  // write instead of paying it twice on the hot path.
  const trust = stored === null ? null : yield* membership(stored.genesis, who.projection);

  // Unreadable is refused per ref, not for the batch, so the one push that can
  // fix it still lands; see `repairable`. A rules file that will not parse
  // otherwise refused every write on the repository including its own repair,
  // and the only way back was filesystem access to the host.
  const principal = yield* standing(who, trust);
  const published = yield* rulesOf().pipe(Effect.orElseSucceed(() => null));
  if (published === null) {
    const anonymous = yield* Effect.serviceOption(Auth.AnonymousWrites);
    const open = stored === null && Option.getOrElse(anonymous, () => false);
    const repository = yield* Repository;
    const decisions: Decision[] = [];
    for (const update of updates) {
      if (!repairable(update.name, principal, open)) {
        decisions.push({
          ok: false,
          ref: update.name,
          reason: "the repository's policy could not be evaluated",
        });
        continue;
      }
      // The value the ref actually holds, exactly as `evaluate` computes it.
      // `null` here is not "no opinion" — the store reads it as *must not
      // exist*, and the ref being repaired exists by definition, so the swap
      // could never match: the push was allowed, reported as allowed, and
      // never applied. The door stayed shut while the boundary said it was
      // open. The client's own old-oid still wins where it declared one.
      const stale = yield* repository.readRef(update.name);
      decisions.push({
        ok: true,
        allowed: { update, expected: update.expected === undefined ? stale : update.expected },
      });
    }
    const refused = decisions.flatMap((decision) => (decision.ok ? [] : [decision]));
    return {
      updates:
        atomic && refused.length > 0
          ? []
          : decisions.flatMap((decision) =>
              decision.ok
                ? [{ ...decision.allowed.update, expected: decision.allowed.expected }]
                : [],
            ),
      refused,
    };
  }
  const rules = published;

  // How stale a view may be, checked once for the batch rather than per ref.
  // This is the bound on the one failure a hash-linked log cannot rule out by
  // itself: a replica serving a consistent history that stops short of a
  // revocation. Off unless a repository asks for it, because a bound nobody
  // configured would refuse every push on a repository that never checkpoints.
  const ownStale =
    trust === null || rules.maxTrustAgeSeconds <= 0
      ? null
      : Verify.fresh(trust, rules.maxTrustAgeSeconds * 1000);
  const foreign = identityFreshness(who, rules);
  const stale = ownStale !== null && !ownStale.ok ? ownStale : !foreign.ok ? foreign : ownStale;
  // Refused per ref rather than for the batch, so the two refs that lift the
  // bound are still reachable while it holds; see `boundApplies`.
  const withheld = new Set(
    stale === null || stale.ok
      ? []
      : updates.filter((update) => boundApplies(update.name)).map((update) => update.name),
  );
  if (stale !== null && !stale.ok && withheld.size === updates.length) {
    return {
      updates: [],
      refused: updates.map(
        (update) => ({ ok: false, ref: update.name, reason: stale.reason }) as const,
      ),
    };
  }

  // Built once for the batch, and only where the rule is on: it is a read of
  // every session ref, which a repository that does not require provenance
  // should not pay for on every push.
  const sessions = new Map<string, Oid>();
  if (rules.requireProvenance) {
    const repository = yield* Repository;
    for (const [name, value] of yield* repository.refs) {
      const id = Session.sessionOf(name);
      if (id !== null) sessions.set(id, value);
    }
  }

  // Session commands are judged first, and what they vouch for is added only
  // once they have passed. Seeded from the batch's *unjudged* commands, a
  // member holding `source.push` and no hub capability could send a session
  // ref this boundary refuses beside a branch whose commits name it: the
  // session was thrown away and the branch landed with a trailer pointing at
  // a record the repository does not hold.
  const order = [...updates.keys()].sort((left, right) => {
    const leftUpdate = updates[left];
    const rightUpdate = updates[right];
    if (leftUpdate === undefined || rightUpdate === undefined) return left - right;
    const first = Session.sessionOf(leftUpdate.name) === null ? 1 : 0;
    const second = Session.sessionOf(rightUpdate.name) === null ? 1 : 0;
    return first - second || left - right;
  });

  const decisions: Decision[] = [];
  const folds: FoldCache = new Map();
  const mentions: MentionCache = new Map();
  // The expensive one, and the one this door most needs: a push moving several
  // protected branches re-derives a candidate chain per ref, and every step of
  // it is a merge-base walk and three whole-tree flattens — on the synchronous
  // receive-pack path. The runner shares one of these across its own asks;
  // leaving it out here made the untrusted door the costly one.
  const merges: MergeCache = new Map();
  const opening: Openings = new Set();
  for (const at of order) {
    const update = updates[at];
    if (update === undefined) continue;
    // A native client signed an envelope naming the refs it was moving and
    // where to. Checking it here rather than in the guard is not a weakening:
    // the guard runs before the push body exists, so this is the first moment
    // the commands are knowable at all — and the last before they are applied.
    if (withheld.has(update.name) && stale !== null && !stale.ok) {
      decisions[at] = { ok: false, ref: update.name, reason: stale.reason };
      continue;
    }

    const covered = bindEnvelope
      ? coveredByEnvelope(who.envelope, update)
      : ({ ok: true } as const);
    if (!covered.ok) {
      decisions[at] = covered;
      continue;
    }
    const decision = yield* evaluate({
      update,
      principal,
      genesis: stored?.genesis ?? null,
      trust,
      rules,
      sessions,
      folds,
      mentions,
      merges,
      opening,
    });
    // The namespace rules record a create as soon as they have passed it, and
    // they are not the last word: the rules after them refuse too. A create
    // this batch does not actually get would otherwise spend a population slot
    // the create beside it was entitled to.
    if (!decision.ok) opening.delete(update.name);

    // Only now, and only if it passed: a session ref this batch is refusing
    // vouches for nothing.
    const id = rules.requireProvenance ? Session.sessionOf(update.name) : null;
    if (id !== null && decision.ok) {
      if (update.value === null) sessions.delete(id);
      else sessions.set(id, update.value);
    }
    decisions[at] = decision;
  }

  const refusals = decisions.flatMap((decision) => (decision.ok ? [] : [decision]));
  // All or nothing: one refusal in an atomic batch refuses the batch, which is
  // what receive-pack's own `atomic` capability promises a client.
  const allowed =
    atomic && refusals.length > 0
      ? []
      : decisions.flatMap((decision) =>
          decision.ok ? [{ ...decision.allowed.update, expected: decision.allowed.expected }] : [],
        );

  return { updates: allowed, refused: refusals };
});
