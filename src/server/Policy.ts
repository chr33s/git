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
import { Invalid, type StorageFailure } from "../git/Error.ts";
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
import {
  approvals,
  checksPassed,
  project as projectPr,
  type PullRequest as PullRequestState,
} from "../hub/Projection.ts";
import { permits } from "../trust/Certificate.ts";
import * as Log from "../trust/Log.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";
import * as Auth from "./Auth.ts";

/**
 * What a branch requires before it will move.
 *
 * Empty by default in every field: a repository that has said nothing about a
 * branch has not protected it, and inventing protection nobody asked for would
 * break every existing push.
 */
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
}

/** Re-exported where the boundary reads it; defined beside the guard. */
export const anonymousWrites = Auth.anonymousWrites;
export type AnonymousWrites = Auth.AnonymousWrites;

export const OPEN: Rules = {
  protected: [],
  requiredApprovals: 0,
  requiredChecks: [],
  requireResolvedThreads: false,
  requirePullRequest: false,
  maxTrustAgeSeconds: 0,
};

/** Where a repository keeps its branch rules, if it has any. */
export const RULES_REF = "refs/meta/policy";
const RULES_PATH = "policy.json";

const RulesDocument = Schema.Struct({
  version: Schema.Literal(1),
  protected: Schema.Array(Schema.String),
  requiredApprovals: Schema.Int,
  requiredChecks: Schema.Array(Schema.String),
  requireResolvedThreads: Schema.Boolean,
  requirePullRequest: Schema.Boolean,
  /** Optional, so a rules file written before this existed still decodes. */
  maxTrustAgeSeconds: Schema.optional(Schema.Int),
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

  return {
    protected: loaded.protected,
    requiredApprovals: loaded.requiredApprovals,
    requiredChecks: loaded.requiredChecks,
    requireResolvedThreads: loaded.requireResolvedThreads,
    requirePullRequest: loaded.requirePullRequest,
    maxTrustAgeSeconds: loaded.maxTrustAgeSeconds ?? 0,
  };
});

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

const refused = (ref: string, reason: string): Decision => ({ ok: false, ref, reason });

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
const isProtected = (rules: Rules, ref: string): boolean => {
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
  readonly folds?: FoldCache;
  /** As `folds`, for the walk that decides which pull requests to fold. */
  readonly mentions?: MentionCache;
  /** As `folds`, for the bounds a batch could otherwise outrun; see `Openings`. */
  readonly opening?: Openings;
}) {
  const repository = yield* Repository;
  const { update } = input;
  // A batch of one when the caller did not bring a map, which is what every
  // direct caller outside `gate` is.
  const folds: FoldCache = input.folds ?? new Map();
  const mentions: MentionCache = input.mentions ?? new Map();
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
    if (update.name.startsWith("refs/meta/trust/")) {
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
    const kind = update.name.startsWith("refs/hub/") ? "hub." : "member.";
    if (
      !input.principal.capabilities.some((held) => held.startsWith(kind) || held === "repo.admin")
    ) {
      return refused(update.name, `appending to ${update.name} needs a ${kind}* capability`);
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
  if (update.name.startsWith("refs/hub/") && update.value !== null) {
    const refusal = yield* signedByRevoked(update.value, current, input.trust, held);
    if (refusal !== null) return refused(update.name, refusal);
  }

  if (!isProtected(input.rules, update.name)) return namespace;

  if (deleting) return refused(update.name, `${update.name} is protected and may not be deleted`);
  if (forced)
    return refused(update.name, `${update.name} is protected and may not be force-pushed`);

  const gate = yield* protectedBranch({
    ref: update.name,
    to: update.value!,
    genesis: input.genesis,
    trust: input.trust,
    rules: input.rules,
    folds,
    mentions,
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

  // A ref in this namespace is a pull request, and nothing else. `refs/hub/`
  // as a whole is undeletable, so a name outside that shape is a permanent
  // entry nothing counts, nothing folds and nothing can ever remove — one
  // more object graph pinned into every ref listing, every advertisement,
  // every collection root and every memo key for the life of the repository.

  if (update.name.startsWith("refs/hub/") && Event.prOf(update.name) === null) {
    return refused(update.name, `${update.name} does not name a pull request`);
  }

  // And its value is a commit of this namespace's own kind. Nothing else here
  // asks: the ceiling walk steps over a foreign head and reports an empty
  // history, and the graft check steps over it too — so a `source.push`
  // holder could point a hub ref at any source commit at all, on a name that
  // can never be deleted, pinning whatever it reaches out of reach of `gc`
  // for good.
  const belongs = update.name.startsWith("refs/hub/") ? Event.isHubCommit : Log.isTrustCommit;
  if (!(yield* belongs(update.value))) {
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
    const count = (yield* Event.pullRequests()).length + opening.size;
    if (count >= (yield* Event.populationOf())) {
      return refused(update.name, `this repository already holds ${count} pull requests`);
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
  if (name.startsWith("refs/hub/")) {
    return (yield* Event.withinCeiling(to))
      ? null
      : `${name} would hold more events than a fold will walk`;
  }
  if (name === Refspec.TRUST_LOG || name.startsWith("refs/meta/trust/log/")) {
    return (yield* Log.withinCeiling(to))
      ? null
      : `${name} would hold more records than a fold will walk`;
  }

  return null;
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
  const hub = name.startsWith("refs/hub/");
  const anchor = hub ? null : yield* repository.resolve(Refspec.TRUST_GENESIS);
  // Bounded by the *ceiling*, not by the namespace. Bounded by the namespace,
  // the walk stopped at a commit whose tree object never arrived — a state
  // every other walk here deliberately steps over, because refs are applied
  // without a connectivity check — and everything behind it dropped out of
  // the set, so the next ordinary reconciling push met an unaccounted parent
  // and was refused for good on a ref that cannot be rewound.
  const ceiling = hub ? yield* Event.ceilingOf() : yield* Log.ceilingOf();

  // Walked here rather than through `Dag.reachable` for the same tolerance in
  // the other direction: a *commit* object that never arrived is still a
  // commit this ref reaches, so it is recorded and simply not descended
  // through. Failing on it — which is what an answer of "this ref holds
  // nothing I can name" amounted to — left the two rules that read this set
  // disagreeing about what to do with it, one waving a graft through and the
  // other refusing an ordinary join for good.
  const pending: Oid[] = [current];
  while (pending.length > 0 && held.size < ceiling) {
    const oid = pending.pop()!;
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
  const reached = yield* held;
  const parents = yield* Dag.reachable(to, current, (commit) => added(commit, reached)).pipe(
    // An unreadable history is not a signature claim. It is refused, or passed
    // over, by the walks that own that question.
    Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
  );
  if (parents === null) return null;

  for (const oid of parents.keys()) {
    if (!(yield* Record.carries(oid, Event.RECORD))) continue;
    const record = yield* Record.read(oid, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) continue;
    const signed = yield* Verify.signers(record.payload, record.signatures);
    const payload = yield* Event.decode(record.payload).pipe(Effect.orElseSucceed(() => null));

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
        return `${signer} has been revoked and may not add a ${payload?.type ?? "event"}`;
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
    if (payload?.type !== "event.redacted") continue;
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
  const hub = name.startsWith("refs/hub/");
  const belongs = hub ? Event.isHubCommit : Log.isTrustCommit;
  // The commit a trust log hangs off is not a record and never will be: it is
  // the genesis, the one legitimate edge out of that namespace. On every push
  // but the log's first it is already reachable from `current`; on the first
  // there is nothing else for the first record to name.
  const repository = yield* Repository;
  const anchor = hub ? null : yield* repository.resolve(Refspec.TRUST_GENESIS);
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
 * What a protected branch demands of the revision arriving on it.
 *
 * `null` means nothing was refused. The pull request is found by what it
 * proposes rather than by anything the push carries: a client that could name
 * its own pull request could name one that was approved for something else.
 */
const protectedBranch = Effect.fn("Policy.protectedBranch")(function* (input: {
  readonly ref: string;
  readonly to: Oid;
  readonly genesis: Genesis;
  readonly trust: TrustProjection;
  readonly rules: Rules;
  /** Shared across one batch; see `FoldCache`. */
  readonly folds: FoldCache;
  /** Shared across one batch; see `MentionCache`. */
  readonly mentions: MentionCache;
}) {
  const { rules } = input;
  const needsReview =
    rules.requirePullRequest ||
    rules.requiredApprovals > 0 ||
    rules.requiredChecks.length > 0 ||
    rules.requireResolvedThreads;
  if (!needsReview) return null;

  /** The nearest miss, reported only if nothing else satisfies the rules. */
  let shortfall: Decision | null = null;

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
    if (cached === undefined && !(yield* proposes(id, input.to, input.mentions))) continue;

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

    // The tip has to *be* the reviewed revision. A merge commit that merely
    // names it as a parent proves nothing about content — a merge's tree is
    // unconstrained, so anybody who may push could wrap whatever they liked
    // around an approved head.
    //
    // So a protected branch is advanced by *pushing the approved revision onto
    // it*, and by nothing else: a merge commit cannot land there, and neither
    // can the API's ref-moving verbs, which are refused on a protected ref by
    // `gateWrite` because they name a branch and not the revision these rules
    // are about. Whoever wants a merge commit makes one on their own branch,
    // opens a pull request for it, and has *that* reviewed.
    if (input.to !== pullRequest.head) continue;

    // Why this one did not satisfy the rules, kept rather than returned. Two
    // pull requests can propose the same revision, and refusing on the first
    // that falls short would let an unapproved duplicate block the approved
    // one purely by sorting first.
    if (approvals(pullRequest).length < rules.requiredApprovals) {
      shortfall ??= refused(
        input.ref,
        `${id} has ${approvals(pullRequest).length} approvals of its current revision, ` +
          `and ${rules.requiredApprovals} are required`,
      );
      continue;
    }
    if (!checksPassed(pullRequest, rules.requiredChecks)) {
      shortfall ??= refused(input.ref, `${id} has not passed ${rules.requiredChecks.join(", ")}`);
      continue;
    }
    if (rules.requireResolvedThreads && pullRequest.threads.some((thread) => !thread.resolved)) {
      shortfall ??= refused(input.ref, `${id} has unresolved review threads`);
      continue;
    }
    return null;
  }

  return (
    shortfall ?? refused(input.ref, `${input.ref} may only be moved by an approved pull request`)
  );
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

export const mayWrite = Effect.fn("Policy.mayWrite")(function* (capability: string) {
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
const repairable = (ref: string, principal: Principal, open: boolean): boolean =>
  ref === RULES_REF && (open || may(principal, "policy.write"));

const boundApplies = (ref: string): boolean =>
  ref !== RULES_REF && ref !== Refspec.TRUST_LOG && !ref.startsWith("refs/meta/trust/log/");

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

  // The same staleness bound `gate` applies. Left only on receive-pack, a
  // repository that had asked for one still accepted `commit`, `branch`,
  // `tagCreate`, `merge`, `rebase`, `cherry-pick`, `pull` and commit-pack
  // against a membership view of any age — which is most of the ways a ref
  // moves.
  if (rules.maxTrustAgeSeconds > 0 && boundApplies(ref)) {
    const trust =
      who.projection.repoId === stored.genesis.repoId
        ? who.projection
        : yield* project(stored.genesis);
    const stale = Verify.fresh(trust, rules.maxTrustAgeSeconds * 1000);
    if (!stale.ok) return stale.reason;
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
  const trust =
    stored === null
      ? null
      : who.projection.repoId === stored.genesis.repoId
        ? who.projection
        : yield* project(stored.genesis);

  // Unreadable is refused per ref, not for the batch, so the one push that can
  // fix it still lands; see `repairable`. A rules file that will not parse
  // otherwise refused every write on the repository including its own repair,
  // and the only way back was filesystem access to the host.
  const principal = { member: who.principal, capabilities: who.capabilities };
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
  const stale =
    trust === null || rules.maxTrustAgeSeconds <= 0
      ? null
      : Verify.fresh(trust, rules.maxTrustAgeSeconds * 1000);
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

  const decisions: Decision[] = [];
  const folds: FoldCache = new Map();
  const mentions: MentionCache = new Map();
  const opening: Openings = new Set();
  for (const update of updates) {
    // A native client signed an envelope naming the refs it was moving and
    // where to. Checking it here rather than in the guard is not a weakening:
    // the guard runs before the push body exists, so this is the first moment
    // the commands are knowable at all — and the last before they are applied.
    if (withheld.has(update.name) && stale !== null && !stale.ok) {
      decisions.push({ ok: false, ref: update.name, reason: stale.reason });
      continue;
    }

    const covered = bindEnvelope
      ? coveredByEnvelope(who.envelope, update)
      : ({ ok: true } as const);
    if (!covered.ok) {
      decisions.push(covered);
      continue;
    }
    const decision = yield* evaluate({
      update,
      principal,
      genesis: stored?.genesis ?? null,
      trust,
      rules,
      folds,
      mentions,
      opening,
    });
    // The namespace rules record a create as soon as they have passed it, and
    // they are not the last word: the rules after them refuse too. A create
    // this batch does not actually get would otherwise spend a population slot
    // the create beside it was entitled to.
    if (!decision.ok) opening.delete(update.name);
    decisions.push(decision);
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
