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
import { Effect, Option, Schema } from "effect";

import * as Dag from "../git/Dag.ts";
import { Invalid } from "../git/Error.ts";
import * as Refspec from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid, RefUpdate } from "../git/Store.ts";
import { type Genesis, readGenesis } from "../trust/Genesis.ts";
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
 * is for. Callers fail closed on that failure.
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

/** Whether a pull request's events ever named this revision as a head. */
const proposes = Effect.fn("Policy.proposes")(function* (pr: string, to: Oid, cache: MentionCache) {
  const known = cache.get(pr);
  if (known !== undefined) return known.has(to);

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
}) {
  const repository = yield* Repository;
  const { update } = input;
  // A batch of one when the caller did not bring a map, which is what every
  // direct caller outside `gate` is.
  const folds: FoldCache = input.folds ?? new Map();
  const mentions: MentionCache = input.mentions ?? new Map();
  const current = yield* repository.resolve(update.name);
  // Two readings of "what the ref is now", and they differ for a symbolic ref.
  // Reachability wants the commit it resolves to; the compare-and-swap wants
  // exactly what the store will compare against, which is the ref's own value
  // — `null` for a symref. Using the resolved oid as `expected` made every
  // gated write to a symbolic ref fail as a conflict against a value that was
  // never written there.
  const stored = yield* repository.readRef(update.name);

  // Identity is not a thing a push may edit. This is checked before anything
  // about membership, because a repository whose genesis can move has no
  // membership worth checking.
  if (update.name === Refspec.TRUST_GENESIS && current !== null) {
    return refused(update.name, "the genesis is written once and never moves");
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
    const namespace = yield* namespaceRules(update, current, stored);
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

  const namespace = yield* namespaceRules(update, current, stored);
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
    const revoked = yield* signedByRevoked(update.value, current, input.trust);
    if (revoked !== null) {
      return refused(update.name, `${revoked} has been revoked and may not add events`);
    }
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
  // Asked before the ref exists as well as after. Returning early on a create
  // let the first push of `refs/hub/pr/<new-id>` bring a history of any size
  // at all — onto a namespace nothing can delete, so every later fold, every
  // protected-branch push and every collection paid for it forever.
  const oversized = yield* beyondCeiling(update.name, update.value);
  if (oversized !== null) return refused(update.name, oversized);

  if (current === null) return allowed;
  if (!(yield* contains(current, update.value))) {
    return refused(
      update.name,
      `${update.name} is append-only: the update must contain ${current}`,
    );
  }

  // And it may not graft a second beginning onto the history. Every event a
  // hub ref carries is written onto the ref's current head, so an append-only
  // history has exactly one parentless commit: a pull request's `pr.opened`,
  // a trust log's first record. A push that brings a *new* one is not adding
  // to this history, it is adding another one beside it — and a fold with two
  // roots has to choose between them by something, which on a pull request
  // with no activity yet can only be the oid, which whoever wrote the commit
  // ground. That is how a `hub.create-pr` holder took the authorship, the
  // title and the base of a pull request they had no part in opening.
  const grafted = yield* orphanBeyond(update.name, current, update.value);
  if (grafted !== null) {
    return refused(
      update.name,
      `${update.name} is append-only: ${grafted} begins a second history`,
    );
  }

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
) {
  if (trust.revoked.size === 0) return null;

  const parents = yield* Dag.reachable(to, current, (commit) => Event.isHubCommit(commit)).pipe(
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
    for (const signer of yield* Verify.signers(record.payload, record.signatures)) {
      if (openWindow(trust.revoked.get(signer)) !== null) return signer;
    }
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
  current: Oid,
  to: Oid,
) {
  // Bounded to the namespace's own commits, like every other walk of them. An
  // unbounded one is what a hub commit naming a *source* commit as a second
  // parent turns into: the whole repository history, walked synchronously on
  // the receive-pack path, from a push of one tiny commit. The ceiling check
  // above does not catch it — that walk is bounded, so it simply steps over
  // the foreign parent and finds nothing to refuse.
  const belongs = name.startsWith("refs/hub/") ? Event.isHubCommit : Log.isTrustCommit;
  const parents = yield* Dag.reachable(to, current, (commit) => belongs(commit)).pipe(
    Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
  );
  // Unreadable is not "no second root": the objects arrived with this push, so
  // a missing one is a push this cannot judge rather than one it may wave on.
  if (parents === null) return to;
  for (const [commit, of] of parents) {
    if (of.length > 0 || commit === current) continue;
    if (yield* contains(commit, current)) continue;
    return commit;
  }
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
    return Option.getOrElse(open, () => false)
      ? null
      : "this repository has no membership to authorize a write; run `hub init` to give it one";
  }

  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);
  const principal = { member: who.principal, capabilities: who.capabilities };
  if (principal.member === null) return "authentication required to write refs";
  return may(principal, capability) ? null : `this needs ${capability}`;
});

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
    return Option.getOrElse(open, () => false)
      ? null
      : "this repository has no membership to authorize a write; run `hub init` to give it one";
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

  const rules = yield* rulesOf();

  // The same staleness bound `gate` applies. Left only on receive-pack, a
  // repository that had asked for one still accepted `commit`, `branch`,
  // `tagCreate`, `merge`, `rebase`, `cherry-pick`, `pull` and commit-pack
  // against a membership view of any age — which is most of the ways a ref
  // moves.
  if (rules.maxTrustAgeSeconds > 0) {
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

  const rules = yield* rulesOf();

  // How stale a view may be, checked once for the batch rather than per ref.
  // This is the bound on the one failure a hash-linked log cannot rule out by
  // itself: a replica serving a consistent history that stops short of a
  // revocation. Off unless a repository asks for it, because a bound nobody
  // configured would refuse every push on a repository that never checkpoints.
  const stale =
    trust === null || rules.maxTrustAgeSeconds <= 0
      ? null
      : Verify.fresh(trust, rules.maxTrustAgeSeconds * 1000);
  if (stale !== null && !stale.ok) {
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
  for (const update of updates) {
    // A native client signed an envelope naming the refs it was moving and
    // where to. Checking it here rather than in the guard is not a weakening:
    // the guard runs before the push body exists, so this is the first moment
    // the commands are knowable at all — and the last before they are applied.
    const covered = bindEnvelope
      ? coveredByEnvelope(who.envelope, update)
      : ({ ok: true } as const);
    if (!covered.ok) {
      decisions.push(covered);
      continue;
    }
    decisions.push(
      yield* evaluate({
        update,
        principal: { member: who.principal, capabilities: who.capabilities },
        genesis: stored?.genesis ?? null,
        trust,
        rules,
        folds,
        mentions,
      }),
    );
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
