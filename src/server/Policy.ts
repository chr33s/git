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

import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import * as Refspec from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid, RefUpdate } from "../git/Store.ts";
import { type Genesis, readGenesis } from "../trust/Genesis.ts";
import { type Member, project, type Projection as TrustProjection } from "../trust/Projection.ts";
import * as Event from "../hub/Event.ts";
import { approvals, checksPassed, project as projectPr } from "../hub/Projection.ts";
import { permits } from "../trust/Certificate.ts";
import * as Auth from "./Auth.ts";

/**
 * What a branch requires before it will move.
 *
 * Empty by default in every field: a repository that has said nothing about a
 * branch has not protected it, and inventing protection nobody asked for would
 * break every existing push.
 */
export interface Rules {
  /** Branch names, or patterns with a trailing `*`. */
  readonly protected: ReadonlyArray<string>;
  readonly requiredApprovals: number;
  readonly requiredChecks: ReadonlyArray<string>;
  readonly requireResolvedThreads: boolean;
  /** Whether a protected branch may only move through a pull request. */
  readonly requirePullRequest: boolean;
}

export const OPEN: Rules = {
  protected: [],
  requiredApprovals: 0,
  requiredChecks: [],
  requireResolvedThreads: false,
  requirePullRequest: false,
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
 * pushes that have always worked. Rules that will not parse are also `OPEN`
 * rather than a failure: a policy file with a typo must not be a repository
 * nobody can read.
 */
export const rulesOf = Effect.fn("Policy.rulesOf")(function* () {
  const repository = yield* Repository;

  const commit = yield* repository.resolve(RULES_REF);
  if (commit === null) return OPEN;

  const loaded = yield* Effect.gen(function* () {
    const info = yield* repository.readCommit(commit);
    const entry = yield* repository.findPath(info.tree, RULES_PATH);
    if (entry === null) return null;

    const bytes = yield* repository.readBlob(entry.oid);
    const json = yield* Effect.try({
      try: () => JSON.parse(decoder.decode(bytes)),
      catch: () => new Invalid({ field: "policy", reason: "policy is not valid JSON" }),
    });
    return yield* decodeRules(json);
  }).pipe(Effect.orElseSucceed(() => null));

  return loaded === null
    ? OPEN
    : {
        protected: loaded.protected,
        requiredApprovals: loaded.requiredApprovals,
        requiredChecks: loaded.requiredChecks,
        requireResolvedThreads: loaded.requireResolvedThreads,
        requirePullRequest: loaded.requirePullRequest,
      };
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

const isProtected = (rules: Rules, ref: string): boolean =>
  rules.protected.some((pattern) =>
    pattern.endsWith("*") ? ref.startsWith(pattern.slice(0, -1)) : ref === pattern,
  );

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
}) {
  const repository = yield* Repository;
  const { update } = input;
  const current = yield* repository.resolve(update.name);

  // Identity is not a thing a push may edit. This is checked before anything
  // about membership, because a repository whose genesis can move has no
  // membership worth checking.
  if (update.name === Refspec.TRUST_GENESIS && current !== null) {
    return refused(update.name, "the genesis is written once and never moves");
  }

  // Not hub-enabled: no genesis, no members, and nothing here to enforce
  // beyond what the namespaces themselves mean — with one exception. A
  // repository with no identity must not acquire one by push: whoever got
  // there first would own it, and the owner would be locked out of their own
  // repository by a stranger. Identity is established locally, by `hub init`.
  if (input.genesis === null || input.trust === null) {
    if (update.name.startsWith("refs/meta/trust/")) {
      return refused(
        update.name,
        "a repository's identity is established by `hub init`, not by a push",
      );
    }
    return yield* namespaceRules(update, current);
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

  const namespace = yield* namespaceRules(update, current);
  if (!namespace.ok) return namespace;

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
) {
  // The client's own old-oid wins when it declared one. A push that names the
  // value it believes the ref holds is asserting something, and replacing that
  // with whatever the ref holds *now* would make every stale push succeed —
  // which is the check receive-pack's old-oid exists to perform. Where the
  // client said nothing, the value read at decision time is the guarantee,
  // so the approvals counted a moment ago cannot be applied to a moved head.
  const expected = update.expected === undefined ? current : update.expected;
  const allowed: Decision = { ok: true, allowed: { update, expected } };
  if (!Refspec.isAppendOnly(update.name)) return allowed;

  if (update.value === null) {
    return refused(update.name, `${update.name} is append-only and may not be deleted`);
  }
  if (current === null) return allowed;
  return (yield* contains(current, update.value))
    ? allowed
    : refused(update.name, `${update.name} is append-only: the update must contain ${current}`);
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
}) {
  const { rules } = input;
  const needsReview =
    rules.requirePullRequest ||
    rules.requiredApprovals > 0 ||
    rules.requiredChecks.length > 0 ||
    rules.requireResolvedThreads;
  if (!needsReview) return null;

  const repository = yield* Repository;

  for (const id of yield* Event.pullRequests()) {
    const pullRequest = yield* projectPr(input.genesis, input.trust, id);
    if (pullRequest.base !== input.ref || pullRequest.head === null) continue;
    // A closed or already-merged pull request authorizes nothing further.
    // "Descends from an approved head" is not enough on its own: every commit
    // made after a merge descends from it, so one merged pull request would
    // otherwise unlock direct pushes to the branch forever.
    if (pullRequest.state !== "open") continue;

    // The tip has to *be* the reviewed revision, or a merge that takes it as
    // a parent — the commit a merge of this pull request would produce.
    // Anything further along is a different change wearing its name.
    const arriving =
      input.to === pullRequest.head ||
      (yield* repository.readCommit(input.to).pipe(
        Effect.map((commit) => commit.parents.includes(pullRequest.head!)),
        Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)),
      ));
    if (!arriving) continue;

    if (approvals(pullRequest).length < rules.requiredApprovals) {
      return refused(
        input.ref,
        `${id} has ${approvals(pullRequest).length} approvals of its current revision, ` +
          `and ${rules.requiredApprovals} are required`,
      );
    }
    if (!checksPassed(pullRequest, rules.requiredChecks)) {
      return refused(input.ref, `${id} has not passed ${rules.requiredChecks.join(", ")}`);
    }
    if (rules.requireResolvedThreads && pullRequest.threads.some((thread) => !thread.resolved)) {
      return refused(input.ref, `${id} has unresolved review threads`);
    }
    return null;
  }

  return refused(input.ref, `${input.ref} may only be moved by an approved pull request`);
});

/**
 * Judge a batch, then apply what passed under the values it was judged against.
 *
 * All or nothing when `atomic` is set, which is what receive-pack's own
 * capability means. The compare-and-swap is not optional: a Durable Object
 * serializes these anyway, and the node and filesystem backends do not, so the
 * guarantee has to come from the update rather than from the host.
 */
export const apply = Effect.fn("Policy.apply")(function* (input: {
  readonly updates: ReadonlyArray<RefUpdate>;
  readonly principal: Principal;
  readonly genesis: Genesis | null;
  readonly trust: TrustProjection | null;
  readonly rules: Rules;
  readonly atomic?: boolean;
}) {
  const repository = yield* Repository;

  const decisions: Decision[] = [];
  for (const update of input.updates) {
    decisions.push(
      yield* evaluate({
        update,
        principal: input.principal,
        genesis: input.genesis,
        trust: input.trust,
        rules: input.rules,
      }),
    );
  }

  const refusals = decisions.filter((decision) => !decision.ok);
  if (input.atomic === true && refusals.length > 0) {
    return { applied: [], refused: refusals };
  }

  const allowed = decisions.flatMap((decision) => (decision.ok ? [decision.allowed] : []));
  const results = yield* repository.receive(
    allowed.map((entry) => ({ ...entry.update, expected: entry.expected })),
    { atomic: input.atomic ?? false },
  );

  return { applied: results, refused: refusals };
});

export type PolicyError = Invalid | ObjectNotFound | StorageFailure;

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
export const gateWrite = Effect.fn("Policy.gateWrite")(function* (ref: string) {
  const stored = yield* readGenesis();
  if (stored === null) return null;

  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);
  const principal = { member: who.principal, capabilities: who.capabilities };

  if (principal.member === null) return "authentication required to write refs";
  if (!may(principal, "source.push")) return "pushing needs source.push";
  if (ref === Refspec.TRUST_GENESIS) return "the genesis is written once and never moves";
  if (Refspec.isAppendOnly(ref)) return `${ref} may only be appended to by the hub`;
  if (ref === RULES_REF && !may(principal, "policy.write")) {
    return `writing ${RULES_REF} needs policy.write`;
  }

  const rules = yield* rulesOf();
  return isProtected(rules, ref)
    ? `${ref} is protected and may only be moved by an approved pull request`
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
) {
  // A failure to read identity is not "this repository has none": treating it
  // that way would drop every rule below at the moment storage was least
  // trustworthy. `null` means the ref is genuinely absent.
  const stored = yield* readGenesis();
  const trust = stored === null ? null : yield* project(stored.genesis);
  const requester = yield* Effect.serviceOption(Auth.Requester);
  const who = Option.getOrElse(requester, () => Auth.anonymous);

  const rules = yield* rulesOf();
  const decisions: Decision[] = [];
  for (const update of updates) {
    // A native client signed an envelope naming the refs it was moving and
    // where to. Checking it here rather than in the guard is not a weakening:
    // the guard runs before the push body exists, so this is the first moment
    // the commands are knowable at all — and the last before they are applied.
    const covered = coveredByEnvelope(who.envelope, update);
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
