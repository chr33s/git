/**
 * The runtime side of the audit trace: what happened around an invocation.
 *
 * ```text
 * refs/hub/trace/<session>
 *   context-exposure       what repository evidence crossed the boundary
 *   invocation-telemetry   what the runtime did with it
 *   tool-operation         what it then did to the world
 *   workspace-transition   tree A → tree B, by Git identity
 *   context-compaction     an observed lifecycle change
 *   trace-health           what this capture path is known to have lost
 * ```
 *
 * Pre-call and post-call are separate signed records because they happen at
 * different times and fail independently (docs/telemetry.md §1): a harness that
 * crashes mid-call has an exposure and no runtime record, and a record shape
 * that required both would have to either lie or lose the exposure. The join is
 * `Invocation.ts`'s job, and it is by Git record OID — §3 — not by timestamp.
 *
 * Three rules shape every schema here, and each of them is a way of not lying:
 *
 * 1. **Unknown is absent.** §8: omit rather than invent zeros. A provider that
 *    reported no token count and one that reported zero are different facts, and
 *    a schema with defaults cannot tell them apart afterwards.
 * 2. **Evidence class travels with the value.** §5: `usage.source` says whether
 *    a number was measured, reported by a provider or estimated, and OTel
 *    transport never upgrades it.
 * 3. **Distinctions stay distinct.** §7.3: span status, error class and
 *    generation finish reason are three fields, because a generation that
 *    stopped at a length limit is a *successful* operation and collapsing them
 *    turns every long answer into an error.
 */
import { DateTime, Effect, Schema } from "effect";

import type { PrivateKey } from "../crypto/SshSignature.ts";
import { Capture } from "../context/Exposure.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Trace from "../hub/Trace.ts";

export { Capture, qualify, unqualify };

/** The record types this version writes and reads. */
export const INVOCATION = "invocation-telemetry";
export const TOOL = "tool-operation";
export const WORKSPACE = "workspace-transition";
export const COMPACTION = "context-compaction";
export const HEALTH = "trace-health";

/**
 * What every trace record carries, whatever it says.
 *
 * The same envelope a session record carries: the repository so a record
 * cannot be replayed into another one, an id of its own, and the trust head
 * its author signed against.
 */
const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  session: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  /** `null` means the author recorded none; see `hub/Event`'s own envelope. */
  trustHead: Schema.NullOr(Schema.String),
};

// -- vocabularies ---------------------------------------------------------------

/**
 * How a runtime value came to be known (§5).
 *
 * `observed` is measured at the harness boundary; `reported` is somebody
 * else's number; `derived` is computed from the other two. A product may not
 * present the last two as the first, which is only possible if the class is
 * stored rather than inferred from which field it landed in.
 */
export const EVIDENCE = ["observed", "reported", "derived"] as const;

/** Where a usage number came from. Provider counts stay `reported` (§7.2). */
export const USAGE_SOURCES = ["provider", "harness", "estimated", "other"] as const;

/** Operation outcome, kept apart from generation finish (§7.3). */
export const STATUSES = ["unset", "ok", "error"] as const;

/** Where a context limit was learned (§9). */
export const LIMIT_SOURCES = [
  "provider",
  "model-catalog",
  "harness-config",
  "runtime",
  "other",
] as const;

/** Where a capture was taken, before anything could sample it (§12). */
export const STAGES = [
  "sdk-export",
  "local-collector",
  "remote-collector",
  "hook",
  "embedded",
  "other",
] as const;

// -- invocation telemetry -------------------------------------------------------

/**
 * Token counts, and who says so.
 *
 * Every field optional because §8 makes absence the honest answer for anything
 * the runtime did not report. `source` is not optional: a number with no
 * evidence class is a number a product will eventually print as a fact.
 */
export const Usage = Schema.Struct({
  source: Schema.Literals(USAGE_SOURCES),
  /** Required by §7.2 when `source` is `estimated`; see `check`. */
  estimator: Schema.optional(Schema.String),
  inputTokens: Schema.optional(Schema.Int),
  outputTokens: Schema.optional(Schema.Int),
  cacheReadInputTokens: Schema.optional(Schema.Int),
  cacheWriteInputTokens: Schema.optional(Schema.Int),
  reasoningOutputTokens: Schema.optional(Schema.Int),
});
export type Usage = typeof Usage.Type;

/**
 * The requested model and the one that answered, never merged.
 *
 * §7.1: if only one is known, omit the other rather than copying it. A
 * response model copied from the request is an unverifiable claim about which
 * weights ran, and it is exactly the claim an audit is asked to settle.
 */
export const Model = Schema.Struct({
  provider: Schema.optional(Schema.String),
  requested: Schema.optional(Schema.String),
  response: Schema.optional(Schema.String),
});

/**
 * One provider attempt, when instrumentation exposed it (§6.2).
 *
 * Never inferred from duration, timestamp gaps or missing response fields —
 * the whole reason this is optional is that a guess here invents retries that
 * did not happen and hides ones that did.
 */
export const Attempt = Schema.Struct({
  index: Schema.Int,
  status: Schema.Literals(STATUSES),
  errorType: Schema.optional(Schema.String),
});
export type Attempt = typeof Attempt.Type;

/**
 * Context size and the limits believed to apply (§9).
 *
 * `renderBytes` is harness-observed and tokenizer-independent, which is why it
 * is the one number here that is never a provider's word. The two limits are
 * different questions — total capacity, and what was usable for *this* call
 * after reserved output — and each carries where it was learned, because a
 * pressure ratio over a denominator from a stale model catalogue is a ratio
 * with no meaning.
 */
export const ContextFacts = Schema.Struct({
  renderBytes: Schema.optional(Schema.Int),
  compacted: Schema.optional(Schema.Boolean),
  contextWindowTokens: Schema.optional(Schema.Int),
  contextWindowSource: Schema.optional(Schema.Literals(LIMIT_SOURCES)),
  effectiveInputLimitTokens: Schema.optional(Schema.Int),
  effectiveInputLimitSource: Schema.optional(Schema.Literals(LIMIT_SOURCES)),
});
export type ContextFacts = typeof ContextFacts.Type;

export const InvocationTelemetry = Schema.Struct({
  type: Schema.tag(INVOCATION),
  ...envelope,
  /**
   * The Context Exposure this invocation was made against, by record OID.
   *
   * `null` for an invocation with no repository context — a harness call that
   * exposed nothing. Never a trace id: §3 makes Git record OIDs the canonical
   * join and everything OTel mints correlation metadata.
   */
  exposure: Schema.NullOr(Schema.String),
  capture: Schema.NullOr(Capture),
  operation: Schema.optional(Schema.Struct({ name: Schema.String })),
  model: Schema.optional(Model),
  usage: Schema.optional(Usage),
  outcome: Schema.optional(
    Schema.Struct({
      status: Schema.Literals(STATUSES),
      errorType: Schema.optional(Schema.String),
    }),
  ),
  response: Schema.optional(Schema.Struct({ finishReasons: Schema.Array(Schema.String) })),
  context: Schema.optional(ContextFacts),
  agent: Schema.optional(
    Schema.Struct({
      id: Schema.optional(Schema.String),
      name: Schema.optional(Schema.String),
      version: Schema.optional(Schema.String),
    }),
  ),
  /** Correlation only; it never stands in for Git+ session identity (§7.4). */
  conversation: Schema.optional(Schema.Struct({ externalId: Schema.String })),
  /** Absent when attempts were not instrumented; never a guess (§6.2). */
  attempts: Schema.optional(Schema.Array(Attempt)),
});
export type InvocationTelemetry = typeof InvocationTelemetry.Type;

// -- tool operations ------------------------------------------------------------

/**
 * What a tool call did, without what it said.
 *
 * §7.5: raw arguments and results are the highest secret-leak content class in
 * the system and are not canonical here. What is durable is the shape of the
 * call and the shape of its answer — a length, a digest, a truncation flag —
 * which is enough to audit "a 40 MB result was cut to 2 KB before the model
 * saw it" without replicating either.
 */
export const ToolOperation = Schema.Struct({
  type: Schema.tag(TOOL),
  ...envelope,
  /** The invocation this call belongs to, by record OID; `null` when unknown. */
  invocation: Schema.NullOr(Schema.String),
  capture: Schema.NullOr(Capture),
  tool: Schema.Struct({
    name: Schema.String,
    callId: Schema.optional(Schema.String),
    kind: Schema.optional(Schema.String),
    description: Schema.optional(Schema.String),
  }),
  outcome: Schema.optional(
    Schema.Struct({
      status: Schema.Literals(STATUSES),
      errorType: Schema.optional(Schema.String),
    }),
  ),
  result: Schema.optional(
    Schema.Struct({
      bytes: Schema.optional(Schema.Int),
      /** Of a body retained elsewhere, so the reference is checkable. */
      digest: Schema.optional(Schema.String),
      truncated: Schema.optional(Schema.Boolean),
    }),
  ),
  /** What it changed in the repository, if anything (§7.5). */
  mutation: Schema.optional(
    Schema.Struct({
      paths: Schema.optional(Schema.Int),
      beforeTree: Schema.optional(Schema.String),
      afterTree: Schema.optional(Schema.String),
    }),
  ),
});
export type ToolOperation = typeof ToolOperation.Type;

// -- workspace ------------------------------------------------------------------

/**
 * Tree A became tree B (§11).
 *
 * Git tree identity is authoritative for workspace state, and that is the
 * whole record: an OTel tool span can say which operation was associated with
 * the change, but only a tree oid says what the workspace *was*. `operation`
 * points at the tool record when one is known.
 */
export const WorkspaceTransition = Schema.Struct({
  type: Schema.tag(WORKSPACE),
  ...envelope,
  beforeTree: Schema.String,
  afterTree: Schema.String,
  operation: Schema.NullOr(Schema.String),
});
export type WorkspaceTransition = typeof WorkspaceTransition.Type;

// -- context lifecycle ----------------------------------------------------------

/**
 * A compaction the harness watched happen (§10).
 *
 * Written *only* when the transition was directly observed with facts to add,
 * which is why `evidence` is here and required: the `context.compacted` boolean
 * on an invocation says a compaction had happened at some point, and a record
 * that merely restated it as an event would be inventing a moment.
 */
export const ContextCompaction = Schema.Struct({
  type: Schema.tag(COMPACTION),
  ...envelope,
  evidence: Schema.Literals(["observed", "reported"]),
  strategy: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(Schema.String),
  beforeTokens: Schema.optional(Schema.Int),
  afterTokens: Schema.optional(Schema.Int),
});
export type ContextCompaction = typeof ContextCompaction.Type;

// -- capture health -------------------------------------------------------------

/**
 * What this capture path is known to have lost (§12.1).
 *
 * The record that makes "complete" sayable. Without one, an absence in the
 * trace means nothing at all — it could be a call that did not happen or a
 * batch a collector dropped — so the projection reports coverage as `unknown`
 * rather than assuming the pipeline behaved.
 */
export const TraceHealth = Schema.Struct({
  type: Schema.tag(HEALTH),
  ...envelope,
  source: Schema.String,
  stage: Schema.optional(Schema.Literals(STAGES)),
  /** `none` is the only value that supports a completeness claim. */
  sampling: Schema.String,
  transformed: Schema.Boolean,
  dropped: Schema.Int,
  /** Export queue overflow, batch dropped, processor filtered, and so on. */
  reasons: Schema.optional(Schema.Array(Schema.String)),
});
export type TraceHealth = typeof TraceHealth.Type;

// -- the union ------------------------------------------------------------------

export const Payload = Schema.Union([
  InvocationTelemetry,
  ToolOperation,
  WorkspaceTransition,
  ContextCompaction,
  TraceHealth,
]);
export type Payload = typeof Payload.Type;

const decodePayload = Schema.decodeUnknownEffect(Payload);
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Every key a trace payload may carry, in the order it is written.
 *
 * One global property list fixes the order of every object in the document and
 * filters out anything not named here, the way `context/Pack.ts` does — with
 * the same warning attached: a field missing from this list is dropped
 * silently, so `Records.test.ts` round-trips a maximal value of every kind.
 */
const KEYS = [
  // `version` covers both the envelope's and `agent.version`: the list is
  // global, so one entry positions the key wherever it appears.
  "version",
  "type",
  "repo",
  "session",
  "id",
  "issuedAt",
  "trustHead",
  "exposure",
  "invocation",
  "capture",
  "transport",
  "stage",
  "traceId",
  "spanId",
  "semconv",
  "profile",
  "revision",
  "operation",
  "name",
  "model",
  "provider",
  "requested",
  "response",
  "finishReasons",
  "usage",
  "source",
  "estimator",
  "inputTokens",
  "outputTokens",
  "cacheReadInputTokens",
  "cacheWriteInputTokens",
  "reasoningOutputTokens",
  "outcome",
  "status",
  "errorType",
  "context",
  "renderBytes",
  "compacted",
  "contextWindowTokens",
  "contextWindowSource",
  "effectiveInputLimitTokens",
  "effectiveInputLimitSource",
  "agent",
  "conversation",
  "externalId",
  "attempts",
  "index",
  "tool",
  "callId",
  "kind",
  "description",
  "result",
  "bytes",
  "digest",
  "truncated",
  "mutation",
  "paths",
  "beforeTree",
  "afterTree",
  "evidence",
  "strategy",
  "reason",
  "beforeTokens",
  "afterTokens",
  "sampling",
  "transformed",
  "dropped",
  "reasons",
];

/** The bytes that are signed and the bytes that are stored, in one encoding. */
export const encode = (payload: Payload): Uint8Array =>
  encoder.encode(`${JSON.stringify(payload, KEYS, 2)}\n`);

export const decode = Effect.fn("telemetry.Records.decode")(function* (bytes: Uint8Array) {
  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "trace", reason: "trace record is not valid JSON" }),
  });
  const payload = yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({ field: "trace", reason: `malformed trace record: ${issue.message}` }),
    ),
  );
  return yield* check(payload);
});

/**
 * The rules a schema cannot state.
 *
 * Two of them, both from §7.2 and §8: an estimate has to name its estimator,
 * because an unattributed estimate is indistinguishable from a measurement
 * once it is in a table; and a qualified OID field has to hold one, because a
 * join by something that is not a record OID is a join §3 does not allow.
 */
export const check = Effect.fn("telemetry.Records.check")(function* (payload: Payload) {
  if (payload.type === INVOCATION) {
    if (payload.usage?.source === "estimated" && payload.usage.estimator === undefined) {
      return yield* new Invalid({
        field: "usage",
        reason: "estimated usage must name the estimator that produced it",
      });
    }
    yield* reference("exposure", payload.exposure);
  }
  if (payload.type === TOOL) yield* reference("invocation", payload.invocation);
  if (payload.type === WORKSPACE) {
    yield* reference("beforeTree", payload.beforeTree);
    yield* reference("afterTree", payload.afterTree);
    yield* reference("operation", payload.operation);
  }
  return payload;
});

const reference = Effect.fnUntraced(function* (field: string, value: string | null) {
  if (value !== null && unqualify(value) === null) {
    return yield* new Invalid({
      field,
      reason: `'${value}' is not a qualified object id this repository can resolve`,
    });
  }
});

// -- writing --------------------------------------------------------------------

/**
 * The envelope every trace record shares, filled in from the repository.
 *
 * `trustHead` is the log head this signer had seen. It is theirs to state, and
 * recording the real one is what lets an honest record be judged against the
 * membership its author could actually see.
 */
export const context = Effect.fn("telemetry.Records.context")(function* (
  repo: string,
  session: string,
) {
  const repository = yield* Repository;
  return {
    version: 1,
    repo,
    session,
    id: Event.newId(),
    issuedAt: DateTime.formatIso(yield* DateTime.now),
    trustHead: yield* repository.resolve(TRUST_LOG),
  } as const;
});

/**
 * Sign one trace record and append it, returning its canonical identity.
 *
 * Validated before it is signed rather than after it is written: an invalid
 * record on an append-only ref is a record nothing can remove, and the ones
 * this refuses are exactly the ones a later reader would have to decide what
 * to believe about.
 */
export const record = Effect.fn("telemetry.Records.record")(function* (
  payload: Payload,
  key: PrivateKey,
) {
  yield* check(payload);
  const commit = yield* Trace.append({
    session: payload.session,
    type: payload.type,
    id: payload.id,
    payload: encode(payload),
    key,
  });
  return { commit, id: payload.id, oid: qualify(commit) } as const;
});

// -- reading --------------------------------------------------------------------

export interface Entry {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly payload: Payload;
  readonly signatures: ReadonlyArray<string>;
}

/**
 * Every telemetry record on one session's trace ref, oldest first.
 *
 * Steps over the kinds it does not own — context exposures are read by
 * `context/Exposure.ts` off the same walk — and reports a record that claims a
 * kind it then fails to decode, which is the only absence here that means
 * something is wrong.
 */
export const entries = Effect.fn("telemetry.Records.entries")(function* (
  session: string,
  taken?: Trace.Walk,
) {
  // A caller that already holds the walk hands it in. `Invocation.project`
  // reads both this namespace and the exposures off one ref, and taking the
  // walk once per reader meant three `Dag.reachable` passes and three payload
  // reads per record for one `session show --audit`.
  const walked = taken ?? (yield* Trace.walk(session));
  const records: Array<Entry> = [];
  const unreadable: Array<Oid> = [...walked.unreadable];

  const kinds = new Set([INVOCATION, TOOL, WORKSPACE, COMPACTION, HEALTH]);
  for (const entry of walked.records) {
    if (entry.type === null || !kinds.has(entry.type)) continue;
    const payload = yield* decode(entry.payload).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) {
      unreadable.push(entry.commit);
      continue;
    }
    records.push({
      commit: entry.commit,
      parents: entry.parents,
      payload,
      signatures: entry.signatures,
    });
  }

  return { records, unreadable, parents: walked.parents } as const;
});

export type RecordError = Invalid | ObjectNotFound | StorageFailure;
