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
import { DateTime, Effect, Predicate, Schema } from "effect";

import type { PrivateKey } from "../crypto/SshSignature.ts";
import { Capture, checkCapture, STAGES } from "../context/Exposure.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Secrets from "../hub/Secrets.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import * as Claim from "../hub/Claim.ts";
import * as Trace from "../hub/Trace.ts";
import type { Projection } from "../trust/Projection.ts";
import { trustReach } from "../hub/Projection.ts";
import * as Verify from "../trust/Verify.ts";

export { Capture, qualify, unqualify };

/** The record types this version writes and reads. */
export const INVOCATION = "invocation-telemetry";
export const TOOL = "tool-operation";
export const WORKSPACE = "workspace-transition";
export const COMPACTION = "context-compaction";
export const HEALTH = "trace-health";
/** The tag every hub namespace spells the same way; see `hub/Tombstone.ts`. */
export const REDACTED = Tombstone.TAG;

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

/**
 * Where a capture was taken, before anything could sample it (§12).
 *
 * Defined beside `Capture` and re-exported here, so the vocabulary a writer is
 * held to and the one a `trace-health` record declares cannot drift apart.
 */
export { STAGES } from "../context/Exposure.ts";

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
  /**
   * A bare string, for the reason `context/Exposure.Capture.stage` is one: a
   * closed vocabulary here is a closed vocabulary on the *read* path, so a
   * health record naming a stage a newer producer knows failed `decode`
   * outright — `entries` called it unreadable, `session show --audit` reported
   * damage, and `coverageOf` lost it, dropping coverage from `complete` to
   * `unknown` permanently on an append-only ref. `check` holds a writer to the
   * vocabulary; see there.
   */
  stage: Schema.optional(Schema.String),
  /** `none` is the only value that supports a completeness claim. */
  sampling: Schema.String,
  transformed: Schema.Boolean,
  dropped: Schema.Int,
  /** Export queue overflow, batch dropped, processor filtered, and so on. */
  reasons: Schema.optional(Schema.Array(Schema.String)),
});
export type TraceHealth = typeof TraceHealth.Type;

// -- the union ------------------------------------------------------------------

/**
 * A record removed from this trace.
 *
 * The namespace needs one for the same reason a session does, and more
 * urgently: a retained render carries the task string verbatim and the exact
 * bytes of every exposed file, so a credential that leaked into a prompt is in
 * the trace as well as in the session. Without a tombstone here, `session
 * redact` removed the prompt from the account of the work and left it readable
 * in the account of the runtime — a removal that was not one.
 *
 * The same tag all three namespaces spell the same way, so `Tombstone.claims`
 * charges `hub.redact` at the boundary without knowing which ref it is on, and
 * `Redaction` finds it with the walk it already makes.
 */
export const RecordRedacted = Schema.Struct({
  type: Schema.tag(Tombstone.TAG),
  ...envelope,
  ...Tombstone.fields,
});
export type RecordRedacted = typeof RecordRedacted.Type;

export const Payload = Schema.Union([
  InvocationTelemetry,
  ToolOperation,
  WorkspaceTransition,
  ContextCompaction,
  TraceHealth,
  RecordRedacted,
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
  "targetCommit",
  "target",
];

/** The bytes that are signed and the bytes that are stored, in one encoding. */
export const encode = (payload: Payload): Uint8Array =>
  encoder.encode(`${JSON.stringify(payload, KEYS, 2)}\n`);

export const decode = Effect.fn("telemetry.Records.decode")(function* (bytes: Uint8Array) {
  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "trace", reason: "trace record is not valid JSON" }),
  });
  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({ field: "trace", reason: `malformed trace record: ${issue.message}` }),
    ),
  );
});

/**
 * The rules a schema cannot state.
 *
 * Two of them, both from §7.2 and §8: an estimate has to name its estimator,
 * because an unattributed estimate is indistinguishable from a measurement
 * once it is in a table; and a qualified OID field has to hold one, because a
 * join by something that is not a record OID is a join §3 does not allow.
 */
/**
 * A count that cannot be negative, because none of these can be.
 *
 * `Schema.Int` is signed, and the `--event` path takes caller JSON straight
 * through — `Semconv` guards its own reads and nothing guarded this one. A
 * negative `inputTokens` rendered as `pressure -5% of the effective input
 * limit (derived)`, which is a derived number presented as a measurement of
 * something impossible.
 */
const counting = Effect.fnUntraced(function* (field: string, value: number | undefined) {
  if (value !== undefined && value < 0) {
    return yield* new Invalid({ field, reason: `${field} cannot be negative` });
  }
});

/**
 * What a record's *signed* payload says it is, or `null` where it will not
 * decode.
 *
 * The commit message carries the same word, and it is the one a reader reaches
 * for first because it survives redaction — but nobody signed it. Every
 * decision that changes what is written or what is removed asks this instead.
 */
const typeOf = Effect.fnUntraced(function* (bytes: Uint8Array) {
  const payload = yield* decode(bytes).pipe(Effect.orElseSucceed(() => null));
  return payload?.type ?? null;
});

/**
 * The rules a *writer* is held to, which is not the same as the rules a reader
 * applies.
 *
 * `hub/Event` keeps these apart — `decode` gives you the payload and the fold
 * calls `validate` separately, where a failure skips one event rather than
 * making it unreadable — and this namespace collapsed the two by calling this
 * from `decode`. So an open vocabulary became a closed one on the read path:
 * `checkCapture` exists precisely because `stage` is a bare string "so an
 * older reader can still read a stage a newer producer names", and folding it
 * in here meant a record whose `capture.stage` this version does not know read
 * as damage. `session show --audit` printed "No invocations recorded for this
 * session." and "1 record(s) could not be read here." for a whole invocation —
 * its usage, its attempts, its context join — permanently, on a ref nothing
 * can rewind, because one field named a collector this version had not heard
 * of.
 *
 * The rest go the same way for the same reason. A negative count and an
 * unqualified reference are writer errors this refuses at the door; a reader
 * that hits one is reading a record somebody already wrote, and dropping the
 * whole record tells it less than reading it does. Both are then judged where
 * they are used: `tombstonesOn` unqualifies and steps over, `verified` refuses
 * a timestamp it cannot parse.
 */
export const check = Effect.fn("telemetry.Records.check")(function* (payload: Payload) {
  // The one field a schema cannot state and every namespace but these two
  // checked. `hub/Event.validate` refuses this before an event is treated as a
  // statement, and both of these skipped it — so a record carrying
  // `"issuedAt": "not-a-date"` decoded, and the `Verify.Made` built from it a
  // few lines later carried an `Invalid Date`. Inert only because
  // `Verify.authorize` happens never to read `made.at` today; `at` is on the
  // public interface, and the next reader of it would have mis-judged exactly
  // these two record kinds and no others.
  if (Number.isNaN(Date.parse(payload.issuedAt))) {
    return yield* new Invalid({ field: "issuedAt", reason: `not a date: '${payload.issuedAt}'` });
  }

  if (payload.type === INVOCATION) {
    yield* counting("inputTokens", payload.usage?.inputTokens);
    yield* counting("outputTokens", payload.usage?.outputTokens);
    yield* counting("cacheReadInputTokens", payload.usage?.cacheReadInputTokens);
    yield* counting("cacheWriteInputTokens", payload.usage?.cacheWriteInputTokens);
    yield* counting("reasoningOutputTokens", payload.usage?.reasoningOutputTokens);
    yield* counting("renderBytes", payload.context?.renderBytes);
    yield* counting("contextWindowTokens", payload.context?.contextWindowTokens);
    yield* counting("effectiveInputLimitTokens", payload.context?.effectiveInputLimitTokens);
    for (const attempt of payload.attempts ?? []) yield* counting("index", attempt.index);
  }
  if (payload.type === HEALTH) {
    yield* counting("dropped", payload.dropped);
    // The same vocabulary `checkCapture` holds a capture to, asked directly
    // rather than through a `Capture` this record does not have.
    if (payload.stage !== undefined && !STAGES.some((known) => known === payload.stage)) {
      return yield* new Invalid({
        field: "stage",
        reason: `'${payload.stage}' is not a capture stage; one of ${STAGES.join(", ")}`,
      });
    }
  }
  if (payload.type === INVOCATION || payload.type === TOOL) {
    yield* checkCapture(payload.capture);
  }
  if (payload.type === TOOL) {
    yield* counting("bytes", payload.result?.bytes);
    yield* counting("paths", payload.mutation?.paths);
  }
  if (payload.type === COMPACTION) {
    yield* counting("beforeTokens", payload.beforeTokens);
    yield* counting("afterTokens", payload.afterTokens);
  }
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
  // The one field whose value drives an irreversible deletion, and the one
  // that was not checked: an unqualified `targetCommit` is accepted, signed and
  // appended, and `Redaction.tombstonesOn` then unqualifies it to `null` and
  // steps over it — a redaction that reports success and is never honoured,
  // leaving the bytes the operator was told were gone clonable forever.
  if (payload.type === REDACTED) yield* reference("targetCommit", payload.targetCommit);
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
 * The parts of a trace record a person or an agent wrote.
 *
 * Object ids, ref names, model names and the envelope are this repository and
 * its provider talking about themselves; a secret does not arrive that way,
 * and scanning them only makes the scan wrong — the same reasoning
 * `hub/Session.prose` gives. What is left is free text somebody typed.
 *
 * The render body is deliberately not here. It is the exposed repository bytes
 * verbatim, which a heuristic scanner would refuse constantly and which
 * `--retain-render=false` and `trace redact` are the answers for.
 */
const prose = (payload: Payload) => {
  // Every writer-supplied string on the record, found by walking it, rather
  // than a list of the ones somebody remembered. The list was wrong four times
  // over: `TraceHealth.source`/`sampling`, `attempts[].errorType`,
  // `usage.estimator` and `capture.transport` were each added only after a
  // reviewer found the same token refused in one field and signed in the next,
  // and `tool.name`, `agent.name`, `conversation.externalId`, `operation.name`
  // and `response.finishReasons` were still open. A harness that records the
  // invoked command line as the tool name puts the whole command, headers
  // included, on a ref this version cannot rewind.
  //
  // What is skipped is what this repository and its provider say about
  // themselves rather than what somebody typed: the envelope, and any value
  // that is a qualified object id or digest. A secret does not arrive as
  // `sha1:<forty hex>`, and scanning those only makes the scan wrong. Every
  // other string goes in, including a field added tomorrow.
  const said: Array<string> = [];
  const opaque: Array<string> = [];
  // The values inside a decoded payload: strings, numbers, booleans, and the
  // objects and arrays that hold them. Not a parse boundary — `decodePayload`
  // was that, above — so this walks what the schema already produced.
  type Held = string | number | boolean | null | undefined | { [key: string]: Held } | Held[];
  const walk = (value: Held, identifier: boolean): void => {
    if (Predicate.isString(value)) {
      if (!QUALIFIED.test(value)) (identifier ? opaque : said).push(value);
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) walk(item, identifier);
      return;
    }
    if (!Predicate.isObject(value)) return;
    for (const [name, held] of Object.entries(value))
      walk(held, identifier || IDENTIFIER.has(name));
  };
  for (const [name, held] of Object.entries(payload)) {
    if (ENVELOPE.has(name)) continue;
    // SAFETY: `payload` came from `decodePayload`, so every value in it is one
    // of the JSON shapes `Held` names.
    walk(held as Held, IDENTIFIER.has(name));
  }
  return { said: said.join("\n"), opaque: opaque.join("\n") } as const;
};

/**
 * The fields the recorder fills in, not the caller; see `context`.
 *
 * Matched against the payload's *own* keys and no deeper. Applied at every
 * depth, `id` and `version` also named `agent.id` and `agent.version` — two
 * writer-supplied strings fed straight from `gen_ai.agent.id` and
 * `gen_ai.agent.version` — so a token in `agent.name` was refused and the same
 * token in `agent.id` was signed.
 */
const ENVELOPE = new Set(["type", "version", "repo", "session", "id", "issuedAt", "trustHead"]);

/** A qualified object id or digest: this repository naming its own objects. */
const QUALIFIED = /^(?:sha1|sha256):[0-9a-f]+$/;

/**
 * Fields whose value is an identifier somebody else minted.
 *
 * Still scanned, but only for the *shapes* a credential has — a `ghp_` prefix,
 * a connection string, a `key=` — and not for entropy.
 * `conversation.externalId` is documented as "the provider's own conversation
 * id, as correlation and nothing else", `tool.callId` and `agent.id` are the
 * same, and an opaque base62 id of thirty-two characters is indistinguishable
 * from a token by entropy alone. Scanned as prose, `trace record` refused the
 * whole `invocation-telemetry` record — no override, nothing written, and the
 * only way through was for the harness to drop the correlation id §7.4 exists
 * to record. UUID- and hex-shaped ids stay under the threshold, so it failed
 * for some providers and not others, which is the worst way for it to fail.
 */
const IDENTIFIER = new Set(["externalId", "callId", "traceId", "spanId", "digest", "id"]);

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
  /** Set only by `redact`, which has already checked what the tombstone names. */
  writing = false,
) {
  yield* check(payload);
  // Scanned before it is written, not after it has replicated — the reasoning
  // `Session.issue` gives, and this namespace was the only one of the four
  // without it while taking caller JSON straight through `trace record
  // --event` onto an append-only ref.
  const parts = prose(payload);
  // The entropy rule is dropped for identifiers and kept for everything else;
  // the pattern rules apply to both.
  const leaked = [
    ...Secrets.scan(parts.said),
    ...Secrets.scan(parts.opaque).filter((finding) => finding.kind !== "high-entropy string"),
  ];
  if (leaked.length > 0) {
    return yield* new Invalid({
      field: "trace",
      reason: `this record looks like it carries ${leaked
        .map((finding) => `a ${finding.kind} (${finding.hint})`)
        .join(", ")}`,
    });
  }
  // Charged here rather than in `redact` alone, because this is the door every
  // writer comes through: `trace record --event` feeds caller JSON straight
  // into this function, so a member holding only `hub.trace` could write the
  // tombstone tag by hand and skip the one local check that turns a lapsed or
  // unauthorised redactor away. Worse than skipping it — the boundary charges
  // `hub.redact` for any record whose bytes claim the tag, so the ref became
  // unpushable for good on a namespace nothing can rewind.
  if (payload.type === REDACTED) {
    yield* Tombstone.permitted(key);
    // And it has to have come through `redact`, which is the only path that
    // checks the target is on *this* session's ref, is unambiguous, and is not
    // itself a tombstone. A hand-written one reaching here satisfied none of
    // those: `Redaction.marks` pools tombstones across refs and honours a
    // cross-session one for `gc`, while the target session's own projection
    // never lists it — so a record was removed and the session that held it
    // reported it merely unreadable.
    if (!writing) {
      return yield* new Invalid({
        field: "type",
        reason: "a tombstone is written with `trace redact`, which checks what it names",
      });
    }
  }
  const commit = yield* Trace.append({
    session: payload.session,
    type: payload.type,
    id: payload.id,
    payload: encode(payload),
    key,
  });
  return { commit, id: payload.id, oid: qualify(commit) } as const;
});

/**
 * Remove one record's content from this trace.
 *
 * `target` is the record's own id or its commit oid, whichever the caller
 * holds — an operator acting on a leak has the oid an audit printed, not an
 * event id buried in a payload. Either way the tombstone names the *commit*,
 * because a commit is what stays stable across exactly the change a tombstone
 * makes. Nothing is deleted: the tombstone replicates, and the bytes go at the
 * next `gc`, which is the only pass that can tell whether the blob is
 * reachable from anywhere else — see `hub/Redaction.ts`.
 *
 * What the tombstone covers is the record's own payload — `event.json`, and
 * for a Context Exposure `context/pack.json` and `context/render.bin` too.
 * The render is the point: it is where the verbatim prompt and the exposed
 * file bytes are — and `context/view` goes too, because a dirty checkout's
 * overlay is reachable through this record and nothing else. What a branch
 * also reaches survives regardless; see `hub/Redaction.ATTACHED`.
 *
 * "Covers" is not "removes", and the gap is the whole of `Redaction.withheld`.
 * A Pack and a ContextRender are deterministic, so two exposures of one view
 * and one task name one object — and `gc` will not delete an object a live
 * record still needs. The removal is then partial, which is the honest and
 * only safe answer, but it has to be said out loud: `trace redact` asks
 * `withheld` and names the records still holding the bytes, because otherwise
 * this reports success while the verbatim prompt stays clonable off the ref.
 */
export const redact = Effect.fn("telemetry.Records.redact")(function* (input: {
  readonly repo: string;
  readonly session: string;
  readonly target: string;
  readonly reason: string;
  readonly key: PrivateKey;
}) {
  // `record` charges the capability for any tombstone; asked again here only
  // to refuse before the walk rather than after it.
  yield* Tombstone.permitted(input.key);

  const walked = yield* Trace.walk(input.session);
  const wanted = unqualify(input.target);
  const matches = (entry: { readonly id: string | null; readonly commit: Oid }) =>
    entry.id === input.target || (wanted !== null && entry.commit === wanted);
  // The unreadable records too. One whose payload another replica already
  // collected is present on this ref and undecodable here, and refusing to
  // name it left the operator unable to write the local tombstone that would
  // explain the absence.
  const readable = walked.records.filter((entry) => matches(entry));
  const claimants = [...readable, ...walked.unreadable.filter((entry) => matches(entry))];
  if (claimants.length === 0) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.session} has no trace record ${input.target}`,
    });
  }
  if (claimants.length > 1) {
    return yield* new Invalid({
      field: "target",
      reason: `${input.session} has ${claimants.length} records claiming ${input.target}`,
    });
  }
  const found = claimants[0]!;
  // From the payload where there is one, and only from the commit message
  // where there is not — a record whose payload another replica collected is
  // the one case with nothing else to go on. `type` comes off the *unsigned*
  // commit message, so a tombstone whose message said anything else passed
  // this guard: redacting it destroyed the only decodable record of the
  // earlier removal, `Redaction.tombstonesOn` stopped finding it, and `gc`
  // re-protected the payload the operator had been told was gone.
  const decodable = readable[0];
  const claimed = decodable === undefined ? found.type : yield* typeOf(decodable.payload);
  if (claimed === REDACTED) {
    return yield* new Invalid({
      field: "target",
      reason: "a tombstone is the record of a removal and is not itself removable",
    });
  }

  const written = yield* record(
    {
      ...(yield* context(input.repo, input.session)),
      type: REDACTED,
      target: input.target,
      targetCommit: qualify(found.commit),
      reason: input.reason,
    },
    input.key,
    true,
  );
  // The record the removal is *about*, beside the tombstone that states it. A
  // caller asking what the removal will actually take needs the target, and
  // recovering it meant re-walking the ref and re-decoding the tombstone that
  // had just been written.
  return { ...written, targetCommit: found.commit } as const;
});

// -- reading --------------------------------------------------------------------

export interface Entry {
  readonly commit: Oid;
  readonly parents: ReadonlyArray<Oid>;
  readonly payload: Payload;
  /** The exact bytes every signature covers. */
  readonly bytes: Uint8Array;
  readonly signatures: ReadonlyArray<string>;
}

/**
 * Every telemetry record on one session's trace ref, oldest first.
 *
 * Steps over the kinds it does not own — context exposures are read by
 * `context/Exposure.ts` off the same walk — and reports a record that claims a
 * kind it then fails to decode, which is the only absence here that means
 * something is wrong.
 *
 * What it owns is decided by the signed payload, and only falls back to the
 * commit message when there is no payload left to ask. The message is a hint
 * that survives redaction, which is why `unreadable` still leans on it; it is
 * also unsigned, which is why nothing else does.
 */

export const entries = Effect.fn("telemetry.Records.entries")(function* (
  session: string,
  taken?: Trace.Walk,
  /**
   * This repository's own id, where the caller knows it.
   *
   * The other half of the binding, and it belongs here for the reason the
   * session half does: `Invocation.project` was doing it with a bare filter
   * whose rejects went nowhere — not into `foreign`, not into `unreadable` —
   * so a record naming another repository vanished from the audit entirely,
   * while the identical mismatch on the session was reported. A member holding
   * `hub.trace` in two repositories is enough to land one, since the boundary
   * does not read payload contents.
   */
  repo?: string,
) {
  // A caller that already holds the walk hands it in. `Invocation.project`
  // reads both this namespace and the exposures off one ref, and taking the
  // walk once per reader meant three `Dag.reachable` passes and three payload
  // reads per record for one `session show --audit`.
  const walked = taken ?? (yield* Trace.walk(session));
  const records: Array<Entry> = [];
  /**
   * Records on this ref whose own signed payload names a different session or
   * repository, with what each says it is.
   *
   * The kind matters to a caller: an `invocation-telemetry` a peer landed is
   * still a boundary in the history even though this session will not render
   * it, and `Invocation.project` has to treat it as one or a transition after
   * it is attributed to the invocation before.
   */
  const foreign: Array<{ readonly commit: Oid; readonly type: Payload["type"] }> = [];
  // Through the same partition. A redaction takes the payload and leaves the
  // commit, so the message is all that is left to say whose record it was —
  // and each reader seeded this list with its own test of that message, which
  // is where two of the six variants came from: a message naming nothing, and
  // a message naming something neither namespace knows. `ownerOf` answers
  // both, because it answers everything.
  const unreadable: Array<Oid> = walked.unreadable
    .filter((entry) => Claim.ownerOf(entry) === "telemetry")
    .map((entry) => entry.commit);

  for (const entry of walked.records) {
    // One question, asked in one place, for both readers; see `hub/Claim.ts`.
    // Telemetry is the default side of that partition, which is what makes it
    // total: a record nothing else claims is still something this ref holds,
    // and a reader that says so is the difference between an audit somebody
    // can walk around and one they cannot.
    if (Claim.ownerOf(entry) !== "telemetry") continue;

    const payload = yield* decode(entry.payload).pipe(Effect.orElseSucceed(() => null));
    // Ours and unreadable is damage — the absence an audit exists to report.
    if (payload === null) {
      unreadable.push(entry.commit);
      continue;
    }

    // Bound to the ref it was read from and to this repository. Nothing on
    // this read path checked either: `check` looks at timestamps, counts and
    // oid shapes, and `verified` asks only for a good signature from a
    // `hub.trace` holder — so a validly signed `trace-health` record from
    // another session, appended here because replication is not policy-gated,
    // folded into this session's coverage and flipped it from `unknown` to
    // `complete`. Reported rather than dropped, for the reason
    // `Exposure.entries` reports its own.
    if (!Claim.bound(payload, { repo, session })) {
      foreign.push({ commit: entry.commit, type: payload.type });
      continue;
    }

    records.push({
      commit: entry.commit,
      parents: entry.parents,
      payload,
      bytes: entry.payload,
      signatures: entry.signatures,
    });
  }

  return { records, unreadable, foreign, parents: walked.parents } as const;
});

/**
 * Whether a trace record was signed by somebody this repository trusted then.
 *
 * The same question `context/Exposure.audit` asks of an exposure, asked of the
 * runtime half — and for the same reason its docstring gives: a record that
 * arrived by replication never passed this host's boundary, so "who signed it,
 * and could they" is one the reader has to be able to ask for itself. Without
 * it a `session show --audit` verified its Context half and took its Runtime
 * half, its tool list and — worst — its `coverage: "complete"` claim entirely
 * on faith.
 *
 * `made` dates the judgement, so a signer who has since left does not
 * retroactively invalidate what they wrote; see `Exposure.trusted`.
 */
export const verified = Effect.fn("telemetry.Records.verified")(function* (input: {
  readonly entry: Entry;
  readonly bytes: Uint8Array;
  readonly projection: Projection;
  /** One trust-log walk shared across a run; see `context/Exposure.audit`. */
  readonly reach?: ReturnType<typeof trustReach>;
}) {
  // Judged here rather than refused at `decode`, which would have made the
  // whole record unreadable over a field only this needs. `Verify.Made` takes
  // a `Date`, and `new Date("not-a-date")` is an `Invalid Date` that compares
  // false against everything — so a record with an unparseable timestamp would
  // be judged against a moment that does not exist. It is not trusted instead.
  const at = Date.parse(input.entry.payload.issuedAt);
  if (Number.isNaN(at))
    return `record is dated '${input.entry.payload.issuedAt}', which is not a date`;
  const made = new Date(at);

  // `signed` for the reason `context/Exposure.trusted` passes it: `authorize`
  // verifies the list itself otherwise, and a projection over a long session
  // paid for every signature twice, over attacker-supplied input.
  const asked = {
    projection: input.projection,
    bytes: input.bytes,
    signatures: input.entry.signatures,
    signed: yield* Verify.signers(input.bytes, input.entry.signatures),
    capability: CAPABILITY,
    made: {
      at: made,
      trustHead: headOf(input.entry.payload.trustHead),
    },
  };
  const decision = yield* input.reach === undefined
    ? Verify.authorize(asked)
    : Verify.authorize({ ...asked, seen: input.reach.ancestry, contains: input.reach.contains });
  return decision.ok ? null : decision.reason;
});

/** The capability a trace producer holds; the same one the boundary charges. */
export const CAPABILITY = "hub.trace";

const headOf = (value: string | null): Oid | null =>
  value !== null && isOid(value) ? value : null;

export type RecordError = Invalid | ObjectNotFound | StorageFailure;
