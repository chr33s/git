/**
 * OpenTelemetry GenAI in, stable Git+ records out.
 *
 * ```text
 * OTel GenAI semconv
 * provider/custom attributes
 * hook fallback
 *         ↓
 *  versioned normalization      ← this module
 *         ↓
 *  stable Git+ audit records
 * ```
 *
 * The contract is one sentence (docs/telemetry.md §4.1): **OTel defines what
 * the incoming signal means; Git+ defines how selected audit facts are
 * stored.** Field names may be renamed here. Meaning may not be changed. So
 * `gen_ai.response.finish_reasons` becomes `response.finishReasons` and stays a
 * list of generation stop reasons — it does not become an error, and it does
 * not merge with span status, however convenient one field would be (§7.3).
 *
 * The second rule is about what is *not* here. Nothing in this module infers.
 * Attempts come from attempt instrumentation or they are absent (§6.2);
 * retries inside one inference span stay inside one logical Invocation (§6.1);
 * a retrieval span becomes a diagnostic and never evidence that anything
 * crossed the invocation boundary, which is Context Exposure's question and
 * only its question (§7.6).
 *
 * The span shape taken here is the flat one an SDK hands a processor —
 * attributes as a map of primitives — not an OTLP envelope. Decoding protobuf
 * or the `{"stringValue": …}` JSON encoding is a collector's job, and putting
 * it here would make this module a partial OTLP implementation that has to
 * track a wire format it has no other reason to know.
 *
 * Two things are deliberately not read. **Baggage** is never consulted: §12.2
 * forbids it carrying integrity or authority for repository identity, member
 * identity, capabilities, instruction authority, pack identity or policy, and
 * the way to guarantee that is to have no code path that looks at it.
 * **Metrics** likewise: §7.8 makes aggregates useful for a fleet dashboard and
 * unusable for reconstructing one historical Invocation, because aggregation
 * loses exactly the event identity an audit is about.
 */
import { Effect, Predicate, Schema } from "effect";

import { Invalid } from "../git/Error.ts";
import type { Capture } from "../context/Exposure.ts";
import * as Records from "./Records.ts";

/** The semantic-convention profile this module claims to interpret. */
export const PROFILE = "open-telemetry/semantic-conventions-genai";

/**
 * Attribute values, taken as they come.
 *
 * Deliberately unconstrained. OTel's attribute model has `int[]`, `double[]`
 * and `bool[]` beside the shapes this module reads, and exporters emit `null`
 * for a value they could not resolve — so a schema listing only what is read
 * here refused whole inference spans over an attribute that would have been
 * ignored anyway, reporting a perfectly good span as "malformed". What a value
 * has to be is decided by the reader that wants it, one attribute at a time,
 * which is where the convention says what type it is.
 */
const Attributes = Schema.Record(Schema.String, Schema.Unknown);

export const SpanEvent = Schema.Struct({
  name: Schema.String,
  attributes: Schema.optional(Attributes),
});

export const Span = Schema.Struct({
  name: Schema.optional(Schema.String),
  traceId: Schema.optional(Schema.String),
  spanId: Schema.optional(Schema.String),
  /**
   * Both spellings of the status code.
   *
   * The strings are OTLP's JSON form; the numbers are `SpanStatusCode` as an
   * OpenTelemetry JS `ReadableSpan` carries it — 0 unset, 1 ok, 2 error — and
   * this module's docstring calls "the flat one an SDK hands a processor" an
   * accepted shape. Taking only the strings, a span exported that way failed
   * `decodeSpan` and `trace record --otel` refused the whole thing as
   * malformed: the same over-strict read the `Attributes` schema just above
   * was deliberately loosened to avoid.
   */
  status: Schema.optional(
    Schema.Struct({
      code: Schema.Union([Schema.Literals(["unset", "ok", "error"]), Schema.Literals([0, 1, 2])]),
      message: Schema.optional(Schema.String),
    }),
  ),
  /**
   * Optional, as OTLP has it.
   *
   * Required, a span that carried none failed as "malformed" — the same
   * dishonest answer the permissive value type above was written to stop,
   * one field up.
   */
  attributes: Schema.optional(Attributes),
  events: Schema.optional(Schema.Array(SpanEvent)),
});
export type Span = typeof Span.Type;

const decodeSpan = Schema.decodeUnknownEffect(Span);
const decoder = new TextDecoder();

/** A span from the JSON an exporter or a harness hook wrote. */
export const decode = Effect.fn("telemetry.Semconv.decode")(function* (bytes: Uint8Array) {
  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "span", reason: "span is not valid JSON" }),
  });
  return yield* decodeSpan(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "span", reason: `malformed span: ${issue.message}` }),
    ),
  );
});

// -- attribute readers ----------------------------------------------------------

/**
 * One attribute, read at the type the convention declares it to have.
 *
 * Coercion is deliberately absent. An attribute an SDK exported as a string
 * where the convention says integer is an attribute somebody is exporting
 * wrongly, and silently parsing it would put a number into an audit record
 * that the upstream signal did not contain.
 */
const text = (span: Span, key: string): string | undefined => {
  const value = span.attributes?.[key];
  return Predicate.isString(value) && value !== "" ? value : undefined;
};

const count = (span: Span, key: string): number | undefined => {
  const value = span.attributes?.[key];
  return Predicate.isNumber(value) && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
};

const flag = (span: Span, key: string): boolean | undefined => {
  const value = span.attributes?.[key];
  return Predicate.isBoolean(value) ? value : undefined;
};

/**
 * A list attribute, taking a lone string as a list of one.
 *
 * `gen_ai.response.finish_reasons` is an array in the convention, and every
 * other exporter writes it as a bare string when there was exactly one. Reading
 * only the array form dropped the finish reason for the common case; reading
 * the string as one element preserves the upstream meaning rather than
 * changing it.
 */
const list = (span: Span, key: string): ReadonlyArray<string> | undefined => {
  const value = span.attributes?.[key];
  if (Array.isArray(value)) {
    // Only the string members: a mixed array is an exporter writing something
    // the convention does not describe, and coercing its numbers into strings
    // would put values in the record that the signal did not contain.
    const strings = value.filter((member) => Predicate.isString(member));
    return strings.length === 0 ? undefined : strings;
  }
  return Predicate.isString(value) && value !== "" ? [value] : undefined;
};

/** A value from a closed vocabulary, or nothing — never a coerced near-miss. */
const oneOf = <A extends string>(
  value: string | undefined,
  allowed: ReadonlyArray<A>,
): A | undefined => allowed.find((candidate) => candidate === value);

// -- §7.1 inference -------------------------------------------------------------

export const operationOf = (span: Span): string | undefined => text(span, "gen_ai.operation.name");

/**
 * Requested and response model, each only when the span carried it.
 *
 * §7.1 forbids copying one into the other. The whole value is omitted when the
 * span said nothing about any of the three, because an empty object in the
 * record reads as "we looked and there was no provider", which is a different
 * claim from "the span did not say".
 */
export const modelOf = (span: Span) => {
  const model = {
    provider: text(span, "gen_ai.provider.name"),
    requested: text(span, "gen_ai.request.model"),
    response: text(span, "gen_ai.response.model"),
  };
  return model.provider === undefined &&
    model.requested === undefined &&
    model.response === undefined
    ? undefined
    : model;
};

/**
 * Provider usage, which stays `reported` however it arrived (§7.2).
 *
 * OTel transport does not upgrade an evidence class: a token count that came
 * through an SDK, a collector and a signed Git record is still a number the
 * provider chose to send, and `source: "provider"` is what keeps a product
 * from printing it beside an observed byte length as though they were the same
 * kind of fact.
 */
export const usageOf = (span: Span): Records.Usage | undefined => {
  const usage = {
    source: "provider" as const,
    inputTokens: count(span, "gen_ai.usage.input_tokens"),
    outputTokens: count(span, "gen_ai.usage.output_tokens"),
    cacheReadInputTokens: count(span, "gen_ai.usage.cache_read.input_tokens"),
    cacheWriteInputTokens: count(span, "gen_ai.usage.cache_write.input_tokens"),
    reasoningOutputTokens: count(span, "gen_ai.usage.reasoning.output_tokens"),
  };
  const reported = [
    usage.inputTokens,
    usage.outputTokens,
    usage.cacheReadInputTokens,
    usage.cacheWriteInputTokens,
    usage.reasoningOutputTokens,
  ].some((value) => value !== undefined);
  return reported ? usage : undefined;
};

/**
 * Operation outcome, from span status and `error.type` — and nothing else.
 *
 * Not from finish reasons, which is the collapse §7.3 exists to forbid: a
 * generation that stopped at a length limit ended the way the caller asked it
 * to, and an audit that painted it red would teach its readers to ignore red.
 */
/** The string name of either spelling of a status code. */
const codeOf = (
  code: Span["status"] extends undefined ? never : NonNullable<Span["status"]>["code"],
) => (code === 0 ? "unset" : code === 1 ? "ok" : code === 2 ? "error" : code);

export const outcomeOf = (span: Span) => {
  const status = span.status === undefined ? undefined : codeOf(span.status.code);
  const errorType = text(span, "error.type");
  if (status === undefined && errorType === undefined) return undefined;
  // `unset` is OTLP's *default* status code, so it is normally on the wire —
  // and instrumentation that sets `error.type` in a catch block without
  // calling `setStatus` is ordinary. Firing the fallback only when `status` is
  // absent made the same failed call read `error` or `unset` depending on
  // whether the producer happened to emit the default, and a tool span with
  // `error.type: "not_found"` rendered as `read_file · unset` with no sign of
  // failure anywhere in the audit. A declared `ok` still wins: that is a
  // producer saying it succeeded, which is not this function's to overrule.
  const said = status === undefined || status === "unset" ? undefined : status;
  return { status: said ?? (errorType === undefined ? "unset" : "error"), errorType } as const;
};

export const responseOf = (span: Span) => {
  const finishReasons = list(span, "gen_ai.response.finish_reasons");
  return finishReasons === undefined ? undefined : { finishReasons };
};

// -- §7.4 agents ----------------------------------------------------------------

export const agentOf = (span: Span) => {
  const agent = {
    id: text(span, "gen_ai.agent.id"),
    name: text(span, "gen_ai.agent.name"),
    version: text(span, "gen_ai.agent.version"),
  };
  return agent.id === undefined && agent.name === undefined && agent.version === undefined
    ? undefined
    : agent;
};

/**
 * The provider's own conversation id, as correlation and nothing else.
 *
 * §7.4: it must not replace Git+ session identity. It lives in its own field
 * for that reason — a value named `conversation.externalId` cannot be mistaken
 * for the session a record is bound to, and the binding stays the signed
 * envelope's.
 */
export const conversationOf = (span: Span) => {
  const externalId = text(span, "gen_ai.conversation.id");
  return externalId === undefined ? undefined : { externalId };
};

// -- §9 context extensions ------------------------------------------------------

/**
 * Git+ extension attributes for context size and limits.
 *
 * Namespaced under `gitplus.` because they are not OTel's: §9 defines them,
 * and reading them out of an OTel attribute called something else would be
 * exactly the "change upstream semantic meaning" §4.1 forbids.
 */
export const CONTEXT_ATTRIBUTES = {
  renderBytes: "gitplus.context.render_bytes",
  contextWindowTokens: "gitplus.context.window_tokens",
  contextWindowSource: "gitplus.context.window_source",
  effectiveInputLimitTokens: "gitplus.context.effective_input_limit_tokens",
  effectiveInputLimitSource: "gitplus.context.effective_input_limit_source",
} as const;

export const contextOf = (span: Span): Records.ContextFacts | undefined => {
  const facts = {
    renderBytes: count(span, CONTEXT_ATTRIBUTES.renderBytes),
    // The one GenAI attribute here; the rest are Git+ extensions.
    compacted: flag(span, "gen_ai.conversation.compacted"),
    contextWindowTokens: count(span, CONTEXT_ATTRIBUTES.contextWindowTokens),
    contextWindowSource: oneOf(
      text(span, CONTEXT_ATTRIBUTES.contextWindowSource),
      Records.LIMIT_SOURCES,
    ),
    effectiveInputLimitTokens: count(span, CONTEXT_ATTRIBUTES.effectiveInputLimitTokens),
    effectiveInputLimitSource: oneOf(
      text(span, CONTEXT_ATTRIBUTES.effectiveInputLimitSource),
      Records.LIMIT_SOURCES,
    ),
  };
  return Object.values(facts).every((value) => value === undefined) ? undefined : facts;
};

// -- §6.2 attempts --------------------------------------------------------------

/** The span event an attempt-instrumented harness emits, one per attempt. */
export const ATTEMPT_EVENT = "gen_ai.attempt";

/**
 * Attempts, and only from attempt instrumentation.
 *
 * §6.2 forbids inferring attempt count or status from duration, timestamp gaps
 * or missing response fields, so the only source here is an explicit event per
 * attempt. A harness without that instrumentation gets `undefined`, which the
 * projection renders as no Attempts section at all rather than as "1 attempt" —
 * the difference between "it succeeded first time" and "nobody was counting".
 *
 * This event name is a documented best-effort mapping rather than an upstream
 * convention, which is why a caller that has not declared a semconv revision
 * gets no `semconv` block: §4.1 lets us map, and forbids claiming adherence.
 */
export const attemptsOf = (span: Span): ReadonlyArray<Records.Attempt> | undefined => {
  // Collected with the index left open where the event did not state one, so
  // the numbers that *were* stated are known before any are invented. Filling
  // by position as they arrived produced collisions the moment instrumentation
  // mixed the two — `[{no index}, {index: 1}]` gave two attempts both numbered
  // 1, written into a signed, immutable record and printed as two identical
  // rows.
  const collected: Array<{
    index: number | null;
    status: Records.Attempt["status"];
    errorType?: string;
  }> = [];
  for (const event of span.events ?? []) {
    if (event.name !== ATTEMPT_EVENT) continue;
    const attributes = event.attributes ?? {};
    const raw = attributes["gen_ai.attempt.index"];
    const declared = attributes["gen_ai.attempt.status"];
    const failure = attributes["error.type"];

    // An event with no index is still an attempt somebody counted. Dropped, a
    // harness emitting only `status`/`error.type` rendered as no Attempts
    // section at all — "nobody was counting" for a run where somebody was,
    // which is the conflation this field exists to prevent.
    const index = Predicate.isNumber(raw) && Number.isSafeInteger(raw) && raw >= 0 ? raw : null;

    const errorType = Predicate.isString(failure) ? failure : undefined;
    const status =
      oneOf(Predicate.isString(declared) ? declared : undefined, Records.STATUSES) ??
      (errorType === undefined ? "unset" : "error");
    collected.push(errorType === undefined ? { index, status } : { index, status, errorType });
  }
  if (collected.length === 0) return undefined;

  // Every event stated its index: they are a total order, so order by them —
  // and then number by position, exactly as the branch below does. Kept
  // verbatim, the two branches numbered from different bases: a 0-based
  // emitter rendered `0 ok / 1 error / 2 ok` here and `1 ok / 2 error / 3 ok`
  // there, so one harness's three attempts came out with different numbers on
  // two runs depending only on whether one event happened to omit its index —
  // in a signed, immutable record. `index` means one thing on every record
  // now: the position in the attempt sequence, counted from one. What a
  // declared index contributes is the order, which is what is read from it.
  //
  // As long as they are actually distinct. Instrumentation that emits two events
  // both carrying `gen_ai.attempt.index: 1` produced a signed, immutable
  // record with two attempts numbered 1 and two identical rows in the audit,
  // which is the failure this collection strategy exists to prevent, reached
  // through this branch instead of the mixed one.
  const declared = collected.flatMap((entry) => (entry.index === null ? [] : [entry.index]));
  if (declared.length === collected.length && new Set(declared).size === declared.length) {
    return collected
      .map((entry) => ({ ...entry, index: entry.index ?? 0 }))
      .sort((left, right) => left.index - right.index)
      .map((entry, at) => ({ ...entry, index: at + 1 }));
  }

  // Some did and some did not, and the two cannot be reconciled: an index
  // scanned up from the lowest free number puts an un-indexed event that was
  // emitted *first* after a later one that declared `1`, and sorting then
  // reverses them — so the audit asserted the first attempt succeeded and a
  // second failed, which is the opposite of what happened.
  //
  // Emission order is what the span actually records, so it is kept and the
  // numbers are the positions in it. That overrides a declared index, which is
  // a real loss — and the alternative is a record whose order is wrong, which
  // is the one thing an attempt list is for.
  return collected.map((entry, at) => ({ ...entry, index: at + 1 }));
};

// -- §7.5 tools -----------------------------------------------------------------

export const toolOf = (span: Span) => {
  const name = text(span, "gen_ai.tool.name");
  if (name === undefined) return undefined;
  return {
    name,
    callId: text(span, "gen_ai.tool.call.id"),
    kind: text(span, "gen_ai.tool.type"),
    description: text(span, "gen_ai.tool.description"),
  };
};

// -- capture --------------------------------------------------------------------

export interface Options {
  /** Where the signal was picked off; see `Records.STAGES` and §12. */
  readonly stage?: string;
  /**
   * The upstream semconv revision this signal declared.
   *
   * Absent means no `semconv` block is written: §4.1 permits a best-effort
   * mapping when the revision is unknown and forbids claiming strict adherence
   * for it, and an empty claim is the only way to say that in a record.
   */
  readonly revision?: string;
  /** The Context Exposure this invocation was made against, by record OID. */
  readonly exposure?: string | null;
  /**
   * The invocation a tool span belongs beneath, by record OID.
   *
   * The tool half of `exposure`, and needed for the same reason: nothing on a
   * `execute_tool` span names a Git record, so without this a tool operation
   * can never be attached and `Invocation.Projection.tools` — "kept beneath
   * the invocation each names when it names one" — always renders detached.
   */
  readonly invocation?: string | null;
}

export const captureOf = (span: Span, options: Options): Capture => {
  const capture = {
    transport: "otel",
    stage: options.stage,
    traceId: span.traceId,
    spanId: span.spanId,
    semconv:
      options.revision === undefined ? undefined : { profile: PROFILE, revision: options.revision },
  };
  return capture;
};

// -- normalization --------------------------------------------------------------

/**
 * What a span is, as far as this module is willing to say.
 *
 * A closed set rather than a partial record with everything optional, because
 * the three kinds are read completely differently downstream: an inference
 * span is one logical Invocation, a tool span is an operation beneath one, and
 * a retrieval span is a *diagnostic* that §7.6 forbids treating as proof that
 * anything crossed the invocation boundary. Anything else is `unsupported`,
 * which a caller can log rather than half-normalize.
 */
export type Normalized =
  | { readonly kind: "inference"; readonly fields: InferenceFields }
  | { readonly kind: "tool"; readonly fields: ToolFields }
  | {
      readonly kind: "retrieval";
      /**
       * Selector diagnostics, never evidence.
       *
       * §7.6: a retrieval span says a retrieval ran. Whether its results
       * reached the model is what a Context Pack answers, and nothing here may
       * stand in for one.
       */
      readonly diagnostics: { readonly operation: string; readonly capture: Capture };
    }
  | { readonly kind: "unsupported"; readonly operation: string | null };

export interface InferenceFields {
  readonly exposure: string | null;
  readonly capture: Capture;
  readonly operation?: { readonly name: string };
  readonly model?: ReturnType<typeof modelOf>;
  readonly usage?: Records.Usage;
  readonly outcome?: ReturnType<typeof outcomeOf>;
  readonly response?: ReturnType<typeof responseOf>;
  readonly context?: Records.ContextFacts;
  readonly agent?: ReturnType<typeof agentOf>;
  readonly conversation?: ReturnType<typeof conversationOf>;
  readonly attempts?: ReadonlyArray<Records.Attempt>;
}

export interface ToolFields {
  readonly invocation: string | null;
  readonly capture: Capture;
  readonly tool: NonNullable<ReturnType<typeof toolOf>>;
  readonly outcome?: ReturnType<typeof outcomeOf>;
}

/** Operations this module reads as one logical inference call (§6.1). */
const INFERENCE = new Set(["chat", "generate_content", "text_completion", "invoke_agent"]);

/**
 * One span, normalized — or refused, without a half-answer.
 *
 * One compliant inference span becomes exactly one logical Invocation (§6.1).
 * Transient retries *inside* it are not split out: if instrumentation exposed
 * them they ride along as attempts, and if it did not they are invisible,
 * which is the honest outcome. A provider fallback that upstream modelled as a
 * second span becomes a second Invocation on its own, because that is what
 * upstream said happened (§6.3).
 */
export const normalize = (span: Span, options: Options = {}): Normalized => {
  const operation = operationOf(span);
  const capture = captureOf(span, options);

  if (operation === "execute_tool") {
    const tool = toolOf(span);
    return tool === undefined
      ? { kind: "unsupported", operation: operation ?? null }
      : {
          kind: "tool",
          fields: {
            invocation: options.invocation ?? null,
            capture,
            tool,
            outcome: outcomeOf(span),
          },
        };
  }

  if (operation === "embeddings" || operation === "retrieve") {
    return { kind: "retrieval", diagnostics: { operation, capture } };
  }

  if (operation === undefined || !INFERENCE.has(operation)) {
    return { kind: "unsupported", operation: operation ?? null };
  }

  return {
    kind: "inference",
    fields: {
      exposure: options.exposure ?? null,
      capture,
      operation: { name: operation },
      model: modelOf(span),
      usage: usageOf(span),
      outcome: outcomeOf(span),
      response: responseOf(span),
      context: contextOf(span),
      agent: agentOf(span),
      conversation: conversationOf(span),
      attempts: attemptsOf(span),
    },
  };
};
