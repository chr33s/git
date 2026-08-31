/**
 * What normalization is allowed to do to an upstream signal, and what it is not.
 *
 * Almost every assertion here is a *negative* one, because that is where the
 * acceptance criteria live (docs/telemetry.md §19): a retry is not a second
 * Invocation, an attempt is not inferred, a finish reason is not an error, a
 * response model is not the requested one, and a retrieval span is not
 * evidence about repository context. Each of those is a shortcut that makes an
 * audit read better and say something false.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import * as Semconv from "./Semconv.ts";

const span = (
  attributes: Semconv.Span["attributes"],
  rest: Partial<Semconv.Span> = {},
): Semconv.Span => ({ attributes, ...rest });

const chat = (extra: Semconv.Span["attributes"] = {}, rest: Partial<Semconv.Span> = {}) =>
  span({ "gen_ai.operation.name": "chat", ...extra }, rest);

describe("GenAI normalization", () => {
  it("maps one compliant inference span to one logical Invocation", () => {
    const normalized = Semconv.normalize(
      chat({
        "gen_ai.provider.name": "anthropic",
        "gen_ai.request.model": "model-x",
        "gen_ai.response.model": "model-x-20260815",
        "gen_ai.usage.input_tokens": 118_420,
        "gen_ai.usage.output_tokens": 4281,
        "gen_ai.usage.cache_read.input_tokens": 90_210,
        "gen_ai.response.finish_reasons": ["stop"],
      }),
      { stage: "sdk-export", exposure: "sha1:" + "a".repeat(40) },
    );

    assert.equal(normalized.kind, "inference");
    if (normalized.kind !== "inference") return;
    assert.equal(normalized.fields.operation?.name, "chat");
    assert.equal(normalized.fields.model?.provider, "anthropic");
    assert.deepEqual(normalized.fields.response?.finishReasons, ["stop"]);
    assert.equal(normalized.fields.capture.stage, "sdk-export");
    assert.equal(normalized.fields.exposure, `sha1:${"a".repeat(40)}`);
  });

  it("keeps the requested and response models distinct", () => {
    const both = Semconv.modelOf(
      chat({ "gen_ai.request.model": "model-x", "gen_ai.response.model": "model-x-20260815" }),
    );
    assert.equal(both?.requested, "model-x");
    assert.equal(both?.response, "model-x-20260815");

    // §7.1: if only one is known, omit the other rather than copying it. A
    // response model copied from the request is a claim about which weights
    // ran that nothing checked.
    const requested = Semconv.modelOf(chat({ "gen_ai.request.model": "model-x" }));
    assert.equal(requested?.requested, "model-x");
    assert.equal(requested?.response, undefined);
  });

  it("keeps provider usage reported however it arrived", () => {
    const usage = Semconv.usageOf(chat({ "gen_ai.usage.input_tokens": 10 }));
    // OTel transport does not upgrade an evidence class (§5).
    assert.equal(usage?.source, "provider");
    assert.equal(usage?.inputTokens, 10);
    assert.equal(usage?.outputTokens, undefined);

    // Nothing reported at all is absent usage, not a row of zeros (§8).
    assert.equal(Semconv.usageOf(chat()), undefined);
  });

  it("keeps a length finish from becoming an error", () => {
    const normalized = Semconv.normalize(
      chat({ "gen_ai.response.finish_reasons": ["length"] }, { status: { code: "ok" } }),
    );
    assert.equal(normalized.kind, "inference");
    if (normalized.kind !== "inference") return;
    // §7.3: a generation that stopped at a length limit is a successful
    // operation, and the two facts stay in two fields.
    assert.equal(normalized.fields.outcome?.status, "ok");
    assert.equal(normalized.fields.outcome?.errorType, undefined);
    assert.deepEqual(normalized.fields.response?.finishReasons, ["length"]);
  });

  it("keeps span status and error class apart", () => {
    const failed = Semconv.outcomeOf(
      span({ "error.type": "timeout" }, { status: { code: "error" } }),
    );
    assert.equal(failed?.status, "error");
    assert.equal(failed?.errorType, "timeout");

    // Neither reported: absent, rather than a confident `unset`.
    assert.equal(Semconv.outcomeOf(span({})), undefined);
  });

  it("reports attempts only when instrumentation emitted them", () => {
    // §6.2 and §19.4: attempt count and status are never inferred from
    // duration, timestamp gaps or missing response fields.
    assert.equal(Semconv.attemptsOf(chat()), undefined);

    const observed = Semconv.attemptsOf(
      span(
        { "gen_ai.operation.name": "chat" },
        {
          events: [
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 2, "gen_ai.attempt.status": "ok" },
            },
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 1, "error.type": "timeout" },
            },
          ],
        },
      ),
    );
    assert.deepEqual(observed, [
      { index: 1, status: "error", errorType: "timeout" },
      { index: 2, status: "ok" },
    ]);
  });

  it("takes a status code in either spelling", () => {
    // An OpenTelemetry JS `ReadableSpan` carries `status.code` as the numeric
    // `SpanStatusCode` enum, and this module accepts "the flat one an SDK
    // hands a processor". Taking only OTLP's JSON strings, a span exported
    // that way failed to decode and `trace record --otel` refused the whole
    // thing as malformed.
    assert.deepEqual(Semconv.outcomeOf(span({}, { status: { code: 2 } })), {
      status: "error",
      errorType: undefined,
    });
    assert.deepEqual(Semconv.outcomeOf(span({}, { status: { code: 1 } })), {
      status: "ok",
      errorType: undefined,
    });
    assert.deepEqual(
      Semconv.outcomeOf(span({ "error.type": "timeout" }, { status: { code: 0 } })),
      {
        status: "error",
        errorType: "timeout",
      },
    );
  });

  it.effect("decodes a span whose status code is the numeric enum", () =>
    Effect.gen(function* () {
      // The refusal was in the schema, not the reader: `decodeSpan` rejected
      // the whole span, so `trace record --otel` called it malformed.
      const decoded = yield* Semconv.decode(
        new TextEncoder().encode(
          JSON.stringify({ attributes: { "gen_ai.operation.name": "chat" }, status: { code: 2 } }),
        ),
      );
      assert.equal(Semconv.outcomeOf(decoded)?.status, "error");
    }),
  );

  it("reads a declared error type as an error even beside the default status", () => {
    // `unset` is OTLP's *default* status code, so it is normally on the wire,
    // and instrumentation that sets `error.type` in a catch block without
    // calling `setStatus` is ordinary. Firing the fallback only when `status`
    // was absent made the same failed call read `error` or `unset` depending
    // on whether the producer emitted the default — and a tool span with
    // `error.type: "not_found"` rendered as `read_file · unset`, with no sign
    // of failure anywhere in the audit.
    assert.deepEqual(
      Semconv.outcomeOf(span({ "error.type": "not_found" }, { status: { code: "unset" } })),
      { status: "error", errorType: "not_found" },
    );

    // A producer that says it succeeded is not this function's to overrule.
    assert.deepEqual(
      Semconv.outcomeOf(span({ "error.type": "not_found" }, { status: { code: "ok" } })),
      { status: "ok", errorType: "not_found" },
    );
  });

  it("numbers attempts from one whichever base the emitter counted from", () => {
    // A 0-based emitter, every index declared and distinct — the branch that
    // kept them verbatim. The mixed branch numbers by position from 1, so the
    // same three attempts from the same harness came out `0 ok / 1 error /
    // 2 ok` on a run where every event carried its index and `1 ok / 2 error /
    // 3 ok` on one where a single event omitted it. Both go into a signed,
    // immutable record, and neither says which base it used.
    const observed = Semconv.attemptsOf(
      span(
        { "gen_ai.operation.name": "chat" },
        {
          events: [
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 0, "gen_ai.attempt.status": "ok" },
            },
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 1, "error.type": "timeout" },
            },
          ],
        },
      ),
    );
    assert.deepEqual(observed, [
      { index: 1, status: "ok" },
      { index: 2, status: "error", errorType: "timeout" },
    ]);
  });

  it("keeps attempts in the order the span emitted them", () => {
    // A failure first, then a success that declared index 1. Renumbering the
    // un-indexed event to the lowest free number and sorting put the success
    // first — so the audit asserted the first attempt succeeded and a second
    // failed, which is the opposite of what happened.
    const observed = Semconv.attemptsOf(
      span(
        { "gen_ai.operation.name": "chat" },
        {
          events: [
            { name: Semconv.ATTEMPT_EVENT, attributes: { "error.type": "overloaded" } },
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 1, "gen_ai.attempt.status": "ok" },
            },
          ],
        },
      ),
    );
    assert.deepEqual(
      observed?.map((attempt) => attempt.errorType ?? attempt.status),
      ["overloaded", "ok"],
    );
    assert.deepEqual(
      observed?.map((attempt) => attempt.index),
      [1, 2],
    );
  });

  it("does not give two attempts the same number", () => {
    // Instrumentation that indexes some events and not others. Filling by
    // position produced a collision — two attempts both numbered 1, written
    // into a signed, immutable record and printed as two identical rows.
    const observed = Semconv.attemptsOf(
      span(
        { "gen_ai.operation.name": "chat" },
        {
          events: [
            { name: Semconv.ATTEMPT_EVENT, attributes: { "error.type": "timeout" } },
            { name: Semconv.ATTEMPT_EVENT, attributes: { "gen_ai.attempt.index": 1 } },
            { name: Semconv.ATTEMPT_EVENT, attributes: { "gen_ai.attempt.status": "ok" } },
          ],
        },
      ),
    );
    assert.deepEqual(
      observed?.map((attempt) => attempt.index),
      [1, 2, 3],
    );
    assert.equal(new Set(observed?.map((attempt) => attempt.index)).size, 3);
  });

  it("does not trust two events that declare the same index", () => {
    // The all-declared branch used the values verbatim, so instrumentation
    // emitting `index: 1` twice produced a signed record with two attempts
    // numbered 1 and two identical rows in the audit.
    const observed = Semconv.attemptsOf(
      span(
        { "gen_ai.operation.name": "chat" },
        {
          events: [
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 1, "error.type": "overloaded" },
            },
            {
              name: Semconv.ATTEMPT_EVENT,
              attributes: { "gen_ai.attempt.index": 1, "gen_ai.attempt.status": "ok" },
            },
          ],
        },
      ),
    );
    assert.equal(new Set(observed?.map((attempt) => attempt.index)).size, 2);
    assert.deepEqual(
      observed?.map((attempt) => attempt.errorType ?? attempt.status),
      ["overloaded", "ok"],
    );
  });

  it("does not split retries inside one span into several Invocations", () => {
    // §6.1 and §19.3: one compliant inference span is one logical Invocation,
    // attempts and all. Two attempts ride along beneath it; they do not become
    // two rows.
    const normalized = Semconv.normalize(
      span(
        { "gen_ai.operation.name": "chat" },
        {
          events: [
            { name: Semconv.ATTEMPT_EVENT, attributes: { "gen_ai.attempt.index": 1 } },
            { name: Semconv.ATTEMPT_EVENT, attributes: { "gen_ai.attempt.index": 2 } },
          ],
        },
      ),
    );
    assert.equal(normalized.kind, "inference");
    if (normalized.kind !== "inference") return;
    assert.equal(normalized.fields.attempts?.length, 2);
  });

  it("reads a retrieval span as diagnostics, never as repository context", () => {
    const normalized = Semconv.normalize(span({ "gen_ai.operation.name": "retrieve" }));
    // §7.6 and §19.9: it must not be treated as proof that retrieved material
    // crossed the invocation boundary. Context Exposure answers that, and only
    // it — so this cannot become an inference record at all.
    assert.equal(normalized.kind, "retrieval");
  });

  it("reads a tool span as an operation, not as an invocation", () => {
    const normalized = Semconv.normalize(
      span({
        "gen_ai.operation.name": "execute_tool",
        "gen_ai.tool.name": "read_file",
        "gen_ai.tool.call.id": "call_7",
        "gen_ai.tool.type": "function",
      }),
    );
    assert.equal(normalized.kind, "tool");
    if (normalized.kind !== "tool") return;
    assert.equal(normalized.fields.tool.name, "read_file");
    assert.equal(normalized.fields.tool.callId, "call_7");
    assert.equal(normalized.fields.tool.kind, "function");
  });

  it.effect("reads a span carrying attribute types it does not use", () =>
    Effect.promise(async () => {
      // OTel's attribute model has `int[]`, `double[]` and `bool[]` beside the
      // shapes read here, and exporters emit `null` for a value they could not
      // resolve. A schema listing only what is read refused the whole span —
      // reporting a good inference span as malformed over an attribute it
      // would have ignored.
      const json = JSON.stringify({
        attributes: {
          "gen_ai.operation.name": "chat",
          "gen_ai.usage.input_tokens": 42,
          "some.int.array": [1, 2, 3],
          "some.bool.array": [true, false],
          "some.unresolved": null,
        },
      });
      const span = await Effect.runPromise(Semconv.decode(new TextEncoder().encode(json)));
      const normalized = Semconv.normalize(span);
      assert.equal(normalized.kind, "inference");
      if (normalized.kind !== "inference") return;
      assert.equal(normalized.fields.usage?.inputTokens, 42);
    }),
  );

  it.effect("reads a span that carried no attributes at all", () =>
    Effect.promise(async () => {
      // `attributes` is optional in OTLP. Required, a span that carried none
      // failed as "malformed" rather than as what it is: a span this version
      // does not record.
      const span = await Effect.runPromise(
        Semconv.decode(new TextEncoder().encode(JSON.stringify({ spanId: "00f067aa0ba902b7" }))),
      );
      assert.equal(Semconv.normalize(span).kind, "unsupported");
    }),
  );

  it("takes only the string members of a mixed list attribute", () => {
    // Coercing the numbers would put values in the record the signal did not
    // contain; dropping the whole attribute would lose the ones it did.
    const mixed = span({ "gen_ai.response.finish_reasons": ["stop", 7, "length"] });
    assert.deepEqual(Semconv.responseOf(mixed)?.finishReasons, ["stop", "length"]);
  });

  it("refuses a span whose operation this version does not read", () => {
    assert.equal(Semconv.normalize(span({})).kind, "unsupported");
    assert.equal(
      Semconv.normalize(span({ "gen_ai.operation.name": "invented" })).kind,
      "unsupported",
    );
  });

  it("claims a semconv revision only when the signal declared one", () => {
    // §4.1: a documented best-effort mapping is allowed; claiming strict
    // adherence for a signal that declared no revision is not.
    const guessed = Semconv.captureOf(chat(), { stage: "hook" });
    assert.equal(guessed.semconv, undefined);

    const declared = Semconv.captureOf(chat(), { stage: "sdk-export", revision: "1.37.0" });
    assert.equal(declared.semconv?.profile, Semconv.PROFILE);
    assert.equal(declared.semconv?.revision, "1.37.0");
  });

  it("reads the context limits as Git+ extensions, under their own names", () => {
    const facts = Semconv.contextOf(
      chat({
        "gen_ai.conversation.compacted": true,
        [Semconv.CONTEXT_ATTRIBUTES.renderBytes]: 483_921,
        [Semconv.CONTEXT_ATTRIBUTES.effectiveInputLimitTokens]: 180_000,
        [Semconv.CONTEXT_ATTRIBUTES.effectiveInputLimitSource]: "harness-config",
        // Not one of the documented sources: dropped rather than stored as a
        // vocabulary value this version does not define.
        [Semconv.CONTEXT_ATTRIBUTES.contextWindowSource]: "a guess",
      }),
    );
    assert.equal(facts?.compacted, true);
    assert.equal(facts?.renderBytes, 483_921);
    assert.equal(facts?.effectiveInputLimitTokens, 180_000);
    assert.equal(facts?.effectiveInputLimitSource, "harness-config");
    assert.equal(facts?.contextWindowSource, undefined);
  });

  it("keeps a conversation id as correlation, in its own field", () => {
    const normalized = Semconv.normalize(chat({ "gen_ai.conversation.id": "conv_42" }));
    assert.equal(normalized.kind, "inference");
    if (normalized.kind !== "inference") return;
    // §7.4: correlation metadata, and never a stand-in for session identity —
    // which is why it is not called `session` anywhere in the record.
    assert.equal(normalized.fields.conversation?.externalId, "conv_42");
  });
});
