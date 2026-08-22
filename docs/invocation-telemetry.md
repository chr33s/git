# Git-Native Invocation Telemetry

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Spec revision:** draft-4

## 1. Summary

This specification defines durable runtime provenance for coding-agent invocations.

[Context Packs](context-pack.md) answer:

> **What Git-grounded repository evidence and semantically framed ContextRender were associated with an invocation?**

Invocation Telemetry answers:

> **Under what observable or explicitly reported runtime conditions did that invocation and its resulting operations occur?**

The preferred runtime capture interface is harness-native **OpenTelemetry (OTel)** using the OpenTelemetry GenAI semantic conventions when the harness implements them.

The architecture is:

```text
harness / model runtime
        │
        │ OTel GenAI spans / events / logs when available
        │ hooks or embedded capture as fallback
        ↓
loss-intolerant Git+ audit ingest
        │
        │ interpret upstream semantics
        │ normalize stable Git+ concepts
        ↓
refs/hub/trace/<session-id>
        │
        └── signed Git-native audit provenance

ordinary OTel export may independently flow to
observability backends with normal sampling/filtering
```

OTel is the preferred **capture, semantic, and correlation input**. It is not the durable Git+ protocol. Context Packs, ContextRender commitments, Git reachability, signatures, and immutable trace-record identity remain Git-native.

The goal is an **agent flight recorder**, not an inference-attestation system. Nothing here proves attention, understanding, memory, reasoning, or causation.

---

## 2. Session index versus audit trace

The existing session namespace remains the distilled policy-visible record:

```text
refs/hub/session/<session-id>
```

Detailed runtime provenance belongs in a sibling namespace:

```text
refs/hub/trace/<session-id>
```

The trace is:

- signed;
- append-only or DAG-preserving under concurrent writers;
- bound to the same repository and session identity;
- independently replicated according to audit-retention policy;
- **not consulted for authorization, membership, mergeability, protected-branch policy, or `requireProvenance` checks**.

High-frequency observability MUST NOT become policy-path cost.

Existing aggregate fields such as `session.produced.usage` MAY remain in the session DAG as small convenience summaries.

### 2.1 Immutable trace identity

Each canonical trace record is carried by a signed Git record commit. Later records MUST reference earlier canonical records by qualified Git commit OID:

```text
sha1:<hex>
sha256:<hex>
```

OTel `TraceId` and `SpanId` are correlation identifiers, not durable Git+ record identity.

If low-value events are batched, a batch entry MUST have an immutable locator such as:

```json
{
  "record": "sha1:abc123...",
  "index": 7
}
```

Context Exposure and logical Invocation Telemetry SHOULD normally remain standalone records so their commit OIDs are directly referenceable.

---

## 3. OpenTelemetry GenAI semantic-convention adherence

### 3.1 Interpretation rule

When an incoming OTel signal claims to implement the OpenTelemetry GenAI semantic conventions, Git+ MUST interpret that signal according to the declared convention version or revision.

Git+ MAY normalize the representation, but MUST NOT change the upstream semantic meaning.

In particular:

> **Upstream semantic conventions define what an incoming OTel signal means; Git+ defines how selected audit facts are durably represented in Git.**

A producer SHOULD preserve enough version metadata to identify the mapping used, for example:

```json
{
  "capture": {
    "transport": "otel",
    "semconv": {
      "profile": "open-telemetry/semantic-conventions-genai",
      "version": "development",
      "revision": "55a32cddb97d99cec08d5ee081e74206a0636041"
    }
  }
}
```

A released version or other stable upstream identifier is preferable when available.

If no convention version is declared, Git+ MAY use a documented best-effort mapping, but MUST NOT claim strict semconv adherence for that signal.

### 3.2 Stable Git+ schema remains independent

The GenAI conventions are currently evolving. Git+ therefore normalizes them into stable audit concepts:

```text
OTel GenAI semconv vN
provider-specific OTel attributes
custom harness instrumentation
        ↓
versioned normalization mapping
        ↓
stable Git+ trace schema
```

A historical Git+ record MUST remain interpretable without replaying the original OTel pipeline or loading the exact historical semantic-convention package.

Original namespaced attributes MAY be retained as diagnostic metadata, subject to security and retention policy.

### 3.3 Correlation fields

A normalized record MAY preserve:

```json
{
  "capture": {
    "transport": "otel",
    "stage": "sdk-export",
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7",
    "scope": "example.agent-runtime"
  }
}
```

These fields allow correlation with external OTel systems. They MUST NOT replace:

- signed Git record identity;
- repository/session binding;
- Context Pack verification;
- ContextRender commitments;
- Git reachability.

### 3.4 Hooks remain a fallback

A harness without suitable OTel MAY use vendor hooks or embedded capture.

```json
{
  "capture": {
    "transport": "hook",
    "integration": "vendor-hooks-v2"
  }
}
```

Fallback adapters SHOULD normalize into the same Git+ concepts where the observed facts have equivalent meaning. They MUST NOT pretend to be OTel-semconv compliant unless they actually emit/interpret that contract.

---

## 4. Loss-intolerant audit ingest

Ordinary observability pipelines may sample, filter, redact, aggregate, transform, or drop telemetry. Those behaviors are useful operationally but weaken an audit trail.

A Git+ deployment claiming durable per-invocation audit SHOULD therefore use a dedicated audit branch:

```text
                    ┌── Git+ audit ingest
harness → OTel SDK ─┤   no intentional lossy sampling/filtering
                    │
                    └── normal observability export
                        sampling/filtering/aggregation allowed
```

### 4.1 Preferred ingestion point

Prefer ingestion before arbitrary Collector processors can mutate or remove audit-relevant fields.

Record the capture stage when known:

```text
sdk-export
local-collector
remote-collector
hook
embedded
other
```

If the audit path is known to be sampled, filtered, or transformed before Git+ ingestion, the affected trace classes MUST NOT be presented as complete.

### 4.2 Trace health

Where detectable, record exporter/receiver health:

```json
{
  "type": "trace-health",
  "source": "otel",
  "stage": "local-collector",
  "sampling": "none",
  "transformed": false,
  "dropped": 0
}
```

These values are capture-system claims, not cryptographic proof that external infrastructure behaved correctly.

### 4.3 No Baggage authority

OTel Baggage MUST NOT be used as an authority or integrity channel for:

- repository identity;
- member identity;
- instruction authority;
- capabilities;
- Context Pack identity;
- authorization decisions.

Baggage MAY be copied into descriptive metadata after validation, but Git+ trust decisions derive from Git-native signed records and repository policy.

---

## 5. Evidence classes

Runtime values have three evidence classes:

```text
observed
  measured directly by the harness/Git+ capture boundary

reported
  supplied by a provider, tool, model runtime, or upstream instrumentation

derived
  computed from observed or reported values
```

OTel transport does not upgrade evidence class.

For example, a provider token count carried by an OTel span remains **provider-reported**.

A consumer MUST NOT silently promote `reported` or `derived` values to `observed` facts.

---

## 6. GenAI signal mapping

### 6.1 Logical inference spans

A GenAI inference span represents one **logical inference operation** as defined by the upstream semantic convention.

Git+ SHOULD map one compliant inference span to one logical `invocation-telemetry` record.

Core mapping:

```text
gen_ai.operation.name                  → operation.name
gen_ai.provider.name                   → model.provider
gen_ai.request.model                   → model.requested
gen_ai.response.model                  → model.response
gen_ai.response.finish_reasons         → response.finishReasons
error.type                             → outcome.errorType
span status                            → outcome.status
gen_ai.conversation.compacted          → context.compacted
```

OTel span kind, timestamps, TraceId, SpanId, instrumentation scope, and other standard trace metadata MAY be retained as capture metadata.

### 6.2 Usage mapping

When present:

```text
gen_ai.usage.input_tokens              → usage.inputTokens
gen_ai.usage.output_tokens             → usage.outputTokens
gen_ai.usage.cache_read.input_tokens   → usage.cacheReadInputTokens
gen_ai.usage.cache_write.input_tokens  → usage.cacheWriteInputTokens
gen_ai.usage.reasoning.output_tokens   → usage.reasoningOutputTokens
```

Modality-specific token fields MAY be retained under a namespaced or structured extension when useful.

Provider-generated usage remains `reported` / `source: provider` even when transported through OTel.

### 6.3 Agent spans

When the harness emits GenAI agent/framework spans, Git+ SHOULD preserve applicable stable meaning such as:

```text
gen_ai.operation.name        → operation.name
gen_ai.agent.id              → agent.id
gen_ai.agent.name            → agent.name
gen_ai.agent.version         → agent.version
gen_ai.conversation.id       → conversation.externalId
```

Operations such as `invoke_agent`, `invoke_workflow`, and `plan` MAY become separate audit records when useful.

`gen_ai.conversation.id` is external correlation metadata. It MUST NOT replace the authoritative Git+ session identity.

### 6.4 Execute-tool spans

A semconv execute-tool span uses:

```text
gen_ai.operation.name = execute_tool
gen_ai.tool.name
gen_ai.tool.call.id
gen_ai.tool.type
gen_ai.tool.description
```

Git+ SHOULD map these into compact tool telemetry:

```text
gen_ai.tool.name          → tool.name
gen_ai.tool.call.id       → tool.callId
gen_ai.tool.type          → tool.type
gen_ai.tool.description   → tool.description
```

Tool-call arguments and results are opt-in upstream and often sensitive. Git+ SHOULD NOT persist their raw bodies canonically by default.

### 6.5 Retrieval spans

A GenAI retrieval span (`gen_ai.operation.name = retrieval`) MAY be retained as selector/retrieval diagnostics.

It MUST NOT be treated as a Context Pack or as proof that retrieved documents crossed the model invocation boundary.

Context Pack and Context Exposure verification remain authoritative for repository-context exposure.

### 6.6 Events and logs

OTel Events or correlated LogRecords MAY carry inference details or harness lifecycle changes.

Git+ MAY normalize relevant point-in-time facts such as:

```text
context compaction
context truncation
workspace transition
known retry/fallback detail
trace/export health
```

Raw prompt/input/output events SHOULD remain opt-in and normally non-canonical because they are transcript-like and may contain secrets or source.

### 6.7 Metrics

OTel metrics are useful for fleet-level dashboards such as latency, token rates, cache behavior, or tool-failure rates.

Metrics MUST NOT be the sole source for reconstructing one historical invocation because aggregation loses event identity and causal detail.

---

## 7. Invocation Telemetry record

A normalized logical invocation record may be:

```json
{
  "type": "invocation-telemetry",
  "exposure": "sha1:abc123...",
  "capture": {
    "transport": "otel",
    "stage": "sdk-export",
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7",
    "semconv": {
      "profile": "open-telemetry/semantic-conventions-genai",
      "revision": "55a32cddb97d99cec08d5ee081e74206a0636041"
    }
  },
  "operation": {
    "name": "chat"
  },
  "model": {
    "provider": "anthropic",
    "requested": "model-x",
    "response": "model-x-20260815"
  },
  "usage": {
    "source": "provider",
    "inputTokens": 118420,
    "outputTokens": 4281,
    "cacheReadInputTokens": 90210,
    "cacheWriteInputTokens": 1200
  },
  "outcome": {
    "status": "ok"
  },
  "response": {
    "finishReasons": ["stop"]
  },
  "context": {
    "renderBytes": 483921,
    "compacted": false,
    "contextWindowTokens": 200000,
    "contextWindowSource": "provider"
  }
}
```

`exposure` is the qualified Git commit OID of the prior Context Exposure trace record.

### 7.1 Requested versus response model

`model.requested` and `model.response` are distinct because the model requested by the caller may differ from the model identifier reported in the response.

Neither is proof of stable underlying model weights or infrastructure.

If only one side is known, omit the other rather than copying values across the distinction.

### 7.2 Usage

Token values are non-negative integers when present.

`usage.source` SHOULD distinguish at least:

```text
provider
estimated
```

Estimated usage SHOULD include an estimator identifier and MUST NOT be presented as provider-reported.

Whole-invocation `inputTokens` MUST NOT be described as the token size of repository ContextRender alone.

### 7.3 Outcome versus finish reasons

Operation outcome and generation finish reason are separate concepts.

```text
outcome.status
  unset | ok | error

outcome.errorType
  timeout, provider/library error class, status code, or other low-cardinality error identifier

response.finishReasons
  provider/model generation stop reasons such as stop, length, content filtering, etc.
```

A successful request that ends because of a token limit is not automatically an error.

Git+ MUST NOT collapse `gen_ai.response.finish_reasons`, OTel span status, and `error.type` into one field.

### 7.4 ContextRender size and window semantics

`context.renderBytes` is the harness-observed body-byte total across ContextRender segments. It is tokenizer-independent.

Two limits remain distinct Git+ extensions:

```text
contextWindowTokens
  total sequence/context capacity believed to apply

effectiveInputLimitTokens
  maximum input token budget believed usable for this invocation
  after reserved output or other harness/provider constraints
```

Each SHOULD carry a source:

```text
provider
model-catalog
harness-config
runtime
```

Derived ratios have different meanings:

```text
inputTokens / contextWindowTokens
  share of total context window occupied by reported input

inputTokens / effectiveInputLimitTokens
  input pressure, only when a compatible effective input ceiling is known
```

If a limit is unknown or ambiguous, omit it rather than guessing.

---

## 8. Retries, attempts, and fallbacks

### 8.1 Logical operation rule

The upstream GenAI inference convention defines a span as a logical operation. Automatic retries for a transient issue may therefore be covered by the same inference span.

Git+ MUST NOT split one compliant logical inference span into multiple logical `invocation-telemetry` records merely because the underlying client may have retried.

```text
one GenAI inference span
        ↓
one Git+ logical invocation
```

### 8.2 Provider-attempt detail

If the harness/provider exposes attempt-level detail separately, Git+ MAY retain it as subordinate metadata or child audit records.

For example, after the logical invocation completes:

```json
{
  "attempts": [
    { "number": 1, "outcome": "error", "errorType": "timeout" },
    { "number": 2, "outcome": "ok" }
  ]
}
```

Attempt information is optional and MUST NOT be inferred from span duration, missing responses, or timestamp proximity.

### 8.3 Fallbacks

If a fallback is represented upstream as a distinct inference span, it becomes a distinct logical invocation record.

If upstream instrumentation represents fallback behavior inside one logical span, Git+ preserves that logical boundary and MAY retain observed fallback/attempt detail underneath it.

The persisted Git+ model MUST follow the semantics of the source signal rather than inventing new span boundaries.

---

## 9. Context lifecycle

### 9.1 Compacted conversation indicator

When a semconv inference span reports:

```text
gen_ai.conversation.compacted = true
```

Git+ SHOULD normalize:

```json
{
  "context": {
    "compacted": true
  }
}
```

This means the effective conversation context used for that logical operation was a compacted view of prior conversation state.

It does not identify when, why, or how compaction happened.

### 9.2 Rich compaction events

When the harness separately observes an actual compaction transition, Git+ MAY record:

```json
{
  "type": "context-compaction",
  "invocation": "sha1:logical-invocation-record...",
  "strategy": "summary",
  "reason": "context-window"
}
```

Recommended strategies:

```text
summary
dedupe
replace
provider-managed
other
```

A separate event MUST NOT be fabricated solely because `gen_ai.conversation.compacted` was true.

### 9.3 Truncation

When directly observable:

```json
{
  "type": "context-truncation",
  "reason": "context-window",
  "dropped": {
    "repositoryItems": 4,
    "toolResults": 7
  }
}
```

Dropped counts are diagnostics. Exact repository exposure comes from Context Packs and ContextRender records.

Lifecycle records describe what the harness/runtime did. They MUST NOT be phrased as claims that a model forgot, ignored, understood, or remembered something.

---

## 10. Tool telemetry

A canonical tool record MAY retain compact facts:

```json
{
  "type": "tool-telemetry",
  "invocation": "sha1:logical-invocation-record...",
  "capture": {
    "transport": "otel",
    "traceId": "...",
    "spanId": "..."
  },
  "operation": "execute_tool",
  "tool": {
    "name": "read-file",
    "callId": "call_123",
    "type": "function"
  },
  "outcome": {
    "status": "ok"
  },
  "result": {
    "bytes": 8421,
    "truncated": false,
    "digest": "sha256:..."
  }
}
```

The canonical record SHOULD contain metadata and digests, not raw arguments/results.

Repetitive successful tool spans MAY be batched or summarized. Raw bodies MAY remain disposable side objects or only in the external OTel backend.

A successful tool call does not prove its result entered a later model invocation.

---

## 11. Workspace transitions

When a repository-mutating operation changes the effective tree, Git+ MAY record:

```json
{
  "type": "workspace-transition",
  "operation": "sha1:tool-record...",
  "beforeTree": "sha256:aaa...",
  "afterTree": "sha256:bbb..."
}
```

Both OIDs SHOULD remain reachable for the intended audit-retention period.

Capture at meaningful boundaries rather than every filesystem syscall:

- after an agent edit tool completes;
- before the next repository-affecting model invocation;
- before commit creation;
- after checkout, merge, rebase, or similar Git state changes.

The next Context Pack `view.tree` remains authoritative for retrieval.

---

## 12. Context Packs remain Git-native

Context Packs and ContextRender MUST NOT be reduced to ordinary GenAI span attributes.

The harness/Git+ integration directly creates:

```text
Repository View
Context Pack
ContextRender
Context Exposure record
```

because those require:

- Git object verification;
- blob/gitlink resolution;
- semantic placement framing;
- real Git reachability to `view.tree`;
- durable render commitments.

OTel trace/span identity MAY be attached to a Context Exposure as descriptive correlation metadata when available.

Opt-in upstream payload attributes such as `gen_ai.input.messages`, `gen_ai.output.messages`, and `gen_ai.system_instructions` do not substitute for ContextRender verification.

---

## 13. Capture capability versus actual coverage

Distinguish:

```text
capture capability
  which runtime classes the instrumentation can observe

trace coverage
  what audit records actually arrived and whether known loss/transformation occurred
```

A declaration MAY say:

```json
{
  "visibility": {
    "integration": "otel-genai",
    "stage": "sdk-export",
    "capabilities": ["inference", "agent", "tools", "lifecycle", "workspace"]
  }
}
```

This is not proof every signal was captured.

A projection may classify each class as:

```text
available
partial
unknown
```

`complete` SHOULD be used only when the path has a defined completeness mechanism, no intentional sampling/filtering for that class, and no known loss.

> **No compaction record exists** is not equivalent to **the harness proves no compaction occurred**.

---

## 14. Security and retention

Provider envelopes, raw tool bodies, prompts, complete input/output messages, and transcript-like payloads SHOULD NOT become canonical trace records by default.

OTel attributes may contain source, paths, prompts, user identifiers, or secrets. The audit ingester MUST apply repository secret-handling, access, and retention policy before persistence.

The normal observability branch MAY apply independent redaction, aggregation, sampling, or retention.

The audit trace remains policy-invisible: losing optional trace detail may reduce auditability but MUST NOT retroactively change authorization or merge validity.

---

## 15. Product surface

Useful read surfaces include:

```text
git+ session show <session>
git+ trace show <session>
git+ context audit <operation-or-trace-record>
```

A product may correlate:

```text
repository view/tree
Context Pack and ContextRender
OTel TraceId / SpanId
semconv profile/version
logical operation name
agent identity when reported
requested and response model
input/output/cache/reasoning tokens
operation status / error.type
response finish reasons
context compacted indicator
window/effective input limits
provider attempts when explicitly observed
tool diagnostics
workspace transitions
resulting commits/refs
capture capability / actual coverage
```

Derived values MUST be labeled derived.

---

## 16. Recommended V1 profile

Start with:

```text
1. OTel GenAI ingestion when the harness provides it
2. versioned semconv interpretation at ingest
3. hooks/embedded capture only as fallback
4. a dedicated loss-intolerant audit export branch
5. one Git+ logical invocation per semconv inference span
6. provider/request/response model mapping
7. provider-reported input/output/cache usage
8. separate outcome/error and finish-reason fields
9. gen_ai.conversation.compacted ingestion
10. execute_tool span mapping
11. Context Exposure records in refs/hub/trace/<session>
12. contextWindowTokens/effectiveInputLimitTokens as Git+ extensions
13. optional attempt/fallback detail only when explicitly observable
14. beforeTree → afterTree at meaningful repository boundaries
15. capture capability plus detectable trace-loss/transformation markers
```

---

## 17. OpenTelemetry compatibility guidance

Primary upstream source:

```text
https://github.com/open-telemetry/semantic-conventions-genai
```

Relevant upstream concepts include:

- GenAI inference spans;
- GenAI agent/framework spans;
- execute-tool spans;
- retrieval spans;
- GenAI events;
- GenAI metrics;
- provider-specific conventions.

The upstream GenAI conventions are currently Development-status and may evolve independently of Git+.

Implementations SHOULD pin/report the convention revision they understand and SHOULD test the normalizer against upstream reference scenarios where practical.

Git+ SHOULD follow upstream semantic meaning while avoiding upstream field-name churn in the durable protocol.

---

## Final invariant

> **OpenTelemetry GenAI semantic conventions define the meaning of compliant incoming runtime signals; Git+ preserves that meaning while normalizing selected audit facts into signed, policy-invisible Git records. One compliant inference span maps to one logical invocation, automatic retries remain subordinate to that logical operation, requested and response models stay distinct, finish reasons stay separate from operation errors, and standard agent/tool semantics are preserved. Git-native Context Packs, ContextRender commitments, session identity, reachability, and trust remain outside and stronger than OTel correlation.**
