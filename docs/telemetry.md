# Git-Native Telemetry

**Status:** Draft specification and architecture note  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Revision:** draft-5

## 1. Summary

This document defines the Git+ runtime telemetry model, its OpenTelemetry ingestion contract, its durable Git representation, and the harness/API/UI integration used to surface that provenance.

[Context Packs](context-pack.md) answer:

> **What Git-grounded repository evidence and semantically framed ContextRender were associated with an invocation?**

Telemetry answers:

> **Under what observable or explicitly reported runtime conditions did that invocation and its resulting operations occur?**

The preferred runtime interface is harness-native **OpenTelemetry (OTel)** using the OpenTelemetry GenAI semantic conventions when the harness implements them.

The architecture is:

```text
Repository View / Context Pack / ContextRender
        │
        │ exact Git-native repository provenance
        ↓
Context Exposure
        │
        ├──────────────────────────────┐
        │                              │
        ↓                              │
logical model / agent operation        │
        │                              │
        │ OTel GenAI spans/events/logs │
        ↓                              │
loss-intolerant Git+ audit ingest ◀────┘
        │
        │ interpret upstream semantics
        │ normalize stable Git+ concepts
        │ sign + retain selected audit facts
        ↓
refs/hub/trace/<session-id>
        │
        ↓
hub projection / Flight Recorder
```

Ordinary OTel export may independently flow to normal observability backends with sampling, filtering, aggregation, transformation, and shorter retention.

OTel is the preferred **runtime capture, semantic, and correlation input**. It is not the durable Git+ protocol. Context Packs, ContextRender commitments, Git reachability, signatures, immutable trace-record identity, repository/session trust, and policy remain Git-native.

The goal is an **agent flight recorder**, not an inference-attestation system. Nothing here proves attention, understanding, memory, reasoning, or causation.

---

## 2. Scope and layering

The system intentionally separates four concerns:

```text
Repository provenance
  Repository View
  Context Pack

Exposure provenance
  ContextRender
  Context Exposure

Runtime provenance
  OTel GenAI runtime signals
  Git+ telemetry trace

Product projection
  session summaries
  Flight Recorder
  commit / Change Request provenance
```

Runtime telemetry MUST NOT affect Context Pack identity or the verification of Git-grounded evidence.

Weak or missing runtime telemetry MUST NOT invalidate otherwise valid repository provenance. Conversely, valid Git evidence MUST NOT imply that runtime telemetry is complete.

---

## 3. Session index versus audit trace

The existing session namespace remains the distilled, policy-visible record:

```text
refs/hub/session/<session-id>
```

It may contain small durable facts such as:

```text
prompt / task
produced result
commit / CR relationship
decisions
compact learning note
aggregate usage when available
```

Detailed runtime provenance belongs in a sibling namespace:

```text
refs/hub/trace/<session-id>
```

The trace may contain:

```text
Context Exposure
logical Invocation Telemetry
selected agent operations
selected tool operations
context lifecycle events
workspace transitions
trace-health / known-loss records
```

The trace is:

- signed;
- append-only or DAG-preserving under concurrent writers;
- bound to the same repository and session identity;
- independently replicated according to audit-retention policy;
- **not consulted for authorization, membership, mergeability, protected-branch policy, or `requireProvenance` checks**.

High-frequency observability MUST NOT become policy-path cost.

### 3.1 Immutable trace identity

Each canonical trace record is carried by a signed Git record commit. Later canonical records MUST reference earlier canonical records by qualified Git commit OID:

```text
sha1:<hex>
sha256:<hex>
```

OTel `TraceId` and `SpanId` are correlation identifiers, not durable Git+ record identity.

If low-value events are batched, an entry MUST have an immutable locator such as:

```json
{
  "record": "sha1:abc123...",
  "index": 7
}
```

Context Exposure and logical Invocation Telemetry SHOULD normally remain standalone records so their commit OIDs are directly referenceable.

---

## 4. OpenTelemetry GenAI semantic-convention adherence

### 4.1 Interpretation rule

When an incoming OTel signal claims to implement the OpenTelemetry GenAI semantic conventions, Git+ MUST interpret the signal according to the declared convention version or revision.

Git+ MAY normalize the representation, but MUST NOT change the upstream semantic meaning.

> **Upstream semantic conventions define what an incoming OTel signal means; Git+ defines how selected audit facts are durably represented in Git.**

A producer SHOULD preserve enough version metadata to identify the mapping used:

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

### 4.2 Stable Git+ schema remains independent

The GenAI conventions evolve independently of Git+. Therefore:

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

### 4.3 Correlation fields

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

### 4.4 Hooks remain a fallback

A harness without suitable OTel MAY use vendor hooks or embedded capture:

```json
{
  "capture": {
    "transport": "hook",
    "integration": "vendor-hooks-v2"
  }
}
```

Fallback adapters SHOULD normalize into the same Git+ concepts where observed facts have equivalent meaning. They MUST NOT claim OTel GenAI semconv adherence unless that contract is actually implemented.

---

## 5. Loss-intolerant audit ingest

Ordinary observability pipelines may sample, filter, redact, aggregate, transform, or drop telemetry. Those behaviors are useful operationally but weaken an audit trail.

A Git+ deployment claiming durable per-invocation audit SHOULD therefore use a dedicated audit branch:

```text
                    ┌── Git+ audit ingest
harness → OTel SDK ─┤   no intentional lossy sampling/filtering
                    │
                    └── normal observability export
                        sampling/filtering/aggregation allowed
```

### 5.1 Preferred ingestion point

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

If the audit path is known to be sampled, filtered, or transformed before Git+ ingestion, affected trace classes MUST NOT be presented as complete.

### 5.2 Trace health

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

Useful known-loss signals include:

```text
export queue overflow
batch dropped
receiver unavailable
collector sampling enabled
processor filtered audit class
```

These values are capture-system claims, not cryptographic proof that external infrastructure behaved correctly.

Absence of an event is meaningful only in the context of known capture capability and trace health.

### 5.3 No Baggage authority

OTel Baggage MUST NOT be used as an authority or integrity channel for:

- repository identity;
- member identity;
- instruction authority;
- capabilities;
- Context Pack identity;
- authorization decisions.

Baggage MAY be copied into descriptive metadata after validation. Git+ trust decisions derive from Git-native signed records and repository policy.

---

## 6. Evidence classes

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

Examples:

```text
ContextRender body byte length       observed
harness wall-clock duration          observed
provider token count via OTel        reported
provider response model via OTel     reported
context utilization ratio            derived
estimated dollar cost                derived
```

A consumer MUST NOT silently promote `reported` or `derived` values to `observed` facts.

---

## 7. GenAI signal mapping

### 7.1 Logical inference spans

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

### 7.2 Usage mapping

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

### 7.3 Agent spans

When the harness emits GenAI agent/framework spans, Git+ SHOULD preserve applicable stable meaning:

```text
gen_ai.operation.name        → operation.name
gen_ai.agent.id              → agent.id
gen_ai.agent.name            → agent.name
gen_ai.agent.version         → agent.version
gen_ai.conversation.id       → conversation.externalId
```

Operations such as `invoke_agent`, `invoke_workflow`, and `plan` MAY become separate audit records when useful.

`gen_ai.conversation.id` is external correlation metadata. It MUST NOT replace authoritative Git+ session identity.

### 7.4 Execute-tool spans

For `gen_ai.operation.name = execute_tool`, useful fields include:

```text
gen_ai.tool.name
gen_ai.tool.call.id
gen_ai.tool.type
gen_ai.tool.description
```

Git+ SHOULD map them as:

```text
gen_ai.tool.name          → tool.name
gen_ai.tool.call.id       → tool.callId
gen_ai.tool.type          → tool.type
gen_ai.tool.description   → tool.description
```

Tool-call arguments and results are opt-in upstream and frequently sensitive. Git+ SHOULD NOT persist raw arguments/results canonically by default.

### 7.5 Retrieval spans

A GenAI retrieval span (`gen_ai.operation.name = retrieval`) MAY be retained as selector/retrieval diagnostics.

It MUST NOT be treated as a Context Pack or proof that retrieved documents crossed the model invocation boundary.

Context Pack and Context Exposure verification remain authoritative for repository-context exposure.

### 7.6 Events and logs

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

### 7.7 Metrics

OTel metrics are useful for fleet-level dashboards such as latency, token rates, cache behavior, or tool-failure rates.

Metrics MUST NOT be the sole source for reconstructing one historical invocation because aggregation loses event identity and causal detail.

---

## 8. Invocation Telemetry record

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

### 8.1 Requested versus response model

`model.requested` and `model.response` are distinct because the model requested by the caller may differ from the model identifier reported in the response.

Neither proves stable underlying model weights or infrastructure.

If only one side is known, omit the other rather than copying values across the distinction.

### 8.2 Usage

Token values are non-negative integers when present.

`usage.source` SHOULD distinguish at least:

```text
provider
estimated
```

Estimated usage SHOULD include an estimator identifier and MUST NOT be presented as provider-reported.

Whole-invocation `inputTokens` MUST NOT be described as the token size of repository ContextRender alone.

### 8.3 Outcome versus finish reasons

Operation outcome and generation finish reason are separate concepts:

```text
outcome.status
  unset | ok | error

outcome.errorType
  timeout, provider/library error class, status code, or another low-cardinality error identifier

response.finishReasons
  provider/model generation stop reasons such as stop, length, content filtering, etc.
```

A successful request that ends because of a token limit is not automatically an error.

Git+ MUST NOT collapse `gen_ai.response.finish_reasons`, OTel span status, and `error.type` into one field.

### 8.4 ContextRender size and window semantics

`context.renderBytes` is the harness-observed body-byte total across ContextRender segments. It is tokenizer-independent.

Two limits are distinct Git+ extensions:

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

## 9. Retries, attempts, and fallbacks

### 9.1 Logical operation rule

The upstream GenAI inference convention defines a span as a logical operation. Automatic retries for a transient issue may therefore be covered by the same inference span.

Git+ MUST NOT split one compliant logical inference span into multiple logical `invocation-telemetry` records merely because the underlying client may have retried.

```text
one GenAI inference span
        ↓
one Git+ logical invocation
```

### 9.2 Provider-attempt detail

If the harness/provider exposes attempt-level detail separately, Git+ MAY retain it as subordinate metadata or child audit records:

```json
{
  "attempts": [
    { "number": 1, "outcome": "error", "errorType": "timeout" },
    { "number": 2, "outcome": "ok" }
  ]
}
```

Attempt information is optional and MUST NOT be inferred from span duration, missing responses, timestamp gaps, or proximity.

### 9.3 Fallbacks

If fallback is represented upstream as a distinct inference span, it becomes a distinct logical invocation record.

If upstream instrumentation represents fallback behavior inside one logical span, Git+ preserves that logical boundary and MAY retain observed fallback/attempt detail underneath it.

The persisted Git+ model MUST follow source-signal semantics rather than inventing new span boundaries.

---

## 10. Context lifecycle

### 10.1 Compacted conversation indicator

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

This means the effective conversation context used for the logical operation was a compacted view of prior conversation state.

It does not identify when, why, or how compaction happened.

### 10.2 Rich compaction events

When the harness separately observes an actual compaction transition, Git+ MAY record:

```json
{
  "type": "context-compaction",
  "invocation": "sha1:logical-invocation-record...",
  "strategy": "summary",
  "reason": "context-window"
}
```

Recommended strategies include:

```text
summary
dedupe
replace
provider-managed
other
```

A separate event MUST NOT be fabricated solely because `gen_ai.conversation.compacted` was true.

### 10.3 Truncation

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

## 11. Tool telemetry

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

A successful tool call does not prove its result entered a later model invocation. Repository evidence that crosses that later invocation boundary should appear in its Context Pack where representable.

---

## 12. Workspace transitions

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
- before the next repository-affecting logical invocation;
- before commit creation;
- after checkout, merge, rebase, or similar Git state changes.

The next Context Pack `view.tree` remains authoritative for retrieval.

A practical implementation may capture lazily:

```text
edit starts
  remember tree A

writes happen
  mark workspace dirty

before next auditable logical invocation
  materialize tree B
  record A → B
  next Context Pack uses B
```

An OTel tool span can identify the operation that caused mutation, but Git tree OIDs remain authoritative repository identities.

---

## 13. Context Packs remain Git-native

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
- ContextRender semantic placement framing;
- real Git reachability to `view.tree`;
- durable render commitments.

OTel trace/span identity MAY be attached to a Context Exposure as descriptive correlation metadata:

```json
{
  "type": "context-exposure",
  "pack": "sha1:...",
  "renderFormat": "git+context-render/v1",
  "renderDigest": "sha256:...",
  "capture": {
    "transport": "otel",
    "traceId": "...",
    "spanId": "..."
  }
}
```

Opt-in upstream payload attributes such as `gen_ai.input.messages`, `gen_ai.output.messages`, and `gen_ai.system_instructions` do not substitute for ContextRender verification.

---

## 14. Capture capability versus actual coverage

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

## 15. Security and retention

Provider envelopes, raw tool bodies, prompts, complete input/output messages, and transcript-like payloads SHOULD NOT become canonical trace records by default.

OTel attributes may contain source, paths, prompts, user identifiers, or secrets. The audit ingester MUST apply repository secret-handling, access, and retention policy before persistence.

The normal observability branch MAY apply independent redaction, aggregation, sampling, or retention.

The audit trace remains policy-invisible: losing optional trace detail may reduce auditability but MUST NOT retroactively change authorization or merge validity.

> **The Git trace is the audit index, not a second telemetry backend.**

---

# Part II — Harness, API, and UI integration

## 16. Integration modes

### 16.1 Preferred: harness-native OTel GenAI

If the harness/runtime emits OpenTelemetry GenAI semantic conventions, Git+ SHOULD consume those signals directly instead of reconstructing runtime activity from vendor hooks.

Useful standard operations include:

```text
inference spans
  chat / generate_content / text_completion / other inference

agent/framework spans
  invoke_agent
  invoke_workflow
  plan

tool spans
  execute_tool

retrieval spans
  retrieval
```

Standard GenAI attributes can supply provider, requested/response model, usage, finish reasons, agent/tool identity, conversation correlation, and compacted-conversation state.

### 16.2 Embedded Git+ capture

Git+ still needs a direct harness boundary for repository provenance:

```text
before logical inference
  materialize Repository View
  build Context Pack
  build semantically framed ContextRender
  append Context Exposure

logical inference span completes
  normalize OTel runtime facts
  append Invocation Telemetry
```

This direct integration is necessary because Context Packs provide guarantees beyond generic telemetry.

### 16.3 Fallback: vendor hooks

When native OTel is absent or incomplete:

```text
vendor hook
    ↓
.chr33s/telemetry.mjs
    ↓ normalize only observed fields
    ↓
git+ trace record
```

Vendor event names remain adapter details.

---

## 17. Machine-facing interfaces

### 17.1 OTel audit receiver/exporter

Preferred shape:

```text
harness OTel SDK
  → OTLP localhost / unix socket / embedded receiver
  → versioned GenAI semconv normalizer
  → Git+ trace writer
```

The exact transport binding is implementation-specific.

The receiver SHOULD obtain repository/session association from authenticated local configuration rather than trusting arbitrary incoming telemetry attributes to select another repository.

### 17.2 Fallback recorder

Keep a generic fallback primitive:

```text
git+ trace record <repo> \
  --session <session-id> \
  --key <private-key> \
  --event <event.json>
```

It should:

```text
validate
secret-scan / apply retention rules
bind repository + session
sign
append to refs/hub/trace/<session-id>
print resulting qualified record OID
```

The durable trace schema is the same after normalization regardless of whether input came from OTel, hooks, or embedded capture.

---

## 18. Trace volume and batching

OTel may produce far more detail than belongs in Git.

Recommended canonical Git records:

```text
Context Exposure
logical Invocation Telemetry
selected agent operations
workspace transitions when audit-relevant
context lifecycle changes when directly observed
tool failures/truncations/mutation summaries
trace-health / known-loss markers
```

Usually external-only or side telemetry:

```text
every successful read/grep
raw provider request/response
raw tool arguments/results
complete input/output messages
stream chunks
high-cardinality timing detail
fleet metrics
```

Low-value repetitive records MAY be batched or summarized as long as any retained entry has an immutable locator and the projection does not invent causal detail that was discarded.

---

## 19. Server projection

The browser SHOULD NOT reconstruct semconv meaning, trace DAGs, trust state, or causal bindings itself.

### 19.1 Session list

`/hub/sessions` should stay cheap and may include trace-derived summaries:

```json
{
  "session": "0198f2aa-...",
  "usage": {
    "inputTokens": 118420,
    "outputTokens": 4281
  },
  "telemetry": {
    "transport": "otel",
    "semconv": "genai",
    "logicalInvocations": 18,
    "providerAttempts": 20,
    "providerAttemptsKnown": true,
    "peakInputPressure": 0.96,
    "peakInvocation": "sha1:abc123...",
    "compactedInvocations": 2,
    "toolFailures": 1,
    "coverage": "partial"
  }
}
```

`providerAttempts` MUST be omitted when attempt-level instrumentation is unavailable. It MUST NOT be guessed from logical invocation count.

`peakInputPressure` exists only when a compatible effective input limit is known. The summary SHOULD retain the invocation record that produced the peak rather than pairing a session-wide maximum with an unrelated model/window.

### 19.2 Session detail

```text
GET /hub/sessions/:id
```

joins:

```text
small policy-visible session DAG
+
policy-invisible Git+ audit trace
+
optional external OTel links
```

The server owns:

- signature/trust verification;
- session/trace folding;
- GenAI semconv normalization;
- causal parentage;
- redaction state;
- Context Exposure / invocation joins;
- agent/tool/workspace joins;
- trace health and coverage;
- derived diagnostics.

The browser owns presentation.

---

## 20. UI principles

### 20.1 Lead with repository context

The useful story is:

```text
tests/auth.test.ts exposed
      ↓
logical inference
      ↓
96% effective input pressure
      ↓
compacted context reported/observed
      ↓
tests/auth.test.ts absent from next exposure
      ↓
workspace mutation
      ↓
commit
```

OTel plumbing should be available for experts without dominating the primary view.

### 20.2 Requested versus response model

Show one model label when both values match.

When they differ, expose the distinction:

```text
requested  model-x
response   model-x-20260815
```

Do not silently overwrite one with the other.

### 20.3 Finish reason versus error

Display:

```text
✓ completed · finish: length
```

separately from:

```text
✕ error · timeout
```

Normal generation termination must not be rendered as transport/provider failure.

### 20.4 Observable language only

Prefer:

> `tests/auth.test.ts` was present in Exposure A and absent after a recorded compaction before Exposure B.

Avoid:

> The model forgot the test.

Derived diagnostics SHOULD describe observed transitions and explicitly reported facts, not cognition.

---

## 21. Activity rows

A healthy session might show:

```text
0198f2aa  claude-code · model-x
3 commits · 18 invocations · 124k tokens · ✓ context
```

A session with useful warnings might show:

```text
0198f2bb  codex · model-y
18 invocations · 20 attempts · 96% input pressure · 2 compacted · ⚠ context
```

Only show attempts when explicitly known.

An optional detail indicator may show:

```text
OTel GenAI · audit capture available
```

Missing telemetry MUST NOT render as zero.

---

## 22. Flight Recorder

The primary detailed UI should be a session Flight Recorder rather than a raw telemetry dump.

Example:

```text
Session 0198f2aa
Claude Code
────────────────────────────────────────

09:42  Context Exposure
       tree 79ad…
       7 blobs · 1 gitlink
       placements: developer, tool
       ✓ evidence + render verified
       OTel span 00f067…

09:42  Inference · chat
       provider anthropic
       requested model-x
       response  model-x-20260815
       118k input · 90k cache-read · 4.2k output
       ✓ completed · finish: stop
       2 provider attempts reported
         1 timeout
         2 success

09:43  Tool · read-file
       call call_123
       ✓ 8.4 KB · result digest retained

09:45  Workspace
       tree 79ad… → a130…

09:46  Context
       compacted prior conversation
       separate compaction transition: summary · context-window

09:46  Context diff
       - tests/auth.test.ts

09:47  Inference · chat
       172k / 180k effective input limit
       ⚠ 96% input pressure

09:48  Commit
       abc123 Fix auth policy
```

### 22.1 External OTel deep link

If an observability backend is configured, rows MAY expose:

```text
Open trace
```

The external backend is supplemental. Git+ audit pages MUST remain useful after external telemetry expires, is deleted, or is sampled out.

### 22.2 Preserve concurrency

Session and trace histories may be DAGs. The UI SHOULD show branches/lanes for concurrent or resumed divergent histories rather than infer causality from timestamps.

---

## 23. Context Pack diffs

Adjacent exposures should support:

```text
Context A → B

Added
+ tests/auth.test.ts
+ src/policy.ts

Removed
- docs/design.md
```

Clicking a blob opens the exact historical blob/range. Clicking a gitlink shows the recorded submodule commit pointer.

OTel retrieval documents do not replace this Git-native historical evidence view.

This is one of the most useful diagnostics for context loss because it makes repository-context transitions inspectable without reconstructing raw prompts.

---

## 24. Provenance and capture status

Display evidence dimensions separately:

```text
Repository evidence   ✓ verified
View reachability     ✓ retained through Git trace record
Context render        ✓ retained and digest verified
Runtime semantics     OTel GenAI · declared revision
Runtime usage         provider reported
Runtime capture       partial
Collector path        local-collector · transformed
Provider attempts     unknown
Tool body             expired
Workspace alignment   ✓ matched recorded invocation boundary
```

Useful warnings include:

```text
⚠ audit OTel export reported drops
⚠ sampling/filtering exists before Git+ ingest
⚠ capture path transformed telemetry
⚠ semconv version unknown; best-effort mapping
⚠ attempt-level telemetry unavailable
✕ path/blob mismatch
✕ gitlink mode/commit mismatch
```

Historical workspace state SHOULD be judged against its recorded invocation boundary, not against the repository's current branch tip.

---

## 25. Audit summary

A derived summary card may show:

```text
Agent session audit

Context provenance      ✓ 18 / 18 verified
Runtime capture          OTel GenAI · partial
Audit exporter health    ✓ no known drops
Workspace alignment      ✓ matched invocation boundaries
Peak input pressure      ⚠ 96% · sha1:c3…
Compacted invocations    2
Provider attempts        unknown
Failed tools             1
Knowledge continuity     ⚠ test absent after compaction
```

Derived diagnosis MUST remain visibly derived and use observable language.

---

## 26. Commit and Change Request surfaces

Repository artifacts SHOULD link back to the Flight Recorder only when a causal binding exists.

A commit may show:

```text
Produced by session 0198f2aa
Causally bound logical invocation: sha1:c3…
chat · model-y · 96% input pressure
Context Pack: 12 blobs · 1 gitlink
OTel trace available
```

Do not infer a causal invocation merely from timestamp proximity.

---

## 27. Metrics stay operational

OTel metrics can power fleet-level views:

```text
tokens per day
latency distributions
cache hit ratios
tool failure rates
model/provider mix
```

They are not the canonical source for one session's audit history.

The Flight Recorder SHOULD use per-operation trace facts, not reconstructed metric aggregates.

---

## 28. Knowledge durability relationship

Repository Memory and durable knowledge remain independent of OTel transport.

A Memory entry may show cited claims and structured evidence dependencies:

```text
gotcha    Worker auth tests require production policy fixture
source    session 0198f2aa
support   2 blobs · 1 gitlink
state     ⚠ config/policy.json changed; revalidate
```

Telemetry helps diagnose whether recalled knowledge survived runtime context lifecycle. It does not turn a cited claim into truth.

---

## 29. Implementation sequence

### Phase 1 — existing session summary

Use current `session.produced.usage` in Activity. Do not invent detailed runtime semantics before detailed capture exists.

### Phase 2 — semconv-aware OTel audit ingest

Implement:

```text
OTLP audit receiver/exporter integration
versioned OpenTelemetry GenAI mapping
one logical invocation per inference span
refs/hub/trace/<session>
trace-health / capture-stage metadata
```

Start with inference spans.

### Phase 3 — model/usage/outcome mapping

Add explicit mappings for:

```text
provider
requested vs response model
input/output/cache/reasoning usage
span status + error.type
finish reasons
conversation.compacted
```

### Phase 4 — agent/tool/retrieval support

Add GenAI agent/framework spans, execute-tool spans, and retrieval diagnostics while preserving their upstream meanings.

### Phase 5 — Context Exposure integration

Add direct Git+ Repository View / Context Pack / ContextRender capture and correlate exposures with inference spans.

### Phase 6 — fallback hooks

Add `git+ trace record` and vendor adapters for harnesses without sufficient native OTel.

### Phase 7 — server projection and Flight Recorder

Add session-detail projections, context diffs, logical invocation/attempt hierarchy, pressure, tool groups, workspace transitions, capture health, and external OTel links.

---

## 30. Acceptance criteria

The telemetry architecture is successful when:

1. a harness emitting OTel GenAI conventions can feed Git+ without a vendor-specific runtime adapter;
2. Git+ interprets a declared semconv version according to upstream meaning;
3. one semconv inference span maps to one logical invocation;
4. automatic retries do not become invented logical spans;
5. provider attempts appear only when explicitly observed;
6. requested and response model identifiers remain distinct;
7. finish reasons remain distinct from span errors;
8. standard agent/tool attributes map predictably;
9. `gen_ai.conversation.compacted` is preserved without fabricating a compaction transition;
10. retrieval spans never substitute for Context Exposure evidence;
11. the audit path is independent from lossy observability sampling/filtering;
12. Git record OIDs remain canonical durable identity;
13. OTel Baggage is never treated as authority;
14. Context Packs/ContextRender remain directly Git-grounded;
15. high-volume OTel detail does not turn Git into a second telemetry backend;
16. high-frequency trace data remains outside policy-critical session folds;
17. exporter loss/sampling/transformation is visible when known;
18. workspace mutations remain grounded by Git trees, not telemetry strings;
19. the Flight Recorder remains useful after external OTel retention expires;
20. a reviewer can move from a commit or Change Request to repository evidence, runtime audit, and optionally the external OTel trace.

---

## 31. Upstream compatibility

Primary upstream specification:

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

Other useful OTel references include:

- Logs and trace correlation: `https://opentelemetry.io/docs/specs/otel/logs/`
- Log data model / TraceId / SpanId: `https://opentelemetry.io/docs/specs/otel/logs/data-model/`
- Collector transformations/filtering: `https://opentelemetry.io/docs/collector/transforming-telemetry/`
- Baggage security considerations: `https://opentelemetry.io/docs/concepts/signals/baggage/`

---

## Final invariant

> **OpenTelemetry GenAI semantic conventions define the meaning of compliant incoming runtime signals; Git+ preserves that meaning while normalizing selected audit facts into signed, policy-invisible Git records. One compliant inference span maps to one logical invocation, automatic retries remain subordinate to that logical operation, requested and response models stay distinct, finish reasons stay separate from operation errors, and standard agent/tool semantics are preserved. Git-native Context Packs, ContextRender commitments, session identity, reachability, and trust remain outside and stronger than OTel correlation. The product surfaces this as an auditable Flight Recorder focused on repository-context change rather than raw telemetry plumbing.**
