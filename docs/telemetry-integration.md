# Git-Native Telemetry Integration and UI

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-22  
**Revision:** draft-4

## 1. Summary

This document describes how [Invocation Telemetry](invocation-telemetry.md) and [Context Packs](context-pack.md) should be integrated with coding-agent harnesses and surfaced in the Git+ UI.

The preferred implementation is **OpenTelemetry GenAI semconv first**:

```text
agent harness / model runtime
        │
        │ OTel GenAI spans + events/logs
        ↓
       OTLP
        │
        ├──────────────→ normal observability pipeline
        │                 sampling / transforms allowed
        │
        ↓
Git+ audit ingester
        │
        │ interpret declared GenAI semconv
        │ normalize without changing meaning
        │ Git-ground + sign
        ↓
refs/hub/trace/<session-id>
        │
        ↓
hub projection
        │
        ↓
Activity · Commit/CR provenance · Flight Recorder
```

Harness hooks remain a compatibility fallback when native OTel does not expose the required runtime boundaries.

The protocol boundary is:

```text
Context Pack / ContextRender
  exact repository provenance

OTel GenAI semconv
  runtime operation semantics + correlation

Git+ trace
  durable normalized audit projection
```

The product goal is not another observability dashboard. It is to explain how repository context, runtime conditions, tools, mutations, agents, and model calls relate to the code that was produced.

---

## 2. Storage separation

The policy-critical session DAG stays small:

```text
refs/hub/session/<session-id>
  prompt / produced result / decisions / aggregate usage
  may be consulted by provenance or policy
```

Detailed audit provenance lives separately:

```text
refs/hub/trace/<session-id>
  Context Exposure
  logical Invocation Telemetry
  selected agent/tool operations
  context lifecycle
  workspace transitions
  trace health
```

The trace MUST NOT participate in authorization, membership, protected-branch policy, mergeability, or `requireProvenance` checks.

---

## 3. Integration modes

### 3.1 Preferred: harness-native OTel GenAI

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

Trace/span IDs provide correlation with external OTel systems.

### 3.2 Embedded Git+ capture

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

### 3.3 Fallback: vendor hooks

When native OTel is absent or incomplete:

```text
vendor hook
    ↓
.chr33s/telemetry.mjs
    ↓ normalize only observed fields
    ↓
git+ trace record
```

Vendor event names remain adapter details. Fallback adapters SHOULD mimic Git+ stable concepts where meanings are equivalent, but MUST NOT claim OTel GenAI semconv adherence unless that contract is actually implemented.

---

## 4. Semconv interpretation contract

The normalizer has two responsibilities:

```text
1. understand the declared upstream semantic convention
2. project it into stable Git+ fields without changing meaning
```

If a signal claims `open-telemetry/semantic-conventions-genai` compatibility, Git+ MUST interpret it according to the declared version/revision.

Example capture metadata:

```json
{
  "capture": {
    "transport": "otel",
    "stage": "sdk-export",
    "traceId": "...",
    "spanId": "...",
    "semconv": {
      "profile": "open-telemetry/semantic-conventions-genai",
      "revision": "55a32cddb97d99cec08d5ee081e74206a0636041"
    }
  }
}
```

The durable Git+ schema SHOULD NOT require old clients to understand historical upstream attribute names.

### 4.1 Core inference mapping

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

### 4.2 Usage mapping

```text
gen_ai.usage.input_tokens              → usage.inputTokens
gen_ai.usage.output_tokens             → usage.outputTokens
gen_ai.usage.cache_read.input_tokens   → usage.cacheReadInputTokens
gen_ai.usage.cache_write.input_tokens  → usage.cacheWriteInputTokens
gen_ai.usage.reasoning.output_tokens   → usage.reasoningOutputTokens
```

Provider-reported token values remain provider-reported after transport through OTel.

### 4.3 Agent mapping

```text
gen_ai.agent.id             → agent.id
gen_ai.agent.name           → agent.name
gen_ai.agent.version        → agent.version
gen_ai.conversation.id      → conversation.externalId
```

`conversation.externalId` is correlation metadata. It is not the Git+ session identity.

### 4.4 Tool mapping

For `gen_ai.operation.name = execute_tool`:

```text
gen_ai.tool.name          → tool.name
gen_ai.tool.call.id       → tool.callId
gen_ai.tool.type          → tool.type
gen_ai.tool.description   → tool.description
```

Raw tool arguments/results are opt-in upstream and SHOULD remain non-canonical by default.

### 4.5 Retrieval mapping

`gen_ai.operation.name = retrieval` may contribute selector/retrieval diagnostics.

A retrieval span is **not** evidence that a document entered model context. Only the Context Pack / Context Exposure layer establishes repository-context exposure.

---

## 5. Logical invocations and provider attempts

This distinction is important for semconv compatibility.

### 5.1 One inference span = one logical invocation

The GenAI inference convention models a logical operation. Automatic transient retries may occur inside that span.

Therefore:

```text
one compliant inference span
        ↓
one Git+ logical invocation record
```

Git+ MUST NOT split one compliant inference span into multiple logical invocations merely because the underlying provider client retried.

### 5.2 Attempt detail is subordinate

If instrumentation exposes provider-attempt detail separately, the normalizer MAY attach it beneath the logical invocation:

```text
Logical invocation · span 00f067…
  ├─ attempt 1 · timeout
  └─ attempt 2 · success
```

Attempt count/status MUST NOT be inferred from duration, timestamp gaps, or missing response fields.

### 5.3 Fallbacks follow upstream logical boundaries

If model/provider fallback is represented by another inference span, Git+ creates another logical invocation.

If upstream instrumentation keeps fallback inside the same logical span, Git+ keeps one logical record and MAY attach observed fallback/attempt detail.

The normalizer follows the source semantics rather than inventing span boundaries.

---

## 6. Outcome and finish reason are separate

The UI and API must not conflate:

```text
operation outcome
  OTel span status + error.type

model generation finish
  gen_ai.response.finish_reasons
```

Examples:

```text
status=ok, finishReasons=[length]
  successful provider operation whose generation hit a limit

status=error, errorType=timeout
  provider/client operation failed
```

This distinction should survive all the way from ingestion to UI.

---

## 7. Context lifecycle

When `gen_ai.conversation.compacted = true`, the invocation projection SHOULD show:

```text
Context: compacted prior conversation
```

That boolean does not prove when/how compaction happened.

A separate Git+ `context-compaction` trace event should exist only when the harness directly observes a transition with richer facts such as strategy or reason.

Likewise, truncation records should be emitted only when truncation is observable rather than inferred from absent content.

---

## 8. Audit tee: do not reuse a lossy observability path blindly

Normal OTel pipelines frequently sample, filter, redact, aggregate, and transform telemetry.

Preferred topology:

```text
                         ┌─ Git+ audit exporter / receiver
                         │  no intentional sampling of audit classes
harness → OTel SDK ──────┤  no untracked field-dropping transforms
                         │
                         └─ normal OTel backend
                            sampling / filtering / aggregation allowed
```

Prefer ingest at or near `sdk-export` before arbitrary Collector processors.

If Git+ receives data after a Collector, preserve that stage and any known sampling/transformation status.

The UI must not claim stronger coverage than the capture path supports.

### 8.1 Audit exporter health

Useful known-loss signals include:

```text
export queue overflow
batch dropped
receiver unavailable
collector sampling enabled
processor filtered audit class
```

Normalize these into `trace-health` or equivalent projected visibility.

Absence of an event is meaningful only with known capture capability and trace health.

---

## 9. Context Packs remain direct Git+ capture

Context Packs and ContextRender SHOULD NOT be reduced to generic GenAI attributes.

The harness/Git+ integration directly creates:

```text
Repository View
Context Pack
ContextRender
Context Exposure record
```

because these require:

- Git object verification;
- blob/gitlink resolution;
- ContextRender semantic placement framing;
- real Git reachability to `view.tree`;
- durable render commitments.

OTel correlation MAY be attached to the exposure:

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

Opt-in `gen_ai.input.messages`, `gen_ai.output.messages`, or `gen_ai.system_instructions` do not replace ContextRender verification.

---

## 10. Baggage is not an authority channel

Do not use OTel Baggage for:

```text
repository identity
member identity
capabilities
instruction authority
Context Pack identity
policy decisions
```

For correlation, prefer explicit trace/span attributes or validated Git+ metadata. Repository trust/policy and signed Git records remain authoritative.

---

## 11. Machine-facing interfaces

### 11.1 OTel audit receiver/exporter

Preferred shape:

```text
harness OTel SDK
  → OTLP localhost / unix socket / embedded receiver
  → versioned GenAI semconv normalizer
  → Git+ trace writer
```

The receiver should know repository/session association from authenticated local configuration rather than trusting arbitrary incoming attributes to choose another repository.

### 11.2 Fallback recorder

Keep:

```text
git+ trace record <repo> \
  --session <session-id> \
  --key <private-key> \
  --event <event.json>
```

for non-OTel adapters.

The trace schema after normalization is the same regardless of input transport.

---

## 12. Trace volume and retention

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

The Git trace is an audit index, not a second telemetry backend.

---

## 13. Workspace capture

Workspace tree capture remains boundary-driven:

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

## 14. Server projection

The browser should not reconstruct semconv meaning, trace DAGs, or trust state.

### 14.1 Session list

`/hub/sessions` stays cheap and may include:

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

`providerAttempts` MUST be omitted when attempt-level instrumentation is unavailable. It must not be guessed from logical invocation count.

### 14.2 Session detail

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

## 15. UI principles

### 15.1 Lead with repository context

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

### 15.2 Requested versus response model

Show a single model label when both values match.

When they differ, expose the distinction:

```text
requested  model-x
response   model-x-20260815
```

Do not silently overwrite one with the other.

### 15.3 Finish reason versus error

Display:

```text
✓ completed · finish: length
```

separately from:

```text
✕ error · timeout
```

This avoids treating normal generation termination as transport/provider failure.

---

## 16. Activity rows

Healthy session:

```text
0198f2aa  claude-code · model-x
3 commits · 18 invocations · 124k tokens · ✓ context
```

Session with useful warnings:

```text
0198f2bb  codex · model-y
18 invocations · 20 attempts · 96% input pressure · 2 compacted · ⚠ context
```

Only show attempts when explicitly known.

An optional detail indicator may show:

```text
OTel GenAI · audit capture available
```

Missing telemetry must never render as zero.

---

## 17. Flight Recorder

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

### 17.1 External OTel deep link

If an observability backend is configured, rows MAY expose **Open trace**.

Git+ audit pages must remain useful after external telemetry expires or is sampled out.

### 17.2 Preserve concurrency

Session/trace histories may be DAGs. Show branches/lanes for concurrent or resumed divergent histories rather than inferring causality from timestamps.

---

## 18. Context Pack diffs

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

---

## 19. Provenance and capture status

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

Weak runtime capture must not invalidate valid Git evidence, and valid Git evidence must not imply complete runtime capture.

---

## 20. Commit and Change Request surfaces

Repository artifacts should link back to the Flight Recorder only with a causal binding.

```text
Produced by session 0198f2aa
Causally bound logical invocation: sha1:c3…
chat · model-y · 96% input pressure
Context Pack: 12 blobs · 1 gitlink
OTel trace available
```

Do not infer a causal invocation merely from timestamp proximity.

---

## 21. Metrics stay operational

OTel metrics can power fleet views:

```text
tokens per day
latency distributions
cache hit ratios
tool failure rates
model/provider mix
```

They are not the canonical source for one session's audit history.

---

## 22. Implementation sequence

### Phase 1 — existing session summary

Use current `session.produced.usage` in Activity. Do not invent detailed runtime semantics before capture exists.

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

Add session detail projections, context diffs, logical invocation/attempt hierarchy, pressure, tool groups, workspace transitions, capture health, and external OTel links.

---

## 23. Acceptance criteria

The integration is useful when:

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
16. the Flight Recorder remains useful after external OTel retention expires.

---

## 24. Upstream compatibility

Primary upstream specification:

```text
https://github.com/open-telemetry/semantic-conventions-genai
```

The GenAI conventions are currently Development-status. The normalizer SHOULD therefore pin/report the revision it understands and SHOULD test mappings against upstream reference scenarios where practical.

Git+ should track upstream semantic changes at the ingestion boundary without forcing those changes into its durable Git schema.

---

## Final principle

> **Harness-native OpenTelemetry GenAI is the preferred runtime semantic interface; Git+ is the durable repository audit projection. A declared semconv controls interpretation of incoming spans, events, and logs, while Git+ normalizes that meaning into stable signed records. Logical inference spans remain logical invocations, retry attempts remain subordinate, model request/response and finish/error distinctions survive normalization, and Git-native Context Packs retain the stronger repository-evidence guarantees.**
