# Git-Native Telemetry Integration and UI

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-22  
**Revision:** draft-3

## 1. Summary

This document describes how [Invocation Telemetry](invocation-telemetry.md) and [Context Packs](content-pack.md) should be integrated with coding-agent harnesses and surfaced in the Git+ UI.

The preferred implementation is **OpenTelemetry-first**:

```text
agent harness / model runtime
        │
        │ native OTel spans + events/logs
        ↓
       OTLP
        │
        ├──────────────→ normal observability pipeline
        │                 sampling / transforms allowed
        │
        ↓
Git+ audit ingester
        │
        │ select + normalize + Git-ground + sign
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

The protocol boundary remains Git-native:

```text
Context Pack / ContextRender
  exact repository provenance

OTel
  runtime capture + correlation

Git+ trace
  durable normalized audit projection
```

The product goal is not another observability dashboard. It is to explain how repository context, runtime pressure, tools, mutations, and model calls related to the code that was produced.

---

## 2. Storage separation

The policy-critical session DAG remains small:

```text
refs/hub/session/<session-id>
  prompt / produced result / decisions / aggregate usage
  may be consulted by provenance or policy
```

Detailed audit provenance lives separately:

```text
refs/hub/trace/<session-id>
  Context Exposure
  Invocation Telemetry
  lifecycle events
  workspace transitions
  selected tool diagnostics
```

The trace MUST NOT participate in authorization, membership, protected-branch policy, mergeability, or `requireProvenance` checks.

This is true whether the trace originated from OTel or from fallback hooks.

---

## 3. Integration modes

### 3.1 Preferred: harness-native OTel

If the harness/runtime emits suitable OTel, Git+ SHOULD ingest it instead of reconstructing runtime activity from vendor hooks.

The harness can naturally expose:

```text
spans
  model invocations
  tool invocations
  sub-agent operations
  other duration-bearing work

events / structured logs
  context compaction
  context truncation
  retry/fallback selection
  workspace state transitions

span/log attributes
  model/provider
  token usage
  finish status
  timing
  runtime limits
```

Trace/span IDs provide correlation between these signals and an external observability backend.

Git+ then normalizes only the subset needed for durable repository audit.

### 3.2 Embedded integration

When Git+ is part of the harness itself, an embedded adapter may capture the same logical boundaries directly.

This is particularly useful for Context Pack and ContextRender creation because those guarantees are stronger than generic telemetry.

```text
beforeInvocation
  materialize Repository View
  build Context Pack
  build semantically framed ContextRender
  append Context Exposure

provider.invoke(...)

afterInvocation
  normalize provider/runtime facts
  append Invocation Telemetry
```

### 3.3 Fallback: vendor hooks

When native OTel is absent or incomplete:

```text
vendor hook
    ↓
.chr33s/telemetry.mjs
    ↓ normalize observed fields
    ↓
git+ trace record
```

Vendor event names remain adapter details. Hook integrations should not force their vocabulary into the durable Git+ schema.

---

## 4. Audit tee: do not reuse a lossy observability path blindly

Normal OTel pipelines frequently sample, filter, redact, aggregate, and transform telemetry.

That is appropriate for operational observability but must not silently weaken a claimed durable audit trail.

Preferred topology:

```text
                         ┌─ Git+ audit exporter / receiver
                         │  no intentional sampling of audit classes
harness → OTel SDK ──────┤  no untracked field-dropping transforms
                         │
                         └─ normal OTel backend
                            sampling / filtering / aggregation allowed
```

### 4.1 Strongest ingestion point

Prefer ingestion at or near `sdk-export`, before arbitrary collectors can mutate or remove data.

If Git+ receives telemetry after a collector, preserve that fact:

```json
{
  "capture": {
    "transport": "otel",
    "stage": "local-collector"
  }
}
```

The UI should then avoid claiming stronger completeness than the path can support.

### 4.2 Audit exporter health

The audit exporter/receiver should expose loss when it can detect it.

Examples:

```text
export queue overflow
batch dropped
receiver unavailable
collector sampling enabled
processor filtered audit class
```

Git+ can normalize these into `trace-health` records or session-level visibility state.

Absence of a trace record is meaningful only in the context of capture capability and known exporter health.

---

## 5. OTel-to-Git+ normalization

OTel is not the persisted Git+ schema.

The normalization layer should answer:

```text
What stable audit concept does this signal represent?
What was its evidence class?
What immutable Git record does it become?
```

For example:

```text
OTel model-invocation span
        ↓
model/provider attributes
usage attributes
finish status
duration
trace/span IDs
        ↓
Git+ Invocation Telemetry
```

or:

```text
OTel event/log "context compacted"
        ↓
Git+ context-compaction record
```

### 5.1 Preserve correlation, not dependence

A normalized record may preserve:

```json
{
  "capture": {
    "transport": "otel",
    "traceId": "...",
    "spanId": "...",
    "scope": "agent-runtime"
  }
}
```

The external OTel trace can then be opened from the Git+ UI if configured.

But Git+ durable joins use Git record commit OIDs:

```text
OTel TraceId / SpanId
  external correlation

Git record OID
  canonical durable audit identity
```

### 5.2 Do not freeze OTel GenAI attribute names into the protocol

The normalizer should support a versioned mapping layer.

```text
OTel semantic conventions vN
provider-specific OTel attributes
custom harness instrumentation
        ↓
normalization mapping
        ↓
Git+ stable fields
```

Persist the instrumentation scope/version or semantic-convention version as diagnostic metadata when useful.

Git+ should not require historical clients to understand every upstream convention version.

### 5.3 Evidence class survives transport

Examples:

```text
provider token count carried in OTel
  still provider-reported

harness wall-clock duration carried in OTel
  harness-observed

pressure ratio computed by hub
  derived
```

The transport does not upgrade the authority of a value.

---

## 6. Context Packs remain direct Git+ capture

Context Packs and ContextRender SHOULD NOT be reduced to generic OTel attributes.

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
- semantic ContextRender placement framing;
- real Git reachability to `view.tree`;
- durable render commitments.

OTel correlation is attached to the exposure when available:

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

That lets the UI join exact repository provenance with the model invocation span without making OTel storage authoritative.

---

## 7. Baggage is not an authority channel

Do not use OTel Baggage for:

```text
repository authority
member identity
capabilities
instruction authority
Context Pack identity
policy decisions
```

Baggage may be propagated to downstream services and has no built-in integrity guarantee.

For correlation, prefer explicit span/log attributes or validated Git+ metadata. Authorization continues to derive from repository trust/policy and signed records.

---

## 8. Machine-facing interfaces

### 8.1 OTel receiver/exporter path

The preferred integration should expose an OTLP-compatible audit endpoint or exporter path such as conceptually:

```text
harness OTel SDK
  → OTLP localhost / unix socket / embedded receiver
  → Git+ normalizer
```

The exact transport binding is implementation-specific.

The receiver should know the repository/session association from authenticated local configuration, not trust arbitrary incoming attributes to choose another repository.

### 8.2 Generic fallback recorder

Keep a generic fallback primitive for non-OTel adapters:

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

The same trace schema is produced whether input came from OTel, hooks, or embedded capture.

---

## 9. Trace volume and batching

OTel may produce far more telemetry than belongs in Git.

Git+ should select, distill, and batch.

Recommended canonical records:

```text
Context Exposure
Invocation Telemetry
Workspace Transition when audit-relevant
Context compaction/truncation when audit-relevant
Tool failures/truncations or mutation summaries
Trace-health / known-loss markers
```

Usually external-only or side telemetry:

```text
every successful read/grep
raw provider request/response
raw tool bodies
verbose model stream events
high-cardinality timing detail
fleet metrics
```

The Git trace is the durable audit index, not a second OTel backend.

---

## 10. Workspace capture

Workspace tree capture should remain boundary-driven.

```text
edit begins
  remember tree A

writes happen
  workspace marked dirty

before next auditable invocation
  materialize tree B
  record A → B
  next Context Pack uses B
```

An OTel tool span can identify the operation that caused the mutation, but the authoritative repository identities are the Git tree OIDs.

---

## 11. Server projection

The browser should not reconstruct session/trace DAG semantics or trust state.

### 11.1 Session list

`/hub/sessions` stays cheap and may show trace-derived summaries:

```json
{
  "session": "0198f2aa-...",
  "usage": {
    "inputTokens": 118420,
    "outputTokens": 4281
  },
  "telemetry": {
    "transport": "otel",
    "invocations": 18,
    "peakInputPressure": 0.96,
    "peakInvocation": "sha1:abc123...",
    "compactions": 2,
    "retries": 1,
    "toolFailures": 1,
    "coverage": "partial"
  }
}
```

`peakInputPressure` exists only when a compatible effective input limit is known.

### 11.2 Session detail

```text
GET /hub/sessions/:id
```

should join:

```text
small session DAG
+
policy-invisible Git+ trace
+
optional external OTel correlation links
```

The server owns:

- trust/signature verification;
- session/trace folding;
- causal parentage;
- redaction state;
- joins between exposures, invocations, tools, transitions, commits, and refs;
- trace visibility/health;
- derived diagnostics.

The browser owns presentation.

---

## 12. UI principle

Lead with repository-context changes, not telemetry plumbing.

Useful story:

```text
tests/auth.test.ts exposed
      ↓
model invocation span
      ↓
96% effective input pressure
      ↓
context compaction event
      ↓
tests/auth.test.ts absent from next exposure
      ↓
workspace mutation
      ↓
commit
```

The OTel trace/span IDs should be available for experts, but they should not dominate the primary UI.

---

## 13. Activity rows

Healthy session:

```text
0198f2aa  claude-code · model-x
3 commits · 124k tokens · ✓ context
```

Session with useful warnings:

```text
0198f2bb  codex · model-y
284k tokens · 96% input pressure · 3 compactions · 1 retry · ⚠ context
```

An optional detail indicator may show:

```text
OTel · audit capture available
```

Missing telemetry must never render as zero.

---

## 14. Flight Recorder

Example:

```text
Session 0198f2aa
Claude Code · model-x
────────────────────────────────────────

09:42  Context Exposure
       tree 79ad…
       7 blobs · 1 gitlink
       placements: developer, tool
       ✓ evidence + render verified
       OTel span 00f067…

09:42  Invocation
       model-x
       118k input · 90k cached · 4.2k output
       59% of total context window

09:43  Tool activity
       14 operations · 1 failure
       [Open OTel trace]

09:45  Workspace
       tree 79ad… → a130…

09:46  Context compaction
       context-window · summary

09:46  Context diff
       - tests/auth.test.ts

09:47  Invocation
       172k / 180k effective input limit
       ⚠ 96% input pressure

09:48  Commit
       abc123 Fix auth policy
```

### 14.1 External OTel deep link

If an observability backend is configured, a trace/span row MAY expose:

```text
Open trace
```

The external backend is supplemental. Git+ audit pages must remain useful when that backend has expired, sampled out, or deleted detailed telemetry.

### 14.2 Preserve concurrency

Session and trace histories may be DAGs. The UI should show branches/lanes for concurrent records or resumed divergent histories rather than infer causality from timestamps.

---

## 15. Context Pack diffs

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

OTel does not replace this Git-native historical evidence view.

---

## 16. Provenance and capture status

Display evidence dimensions separately:

```text
Repository evidence   ✓ verified
View reachability     ✓ retained through Git trace record
Context render        ✓ retained and digest verified
Runtime usage         provider reported
Runtime capture       OTel · partial
Collector path        local-collector · transformed
Tool body             expired
Workspace alignment   ✓ matched recorded invocation boundary
```

Useful warnings include:

```text
⚠ audit OTel export reported drops
⚠ sampling/filtering exists before Git+ ingest
⚠ capture path transformed telemetry
⚠ lifecycle capture unavailable
✕ path/blob mismatch
✕ gitlink mode/commit mismatch
```

A green repository-provenance state must not hide weak runtime-capture coverage, and weak OTel coverage must not invalidate valid Git evidence.

---

## 17. Audit summary

A derived card may show:

```text
Agent session audit

Context provenance      ✓ 18 / 18 verified
Runtime capture          OTel · partial
Audit exporter health    ✓ no known drops
Workspace alignment      ✓ matched invocation boundaries
Peak input pressure      ⚠ 96% · sha1:c3…
Compactions              2
Retries                  1
Failed tools             1
Knowledge continuity     ⚠ test removed after compaction
```

Derived diagnosis must use observable language.

Prefer:

> `tests/auth.test.ts` was present in Exposure A and absent after a recorded compaction before Exposure B.

Avoid:

> The model forgot the test.

---

## 18. Commit and Change Request surfaces

Repository artifacts should link back to the Flight Recorder.

A commit may show:

```text
Produced by session 0198f2aa
Causally bound invocation: sha1:c3…
model-y · 96% input pressure
Context Pack: 12 blobs · 1 gitlink
OTel trace available
```

Only show an invocation as causal when the Git/session/trace relationship supports that claim. Do not infer it merely from timestamp proximity.

---

## 19. Metrics stay operational

OTel metrics can power separate fleet views:

```text
tokens per day
latency distributions
cache hit ratios
tool failure rates
model/provider mix
```

Those are valuable, but they are not the canonical source for one session's audit history.

The Flight Recorder should use per-invocation trace facts, not reconstructed metric aggregates.

---

## 20. Knowledge durability UI

Repository Memory should continue to display cited claims and structured evidence dependencies independently of OTel:

```text
gotcha  Worker auth tests require production policy fixture
source  session 0198f2aa
support 2 blobs · 1 gitlink
state   ⚠ config/policy.json changed; revalidate
```

OTel helps diagnose whether recalled knowledge survived runtime context lifecycle; it does not turn a cited claim into truth.

---

## 21. Implementation sequence

### Phase 1 — existing session summary

Use current `session.produced.usage` in Activity. Do not invent window pressure or trace completeness before detailed capture exists.

### Phase 2 — OTel audit ingest

Implement:

```text
OTLP audit receiver/exporter integration
OTel → Git+ normalization
refs/hub/trace/<session>
trace-health / capture-stage metadata
```

Support model invocation telemetry first.

### Phase 3 — Context Exposure integration

Add direct Git+ capture for:

```text
Repository View
Context Pack
ContextRender
Context Exposure
```

Correlate each exposure with the relevant OTel trace/span when available.

### Phase 4 — fallback hooks and generic recorder

Add:

```text
git+ trace record
vendor hook adapters
```

for harnesses without sufficient native OTel.

### Phase 5 — server projection

Extend `/hub/sessions` and add `GET /hub/sessions/:id` joining session, Git trace, and optional external OTel links.

### Phase 6 — Flight Recorder

Add context diffs, pressure, retries, tool groups, workspace transitions, capture-health status, and external trace deep links.

This order uses the standard runtime signal first instead of building a vendor-adapter matrix before it is necessary.

---

## 22. Acceptance criteria

The integration is useful when:

1. a harness with native OTel can feed Git+ without a vendor-specific runtime adapter;
2. hooks remain available when OTel is missing or insufficient;
3. audit capture is independent from lossy observability sampling/filtering;
4. Git+ normalizes OTel conventions instead of persisting them as the durable protocol contract;
5. OTel TraceId/SpanId provide correlation while Git record OIDs remain canonical identity;
6. OTel Baggage is never treated as authority or integrity evidence;
7. Context Packs/ContextRender remain directly Git-grounded and survive external telemetry loss;
8. provider-reported values remain distinct from harness-observed and derived values;
9. high-volume OTel detail does not turn the Git trace into a second telemetry backend;
10. high-frequency trace data remains outside policy-critical session folds;
11. exporter loss/sampling/transformation is visible when known;
12. workspace mutations remain grounded by Git trees, not by telemetry strings;
13. the Flight Recorder remains useful after external OTel retention expires;
14. a reviewer can move from a commit or Change Request to repository evidence, runtime audit, and optionally the external OTel trace.

---

## 23. Upstream compatibility notes

Useful OTel references:

- Logs and trace correlation: `https://opentelemetry.io/docs/specs/otel/logs/`
- Log data model / TraceId / SpanId: `https://opentelemetry.io/docs/specs/otel/logs/data-model/`
- Semantic conventions: `https://opentelemetry.io/docs/specs/otel/semantic-conventions/`
- Collector transformations/filtering: `https://opentelemetry.io/docs/collector/transforming-telemetry/`
- Baggage security considerations: `https://opentelemetry.io/docs/concepts/signals/baggage/`

The mapping layer should be versioned because semantic conventions may evolve independently of Git+.

---

## Final principle

> **Harness-native OpenTelemetry is the preferred way to capture runtime behavior; Git+ is the durable repository audit projection. OTel carries operation correlation and runtime facts, Git+ selects and normalizes the audit-relevant subset into signed records, and Context Packs/ContextRender retain the stronger Git-native evidence guarantees. Hooks are a fallback, metrics remain operational, and lossy observability processing must never be mistaken for a complete audit trail.**
