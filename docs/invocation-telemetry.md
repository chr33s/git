# Git-Native Invocation Telemetry

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Spec revision:** draft-3

## 1. Summary

This specification defines durable runtime provenance for coding-agent invocations.

[Context Packs](content-pack.md) answer:

> **What Git-grounded repository evidence and semantically framed ContextRender were associated with an invocation?**

Invocation Telemetry answers:

> **Under what observable or explicitly reported runtime conditions did that invocation and its resulting operations occur?**

The preferred runtime capture interface is **harness-native OpenTelemetry (OTel)**. OTel supplies a common trace/log/event transport and correlation model; `git+` selects and normalizes audit-relevant observations into signed Git records.

The architecture is:

```text
harness / model runtime
        │
        │ native OTel when available
        │ hooks only as fallback
        ↓
loss-intolerant Git+ audit ingest
        │
        │ normalize stable concepts
        ↓
refs/hub/trace/<session-id>
        │
        └── signed Git-native audit provenance

ordinary OTel export may independently flow to
observability backends with normal sampling/filtering
```

OTel is an **ingestion and correlation layer**, not the durable Git+ protocol. Context Packs, ContextRender commitments, Git reachability, signatures, and immutable trace-record identity remain Git-native.

The goal is an **agent flight recorder**, not an inference-attestation system. Nothing here proves attention, understanding, memory, reasoning, or causation.

---

## 2. Session index versus audit trace

The existing session namespace remains the distilled record:

```text
refs/hub/session/<session-id>
```

It is intentionally small and may be consulted by provenance and protected-branch policy.

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

High-frequency observability MUST NOT turn into policy-path cost.

Existing aggregate fields such as `session.produced.usage` MAY remain in the session DAG as small convenience summaries.

### 2.1 Immutable trace identity

Each canonical trace record is carried by a signed Git record commit. Later records MUST reference earlier canonical records by qualified Git commit OID:

```text
sha1:<hex>
sha256:<hex>
```

OTel `TraceId` and `SpanId` are correlation identifiers, not durable Git+ record identity.

Human-facing event IDs MAY exist for display/search, but they are not sufficient immutable cross-record references.

If low-value records are batched, a batch entry MUST have an immutable locator such as:

```json
{
  "record": "sha1:abc123...",
  "index": 7
}
```

Context Exposure and Invocation Telemetry SHOULD normally remain standalone records so their commit OIDs are directly referenceable.

---

## 3. Preferred capture source: OpenTelemetry

### 3.1 Why OTel

When a harness already emits OTel, `git+` SHOULD consume that signal rather than reconstructing provider calls, tool operations, retries, timings, and usage from vendor-specific hooks.

Useful OTel properties include:

- trace/span parentage for operation correlation;
- log records carrying trace/span context;
- span attributes for model/tool/runtime facts;
- point-in-time events/logs for context lifecycle changes;
- OTLP as a common export protocol;
- compatibility with ordinary observability infrastructure.

This specification does not require one OTel SDK, Collector, backend, or GenAI semantic-convention version.

### 3.2 OTel is not the canonical Git+ schema

OTel semantic conventions evolve independently of this protocol. In particular, GenAI conventions may change names, stability levels, or repositories over time.

Therefore:

```text
OTel attributes/events
        ↓
Git+ normalization adapter
        ↓
stable Git+ runtime concepts
```

A Git+ verifier MUST NOT require a historical OTel semantic-convention package to interpret a persisted trace record.

The normalizer MAY preserve original OTel namespaced attributes for diagnostics, but core fields in this specification retain stable Git+ meaning.

### 3.3 OTel correlation fields

A normalized record MAY preserve its OTel origin:

```json
{
  "capture": {
    "transport": "otel",
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7",
    "scope": "example.agent-runtime"
  }
}
```

These fields allow correlation with an external OTel backend.

They MUST NOT replace:

- the signed Git record identity;
- the repository/session binding;
- Context Pack verification;
- ContextRender commitments;
- Git reachability.

### 3.4 Hooks remain a fallback

A harness without suitable OTel MAY use vendor hooks or an embedded adapter.

A normalized record MAY instead say:

```json
{
  "capture": {
    "transport": "hook",
    "integration": "vendor-hooks-v2"
  }
}
```

The Git+ trace schema MUST NOT depend on vendor hook names.

---

## 4. Loss-intolerant audit ingest

Ordinary observability pipelines are allowed to sample, filter, redact, aggregate, transform, or drop telemetry. Those behaviors are useful for operations but weaken an audit trail.

A Git+ deployment that claims durable per-invocation audit SHOULD therefore use a dedicated audit branch of the OTel pipeline:

```text
                    ┌── Git+ audit ingest
harness → OTel SDK ─┤   no intentional lossy sampling/filtering
                    │
                    └── normal observability export
                        sampling/filtering/aggregation allowed
```

### 4.1 Preferred ingestion point

The strongest capture point is before arbitrary collector processors can drop or mutate audit-relevant fields.

A producer SHOULD identify where normalized input was observed:

```text
sdk-export
local-collector
remote-collector
hook
embedded
other
```

Example:

```json
{
  "capture": {
    "transport": "otel",
    "stage": "sdk-export",
    "traceId": "...",
    "spanId": "..."
  }
}
```

### 4.2 Sampling and transformation

If the Git+ audit path is known to be sampled, filtered, or transformed before ingestion, the trace MUST NOT claim complete capture for affected classes.

Where known, trace health SHOULD record facts such as:

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

Values are claims by the capture system, not proofs that an external collector behaved correctly.

### 4.3 No Baggage authority

OTel Baggage MUST NOT be used as an authority or integrity channel for:

- repository identity;
- member identity;
- instruction authority;
- capabilities;
- Context Pack identity;
- authorization decisions.

Baggage may propagate across service boundaries and has no built-in integrity guarantee. If a baggage value is useful for correlation, the normalizer MAY copy it into descriptive metadata after validation, but Git+ trust decisions MUST derive from Git-native signed records and repository policy.

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

OTel is a transport and does not determine the evidence class by itself.

For example, a provider input-token count transported through an OTel span remains **provider-reported**; it does not become harness-observed merely because OTel carried it.

A consumer MUST NOT silently promote `reported` or `derived` values to `observed` facts.

Examples:

```text
ContextRender body byte length       observed
harness wall-clock duration          observed
provider token count via OTel        reported
provider model revision via OTel     reported
context utilization ratio            derived
estimated dollar cost                derived
```

---

## 6. Signal mapping

### 6.1 Spans

Operations with meaningful duration SHOULD map naturally from OTel spans, for example:

```text
model invocation
tool invocation
remote repository operation
sub-agent execution
```

Git+ does not need to persist every span. The ingester selects audit-relevant facts and may summarize repetitive successful operations.

### 6.2 Events and logs

Point-in-time state transitions MAY arrive as OTel Events or structured LogRecords correlated to a span, for example:

```text
context exposed
context compacted
context truncated
workspace transitioned
retry selected
fallback selected
```

Git+ normalizes their meaning into its own trace record types.

### 6.3 Metrics

OTel metrics are useful for fleet-level operational dashboards such as token rates, latency distributions, or tool-failure counts.

Metrics MUST NOT be used as the sole source for reconstructing one historical invocation because aggregation loses event identity and causal detail.

The durable audit trail is built from selected spans/events/logs plus Git-native Context Pack/exposure records.

---

## 7. Invocation Telemetry record

A minimal normalized record is:

```json
{
  "type": "invocation-telemetry",
  "exposure": "sha1:abc123...",
  "capture": {
    "transport": "otel",
    "stage": "sdk-export",
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7"
  },
  "model": {
    "provider": "example",
    "id": "model-x"
  },
  "usage": {
    "source": "provider",
    "inputTokens": 118420,
    "outputTokens": 4281,
    "cachedInputTokens": 90210
  },
  "context": {
    "renderBytes": 483921,
    "contextWindowTokens": 200000,
    "contextWindowSource": "provider"
  }
}
```

`exposure` is the qualified Git commit OID of the prior Context Exposure trace record.

### 7.1 Model identity

A producer MAY record:

```json
{
  "provider": "example",
  "id": "model-x",
  "revision": "2026-08-15"
}
```

These are operational labels. A stable model name is not proof of stable model weights or infrastructure. `revision` MUST be omitted when the provider/runtime did not report one.

### 7.2 Usage

Core fields are:

```text
inputTokens
outputTokens
cachedInputTokens
```

Each is a non-negative integer when present.

`usage.source` SHOULD distinguish at least:

```text
provider
estimated
```

Estimated usage SHOULD include an estimator identifier and MUST NOT be presented as provider-reported.

Whole-invocation `inputTokens` MUST NOT be described as the token size of repository ContextRender alone.

### 7.3 ContextRender size

`context.renderBytes` is the harness-observed body-byte total across ContextRender segments. It is tokenizer-independent.

A producer MAY additionally estimate repository-context tokens, but the estimate MUST be labeled estimated.

### 7.4 Context-window semantics

Two limits are distinct:

```text
contextWindowTokens
  total sequence/context capacity the harness believes applies

effectiveInputLimitTokens
  maximum input token budget the harness believes is actually usable for this invocation
  after reserved output budget or other harness/provider constraints
```

A producer MUST NOT use one name for the other.

Each limit SHOULD carry a source:

```text
provider
model-catalog
harness-config
runtime
```

If a limit is unknown or ambiguous, omit it rather than guessing.

Derived ratios have different meanings:

```text
inputTokens / contextWindowTokens
  share of total context window occupied by reported input

inputTokens / effectiveInputLimitTokens
  input pressure, only when a compatible effective input ceiling is known
```

### 7.5 Timing and finish status

A producer MAY record harness-observed duration and SHOULD record a coarse finish status when known:

```text
success
length
content-filter
cancelled
timeout
provider-error
other
```

### 7.6 Retries and fallbacks

Distinct provider invocations remain distinct canonical records when retained.

A later record references the prior invocation by Git commit OID:

```json
{
  "attempt": {
    "number": 2,
    "previous": "sha1:def456...",
    "reason": "timeout"
  }
}
```

OTel parentage MAY help the ingester discover the relationship, but the persisted Git+ relationship uses immutable Git record identity.

A harness MUST NOT collapse several behaviorally or billably distinct provider calls into one telemetry record merely because they produced one final agent response.

---

## 8. Context lifecycle

### 8.1 Compaction

A normalized record MAY be:

```json
{
  "type": "context-compaction",
  "capture": {
    "transport": "otel",
    "traceId": "...",
    "spanId": "..."
  },
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

### 8.2 Truncation

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

Dropped counts are diagnostics, not exact item identity. Exact repository exposure comes from surrounding Context Packs and ContextRender records.

### 8.3 Observable language

Lifecycle records describe what the harness/runtime did. They MUST NOT be phrased as "the model forgot" or equivalent cognitive claims.

---

## 9. Tool telemetry

Detailed tool traces are high-volume and frequently sensitive.

A canonical trace MAY retain compact audit facts, especially failures, truncation, and repository mutation:

```json
{
  "type": "tool-telemetry",
  "invocation": "sha1:789abc...",
  "tool": "read-file",
  "status": "success",
  "result": {
    "bytes": 8421,
    "truncated": false,
    "digest": "sha256:..."
  }
}
```

The canonical record SHOULD contain metadata and digests, not raw result bodies.

Repetitive successful spans MAY be batched or summarized; raw tool bodies MAY remain disposable side objects or only in the external OTel backend.

A successful tool call does not prove its result entered a later model invocation. Repository evidence that crossed the later invocation boundary should appear in that invocation's Context Pack where representable.

---

## 10. Workspace transitions

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

The implementation does not need a tree after every filesystem syscall. Capture at meaningful boundaries:

- after an agent edit tool completes;
- before the next repository-affecting model invocation;
- before commit creation;
- after checkout, merge, rebase, or similar Git state changes.

The next Context Pack `view.tree` remains authoritative for retrieval.

---

## 11. Capture capability versus actual coverage

An integration may know what it is capable of observing without proving every event was captured.

Therefore distinguish:

```text
capture capability
  which runtime classes the instrumentation can observe

trace coverage
  what audit records actually arrived and whether known loss/transformation occurred
```

An OTel integration MAY declare:

```json
{
  "visibility": {
    "integration": "otel",
    "stage": "sdk-export",
    "capabilities": ["invocation", "tools", "lifecycle", "workspace"]
  }
}
```

This MUST NOT be presented as proof of complete capture.

Where loss can be detected, record trace health or exporter health. A projection may classify each class as:

```text
available
partial
unknown
```

`complete` SHOULD be used only when the capture path has a defined completeness mechanism, no intentional sampling/filtering for that class, and no known loss.

> **No compaction record exists** is not equivalent to **the harness proves no compaction occurred**.

---

## 12. Security and retention

Provider envelopes, raw tool bodies, prompts, and transcript-like payloads SHOULD NOT become canonical trace records by default.

OTel attributes themselves may contain source, paths, prompts, user identifiers, or secrets. The audit ingester MUST apply repository secret-handling and access policy before persistence.

The normal observability branch MAY apply stronger redaction, aggregation, or retention rules independently.

The audit trace MUST remain policy-invisible: losing or expiring optional trace detail may reduce auditability but MUST NOT retroactively change whether a source push, review, membership grant, or merge was authorized.

---

## 13. Product surface

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
OTel trace/span correlation
model/provider
input/output/cache tokens
window/effective input limits and their sources
retries/fallbacks
context lifecycle events
tool failures/truncation
workspace transitions
resulting commits/refs
capture capability / actual coverage
```

Derived values MUST be labeled derived.

---

## 14. Recommended V1 profile

Start with:

```text
1. native OTel ingestion when the harness provides it
2. hooks/embedded capture only as fallback
3. a dedicated loss-intolerant audit export branch
4. Context Exposure records in refs/hub/trace/<session>
5. provider-reported input/output/cache usage
6. contextWindowTokens and/or effectiveInputLimitTokens with explicit source
7. retries/fallbacks as distinct invocations
8. compaction/truncation events when observable
9. compact tool failure/truncation diagnostics
10. beforeTree → afterTree at meaningful repository boundaries
11. capture capability plus detectable trace-loss/transformation markers
```

Git+ SHOULD preserve external OTel correlation identifiers where useful while treating Git record OIDs as the canonical durable identity.

---

## 15. OpenTelemetry compatibility guidance

The OTel specifications and semantic conventions evolve independently of Git+.

Useful upstream references include:

- OpenTelemetry Logs and trace correlation: `https://opentelemetry.io/docs/specs/otel/logs/`
- OTel LogRecord trace-context fields: `https://opentelemetry.io/docs/specs/otel/logs/data-model/`
- Semantic conventions: `https://opentelemetry.io/docs/specs/otel/semantic-conventions/`
- Collector transformations/filtering: `https://opentelemetry.io/docs/collector/transforming-telemetry/`
- Baggage security considerations: `https://opentelemetry.io/docs/concepts/signals/baggage/`

An implementation SHOULD pin and report the instrumentation/semantic-convention version it understands, but a persisted Git+ record MUST remain interpretable without replaying the original OTel pipeline.

---

## Final invariant

> **OpenTelemetry is the preferred runtime capture and correlation layer; Git+ is the durable audit projection. Audit-relevant OTel observations are normalized into signed, policy-invisible Git records whose canonical identity is their Git record OID. The audit path avoids silent observability sampling/filtering, preserves evidence-class and token-window semantics, and never substitutes OTel correlation or Baggage for Git-native trust, Context Pack verification, or ContextRender commitments.**
