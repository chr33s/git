# Git-Native Invocation Telemetry

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Spec revision:** draft-2

## 1. Summary

This specification defines runtime provenance for coding-agent invocations without turning the policy-critical session DAG into a high-frequency trace store.

[Context Packs](content-pack.md) answer:

> **What Git-grounded repository evidence and semantic ContextRender were associated with an invocation?**

Invocation Telemetry answers:

> **Under what observable or explicitly reported runtime conditions did that invocation and its resulting operations occur?**

The useful architecture is:

```text
Repository provenance
  Repository View
  Context Pack
        ↓
Exposure provenance
  ContextRender
  Context Exposure Event
        ↓
Runtime provenance
  Invocation Telemetry
  Context Lifecycle
  Workspace Transitions
  Tool Telemetry
```

The first two layers and runtime events may be correlated, but runtime telemetry MUST NOT affect Context Pack validity.

The goal is an **agent flight recorder**, not an inference-attestation system. Nothing here proves attention, understanding, memory, reasoning, or causation.

---

## 2. Storage architecture: session index versus audit trace

The existing session namespace remains the distilled record:

```text
refs/hub/session/<session-id>
```

It is intentionally small and may be consulted by provenance and protected-branch policy.

Per-invocation audit records MUST NOT be added there merely because they are related to a session. A conforming implementation SHOULD use a sibling namespace:

```text
refs/hub/trace/<session-id>
```

The trace is:

- signed;
- append-only or DAG-preserving under concurrent writers;
- bound to the same repository and session identity;
- independently replicated according to audit-retention policy;
- **not consulted for authorization, membership, mergeability, protected-branch policy, or `requireProvenance` checks**.

This keeps observability volume from becoming a policy-path cost.

### 2.1 Session-level summaries remain small

Existing fields such as aggregate `session.produced.usage` MAY remain in the session DAG as a convenience summary.

The detailed trace is the source for per-invocation investigation when retained. The aggregate summary does not need to reproduce every provider-specific field.

### 2.2 Trace record identity

Each canonical trace event is carried by a signed Git record commit. Later records MUST reference earlier canonical events by qualified record commit OID:

```text
sha1:<hex>
sha256:<hex>
```

Human-facing event IDs MAY be retained for display, but they are not immutable cross-event identity.

If an implementation later batches several low-value events into one trace record, it MUST define an equally immutable locator such as `{record:<qualified-oid>, index:<n>}`. V1 integrations SHOULD prefer standalone records for Context Exposure and Invocation Telemetry so their commit OIDs are directly referenceable; high-volume tool detail may be batched or side-stored.

---

## 3. Evidence classes

Runtime values have three evidence classes:

```text
observed
  measured directly by the harness at a defined boundary

reported
  supplied by a provider, tool, or runtime

derived
  computed from observed or reported values
```

A consumer MUST NOT silently promote `reported` or `derived` values to `observed` facts.

Examples:

```text
ContextRender bytes             observed
wall-clock duration             observed
provider token count            reported
provider model revision         reported
context utilization ratio       derived
estimated dollar cost           derived
```

---

## 4. Invocation Telemetry

A minimal record is:

```json
{
  "type": "invocation-telemetry",
  "exposure": "sha1:abc123...",
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

### 4.1 Model identity

A producer MAY record:

```json
{
  "provider": "example",
  "id": "model-x",
  "revision": "2026-08-15"
}
```

These are operational labels. A stable model name is not proof of stable model weights or infrastructure. `revision` MUST be omitted when the provider/runtime does not supply one.

### 4.2 Usage

Core fields are:

```text
inputTokens
outputTokens
cachedInputTokens
```

Each is a non-negative integer when present.

`usage.source` SHOULD be:

```text
provider
estimated
```

Estimated usage SHOULD include an estimator identifier and MUST NOT be presented as provider-reported.

Whole-invocation `inputTokens` MUST NOT be described as the token size of repository ContextRender alone.

### 4.3 ContextRender size

`context.renderBytes` is the harness-observed body-byte total across ContextRender segments. It is tokenizer-independent and can remain useful across model migrations.

A producer MAY additionally estimate repository-context tokens, but the estimate MUST be labeled estimated.

### 4.4 Context-window semantics

Two limits are distinct:

```text
contextWindowTokens
  total sequence/context capacity the harness believes applies

effectiveInputLimitTokens
  maximum input token budget the harness believes is actually usable for this invocation
  after any reserved output budget or other harness/provider constraint
```

A producer MUST NOT use one name for the other.

Each recorded limit SHOULD carry its source:

```text
provider
model-catalog
harness-config
runtime
```

Example:

```json
{
  "context": {
    "contextWindowTokens": 200000,
    "contextWindowSource": "provider",
    "effectiveInputLimitTokens": 180000,
    "effectiveInputLimitSource": "harness-config"
  }
}
```

If a limit is unknown or ambiguous, omit it rather than guessing.

Derived ratios have different meanings:

```text
inputTokens / contextWindowTokens
  share of total context window occupied by reported input

inputTokens / effectiveInputLimitTokens
  input pressure, only when an effective input limit with compatible semantics is known
```

A UI MUST NOT label the first ratio "input pressure" unless the denominator really is an effective input ceiling.

### 4.5 Timing and finish status

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

Provider-specific finish reasons may be namespaced metadata.

### 4.6 Retries and fallbacks

Distinct provider invocations remain distinct trace records.

A later invocation references the prior invocation record by commit OID:

```json
{
  "attempt": {
    "number": 2,
    "previous": "sha1:def456...",
    "reason": "timeout"
  }
}
```

A harness MUST NOT collapse several behaviorally or billably distinct provider calls into one telemetry record merely because they produced one final agent response.

---

## 5. Context lifecycle

### 5.1 Compaction

A harness MAY record:

```json
{
  "type": "context-compaction",
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

### 5.2 Truncation

A harness MAY record:

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

Dropped counts are diagnostics, not exact item identity. Exact repository exposure is determined from surrounding Context Packs/ContextRender artifacts.

### 5.3 Observable language

Lifecycle records describe what the harness/runtime did. They MUST NOT be phrased as "the model forgot" or equivalent cognitive claims.

---

## 6. Tool telemetry

Detailed tool traces are high-volume and frequently sensitive.

A canonical audit trace MAY retain compact tool facts, especially failures, truncation, and repository mutations:

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

Bulky tool traces MAY be batched or retained as disposable side objects. Their expiry MUST NOT invalidate canonical repository provenance.

A successful tool call does not prove its result entered a later model invocation. Repository evidence that later crossed the invocation boundary should appear in the later Context Pack where representable.

---

## 7. Workspace transitions

When a repository-mutating operation changes the effective tree, a trace MAY record:

```json
{
  "type": "workspace-transition",
  "operation": "sha1:tool-record...",
  "beforeTree": "sha256:aaa...",
  "afterTree": "sha256:bbb..."
}
```

Both OIDs SHOULD remain reachable for the intended audit-retention period.

V1 does not require a tree after every filesystem syscall. Capture at meaningful boundaries:

- after an agent edit tool completes;
- before the next repository-affecting model invocation;
- before commit creation;
- after checkout, merge, rebase, or similar Git state changes.

The next Context Pack `view.tree` remains authoritative for retrieval.

---

## 8. Capture capability versus actual coverage

An integration may know what it is **capable of observing** without proving every event in that class was captured.

Therefore use distinct concepts:

```text
capture capability
  what hooks / runtime boundaries this integration can observe

trace coverage
  what records actually arrived and whether known loss occurred
```

A session-start or adapter declaration MAY say:

```json
{
  "visibility": {
    "integration": "claude-code-hooks",
    "capabilities": ["session", "tools", "workspace"]
  }
}
```

This MUST NOT be presented as proof that every tool or workspace event was captured.

Where the adapter can detect loss, it SHOULD record trace health such as:

```json
{
  "type": "trace-health",
  "source": "claude-code-hooks",
  "sequence": 42,
  "dropped": 0
}
```

A projection may then classify a telemetry class as:

```text
available
partial
unknown
```

A boolean `complete: true` SHOULD be used only when the integration has a defined completeness mechanism sufficient to support that claim.

> **No compaction record exists** is not equivalent to **the harness proves no compaction occurred**.

---

## 9. Logical capture points

The protocol is independent of vendor hook names.

### `beforeInvocation`

```text
capture/verify repository view
construct Context Pack
construct final ContextRender
append Context Exposure trace record
```

### `afterInvocation`

```text
capture model/provider
capture provider-reported usage
capture limits and their sources
capture finish status and duration
append Invocation Telemetry trace record
```

### `beforeTool` / `afterTool`

```text
bind to originating invocation record
record failure/truncation/result size
hash retained result bytes when useful
mark repository mutation
```

### `beforeContextCompaction` / `afterContextCompaction`

```text
record strategy/reason
record coarse dropped counts if known
```

### `before next invocation` / `afterWorkspaceMutation`

```text
materialize effective tree at an audit-relevant boundary
append beforeTree → afterTree when changed
```

---

## 10. Security, retention, and policy isolation

Provider envelopes and raw tool/result bodies SHOULD NOT be canonical trace events by default. They are high-volume, provider-specific, and likely to contain secrets or source.

All trace data follows repository access control, secret handling, redaction, and retention policy.

The audit trace MUST remain **policy-invisible**: losing, corrupting, or expiring optional trace detail may reduce auditability but MUST NOT retroactively change whether a source push, review, membership grant, or merge was authorized.

---

## 11. Product surface

Useful read surfaces include:

```text
git+ session show <session>
git+ trace show <session>
git+ context audit <operation-or-trace-record>
```

A product may project:

```text
repository view/tree
Context Pack and ContextRender
model/provider
input/output/cache tokens
window and effective input limits with sources
retries/fallbacks
context lifecycle events
tool failures/truncation
workspace transitions
resulting commits/refs
trace capability / actual coverage
```

Derived values MUST be labeled derived.

---

## 12. Recommended V1 profile

Start with:

```text
1. Context Exposure records in refs/hub/trace/<session>
2. provider-reported input/output/cache usage
3. contextWindowTokens and/or effectiveInputLimitTokens with explicit source
4. retries/fallbacks as distinct invocations
5. compaction/truncation events when observable
6. compact tool failure/truncation diagnostics
7. beforeTree → afterTree at meaningful repository boundaries
8. capture capability plus detectable trace-loss markers
```

---

## Final invariant

> **Invocation Telemetry is signed runtime provenance in a sibling audit trace, not policy-critical session history. It records harness-observed and provider-reported conditions, references canonical events by immutable Git record identity, distinguishes capability from actual coverage, and keeps token/window semantics explicit. It can explain where context may have been lost without claiming model cognition or causation.**
