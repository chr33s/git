# Git-Native Telemetry

**Status:** Draft specification and architecture note  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Revision:** draft-7

## 1. Purpose

Git+ records the observable runtime conditions around agent work without turning Git into a general observability backend.

Users inspect one **Invocation**:

```text
Invocation
  Context
    exact Repository View
    Context Pack
    ContextRender commitment

  Runtime
    model/provider
    usage
    outcome / finish reason
    retries/attempts when observed

  Operations
    tools / agents / retrieval diagnostics
    workspace transitions
    context lifecycle

  Capture
    OTel correlation
    coverage / known loss
```

The durable trace keeps pre-call Context Exposure and post-call runtime telemetry as separate signed records because they happen at different times and fail independently. The server and UI join them into the Invocation projection.

[Context Packs](context-pack.md) define exact Git-grounded repository exposure. This document defines runtime semantics, capture, signed trace storage, and the Invocation projection.

Nothing here proves model attention, understanding, memory, reasoning, or causation.

---

## 2. Three-plane product model

Git+ presents three product planes:

```text
WORK
  tasks / PRs / sessions / decisions

KNOWLEDGE
  Knowledge Concepts / Repository Memory

AUDIT
  Invocations
```

The audit plane is backed by two refs with different cost and policy roles:

```text
refs/hub/session/<session-id>
  distilled policy-visible work provenance

refs/hub/trace/<session-id>
  detailed policy-invisible audit provenance
```

The trace may contain:

```text
Context Exposure
Invocation Telemetry
selected agent/tool operations
workspace transitions
context lifecycle
trace health / known-loss markers
```

High-cardinality trace data MUST NOT participate in authorization, membership, protected-branch policy, mergeability, or `requireProvenance` checks.

---

## 3. Identity and causal joins

Each canonical trace record is a signed Git record commit.

Later canonical records MUST reference earlier canonical records by qualified Git commit OID:

```text
sha1:<hex>
sha256:<hex>
```

Example:

```json
{
  "type": "invocation-telemetry",
  "exposure": "sha1:abc123..."
}
```

OTel `TraceId`/`SpanId`, provider request IDs, conversation IDs, and harness event IDs are correlation metadata. They are not durable Git+ record identity.

Human-facing CLI input MAY accept ordinary Git revisions or unambiguous abbreviated OIDs; serialized records use qualified OIDs.

Trace history may be a DAG. Implementations MUST preserve causal parentage and MUST NOT infer causation from timestamps alone.

---

## 4. OpenTelemetry GenAI ingestion

Harness-native OpenTelemetry is the preferred runtime semantic interface:

```text
agent / harness
  native OTel GenAI spans + events/logs
        │
        ├──────────────→ normal observability backend
        │                 sampling / transforms allowed
        │
        ↓
Git+ audit ingest
  interpret declared semconv
  normalize selected facts
  sign + append
        ↓
refs/hub/trace/<session>
```

Hooks or embedded callbacks remain fallback capture paths when native OTel does not expose the needed boundaries.

### 4.1 Semconv interpretation contract

When an incoming signal claims OpenTelemetry GenAI semantic-convention compatibility, Git+ MUST interpret it according to the declared upstream version/revision.

Git+ MAY normalize field names but MUST NOT change upstream semantic meaning.

> **OTel defines what the incoming signal means. Git+ defines how selected audit facts are stored.**

Capture metadata SHOULD retain the mapping profile/revision:

```json
{
  "capture": {
    "transport": "otel",
    "stage": "sdk-export",
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7",
    "semconv": {
      "profile": "open-telemetry/semantic-conventions-genai",
      "revision": "<upstream revision>"
    }
  }
}
```

If no convention revision is known, Git+ MAY use a documented best-effort mapping but MUST NOT claim strict semconv adherence for that signal.

### 4.2 Stable Git+ schema

The durable schema stays independent of evolving upstream attribute names:

```text
OTel GenAI semconv
provider/custom attributes
hook fallback
        ↓
versioned normalization
        ↓
stable Git+ audit records
```

Historical Git+ records must remain interpretable without replaying the original OTel pipeline.

---

## 5. Evidence classes

Runtime values have three evidence classes:

```text
observed
  measured directly at the harness/Git+ boundary

reported
  supplied by provider, tool, runtime, or upstream instrumentation

derived
  computed from observed/reported values
```

OTel transport does not upgrade an evidence class.

Examples:

```text
ContextRender byte length       observed
harness wall-clock duration     observed
provider token count            reported
provider response model         reported
input pressure ratio            derived
estimated cost                  derived
```

Products MUST NOT silently present reported or derived values as observed facts.

---

## 6. Logical invocations and attempts

### 6.1 One inference span = one logical Invocation

A compliant GenAI inference span represents one logical inference operation:

```text
one compliant inference span
        ↓
one logical Invocation Telemetry record
```

Automatic transient retries inside that span MUST NOT be split into invented logical Invocations.

### 6.2 Provider attempts are subordinate

When instrumentation exposes attempt detail, Git+ MAY attach it beneath the logical Invocation:

```text
Invocation
  attempt 1 · timeout
  attempt 2 · success
```

Attempt count/status MUST NOT be inferred from duration, timestamp gaps, or missing response fields.

If attempt-level instrumentation is unavailable, omit attempts or report them as unknown.

### 6.3 Fallbacks follow upstream boundaries

If provider/model fallback is represented by another inference span, Git+ creates another logical Invocation. If upstream keeps the fallback inside one logical span, Git+ keeps one Invocation and MAY attach observed attempt/fallback detail.

---

## 7. GenAI normalization

### 7.1 Inference fields

```text
gen_ai.operation.name          → operation.name
gen_ai.provider.name           → model.provider
gen_ai.request.model           → model.requested
gen_ai.response.model          → model.response
gen_ai.response.finish_reasons → response.finishReasons
error.type                     → outcome.errorType
span status                    → outcome.status
gen_ai.conversation.compacted  → context.compacted
```

Requested and response model identifiers remain distinct. If only one is known, omit the other instead of copying it.

### 7.2 Usage

When present:

```text
gen_ai.usage.input_tokens              → usage.inputTokens
gen_ai.usage.output_tokens             → usage.outputTokens
gen_ai.usage.cache_read.input_tokens   → usage.cacheReadInputTokens
gen_ai.usage.cache_write.input_tokens  → usage.cacheWriteInputTokens
gen_ai.usage.reasoning.output_tokens   → usage.reasoningOutputTokens
```

Provider usage remains `reported` / `source: provider` after OTel transport.

Estimated usage MUST be labeled estimated and SHOULD carry an estimator identifier.

Whole-invocation `inputTokens` MUST NOT be described as repository ContextRender tokens.

### 7.3 Outcome and finish reason

Operation outcome and generation finish are separate:

```text
outcome.status
  unset | ok | error

outcome.errorType
  timeout or another low-cardinality error class

response.finishReasons
  model/provider generation stop reasons
```

A generation that ends because of a length limit can still be a successful operation. Git+ MUST NOT collapse finish reasons, OTel span status, and `error.type` into one field.

### 7.4 Agent operations

When present:

```text
gen_ai.operation.name   → operation.name
gen_ai.agent.id         → agent.id
gen_ai.agent.name       → agent.name
gen_ai.agent.version    → agent.version
gen_ai.conversation.id  → conversation.externalId
```

`conversation.externalId` is correlation metadata and MUST NOT replace Git+ session identity.

### 7.5 Tool operations

For `gen_ai.operation.name = execute_tool`:

```text
gen_ai.tool.name        → tool.name
gen_ai.tool.call.id     → tool.callId
gen_ai.tool.type        → tool.type
gen_ai.tool.description → tool.description
```

Raw tool arguments/results are often sensitive and SHOULD NOT be canonical by default.

Useful durable tool summaries include:

```text
name / type / call id
status / error class
result byte length
digest when a body is retained elsewhere
truncation flag
repository mutation summary
```

### 7.6 Retrieval operations

A GenAI retrieval span MAY become selector/retrieval diagnostics.

It MUST NOT be treated as proof that retrieved material crossed the invocation boundary. Context Pack / Context Exposure answer that question.

### 7.7 Events and logs

Correlated OTel events/logs MAY contribute point-in-time facts such as:

```text
context compaction
context truncation
workspace transition
known retry/fallback detail
capture/export health
```

Raw prompts, model outputs, complete tool bodies, and stream chunks SHOULD remain opt-in and normally non-canonical.

### 7.8 Metrics

OTel metrics are useful for fleet dashboards but MUST NOT be the sole source for reconstructing one historical Invocation because aggregation loses event identity and causality.

---

## 8. Invocation Telemetry record

A normalized logical runtime record may be:

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
      "revision": "<upstream revision>"
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
    "cacheReadInputTokens": 90210
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
    "effectiveInputLimitTokens": 180000,
    "effectiveInputLimitSource": "harness-config"
  }
}
```

Unknown values SHOULD be omitted instead of invented as zeros or defaults.

---

## 9. Context limits and pressure

Two limits are distinct Git+ extensions:

```text
contextWindowTokens
  total sequence/context capacity believed to apply

effectiveInputLimitTokens
  maximum input budget believed usable for this invocation
  after reserved output or other runtime constraints
```

Each SHOULD identify its source:

```text
provider
model-catalog
harness-config
runtime
other
```

Derived pressure is meaningful only when numerator and denominator use compatible semantics.

Preferred diagnostic when available:

```text
inputPressure = inputTokens / effectiveInputLimitTokens
```

If the denominator is unknown or incompatible, omit the ratio.

`context.renderBytes` is harness-observed byte size and is tokenizer-independent.

---

## 10. Context lifecycle

When `gen_ai.conversation.compacted = true`, the Invocation projection can state:

```text
context: compacted prior conversation
```

That boolean does not prove when or how compaction happened.

A separate `context-compaction` trace record SHOULD exist only when the harness directly observes a transition with additional facts such as strategy or reason.

Likewise, emit truncation events only when directly observable. Do not infer them from absent content.

---

## 11. Workspace transitions

Git tree identity is authoritative for repository workspace state.

A boundary-driven capture pattern is:

```text
before auditable work
  remember tree A

writes happen
  mark workspace dirty

before next auditable invocation
  materialize tree B
  record A → B
  next Context Pack uses B
```

An OTel tool span may identify the operation associated with mutation, but Git tree OIDs are the durable workspace identity.

A workspace transition SHOULD record only what is needed for audit:

```json
{
  "type": "workspace-transition",
  "beforeTree": "sha1:...",
  "afterTree": "sha1:...",
  "operation": "sha1:<tool-record>"
}
```

---

## 12. Loss-intolerant audit capture

Normal OTel pipelines may sample, filter, redact, aggregate, transform, or drop telemetry.

A deployment claiming durable per-Invocation audit SHOULD use a dedicated path:

```text
                    ┌── Git+ audit ingest
harness → OTel SDK ─┤   no intentional lossy sampling of audit classes
                    │
                    └── normal observability export
                        sampling/filtering/aggregation allowed
```

Prefer capture at or near SDK export before arbitrary Collector processors.

Known capture stages include:

```text
sdk-export
local-collector
remote-collector
hook
embedded
other
```

If audit-relevant signals were sampled, filtered, or transformed before Git+ ingestion, the affected capture MUST NOT be presented as complete.

### 12.1 Trace health

Where detectable, record known loss/capture health:

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

Useful loss signals include:

```text
export queue overflow
batch dropped
receiver unavailable
collector sampling enabled
processor filtered audit class
```

Absence of an event is meaningful only alongside known capture capability and trace health.

### 12.2 No Baggage authority

OTel Baggage MUST NOT carry integrity or authority for:

```text
repository identity
member identity
capabilities
instruction authority
Context Pack identity
policy decisions
```

Baggage may contribute descriptive correlation only after validation.

---

## 13. Retention and volume

The Git trace is an audit index, not a second OTel backend.

Usually worth retaining canonically:

```text
Context Exposure
logical Invocation Telemetry
selected agent/tool operations
workspace transitions when audit-relevant
context lifecycle changes when observed
trace-health / known-loss markers
```

Usually external-only or short-lived:

```text
every successful read/grep
raw provider request/response
raw prompts/outputs
raw tool arguments/results
stream chunks
high-cardinality timing detail
fleet metrics
```

High-volume detail MAY be batched or stored externally. Context Exposure and Invocation Telemetry SHOULD normally remain standalone signed records so their commit OIDs are directly referenceable.

If a producer batches records, each entry needs an immutable locator:

```json
{
  "record": "sha1:abc123...",
  "index": 7
}
```

Trace storage has its own host-defined resource bounds and must not inherit the policy-critical session fold's budget.

---

### 13.1 Known cost: the pre-call redaction lookup

`context for --session` asks `hub/Redaction.removed()` which blobs a counted,
bound tombstone names, so that a deterministic render cannot put removed bytes
back into the object store. That answer is computed by walking every record on
every session, task and trace ref. Each `context for` is a fresh process, so
the in-process per-ref memo is always cold, and a trace ref gains a record per
model invocation — the cost of invocation N therefore grows with every record
written before it.

Measured against the in-memory store, ten records per session, after the
per-record read reduction in `trust/Record.payloadOf`:

| total trace records | time per `context for --session` |
| ------------------- | -------------------------------- |
| 200                 | 5.6 ms                           |
| 400                 | 10.4 ms                          |
| 800                 | 20.9 ms                          |

Linear, as the walk is. Extrapolated, a repository holding 20 000 trace records
— a hundred sessions at two hundred invocations each — costs about half a
second on the path that runs before every call. A filesystem store is slower
per object than the one measured.

`hub/Redaction.Answers` is the port for keeping that answer across a process
boundary — the shape `git/Search.SearchIndex` already uses for derived state,
so that where it is kept stays a deployment decision. It is asked for rather
than required, so a host that provides no layer is unaffected and no caller of
this module gains an opinion about durable state.

It keeps one entry per _ref_, and that is what makes it work at all. A single
repository-wide answer keyed on every record ref's head can never be read by
this workload: `context for --session S` asks, and then appends to
`refs/hub/trace/S`, so the next invocation computes a different key and misses
every time while still paying to write. Per ref, an append invalidates the one
ref it moved and the rest are read instead of walked, which is the part that
grows with the repository.

It is also the level with no trust in it. Whether a tombstone _counts_ is
folded fresh against the trust log on every read, because a grant arriving
after the tombstone it authorises moves no record ref — a kept answer that
included that judgement would stand while the removal it should have honoured
went unhonoured. What is kept is what the walk found: the target, whether it
binds to this ref and repository, and who signed it.

The key names the storage identity, the genesis, the namespace's ceiling, the
ref and its head, which is everything the kept part depends on.

No layer at all is the behaviour measured above, and what a host with nowhere
durable to write gets. The CLI provides `Redaction.node.beside`, which keeps
the answers in
`<git-dir>/gitplus/redaction.json`: a file rather than a ref, because a ref
would have to be hidden from the advertisement, cleaned up by `hub disable`
and kept out of `gc`'s reachability, and none of those questions have anything
to do with the answer. Git keeps its own derived state as files in the same
place for the same reason, and treats every one of them as disposable. So does
this: delete the file and the next run recomputes.

An implementation MUST fail closed. `read` answers `null` for anything it is
not certain of: a key it does not hold, a store it cannot read, a value it
cannot parse. Answering "nothing was removed" instead re-opens the
resurrection this lookup exists to prevent — the next identical `context for`
retains a render whose bytes an operator removed, under the same oid, where
the redacted record's surviving tree entry resolves them — and it does so
silently, because a retained render is the ordinary case and raises no notice.

Only a _whole_ answer is offered to the port. `marks` folds completeness over
every record ref, and a short walk is never written: the refs do not move when
the missing objects arrive, so a kept subset would never recover.

The alternative is to drop the pre-call check and rely on the audit refusing to
present a record a tombstone names (§14), which is cheaper and leaves removed
bytes able to re-enter the object store.

## 14. Invocation projection

The server owns trust verification, DAG folding, semconv normalization, joins, redaction state, coverage, and derived diagnostics.

The browser and CLI receive a projected Invocation; they do not rebuild protocol state themselves.

```text
Invocation sha1:c3…

Context
  tree      79ad…
  evidence  7 blobs · 1 gitlink
  render    ✓ verified

Runtime
  chat · anthropic
  requested model-x
  response  model-x-20260815
  usage     118k input · 90k cache-read · 4.2k output
  finish    stop

Attempts
  1 timeout
  2 success

Workspace
  79ad… → a130…

Capture
  OTel GenAI · sdk-export
  coverage complete
```

Omit the Attempts section when attempts were not explicitly observed.

When request and response model are equal, the UI may show one model label. When they differ, show both.

Normal finish reasons such as `length` must not be styled as provider errors unless `outcome.status` is also an error.

---

## 15. Session and Flight Recorder UX

The normal audit workflow is session-centric:

```bash
git+ session show <session>
git+ session show <session> --audit
```

`--audit` joins the policy-visible session projection with its policy-invisible Invocation history.

Branch lookup is also supported:

```bash
git+ session show --branch=<branch> --audit
```

The Flight Recorder presents Invocation rows and preserves concurrent trace lanes when the history is a DAG. It MUST NOT manufacture a single causal order from timestamps when concurrent parents exist.

Example:

```text
Context Exposure
  tests/auth.test.ts present
        ↓
Invocation
  96% effective input pressure
        ↓
context compaction observed/reported
        ↓
next Invocation
  tests/auth.test.ts absent
        ↓
workspace transition
        ↓
commit
```

OTel plumbing remains available in expert detail, but the primary UI vocabulary is Work / Knowledge / Audit and Invocation.

### 15.1 External OTel links

If an external observability backend is configured, an Invocation MAY expose an **Open trace** link.

Git+ audit pages must remain useful after external telemetry is sampled out or expires.

---

## 16. Harness plumbing

Raw trace writing is an integration surface, not a normal human workflow.

A fallback harness without suitable OTel may use:

```text
git+ trace record \
  --session=<session-id> \
  --key=<private-key> \
  --event=<event.json>
```

The command follows the shared repository-discovery convention: the current checkout is the default, and explicit repository selection remains available for bare/server use.

The recorder validates the normalized event, applies retention/secret policy, binds repository/session identity, signs it, appends under `refs/hub/trace/<session>`, and returns the qualified Git record OID.

Native OTel ingestion should use the same trace writer without requiring one shell command per span/event.

There is no normal `git+ trace show` workflow; `session show --audit` is the product-level projection.

---

## 17. Repository discovery and CLI identity

Repo-scoped audit commands SHOULD discover the current checkout by default, matching ordinary Git ergonomics.

Explicit repository/root selection remains available for bare/server use.

```bash
cd project

git+ session show --branch=refs/heads/feature/auth --audit
git+ context audit abc123
git+ knowledge check
```

CLI inputs MAY accept ordinary revisions, refs, and unambiguous abbreviated OIDs. Canonical records always serialize qualified OIDs.

---

## 18. Security

- Trace content may contain sensitive runtime metadata and follows repository access/redaction/retention policy.
- Raw prompts, outputs, and tool bodies are non-canonical by default.
- OTel identifiers and Baggage do not create repository authority.
- Weak or partial runtime capture does not invalidate otherwise valid Git repository evidence.
- Valid Git evidence does not imply complete runtime capture.
- Imported or reported runtime metadata does not prove cognition or causation.

---

## 19. Acceptance criteria

Telemetry is successful when:

1. a compliant OTel GenAI inference span maps to one logical Invocation;
2. upstream semconv meaning is preserved through normalization;
3. automatic retries are not invented as logical Invocations;
4. attempts are shown only when explicitly observed;
5. requested and response models remain distinct;
6. finish reasons remain distinct from runtime errors;
7. provider usage remains reported, not observed;
8. Context Exposure remains the authoritative repository-context boundary;
9. OTel retrieval spans never substitute for Context Exposure;
10. audit ingest can remain independent of lossy observability pipelines;
11. known sampling, transformation, or loss weakens coverage claims;
12. Git record OIDs remain canonical audit identity;
13. high-volume trace data never enters policy-critical session folds;
14. the normal product UX presents one Invocation without requiring users to understand separate pre/post trace records;
15. `session show --audit` and the Flight Recorder preserve causal DAG structure;
16. external OTel retention is not required to inspect durable Git+ audit history.
