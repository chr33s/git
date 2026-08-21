# Git-Native Telemetry Integration and UI

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-22  
**Revision:** draft-2

## 1. Summary

This document describes how [Invocation Telemetry](invocation-telemetry.md) and [Context Packs](content-pack.md) can be captured by coding-agent harnesses, recorded by `git+`, projected by the hub, and surfaced in the UI.

The protocol specs define the facts and identities. This document defines a non-normative implementation/product shape.

```text
agent harness
     │
     │ logical capture points
     ↓
harness-specific adapter
     │
     │ normalized event payloads
     ↓
git+ trace recorder
     │
     │ validate · bind · sign · append
     ↓
refs/hub/trace/<session-id>
     │
     │ server projection
     ↓
/hub/sessions
/hub/sessions/:id
     │
     ↓
Activity · Change Requests · Commits · Flight Recorder
```

The policy-critical session DAG remains separate:

```text
refs/hub/session/<session-id>
  distilled prompt / produced result / decisions / aggregate usage
  may be consulted by provenance or policy

refs/hub/trace/<session-id>
  detailed context exposure / invocation / lifecycle / tool / workspace audit
  never consulted for authorization or merge policy
```

The product goal is not a generic observability dashboard. It is to explain repository-agent failures in repository terms.

---

## 2. Harness integration modes

### 2.1 Embedded integration

The strongest integration wraps the actual model/tool loop:

```text
beforeInvocation
  capture current Repository View
  build Context Pack
  build semantically framed ContextRender
  append Context Exposure trace record

provider.invoke(...)

afterInvocation
  capture provider/model
  capture usage and limit sources
  capture finish status/duration
  append Invocation Telemetry trace record

beforeTool / afterTool
  record compact diagnostics
  mark repository mutation

before next invocation
  materialize effective tree if dirty
  append Workspace Transition if changed
```

### 2.2 Vendor-hook integration

A vendor adapter maps whatever hooks exist to the same logical capture points:

```text
vendor event
    ↓
.chr33s/telemetry.mjs
    ↓ normalize only observed values
    ↓
git+ trace record
```

Vendor hook names MUST remain adapter details.

A hook integration may have partial visibility. That is valid and must be surfaced honestly.

---

## 3. Machine-facing recorder

Use a generic trace recorder rather than one CLI verb per event kind:

```text
git+ trace record <repo> \
  --session <session-id> \
  --key <private-key> \
  --event <event.json>
```

The command should:

```text
read event
  ↓
schema validate
  ↓
enforce payload/secret rules
  ↓
bind repository + session
  ↓
sign with member key
  ↓
append to refs/hub/trace/<session-id>
```

The adapter only:

```text
reads vendor payload
extracts values it actually observes/receives
labels evidence source correctly
calls git+
```

### 3.1 Immutable references

When the recorder appends a canonical trace event, it SHOULD print the qualified record commit OID so the adapter can bind later events:

```text
sha1:abc123...
```

A later Invocation Telemetry event uses that commit OID as `exposure`. Retry/fallback records use prior invocation record OIDs.

Do not use a UUID-like event ID as the only cross-record identity.

---

## 4. Trace volume and batching

The audit trace is separate specifically so high-frequency observability does not make policy folds expensive.

Even there, raw tool chatter should not produce unlimited tiny Git commits.

Recommended shape:

```text
standalone canonical records
  Context Exposure
  Invocation Telemetry
  Workspace Transition when audit-relevant
  lifecycle events when audit-relevant

batched/side telemetry
  repetitive successful tool calls
  raw tool bodies
  provider envelopes
  verbose execution traces
```

The trace implementation SHOULD have its own linear/DAG fold and resource ceiling appropriate to audit data. It MUST NOT reuse a policy-path ceiling merely for convenience.

---

## 5. Capture capability and trace health

At adapter/session start, record what the integration is capable of observing:

```json
{
  "visibility": {
    "integration": "claude-code-hooks",
    "capabilities": ["session", "tools", "workspace"]
  }
}
```

This is **capability**, not proof of complete capture.

Where possible, adapters SHOULD emit detectable trace health:

```json
{
  "type": "trace-health",
  "source": "claude-code-hooks",
  "sequence": 42,
  "dropped": 0
}
```

The server can project each class as:

```text
available
partial
unknown
```

Only integrations with a defined completeness mechanism should claim `complete`.

---

## 6. Event normalization

Adapters SHOULD NOT:

- retokenize and label the result provider-reported;
- infer a context limit from an unpinned lookup;
- fabricate model revisions;
- claim successful tool output entered a model invocation without exposure evidence;
- convert lack of a compaction hook into "no compaction happened";
- erase the distinction between total context window and effective input limit.

For token/window fields, preserve both semantics and source:

```json
{
  "contextWindowTokens": 200000,
  "contextWindowSource": "provider",
  "effectiveInputLimitTokens": 180000,
  "effectiveInputLimitSource": "harness-config"
}
```

---

## 7. Workspace capture should be lazy

Do not write an overlay tree after every filesystem syscall.

```text
edit tool begins
  remember tree A

edit tool completes
  mark workspace dirty

before next auditable invocation
  materialize tree B
  append A → B
  use B as next Context Pack view.tree
```

The invariant is:

> When repository state matters for an auditable invocation, there is an exact effective Git tree for that boundary.

---

## 8. ContextRender capture

The harness owns the ContextRender boundary.

The renderer produces ordered segments with logical placement, media type, and exact bytes. The provider adapter maps those segments into provider fields without changing order, placement semantics, or bytes.

If an adapter changes any of those, it must request or construct a new final ContextRender and digest before invocation.

This avoids the subtle failure where identical repository bytes are recorded but moved from a developer/system-equivalent channel into a user/tool channel.

---

## 9. Server projection

The browser should not reconstruct session/trace DAG semantics or trust state.

### 9.1 Session listing

Keep `/hub/sessions` cheap. It may include a derived runtime summary when trace data exists:

```json
{
  "session": "0198f2aa-...",
  "agent": { "kind": "claude-code", "model": "model-x", "harness": "2.x" },
  "commits": 3,
  "usage": { "inputTokens": 118420, "outputTokens": 4281 },
  "telemetry": {
    "invocations": 18,
    "peakInputShare": 0.91,
    "peakInputPressure": 0.96,
    "peakInvocation": "sha1:abc123...",
    "compactions": 2,
    "retries": 1,
    "toolFailures": 1,
    "coverage": "partial"
  }
}
```

`peakInputPressure` should exist only when a compatible effective input limit is known. If a session changed models/limits, retain the invocation identity that produced each peak rather than pairing a session-wide token number with the wrong denominator.

### 9.2 Session detail

A detail endpoint should project the session plus its sibling audit trace:

```text
GET /hub/sessions/:id
```

Conceptually:

```json
{
  "session": "0198f2aa-...",
  "visibility": { "tools": "available", "lifecycle": "unknown" },
  "events": [
    { "type": "session.opened", "source": "session" },
    { "type": "context-exposure", "source": "trace" },
    { "type": "invocation-telemetry", "source": "trace" },
    { "type": "workspace-transition", "source": "trace" },
    { "type": "session.produced", "source": "session" }
  ]
}
```

The server owns:

- trust/signature verification;
- session and trace folding;
- redaction state;
- causal parentage;
- joins between exposures, invocations, tools, transitions, commits, and refs;
- derived diagnostics.

The browser owns presentation.

---

## 10. UI principle

The product SHOULD lead with repository-context transitions, not provider metrics.

```text
tests/auth.test.ts exposed
      ↓
input pressure reached 96%
      ↓
context compaction recorded
      ↓
tests/auth.test.ts absent from next exposure
      ↓
edit to src/auth.ts
```

That is more useful than a generic token dashboard.

---

## 11. Activity session rows

Keep the row compact:

```text
0198f2aa  claude-code · model-x
3 commits · 124k tokens · 61% window share · ✓ context
```

When effective input pressure is known:

```text
0198f2bb  codex · model-y
284k tokens · 96% input pressure · 3 compactions · 1 retry · ⚠ context
```

Missing telemetry must not render as zero.

---

## 12. Session Flight Recorder

A session detail should present causal events in human terms:

```text
Session 0198f2aa
Claude Code · model-x
────────────────────────────────────────

09:42  Prompt
       Fix authentication policy...

09:42  Context Exposure
       tree 79ad…
       7 blob items · 1 gitlink · 31 KB
       placements: developer, tool
       ✓ evidence + render verified

09:42  Invocation
       model-x
       118k input
       59% of total context window
       90k cached · 4.2k output

09:43  Tool group
       14 operations · 1 failure

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

### 12.1 Preserve concurrency

The session/trace history may be a DAG, not a single total causal line.

When two records are concurrent or a session was resumed on diverging heads, the Flight Recorder SHOULD show lanes/branches or another explicit concurrency affordance. Wall-clock timestamps may help presentation but MUST NOT be used to invent causal parentage.

---

## 13. Context Pack diffs

Adjacent exposures can show:

```text
Context #17 → #18

Added
+ tests/auth.test.ts
+ src/policy.ts

Removed
- docs/design.md
```

A clicked blob opens the exact historical blob/range. A clicked gitlink shows the recorded submodule commit pointer, not an implied submodule file view.

---

## 14. Provenance status

Report evidence components separately:

```text
Repository evidence  ✓ verified
View reachability    ✓ retained through trace record
Context render       ✓ retained and digest verified
Runtime usage        provider reported
Trace visibility     partial
Tool body             expired
Workspace transition ✓ matched pre-operation tree
```

Avoid ambiguous historical labels such as:

```text
Latest repository tree ✓ current
```

A historical invocation should instead be judged against the repository/workspace tree recorded at **that invocation boundary**.

Useful states:

```text
✓ verified
⚠ body unavailable, digest retained
✕ path/blob mismatch
✕ gitlink mode/commit mismatch
⚠ invocation used tree A after recorded A → B transition
⚠ visibility unknown for this event class
```

---

## 15. Tool operations

Collapse successful tool chatter by default:

```text
▸ 14 tool operations
    12 successful
     1 truncated
     1 failed
```

Raw bodies appear only when a retained side object exists, access policy allows it, and the viewer explicitly requests it.

---

## 16. Retry/fallback visualization

Show distinct attempts and immutable references:

```text
Invocation sha1:a1… · model-x
        │ timeout
        ↓ retry
Invocation sha1:b2… · model-x
        │ provider error
        ↓ fallback
Invocation sha1:c3… · model-y
        ✓ success
```

---

## 17. Audit summary

A derived summary may show:

```text
Agent session audit

Context provenance      ✓ 18 / 18 verified
Workspace alignment     ✓ matched at recorded invocation boundaries
Peak input pressure     ⚠ 96% · invocation sha1:c3…
Compactions             2
Retries                 1
Failed tools            1
Trace visibility        partial
Knowledge continuity    ⚠ test removed after recorded compaction
```

Derived diagnosis MUST use observable language.

Prefer:

> `tests/auth.test.ts` was present in Exposure A and absent after a recorded compaction before Exposure B.

Avoid:

> The model forgot the test.

---

## 18. Commit and Change Request surfaces

Link repository artifacts back to the Flight Recorder.

A commit may show telemetry only when there is a **causal binding**, not merely because an invocation timestamp was nearby.

Prefer:

```text
Produced by session 0198f2aa
Causally bound preceding invocation: sha1:c3…
  model-y
  96% input pressure
  Context Pack: 12 blobs · 1 gitlink
```

If no explicit/derivable causal binding exists, omit "last invocation before commit" rather than infer it from wall-clock ordering.

---

## 19. Knowledge durability UI

Repository Memory should display cited claims and structured evidence dependencies separately:

```text
gotcha  Worker auth tests require production policy fixture
source  session 0198f2aa
support 2 blobs · 1 gitlink
state   ⚠ config/policy.json changed; revalidate
```

A changed dependency is not automatic falsification.

See [knowledge-durability.md](knowledge-durability.md).

---

## 20. Implementation sequence

### Phase 1 — existing session summary

Use the current aggregate `session.produced.usage` in Activity. Do not invent window pressure or trace completeness before those measurements exist.

### Phase 2 — trace recorder

Add:

```text
git+ trace record
refs/hub/trace/<session>
```

with schemas for exposure, invocation telemetry, lifecycle, tool summary, trace health, and workspace transitions.

### Phase 3 — trace-derived summaries

Extend `/hub/sessions` with model/window/pressure/retry/visibility summaries derived from actual trace records.

### Phase 4 — session detail projection

Add `GET /hub/sessions/:id` joining the small session DAG with the policy-invisible trace.

### Phase 5 — Flight Recorder and Context Pack integration

Add context diffs, semantic placements, retries, tool groups, workspace transitions, reachability checks, and evidence links.

This order avoids presenting UI metrics before the system can capture them.

---

## 21. Documentation hierarchy

```text
agents.md
  membership + existing session lifecycle

knowledge-durability.md
  Capture → Retention → Recall objective

content-pack.md
  repository + exposure protocol

invocation-telemetry.md
  runtime audit-trace protocol

telemetry-integration.md
  this harness/API/UI guidance
```

Architecture notes should link to protocol specs instead of duplicating normative wire rules.

---

## 22. Acceptance criteria

The integration is useful when:

1. high-frequency telemetry never increases policy-critical session fold cost;
2. adapters can record trace events without knowing Git signing/ref internals;
3. cross-event joins use immutable record identity;
4. capability is distinct from actual trace coverage/loss;
5. token-window metrics preserve denominator semantics and source;
6. provider-reported values remain distinct from estimates/derived values;
7. workspace trees are captured lazily at meaningful boundaries;
8. ContextRender placement changes cannot occur outside the recorded digest artifact;
9. the server, not the browser, folds DAG/trust/redaction semantics;
10. concurrent records are not falsely linearized as causal;
11. historical tree status is judged against the recorded boundary, not today's branch tip;
12. commits/CRs link to telemetry only through causal provenance;
13. raw side telemetry can expire without invalidating canonical provenance;
14. structured Memory evidence can surface "needs revalidation" without claiming truth/falsity;
15. the UI explains repository-context change without claiming model cognition.

---

## Final principle

> **Harnesses capture what they can observe; `git+` stores high-frequency audit facts in a signed policy-invisible trace while keeping the session DAG distilled; the hub projects causal repository/context/runtime evidence; and the UI explains how repository knowledge changed before code was produced. Telemetry is most valuable when it connects context to repository operations, not when it merely counts tokens.**
