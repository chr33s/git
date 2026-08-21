# Git-Native Telemetry Integration and UI

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-21

## 1. Summary

This document describes how [Invocation Telemetry](invocation-telemetry.md) can be captured by coding-agent harnesses, recorded by `git+`, projected by the hub, and surfaced in the product UI.

The protocol specification defines the facts worth recording. This document describes the integration shape.

The intended architecture is:

```text
agent harness
     │
     │ logical capture points
     ↓
harness-specific adapter
     │
     │ normalized event payloads
     ↓
git+ session recorder
     │
     │ validate · bind · sign · append
     ↓
refs/hub/session/<session-id>
     │
     │ server projection
     ↓
/hub/sessions
/hub/sessions/:id
     │
     ↓
Activity · Change Requests · Commits · Flight Recorder
```

The product goal is not to build a generic observability dashboard.

It is to make repository-agent failures explainable in repository terms:

```text
relevant test was present
    ↓
context pressure increased
    ↓
compaction occurred
    ↓
test disappeared from the next exposure
    ↓
agent edited related code
```

The UI should help a human answer:

> **What happened to the agent's repository knowledge before this operation?**

without requiring inspection of raw session JSON or provider transcripts.

---

## 2. Relationship to the existing harness integration

`git+ session enable` already establishes the basic integration pattern:

1. write an inspectable script under `.chr33s/`;
2. install harness hooks that call it;
3. normalize harness input into `git+ session` commands;
4. sign resulting events with the agent's repository member key;
5. append them to the existing session DAG.

Telemetry SHOULD extend this pattern rather than introducing a separate daemon, trace service, or telemetry namespace.

A checkout may therefore contain:

```text
.chr33s/
  session.mjs
  telemetry.mjs
  session.id
```

An implementation MAY combine `session.mjs` and `telemetry.mjs` into one script. The conceptual split is useful because session lifecycle and fine-grained runtime capture have different availability across harnesses.

The generated adapter remains ordinary local code. An operator should be able to inspect, edit, replace, or remove it.

---

## 3. Two integration modes

### 3.1 Embedded harness integration

The strongest integration wraps the actual model and tool runtime.

It can observe the protocol's logical boundaries directly:

```text
beforeInvocation
  capture current repository view
  build Context Pack
  build ContextRender
  append Context Exposure Event

provider.invoke(...)

afterInvocation
  capture provider/model
  capture usage
  capture finish status
  capture duration
  append Invocation Telemetry

beforeTool / afterTool
  record compact tool diagnostics
  mark repository mutation

before next invocation
  materialize the resulting effective tree
  record Workspace Transition when needed
```

This mode can provide the highest telemetry coverage because it sees individual provider invocations, retries, fallbacks, context assembly, and tool execution before vendor abstractions hide them.

An embedded integration is preferred when `git+` is part of the harness itself or when a custom orchestrator already owns the model loop.

### 3.2 Vendor hook integration

Many agent products expose only selected lifecycle hooks.

A vendor adapter maps those hooks onto the same logical capture points:

```text
vendor event
    ↓
.chr33s/telemetry.mjs
    ↓
normalize
    ↓
git+ session record
```

The core session or telemetry protocol MUST NOT depend on names such as `PostToolUse`, `Stop`, `UserPromptSubmit`, or any equivalent vendor API.

Those are adapter details.

A hook-based integration may have partial visibility. For example, it may observe:

```text
session start        yes
tool completion      yes
workspace mutation   yes
provider token usage no
context compaction   no
per-model invocation no
```

Partial telemetry is valid. Missing runtime telemetry MUST NOT invalidate Context Packs, Context Exposure Events, session provenance, or resulting commits.

---

## 4. One machine-facing recording primitive

Harness adapters should not need a CLI verb for every telemetry event type.

A useful machine interface is:

```text
git+ session record <repo> \
  --session <session-id> \
  --key <private-key> \
  --event <event.json>
```

Example input:

```json
{
  "type": "invocation-telemetry",
  "exposure": "0198f2b0-...",
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
    "windowLimitTokens": 200000
  }
}
```

The command should perform the repository-specific work the adapter should not duplicate:

```text
read event
  ↓
schema validate
  ↓
enforce payload and secret rules
  ↓
bind repository and session
  ↓
sign with the member key
  ↓
append to refs/hub/session/<session-id>
```

The adapter's responsibilities should remain small:

```text
read vendor payload
extract observable values
label their provenance correctly
call git+
```

Friendly human-facing commands such as:

```text
git+ session telemetry <session>
git+ context audit <event>
```

may exist as read surfaces, but harnesses SHOULD use the stable generic recording interface.

---

## 5. Telemetry coverage is itself important context

Absence of an event has two possible meanings:

```text
the event did not happen

or

the integration could not observe it
```

Those MUST NOT be silently conflated.

A session SHOULD therefore expose its telemetry coverage when known.

One possible session-level declaration is:

```json
{
  "telemetry": {
    "integration": "claude-code-hooks",
    "captures": [
      "session",
      "tools",
      "workspace"
    ]
  }
}
```

A server projection may normalize that into capabilities such as:

```text
contextExposure       yes
invocationUsage       no
contextLifecycle      no
toolOperations        yes
workspaceTransitions  yes
```

The exact wire shape may evolve, but the product semantics are important:

> **No compaction event exists** is not equivalent to **the harness proves no compaction occurred**.

The UI SHOULD distinguish telemetry that is complete for a class from telemetry that is merely absent.

---

## 6. Event normalization

Harness adapters SHOULD normalize only values they actually observe or receive.

Examples:

```text
provider token count
  provider-reported

ContextRender byte length
  harness-observed

context utilization
  derived by reader

model revision
  omitted unless provider/runtime supplied one
```

Adapters SHOULD NOT:

- retokenize a provider request and label the result provider-reported;
- infer a context-window limit from an unpinned web lookup;
- fabricate a model revision from a model family name;
- describe successful tool output as having reached the next invocation unless the next Context Exposure Event supports that claim;
- convert lack of a lifecycle hook into a claim that no compaction occurred.

This keeps vendor integration adapters thin and keeps evidence classes meaningful.

---

## 7. Workspace tree capture should be lazy

Constructing an overlay Git tree after every low-level filesystem write is unnecessary and can create excessive object churn.

A better implementation is boundary-driven:

```text
edit tool begins
  remember effective tree A

edit tool completes
  mark workspace dirty

more local filesystem writes
  remain dirty

before next model invocation
  materialize effective tree B
  append Workspace Transition A → B
  use B as the next Context Pack view.tree
```

Other useful materialization boundaries include:

- before a commit is created;
- after checkout;
- after merge or rebase;
- before another repository-affecting model invocation.

The important invariant is not that every filesystem mutation has its own tree object.

It is:

```text
when repository state matters for an auditable invocation,
there is an exact effective Git tree for that boundary
```

---

## 8. Token usage capture

Token telemetry SHOULD be boring.

When the provider/runtime returns:

```json
{
  "inputTokens": 118420,
  "outputTokens": 4281,
  "cachedInputTokens": 90210
}
```

`git+` records those values as provider-reported facts.

It does not need to reproduce the provider tokenizer or prove the accounting.

If only an estimator is available, the event labels it estimated and records the estimator identity when useful.

The server or UI may later derive:

```text
inputTokens / windowLimitTokens
```

for context pressure.

Derived utilization SHOULD NOT be signed protocol state when the underlying measurements are already available.

---

## 9. Retry and fallback capture

A harness may perform several actual provider invocations before one agent response exists.

Those attempts remain distinct telemetry records:

```text
Invocation 8 · model A
  timeout
      ↓ retry
Invocation 9 · model A
  provider error
      ↓ fallback
Invocation 10 · model B
  success
```

A final response MUST NOT cause several behaviorally or billably distinct model calls to collapse into one record.

This is important both for cost analysis and for attribution of the context/model combination that actually preceded an agent operation.

---

## 10. Server projection

The browser should not reconstruct session DAG ordering or telemetry semantics itself.

The hub server should project canonical session history into product-oriented read models.

### 10.1 Session listing

The existing session listing should remain cheap.

A projected row may grow from aggregate usage into a small telemetry summary:

```json
{
  "session": "0198f2aa-...",
  "agent": {
    "kind": "claude-code",
    "model": "model-x",
    "harness": "2.x"
  },
  "commits": 3,
  "pulls": ["..."],
  "usage": {
    "inputTokens": 118420,
    "outputTokens": 4281
  },
  "telemetry": {
    "invocations": 18,
    "peakContextTokens": 191200,
    "windowLimitTokens": 200000,
    "compactions": 2,
    "retries": 1,
    "toolFailures": 1,
    "warnings": 1
  }
}
```

The projection MAY include derived summaries such as peak context pressure, but they should remain clearly product-derived rather than signed source events.

### 10.2 Session detail

A detail endpoint should expose the projected causal timeline:

```text
GET /hub/sessions/:id
```

Conceptually:

```json
{
  "session": "0198f2aa-...",
  "coverage": {
    "contextExposure": true,
    "invocationUsage": true,
    "contextLifecycle": true,
    "toolOperations": true,
    "workspaceTransitions": true
  },
  "events": [
    { "type": "session.prompted", "...": "..." },
    { "type": "context-exposure", "...": "..." },
    { "type": "invocation-telemetry", "...": "..." },
    { "type": "tool-telemetry", "...": "..." },
    { "type": "workspace-transition", "...": "..." }
  ]
}
```

The server owns:

- session DAG projection;
- ordering;
- redaction state;
- trust/signature acceptance;
- causal relationships;
- basic joins between exposure, invocation, tools, transitions, commits, and refs.

The browser owns presentation and non-authoritative derived diagnosis.

---

## 11. UI principle: telemetry explains repository knowledge changes

The product SHOULD NOT lead with provider metrics.

A generic dashboard such as:

```text
Tokens today
Average latency
Provider spend
```

may be useful operationally, but it is not the core Git+ product advantage.

The stronger experience is:

> **Show the chain between repository evidence, context lifecycle, runtime conditions, and code changes.**

For example:

```text
tests/auth.test.ts exposed
      ↓
96% context pressure
      ↓
context compaction
      ↓
tests/auth.test.ts absent
      ↓
edit to src/auth.ts
```

That turns telemetry into an explanation of agent context loss rather than another metrics surface.

---

## 12. Level 1 UI: Activity session rows

The Activity screen already has a natural Sessions section.

The list should remain compact and glanceable.

A healthy session might show:

```text
0198f2aa  claude-code · model-x
3 commits · 1 PR · 124k tokens · 61% peak · ✓ context
```

A session with relevant runtime events might show:

```text
0198f2bb  codex · model-y
284k tokens · 97% peak · 3 compactions · 1 retry · ⚠ context
```

The row SHOULD emphasize unusual or actionable signals rather than display every available field.

Useful row-level facts include:

```text
commits / PRs
aggregate tokens
peak context pressure
compaction count
retry/fallback count
failed tool count
context-provenance status
open decisions
```

If telemetry coverage is partial, the row should not render missing signals as zero.

---

## 13. Level 2 UI: Session Flight Recorder

Clicking a session should open the primary detailed telemetry experience: a causal event timeline.

Example:

```text
Session 0198f2aa
Claude Code · model-x
────────────────────────────────────────

09:42  Prompt
       Fix authentication policy...

09:42  Context
       tree 79ad…
       7 files · 31 KB
       ✓ all evidence verified

09:42  Invocation
       model-x
       118k / 200k tokens   ██████░░░░ 59%
       90k cached · 4.2k output

09:43  Tool · read-file
       tests/auth.test.ts
       ✓ 9.2 KB

09:43  Context
       + tests/auth.test.ts

09:44  Invocation
       157k / 200k          ████████░░ 79%

09:45  Edit
       tree 79ad… → a130…
       3 files changed

09:46  Context compaction
       context-window · summary

09:46  Context
       tree a130…
       - tests/auth.test.ts

09:47  Invocation
       191k / 200k          ██████████ 96%
       ⚠ high context pressure

09:48  Commit
       abc123 Fix auth policy
```

This view should be understandable without knowing the session event schema.

Raw event JSON can remain an advanced/debug surface.

---

## 14. Context pressure visualization

When both `inputTokens` and `windowLimitTokens` exist, the UI SHOULD make context pressure visually apparent.

For example:

```text
Context window
157k / 200k
████████░░ 79%
```

Any severity thresholds are product policy, not protocol semantics.

A UI may choose rules such as:

```text
below 70%   quiet
70–90%      visible
above 90%   warning
```

but those labels MUST NOT be persisted back into signed telemetry as objective facts.

If the window limit is unknown, display token usage without a utilization bar rather than guessing the denominator.

---

## 15. Context Pack diffs

Successive Context Packs make context loss directly visible.

The Flight Recorder SHOULD support a context diff between adjacent exposures:

```text
Context #17 → Context #18

Added
+ tests/auth.test.ts
+ src/policy.ts

Removed
- docs/design.md
```

After compaction:

```text
Context #22 → Context #23

Removed
- tests/auth.test.ts
- config/worker.json

Between exposures
context-compaction · context-window
```

A file in the diff should open the exact Git blob/range associated with that historical Context Pack, not the current worktree version.

This is one of the most important product benefits of keeping repository evidence Git-grounded.

---

## 16. Provenance status should be explicit

Different parts of the audit chain have different evidence strength.

The UI should report them separately.

For example:

```text
Git evidence      ✓ verified
Context render    ✓ retained and digest verified
Runtime usage     provider reported
Tool trace        expired
Workspace tree    ✓ verified
```

Other useful states include:

```text
✓ Verified
⚠ Pack valid, ContextRender body unavailable
✕ path/blob mismatch
⚠ stale tree
⚠ telemetry coverage partial
```

A green overall badge MUST NOT hide a weaker component, and an unavailable side trace MUST NOT make valid Git provenance appear invalid.

---

## 17. Tool operations stay collapsed by default

The Flight Recorder should not resemble a raw agent transcript.

Tool activity is high-volume and usually secondary until something fails.

Default presentation:

```text
▸ 14 tool operations
    12 successful
     1 truncated
     1 failed
```

Expanded presentation:

```text
read-file   src/auth.ts          ✓ 8.4 KB
grep        validate             ✓ 17 hits
read-file   config/prod.json     ✕ unavailable
edit        src/auth.ts          ✓ tree A → B
```

Raw result bodies appear only when:

- a retained side object exists;
- access policy allows the viewer to read it;
- the viewer explicitly requests it.

The canonical UI should remain useful after raw side telemetry has expired.

---

## 18. Retry and fallback visualization

Retries should preserve their branching/causal meaning:

```text
Invocation 8 · model-x
        │
        └─ timeout
             ↓ retry
        Invocation 9 · model-x
             │
             └─ provider error
                  ↓ fallback
             Invocation 10 · model-y
                  ✓ success
```

This prevents a final agent operation from being incorrectly presented as if one model invocation produced it directly.

---

## 19. Audit summary card

The top of a session detail MAY provide a derived summary:

```text
Agent session audit

Context provenance      ✓ 18 / 18 verified
Latest repository tree  ✓ current
Peak context pressure   ⚠ 96%
Compactions             2
Retries                 1
Failed tools            1
Telemetry coverage      partial
Knowledge continuity    ⚠ relevant test removed after compaction
```

The final line is a derived diagnosis and should be phrased in observable terms.

Prefer:

> `tests/auth.test.ts` was present in Exposure 17 and absent after a recorded compaction before Exposure 18.

Avoid:

> The model forgot `tests/auth.test.ts`.

Likewise prefer:

> The read tool returned `policy.ts`, but `policy.ts` was absent from the next Context Pack.

rather than:

> The agent ignored `policy.ts`.

The UI observes the harness and repository provenance; it does not infer model cognition.

---

## 20. Change Request and commit surfaces

Telemetry should be reachable from code review and history, not only from the Activity page.

A Change Request may show:

```text
Provenance
Claude · session 0198f2aa
3 agent invocations
124k tokens
peak context 88%
✓ final context tree current at last invocation

View session
```

An individual agent-authored commit may show:

```text
Produced by session 0198f2aa

Last invocation before commit
  model-x
  88% context pressure
  Context Pack: 12 repository items
```

These summaries should link to the same Flight Recorder rather than duplicating a second telemetry view.

The repository artifact remains the primary navigation object; telemetry explains its provenance.

---

## 21. Derived diagnostics

The server or UI MAY derive useful diagnostics from canonical facts.

Examples include:

```text
high context pressure
Context Pack changed after compaction
repository item disappeared between exposures
successful repository read absent from the next exposure
Context Pack tree predates a recorded Workspace Transition
retry changed provider/model
telemetry coverage is incomplete for this failure class
```

Derived diagnostics MUST remain distinguishable from signed event claims.

They should be reproducible from the projected facts whenever possible.

The product should prefer a concrete explanation over a generic warning:

```text
better:
  view.tree is A, but the workspace transitioned A → B before this exposure

worse:
  context may be stale
```

---

## 22. Access control, retention, and redaction

The UI must respect the same separation between canonical records and disposable side telemetry as the protocol.

Canonical session facts can survive indefinitely.

Raw provider envelopes, tool-result bodies, and transcript-like telemetry should remain subject to local retention and access policy.

The UI SHOULD clearly represent an expired side object:

```text
Tool result body unavailable
Digest retained: sha256:...
Metadata retained: 9.2 KB · success
```

It MUST NOT treat retention expiry as repository corruption.

Redacted canonical events should continue to appear structurally as redactions, according to the existing session DAG semantics.

---

## 23. Implementation sequence

A useful implementation can land incrementally.

### Phase 1 — session-level runtime summary

Extend the current session projection/UI with:

```text
provider-reported aggregate token usage
known window limit
peak context pressure
telemetry coverage
```

No new detailed screen is required to get initial value.

### Phase 2 — generic event recorder

Add:

```text
git+ session record
```

with schemas for:

```text
invocation-telemetry
context-compaction
context-truncation
tool-telemetry
workspace-transition
```

Extend harness adapters to use it.

### Phase 3 — session detail projection

Add a projected detail read model and endpoint:

```text
GET /hub/sessions/:id
```

including coverage and ordered telemetry events.

### Phase 4 — Flight Recorder

Add:

- context pressure meters;
- retries/fallbacks;
- context diffs;
- collapsed tool groups;
- workspace transitions;
- provenance status.

### Phase 5 — Context Pack/exposure integration

Once Context Pack persistence exists end-to-end, make context additions/removals and exact historical evidence first-class in the Flight Recorder.

This order lets telemetry improve the current Sessions UI without waiting for every provenance primitive to be implemented simultaneously.

---

## 24. Acceptance criteria

The integration is useful when:

1. a harness adapter can record telemetry without knowing Git object/ref/signature internals;
2. unsupported hook classes appear as missing coverage rather than false zeroes;
3. provider-reported token usage remains visibly distinct from estimated or derived values;
4. repository-mutating sessions can expose `beforeTree → afterTree` at meaningful boundaries without writing a tree for every filesystem operation;
5. multiple provider retries/fallbacks remain distinct in session history and UI;
6. the server, not the browser, projects session DAG ordering and trust/redaction state;
7. the Activity page can summarize session runtime health without becoming a telemetry dashboard;
8. a session detail can correlate Context Packs, exposures, invocations, tools, workspace transitions, and resulting commits;
9. successive Context Packs can be diffed to show which repository evidence appeared or disappeared;
10. raw tool/provider bodies can expire without destroying the usefulness of canonical provenance;
11. derived warnings use observable language and never claim model cognition;
12. a reviewer can move from a commit or Change Request to the exact session history that explains its runtime and repository-context provenance.

---

## Final principle

> **Harnesses capture what they can observe; `git+` turns those observations into small signed session facts; the hub projects them beside repository provenance; and the UI uses the result to explain how an agent's repository knowledge changed before code was produced. Telemetry is most valuable when it connects context to repository operations, not when it merely counts tokens.**
