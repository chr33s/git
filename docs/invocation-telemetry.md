# Git-Native Invocation Telemetry

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-21  
**Spec revision:** draft-1

## 1. Summary

This specification defines a small runtime-provenance layer for coding-agent
sessions.

[Context Packs](content-pack.md) answer:

> **What Git-grounded repository evidence was associated with an invocation, and what exact repository-context artifact crossed the harness audit boundary?**

Invocation Telemetry answers the adjacent operational question:

> **Under what observable runtime conditions did that invocation and its resulting agent operations occur?**

The useful model is three layers:

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
  optional Tool Telemetry
```

The separation is deliberate. Token counts, model names, retries, context-window
limits, compaction, tool timing, and workspace transitions MUST NOT affect
Context Pack identity or the verification of Git-grounded repository evidence.

The goal is an **agent flight recorder**, not an inference-attestation system.
The harness records what it can observe or what a provider reports. Nothing in
this specification proves what a model attended to, understood, remembered,
or reasoned about internally.

---

## 2. Problem

Repository-context failures are not all retrieval failures.

An agent may act incorrectly because:

- the retriever never found relevant evidence;
- the retriever found it but a context budget dropped it;
- the harness exposed it on an earlier invocation but later compacted it away;
- a tool read succeeded but its result did not enter the next model invocation;
- the model invocation ran close to its context-window limit;
- a retry or fallback used a different model or context envelope;
- repository state changed between the context exposure and a later operation;
- a tool failed, truncated output, or returned stale data;
- provider or harness behavior changed between otherwise similar incidents.

A Context Pack and Context Exposure Event establish repository provenance, but
they intentionally do not describe all of those runtime conditions.

Without runtime provenance, an investigation often collapses distinct failures
into one vague diagnosis:

```text
"the agent hallucinated"
```

A useful audit should instead be able to distinguish:

```text
retrieval failure
context-selection / budget failure
context-lifecycle failure
tool failure
workspace-staleness failure
model/runtime regression
```

This specification records the small set of runtime facts needed to make those
distinctions without turning the repository into a raw transcript store.

---

## 3. Relationship to existing session provenance

This specification extends the session-provenance model described in
[agents.md](agents.md), especially its append-only signed session event DAG and
its distinction between canonical distilled records and disposable transcript
side objects.

It does **not** replace:

- `session.opened`, which identifies the session, agent, and harness;
- `session.prompted`, which records the instruction or its faithful
  condensation;
- `session.produced`, which binds resulting commits and refs;
- `session.closed.usage`, which MAY remain a compact session-level usage
  summary;
- transcript side objects, which remain the appropriate home for bulky raw
  execution traces when retained at all.

Invocation telemetry adds finer-grained runtime evidence between those
session-level milestones.

A conforming implementation MAY support Context Packs without Invocation
Telemetry, or Invocation Telemetry without retaining detailed tool traces.
The capabilities compose but are independently useful.

---

## 4. Goals and non-goals

### 4.1 Goals

V1 SHOULD make it possible to record, when available:

1. the model and provider requested for an invocation;
2. provider-reported input, output, and cached-token usage;
3. the model context-window limit known to the harness;
4. the size of the repository `ContextRender` associated with the invocation;
5. retry and fallback relationships between invocations;
6. explicit context compaction or truncation performed by the harness;
7. Git-tree transitions caused by repository-mutating agent operations;
8. compact tool-result diagnostics without making raw tool traces canonical;
9. the provenance of each measurement: harness-observed, provider-reported,
   or derived;
10. signed ordering of these facts within the session history.

### 4.2 Non-goals

V1 does **not** attempt to:

- observe or store model chain-of-thought;
- prove model attention, understanding, memory, or causation;
- standardize provider tokenizers;
- make token counts comparable across providers or model families;
- define one universal context-window size for a model name;
- make latency, cost, or token usage part of Context Pack identity;
- persist every raw tool call or tool-result body in the canonical session DAG;
- reproduce provider request serialization;
- require provider request IDs, internal cache keys, or proprietary trace IDs;
- standardize every provider-specific usage field;
- make runtime telemetry an authorization or policy attestation by itself.

Provider-specific detail MAY be retained as namespaced metadata or disposable
side telemetry.

---

## 5. Core principles

### 5.1 Observe the harness, not cognition

Invocation Telemetry records facts visible at the harness boundary and claims
returned by external systems.

It MUST NOT describe a token count as proof that the model read those tokens,
or a Context Exposure Event as proof that the model used the exposed evidence.

Preferred language is:

```text
sent
exposed
reported
observed
retained
compacted
truncated
```

rather than:

```text
read
understood
remembered
used
reasoned from
```

### 5.2 Runtime telemetry does not participate in Context Pack validity

A Context Pack verifier MUST NOT require invocation telemetry to verify:

```text
view.tree + item.path -> item.blob
item.blob + range -> evidence bytes
```

Likewise, a missing token count, unknown model version, or unavailable provider
usage record MUST NOT invalidate a Context Pack or Context Exposure Event.

> **Repository provenance is stable even when runtime telemetry is partial.**

### 5.3 Raw measurements before derived metrics

Where a value can be derived from more stable measurements, implementations
SHOULD retain the measurements and compute the derived value at read time.

For example, prefer:

```json
{
  "inputTokens": 187431,
  "windowLimitTokens": 200000
}
```

over persisting only:

```json
{ "utilization": 0.937155 }
```

Similarly, cost is normally derived from provider usage plus a pricing table
that changes independently. A producer MAY record cost as a diagnostic, but it
MUST identify its source and MUST NOT treat it as immutable spend truth.

### 5.4 Reported, observed, and derived are different evidence classes

Runtime values fall into three classes:

```text
observed
  measured directly by the harness at a defined boundary

reported
  supplied by a model provider, tool, or other external runtime

derived
  computed from observed or reported values
```

Examples:

```text
ContextRender byte length       observed
wall-clock invocation duration  observed
provider input token count      reported
provider cached-token count     reported
context-window utilization      derived
estimated dollar cost           derived
```

A consumer MUST NOT silently promote `reported` or `derived` values to
`observed` facts.

### 5.5 Canonical records stay distilled

The session DAG is not a transcript store.

Small audit facts MAY be canonical session events. Bulky or secret-laden raw
payloads—full tool-result bodies, provider request/response envelopes, verbose
execution traces—SHOULD remain disposable side telemetry, referenced by digest
when a canonical record needs to name them.

This preserves the existing session-provenance rule:

> **The refs are the index, not the corpus.**

### 5.6 Hooks are capture points, not protocol dependencies

Implementations will integrate with different harnesses: Claude Code, Codex,
custom orchestrators, CI agents, local model runners, or future systems.

This specification names **logical capture points**. It does not require any
vendor's hook API or hook naming convention.

### 5.7 Signed history provides ordering

Canonical telemetry events belong in the signed session history. The session
DAG supplies authorship, causal ordering, redaction semantics, and replication.

Telemetry payloads MUST NOT invent a second signing or event-ordering system.

---

## 6. Invocation Telemetry

An Invocation Telemetry record describes one completed or failed model
invocation.

A minimal payload is:

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

The surrounding session event supplies the normal session ID, event ID,
signer, signature, and causal parentage. Examples in this document show only
the telemetry payload fields relevant to this specification.

### 6.1 `exposure`

When the invocation had a Context Exposure Event, `exposure` SHOULD identify
that prior event.

This creates the audit join:

```text
Context Pack
     ↓
Context Exposure Event
     ↓ exposure
Invocation Telemetry
     ↓
agent/tool operations
```

The telemetry event occurs after the provider invocation has completed or
failed, so referencing the already-existing exposure event does not create a
content-addressing cycle.

If no Context Exposure Event exists, `exposure` MAY be absent. A consumer MUST
NOT infer repository-context provenance from telemetry alone.

### 6.2 Model identity

`model` MAY contain:

```json
{
  "provider": "example",
  "id": "model-x",
  "revision": "2026-08-15"
}
```

`provider` and `id` are the identifiers the harness requested or was told it
used. `revision` is optional because many providers do not expose a stable
model revision.

These fields are operational labels, not cryptographic identity. A model
provider may serve changing weights or infrastructure behind a stable model
name.

A producer MUST NOT claim a stable model revision when the provider does not
supply one.

### 6.3 Usage

Core usage fields are:

```text
inputTokens
outputTokens
cachedInputTokens
```

Each is a non-negative integer when present.

`usage.source` SHOULD be one of:

```text
provider
estimated
```

When `source` is `provider`, the counts are exactly the values returned by the
provider or model runtime for that invocation.

When `source` is `estimated`, the producer SHOULD include an `estimator`
identifier:

```json
{
  "source": "estimated",
  "estimator": "provider-tokenizer-v3",
  "inputTokens": 117930
}
```

Estimated usage MUST NOT be presented as provider-reported usage.

A provider MAY expose additional token classes such as reasoning tokens,
cache-write tokens, audio tokens, image tokens, or provider-specific input
classes. Implementations MAY preserve them under a namespaced extension rather
than expanding the V1 core vocabulary for every provider feature.

### 6.4 Whole-invocation usage versus repository-context size

Provider `inputTokens` normally describes the provider's **whole invocation
input**, which may include:

- system and developer instructions;
- user messages;
- prior conversation turns;
- tool results;
- repository `ContextRender` content;
- provider-specific framing.

It MUST NOT be interpreted as the token size of the Context Pack or
`ContextRender` alone.

`context.renderBytes` is the harness-observed byte length of the exact
`ContextRender` associated with the exposure event. Because bytes are independent
of tokenizer choice, this value is stable across model migrations.

A producer MAY additionally estimate repository-context tokens, but such a
value MUST be labeled estimated and MUST NOT replace `renderBytes`.

### 6.5 Context-window limit

When known, `context.windowLimitTokens` records the maximum input/context
capacity the harness believed applied to the invocation.

The source MAY be:

- provider metadata;
- a model catalog pinned by the harness;
- a local runtime configuration.

If the limit is unknown or ambiguous, the field SHOULD be omitted rather than
guessed.

Context utilization is derived:

```text
inputTokens / windowLimitTokens
```

and need not be persisted.

An audit UI SHOULD make high context pressure easy to see, because context
loss, truncation, and compaction often correlate with it.

### 6.6 Timing

A producer MAY record harness-observed timing:

```json
{
  "timing": {
    "durationMs": 8421
  }
}
```

Session event timestamps already provide coarse history. Timing fields exist
for runtime diagnosis and performance analysis, not causal proof.

Provider-reported server timing MAY be retained separately from harness wall
clock and SHOULD be labeled as provider-reported.

### 6.7 Finish status

A producer SHOULD record a coarse finish status when known:

```text
success
length
content-filter
cancelled
timeout
provider-error
other
```

Provider-specific finish reasons MAY be retained as descriptive metadata.

The core status is deliberately coarse so audit tooling can group failures
without understanding every provider vocabulary.

### 6.8 Retries and fallbacks

Retries and fallbacks MUST remain visible when telemetry is retained.

A later invocation MAY identify the prior telemetry event:

```json
{
  "attempt": {
    "number": 2,
    "previous": "0198f2b1-...",
    "reason": "timeout"
  }
}
```

If a retry changes provider or model, the later event records the new model
identity normally.

A harness MUST NOT collapse several billable or behaviorally distinct provider
invocations into one telemetry record merely because they produced one final
agent response.

This makes flows such as the following auditable:

```text
model A → timeout
model A → retry
model B → fallback
```

### 6.9 Session-level usage summaries

A session MAY continue to carry aggregate usage in `session.closed.usage`.

When per-invocation telemetry exists, a session-level summary SHOULD be treated
as a convenience projection over those records plus any invocations whose
telemetry was not retained.

The summary need not reproduce provider-specific detail.

---

## 7. Context lifecycle

Context changes between invocations. Those changes are often the missing link
between "the agent saw this earlier" and "the agent acted as if it no longer
knew it."

### 7.1 Compaction

A harness that intentionally summarizes, compresses, or replaces prior context
MAY record:

```json
{
  "type": "context-compaction",
  "strategy": "summary",
  "reason": "context-window"
}
```

`strategy` is descriptive. Recommended core values are:

```text
summary
dedupe
replace
provider-managed
other
```

The compaction event does not need to serialize the entire before/after
conversation state. The next Context Exposure Event provides the authoritative
repository-context artifact for the subsequent invocation.

If the compaction produced a retained summary artifact, the event MAY reference
that artifact by digest under the session's ordinary sensitive-content
retention rules.

### 7.2 Truncation

When the harness deliberately drops content rather than semantically compacting
it, it MAY record:

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

Recommended core reasons are:

```text
context-window
host-limit
provider-limit
policy
error
other
```

Dropped counts are diagnostics, not proof of which exact items disappeared.
Exact repository exposure is determined by comparing surrounding Context Packs
and ContextRender artifacts.

### 7.3 Context lifecycle and Context Packs

A context lifecycle event explains **why the available context may have
changed**. It does not replace the next Context Pack.

The intended audit path is:

```text
Exposure N
  pack contains policy.ts
        ↓
context-compaction
        ↓
Exposure N+1
  pack no longer contains policy.ts
        ↓
agent operation
```

This is stronger than trying to infer retention from old turns because each
exposure records the repository context actually associated with the later
invocation.

---

## 8. Tool telemetry

Detailed tool-use sequences are valuable but high-volume and frequently
sensitive. V1 therefore does **not** require one canonical session event per
tool call.

### 8.1 Canonical tool diagnostics

A producer MAY record small, audit-relevant tool facts in the session history,
especially for failures or repository-mutating operations:

```json
{
  "type": "tool-telemetry",
  "invocation": "0198f2b2-...",
  "tool": "read-file",
  "status": "success",
  "result": {
    "bytes": 8421,
    "truncated": false,
    "digest": "sha256:..."
  }
}
```

`invocation` identifies the Invocation Telemetry event that led to the tool
operation when such an event exists.

The canonical record SHOULD contain metadata and digests, not the raw result
body.

### 8.2 Side telemetry for detailed traces

A full tool trace MAY be retained as a disposable side object:

```json
{
  "trace": {
    "hash": "sha256:...",
    "size": 381244,
    "media": "application/jsonl"
  }
}
```

Like session transcript objects, detailed tool telemetry:

- is not required for session reconstruction;
- MAY be absent on a replica;
- follows local retention policy;
- MUST follow secret-handling and redaction policy;
- MUST NOT become required Git reachability merely because its digest appears
  in a canonical event.

### 8.3 Tool result versus later model exposure

A successful tool call does not prove its result reached the next model
invocation.

The audit chain is intentionally two-step:

```text
tool telemetry says:
  result existed

next Context Exposure Event says:
  what repository-derived context crossed into the next invocation
```

For repository reads, a producer SHOULD represent evidence that entered the
next invocation in that invocation's Context Pack where possible.

This distinguishes:

```text
agent never requested the evidence
tool request failed
tool returned the evidence but harness omitted it later
evidence was exposed, then compacted away on a subsequent turn
```

### 8.4 Tool identity

`tool` is a descriptive identifier. A producer MAY include a tool or harness
version, but V1 does not define a global tool registry.

Provider- or harness-specific tool metadata SHOULD use namespaced fields.

---

## 9. Workspace transitions

Repository mutation is one of the strongest observable boundaries in a coding
agent session.

When a harness can cheaply capture Git trees around a repository-mutating
operation, it MAY record:

```json
{
  "type": "workspace-transition",
  "operation": "0198f2b3-...",
  "beforeTree": "sha256:aaa...",
  "afterTree": "sha256:bbb..."
}
```

`operation` identifies the tool or agent-operation event responsible for the
mutation when available.

### 9.1 Meaning

A Workspace Transition claims:

> the effective repository tree visible to the harness changed from
> `beforeTree` to `afterTree` as a result of this operation.

Both tree OIDs are Git object identities and SHOULD remain reachable for as
long as the transition is intended to be auditable.

### 9.2 Boundary capture versus every filesystem write

V1 does not require constructing an overlay tree after every low-level file
write.

A producer SHOULD prioritize transitions at audit-relevant boundaries:

- after an agent edit tool completes;
- before a subsequent model invocation;
- before commit creation;
- after checkout, merge, rebase, or other state-changing Git operations.

The next Context Pack's `view.tree` remains authoritative for the repository
snapshot used by retrieval.

### 9.3 Stale-context detection

Workspace transitions make a useful invariant mechanically checkable:

```text
Exposure against tree A
      ↓
workspace transition A → B
      ↓
next repository-affecting invocation should expose context against tree B
```

If the next Context Pack still claims tree A, an auditor has direct evidence of
stale repository context or a capture bug.

---

## 10. Logical harness hooks

The protocol is independent of any vendor hook API. A harness integration
SHOULD identify equivalent capture points for the following logical hooks.

### 10.1 `beforeInvocation`

Useful work:

```text
capture / verify current repository view
construct Context Pack
construct final ContextRender
append Context Exposure Event
record requested provider/model
```

The Context Exposure Event must already exist, or otherwise be unambiguously
bound, before the model invocation it describes.

### 10.2 `afterInvocation`

Useful work:

```text
capture provider-reported usage
capture finish status
capture retry/fallback relationship
capture harness-observed duration
append Invocation Telemetry
```

### 10.3 `beforeTool` / `afterTool`

Useful work:

```text
identify originating invocation
record failure / truncation / result size
hash retained result bytes when useful
capture repository tree before/after mutating tools
```

Detailed tool bodies SHOULD remain side telemetry unless another specification
makes them canonical evidence.

### 10.4 `beforeContextCompaction` / `afterContextCompaction`

Useful work:

```text
record compaction strategy and reason
record coarse dropped counts if known
retain summary digest only when policy permits
```

The next Context Exposure Event records the resulting repository context; the
compaction hook does not need to invent a second canonical context format.

### 10.5 `afterWorkspaceMutation`

When the harness already maintains an overlay tree, this hook is the natural
place to record `beforeTree → afterTree`.

A harness that cannot cheaply construct a tree at every mutation MAY defer
capture until the next invocation boundary.

---

## 11. Storage, reachability, and retention

### 11.1 Canonical telemetry events

Small Invocation Telemetry, Context Lifecycle, Tool Telemetry summaries, and
Workspace Transition events MAY live in the signed session DAG.

They inherit the session specification's:

- append-only event semantics;
- authorship and signature verification;
- replication rules;
- payload bounds;
- redaction behavior;
- trust and revocation semantics.

This specification does not create a new ref namespace.

### 11.2 Side telemetry

Bulky raw traces SHOULD remain outside canonical Git reachability, using the
same content-addressed side-object pattern as session transcripts.

A missing side object is valid. Its canonical reference proves only that a
particular digest was recorded, not that every replica retained the bytes.

### 11.3 Retention profiles

A deployment MAY define local retention profiles such as:

```text
minimal
  canonical telemetry only

debug
  canonical telemetry + recent tool side traces

research
  broader trace retention under explicit security,
  consent, and licensing policy
```

Retention policy is operational configuration and MUST NOT change the meaning
of canonical events that remain.

---

## 12. Security and privacy

### 12.1 Telemetry can be sensitive

Model/provider identifiers, request timing, token counts, tool names, paths,
and raw side traces may reveal operational details even when prompts are not
stored.

A deployment MUST apply the session system's normal access control, secret
handling, redaction, and retention policy.

### 12.2 Do not canonize provider envelopes

Provider request and response bodies SHOULD NOT be embedded in canonical
telemetry events.

They are:

- high-volume;
- provider-specific;
- likely to contain prompts and source;
- subject to retention and privacy requirements;
- unnecessary for the core audit joins.

If retained, they belong in disposable side telemetry.

### 12.3 Provider reports are claims

A provider-reported token count or model identifier is evidence of what the
provider reported to the harness. It is not independently verified by Git or
by the session signature.

The session signature proves which harness/member recorded the claim.

### 12.4 Telemetry does not grant authority

No telemetry field creates instruction authority, repository capability, or
policy approval.

A tool result does not become an instruction because it was logged. A model
identity does not expand the member key's grant. A token budget does not
replace server-side authorization.

---

## 13. Product and audit surface

This specification does not require a CLI, but a useful product surface would
extend the existing session/context commands rather than introduce a separate
observability product.

Examples:

```text
git+ session show <session>
git+ context audit <operation-or-session-event>
git+ session telemetry <session>
```

An audit view SHOULD make the following easy to correlate:

```text
repository view/tree
Context Pack
ContextRender digest and retained bytes
model/provider
input/output/cache tokens
context-window limit
retry/fallback chain
compaction/truncation events
tool failures/truncation
workspace tree transitions
resulting commits/refs
```

Derived values such as context utilization or estimated cost SHOULD be clearly
labeled as derived.

---

## 14. Audit examples

### 14.1 Context-window pressure

An agent makes an incorrect assumption late in a long session.

Audit finds:

```text
Invocation 31
  inputTokens:       187431  (provider-reported)
  windowLimitTokens: 200000

context-compaction
  reason: context-window
  strategy: summary

Invocation 32
  relevant test absent from Context Pack
```

The audit can distinguish a context-lifecycle failure from a failure to ever
retrieve the test.

It still cannot prove that the missing test caused the incorrect assumption.

### 14.2 Tool result omitted from later exposure

Audit finds:

```text
tool: read-file tests/policy.test.ts
  status: success
  result bytes: 9211

next Context Pack:
  tests/policy.test.ts absent
```

The file read succeeded, but the repository evidence was not represented in the
next auditable exposure. The likely investigation target is context assembly,
not the read tool.

### 14.3 Retry changed the model

Audit finds:

```text
Invocation 8
  model: provider-a/model-x
  status: timeout

Invocation 9
  previous: Invocation 8
  reason: fallback
  model: provider-b/model-y
```

The final agent operation is no longer incorrectly attributed to one model
attempt.

### 14.4 Stale repository context after mutation

Audit finds:

```text
Exposure 12
  view.tree: A

workspace-transition
  A -> B

Exposure 13
  view.tree: A
```

The later invocation used a repository view that predates the recorded
workspace mutation. This is direct evidence of stale-context capture or
harness failure.

### 14.5 Provider usage unavailable

A local model runtime does not report token usage.

Audit still has:

```text
Context Pack: verified
ContextRender digest: verified
renderBytes: observed
model: local/runtime-x
usage: absent
```

Repository and exposure provenance remain valid. Telemetry completeness
degrades without breaking the audit chain.

---

## 15. Conformance

### 15.1 Invocation Telemetry producer

A conforming producer that records Invocation Telemetry MUST:

- bind it to the correct signed session history;
- distinguish provider-reported from estimated token usage;
- avoid describing whole-invocation token counts as repository-context token
  counts;
- preserve retry/fallback attempts as distinct invocations when they were
  distinct model calls;
- omit unknown values rather than inventing them;
- keep telemetry independent of Context Pack verification.

### 15.2 Context lifecycle producer

A conforming producer that records compaction or truncation MUST:

- describe the operation as a harness/runtime action, not as model cognition;
- avoid treating coarse dropped counts as exact item identity;
- rely on subsequent Context Packs/ContextRender artifacts for exact later
  repository exposure.

### 15.3 Tool telemetry producer

A conforming producer that records tool telemetry MUST:

- distinguish tool success from later model exposure;
- avoid embedding bulky raw tool bodies in canonical events by default;
- apply normal secret, access-control, redaction, and retention policy to side
  telemetry.

### 15.4 Workspace transition producer

A conforming producer that records workspace transitions MUST:

- use Git tree object identities for `beforeTree` and `afterTree`;
- retain those trees for as long as the transition is claimed to be durably
  auditable;
- not substitute mutable filesystem paths or timestamps for tree identity.

---

## 16. Evaluation

Runtime provenance and agent quality SHOULD be evaluated separately.

Useful runtime-provenance metrics include:

```text
percentage of model invocations with exposure records
percentage with provider-reported usage
percentage with known context-window limits
percentage of repository mutations followed by a fresh view.tree
percentage of failed tool operations represented in telemetry
retry/fallback visibility
telemetry retention coverage
```

Useful investigations can then correlate those with outcome labels already
present in repository/session provenance:

```text
merged / unmerged
check passed / failed
approved / rejected
agent correction required
context audit failure
```

Correlation remains correlation. This specification does not convert runtime
telemetry into proof of why a model behaved as it did.

---

## 17. Recommended V1 capture profile

A minimal useful implementation SHOULD start with five signals:

```text
1. provider-reported input/output/cache token usage
2. known model context-window limit
3. explicit context compaction/truncation events
4. compact tool failure/truncation diagnostics, with raw traces optional
5. beforeTree -> afterTree for audit-relevant repository mutations
```

This profile captures most of the operational evidence needed to distinguish
retrieval, context-lifecycle, tool, and stale-workspace failures without making
normal sessions transcript-sized.

Fields such as detailed latency breakdowns, dollar cost, rate-limit state,
provider request IDs, GPU/runtime counters, reasoning-token classes, or
provider cache internals SHOULD remain optional extensions until concrete
product or audit requirements justify standardizing them.

---

## Final invariant

> **Context Packs record Git-grounded repository evidence; Context Exposure Events commit to the exact repository ContextRender that crossed the harness context-to-invocation audit boundary; Invocation Telemetry records observable or explicitly reported runtime conditions around that invocation. Canonical records remain small and signed, detailed traces remain disposable side telemetry, and none of these records claims to observe model cognition or prove causation.**
