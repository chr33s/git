# Git-Native Knowledge Durability

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-22  
**Revision:** draft-2

## 1. Summary

A repository should retain the operational knowledge necessary for the next competent human or agent to continue the work, even when the previous contributor, model session, or hosting platform is gone.

The problem has three stages:

```text
Capture
  Did somebody record the claim or evidence while it was available?

      ↓

Retention
  Does that record survive the person, session, tool, or host?

      ↓

Recall
  Does the next human or agent receive it when relevant?
```

No one primitive solves all three. The architecture composes signed sessions and decisions, Repository Memory, [Context Packs](content-pack.md), Context Exposure records, and [Invocation Telemetry](invocation-telemetry.md).

The goal is **durable, attributable repository knowledge**, not exhaustive recording of human thought or model cognition.

A crucial distinction is:

> **A citation proves where a claim came from. It does not, by itself, prove the claim is true or still current.**

---

## 2. Scope

The architecture is intended to preserve captured repository-specific operational knowledge such as:

- why a change exists;
- which constraint or decision shaped it;
- conventions repeatedly observed across work;
- repository-specific gotchas;
- friction that repeatedly wastes effort;
- tests and configuration that constrain an implementation;
- the repository context available to an agent when it acted;
- whether context later disappeared through selection, compaction, truncation, or assembly failure.

It cannot preserve knowledge that never crosses a capture boundary: private verbal conversations, undocumented customer dependencies, tacit skill, or model-internal reasoning remain outside the guarantee.

---

## 3. Capture

### 3.1 Sessions capture work context

The policy-visible session DAG remains distilled:

```text
who was working
what was asked
what was produced
what decisions were requested/resolved
what the session learned in a compact note
aggregate usage when available
```

High-frequency invocation/tool telemetry belongs in the sibling audit trace, not in the policy-critical session DAG.

### 3.2 Decisions capture judgement

Material judgement SHOULD become signed decision provenance rather than remain only in chat.

```text
decision.requested
       ↓
decision.resolved
       ↓
session.produced
       ↓
commit / pull request
```

### 3.3 Session-end distillation captures reusable claims

At session end, the harness or a distiller SHOULD identify a small set of reusable repository-specific claims:

```text
convention
gotcha
decision
friction
```

Use **session end** or **stop hook** terminology unless the session protocol later defines an actual `session.closed` event. The current durable result is `session.produced` plus its note/usage fields.

### 3.4 Cited claims, not uncited truth

A distilled claim MUST cite durable provenance such as:

- a session;
- a decision record;
- a commit;
- a Context Pack / exposure;
- other repository evidence.

A claim with citations is **attributable**. A citation alone does not make it verified truth.

Products SHOULD distinguish:

```text
source verified
  the cited record/object exists and matches its identity

claim current
  structured evidence dependencies still match the current repository view

claim true
  a stronger semantic judgement that this architecture does not generally prove
```

---

## 4. Structured evidence dependencies

Free-text citations are insufficient for machine staleness checks. A Repository Memory entry SHOULD therefore be able to carry structured evidence dependencies in addition to prose citations.

Example:

```json
{
  "kind": "gotcha",
  "text": "Worker auth tests require the production policy fixture.",
  "cites": [
    {
      "session": "0198f2aa-...",
      "record": "sha1:abc123..."
    }
  ],
  "evidence": [
    {
      "kind": "blob",
      "path": "tests/worker/auth.test.ts",
      "blob": "sha1:def456..."
    },
    {
      "kind": "blob",
      "path": "config/policy.json",
      "blob": "sha1:789abc..."
    },
    {
      "kind": "gitlink",
      "path": "vendor/policy-engine",
      "commit": "sha1:456def..."
    }
  ]
}
```

The locator semantics SHOULD reuse Context Pack evidence rules:

```text
blob dependency
  current tree + path → recorded blob

gitlink dependency
  current tree + path (mode 160000) → recorded submodule commit
```

### 4.1 Staleness states

For each structured dependency, a reader can classify:

```text
unchanged
  current tree resolves to the same object

changed
  path exists but resolves to a different object

missing
  path no longer exists / no longer has the expected item kind

unknown
  dependency was not structured, object unavailable, or repository view unavailable
```

A changed dependency means **the claim needs revalidation**. It does not automatically mean the prose is false.

### 4.2 Evidence granularity

Dependencies SHOULD be as narrow as useful. A path/blob dependency is usually better than pinning the entire repository tree because unrelated changes should not stale every memory entry.

For range-specific claims, an implementation MAY additionally record a byte range, but consumers should treat exact-range equality as evidence identity rather than semantic equivalence.

### 4.3 Decisions and non-source citations

Decision/session records remain citations even when they cannot be mechanically checked for source staleness. `cites` answers "where did this claim come from?"; `evidence` answers "which repository objects can be revalidated?"

---

## 5. Retention

### 5.1 Canonical records are Git-native

Durable claims and provenance live in Git objects/refs so they can replicate, retain signer/history, survive account loss, and be inspected without the original hosting provider.

### 5.2 Raw transcripts are not canonical

Raw transcripts, provider envelopes, full tool output, and scratchpads are high-volume/high-risk. They SHOULD remain disposable side objects when retained at all.

> **The refs are the index, not the corpus.**

### 5.3 Repository Memory is a projection, not truth

Repository Memory is a bounded, regenerable projection of cited durable records. It is not an authoritative instruction source and not an immutable fact database.

The projection may compact, deduplicate, reorder, or evict entries. The underlying cited provenance remains the durable record.

### 5.4 Forgetting can be correct

A memory entry may need revalidation or eviction because its structured dependencies changed, a migration completed, a convention was replaced, or cited content was redacted.

Mechanical forgetting SHOULD remove stale claims from the **active projection** without rewriting historical signed records.

---

## 6. Recall

### 6.1 Session-start recall

A new session SHOULD receive a bounded Repository Memory projection alongside standing instructions/policy.

Memory is data, not authority.

### 6.2 Task-specific recall

Context Packs provide task-specific evidence grounded in the exact current Repository View.

### 6.3 Exposure proves the recall boundary

A Context Exposure record commits to the semantically framed ContextRender that crossed the harness boundary for an invocation.

This distinguishes:

```text
claim existed in Repository Memory

from

claim/evidence was actually present in this invocation's context
```

### 6.4 Runtime telemetry diagnoses failed recall

Invocation Telemetry can distinguish:

```text
capture failure
  claim/evidence was never recorded

retention failure
  it was recorded but no durable record survived

retrieval failure
  durable evidence existed but retrieval did not surface it

context-selection failure
  retrieval surfaced it but budget/filtering excluded it

context-lifecycle failure
  it was exposed earlier but later compacted/truncated away

context-assembly failure
  a tool produced it but it did not enter the next auditable exposure

workspace-staleness failure
  the invocation was grounded against the wrong repository tree
```

---

## 7. Team-member departure

The architecture is specifically useful when a contributor leaves because captured claims and evidence are attached to repository history rather than the contributor's account.

```text
Alice discovers an obscure deployment constraint
        ↓
session/decision captures the claim and evidence
        ↓
session-end distillation emits a cited gotcha
        ↓
Memory retains a bounded projection
        ↓
Alice leaves
        ↓
Bob or a future agent receives the claim
        ↓
structured dependencies are checked against current tree
        ↓
current Context Pack grounds the task in current evidence
```

The repository can continue to answer:

```text
Why did a previous contributor believe this?
Which signed session/decision recorded it?
Which repository objects supported the claim then?
Have those objects changed?
Was the claim/evidence recalled for a later invocation?
```

It cannot recover what Alice never recorded.

---

## 8. Product behavior

A session-end review SHOULD prefer a few high-quality entries with provenance and structured evidence over a large prose summary.

A useful Memory row might show:

```text
gotcha  Worker auth tests require production policy fixture
source  session 0198f2aa · decision 0198...
evidence  2 blobs · 1 gitlink
state   ⚠ config/policy.json changed since observation
```

The UI should say:

> `config/policy.json` changed since this claim was recorded; revalidate the claim.

not:

> This memory is false.

### 8.1 Useful audit questions

```text
What reusable claims were captured in this session?
Which durable records do they cite?
Which structured evidence dependencies still match the current tree?
Was this claim retrieved for a later task?
Was supporting evidence present in that invocation's ContextRender?
If not, where in Capture → Retention → Recall did it disappear?
```

---

## 9. Documentation hierarchy

These documents have distinct responsibilities:

```text
agents.md
  existing membership + session lifecycle

knowledge-durability.md
  product objective: Capture → Retention → Recall

content-pack.md
  normative repository + exposure provenance

invocation-telemetry.md
  normative runtime trace model

telemetry-integration.md
  non-normative harness/API/UI guidance
```

The architecture notes SHOULD link to the protocol specs rather than duplicate normative wire rules.

---

## 10. Success criteria

The architecture is successful when:

1. captured repository-specific claims survive contributor/account loss;
2. claims remain attributable to signed provenance;
3. citations are not misrepresented as proof of truth;
4. structured blob/gitlink dependencies allow machine staleness checks where possible;
5. a changed dependency triggers revalidation rather than automatic falsification;
6. Repository Memory remains a bounded projection, not an authority source;
7. future humans/agents receive relevant bounded Memory and task-specific current evidence;
8. an auditor can distinguish capture, retention, retrieval, selection, lifecycle, assembly, and stale-state failures;
9. raw transcripts are not required for durable repository knowledge;
10. unrecorded tacit knowledge remains explicitly outside the guarantee.

---

## Final invariant

> **Git+ can make captured repository knowledge durable, attributable, and recallable, but citations prove provenance rather than truth. Structured blob and gitlink dependencies let a future reader detect when supporting repository evidence changed and revalidate the claim. Signed sessions retain the durable index, Repository Memory remains a bounded projection, Context Packs ground current work, and the audit trace explains where recall failed.**
