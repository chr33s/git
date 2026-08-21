# Git-Native Knowledge Durability

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-21  

## 1. Summary

A repository should retain the operational knowledge necessary for the next competent human or agent to continue the work, even when the previous contributor, model session, or hosting platform is gone.

This document describes how the existing Git-native agent primitives combine to reduce repository knowledge loss.

The problem has three distinct stages:

```text
Capture
  Did somebody record the knowledge?

      ↓

Retention
  Does it survive the person, session, tool, or host?

      ↓

Recall
  Does the next human or agent receive it when relevant?
```

No one primitive solves all three. The architecture works by composing session provenance, decisions, repository Memory, Context Packs, Context Exposure Events, and Invocation Telemetry.

The goal is **durable repository knowledge**, not exhaustive recording of human thought or model cognition.

---

## 2. Problem

Repository knowledge is frequently lost when:

- a contributor leaves the team;
- an agent session ends;
- a hosting provider or chat transcript disappears;
- a decision is made in conversation but never attached to the code it affected;
- an agent discovers a repository-specific convention or gotcha but the next agent starts from zero;
- context existed earlier in a session but was later truncated or compacted away;
- a useful tool result never entered a later model invocation;
- a repository evolves and old operational knowledge is not revalidated.

The failure is often described as "knowledge transfer", but there are two different questions:

```text
Did the repository retain the knowledge?

and

Did the next contributor receive the retained knowledge when it mattered?
```

The first is a retention problem. The second is a recall problem.

---

## 3. Scope

This architecture is intended to preserve **captured repository-specific operational knowledge**, including:

- why a change exists;
- which constraint or decision shaped it;
- conventions repeatedly observed across work;
- repository-specific gotchas;
- friction that repeatedly wastes agent or human time;
- tests and configuration that constrain an implementation;
- the repository context available to an agent when it acted;
- whether context later disappeared through compaction, truncation, or assembly failures.

It does not claim to preserve knowledge that never crosses a repository workflow boundary.

Examples outside the guarantee include:

- private verbal conversations that are never recorded;
- private chat discussions never referenced by repository provenance;
- business context nobody writes down;
- undocumented customer dependencies known only to an individual;
- model internal reasoning or attention;
- human tacit skill that cannot be expressed as repository knowledge.

The system can make captured knowledge durable. It cannot recover knowledge that nobody captured.

---

## 4. Capture

Capture answers:

> **Did the repository workflow record the knowledge while it was available?**

The canonical capture surfaces are intentionally small.

### 4.1 Sessions capture work context

Signed session provenance records the durable execution skeleton:

```text
who was working
what was asked
what was planned or summarized
what was produced
what session was resumed
what outcome occurred
```

Raw transcripts remain disposable side objects. The canonical session record is the durable index, not the corpus.

### 4.2 Decisions capture judgement

A decision that materially affects implementation SHOULD be represented as a repository decision event rather than left only in chat or memory.

The useful chain is:

```text
decision.requested
       ↓
decision.resolved
       ↓
session.produced
       ↓
commit / pull request
```

This preserves not only the answer but its causal position in the repository workflow.

### 4.3 Distillation captures reusable knowledge

At useful lifecycle boundaries, especially session close, a harness or distiller SHOULD identify repository-specific knowledge worth carrying forward.

Recommended durable knowledge classes are:

```text
convention
  how this repository normally does something

gotcha
  a surprising constraint or failure mode

decision
  a resolved judgement worth not re-asking

friction
  a repeated source of wasted effort or tool failure
```

A distillation entry MUST cite the session, decision, commit, test, or other repository evidence from which it was derived.

A knowledge entry without citations is advisory prose, not durable repository knowledge.

### 4.4 Capture should happen at hooks

Logical harness capture points are useful because the knowledge is cheapest to record while the harness still has the relevant context:

```text
session start
  capture standing instructions and repository policy

before / after invocation
  capture context exposure and runtime facts

before / after tool
  capture success, failure, truncation, and result identity

after workspace mutation
  capture the resulting repository tree

session close
  distill convention / gotcha / decision / friction
```

These are logical capture points, not dependencies on any vendor-specific hook API.

---

## 5. Retention

Retention answers:

> **Does captured knowledge survive the contributor, model session, harness, or hosting provider?**

### 5.1 Canonical knowledge is Git-native

Durable facts live in Git objects and signed repository refs so that they:

- replicate with repository provenance;
- remain inspectable without the original hosting provider;
- retain signer and causal history;
- survive the original contributor leaving;
- can be validated against exact Git objects.

### 5.2 Raw transcripts are not canonical

Raw transcripts, full tool output, provider envelopes, scratchpads, and similar payloads are high-volume and high-risk content.

They SHOULD remain disposable side objects referenced by digest when needed.

The repository must remain useful even when those side objects expire.

> **The refs are the index, not the corpus.**

### 5.3 Repository Memory is a projection, not truth

Repository Memory is a bounded, regenerable projection of cited durable records.

It SHOULD optimize for what a future contributor is likely to need, rather than trying to preserve everything forever.

Memory can be rebuilt, compacted, deduplicated, and have stale entries evicted without destroying the underlying provenance from which those entries were derived.

### 5.4 Forgetting can be correct

Knowledge durability does not mean permanent promotion of every observation.

Repository knowledge may become stale because:

- the implementation changed;
- a migration completed;
- a convention was replaced;
- a dependency or build system changed;
- the cited source was redacted.

Memory therefore SHOULD support mechanical forgetting while keeping historical provenance inspectable where retention policy permits.

---

## 6. Recall

Recall answers:

> **Does the next competent human or agent receive the retained knowledge when it is relevant?**

Retention without recall still produces repeated rediscovery.

### 6.1 Session-start recall

A new session SHOULD receive a bounded repository Memory projection alongside standing instructions and policy.

Memory is data, not authority. A retrieved memory entry MUST NOT become an instruction merely because it is relevant or frequently observed.

### 6.2 Task-specific recall

[Context Packs](content-pack.md) provide task-specific grounding against an exact repository tree.

They answer which immutable repository evidence was associated with an invocation and allow an auditor to verify that each item belongs to the claimed repository view.

Retrieval quality may evolve independently. The durable guarantee is evidence provenance.

### 6.3 Exposure proves the recall boundary

A Context Exposure Event commits to the exact `ContextRender` artifact that crossed the harness context-to-invocation boundary.

This distinguishes:

```text
knowledge existed in repository Memory

from

knowledge was actually present in this invocation's repository context
```

That distinction is essential when diagnosing apparent knowledge loss.

### 6.4 Runtime telemetry diagnoses failed recall

[Invocation Telemetry](invocation-telemetry.md) records observable runtime conditions such as:

- provider-reported token usage;
- context-window limits;
- compaction and truncation;
- retries and fallback models;
- tool-result inclusion or omission;
- workspace transitions.

This allows an audit to distinguish:

```text
capture failure
  the knowledge was never recorded

retention failure
  it was recorded but no durable record survived

retrieval failure
  durable knowledge existed but retrieval did not surface it

context-selection failure
  retrieval surfaced it but budget/filtering excluded it

context-lifecycle failure
  it was exposed earlier but later compacted or truncated away

context-assembly failure
  a tool produced it but it did not enter the next invocation

workspace-staleness failure
  the invocation was grounded against the wrong repository state
```

This taxonomy is more actionable than treating all of the above as "the agent hallucinated".

---

## 7. Architecture map

The main primitives align to the durability stages as follows:

| Problem | Primitive |
|---|---|
| Capture intent and work | signed session events |
| Capture judgement | decision events |
| Capture reusable learnings | session-close distillation |
| Retain history | Git-native session refs and objects |
| Retain reusable knowledge | repository Memory projection with citations |
| Ground current work | Repository View + Context Pack |
| Verify what crossed into an invocation | ContextRender + Context Exposure Event |
| Diagnose failed recall | Invocation Telemetry |
| Detect stale repository state | workspace tree transitions |

The full path is:

```text
human / agent discovers something
        ↓
session or decision captures it
        ↓
Git-native provenance retains it
        ↓
distiller promotes reusable knowledge into Memory
        ↓
new session retrieves Memory + task-specific evidence
        ↓
Context Pack grounds evidence in an exact Git tree
        ↓
Context Exposure records what crossed into the invocation
        ↓
Invocation Telemetry explains runtime loss or pressure
        ↓
next operation remains auditable
```

---

## 8. Team-member departure

The architecture is specifically useful when a contributor leaves because durable knowledge is attached to repository history rather than to the contributor's account.

For example:

```text
Alice discovers an obscure deployment constraint
        ↓
her session cites the failing config and resolution
        ↓
session close distills a cited gotcha
        ↓
repository Memory retains the gotcha
        ↓
Alice leaves the team
        ↓
Bob or a future agent starts a related task
        ↓
the cited knowledge is recalled and grounded against current Git state
```

The repository can continue to answer:

```text
Why does this code exist?
Who made the relevant decision?
What repository evidence supported it?
What did previous contributors repeatedly learn?
Was that knowledge available to the agent that later changed the code?
```

The contributor's continued availability is no longer required for those captured facts.

### 8.1 What departure still loses

The architecture does not eliminate loss of tacit knowledge that never entered the workflow.

A team that wants better durability therefore needs both:

```text
good capture surfaces
+
good capture discipline
```

The protocol can make recording cheap, attributable, and durable. It cannot force a contributor to articulate something they never record.

---

## 9. Product behavior

The product SHOULD optimize for making high-value capture cheap rather than encouraging exhaustive logging.

At session close, a harness SHOULD be able to propose or automatically distill a small set of cited learnings:

```text
conventions discovered
new gotchas
resolved decisions
repeated friction
```

A useful review surface would show each proposed entry with its citations and allow policy to decide whether it becomes repository Memory.

The product SHOULD prefer one high-quality cited entry over a large uncited summary.

### 9.1 Useful audit questions

A knowledge audit SHOULD make it possible to ask:

```text
What knowledge was captured during this session?
Which memory entries cite this session or decision?
Which entries are now stale against the current tree?
Was this knowledge retrieved for a later task?
Was it present in that invocation's ContextRender?
If not, where in capture → retention → recall did it disappear?
```

---

## 10. Success criteria

The architecture is successful when:

1. a contributor can leave without making captured repository-specific knowledge depend on their account or memory;
2. reusable knowledge can be traced to signed sessions, decisions, commits, or repository evidence;
3. repository Memory can be rebuilt from cited durable records;
4. a future human or agent can receive bounded reusable knowledge at session start;
5. task-specific evidence can be grounded in an exact Repository View;
6. an auditor can determine whether retained knowledge actually crossed into a relevant model invocation;
7. an auditor can distinguish capture, retention, retrieval, selection, lifecycle, assembly, and stale-state failures;
8. raw transcript retention is not required for the repository to preserve the durable knowledge index;
9. stale or redacted knowledge can leave the active Memory projection without rewriting canonical history;
10. none of these guarantees claim to preserve unrecorded tacit knowledge or model cognition.

---

## Final invariant

> **A repository should retain the operational knowledge necessary for the next competent human or agent to continue the work, even when the previous contributor, model session, or hosting platform is gone. The system achieves this only for knowledge that crosses a capture boundary: signed sessions and decisions capture it, Git-native provenance retains it, repository Memory distills it, Context Packs and Context Exposure make recall auditable, and Invocation Telemetry explains where recall failed. Unrecorded tacit knowledge remains outside the guarantee.**
