# Git-Native Knowledge Durability

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-22  
**Revision:** draft-3

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

No one primitive solves all three.

Git+ composes five layers:

```text
signed sessions / decisions / repository evidence
        │
        │ durable provenance
        ↓
Durable Knowledge Concepts
        │
        ├──────────────→ bounded Repository Memory
        │
        └──────────────→ task-specific retrieval
                                │
                                ↓
                           Context Pack
                                │
                                ↓
                         Context Exposure
                                │
                                ↓
                           Telemetry
```

The layers answer different questions:

| Layer | Question |
| --- | --- |
| signed session / decision | What happened, and who said or did it? |
| Durable Knowledge Concept | What reusable thing does the repository currently claim to know? |
| Repository Memory | What small set of knowledge should every new session receive cheaply? |
| Context Pack / Exposure | What exact Git-grounded context crossed this invocation boundary? |
| Telemetry | Under what observable runtime conditions did the invocation occur? |

The goal is **durable, attributable, recallable repository knowledge**, not exhaustive recording of human thought or model cognition.

A crucial distinction remains:

> **A citation proves where a claim came from. It does not, by itself, prove the claim is true or still current.**

---

## 2. Why add Durable Knowledge Concepts

The existing Repository Memory design is intentionally a bounded projection cache. It is small enough to inject at session start, regenerable from durable records, and allowed to evict stale or low-value entries.

Those properties are correct for recall, but they make Memory the wrong place for a larger curated knowledge corpus.

The repository also needs an optional durable unit for knowledge that should:

- remain independently readable after the discovering session ages out;
- be curated by humans or agents;
- carry explicit sources and lifecycle metadata;
- be diffable and reviewable in ordinary Git history;
- support progressive disclosure rather than being loaded whole;
- survive Memory eviction;
- interoperate with external knowledge tools without weakening Git+ provenance.

That unit is a **Durable Knowledge Concept**.

A Concept is publication, not raw evidence. Its claims remain attributable to the signed records, repository objects, and external captures from which it derives.

---

## 3. Architectural roles

### 3.1 Signed provenance is the historical record

Sessions and decisions retain durable work provenance such as:

```text
who was working
what was asked
what was produced
what decisions were requested/resolved
what compact note the session emitted
aggregate usage when available
```

High-frequency invocation/tool telemetry stays in `refs/hub/trace/<session>` rather than the policy-critical session DAG.

Material judgement SHOULD become signed decision provenance rather than remain only in chat.

### 3.2 Durable Knowledge Concepts are curated repository knowledge

A Knowledge Concept captures a reusable repository claim or body of knowledge such as:

```text
architecture rationale
repository convention
gotcha
operational playbook
external-system dependency
migration state
business/domain rule
known friction
important decision summary
```

Concepts are intended to be human-readable and agent-readable without proprietary tooling.

They MAY be authored manually, generated from sessions, or maintained by an agent, but their provenance must remain explicit.

### 3.3 Repository Memory is a bounded projection

Repository Memory remains:

```text
small
regenerable
disposable
session-start friendly
not an authority source
```

Memory SHOULD be derived from the highest-value current Concepts and/or directly from signed provenance when no Concept exists.

A Concept does not have to appear in Memory, and eviction from Memory does not delete the Concept.

### 3.4 Context Packs prove task-specific recall

A Concept existing in the repository is not proof that an invocation received it.

Task-specific retrieval may select:

- a Knowledge Concept;
- its current supporting source files;
- related tests/configuration;
- other repository evidence.

Those repository blobs then appear in the invocation's [Context Pack](context-pack.md) when exposed.

A Context Exposure record proves the harness-side recall boundary for that invocation.

### 3.5 Telemetry diagnoses failed recall

[Telemetry](telemetry.md) distinguishes whether useful knowledge disappeared through capture, retention, retrieval, selection, lifecycle, assembly, compaction, truncation, or stale workspace state.

---

## 4. Knowledge bundle

A repository MAY maintain a human-readable knowledge bundle in ordinary source history.

Recommended default:

```text
.gitplus/knowledge/
  index.md
  architecture/
    index.md
    auth-policy.md
  conventions/
  gotchas/
  playbooks/
  decisions/
```

The path is a product convention, not a Git object-format requirement. Repositories MAY choose another configured location.

Keeping the bundle in ordinary repository history gives it:

- normal Git diffs;
- branch/review workflow;
- commit attribution;
- easy cloning and inspection;
- direct eligibility for Context Pack blob evidence;
- no requirement for a separate knowledge database.

The bundle is not an instruction namespace. Knowledge remains data unless existing harness/repository policy independently grants a particular file instruction authority.

---

## 5. Knowledge Concept format

Git+ SHOULD use an intentionally boring, portable representation: UTF-8 Markdown with YAML frontmatter.

This is compatible in spirit and, where practical, structurally compatible with the **Open Knowledge Format (OKF)**:

```text
https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md
```

Git+ does not require OKF for correctness. OKF is an interoperability target, not a trust root or protocol dependency.

### 5.1 Minimal Concept

Example:

```markdown
---
type: Gotcha
title: Worker auth tests require the production policy fixture
status: stable
generated:
  by: gitplus-distiller/1
  at: 2026-08-22T09:30:00Z
sources:
  - id: discovery-session
    resource: gitplus:record:sha1:abc123...
  - id: policy-config
    resource: config/policy.json
gitplus:
  evidence:
    - kind: blob
      path: tests/worker/auth.test.ts
      blob: sha1:def456...
    - kind: blob
      path: config/policy.json
      blob: sha1:789abc...
---

The worker authentication tests require the production policy fixture.[^policy-config]

[^policy-config]: Current repository configuration supporting this claim.
```

### 5.2 Required and optional shape

Git+ SHOULD keep the portable core small.

Recommended portable fields include:

```text
type
  concept type; required for OKF-compatible export

title
  human-readable title

description
  short retrieval/index description

tags
  cross-cutting categorization

status
  draft | stable | deprecated

generated
  who/what authored the current content and when

verified
  editorial/source-check history

stale_after
  optional absolute temporal revalidation deadline

sources
  stable source IDs and locators
```

Git+-specific provenance SHOULD live under a namespaced extension such as:

```yaml
gitplus:
  cites: ...
  evidence: ...
  verification_records: ...
```

Consumers that do not understand the extension can still read the Markdown Concept.

---

## 6. Stable source IDs and per-claim attribution

Source arrays are frequently reordered by agents and formatters. Positional references such as `sources[0]` are therefore fragile.

Concept sources SHOULD use stable IDs:

```yaml
sources:
  - id: auth-policy
    resource: src/auth/policy.ts
  - id: design-decision
    resource: gitplus:record:sha1:1234...
```

Specific claims MAY use Markdown footnotes keyed to those source IDs:

```markdown
Authentication failures are fail-closed.[^auth-policy]

[^auth-policy]: Repository policy implementation.
```

The source ID is a join key, not evidence identity.

For Git repository evidence, immutable identity comes from the structured Git+ evidence dependency described below.

---

## 7. Provenance: citations versus evidence

A Knowledge Concept SHOULD separate:

```text
cites
  where did this claim or concept come from?

evidence
  which repository objects can be mechanically revalidated?
```

### 7.1 Signed citations

Example:

```yaml
gitplus:
  cites:
    - session: 0198f2aa-...
      record: sha1:abc123...
    - decision: 0198f312-...
      record: sha1:456def...
```

Later canonical-record references SHOULD use qualified Git record commit OIDs, not only display/event UUIDs.

A cited record being present and valid proves provenance for the citation. It does not prove the Concept's prose is true.

### 7.2 Structured repository evidence dependencies

Example:

```yaml
gitplus:
  evidence:
    - kind: blob
      path: tests/worker/auth.test.ts
      blob: sha1:def456...
    - kind: blob
      path: config/policy.json
      blob: sha1:789abc...
    - kind: gitlink
      path: vendor/policy-engine
      commit: sha1:456def...
```

The locator semantics reuse Context Pack evidence rules:

```text
blob dependency
  comparison tree + path → recorded blob

gitlink dependency
  comparison tree + path + mode 160000 → recorded submodule commit
```

This reuse matters: repository evidence should have one identity model across Knowledge Concepts and Context Packs.

### 7.3 Evidence granularity

Dependencies SHOULD be as narrow as useful.

A path/blob dependency is usually better than pinning the entire tree because unrelated repository changes should not stale every Concept.

A range MAY be recorded for claims about a narrow source excerpt, but exact byte-range equality is evidence identity, not semantic equivalence.

---

## 8. External source capture

An external URI is a locator, not durable evidence by itself.

When an external source materially supports long-lived repository knowledge, Git+ SHOULD record enough information to distinguish:

```text
where to look now
from
what content the Concept was actually derived from then
```

Recommended source metadata is:

```yaml
sources:
  - id: vendor-contract
    resource: https://example.com/contracts/api-v3
    retrieved_at: 2026-08-22T09:10:00Z
    content_digest: sha256:...
    snapshot: gitplus:object:sha256:...   # optional retained snapshot
```

Rules:

1. `resource` is a current locator, not immutable evidence;
2. `retrieved_at` records when the producer observed it;
3. `content_digest` commits to captured bytes when exact bytes were available;
4. `snapshot` MAY retain those bytes under repository access/retention policy;
5. absent snapshot retention, the digest is only a commitment and cannot reconstruct expired source content.

External captures may contain sensitive or licensed content and therefore require explicit retention policy.

---

## 9. Generated versus verified

Who wrote a Concept and who checked it are different facts.

A Concept MAY record portable metadata such as:

```yaml
generated:
  by: gitplus-distiller/1
  at: 2026-08-22T09:30:00Z

verified:
  - by: human:alice
    at: 2026-08-22T11:00:00Z
```

This is useful editorial metadata, but it is not sufficient Git+ authority.

### 9.1 Git+ verification provenance

Where verification matters beyond display, Git+ SHOULD bind the verification to a signed repository record or ordinary reviewed commit provenance.

For example:

```yaml
gitplus:
  verification_records:
    - sha1:feed123...
```

The repository can then answer:

```text
who signed the verification?
was the signer a valid member?
what authority/policy applied?
what Concept blob/version was reviewed?
```

### 9.2 Trust tier is not authority

An imported knowledge format may classify a Concept as human-reviewed or machine-confirmed.

Git+ MUST treat that as an editorial/credibility signal only.

It MUST NOT derive repository authorization from a generic ranking such as:

```text
human-reviewed > machine-confirmed > unverified
```

Repository authority remains a function of signed identity, trust graph, capabilities, and policy.

A designated automated verifier may be more relevant to a repository rule than an arbitrary human reviewer.

---

## 10. Freshness and staleness

Knowledge can become stale for different reasons. Git+ SHOULD keep those reasons separate.

### 10.1 Repository staleness

For each structured evidence dependency, a reader can classify against a chosen current Repository View:

```text
unchanged
  path resolves to the same object

changed
  path exists but resolves to a different object

missing
  path no longer exists or has a different item kind

unknown
  object/view unavailable or dependency was not structured
```

A changed dependency means **revalidate the Concept**. It does not prove the prose is false.

### 10.2 Temporal staleness

Some knowledge expires with time even when repository files do not change.

Examples:

```text
on-call contacts
vendor limits
pricing assumptions
supported external runtime versions
certificate/credential procedures
quarterly business rules
```

A Concept MAY therefore declare an absolute deadline:

```yaml
stale_after: 2026-12-31T00:00:00Z
```

At or after that time, the Concept requires revalidation.

Temporal staleness and repository staleness are independent dimensions.

### 10.3 External-source staleness

An external source may also be stale when:

- its expected freshness deadline passed;
- the source can no longer be fetched;
- a newly fetched digest differs from the captured digest;
- current freshness is unknown.

Again, the state is **needs revalidation**, not automatic falsification.

### 10.4 Product state

A useful projection can display:

```text
source provenance   ✓ signed session exists
repository evidence ⚠ config/policy.json changed
external source     ? current state unknown
temporal freshness  ✓ valid until 2026-12-31
editorial review    human:alice · 2026-08-22
```

These signals SHOULD NOT be collapsed into one opaque confidence score.

---

## 11. Lifecycle

Concept lifecycle is useful independently from evidence freshness.

Recommended states:

```text
draft
  useful but not yet published as current repository guidance

stable
  current published knowledge

deprecated
  retained for history but should not normally be recalled as current
```

A Concept can be `stable` and still become stale due to changed evidence or time.

A deprecated Concept SHOULD link to a replacement when one exists.

Deletion is not required to stop active recall; ordinary Git history preserves prior versions.

---

## 12. Progressive disclosure and indexes

A large knowledge corpus should not be injected into every session.

Directories MAY contain generated `index.md` files containing compact descriptions of available Concepts:

```text
.gitplus/knowledge/index.md
.gitplus/knowledge/architecture/index.md
.gitplus/knowledge/playbooks/index.md
```

Indexes are retrieval accelerators and SHOULD be regenerable from Concept frontmatter.

They are not authority or truth sources.

The intended recall path is:

```text
root knowledge index
       ↓
relevant category index
       ↓
selected Knowledge Concept
       ↓
current supporting repository evidence
       ↓
Context Pack
       ↓
Context Exposure
```

This gives agents progressive disclosure without making the whole corpus fit inside Repository Memory.

---

## 13. Repository Memory after Concepts

Repository Memory remains valuable after adding Concepts.

Its role becomes clearer:

```text
Durable Knowledge Concepts
  full curated corpus

Repository Memory
  bounded high-value projection for every session
```

Memory MAY contain compact entries such as:

```text
gotcha  Worker auth tests require production policy fixture
concept .gitplus/knowledge/gotchas/worker-auth.md
source  session 0198f2aa
state   ⚠ supporting config changed
```

### 13.1 Memory generation

At session end, the harness/distiller SHOULD identify a small number of reusable repository-specific claims:

```text
convention
gotcha
decision
friction
```

The distiller can then choose among:

```text
no durable publication
  session note is sufficient

update/create Concept
  knowledge deserves durable curated publication

Memory-only projection
  useful short-term recall but not worth durable publication yet
```

Repositories MAY require review before agent-generated Concepts become `stable`.

### 13.2 Memory remains data, not authority

A future session SHOULD read bounded Memory beside standing instructions/policy.

Neither Memory nor a Knowledge Concept becomes an instruction merely because it is automatically recalled.

---

## 14. Recall and Context Packs

### 14.1 Session-start recall

A new session receives bounded Memory and MAY receive the root knowledge index.

### 14.2 Task-specific retrieval

Retrieval MAY select relevant Concepts and current repository evidence.

A Concept committed in the active Repository View is ordinary blob evidence and can be represented in a Context Pack using its path/blob identity.

### 14.3 Exposure proves recall

The repository can distinguish:

```text
Concept existed

Concept was selected

Concept blob was present in Context Pack

Concept bytes were actually present in ContextRender
```

Only the latter stages establish invocation-specific context exposure.

### 14.4 Current evidence should accompany important claims

A Concept may summarize historical evidence. For tasks where current source state matters, retrieval SHOULD also include the Concept's current structured repository dependencies when useful.

That prevents a stale prose Concept from substituting for the actual current implementation or tests.

---

## 15. Capture → Retention → Recall failure model

With Concepts, the existing failure taxonomy becomes:

```text
capture failure
  useful knowledge/evidence never entered a durable record or Concept

publication failure
  signed provenance existed but reusable knowledge was never promoted when needed

retention failure
  referenced durable object/snapshot no longer survives

freshness failure
  Concept was recalled despite known stale dependencies/deadline

retrieval failure
  current Concept/evidence existed but retrieval did not surface it

context-selection failure
  retrieval surfaced it but budget/filtering omitted it

context-lifecycle failure
  it was exposed earlier but later compacted/truncated away

context-assembly failure
  a tool or Concept was available but did not enter the next auditable exposure

workspace-staleness failure
  invocation was grounded against the wrong Repository View
```

Telemetry can diagnose the runtime/context-lifecycle stages; Concept metadata and structured dependencies diagnose publication/freshness stages.

---

## 16. Team-member departure

The architecture is specifically useful when a contributor leaves:

```text
Alice discovers an obscure deployment constraint
        ↓
session/decision captures provenance
        ↓
agent or Alice publishes a Knowledge Concept
        ↓
Concept cites signed record + supporting blobs
        ↓
Memory retains a compact projection
        ↓
Alice leaves
        ↓
Bob or future agent finds the Concept
        ↓
dependencies checked against current tree/time
        ↓
current Context Pack grounds the task
```

The repository can continue to answer:

```text
What do we currently claim to know?
Why did we believe it?
Who/what generated and verified the current Concept?
Which signed records support it?
Which repository objects supported it then?
Have those objects changed?
Is the Concept past a temporal freshness deadline?
Was the Concept/evidence actually recalled for a later invocation?
```

It still cannot recover tacit knowledge nobody recorded.

---

## 17. OKF interoperability

Git+ SHOULD support OKF as an interchange surface rather than make it mandatory protocol state.

### 17.1 Why interoperate

OKF's useful properties include:

```text
Markdown + YAML portability
human/agent readability
open concept types
stable source IDs
per-claim source attribution
separate generated / verified metadata
lifecycle + temporal freshness
hierarchical indexes for progressive disclosure
```

Those map naturally onto Durable Knowledge Concepts.

### 17.2 Boundary

The compatibility rule is:

> **OKF describes portable knowledge publication metadata; Git+ supplies stronger Git-native identity, signed provenance, repository-evidence verification, authority, and invocation exposure.**

Git+ MUST NOT weaken its guarantees to fit an interchange format.

### 17.3 Export/import

A product MAY expose:

```text
git+ knowledge export --format okf
git+ knowledge import --format okf
```

Imported Concepts are **data**.

Import MUST NOT grant instruction authority, repository membership, verification authority, or policy capability based on frontmatter strings.

On export, Git+-specific provenance can be preserved under namespaced extension fields that OKF consumers are expected to tolerate as unknown metadata.

### 17.4 Concept IDs

For OKF-compatible bundles, the file path relative to the bundle with `.md` removed can serve as a portable Concept ID.

Git+ SHOULD still use Git blob/tree/commit OIDs when immutable repository identity matters.

Path identity is convenient naming; Git OIDs are immutable evidence identity.

### 17.5 Trust metadata

OKF-style `generated` and `verified` metadata MAY round-trip.

They MUST remain distinct from Git+ repository trust and signed verification provenance.

### 17.6 Attested Computation

OKF's Attested Computation concept is potentially useful for future sanctioned checks or reproducible repository operations.

It is **not** part of this Knowledge Durability V1 architecture.

If adopted later, Git+ should preserve the useful distinction between:

```text
definition verified
  is this computation definition approved/current?

execution attested
  did this particular run execute the sanctioned computation?
```

Execution receipts would naturally integrate with Git+ checks/telemetry rather than with Context Pack identity.

---

## 18. Security and authority

Knowledge is a high-value prompt-injection surface because it is intentionally recalled later.

Rules:

1. Knowledge Concepts and Memory are data unless independent policy grants instruction authority.
2. Imported OKF metadata MUST NOT create authority.
3. Source URLs, Concept bodies, and generated summaries are untrusted content for instruction purposes.
4. External snapshots follow repository secret/access/licensing policy.
5. A verifier signature proves who verified; what that verification authorizes remains repository policy.
6. Stale status should be visible before automatic recall when known.
7. Redaction of cited signed provenance should cause derived active Memory and generated indexes to be rebuilt without the redacted claim where required by policy.

---

## 19. Product behavior

Useful commands may include:

```text
git+ knowledge list
git+ knowledge show <concept>
git+ knowledge verify <concept>
git+ knowledge stale [<concept>]
git+ knowledge import --format okf
git+ knowledge export --format okf
git+ session memory --distill
```

The exact command surface is non-normative here.

A Concept detail view should separate:

```text
Content lifecycle
  stable

Generated
  gitplus-distiller/1 · 2026-08-22

Editorial verification
  human:alice · 2026-08-22

Signed provenance
  session sha1:abc… ✓
  decision sha1:def… ✓

Repository dependencies
  tests/worker/auth.test.ts ✓ unchanged
  config/policy.json       ⚠ changed

Temporal freshness
  valid until 2026-12-31

Recall
  last exposed in invocation sha1:789…
```

The UI should say:

> `config/policy.json` changed since this Concept was supported; revalidate the claim.

not:

> This Concept is false.

---

## 20. Documentation hierarchy

```text
agents.md
  existing membership + session lifecycle + bounded Memory implementation

knowledge-durability.md
  Capture → Retention → Recall
  Durable Knowledge Concepts
  Memory projection
  OKF interoperability

context-pack.md
  normative repository evidence + invocation exposure provenance

telemetry.md
  runtime trace protocol + OTel ingestion + harness/API/UI integration
```

The architecture notes SHOULD link to protocol rules rather than duplicate wire/storage rules.

---

## 21. Success criteria

The architecture is successful when:

1. captured repository-specific claims survive contributor/account loss;
2. reusable knowledge can be published independently of the bounded Memory cache;
3. Concepts remain readable and diffable as ordinary Markdown repository content;
4. Concepts remain attributable to signed provenance without pretending citations prove truth;
5. structured blob/gitlink dependencies support machine revalidation;
6. temporal freshness supports knowledge that expires without repository edits;
7. changed or expired dependencies trigger revalidation rather than automatic falsification;
8. generated and verified metadata remain distinct;
9. editorial trust metadata never substitutes for Git+ identity/authority;
10. external-source provenance distinguishes a locator from captured historical bytes;
11. indexes provide progressive disclosure without becoming authority sources;
12. Repository Memory remains a bounded projection rather than a full knowledge database;
13. Context Packs prove whether selected knowledge/evidence reached a particular invocation;
14. Telemetry diagnoses runtime/context-lifecycle failures independently from knowledge freshness;
15. OKF import/export is possible without making OKF a protocol dependency;
16. imported knowledge never gains instruction authority automatically;
17. raw transcripts are not required for durable repository knowledge;
18. unrecorded tacit knowledge remains explicitly outside the guarantee.

---

## Final invariant

> **Git+ separates historical provenance, durable knowledge publication, bounded recall, invocation exposure, and runtime telemetry. Signed sessions and decisions record what happened; Durable Knowledge Concepts publish reusable claims with stable sources, structured Git evidence, lifecycle, and freshness; Repository Memory is a small regenerable projection; Context Packs prove what Git-grounded knowledge/evidence reached an invocation; and Telemetry explains runtime loss. OKF is a useful portable interchange format for Concepts, but Git-native OIDs, signatures, trust, evidence verification, and authority remain the stronger repository contract.**
