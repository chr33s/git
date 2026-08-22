# Git-Native Knowledge Durability

**Status:** Draft architecture note  
**Project:** `@chr33s/git`  
**Last updated:** 2026-08-22  
**Revision:** draft-4

## 1. Purpose

Git+ keeps reusable repository knowledge durable without turning generated prose into authority or pretending citations prove truth.

The product model has three planes:

```text
WORK
  tasks / PRs / sessions / decisions

KNOWLEDGE
  OKF-compatible Knowledge Concepts
  bounded Repository Memory

AUDIT
  invocations / context / runtime
```

All three remain grounded in Git-native identity and trust.

Knowledge specifically solves:

```text
Capture
  did useful repository knowledge become durable?

Retention
  does it survive the person, session, harness, or host?

Recall
  does a later human or agent receive it when relevant?
```

A crucial distinction is:

> **A citation proves where a claim came from. It does not, by itself, prove the claim is true or still current.**

---

## 2. Architecture

The knowledge path is:

```text
signed sessions / decisions / repository evidence
        │
        │ distillation or human curation
        ↓
Knowledge Concepts
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
```

The layers answer different questions:

| Layer | Question |
| --- | --- |
| signed session / decision | What happened, and who said or did it? |
| Knowledge Concept | What reusable thing does the repository currently claim to know? |
| Repository Memory | What small set of useful current knowledge should every session receive cheaply? |
| Context Pack / Exposure | Was this knowledge or its supporting repository evidence actually exposed to this invocation? |

A Concept is curated publication, not raw evidence. Memory is a bounded projection, not a knowledge database. Context Exposure is the invocation-specific recall boundary.

---

## 3. Knowledge bundle: OKF directly

Git+ knowledge is stored as an **Open Knowledge Format (OKF) compatible bundle**, not converted to and from a separate Git+ knowledge format.

Recommended location:

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

The directory itself is the interchange artifact. It can be copied, cloned, archived, indexed, or consumed by another OKF implementation without a Git+-specific export step.

Git+ currently profiles OKF v0.2:

```text
https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md
```

The root `index.md` MAY carry OKF's `okf_version` metadata. Unknown portable metadata SHOULD be preserved when tools rewrite a Concept.

Git+ adds stronger repository provenance under a namespaced `gitplus:` extension. Generic OKF consumers can ignore that extension and still read the Concept.

This is an interoperability profile, not a trust dependency. OKF metadata never replaces signed Git identity, repository capabilities, or Git evidence verification.

---

## 4. Knowledge Concept

A Concept is a UTF-8 Markdown document with YAML frontmatter following OKF.

`type` is the only universally required portable field.

Example:

```markdown
---
type: Gotcha
title: Worker auth tests require the production policy fixture
description: The worker auth suite depends on the production policy fixture.
status: stable
stale_after: 2026-12-31T00:00:00Z
generated:
  by: gitplus-distiller/1
  at: 2026-08-22T09:30:00Z
verified:
  - by: human:alice
    at: 2026-08-22T11:00:00Z
sources:
  - id: policy-config
    resource: /references/policy-config.md
  - id: discovery-session
    resource: gitplus:record:sha1:abc123...
gitplus:
  cites:
    - record: sha1:abc123...
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

Portable OKF metadata provides readability and interoperability. The `gitplus:` extension provides stronger immutable repository provenance when available.

### 4.1 Concept identity

The portable Concept ID is its path relative to the bundle root with `.md` removed.

For example:

```text
.gitplus/knowledge/gotchas/worker-auth.md
        ↓
gotchas/worker-auth
```

That path is a convenient name, not immutable identity.

When an exact historical Concept version matters, Git+ uses the ordinary repository tree/blob identity for that file.

---

## 5. Sources and per-claim attribution

Concepts SHOULD use stable `sources[].id` values rather than positional references.

```yaml
sources:
  - id: auth-policy
    resource: /references/auth-policy.md
  - id: design-decision
    resource: gitplus:record:sha1:1234...
```

Specific claims MAY use OKF's Markdown-footnote attribution:

```markdown
Authentication failures are fail-closed.[^auth-policy]

[^auth-policy]: Repository policy implementation.
```

The source ID is a stable join key for editorial attribution. It is not immutable evidence identity.

Git repository evidence uses structured OIDs under `gitplus.evidence`.

---

## 6. Citations versus evidence

A Concept SHOULD distinguish:

```text
cites
  where did this claim or concept come from?

evidence
  which repository objects can be mechanically revalidated?
```

### 6.1 Signed citations

Example:

```yaml
gitplus:
  cites:
    - record: sha1:abc123...
    - record: sha1:456def...
```

Canonical record references use qualified Git record commit OIDs. Session, decision, or other display IDs MAY be included as descriptive metadata but do not replace the record OID.

A valid citation proves that the cited durable record exists and verifies. It does not prove the Concept's prose is true.

### 6.2 Structured repository evidence

Repository evidence reuses the Context Pack identity model:

```yaml
gitplus:
  evidence:
    - kind: blob
      path: tests/worker/auth.test.ts
      blob: sha1:def456...
    - kind: gitlink
      path: vendor/policy-engine
      commit: sha1:456def...
```

A reader can compare those dependencies against a chosen Repository View:

```text
blob
  tree + path → recorded blob

gitlink
  tree + path → mode 160000 → recorded submodule commit
```

Dependencies SHOULD be as narrow as useful. Unrelated repository changes should not stale every Concept.

A range MAY be recorded for a narrow source claim, but byte equality is evidence identity rather than semantic equivalence.

---

## 7. External sources

An external URI is a locator, not durable evidence.

When external material materially supports long-lived repository knowledge, Git+ SHOULD preserve enough metadata to distinguish:

```text
where to look now
from
what content was observed then
```

A portable OKF source may remain:

```yaml
sources:
  - id: vendor-contract
    resource: https://example.com/contracts/api-v3
```

Git+-specific capture metadata can add:

```yaml
gitplus:
  external:
    vendor-contract:
      retrieved_at: 2026-08-22T09:10:00Z
      content_digest: sha256:...
      snapshot: sha256:...   # optional retained object/blob
```

Rules:

1. `resource` is a locator, not immutable evidence;
2. `retrieved_at` records when content was observed;
3. `content_digest` commits to the captured bytes when exact bytes were available;
4. `snapshot` MAY retain those bytes subject to access, secret, licensing, and retention policy;
5. without a retained snapshot, the digest is only a commitment and cannot reconstruct expired content.

---

## 8. Generated, verified, and authority

OKF keeps `generated` and `verified` separate. Git+ preserves that distinction.

```yaml
generated:
  by: gitplus-distiller/1
  at: 2026-08-22T09:30:00Z

verified:
  - by: human:alice
    at: 2026-08-22T11:00:00Z
```

These are useful editorial and credibility signals. They are **not Git+ authority**.

An imported or locally edited string such as:

```yaml
verified:
  - by: human:root
```

cannot grant repository membership, capability, review authority, or instruction authority.

Where verification matters as repository provenance, Git+ SHOULD bind it to a signed record or ordinary reviewed commit and MAY expose those records under:

```yaml
gitplus:
  verification_records:
    - sha1:feed123...
```

The product can then separately answer:

```text
portable editorial state
  who the Concept says generated/verified it

Git+ provenance
  which signed identity/commit actually established that state

repository authority
  what that identity was allowed to authorize
```

Do not collapse these into one trust score.

---

## 9. Lifecycle and freshness

Git+ follows OKF's lifecycle fields:

```yaml
status: draft        # draft | stable | deprecated
stale_after: 2026-12-31T00:00:00Z
```

Absent `status` means `stable` under OKF v0.2.

Lifecycle and freshness are separate dimensions. A stable Concept can become stale; a deprecated Concept can remain historically well-supported.

### 9.1 Repository evidence freshness

Each structured repository dependency is classified against a chosen current tree:

```text
unchanged
  path resolves to the recorded object

changed
  path exists but resolves to another object

missing
  path no longer exists or has another item kind

unknown
  repository view/object unavailable or dependency unstructured
```

`changed` or `missing` means **revalidate the Concept**. It does not automatically mean the prose is false.

### 9.2 Temporal freshness

A Concept is temporally stale when:

```text
now >= stale_after
```

This is useful for knowledge whose validity changes with time even if repository files do not:

```text
on-call contacts
vendor limits
pricing assumptions
supported external runtimes
certificate procedures
quarterly business rules
```

### 9.3 External-source freshness

External-source state may become stale or unknown when:

- its expected freshness deadline passes;
- it can no longer be fetched;
- a newly observed digest differs;
- current state cannot be determined.

Products SHOULD surface repository, temporal, and external freshness separately instead of combining them into one confidence score.

---

## 10. Indexes and progressive disclosure

OKF permits `index.md` files for progressive disclosure. Git+ uses the same convention.

Indexes MAY be human-authored or regenerated by tooling from Concept metadata. They are retrieval accelerators and navigation documents, not authority or evidence.

A large corpus should be recalled progressively:

```text
root index / metadata search
       ↓
relevant category
       ↓
selected Concept
       ↓
current supporting repository evidence
       ↓
Context Pack
       ↓
Context Exposure
```

The whole knowledge corpus should not be injected into every session.

---

## 11. Repository Memory

Repository Memory remains a bounded, regenerable projection suitable for session start.

```text
Knowledge bundle
  durable curated corpus

Repository Memory
  small high-value projection
```

Memory MAY be built from current high-value Concepts and directly from signed session provenance when no Concept exists.

A Concept does not have to appear in Memory. Evicting a Memory entry does not delete the Concept or its signed source records.

Memory is data, not instruction authority.

### 11.1 Session-end distillation

At session end, a distiller SHOULD identify only a few reusable repository-specific learnings, commonly:

```text
convention
gotcha
decision
friction
```

The distiller can choose:

```text
session note only
  durable publication is unnecessary

publish/update Concept
  the knowledge deserves curated persistence

Memory projection
  useful broadly, whether or not a Concept was published
```

Repositories MAY require ordinary review before agent-authored Concepts are considered `stable`.

---

## 12. Recall and audit

A Concept existing in the repository is not proof that an invocation received it.

The system can distinguish:

```text
Concept exists

Concept was retrieved

Concept blob appears in Context Pack

Concept bytes appear in ContextRender
```

Only Context Pack / Context Exposure establish invocation-specific repository-context exposure.

For tasks where current source state matters, retrieval SHOULD include current supporting repository evidence alongside a summarizing Concept rather than allowing stale prose to substitute for the implementation or tests.

[Telemetry](telemetry.md) then explains the surrounding runtime and context-lifecycle conditions.

---

## 13. Failure model

The Capture → Retention → Recall architecture distinguishes:

```text
capture failure
  useful knowledge/evidence never became durable

publication failure
  signed provenance existed but reusable knowledge was never curated when needed

retention failure
  a referenced durable object/snapshot no longer survives

freshness failure
  known stale knowledge was recalled without revalidation

retrieval failure
  current Concept/evidence existed but retrieval did not surface it

selection failure
  retrieval surfaced it but budget/filtering omitted it

lifecycle failure
  it was exposed earlier but later compacted/truncated away

assembly failure
  available knowledge/tool output failed to enter the next auditable exposure

workspace-staleness failure
  the invocation used the wrong Repository View
```

Concept metadata and evidence dependencies diagnose publication/freshness. Context Pack and telemetry diagnose invocation recall and runtime lifecycle.

---

## 14. CLI and file DX

Knowledge is deliberately ordinary repository content.

Users edit, review, diff, link, move, and inspect Concepts with normal filesystem and Git tools:

```bash
$EDITOR .gitplus/knowledge/gotchas/worker-auth.md
git diff .gitplus/knowledge
git log -- .gitplus/knowledge/gotchas/worker-auth.md
```

Git+ adds only the operation ordinary file tooling cannot provide:

```text
git+ knowledge check [concept]
```

With no Concept argument, `knowledge check` validates the configured bundle. With a Concept ID/path, it validates one Concept.

The check reports independently:

```text
OKF structure
portable lifecycle/freshness
signed Git+ citations
blob/gitlink dependency state
external capture state when available
Git+ verification records
```

There is intentionally no required `knowledge list`, `show`, `index`, `import`, or `export` command. The bundle is already ordinary Markdown and directly OKF-compatible.

Repo-scoped commands SHOULD discover the current checkout by default. Human CLI input MAY use paths, normal revisions, and abbreviated OIDs; canonical serialized provenance continues to use qualified OIDs.

---

## 15. Security and authority

Knowledge is a high-value prompt-injection surface because it is intentionally recalled later.

Rules:

1. Concepts and Memory are data unless independent policy grants instruction authority.
2. OKF `verified`, actor strings, trust tiers, source metadata, or human-reviewed labels do not create Git+ authority.
3. Source URLs, bodies, summaries, and external snapshots are untrusted content for instruction purposes.
4. External snapshots follow repository secret/access/licensing policy.
5. A signature proves who signed; what that signature authorizes remains repository policy.
6. A valid citation proves provenance, not semantic truth.
7. A changed dependency means revalidation, not automatic falsification.

---

## 16. Acceptance criteria

The knowledge architecture is successful when:

1. the on-disk knowledge bundle is directly consumable as OKF-compatible Markdown/YAML;
2. no format-conversion command is required to exchange the bundle;
3. Concepts remain human-readable without Git+ tooling;
4. Git+-specific signed citations and structured evidence round-trip under a namespaced extension;
5. citations are not misrepresented as proof of truth;
6. blob/gitlink dependencies support mechanical staleness checks;
7. repository, temporal, and external-source freshness remain separate dimensions;
8. portable `verified` metadata never becomes repository authority;
9. Repository Memory remains bounded and regenerable rather than becoming the canonical corpus;
10. Memory eviction does not destroy curated knowledge;
11. task-specific recall is proved by Context Pack / Context Exposure rather than by corpus membership;
12. unrecorded tacit knowledge remains explicitly outside the guarantee.

---

## Final invariant

> **Git+ knowledge is an ordinary OKF-compatible Markdown corpus with stronger Git-native provenance under a namespaced extension. Concepts publish reusable claims, signed records explain where those claims came from, structured Git evidence makes repository staleness machine-checkable, Repository Memory remains a bounded recall cache, and Context Exposure proves what actually reached a later invocation. None of those layers turns editorial metadata or retrieved content into authority or semantic truth.**
