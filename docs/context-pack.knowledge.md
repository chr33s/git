# Knowledge, Memory, and Auditable Recall

## Implementation specification for `experimental/context-pack`

| Field                            | Value                                                                                                                         |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Project                          | `chr33s/git` / Git+                                                                                                           |
| Target branch                    | `experimental/context-pack`                                                                                                   |
| Baseline commit                  | `54969361929568926e555807e6c6bd4942561360`                                                                                    |
| Mainline reconciliation baseline | `759308886f32a98c66526478915074c86e94cd87`                                                                                    |
| Specification revision           | `draft-1`                                                                                                                     |
| Date                             | 2026-09-06                                                                                                                    |
| Status                           | Proposed implementation specification; not a claim of completed implementation                                                |
| Suggested repository path        | `docs/context-pack-implementation.md`                                                                                         |
| Delivery scope                   | Finish the Knowledge → Memory → Recall workflow and harden telemetry conformance on the existing context/audit implementation |

> **Decision:** Continue from `experimental/context-pack`. Preserve its Context Pack, ContextRender, Exposure, trace, and Invocation boundaries. Implement the missing knowledge validation and harness workflow on top; do not introduce a second memory backend or depend on the search-persistence or WAL experiments.

This document specifies the next implementation increment. It supplements—not replaces—the branch's `docs/context-pack.md` draft-9, `docs/telemetry.md` draft-7, and `docs/knowledge.md` draft-5. Those documents remain the protocol references. Existing behavior and proposed behavior are distinguished below. Changes that intentionally alter an existing wire contract require a corresponding protocol-document amendment and compatibility tests; this implementation plan is not permission to change signed historical records. [R1], [R2], [R3]

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** express requirements within this proposed increment. Code examples describe contracts or workflows, not already-shipped commands unless explicitly stated.

## Contents

- [1. Goals and non-goals](#1-goals-and-non-goals)
- [2. Baseline and implementation delta](#2-baseline-and-implementation-delta)
- [3. Architectural invariants](#3-architectural-invariants)
- [4. Domain boundaries and data flow](#4-domain-boundaries-and-data-flow)
- [5. Format profile and Concept handling](#5-format-profile-and-concept-handling)
- [6. Knowledge checking](#6-knowledge-checking)
- [7. Eligibility, uncertainty, and corrections](#7-eligibility-uncertainty-and-corrections)
- [8. Repository Memory](#8-repository-memory)
- [9. Task-specific recall and Context Pack assembly](#9-task-specific-recall-and-context-pack-assembly)
- [10. Harness capture, publication, and recall](#10-harness-capture-publication-and-recall)
- [11. Telemetry semantic-convention hardening](#11-telemetry-semantic-convention-hardening)
- [12. CLI and machine-readable contracts](#12-cli-and-machine-readable-contracts)
- [13. Failure and security requirements](#13-failure-and-security-requirements)
- [14. Resource limits and determinism](#14-resource-limits-and-determinism)
- [15. Suggested module changes](#15-suggested-module-changes)
- [16. Acceptance test matrix](#16-acceptance-test-matrix)
- [17. Delivery sequence and merge gates](#17-delivery-sequence-and-merge-gates)
- [18. Compatibility and migration](#18-compatibility-and-migration)
- [19. Definition of done](#19-definition-of-done)
- [20. Pinned references and provenance](#20-pinned-references-and-provenance)

## 1. Goals and non-goals

The increment is complete when a repository-specific learning can be recorded with provenance, optionally curated into an ordinary OKF Concept, checked against a current Repository View, selected for later work, and included in an auditable invocation boundary.

The implementation MUST distinguish these claims:

```text
A claim was recorded.
A Concept was published.
Its provenance is accepted under the evaluated trust state.
Its declared repository dependencies are unchanged.
It was selected for a task.
Its exact bytes crossed the instrumented invocation boundary.
```

None of those claims proves that the prose is true, that a model understood it, or that it caused an outcome.

In scope:

- `git+ knowledge check`, with independent structure, provenance, freshness, and retention diagnostics.
- Concept-aware, byte-bounded Repository Memory with safe cache reuse.
- Task-specific retrieval of Concepts and their current supporting repository evidence.
- A harness integration contract that connects session learning, Memory, selection, exposure, and runtime records.
- Revision-aware telemetry normalization and regression coverage for the existing audit guarantees.

Not in scope: a Go sidecar, an external vector database, a mandatory MCP server, a general knowledge graph, new knowledge CRUD commands, automatic semantic truth detection, automatic execution of Concept code, a general OTLP receiver, a repository-wide WAL, or bundle-delivery infrastructure. A dedicated web Knowledge browser and persistent retrieval indexes can follow independently. The portable domain behavior MUST NOT depend on them.

The first selector SHOULD remain deterministic and lexical. More sophisticated ranking is a replaceable implementation detail and requires evidence of improvement at a comparable context budget.

## 2. Baseline and implementation delta

The baseline branch already separates curated knowledge, bounded Memory, and invocation-specific audit in its design. Its context/audit implementation must be retained rather than recreated. [R1], [R2], [R3]

| Area                                | Baseline evidence                                                                                                    | Required increment                                                                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Repository Views and typed evidence | `src/context/Pack.ts` defines views and separate blob/gitlink items.                                                 | Reuse for Concept dependencies and selected evidence.                                                 |
| Render commitment                   | `src/context/Render.ts` implements `git+context-render/v1`.                                                          | Preserve framing and add end-to-end boundary tests.                                                   |
| Exposure retention                  | Exposure tests exercise real Git reachability and collection.                                                        | Preserve retained-view guarantees while enforcing visibility and content-retention limits.            |
| Invocation audit                    | Trace and telemetry modules exist separately from session records.                                                   | Keep audit out of policy folds and make mapping-profile claims verifiable.                            |
| Repository Memory                   | `src/hub/Memory.ts` derives a capped summary from `session.produced.note`; citations identify sessions.              | Add exact record citations, Concept inputs, safe invalidation, and UTF-8 budgeting.                   |
| Learning hooks                      | The generated hook opens sessions and produces results without supplying a learning note.                            | Add an explicit, bounded learning handoff and session-start recall.                                   |
| Knowledge checking                  | The architecture specifies `knowledge check`; the reviewed implementation does not supply the corresponding feature. | Implement the checker and its CLI.                                                                    |
| Telemetry revisions                 | The normalizer copies the supplied revision into capture metadata but uses one mapping.                              | Dispatch through supported, pinned mapping profiles or report best-effort/unsupported input honestly. |

The implementation observations above are anchored to the pinned source files, not inferred from documentation alone. [R4], [R5], [R6], [R7], [R8], [R9], [R10]

Before integration, reconcile the target with current `main`. The reviewed `main` tip includes fixes previously taken from the context branch; avoid duplicating or dropping those fixes. Record the actual integration base in the implementation PR. [R11]

## 3. Architectural invariants

| ID     | Requirement                                                                                                                                    |
| ------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| INV-01 | Git objects and refs remain authoritative. Retrieval indexes and Memory are disposable projections.                                            |
| INV-02 | Curated Concepts remain ordinary OKF-compatible Markdown in the source tree. Signed records remain their underlying provenance where supplied. |
| INV-03 | All repository evidence for one pack resolves from one captured `view.tree`. Mutable paths are not immutable identity.                         |
| INV-04 | A Concept's path is its portable name; its blob OID identifies exact bytes. Canonical record citations use qualified record commit OIDs.       |
| INV-05 | Editorial status, `verified`, actor strings, repetition, and retrieval rank never grant instruction or repository authority.                   |
| INV-06 | Structure, provenance, repository freshness, temporal freshness, external freshness, and retention are reported separately.                    |
| INV-07 | An invalidated or redacted source cannot remain eligible merely because an old Memory note or index still contains its text.                   |
| INV-08 | Memory selection does not delete Concepts, source records, or historical evidence.                                                             |
| INV-09 | Search operates on the eligible corpus before startup-summary truncation. A rare learning omitted from Memory remains discoverable.            |
| INV-10 | Selection is not exposure. Only the actual instrumented handoff may be recorded as an exposure claim.                                          |
| INV-11 | Exposure retention uses real Git edges. OIDs in Markdown, JSON, or a digest do not establish reachability.                                     |
| INV-12 | High-volume audit records never become input to authorization, membership, mergeability, or `requireProvenance` folds.                         |
| INV-13 | Incomplete work and unavailable evidence are explicit outcomes, not successful empty results or invented defaults.                             |
| INV-14 | Equivalent pinned inputs produce equivalent domain results across supported hosts. Locale and ambient clocks must not silently change results. |

These preserve the existing protocol architecture while strengthening the unimplemented learning and recall behavior. [R1], [R2], [R3]

## 4. Domain boundaries and data flow

```text
Authoritative inputs
  selected Repository View
  signed session / decision records
  current trust and redaction state
  .gitplus/knowledge/ Concept blobs
            |
            v
  parse → check provenance → classify dependencies and freshness
            |
            v
  eligible derived entries, with diagnostics
            |
            +---- bounded startup Memory
            |
            +---- task-specific candidates and supporting evidence
                               |
                               v
                    existing Context Pack
                               |
                               v
                    actual ordered segments
                               |
                               v
                    existing ContextRender
                               |
                               v
                    signed Context Exposure
                               |
                               v
                    joined runtime Invocation
```

Domain operations MUST read through `Repository` and the existing trust/redaction services. CLI, HTTP, and browser adapters MUST NOT independently implement evidence verification by reaching through to storage. Pure parsing and selection helpers SHOULD remain synchronous where they do not perform effects.

The core accepts a captured view, an explicit evaluation time, a source/trust snapshot, and host limits. It MUST NOT read a mutable working tree during dependency checking or rendering after capture. Node-specific filesystem handling belongs at capture and harness-adapter boundaries.

Capture the relevant ref-head vector once per evaluation and resolve source, trust, and redaction reads against those pinned inputs. Do not combine an old source walk with an unrelated new trust head and describe the result as one coherent evaluation. Before automatic injection, recheck the safety-critical heads; if they moved, retry within a bound or omit the unvalidated result. The report describes the evaluated snapshot, not a guarantee of globally latest state across disconnected replicas.

No new canonical `refs/hub/knowledge/*` namespace is required. Concept edits use ordinary source commits and review. Memory continues to use `refs/notes/hub/memory`; audit continues to use `refs/hub/trace/<session>`.

## 5. Format profile and Concept handling

### 5.1 Pinned OKF profile

For this increment, pin the portable format to OKF v0.2 at upstream commit:

```text
GoogleCloudPlatform/knowledge-catalog
62432a095456147ee71e70ac6e4dc0d2dea3ac30
okf/SPEC.md
```

The inspected specification blob is `c06e3eede0c910d0ecf12524c34204156f8795ac`. Store the upstream revision and parser-profile version with conformance fixtures. Do not treat another project's validator or a moving `main` URL as the normative format. [R12]

The default bundle location is `.gitplus/knowledge/`. The CLI MAY accept an explicit repository-relative bundle location; it MUST reject escape outside the chosen repository view.

A minimal document with a valid `type` and Markdown body MUST remain readable. Unknown types and unknown portable metadata MUST NOT be rejected just because Git+ does not interpret them. `index.md` and `log.md` are navigation/history documents, not Concepts. Missing optional provenance is not an OKF structural error. [R12]

### 5.2 Parsing and preservation

Use a portable YAML parser with bounded processing. Disable unsafe constructors and automatic execution. Preserve timestamps as authored strings rather than coercing them to local dates. Reject ambiguous duplicate mapping keys as a Git+ safety diagnostic; do not silently let a later key change the meaning of signed references.

A checker MUST NOT rewrite the input file. Any future writer or distiller MUST preserve unknown metadata values, Markdown content not intentionally changed, and namespaced extensions. A no-op edit MUST preserve the original bytes; a deliberate edit may produce a new blob OID. Historical auditing always reads original blob bytes, never a reserialized parse result.

Unknown executable or attestation metadata remains inert. Validation MUST NOT run an `executor`, `attester`, shell command, script, or URL merely because a Concept contains one.

### 5.3 Timestamp semantics

The pinned upstream format uses ISO 8601 datetimes with an explicit UTC offset for timestamp-valued fields. A date-only `stale_after`, such as `2026-12-31`, MUST NOT be converted implicitly to midnight in any timezone. Report an invalid temporal field and an unknown temporal assessment. Do not mark that Concept fresh. [R12]

An absent `stale_after` means no declared temporal deadline, not an indefinite guarantee of correctness. An offset-aware deadline is stale exactly when `evaluationTime >= stale_after`. Compare instants, not lexicographically sorted strings. Calendar validation MUST reject impossible dates.

### 5.4 Git+ provenance extension

Reuse the extension already described in `docs/knowledge.md`: `gitplus.cites`, `gitplus.evidence`, `gitplus.external`, and `gitplus.verification_records`. Do not create another interchange serialization. [R3]

Illustrative Concept; the OIDs below demonstrate syntax only and do not name real objects:

```yaml
---
type: Gotcha
title: Worker authorization tests need the production fixture
description: Use the production policy fixture when changing worker authorization.
status: draft
stale_after: "2026-12-31T00:00:00Z"
generated:
  by: gitplus-distiller/1
  at: "2026-09-06T10:00:00Z"
sources:
  - id: discovery
    resource: gitplus:record:sha1:1111111111111111111111111111111111111111
gitplus:
  cites:
    - record: sha1:1111111111111111111111111111111111111111
  evidence:
    - kind: blob
      path: tests/worker/auth.test.ts
      blob: sha1:2222222222222222222222222222222222222222
---
The discovery session reported a dependency on this fixture. This document
records that observation; it does not itself prove all tests require it.
```

An agent-generated Concept SHOULD explicitly start as `draft` unless an independently configured publication policy authorizes another state. An omitted status retains the portable format's default; it must not bypass repository source-acceptance or review requirements.

## 6. Knowledge checking

### 6.1 Operation contract

Implement a domain operation conceptually shaped as:

```text
checkKnowledge(view, bundlePath, optionalConcept, evaluationTime, limits)
  → report + per-Concept results
```

A report MUST identify the actual repository view, checker/profile version, evaluation time, and the trust/redaction inputs used. It MUST say whether the requested scope was completely evaluated. These fields are derived diagnostic metadata, not a new signed record type.

The report has independent dimensions:

| Dimension               | Representative outcomes                                              |
| ----------------------- | -------------------------------------------------------------------- |
| Structure               | valid, invalid, unsupported-profile                                  |
| Citation                | accepted, invalid, unavailable, redacted, unsupported-record, absent |
| Repository dependency   | unchanged, changed, missing, unknown                                 |
| Temporal                | within-deadline, stale, no-deadline, invalid                         |
| External current state  | unchanged, changed, stale, unavailable, unknown, not-applicable      |
| Snapshot retention      | retained, missing, not-retained, unknown                             |
| Verification record     | accepted, invalid, unavailable, redacted, absent                     |
| Evaluation completeness | complete, limited, cancelled, unavailable                            |

Do not collapse these dimensions into a `trusted` boolean or confidence score. Successful provenance verification does not set semantic truth to true.

Every diagnostic MUST carry a stable code, severity, affected Concept/field when visible, and a human-readable explanation. Restricted consumers receive opaque or aggregate diagnostics instead of inaccessible paths, OIDs, source strings, or signer details.

### 6.2 Signed citations

For a supplied canonical record citation, the checker MUST validate the qualified commit OID, supported record encoding, repository binding, applicable record-container/session binding, signature, and authority under the existing trust semantics. It MUST consult current applicable revocations and counted redaction tombstones, not only the presence of payload bytes.

Reuse existing validators. A signed object that merely exists in the object database is not automatically an accepted repository record. A source-only clone without the necessary trust/session refs reports unavailable provenance; it MUST NOT fabricate acceptance or automatically fetch additional namespaces.

Ordinary source-commit verification and signed hub-record verification are distinct paths. A file version accepted through normal source/review policy does not have to masquerade as a session record. Portable `verified` metadata is only an editorial claim unless an applicable signed or reviewed record independently establishes that state.

A reference to a record is not evidence that it supports every sentence. The checker MUST NOT infer semantic support from an OID, a footnote, or a matching source ID.

### 6.3 Repository evidence

Use the existing Context Pack evidence resolver for blob and gitlink identity. [R4]

| Condition at the selected tree                                      | Result      |
| ------------------------------------------------------------------- | ----------- |
| Correct item kind and recorded object at the path                   | `unchanged` |
| Same item kind, different object                                    | `changed`   |
| Path absent or resolves to another item kind                        | `missing`   |
| Required tree/object cannot be read or format cannot be interpreted | `unknown`   |

Distinguish a conclusively absent path from a path whose containing tree is unavailable. Do not turn partial-clone failures into `missing`.

Blob ranges remain non-empty half-open byte ranges. Invalid ranges are structural/evidence errors. A changed blob remains `changed` even when a heuristic finds similar text elsewhere. Any later range-equivalence optimization MUST be a separate diagnostic, never a silent replacement of OID identity.

Symlinks are link-target bytes and MUST NOT be followed. Gitlinks prove the parent repository's pointer, not submodule content availability. Neither gitlink checking nor Concept-link traversal may silently enter another repository.

### 6.4 External sources and retained snapshots

The default check is offline. It validates declared locators, timestamps, digest syntax, and locally available snapshots. It does not request remote URLs, execute source descriptors, or assert that a live website is unchanged because an old snapshot still exists.

A retained snapshot's Git blob OID and a raw-content SHA-256 digest are different identities. Verify each with its correct hashing convention. Report whether an actual retention path exists; a `snapshot` OID in YAML alone does not retain its bytes.

Live source revalidation is deferred unless separately implemented with explicit operator authorization, network policy, private-address restrictions, redirect validation, cancellation, and size limits. Offline `unknown` external currency is not an OKF syntax error.

### 6.5 No bundle and partial scopes

A repository with no default bundle returns `bundleAbsent: true`, an empty Concept result, and no manufactured knowledge. Naming a specific nonexistent Concept is an error.

Host limits, missing object sets, and cancellation MUST be reported explicitly. A valid subset does not constitute a complete bundle check. The default CLI cannot exit successfully while an operation-critical failure prevented evaluation of its requested scope.

## 7. Eligibility, uncertainty, and corrections

Format acceptance and automatic recall eligibility are different decisions. A valid generic OKF document can lack Git+ citations, while a perfectly parseable document can contain a revoked citation or changed dependency.

The first implementation uses this conservative recall policy after the host's independent visibility and source-acceptance checks:

| Condition                                                              | Automatic startup Memory / normal knowledge recall                                                   | Explicit inspection                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Valid, source-accepted Concept; no known stale/invalid declared checks | Eligible, carrying the actual check labels                                                           | Available                                                                |
| No Git+ provenance supplied                                            | May be eligible as a source-accepted editorial Concept; MUST be labelled as lacking signed citations | Available                                                                |
| Known invalid or redacted declared provenance                          | Excluded                                                                                             | Available only within permissions and redaction policy, with diagnostics |
| A declared citation/dependency could not be evaluated                  | Excluded from automatic knowledge recall                                                             | Available as unvalidated data                                            |
| Changed/missing dependency or expired deadline                         | Excluded from automatic current-knowledge recall                                                     | Available as needing revalidation                                        |
| Draft or deprecated Concept                                            | Excluded by default                                                                                  | Available as draft/history                                               |
| Malformed Concept or unsupported required profile                      | Excluded                                                                                             | Raw permitted content and diagnostics may be inspected                   |
| No declared temporal deadline                                          | Not disqualified solely for this absence; report `no-deadline`                                       | Available                                                                |
| External current state unknown in an offline check                     | May be shown with that uncertainty; MUST NOT be described as externally revalidated                  | Available                                                                |

“No known stale checks” is not the same as “verified current.” A human-curated Concept without structured dependencies remains an editorial claim, not a mechanically verified fact. Repositories MAY use a stricter independently configured recall policy. Concept metadata itself cannot relax that policy.

For this increment, any invalid/redacted declared citation excludes the Concept from automatic recall until it is re-curated or the source state changes legitimately. Do not guess that another citation supports the now-unsupported portion of its prose.

Corrections use ordinary Concept edits and history. For a replacement at another Concept path, deprecate the old Concept and explain/link the replacement through ordinary Markdown. No new `knowledge.corrected` event or semantic merge engine is required.

The distiller MUST preserve material uncertainty in authored text. It MUST NOT silently merge contradictory claims, promote inference to fact, or choose a winner by repetition. Detection of arbitrary semantic contradictions is outside the checker guarantee. Where contradictory inputs are explicitly known, report them as competing claims and require curation rather than fabricate consensus.

Independent sessions may corroborate the same observation. Count distinct sessions as observation provenance, but never call that count independent verification or a truth score. Repeated events within one session MUST NOT inflate the session count.

## 8. Repository Memory

### 8.1 Separate collection, eligibility, selection, rendering, and persistence

Refactor the current monolithic distillation path into separable operations:

```text
collect(view, sourceSnapshot)
    → all bounded, addressable candidate entries + completeness

assess(candidates, trustSnapshot, evaluationTime)
    → eligible entries + reasons for exclusion

select(eligible, byteBudget)
    → bounded startup subset + omitted count

render(subset)
    → exact UTF-8 Memory bytes

persist(bytes, inputStamp)
    → updated note commit, no-op, or explicit failure
```

These are conceptual boundaries; avoid introducing a service per pure function.

Each derived entry MUST retain its origin type, exact source references, rendered text, and relevant check state. A Concept entry identifies its repository-relative path and blob OID. A session entry identifies exact signed record commit OIDs; a display session ID is supplementary, not a replacement.

Collection MUST make candidates outside the startup budget available to task-specific retrieval. A bounded host may stop collection, but must report partial coverage rather than claim it searched the whole corpus.

### 8.2 Inputs and text extraction

Sources are eligible Concepts and accepted session learnings. A resolved decision MAY be represented using its exact question and answer records; the extractor MUST NOT convert a context-specific answer into a universal policy. A reusable interpretation belongs in a curated Concept.

The initial deterministic Concept summary uses the authored `description` when present, otherwise a bounded extract of the authored body. It MUST identify its source version and preserve uncertainty. It must not invoke a model merely to render the same source entry again.

Session-note grouping may retain the current exact-text behavior, but it MUST aggregate exact record citations and count each session once. Semantic deduplication is not required. A Concept that cites a session learning MAY replace its duplicate display entry, while retaining the cited observation provenance.

### 8.3 Selection and byte limits

Keep the existing startup Memory ceiling of **16,384 UTF-8 bytes**. Count the complete rendered document: headings, labels, provenance, separators, newlines, and any metadata stored in the same note. [R7]

Selection MUST:

- Filter by eligibility before ranking.
- Use a stable, documented ordering with an explicit path/OID tiebreaker, not locale-dependent collation.
- Consider subsequent candidates when an entry does not fit; do not stop at the first oversized entry.
- Keep entries and citation identifiers intact; never truncate through a UTF-8 codepoint or discard the provenance while keeping the claim. An entry with more citations than the note lists names the newest ones whole and states how many more it holds; the full set stays on the derived entry.
- Bound time and allocations while measuring output; do not repeatedly rebuild an unbounded document for every candidate.

A simple initial ordering is Concept-backed summaries first, then session-derived observations, using distinct-session count where meaningful and a stable identity tiebreaker. This is a retrieval heuristic, not a credibility ranking. Avoid claiming that a UUID or author-supplied timestamp establishes global recency or causality.

Repeated synthesis of identical inputs SHOULD yield identical bytes and avoid an unnecessary note commit. Test persistence with compare-and-swap conflicts; retry against fresh inputs or report a conflict, not silent last-writer-wins success.

### 8.4 Cache provenance and invalidation

Keep the existing note address on the genesis commit. A new cache representation MAY attach a small versioned metadata header or companion derived object, but MUST retain a plain-text read path and MUST NOT become a second canonical knowledge store.

The cache input stamp MUST cover:

```text
repository identity and selected view.tree
relevant session/decision source ref heads
trust, redaction, and independent recall-policy inputs
parser, checker, and projection versions
selection parameters
```

The cache MUST also track the earliest applicable time-based recheck boundary. If a full source-head vector is too large, store a digest over a canonical, sorted enumeration; a digest does not eliminate the need to evaluate whether those inputs are still current.

The genesis anchor is shared across branches. Therefore a Memory note built from branch A MUST NOT be injected as current knowledge on branch B solely because both use the same anchor. A view mismatch requires revalidation or rebuilding.

An unsigned header, matching digest string, or fetched note is not proof that the note's prose is derived honestly. The first implementation MUST regenerate deterministic Memory or validate each entry and its rendered text against accepted source versions before automatic injection. Locally trusted memoization may avoid repeated work within a correctly keyed instance; foreign or edited cache bytes are not automatically trusted.

A read after revocation, redaction, Concept change, policy change, or deadline expiry must not serve an obsolete entry merely because a distillation job has not run. On validation failure or insufficient budget, automatic recall omits the suspect entry or returns an explicitly partial/empty result. Raw historical inspection may still show an old note under applicable policy, labelled as historical.

Legacy notes remain readable, but are not silently treated as the new validated cache format.

### 8.5 Memory is not deletion

Removing an entry from a new Memory note does not erase its bytes from older notes, Concepts, commits, exposures, or replicas. The implementation MUST distinguish logical exclusion from physical erasure.

Redaction handling MUST invalidate derived caches and respect the existing retention/garbage-collection model. It MUST NOT promise complete scrubbing when the content remains reachable through an ordinary source branch, another live exposure, or an offline replica. Shared-object retention must be reported honestly. Historical Memory is not a backup exempt from sensitive-content policy.

## 9. Task-specific recall and Context Pack assembly

### 9.1 Candidate discovery

Extend the existing replaceable selector rather than changing the evidence protocol. Knowledge discovery searches the selected view's Concept metadata first, then bounded bodies as needed. Include title, description, tags, and Concept identity in lexical matching. Do not require BM25, embeddings, graph traversal, or a new persistent store.

A parse index MAY cache facts by `(Concept blob OID, parser version)`. Eligibility additionally depends on view reachability, trust/redaction state, policy, and evaluation time. Do not cache those mutable judgments solely by blob OID.

Apply the permitted view and visibility boundary before returning ranked results or diagnostics. Reuse unchanged blob work across refs without exposing Concepts unreachable from the requested view.

Navigation indexes are optional hints. A stale or missing `index.md` MUST NOT make existing eligible Concepts permanently undiscoverable. The index is not the corpus, and the startup Memory note is not the search corpus.

Keep current code-search semantics intact. Knowledge ranking must not silently introduce fuzzy results into a successful exact grep operation. [R6]

### 9.2 Select Concepts with their supporting evidence

For an automatically recalled Concept:

1. Resolve its exact Concept blob from the captured view.
2. Evaluate its declared checks against that same view and trust snapshot.
3. Select the Concept text or an exact, disclosed byte range.
4. Include its relevant current repository dependencies when needed to interpret or validate the learning.
5. Render check labels as derived diagnostics, not as source facts.

Concepts are ordinary `kind: blob` items, usually described with a role/reason such as knowledge/memory. Supporting implementation or test files use the existing blob or gitlink item kinds. Role and reason remain descriptive.

For the initial policy, treat a Concept and the supporting items required by its declared repository dependencies as a selection group. Deduplicate identical evidence across groups. A group that cannot fit the remaining item/byte budget is omitted with a budget diagnostic rather than included as apparently fully supported prose. Explicit historical inspection may choose a different labelled presentation; normal recall must not silently weaken the group.

A reference to an old dependency is not a substitute for current source. If the selected view contains a different object, report the change. Do not replace the recorded OID inside the Concept or silently relabel the new bytes as the originally cited evidence.

### 9.3 Memory outside the source tree

`refs/notes/hub/memory` and session records are not ordinary paths in `view.tree`. The implementation MUST NOT invent a path, add a fake `kind: memory` item, or change the source view solely to make those bytes appear to be repository-file evidence.

Validated derived Memory may be included as a clearly labelled data segment in ContextRender. Where it summarizes source Concepts, include those exact Concept blobs and required supporting evidence in the pack. Where it summarizes session-only learnings, retain qualified record citations in the rendered material and identify it as derived session data, not verified source-tree blob evidence.

The render commitment proves the exact derived bytes supplied; Concept/evidence item checks prove source-tree identity; record-citation checks prove accepted provenance. These remain separate claims. A summary's presence does not establish that all bytes of every cited source were exposed.

### 9.4 Visibility and full-view retention

Omission diagnostics MUST obey the invocation's access boundary. An aggregate omission is not enough if the exposure still retains an unauthorized full view.

Because the exposure's real `context/view` edge retains the entire captured tree, authorized retention MUST be checked for that tree, not only the selected snippets. A restricted invocation needs an explicitly permitted view constructed under independent host policy. The capture must not sweep unrelated untracked files or files outside the allowed root into retained context.

Dirty views need particular care: an unselected secret in a newly captured blob can still be retained through the view edge. Apply the repository's content/secret-retention rules before durable exposure. `--retain-render=false` does not remove bytes held by `context/view` and MUST NOT be presented as a complete privacy control.

Do not retain `view.base` ancestry merely because its OID is present in the manifest. If a product promises historical ancestry retention, it needs its own actual reachability path and authorization review. [R1]

### 9.5 Selection output is not exposure

`context for` may construct, display, or return a pack without a provider call. Such a result is selection/preparation, not proof of an invocation handoff.

Any existing CLI or library path that records an exposure merely on preview MUST be adjusted or clearly separated: durable exposure is recorded by the harness adapter when it takes responsibility for handing the exact rendered segments to the invocation subsystem. Preserve the pack/why/audit product vocabulary; do not conflate these lifecycle steps to avoid adding a flag.

## 10. Harness capture, publication, and recall

### 10.1 Integration boundary

Implement one adapter contract over existing domain services. A real adapter must document the harness callbacks it uses and what each callback can actually observe. Do not assume every `Stop` callback ends a logical session or that a hook can observe provider bytes it never receives.

```text
session start
  discover repository and accepted source view
  open or resume the correct signed session
  validate/build startup Memory as data

before each instrumented logical invocation
  capture the permitted effective view
  discover relevant Concepts and current source evidence
  assemble exact ordered semantic segments
  commit ContextRender and append Context Exposure
  hand those same segments to the invocation subsystem

after the invocation, when observed
  normalize runtime facts with an identified mapping policy
  append runtime record referencing the exposure's qualified commit OID

after substantial work / at actual session end
  review a small number of durable learnings
  record bounded session notes
  optionally propose or update Concepts through ordinary source editing/review
  invalidate or rebuild derived Memory
```

Startup Memory and standing instructions are separate inputs. The harness MUST frame Memory and Concepts as cited data unless independent policy grants a particular source instruction authority.

### 10.2 Learning handoff

The agent or harness component that can evaluate the work supplies the learning. A stop hook cannot infer a useful discovery from a branch name alone.

Persist only reusable conventions, gotchas, decisions, or friction. No learning is a valid result. Do not store private chain-of-thought, raw transcripts, bulk tool output, speculative chatter, or credentials as reusable memory.

Prefer updating an existing Concept when the same subject and lifecycle are involved, but preserve new session observations as provenance where useful. Publication MUST be explicit: editing a file, creating a commit, recording a session note, updating Memory, and successfully pushing are different outcomes.

The initial workflow uses existing `session produce --note`. Add an optional `--note-file <path|->` input to avoid putting sensitive or multiline learning text in process arguments. It MUST be mutually exclusive with `--note`, use the same bounded validation and secret checks, and preserve the existing signed record schema. Treat file paths as local adapter inputs, not paths supplied by a remote Concept.

Automatic Concept publication MUST NOT silently commit or push unrelated changes. Agent-generated drafts use normal review/policy controls; no new capability is granted by calling the distiller.

### 10.3 Hook state and persistence failures

Local adapter state belongs in an explicitly ignored, private harness-state location, not the versioned knowledge bundle. Key it by repository identity and actual harness session identity; one shared `session.id` must not mix concurrent sessions.

On failed persistence, report the failure and keep only an explicitly permitted, bounded retry payload. Do not attach the next unrelated session's output to an old session. Repeated delivery of the same learning must not double-count observations.

Where append retries are supported, preserve a stable event ID/payload across an uncertain retry and reconcile whether that exact event was accepted. An existing event ID with different bytes is a conflict, not an idempotent success. Do not blindly rewrite a new record after an ambiguous timeout and claim exactly-once persistence.

State cleanup, retry retention, and permissions MUST be tested. A local outbox is transport assistance, not a second authoritative memory store; it cannot retain raw transcripts by default.

### 10.4 Exposure and invocation ordering

No transaction can atomically guarantee both a Git append and a remote provider call. The adapter MUST expose that limitation rather than imply provider receipt.

The durable pre-call record states the harness's boundary claim. The adapter appends it when it has finalized the segments and accepted the handoff; a crash before the network request can leave a context-only row. That row proves neither provider receipt nor a completed invocation. Runtime absence remains unknown, not success or zero usage.

After commitment, an adapter MUST NOT reorder, truncate, summarize, inject repository content, or change semantic placement without constructing a new render and exposure for the transformed segments. Provider wire serialization may differ, but the committed semantic segments must be the actual input to that mapping.

Expose audit mode as independent harness configuration:

| Mode                       | Append failure before dispatch                                                                           |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| Best-effort audit, default | Report missing audit coverage; the host may proceed, but cannot report a durable exposure for that call. |
| Required audit             | Refuse dispatch until the required pre-call record is durable.                                           |

This mode governs harness dispatch, not Git authorization. It does not allow trace records to enter protected-branch policy folds. The end-to-end conformance test uses required-audit mode so the boundary is unambiguous.

## 11. Telemetry semantic-convention hardening

### 11.1 Supported mapping profiles

The current normalizer records a supplied convention revision without using it to choose the mapping. Replace that behavior with an explicit profile registry. [R8]

Each supported profile MUST identify an immutable upstream revision, the Git+ mapping version, supported operation classes and fields, and fixtures proving their interpretation. The initial implementation only needs one supported profile. It MUST pin a real upstream revision during the first delivery slice; no placeholder revision may be advertised as supported.

Do not duplicate the upstream specification in this document. The registry and fixtures are the executable compatibility boundary. Support for a tag or compatible range requires an explicit mapping to tested immutable revisions, not a string-prefix guess.

| Input                                         | Required behavior                                                                                              |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Supported declared revision                   | Select its mapping, validate the relevant signal shape, and preserve the profile/revision in capture metadata. |
| No declared revision                          | A documented best-effort mapping may be used; do not claim strict revision conformance.                        |
| Unsupported declared revision                 | Return a typed unsupported-profile outcome; do not stamp it onto a record produced by an unrelated mapping.    |
| Recognized revision but unsupported operation | Return an explicit unsupported-operation outcome or allowed diagnostics, never an invented inference.          |

The ingest adapter MUST account for unsupported/dropped signals in capture-health reporting where that reporting is available. If the health append also fails, report that failure outside Git and do not claim complete coverage.

An operation's classification must follow the selected profile. Do not equate every agent-operation span with one model inference or double-count parent agent spans and child inference spans merely because both carry GenAI attributes.

### 11.2 Semantic preservation

Regression tests MUST preserve the distinctions already emphasized in the branch's telemetry spec:

- Requested and response model remain separate.
- Absent usage is not zero; reported and estimated usage is not observed usage.
- Finish reason, runtime status, and error class remain separate.
- Attempts come only from explicit instrumentation; retries are not invented from duration or timestamp gaps.
- Retrieval diagnostics are not Context Exposure.
- OTel IDs are correlation; qualified Git record OIDs are canonical audit identity.
- Causal parentage is not reconstructed from timestamp proximity.

Existing records remain readable. A historical record carrying an unrecognized profile claim may be reported as having unverified mapping provenance, without rewriting its signed payload or invalidating independently valid pack/render evidence.

The first increment does not require an OTLP receiver. The flat-span adapter and in-process writer can satisfy it when connected to a tested harness/exporter boundary. Raw OTLP envelopes must not be accepted accidentally as flat spans. [R2], [R8]

## 12. CLI and machine-readable contracts

### 12.1 Knowledge checker

Proposed command surface:

```sh
# Current checkout and default bundle.
git+ knowledge check

# A Concept ID or repository-relative Concept path.
git+ knowledge check gotchas/worker-auth

git+ knowledge check .gitplus/knowledge/gotchas/worker-auth.md

# Committed snapshot rather than mutable working-tree state.
git+ knowledge check --ref HEAD --json

# Tighten selected validation findings into a gate.
git+ knowledge check --strict --json

# Explicit bare/server repository selection remains available.
git+ knowledge check --root ./repos --repo project --ref main
```

The implementation MUST follow the branch's existing repository-discovery helpers. A bare repository cannot invent a working tree; it checks an explicitly resolved committed view. In a checkout, the default capture uses the permitted effective working view, including intended dirty knowledge changes. An explicit `--ref` checks that committed revision and excludes later working-tree edits.

Domain checking is read-only. Capturing a dirty view may materialize Git objects, but MUST NOT stage files, change source refs, write Concepts, persist Memory, or append an exposure. State this distinction in help rather than promising zero disk writes.

Accept an optional `--bundle <repository-relative-path>`. The resulting Concept ID is relative to that root. Resolve ordinary unambiguous revisions at the CLI boundary and serialize qualified OIDs in reports.

### 12.2 JSON report envelope

The versioned JSON projection SHOULD expose the following shape. This is a derived CLI/API response, not a canonical signed schema:

```typescript
interface KnowledgeCheckReport {
  version: 1;
  bundle: string;
  bundleAbsent: boolean;
  view: { base: string; tree: string };
  evaluatedAt: string;
  profile: { okfRevision: string; checkerVersion: string };
  inputStamp: string;
  complete: boolean;
  concepts: ReadonlyArray<{
    id: string;
    path: string;
    blob: string;
    structure: string;
    lifecycle: string;
    citations: ReadonlyArray<unknown>;
    repositoryEvidence: ReadonlyArray<unknown>;
    temporal: { state: string; deadline?: string };
    external: ReadonlyArray<unknown>;
    verificationRecords: ReadonlyArray<unknown>;
    eligibility: { startup: boolean; recall: boolean; reasons: ReadonlyArray<string> };
    diagnostics: ReadonlyArray<Diagnostic>;
  }>;
  diagnostics: ReadonlyArray<Diagnostic>;
}

interface Diagnostic {
  code: string;
  severity: "info" | "warning" | "error";
  message: string;
  path?: string;
  field?: string;
}
```

The actual implementation MUST use closed tagged result types for the dimension outcomes in §6, not leave them as unvalidated strings or `unknown`. The abbreviated interface above specifies response organization, not the final TypeScript implementation.

Do not include raw prompts, keys, full external snapshots, or unrelated tool bodies in this response. Avoid enumerating inaccessible sources in errors.

### 12.3 Exit behavior

Use the existing CLI error conventions:

- Exit **0** when the requested scope was evaluated, structural/provenance checks required for that scope have no errors, and the selected strictness gate passed. A missing default bundle is a successful no-op, not a claim that knowledge exists.
- Exit **1** for malformed input, invalid declared provenance, unsupported required profiles, operation-critical unavailable inputs, incomplete evaluation, or a failed selected gate.
- Preserve the CLI's existing interrupt handling for cancellation.

Warnings alone do not fail the default gate. `--strict` additionally fails on changed/missing declared repository dependencies and expired or invalid declared freshness deadlines. Draft/deprecated lifecycle states, absent optional provenance, no declared deadline, and offline-unknown external currency are reported without becoming universal syntax errors. A host may configure a stricter publication gate independently.

`--json` prints one parseable report on stdout for evaluated checks; incidental messages go to stderr. Fatal failures before a report can be assembled use the repository's existing structured error behavior and MUST NOT print a success envelope.

### 12.4 Existing commands to extend, not replace

Keep `session memory`, `session produce`, `context for`, `context why`, `context audit`, `trace record`, and `session show --audit` as the product vocabulary. Add flags only where needed for the behavior specified here; preserve existing explicit-repository call forms.

`session memory --distill` rebuilds and persists a validated projection. A plain human read may display a legacy/historical note with its status; automatic harness injection uses the validated domain operation from §8, not raw note text.

A separate `memory search` command is not required. Task-specific recall belongs in `context for`; Knowledge files remain directly inspectable with ordinary Git/file tools. New CLI help must be generated from the command tree using the branch's documentation tooling rather than hand-maintained twice.

## 13. Failure and security requirements

The implementation MUST make failure dimensions visible without allowing untrusted content to become control input.

| Failure                               | Required handling                                                                              |
| ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Concept parsing fails                 | Report a structural diagnostic; omit from automatic recall; preserve original file.            |
| Cited record is unavailable           | Report unavailable provenance, not invalid signature or accepted provenance.                   |
| Cited record is revoked/redacted      | Exclude affected automatic recall and invalidate cached summaries.                             |
| Repository dependency changed         | Require revalidation; do not label the Concept semantically false.                             |
| Time deadline passed                  | Exclude automatic current-knowledge recall and expose the deadline finding.                    |
| Index is missing/corrupt              | Rebuild or fall back to bounded scanning; do not silently return a confident empty corpus.     |
| Memory validation is incomplete       | Omit unverifiable entries and mark the result partial; required policies may refuse injection. |
| Selection exceeds budget              | Return explicit, visibility-safe omissions.                                                    |
| Render differs after commitment       | Build a new render/exposure; never reuse the earlier digest.                                   |
| Exposure append fails                 | Follow configured audit mode and report missing coverage.                                      |
| Runtime telemetry is absent           | Keep a context-only row; do not invent runtime completion or usage.                            |
| Learning/Concept persistence fails    | Report which stage failed; do not claim publication, replication, or Memory update succeeded.  |
| Redaction cannot erase shared objects | Report logical redaction and remaining retention; do not promise complete deletion.            |

Never interpolate Concept contents, source URLs, descriptions, or cached Memory into a shell command. A selected document may contain instructions to bypass checks; it remains data. Instruction provenance may be reported only through the existing independent authority mechanism.

Secret scanning is heuristic and must be described as such. Bound and inspect newly retained data at the appropriate boundary. Do not print secret-bearing rejected text in diagnostics or copy it into capture-health records.

A replicated Memory note must not serialize host-private or narrower-visibility content into a broadly readable ref. Such a result is either excluded or kept as a non-replicated, correctly scoped local projection. The shared note's audience cannot be widened accidentally by its cache role.

## 14. Resource limits and determinism

The first implementation MUST have bounded parsing, object reads, candidate enumeration, reference checks, traversal, and rendering. Limits are host safeguards, not proof that the whole corpus was inspected.

| Limit                               | Initial policy                                                                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Rendered startup Memory             | Hard cap of 16,384 UTF-8 bytes, including all framing text.                                                                          |
| Individual Concept read             | Default maximum 1 MiB; oversized content is reported and excluded from automatic processing.                                         |
| YAML parsing                        | Bounded depth and scalar length; disable or tightly bound alias expansion; no executable tags.                                       |
| Concepts per check / candidate walk | Explicit host limit and cancellation; reaching it returns incomplete coverage.                                                       |
| Citation/evidence traversal         | Explicit item and object-read limits; reuse existing DAG ceilings where applicable.                                                  |
| Context selection                   | Reuse selector total item/byte limits and Render's writer/parser ceilings; do not create independent budgets whose sum exceeds them. |
| Memory/index caches                 | Bounded per repository instance, discardable, with versioned keys.                                                                   |
| Automatic relation traversal        | No unbounded recursive Concept graph expansion.                                                                                      |

The 1 MiB Concept default is a proposed initial bound, not a measured capacity claim. Tune limits with benchmarks while preserving explicit incomplete outcomes.

Pass the evaluation clock explicitly through the checker. Test non-UTC offsets and deterministic boundary instants. Stable result ordering must not depend on locale, filesystem enumeration order, random map order, or network response order.

Object parsing may be cached by immutable OID, but source reachability, access, trust, and temporal validity must be evaluated against the current input snapshot. A correct cache may reduce work; it cannot change an answer silently.

## 15. Suggested module changes

Exact file names may be adjusted to avoid unnecessary abstraction. Keep the responsibilities separate:

| Module / area                                       | Responsibility                                                                                            |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `src/knowledge/Concept.ts` — new                    | Pure bounded parsing, portable metadata, preservation, Concept identity and profile fixtures.             |
| `src/knowledge/Check.ts` — new                      | Repository-backed citation/dependency validation and independent report dimensions.                       |
| `src/knowledge/Recall.ts` — new if useful           | Candidate extraction, eligibility, and Concept/support grouping; no new authoritative store.              |
| `src/hub/Memory.ts` — extend                        | Separate collection/selection, exact source citations, byte cap, invalidation, and safe note persistence. |
| `src/context/Select.ts` — extend                    | Merge eligible knowledge candidates with existing source selection under one budget.                      |
| `src/context/Pack.ts` / `Render.ts` / `Exposure.ts` | Reuse verification and identity; amend only for demonstrated gaps, not a new evidence format.             |
| `src/telemetry/Semconv.ts` — extend                 | Supported-profile dispatch, explicit best-effort behavior, and mapping conformance results.               |
| `src/cli/knowledge.ts` — new                        | Thin discovery/input/output layer for `knowledge check`.                                                  |
| `src/cli/session.ts` — extend                       | Safe learning-file input, validated Memory access, and harness installation changes.                      |
| Harness adapter code                                | Boundary capture, local session state, persistence reconciliation, and exact segment handoff.             |
| Colocated tests / shared fixtures                   | Format, trust, redaction, budget, portability, CLI, and harness conformance.                              |

Use existing Effect services and tagged errors. Do not add a platform-specific dependency to the shared knowledge/context domain. A YAML dependency, if needed, must be pinned, portable, and exercised in supported runtime builds.

## 16. Acceptance test matrix

Each row is a release requirement, not a report that the test already exists. Tests MUST assert observable results rather than relying solely on comments or self-round-trips.

| Test ID | Scenario                                                       | Required result                                                                         |
| ------- | -------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| K-01    | Minimal Concept; unknown type and nested extension metadata    | Readable without rejection; extensions survive a supported edit; no-op bytes unchanged. |
| K-02    | `index.md` / `log.md` at multiple levels                       | Not misclassified as Concepts.                                                          |
| K-03    | Invalid YAML, duplicate keys, alias/depth abuse                | Bounded failure with diagnostics; no mutation or execution.                             |
| K-04    | Offset-aware timestamps, impossible dates, date-only deadline  | Correct instant comparison; invalid dates do not become fresh.                          |
| K-05    | Valid signature but wrong repository/session/container         | Citation is not accepted.                                                               |
| K-06    | Revoked key or counted source tombstone                        | Affected automatic recall is excluded despite readable payload/cache bytes.             |
| K-07    | Partial clone missing a cited record or tree                   | `unavailable`/`unknown`, not a fabricated invalid signature or absent path.             |
| K-08    | Blob unchanged, changed, missing, or replaced by gitlink       | Correct independent dependency classification.                                          |
| K-09    | Symlink and gitlink evidence                                   | No following links or invented submodule file exposure.                                 |
| K-10    | Invalid, empty, or out-of-bounds byte range                    | Explicit evidence error; valid UTF-8 rendering never splits codepoints.                 |
| K-11    | Portable `verified: human:root` on an unaccepted source        | No new authority or policy eligibility.                                                 |
| K-12    | External snapshot digest and OID with no retention path        | Distinguish matching bytes from durable retention and live freshness.                   |
| K-13    | Offline check containing network URLs and executable metadata  | No network fetch and no code execution.                                                 |
| K-14    | Host limit reached midway through bundle                       | Partial report and failing incomplete-scope gate; no clean empty answer.                |
| M-01    | Multibyte text and citations near Memory ceiling               | Entire output is at most 16,384 bytes and valid UTF-8.                                  |
| M-02    | Oversized first entry followed by useful small entries         | Later fitting entries remain eligible for selection.                                    |
| M-03    | Repeated note in one session and matching note in another      | One observation per session, with exact source record citations.                        |
| M-04    | Branch A Memory read on branch B                               | Revalidate/rebuild against B; no unqualified cross-branch injection.                    |
| M-05    | Same source blobs, changed trust or redaction state            | Cache invalidates or entries are revalidated before injection.                          |
| M-06    | Same refs, clock passes a deadline                             | Stale entry is not automatically injected.                                              |
| M-07    | Foreign/edited Memory note with a forged input stamp           | Re-derived text wins; stamp alone cannot authorize injection.                           |
| M-08    | Memory eviction or cache deletion                              | Concepts and source records remain available; recall can rebuild.                       |
| M-09    | CAS conflict / ambiguous append retry                          | No silent overwrite or false persistence claim; no observation inflation.               |
| R-01    | Rare relevant Concept absent from startup Memory               | Task-specific retrieval can find it from the corpus.                                    |
| R-02    | Selected Concept with current dependencies                     | Pack contains exact Concept/support items from the one view.                            |
| R-03    | Support group does not fit                                     | Explicit budget omission, not silently unsupported prose.                               |
| R-04    | Concept evidence changed since publication                     | Needs-revalidation finding; no rewritten citation or invented semantic equivalence.     |
| R-05    | Memory note is not in the source tree                          | No fake pack path/item kind; derived segment and provenance are labelled separately.    |
| R-06    | Restricted path and unselected sensitive dirty file            | Neither diagnostics nor retained view bypass visibility/retention policy.               |
| R-07    | Corrupt/missing retrieval index                                | Safe fallback or explicit partial outcome; historical pack audit still works.           |
| H-01    | Two concurrent harness sessions                                | No shared-state cross-talk or reuse of another session's ID.                            |
| H-02    | Session end with no durable learning                           | No artificial note/Concept is created.                                                  |
| H-03    | Learning recorded, optional Concept commit/push fails          | Report each completed stage precisely; do not claim full publication.                   |
| H-04    | Preview a pack without an invocation                           | No assertion that the preview crossed a model invocation boundary.                      |
| H-05    | Capture render, then adapter changes placement/order/body      | New commitment/exposure required; prior digest cannot be reused.                        |
| H-06    | Crash after exposure but before runtime completion             | Context-only audit row; no fabricated provider receipt or usage.                        |
| H-07    | Required-audit pre-call append fails                           | No dispatch; failure is visible.                                                        |
| T-01    | Supported, absent, and unsupported semconv revisions           | Correct mapping selection, honest best-effort, or typed refusal.                        |
| T-02    | Parent agent span and child inference span                     | No invented or double-counted logical inference due to naive classification.            |
| T-03    | Missing/zero usage, length finish, retries                     | Preserve absent versus zero, finish versus error, explicit attempts only.               |
| T-04    | Concurrent causal lanes and missing/redacted records           | No timestamp-only joins or misattributed workspace transitions.                         |
| A-01    | Dirty overlay with stock `git fsck --strict` and collection    | Retained allowed view remains reachable through real Git edges.                         |
| A-02    | Render framing golden vectors                                  | Exact cross-implementation bytes/digest, including placement and order changes.         |
| A-03    | Large trace beside protected-branch evaluation                 | Trace data never enters policy-critical folds.                                          |
| A-04    | Shared retained blob after redaction                           | Logical removal and withheld physical deletion are reported distinctly.                 |
| P-01    | Same fixture on memory, Node, OPFS, and Cloudflare test layers | Equivalent domain reports, selection, and render bytes for equivalent inputs.           |
| P-02    | Valid CLI JSON, strict gate, missing bundle, interrupt         | Defined output/exit behavior; generated help matches command tree.                      |

### 16.1 End-to-end release fixture

Build a deterministic two-session fixture using a fake invocation sink; no paid model call is required.

Session A records a reusable discovery with an exact record citation, writes a draft Concept, and passes it through the repository's ordinary acceptance path. Its relevant dependency is committed. Rebuild startup Memory, but deliberately use a budget/ranking fixture that omits this particular Concept.

Session B asks a task for which the Concept is relevant. It MUST retrieve the Concept despite its absence from startup Memory, validate the evidence, construct a pack, and hand exact segments to the fake sink. Independently recompute the sink's render digest and compare it with the exposure. Append a runtime record and inspect the joined Invocation.

Then modify the declared dependency and repeat. Normal automatic recall MUST not silently present the old prose as current. Finally, redact or invalidate the discovery source and confirm that a previously written Memory note cannot resurrect it as eligible knowledge.

### 16.2 Retrieval evaluation

Compare the existing selector with Concept-aware recall over a fixed repository fixture set at the same total byte/item budget. Include irrelevant high-frequency words, rare gotchas, changed dependencies, missing citations, non-ASCII content, and knowledge omitted from startup Memory.

Measure eligible relevant evidence recall, invalid/stale inclusion, supporting-evidence completeness, cold/warm latency, object reads, peak memory, and rendered bytes. Correctness gates require no knowingly ineligible automatic inclusions in the fixtures. Do not promise a universal token saving or speedup; publish measured results and environment details.

## 17. Delivery sequence and merge gates

| Slice                               | Deliverable                                                                                                                            | Merge gate                                                                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| S0 — baseline and profile hardening | Reconcile `main`; pin OKF fixtures and an actual supported telemetry profile; implement supported/absent/unsupported mapping behavior. | Existing context/audit suites remain green; no arbitrary revision is advertised as supported. |
| S1 — knowledge checker              | Portable parser, independent check dimensions, source/ref discovery, `knowledge check`, safe JSON and CLI gate.                        | K-series tests and relevant portability/CLI tests pass.                                       |
| S2 — validated Memory               | Concept/session collection, exact citations, eligibility, UTF-8 budget, safe cache stamps/revalidation, legacy read compatibility.     | M-series tests pass, including branch/trust/time invalidation.                                |
| S3 — task-specific recall           | Concept-aware candidates, supporting-evidence groups, one-view pack assembly, visibility-safe omissions.                               | R-series tests pass and matched-budget retrieval results are recorded.                        |
| S4 — harness learning loop          | Safe learning handoff, concurrent session state, actual invocation-boundary exposure, runtime joins, failure recovery.                 | H-series tests and the two-session end-to-end fixture pass.                                   |

Each slice SHOULD be independently reviewable. Domain correctness MUST NOT wait for a browser screen or external exporter deployment. Portable libraries plus the CLI and one documented, tested harness adapter are the required first product surface.

Run the branch's read-only checks and applicable unit, integration, and Git interoperability suites for each slice. Regenerate command documentation from the command tree after CLI changes. Record the actual commands, environment, and results; a proposed test or inherited green CI badge is not proof that a new slice passed.

Do not merge `experimental/search-persistence` as a prerequisite. Evaluate it after recall behavior is correct and its performance benefit is measured. Keep `experimental/wal` and bundle/maintenance work outside this feature series.

## 18. Compatibility and migration

Existing Context Pack version 1, `git+context-render/v1`, signed session records, trace records, and qualified OID identities MUST remain auditable. No history rewriting or automatic conversion of signed payloads is permitted.

Existing plain Memory notes remain human-readable. New automatic consumers treat them as unvalidated caches until re-derived. A cache format change is a rebuild, not a migration of authoritative knowledge.

Repositories without `.gitplus/knowledge/` continue to work. Session-derived Memory remains available when its records can be validated. Repositories without hub identity can check portable source Concepts, but cannot fabricate signed session provenance or append hub audit records.

The default knowledge bundle is not created merely by installing or running a read command. Imported generic OKF documents remain readable even without Git+ extensions. Stricter recall eligibility must be reported separately from portable conformance.

New learned Concepts use normal source edits/review and may be copied as an ordinary OKF bundle. No export format or proprietary conversion step is required.

If a wire-schema addition is genuinely necessary, document it in the owning protocol spec, add explicit version/unknown-field behavior, and test old record reads. Do not smuggle selection scores, cache ordinals, or local harness state into canonical record identity.

## 19. Definition of done

The increment is ready when all required acceptance tests pass and a reviewer can reproduce the two-session learning/recall fixture from a clean checkout.

The resulting implementation must be able to answer, separately and without overclaiming:

```text
What was learned, and where is it durably recorded?
Which exact Concept version is being recalled?
Which provenance is accepted, and against which trust state?
Which declared dependencies changed or became unavailable?
Why was an entry omitted from startup Memory or task context?
Which exact bytes crossed this instrumented invocation boundary?
Which audit facts are missing, reported, derived, or independently verified?
```

No unsupported format/mapping placeholders may remain in a claimed compatibility profile. No Memory or retrieval optimization may be necessary to verify a historical exposure. No corpus or editorial label may create authority.

## 20. Pinned references and provenance

The repository links below point at the implementation baseline unless noted otherwise. They make the distinction between reviewed source behavior and the new requirements in this document inspectable. This specification does not claim a fresh local test run or modify the remote repository.

[R1]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/docs/context-pack.md
[R2]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/docs/telemetry.md
[R3]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/docs/knowledge.md
[R4]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/context/Pack.ts
[R5]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/context/Render.ts
[R6]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/context/Select.ts
[R7]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/hub/Memory.ts
[R8]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/telemetry/Semconv.ts
[R9]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/cli/session.ts
[R10]: https://github.com/chr33s/git/blob/54969361929568926e555807e6c6bd4942561360/src/context/Exposure.interop.test.ts
[R11]: https://github.com/chr33s/git/commit/759308886f32a98c66526478915074c86e94cd87
[R12]: https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/62432a095456147ee71e70ac6e4dc0d2dea3ac30/okf/SPEC.md

| Reference | Purpose                                                                                    |
| --------- | ------------------------------------------------------------------------------------------ |
| [R1]      | Context Pack draft-9: view/evidence identity, framing, exposure, retention, and security.  |
| [R2]      | Telemetry draft-7: runtime semantics, mapping contract, causal joins, and audit isolation. |
| [R3]      | Knowledge draft-5: native OKF corpus, Git+ provenance, freshness, and bounded Memory.      |
| [R4]      | Existing view and evidence schema/resolution implementation.                               |
| [R5]      | Existing exact render framing and bounds.                                                  |
| [R6]      | Existing replaceable lexical selection and budget behavior.                                |
| [R7]      | Existing session-only Memory projection and current budget implementation.                 |
| [R8]      | Existing flat-span normalization and revision-metadata handling.                           |
| [R9]      | Existing session commands and generated learning-hook behavior.                            |
| [R10]     | Existing stock-Git exposure reachability and collection tests.                             |
| [R11]     | Reviewed mainline tip containing previously extracted context-branch fixes.                |
| [R12]     | Immutable upstream OKF v0.2 format profile, including offset-aware timestamps.             |
