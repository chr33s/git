# Git-Native Context Packs

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Spec revision:** draft-8

## 1. Summary

This specification defines a small Git-native audit primitive for repository context used by coding agents.

The problem is not to standardize how an agent retrieves or ranks code. The problem is to make it possible to answer, after an agent operation:

> **What exact repository state and repository evidence were exposed to the model-invocation boundary?**

V1 defines four concepts:

1. **Repository View** — the exact Git tree from which repository evidence is resolved.
2. **Context Pack** — an immutable manifest of Git-grounded evidence selected for one auditable exposure.
3. **ContextRender** — the exact ordered, semantically framed repository-derived segments handed from the context subsystem to the invocation subsystem.
4. **Context Exposure record** — a signed Git+ audit record binding the pack, render commitment, retained repository view, and optional runtime correlation to one invocation boundary.

The model is:

```text
Repository View
      ↓
any retrieval implementation
      ↓
 Context Pack
      ↓
   renderer
      ↓
 ContextRender
      ↓
Context Exposure
      ↓
logical invocation
      ↓
agent operation
```

The protocol boundary is deliberately narrow:

```text
Retrieval quality
  "Did we choose the right context?"

        is separate from

Context provenance
  "Can we audit what repository context was exposed?"
```

V1 standardizes **context provenance**, not retrieval quality.

Runtime conditions surrounding the invocation—OpenTelemetry correlation, model/provider identity, usage, retries, context compaction or truncation, tool diagnostics, and workspace transitions—are specified in [telemetry.md](telemetry.md). Telemetry MUST NOT change Context Pack identity or Git-evidence verification.

A Context Pack does not prove that a model read, understood, remembered, or used any item. A Context Exposure record does not prove causation. It records a signed harness claim about the repository-derived context that crossed a defined invocation boundary.

---

## 2. Goals and non-goals

### 2.1 Goals

V1 MUST make it possible to:

1. identify the exact repository snapshot used for context;
2. resolve every repository evidence item against that snapshot;
3. distinguish blob evidence from submodule gitlinks;
4. bind exact rendered bytes, ordering, and logical semantic placement;
5. preserve the captured repository tree through real Git reachability;
6. bind a Context Exposure to a later logical invocation without relying on ambiguous event IDs;
7. audit stale, missing, contradictory, or improperly rendered repository context;
8. validate provenance without reproducing the selector that created the pack;
9. record lightweight selector and omission diagnostics without turning them into consensus inputs;
10. preserve verifiable repository provenance for items claimed to have instruction authority.

### 2.2 Non-goals

V1 does **not** standardize:

- a deterministic selector;
- ranking weights or scoring arithmetic;
- embeddings or rerankers;
- Tree-sitter, CodeGraph, LSP, SCIP, or compiler behavior;
- token estimators;
- selector configuration hashes;
- exhaustive candidate logs;
- byte-identical manifests generated independently by different implementations;
- provider request envelopes;
- model cognition, attention, memory, or causation.

Implementations MAY use any retrieval architecture internally.

> **Retrieval may be probabilistic. Evidence identity must not be.**

---

## 3. Audit-trace placement

Context Exposure is detailed audit provenance and belongs in the policy-invisible trace namespace:

```text
refs/hub/trace/<session-id>
```

It MUST NOT be placed in the high-value policy fold merely because it is useful for audit.

The separation is:

```text
refs/hub/session/<session-id>
  distilled lifecycle / decisions / produced result
  may be consulted by protected-branch policy

refs/hub/trace/<session-id>
  Context Exposure / invocation / tools / lifecycle diagnostics
  not consulted for authorization or merge policy
```

A Context Exposure record is signed and bound to the same repository/session identity as its trace, but losing optional trace detail MUST NOT retroactively alter whether a push or merge was authorized.

### 3.1 Canonical record identity

The canonical identity of a persisted Context Exposure is its signed Git record commit OID:

```text
sha1:<hex>
sha256:<hex>
```

Human-facing UUIDs MAY exist for display/search, but later canonical records MUST use the qualified Git commit OID when they need an immutable cross-record reference.

A logical invocation SHOULD reference its prior Context Exposure by record commit OID.

OTel `TraceId`, `SpanId`, provider request IDs, or harness event IDs are correlation identifiers, not replacements for Git record identity.

---

## 4. Repository View

A Repository View names the exact source snapshot from which repository evidence is resolved.

```json
{
  "base": "sha256:abc123...",
  "tree": "sha256:def456..."
}
```

### 4.1 `base`

`base` is the commit OID anchoring committed history for the operation.

It is useful for ancestry and human inspection, but `view.base` alone is not the effective context snapshot.

### 4.2 `tree`

`tree` is the root tree OID of the **effective repository snapshot** visible to retrieval.

For a clean worktree:

```text
view.tree == root tree of view.base
```

For a dirty worktree, the producer MUST construct an overlay tree containing the exact repository bytes retrieval is allowed to inspect.

Retrieval MUST resolve repository evidence from `view.tree`, not from mutable filesystem state after the view has been captured.

### 4.3 Dirty worktrees

A producer MAY construct a dirty view by:

1. hashing modified or newly included files as Git blobs;
2. applying additions, modifications, and deletions over the base tree;
3. writing an overlay tree;
4. recording the resulting root as `view.tree`.

The mechanism is implementation-defined. The result is not.

### 4.4 Object identifiers

Git OIDs use:

```text
<algorithm>:<lowercase-hex>
```

where `algorithm` matches the repository object format, currently `sha1` or `sha256`.

Consumers MUST treat OIDs as opaque strings and MUST NOT assume SHA-1 length.

---

## 5. Context Pack

A Context Pack is ordinary UTF-8 JSON describing repository evidence associated with one auditable exposure.

A minimal mixed-evidence example is:

```json
{
  "version": 1,
  "view": {
    "base": "sha256:abc123...",
    "tree": "sha256:def456..."
  },
  "items": [
    {
      "kind": "blob",
      "path": "src/auth.ts",
      "blob": "sha256:111aaa...",
      "range": [1200, 1840],
      "role": "implementation",
      "reason": "reference"
    },
    {
      "kind": "gitlink",
      "path": "vendor/policy-engine",
      "commit": "sha1:222bbb...",
      "role": "dependency",
      "reason": "import"
    }
  ]
}
```

Protocol-critical fields depend on `kind`.

### 5.1 Blob item

A blob evidence item is:

```json
{
  "kind": "blob",
  "path": "src/auth.ts",
  "blob": "sha256:...",
  "range": [1200, 1840]
}
```

A conforming verifier MUST establish:

```text
view.tree + path → blob
blob + optional range → exact evidence bytes
```

`path` is the tree locator; `blob` is the immutable byte identity.

A floating blob that exists in the object database but cannot be resolved from the recorded `view.tree` is not verified repository evidence for that view.

### 5.2 Gitlink item

A gitlink evidence item is:

```json
{
  "kind": "gitlink",
  "path": "vendor/policy-engine",
  "commit": "sha1:..."
}
```

A conforming verifier MUST establish:

```text
view.tree + path
  → tree entry mode 160000
  → recorded submodule commit OID
```

A gitlink records only the submodule commit pointer visible in the parent repository tree. It does **not** claim that the submodule contents were retrieved, rendered, or exposed.

To claim submodule source bytes as evidence, an implementation needs a separate repository view and evidence model for that submodule repository.

A `gitlink` item MUST NOT contain `blob` or `range` fields.

### 5.3 Symlinks

A symlink is a blob entry with the repository tree's symlink mode. Its blob bytes are the link target text.

A verifier MUST NOT silently follow the symlink when resolving the evidence item.

### 5.4 Byte ranges

Blob ranges are half-open byte offsets:

```text
[start, end)
```

into exact blob bytes before line-ending or encoding transformation.

Rules:

1. `0 ≤ start < end ≤ blobSize`;
2. whole-blob evidence SHOULD omit `range`;
3. a UTF-8 text renderer MUST NOT slice through a codepoint boundary;
4. range validity is checked against the recorded blob, not the current worktree file.

### 5.5 Missing partial-clone or external content

If required Git objects, LFS payloads, or other content are unavailable, the producer MUST report that condition rather than silently substituting different bytes.

A Context Pack can describe only evidence whose repository identity is known. Availability for rendering is a separate runtime condition.

---

## 6. Descriptive metadata

### 6.1 Selector identity

A producer MAY record:

```json
{
  "selector": {
    "name": "repo-context",
    "version": "2.3.1"
  }
}
```

Selector identity is diagnostic metadata. It MUST NOT be treated as proof that another implementation can reproduce the same selection.

A verifier MUST NOT reject a pack because selector identity is absent or unknown.

### 6.2 Item metadata

Items MAY carry fields such as:

```json
{
  "role": "implementation",
  "reason": "reference",
  "symbol": "Policy.checkBranchPolicy"
}
```

These are useful for explanation, not evidence identity.

Recommended `reason` values include:

```text
explicit
search
reference
definition
call
import
test
config
instruction
history
memory
tool-opened
neighbor
other
```

Implementations MAY use namespaced extensions.

### 6.3 Omission diagnostics

A producer MAY record coarse omissions:

```json
{
  "omissions": [
    {
      "path": "tests/auth.test.ts",
      "reason": "budget"
    },
    {
      "reason": "filtered",
      "count": 3
    }
  ]
}
```

Recommended reasons are:

```text
budget
unavailable
filtered
error
other
```

Omissions are deliberately non-exhaustive and non-ranked:

- order MUST NOT imply candidate rank;
- absence MUST NOT imply that an item was never considered;
- omissions MUST NOT contain or imply a canonical selector score;
- an omission is a producer diagnostic claim, not reproducible selection evidence.

### 6.4 Visibility boundary for omissions

Omission diagnostics MUST obey the same visibility boundary as the invocation.

If revealing a filtered path, OID, symbol, or filename would disclose repository content or structure the invocation was not permitted to receive, the producer MUST use aggregate or opaque diagnostics instead.

For example:

```json
{
  "reason": "filtered",
  "count": 3
}
```

is preferable to naming three inaccessible paths.

Audit diagnostics MUST NOT become a side channel around path, tenant, secret, or repository access controls.

---

## 7. Instruction provenance

Selecting content as context does not grant it instruction authority.

A comment, README, generated knowledge concept, memory entry, or source string does not become an instruction merely because it appears in a Context Pack.

When a producer claims that a repository blob was supplied with repository-derived instruction authority, it SHOULD record an annotation such as:

```json
{
  "kind": "blob",
  "path": "AGENTS.md",
  "blob": "sha256:333ccc...",
  "role": "instruction",
  "authority": {
    "source": "repository-instructions",
    "root": "sha256:def456...",
    "path": "AGENTS.md"
  }
}
```

For V1:

1. `authority.root` MUST equal `view.tree`;
2. `authority.path` MUST resolve under that tree to the recorded blob;
3. `authority.source` is descriptive;
4. an invalid authority annotation is an **unverified instruction claim** but does not invalidate the underlying blob evidence.

Actual instruction authority remains a harness/session-policy decision outside this specification.

---

## 8. ContextRender

A Context Pack is not enough to prove what repository-derived bytes crossed the invocation boundary.

Rendering may reorder, truncate, summarize, label, or otherwise transform selected evidence. Logical placement also matters: identical bytes placed in a developer/system-equivalent segment versus a user/tool segment do not have the same invocation semantics.

Therefore V1 defines ContextRender as an ordered sequence of **logical segments**.

### 8.1 Logical segment

A segment contains:

```text
placement
  logical invocation placement/class understood by the harness

mediaType
  media type of the segment body

body
  exact bytes handed across the context-to-invocation boundary
```

Example logical representation:

```json
[
  {
    "placement": "developer",
    "mediaType": "text/plain; charset=utf-8",
    "body": "<bytes>"
  },
  {
    "placement": "tool",
    "mediaType": "text/plain; charset=utf-8",
    "body": "<bytes>"
  }
]
```

The JSON above is illustrative. It is not itself the digest encoding.

### 8.2 Render framing

`renderDigest` MUST bind:

```text
segment order
+ logical semantic placement
+ media type
+ exact body bytes
```

V1 uses an unambiguous length-prefixed framing conceptually equivalent to:

```text
"git+context-render/v1\0"
segment-count
for each segment in order:
  placement-length | placement-bytes
  media-type-length | media-type-bytes
  body-length       | body-bytes
```

Lengths MUST be encoded in one documented fixed binary representation by the implementation profile. A conforming producer and verifier MUST hash the same framing bytes; delimiter-only concatenation is not sufficient.

The important invariant is that two renders with the same bodies but different logical placement or ordering MUST produce different commitments.

### 8.3 Render digest

```text
renderDigest = sha256(<ContextRender framing bytes>)
```

Provider-specific request serialization is outside the digest boundary.

HTTP framing, JSON envelopes, SDK fields, request IDs, transport compression, provider-added values, or other wire details are not part of ContextRender.

### 8.4 Provider adapters

An adapter MAY map logical ContextRender segments into provider-specific message/content fields.

It MUST NOT silently:

- move repository-derived content to a different logical placement;
- alter segment body bytes;
- inject additional repository-derived content outside ContextRender;
- truncate, summarize, or reorder content after the commitment is computed.

If such a transformation is required, the transformed logical segments constitute a new ContextRender and MUST be committed instead.

### 8.5 What the commitment does not prove

ContextRender proves a harness-side boundary claim. It does not prove that:

- the provider received the request;
- the provider preserved the message hierarchy internally;
- the model attended to the content;
- the model used the content in its output.

Those are outside the protocol guarantee.

---

## 9. Context Exposure record

A normalized Context Exposure payload may be:

```json
{
  "type": "context-exposure",
  "pack": "sha256:8d7ad4...",
  "renderFormat": "git+context-render/v1",
  "renderDigest": "sha256:6e91f2...",
  "capture": {
    "transport": "otel",
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7"
  }
}
```

The payload means:

> The signed trace producer claims that the retained Context Pack describes the repository evidence associated with this invocation boundary and that the semantically framed ContextRender committed by `renderDigest` crossed the harness context-to-invocation boundary.

`capture` is optional descriptive runtime correlation. It MUST NOT affect Context Pack identity, render verification, repository authority, or record identity.

### 9.1 Binding to invocation

The following logical invocation SHOULD reference the Context Exposure record by its qualified Git record commit OID:

```json
{
  "type": "invocation-telemetry",
  "exposure": "sha1:abc123..."
}
```

Trace DAG ancestry MAY provide additional causal structure, but timestamp proximity alone is not an authoritative join.

### 9.2 One exposure per auditable invocation

For strongest auditability, each repository-affecting logical invocation SHOULD have one Context Exposure describing the repository context for that invocation.

If repository context changes before a later invocation, a new exposure MUST be recorded.

Tool output that becomes repository context in a later model call SHOULD be represented in that later exposure where it can be Git-grounded.

---

## 10. Durable Git storage and reachability

Mentioning a Git OID inside JSON does **not** create a Git reachability edge.

A durable exposure therefore MUST retain the captured `view.tree` through the actual Git object graph.

### 10.1 Required reachability edge

One valid specialized trace-record tree is:

```text
Context Exposure record commit
└── tree
    ├── record.json
    ├── record.sig
    └── context/
        ├── pack.json
        ├── render.bin          # optional under retention policy
        └── view/               # tree entry whose OID == view.tree
            └── ...
```

`context/view` is a real tree entry pointing at the captured repository root tree. Because Git reachability follows tree entries, the repository objects beneath that tree remain reachable from the exposure record for as long as the record remains reachable.

This rule applies to **clean and dirty views**. A clean historical view can also become unreachable after branch deletion or history rewriting; an OID written only in JSON is not sufficient retention.

### 10.2 Pack identity

`context/pack.json` is the exact persisted Context Pack blob.

Its Git blob OID is the pack identity named by the exposure payload.

V1 does not require canonical JSON. Semantically equivalent reserialization may produce another blob OID, which is acceptable. A consumer MUST NOT claim that reserialized bytes have the original pack OID.

### 10.3 Render retention

When policy permits, `context/render.bin` SHOULD retain the exact ContextRender framing bytes so `renderDigest` can be recomputed.

Rendered content may contain source, secrets, or derived text and therefore follows repository/session retention, access-control, and redaction policy.

If the render body is later redacted or expired, the digest remains a historical commitment but the exact render can no longer be independently inspected.

### 10.4 Base commit retention

Retaining `view.tree` is sufficient to preserve exact repository evidence under that tree.

If a product promises ancestry inspection through `view.base`, it must separately ensure that the base commit and required ancestors remain retained. Merely naming `view.base` in JSON is not a reachability edge either.

### 10.5 Writer implementation

The existing generic signed-record writer may need a specialized attachment/tree facility to construct this layout. A fixed `record.json` + signature-only tree is insufficient for durable Context Exposure because it cannot retain `view.tree` through graph reachability.

---

## 11. Verification

A verifier auditing a Context Exposure SHOULD perform these checks independently.

### 11.1 Record checks

```text
record signature/trust valid for trace producer
record bound to expected repository/session
record commit reachable under intended trace-retention rules
```

### 11.2 Pack checks

```text
pack blob exists
pack blob OID matches payload.pack
pack JSON parses
supported pack version
```

### 11.3 View checks

```text
context/view tree entry exists
context/view tree OID == pack.view.tree
```

### 11.4 Evidence checks

For every `blob` item:

```text
path resolves under view.tree
resolved object == item.blob
range valid when present
```

For every `gitlink` item:

```text
path resolves under view.tree
mode == 160000
resolved object == item.commit
```

### 11.5 Render checks

When retained:

```text
parse documented ContextRender framing
recompute SHA-256
result == renderDigest
```

A verifier MUST distinguish:

```text
repository evidence verified
render commitment verified
render body unavailable
runtime correlation available/unavailable
```

These are separate evidence dimensions.

---

## 12. Retrieval and resource safety

Retrieval remains implementation-specific.

A producer may use lexical search, syntax indexes, semantic search, recursive tool exploration, history, or any combination.

Derived indexes are disposable accelerators. Removing them MUST NOT invalidate a persisted Context Pack.

Implementations MUST still bound attacker-controlled work such as:

- parsing;
- graph expansion;
- history scans;
- candidate generation;
- render size;
- memory use;
- wall-clock time.

Those limits are host policy and need not become protocol fields.

---

## 13. Product surface

A minimal product surface is:

```text
git+ context for --task "..."
git+ context why <pack> [item]
git+ context audit <operation-or-trace-record>
```

### 13.1 `context for`

Generates a Context Pack using the current retrieval implementation.

It MAY display scores, graph paths, token estimates, or selector diagnostics, but those are not required persisted fields.

### 13.2 `context why`

Explains recorded evidence and metadata while distinguishing verified facts from selector explanations.

For a blob it SHOULD show:

```text
kind: blob
path: src/auth.ts
blob: sha256:...
range: 1200-1840
view: sha256:...
reason: reference
```

For a gitlink it SHOULD show the recorded submodule commit pointer and explicitly state that submodule contents were not thereby exposed.

### 13.3 `context audit`

Audits an exposure or invocation and SHOULD report:

```text
repository base commit
retained effective tree
Context Pack OID
render format + digest
whether exact render bytes remain available
logical segment placements
blob/gitlink verification
selector identity when recorded
privacy-safe omission diagnostics
instruction-provenance verification
bound logical invocation record
OTel correlation when present
```

---

## 14. Failure examples

### 14.1 Missing context

An agent changes authentication behavior incorrectly.

Audit shows that `src/policy.ts` and its tests were absent from the exposure.

The audit can state that those repository items were not present in the recorded context. It cannot prove that their absence caused the error.

### 14.2 Stale context

The pack claims:

```text
view.tree: tree NEW
path: src/api.ts
blob: blob OLD
```

but `tree NEW + src/api.ts` resolves to another blob.

The item fails repository-view verification.

### 14.3 Semantic-placement bug

The same source bytes were moved from a developer-equivalent segment into an ordinary user segment.

Because placement is part of ContextRender framing, the new exposure has a different render commitment even when all segment body bytes are identical.

### 14.4 Rendering truncation

The pack contains the correct blob/range but the final render cuts off part of it.

Pack verification can succeed while render inspection shows the actual exposed bytes were incomplete.

### 14.5 Historical object loss prevented

A branch containing the clean source commit is deleted after the session.

Because the Context Exposure record contains a real `context/view` tree edge, the exact captured tree and evidence remain reachable for the trace-retention period.

---

## 15. Acceptance criteria

V1 is successful when:

1. every auditable logical invocation can be associated with one exact Repository View;
2. blob evidence verifies as `view.tree + path → blob` with optional valid byte range;
3. gitlink evidence verifies as `view.tree + path + mode 160000 → commit`;
4. a gitlink never implies submodule content exposure;
5. dirty worktree state is represented by an exact overlay tree rather than mislabeled as `HEAD`;
6. ContextRender binds segment order, logical semantic placement, media type, and exact body bytes;
7. provider serialization remains outside the render digest boundary;
8. a later invocation references its exposure by immutable Git record commit OID;
9. exposure records live in the policy-invisible audit trace rather than expanding the policy-critical session DAG;
10. the exposure's actual Git tree contains a reachability edge to `view.tree` for clean and dirty captures;
11. the pack and required view/evidence objects survive normal Git GC for the intended audit-retention period;
12. omission diagnostics do not leak inaccessible paths or object identities;
13. a verifier can validate evidence without reproducing ranking, embeddings, graph construction, or tokenizer behavior;
14. changing retrieval implementations does not alter the meaning of already persisted packs;
15. Context Pack selection does not grant instruction authority;
16. OTel/provider identifiers remain correlation metadata rather than Git-native identity;
17. no record claims model cognition or causation.

---

## Appendix A — Optional retrieval diagnostics

Implementations MAY keep richer diagnostics outside the core protocol:

```text
candidate scores
rank position
semantic similarity
graph paths
syntax captures
symbol resolution quality
token estimates
candidate/omission logs
retrieval latency
resource counters
reranker identity
```

These can improve retrieval quality without becoming Context Pack validation inputs.

---

## Appendix B — Optional selector reproducibility profiles

A selector MAY define an implementation-specific reproducibility profile that pins extractor versions, query packs, graph representation, ranking arithmetic, resource limits, or token estimators.

That profile answers:

> **Could another conforming selector reproduce this selection?**

The core Context Pack answers:

> **What Git-grounded evidence was associated with this invocation, and can its repository provenance and actual render commitment be verified?**

The guarantees SHOULD remain separate.

---

## Final invariant

> **A Context Pack is an immutable manifest of typed Git-grounded evidence resolved under one exact Repository View. A Context Exposure is a signed, policy-invisible trace record that retains that view through a real Git tree edge and commits to an ordered ContextRender whose digest binds exact repository-derived bytes, media types, and logical semantic placement. Blob and gitlink evidence have distinct verification rules; provider/OTel identifiers are correlation only; retrieval remains replaceable; and neither selection nor exposure proves cognition or causation.**
