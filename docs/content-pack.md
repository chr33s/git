# Git-Native Context Packs

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Spec revision:** draft-8

## 1. Summary

This specification defines a small Git-native audit primitive for repository context used by coding agents.

The problem is not to standardize how an agent retrieves or ranks code. The problem is to make it possible to answer, after an agent operation:

> **What repository state and repository evidence crossed the harness context boundary for the model invocation that preceded this operation?**

V1 defines three concepts:

1. **Repository View** — the exact repository snapshot from which repository evidence may be selected.
2. **Context Pack** — an immutable manifest of Git-grounded repository evidence associated with one auditable exposure.
3. **Context Exposure Event** — a signed audit-trace record binding a Context Pack and an exact, semantically framed ContextRender artifact to one model invocation.

The model is:

```text
Repository View
      ↓
replaceable retrieval
      ↓
 Context Pack
      ↓
   renderer
      ↓
 ContextRender
      ↓
Context Exposure Event
      ↓
model invocation
      ↓
agent operation
```

The protocol boundary is deliberately narrow:

```text
Retrieval quality
  "Did we choose the right context?"

        is separate from

Context provenance
  "Can we audit what repository context crossed the harness boundary?"
```

V1 standardizes **context provenance**, not retrieval quality.

Runtime conditions such as model/provider identity, token usage, retries, context pressure, compaction, tool diagnostics, and workspace transitions are specified separately in [invocation-telemetry.md](invocation-telemetry.md). Product integration and UI guidance live in [telemetry-integration.md](telemetry-integration.md). The broader Capture → Retention → Recall objective is described in [knowledge-durability.md](knowledge-durability.md).

A Context Pack does not prove that a model read, understood, remembered, or used any item. A Context Exposure Event does not prove causation. They provide tamper-evident evidence of what the harness claims crossed its repository-context boundary.

---

## 2. Goals and non-goals

### 2.1 Goals

V1 MUST make it possible to:

1. identify the exact repository snapshot used for retrieval;
2. resolve selected source evidence to immutable Git objects;
3. distinguish ordinary blob evidence from submodule gitlinks;
4. record optional byte ranges for blob evidence;
5. bind repository-context bytes **and their logical placement** to one exposure;
6. preserve the repository view through real Git reachability, not only OIDs embedded in JSON;
7. validate pack evidence without reproducing the selector;
8. record lightweight selector identity, coarse omission diagnostics, and verifiable instruction provenance without making them consensus inputs;
9. bind later invocation/telemetry records to exposures by immutable trace-record identity.

### 2.2 Non-goals

V1 does **not** standardize:

- deterministic retrieval;
- ranking weights or scoring arithmetic;
- fixed-point math;
- token estimators;
- candidate limits;
- CodeGraph schemas;
- Tree-sitter query semantics;
- compiler, LSP, SCIP, or embedding behavior;
- graph digests;
- selector configuration digests;
- task normalization digests;
- exhaustive candidate or omission logs;
- byte-identical pack serialization across independent producers;
- provider request serialization;
- model attention, cognition, memory, or causation.

---

## 3. Core principles

### 3.1 Retrieval is replaceable

A producer MAY use grep, Tree-sitter, compiler APIs, LSP, SCIP, embeddings, an LLM reranker, explicit file reads, recursive tool use, or any combination.

> **Retrieval may be probabilistic. Evidence identity must not be.**

### 3.2 Evidence is Git-grounded

V1 has two core repository evidence item kinds:

```text
blob
  path + blob OID + optional byte range

gitlink
  path + submodule commit OID
```

A path proves membership in the recorded repository tree. The object OID identifies the immutable object at that path.

### 3.3 The effective repository tree is authoritative

`view.tree` is the exact root tree visible to retrieval. Dirty worktrees MUST be represented by an overlay tree; `HEAD` MUST NOT be recorded when the model was supplied different worktree bytes.

### 3.4 Exposure binds bytes and placement

Repository-derived bytes can have materially different meaning depending on whether the harness places them in a system/developer instruction channel, a user/request channel, or a tool-result channel.

Therefore a ContextRender is not merely a concatenated byte string. It is an ordered sequence of **segments**, each binding:

```text
logical placement
media type
exact bytes
```

Provider JSON, SDK serialization, HTTP framing, and transport details remain outside the digest boundary.

### 3.5 Durable evidence requires real Git edges

An OID written inside JSON is not a Git reachability edge.

A durable exposure MUST make `view.tree` reachable through the Git object graph from the audit record or another retained provenance root. This rule applies to both clean and dirty views.

### 3.6 High-frequency audit provenance is not policy-critical session history

The existing session DAG is the distilled record used by provenance and policy. Per-invocation Context Exposure Events belong in the sibling signed audit trace described by [invocation-telemetry.md](invocation-telemetry.md), normally:

```text
refs/hub/trace/<session-id>
```

The trace MUST NOT be consulted to authorize pushes, merges, membership, or protected-branch policy.

### 3.7 Context does not grant instruction authority

Selecting a file, comment, memory entry, summary, or tool result MUST NOT make it an instruction merely because it appears in context. `authority` metadata records provenance of an instruction claim; actual authority remains harness/session policy.

---

## 4. Repository View

A Repository View names the exact source snapshot from which repository evidence may be selected:

```json
{
  "base": "sha256:abc123...",
  "tree": "sha256:def456..."
}
```

### 4.1 `base`

`base` is the commit OID anchoring committed history for the operation.

### 4.2 `tree`

`tree` is the root tree OID of the **effective repository snapshot** visible to retrieval.

For a clean worktree:

```text
view.tree == root tree of view.base
```

For a dirty worktree, the implementation MUST construct an overlay tree representing every repository path retrieval is allowed to inspect.

The selector MUST resolve evidence from `view.tree`, not from mutable filesystem state after the view has been captured.

### 4.3 Object identifiers

Git object identifiers are encoded as:

```text
<algorithm>:<lowercase-hex>
```

where `algorithm` matches the repository object format, currently `sha1` or `sha256`.

Consumers MUST treat Git OIDs as opaque and MUST NOT assume SHA-1 length.

---

## 5. Context Pack

A Context Pack is an immutable manifest of repository evidence associated with one auditable exposure.

```json
{
  "version": 1,
  "view": {
    "base": "sha256:abc123...",
    "tree": "sha256:def456..."
  },
  "selector": {
    "name": "repo-context",
    "version": "2.3.1"
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
      "role": "dependency"
    }
  ]
}
```

### 5.1 Blob items

A blob item is:

```json
{
  "kind": "blob",
  "path": "src/auth.ts",
  "blob": "sha256:...",
  "range": [1200, 1840]
}
```

Rules:

1. `kind` MUST equal `blob`;
2. `path` MUST resolve under `view.tree` to a non-gitlink tree entry;
3. the resolved object OID MUST equal `blob`;
4. `range`, when present, is a half-open byte range `[start, end)` into the exact blob bytes;
5. `0 ≤ start < end ≤ blobSize`;
6. a UTF-8 renderer MUST NOT slice through a codepoint boundary.

A floating blob that exists in the object database but does not resolve from `view.tree` is not verified repository evidence for that view.

### 5.2 Gitlink items

A gitlink item is:

```json
{
  "kind": "gitlink",
  "path": "vendor/policy-engine",
  "commit": "sha1:..."
}
```

Rules:

1. `kind` MUST equal `gitlink`;
2. `path` MUST resolve under `view.tree` to Git mode `160000`;
3. the gitlink target OID MUST equal `commit`;
4. `range` and `blob` MUST NOT appear on a gitlink item.

A gitlink proves only that the recorded repository view pointed at that submodule commit. It does **not** prove the contents of the submodule repository were available to the model. Traversing and auditing submodule contents requires a separately identified repository view or a future extension.

### 5.3 Symlinks

Symlinks are blob items containing the link bytes. Verification MUST NOT silently follow the symlink target.

### 5.4 Descriptive metadata

Fields such as `role`, `reason`, `symbol`, and selector metadata are descriptive. A verifier MUST NOT require them to validate repository evidence.

Recommended `reason` labels include:

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

### 5.5 Selector identity

A producer MAY record:

```json
{
  "selector": {
    "name": "repo-context",
    "version": "2.3.1"
  }
}
```

This identifies the implementation for diagnosis and evaluation. It MUST NOT be interpreted as a reproducibility claim or selector configuration digest.

### 5.6 Omission diagnostics

A producer MAY record coarse non-exhaustive omissions:

```json
{
  "omissions": [
    { "path": "tests/auth.test.ts", "reason": "budget" },
    { "reason": "filtered", "count": 3 }
  ]
}
```

Recommended core reasons are:

```text
budget
unavailable
filtered
error
other
```

Rules:

- omission order MUST NOT imply rank;
- omissions MUST NOT contain or imply canonical selector scores;
- absence from `omissions` MUST NOT prove an item was never considered;
- omission diagnostics are producer claims, not reproducible selection evidence;
- omission diagnostics MUST obey the same visibility policy as the consumer that may read them.

If naming a filtered path or OID would reveal a resource the consumer is not authorized to know exists, the producer MUST omit that identifier and MAY record only an aggregate or opaque diagnostic.

### 5.7 Verifiable instruction provenance

A blob item MAY carry:

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
2. `authority.path` MUST resolve under that tree to the same blob;
3. `authority.source` is descriptive;
4. a failed annotation SHOULD be surfaced as an **unverified instruction claim** without invalidating the underlying blob evidence.

A valid annotation proves provenance of the claimed instruction source, not that the source actually had authority.

### 5.8 Encoding and identity

A Context Pack is ordinary UTF-8 JSON. V1 does not require JCS or byte-identical serialization across producers.

When persisted as a Git blob, the pack identity is the Git OID of the **actual bytes persisted**. Semantically equivalent serializations MAY have different OIDs.

---

## 6. ContextRender

### 6.1 Logical segments

A ContextRender is an ordered sequence of segments:

```text
segment 0
  placement = developer
  media     = text/plain; charset=utf-8
  bytes     = ...

segment 1
  placement = tool
  media     = text/plain; charset=utf-8
  bytes     = ...
```

Core logical placement values are:

```text
system
developer
user
tool
other
```

A producer MAY use a namespaced placement when a harness has semantics that cannot be faithfully represented by the core set.

`placement` records the harness-level semantic channel. It does not itself grant instruction authority.

### 6.2 Framing

`renderDigest` MUST bind segment order, placement, media type, and exact bytes without depending on JSON canonicalization.

V1 encodes the ContextRender digest artifact as:

```text
ASCII "git+ContextRender\0v1\0"
for each segment, in order:
  u32be placementByteLength
  placement UTF-8 bytes
  u32be mediaByteLength
  media UTF-8 bytes
  u64be bodyByteLength
  exact body bytes
```

Integers are unsigned big-endian. Lengths count bytes, not characters.

The digest is:

```text
sha256(<framed ContextRender artifact>)
```

This deliberately standardizes only the tiny framing required to make the commitment unambiguous.

### 6.3 Provider mapping

Provider JSON envelopes, SDK serialization, message IDs, HTTP framing, transport compression, and provider-added fields remain outside the digest boundary.

A provider adapter MUST map each ContextRender segment into an equivalent provider field without changing:

```text
segment order
logical placement semantics
segment body bytes
```

If it must transform, truncate, summarize, reorder, or change placement, the harness MUST construct and hash a new final ContextRender.

The adapter MUST NOT inject additional repository-derived content outside ContextRender.

---

## 7. Context Exposure Event

A Context Exposure Event is a signed audit-trace record:

```json
{
  "type": "context-exposure",
  "pack": "sha256:8d7ad4...",
  "renderFormat": "git+context-render/v1",
  "renderDigest": "sha256:6e91f2..."
}
```

It means:

> The harness claims that `pack` describes the repository evidence for this invocation and that the final semantically framed ContextRender hashed to `renderDigest`.

The record derives authorship and ordering from the signed audit trace. It does not carry a second signature system.

### 7.1 Immutable event references

Later invocation or telemetry records MUST reference an exposure by the **qualified Git commit OID of the trace record that contains it**, not merely by a UUID-like event ID.

Human-friendly event IDs MAY exist for display/search, but they are not sufficient immutable references when multiple records could claim the same identifier.

The exposure itself MUST NOT contain its own commit OID.

### 7.2 One exposure per auditable invocation

For strongest auditability, each repository-affecting model invocation SHOULD have one exposure. If repository context changes between invocations, a new exposure MUST be recorded.

---

## 8. Durable storage and reachability

### 8.1 Required reachability shape

For a durable exposure, the audit record MUST create a real Git reachability path to both the pack and the captured repository view.

One valid layout is:

```text
trace record commit
└── tree
    ├── event.json
    ├── event.sig
    └── context/
        ├── pack.json
        └── view/          mode 040000 → exact view.tree
```

`context/view` is a tree entry whose OID is exactly `view.tree`.

This preserves the entire recorded tree through ordinary Git reachability, including selected evidence, even if source branches move, are deleted, or history is rewritten.

Embedding `view.tree` only as text inside `pack.json` is insufficient for durable audit.

### 8.2 Clean and dirty views

The reachability rule applies equally to clean and dirty views. Dirty overlay trees are not a special case; both can otherwise become unreachable.

### 8.3 ContextRender retention

Exact ContextRender bodies SHOULD be retained when policy permits, but they may contain sensitive source. They follow the session/trace retention and redaction lifecycle.

If bytes remain available, a verifier MUST be able to recompute `renderDigest`. If the body expires, the digest remains a commitment but the rendering is no longer independently inspectable.

---

## 9. Audit trace relationship

Per-invocation exposure is historical audit evidence, but it is not merge authorization or branch policy state.

A conforming implementation SHOULD place exposure records in the sibling trace namespace described by [invocation-telemetry.md](invocation-telemetry.md):

```text
refs/hub/session/<session>   distilled provenance / policy-visible
refs/hub/trace/<session>     detailed signed audit trace / policy-invisible
```

The trace event envelope MUST bind the repository and session identity.

An implementation MUST NOT make protected-branch validation depend on folding the high-frequency trace.

---

## 10. Product surface

A minimal product surface is:

```text
git+ context for --task "..."
git+ context why <pack> [item]
git+ context audit <operation-or-trace-record>
```

`context audit` SHOULD report:

```text
base commit
effective tree
whether the view tree is durably reachable
pack OID
blob/gitlink verification
ContextRender format and digest
retained render availability
segment placements
selector identity / omissions when available
instruction provenance verification
exposure → invocation binding
```

---

## 11. Audit examples

### 11.1 Missing evidence

A pack lacks `tests/policy.test.ts`. The audit can state that the test was absent from the recorded repository context. It cannot prove that the absence caused an incorrect edit.

### 11.2 Stale evidence

`view.tree` resolves `src/policy.ts` to blob NEW, while the pack records blob OLD. The item fails repository-view verification.

### 11.3 Semantic placement mismatch

The recorded ContextRender says `AGENTS.md` was in a `developer` segment, but the provider adapter placed those bytes in a `user` field. The adapter did not faithfully map the recorded artifact; the exposure is not a valid account of the provider-bound context.

### 11.4 Submodule pointer

A gitlink item proves `vendor/policy-engine` pointed at commit X in the parent repository. It does not prove files inside that submodule commit were exposed.

### 11.5 Reachability failure

A pack names historical tree A only inside JSON. After branch deletion, tree A is garbage-collected. The pack bytes survive but the repository evidence does not. This is a durable-audit failure. A conforming persisted exposure prevents it through a real `context/view` tree edge.

---

## 12. Acceptance criteria

V1 is successful when:

1. every auditable exposure identifies one exact Repository View;
2. every blob item resolves from `view.tree` to the recorded blob;
3. every gitlink item resolves from `view.tree` as mode `160000` to the recorded commit;
4. dirty views use exact overlay trees;
5. ContextRender binds exact bytes, order, media type, and logical placement;
6. provider envelopes remain outside the digest without permitting unrecorded repository-derived content;
7. later events reference exposures by immutable trace-record commit identity;
8. persisted exposures create real Git reachability to `view.tree` and the pack;
9. clean and dirty historical views survive ordinary GC for the intended retention period;
10. selector identity and omission diagnostics remain non-consensus diagnostics;
11. omission metadata does not leak hidden resources;
12. instruction provenance can be verified without turning relevance into authority;
13. validation requires no deterministic ranking, graph hashing, token estimator, or cross-platform numeric convention;
14. high-frequency exposure records are not required on the policy-critical session DAG;
15. no object claims model cognition or causation.

---

## Final invariant

> **A Context Pack is an immutable manifest of Git-grounded blob and gitlink evidence under an exact repository tree. A durable exposure keeps that tree reachable through real Git edges. A Context Exposure Event commits to an ordered, semantically placed ContextRender at the harness boundary and is referenced later by immutable trace-record identity. Retrieval and ranking remain replaceable; provider serialization, runtime telemetry, cognition, and causation remain outside the Context Pack guarantee.**
