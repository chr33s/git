# Git-Native Context Packs

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-22  
**Spec revision:** draft-9

## 1. Purpose

A Context Pack makes repository context auditable without standardizing how an agent retrieves or ranks code.

It answers:

> **What exact Git repository state and repository evidence did the harness expose at this invocation boundary?**

V1 has four protocol concepts:

```text
Repository View
      ↓
 Context Pack
      ↓
 ContextRender
      ↓
Context Exposure
      ↓
 logical invocation
```

- **Repository View** — the exact Git tree from which repository evidence is resolved.
- **Context Pack** — an immutable JSON manifest of Git-grounded evidence selected for one exposure.
- **ContextRender** — the exact ordered semantic segments handed from the context subsystem to the invocation subsystem.
- **Context Exposure** — a signed Git+ audit record binding the pack, render commitment, retained view, and optional runtime correlation.

Retrieval quality is intentionally outside the protocol. A pack can prove what evidence was exposed without proving that the selector chose good evidence, that the model attended to it, or that it caused an outcome.

Runtime model/provider details, tools, usage, retries, context lifecycle, and OpenTelemetry correlation are specified in [telemetry.md](telemetry.md).

---

## 2. Invariants

A conforming implementation MUST preserve these invariants:

1. repository evidence resolves against one exact `view.tree`;
2. blob evidence and submodule gitlinks are distinct item kinds;
3. dirty-worktree context is represented by an exact overlay tree rather than mislabeled as `HEAD`;
4. the ContextRender commitment binds segment order, logical placement, media type, and exact body bytes;
5. a durable exposure keeps `view.tree` reachable through the Git object graph, not merely by writing its OID in JSON;
6. canonical cross-record references use qualified Git record commit OIDs;
7. Context Exposure is audit data and does not enter authorization or protected-branch policy folds;
8. selected content does not gain instruction authority merely by being present in context;
9. omission diagnostics obey the same visibility boundary as the invocation;
10. retrieval indexes and selector internals remain replaceable.

> **Retrieval may be probabilistic. Evidence identity must not be.**

---

## 3. Audit placement and identity

Context Exposure records live under the policy-invisible audit trace:

```text
refs/hub/session/<session-id>
  distilled lifecycle / decisions / produced result
  may be consulted by policy

refs/hub/trace/<session-id>
  Context Exposure / invocation / tools / workspace / lifecycle
  not consulted for authorization or merge policy
```

A persisted Context Exposure is canonically identified by its signed Git record commit OID:

```text
sha1:<hex>
sha256:<hex>
```

Display IDs, OTel `TraceId`/`SpanId`, provider request IDs, and harness event IDs are correlation identifiers only.

A later logical invocation SHOULD reference its exposure by record commit OID.

---

## 4. Repository View

A Repository View names the exact source snapshot used for repository-context resolution:

```json
{
  "base": "sha256:abc123...",
  "tree": "sha256:def456..."
}
```

### 4.1 `base`

`base` is the commit anchoring committed history for the operation. It is useful for ancestry and inspection but is not, by itself, the effective context snapshot.

### 4.2 `tree`

`tree` is the root tree OID of the effective repository snapshot visible to retrieval.

For a clean worktree:

```text
view.tree == root tree of view.base
```

For a dirty worktree, the producer MUST construct an overlay tree containing the exact repository paths and bytes retrieval is allowed to inspect.

Retrieval MUST resolve repository evidence from `view.tree`, not from mutable filesystem state after the view has been captured.

### 4.3 Object identifiers

Git OIDs use:

```text
<algorithm>:<lowercase-hex>
```

where the algorithm matches the repository object format, currently `sha1` or `sha256`.

Serialized protocol records MUST use qualified OIDs. Human-facing CLI input MAY accept any unambiguous revision or abbreviated OID that the repository can resolve.

---

## 5. Context Pack

A Context Pack is ordinary UTF-8 JSON.

Example:

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

The pack's identity is the Git blob OID of the exact JSON bytes persisted. V1 does not require canonical JSON; semantically equivalent serializations may have different pack OIDs.

### 5.1 Blob evidence

A blob item is:

```json
{
  "kind": "blob",
  "path": "src/auth.ts",
  "blob": "sha256:...",
  "range": [1200, 1840]
}
```

A verifier MUST establish:

```text
view.tree + path → recorded blob
recorded blob + optional range → exact evidence bytes
```

A blob that exists in the object database but cannot be resolved from `view.tree` at `path` is not verified evidence for that view.

Ranges are half-open byte offsets `[start, end)` into exact blob bytes. They MUST satisfy:

```text
0 ≤ start < end ≤ blobSize
```

Whole-blob evidence SHOULD omit `range`. A UTF-8 renderer MUST NOT slice through a codepoint boundary.

Symlinks are blob evidence containing the link-target bytes. A verifier MUST NOT silently follow them.

### 5.2 Gitlink evidence

A gitlink item is:

```json
{
  "kind": "gitlink",
  "path": "vendor/policy-engine",
  "commit": "sha1:..."
}
```

A verifier MUST establish:

```text
view.tree + path
  → mode 160000
  → recorded submodule commit OID
```

A gitlink proves only that the parent repository pointed at that submodule commit. It does not prove that submodule files were retrieved or exposed.

A gitlink MUST NOT contain `blob` or `range`.

To expose source from a submodule, the producer needs a separately grounded repository view for that repository.

### 5.3 Unavailable content

If a required Git object, partial-clone object, LFS payload, or other content is unavailable, the producer MUST report the condition rather than substitute different bytes.

---

## 6. Descriptive metadata

Only repository identity and evidence-resolution fields are protocol-critical. Selection metadata remains descriptive.

A producer MAY record selector identity:

```json
{
  "selector": {
    "name": "repo-context",
    "version": "2.3.1"
  }
}
```

Items MAY carry fields such as `role`, `reason`, or `symbol`.

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

A verifier MUST NOT require selector identity, scores, graph paths, embeddings, parser metadata, or token estimates to verify repository evidence.

### 6.1 Omission diagnostics

A producer MAY record coarse omissions:

```json
{
  "omissions": [
    { "path": "tests/auth.test.ts", "reason": "budget" },
    { "reason": "filtered", "count": 3 }
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

Omissions are non-exhaustive and non-ranked. Their order MUST NOT imply rank, and absence MUST NOT imply that an item was never considered.

Omission diagnostics MUST obey the invocation's visibility boundary. If naming a filtered path, OID, symbol, or filename would reveal inaccessible repository structure, the producer MUST use aggregate or opaque diagnostics instead.

---

## 7. Instruction provenance

Context relevance does not grant instruction authority.

A comment, README, Knowledge Concept, memory entry, source string, or retrieved tool result remains data unless independent harness/repository policy grants it authority.

When a producer claims that a repository blob was supplied with repository-derived instruction authority, it SHOULD record verifiable provenance:

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
2. `authority.path` MUST resolve under that tree to the recorded item blob;
3. `authority.source` is descriptive;
4. an invalid annotation is an unverified instruction claim, not invalidation of the underlying evidence item.

---

## 8. ContextRender

A Context Pack says which repository evidence was selected. ContextRender commits to what repository-derived bytes actually crossed the harness context-to-invocation boundary.

A ContextRender is an ordered sequence of logical segments. Each segment has:

```text
placement
  logical invocation placement understood by the harness

mediaType
  media type of the segment body

body
  exact bytes crossing the boundary
```

Core placement values are:

```text
system
developer
user
tool
other
```

Implementations MAY use namespaced extension placements. Placement records semantic location, not instruction authority.

### 8.1 Exact V1 framing

There is exactly one V1 digest framing.

All integers are unsigned big-endian. Strings are UTF-8 bytes with no terminator. No Unicode normalization is performed. Body bytes are hashed exactly as supplied.

```text
ASCII "git+ContextRender\0v1\0"

u32be segmentCount

for each segment, in order:
  u32be placementByteLength
  placement UTF-8 bytes

  u32be mediaTypeByteLength
  mediaType UTF-8 bytes

  u64be bodyByteLength
  exact body bytes
```

No alternative delimiter scheme, integer width, byte order, JSON encoding, or implementation profile is valid for `git+context-render/v1`.

The commitment is:

```text
renderDigest = sha256(<exact framing bytes above>)
```

Therefore any change to segment order, placement, media type, or body bytes changes the digest.

### 8.2 Provider adapters

Provider-specific request serialization is outside the digest boundary.

An adapter MAY map logical segments into provider message/content fields, but after `renderDigest` is computed it MUST NOT silently:

- change logical placement;
- alter body bytes;
- reorder segments;
- inject additional repository-derived context;
- truncate or summarize repository-derived context.

If such a transformation is required, the transformed segments constitute a new ContextRender and MUST be hashed instead.

The commitment is a harness-side boundary claim. It does not prove that a provider received the request, preserved the hierarchy internally, or that the model attended to or used the content.

---

## 9. Context Exposure

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

It means that the signed trace producer claims the retained Context Pack describes repository evidence for the invocation boundary and that the committed ContextRender crossed that boundary.

`capture` is optional descriptive runtime correlation. It MUST NOT affect pack identity, render verification, authority, or record identity.

The following logical invocation SHOULD reference the exposure by qualified Git record commit OID:

```json
{
  "type": "invocation-telemetry",
  "exposure": "sha1:abc123..."
}
```

Trace ancestry MAY provide additional causal structure. Timestamp proximity alone is not an authoritative join.

For strongest auditability, each repository-affecting logical invocation SHOULD have one exposure. If repository context changes before a later invocation, the later invocation requires a new exposure.

---

## 10. Durable Git reachability

Writing an OID inside JSON does not make the referenced Git object reachable.

A durable Context Exposure MUST retain `view.tree` through an actual Git tree edge.

One valid record layout is:

```text
Context Exposure record commit
└── tree
    ├── event.json
    ├── event.sig
    └── context/
        ├── pack.json
        ├── render.bin          # optional under retention policy
        └── view/               # tree entry OID == view.tree
            └── ...
```

The exact top-level event filenames SHOULD follow the repository's existing signed-record convention. The specialized requirement is the attached `context/` subtree and its real `context/view` edge.

This rule applies to clean and dirty views. A clean historical tree can also become unreachable after history rewriting or branch deletion.

When policy permits, `context/render.bin` SHOULD retain the exact V1 framing bytes so `renderDigest` can be recomputed. If those bytes later expire or are redacted, the digest remains a commitment but the render can no longer be independently inspected.

If ancestry through `view.base` is promised, the base commit and required ancestors require their own retention path; naming the commit in JSON is not enough.

---

## 11. Verification

A Context Exposure audit SHOULD report these dimensions independently:

### Record

```text
signature/trust valid for trace producer
repository/session binding valid
record reachable under trace-retention policy
```

### Pack and view

```text
pack blob exists
pack OID matches payload.pack
pack JSON parses
supported pack version
context/view exists
context/view OID == pack.view.tree
```

### Evidence

For each blob:

```text
path resolves under view.tree
resolved object == item.blob
range valid when present
```

For each gitlink:

```text
path resolves under view.tree
mode == 160000
resolved object == item.commit
```

### Render

When retained:

```text
parse exact git+context-render/v1 framing
recompute SHA-256
result == renderDigest
```

A verifier MUST distinguish valid repository evidence, valid render commitment, unavailable render body, and available/unavailable runtime correlation.

---

## 12. Retrieval and resource safety

Retrieval remains implementation-specific. Producers may use lexical search, syntax indexes, semantic search, recursive tool exploration, history, or any combination.

Derived indexes are disposable accelerators and MUST NOT be required to validate an already persisted pack.

Implementations still need host-defined bounds on parsing, graph expansion, history scans, candidate generation, render size, memory, and wall-clock time.

---

## 13. Product surface

The normal user-facing commands are intentionally small:

```text
git+ context for --task "..."
git+ context why <pack> [item]
git+ context audit <invocation-or-exposure>
```

Repo-scoped commands SHOULD discover the current checkout by default. Explicit repository selection remains available for bare/server administration.

CLI inputs MAY use ordinary Git revisions and unambiguous abbreviated OIDs; serialized protocol records continue to use qualified OIDs.

The UI SHOULD project Context Exposure together with its logical runtime record as one **Invocation**. Users only need to inspect the separate trace records when debugging protocol/audit details.

### `context for`

Builds a Context Pack using the current retrieval implementation and prints the selected evidence. Selector scores and diagnostics may be shown but are not protocol identity.

### `context why`

Explains recorded evidence and descriptive selection metadata while distinguishing verified Git facts from selector explanations.

### `context audit`

Verifies the historical exposure and reports repository view, pack identity, blob/gitlink checks, ContextRender status, bound logical invocation, and optional OTel correlation.

---

## 14. Security summary

- Context selection does not create instruction authority.
- Repository evidence must obey path/repository/tenant access control.
- Omission diagnostics cannot reveal inaccessible structure.
- ContextRender may contain source or secrets and follows repository retention/redaction policy.
- External runtime identifiers are correlation metadata, not Git identity.
- A valid exposure proves a signed harness-side claim about repository context, not cognition or causation.

---

## 15. Acceptance criteria

V1 is successful when:

1. every audited repository-affecting invocation can identify one exact Repository View;
2. every blob and gitlink item verifies against `view.tree` using its kind-specific rule;
3. dirty context uses an exact overlay tree;
4. `git+context-render/v1` produces cross-language-identical framing bytes and digest;
5. changing segment placement or order changes the render digest;
6. the persisted exposure keeps `view.tree` reachable through an actual Git edge;
7. later canonical records reference the exposure by Git record commit OID;
8. OTel/provider IDs remain correlation only;
9. omission diagnostics do not bypass visibility controls;
10. retrieval implementation details are unnecessary for verification;
11. Context Exposure volume does not affect policy-critical session folds;
12. valid evidence or exposure is never represented as proof of attention, understanding, or causation.

---

## Final invariant

> **A Context Pack identifies Git-grounded repository evidence under one exact tree. ContextRender commits to the exact ordered semantic segments that crossed the harness context boundary using one fixed V1 binary framing. A signed Context Exposure retains the pack and view through real Git reachability and binds that exposure to a logical invocation without turning retrieval metadata, runtime correlation, or context relevance into authority.**
