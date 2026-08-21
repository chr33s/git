# Git-Native Context Packs

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-21  
**Spec revision:** draft-6

## 1. Summary

This specification defines a small Git-native audit primitive for repository context used by coding agents.

The problem is not to standardize how an agent retrieves or ranks code. The problem is to make it possible to answer, after an agent operation:

> **What repository state and repository evidence were available to the agent at that point?**

V1 defines three concepts:

1. **Repository View** — the exact repository snapshot from which context was drawn.
2. **Context Pack** — an immutable manifest of repository evidence associated with one model invocation or agent operation.
3. **Context Exposure Event** — a signed session record binding a Context Pack, and the exact ContextRender bytes that crossed the harness context-to-invocation boundary, to a point in agent history.

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
  "Can we audit what context was actually supplied?"
```

V1 standardizes **context provenance**, not retrieval quality.

A Context Pack does not prove that a model read, understood, remembered, or used any item. A Context Exposure Event does not prove causation. Together with signed session history, they provide a tamper-evident record of the repository context that the harness claims crossed its context-to-invocation audit boundary for a model invocation.

---

## 2. Problem

Coding agents can fail because repository context is missing, stale, truncated, or hallucinated.

Typical failures include:

- an agent edits code without seeing a relevant implementation;
- an agent reasons from a stale version of a file;
- an agent never sees the tests or configuration that constrain a change;
- a context window drops repository evidence that was available earlier;
- a retrieval implementation silently changes behavior;
- an agent claims a repository fact that was never present in its supplied context.

Today these failures are difficult to investigate because repository retrieval is often transient and weakly connected to the agent operation that follows.

For auditability, the useful questions are:

```text
Which repository snapshot was active?
Which exact blobs or byte ranges were selected?
Which exact repository-context bytes crossed the harness context-to-invocation boundary?
Which model invocation and agent operation followed that exposure?
```

The selector's internal score, graph traversal, embedding model, parser implementation, or token estimator is secondary evidence. It is not required to answer those questions.

---

## 3. Goals and non-goals

### 3.1 Goals

V1 MUST make it possible to:

1. identify the exact repository snapshot used for context;
2. resolve selected source evidence to immutable Git bytes;
3. record the exact ContextRender bytes handed to the model-invocation subsystem for a model invocation;
4. bind that exposure to signed session history;
5. audit stale, missing, or contradictory repository context after an agent operation;
6. validate a pack without reproducing the retrieval implementation that created it;
7. record lightweight selector identity and coarse omission diagnostics without turning them into selection-consensus inputs;
8. preserve verifiable repository provenance for context that a producer claims was authoritative instruction.

### 3.2 Non-goals

V1 does **not** standardize:

- a deterministic selector;
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
- deterministic tie-breaking;
- exhaustive omission ranking or candidate logs;
- byte-identical manifests produced independently by different implementations.

Implementations MAY use any of those techniques internally.

They are not required to validate Context Pack provenance.

### 3.3 Selection is not cognition

A Context Pack records repository evidence associated with an invocation. A Context Exposure Event records a harness claim that a particular ContextRender crossed the harness context-to-invocation audit boundary.

Neither proves that the model:

- attended to an item;
- understood it;
- retained it across turns;
- based an action on it;
- had sufficient context to act correctly.

This specification uses **exposed** and **available**, not **read**, **understood**, or **used**.

---

## 4. Core principles

### 4.1 Retrieval is replaceable

The protocol MUST NOT depend on one retrieval architecture.

A producer MAY select repository context using:

- explicit file reads;
- grep or lexical search;
- Tree-sitter;
- `tree-sitter-graph`;
- compiler APIs;
- LSP;
- SCIP;
- embeddings;
- an LLM reranker;
- an agent recursively opening files;
- repository-specific indexes;
- any combination of the above.

The durable boundary begins when that machinery produces references to repository evidence.

> **Retrieval may be probabilistic. Evidence identity must not be.**

### 4.2 Evidence is Git-grounded

Repository evidence is identified by Git object identity and, when needed, a byte range.

For source evidence, the durable identity is:

```text
blob OID + optional byte range
```

Paths, symbol names, ranking scores, and parser node handles are not immutable evidence identity.

### 4.3 The effective repository tree is authoritative

A pack MUST name the exact tree against which its evidence is interpreted.

This includes dirty-worktree operation. A producer MUST NOT record `HEAD` as the effective view while supplying different worktree bytes to the model.

### 4.4 Rendered exposure matters

A manifest of selected evidence is not sufficient to audit what reached the model.

Rendering may:

- truncate evidence;
- add line numbers;
- omit ranges;
- insert summaries;
- reorder items;
- apply a context-window limit;
- accidentally render stale bytes.

Therefore every auditable model invocation MUST construct a **ContextRender** artifact containing the exact repository-context bytes handed from the context subsystem to the model-invocation subsystem, and MUST record a digest of that artifact. Provider-specific request serialization is outside this digest boundary (§7.2).

### 4.5 Session history provides ordering

Context exposure is historical provenance. It belongs in the signed session/event history.

A Context Exposure Event MUST occur before the model invocation or agent operation whose repository context it describes. The following operation MUST reference that exposure event, or the surrounding session protocol MUST otherwise bind the operation to it unambiguously.

The exposure event MUST NOT contain a self-reference to its own event identifier.

### 4.6 Context does not grant instruction authority

A Context Pack describes exposure, not authority.

Selecting a file, comment, documentation fragment, memory entry, or generated summary MUST NOT cause it to become an instruction merely because it appears in context.

Instruction authority is enforced by the harness or session policy outside this specification.

When a producer claims that a repository item was supplied with instruction authority, it SHOULD record verifiable instruction provenance using `authority` (§6.8). That annotation proves where the claimed instruction came from in the recorded repository view; it does **not** itself grant authority.

### 4.7 Derived indexes are disposable

Parsers, syntax trees, graphs, embeddings, symbol indexes, and summaries are accelerators.

Deleting them MUST NOT corrupt a persisted Context Pack or prevent verification of its Git-grounded evidence.

---

## 5. Repository View

A Repository View names the exact source snapshot from which repository evidence may be selected.

```json
{
  "base": "sha256:abc123...",
  "tree": "sha256:def456..."
}
```

### 5.1 `base`

`base` is the commit OID that anchors committed history for the operation.

It is useful for:

- human inspection;
- ancestry checks;
- linking the operation to normal Git history;
- explaining whether context came from committed or edited state.

### 5.2 `tree`

`tree` is the root tree OID of the **effective repository snapshot** visible to context retrieval.

For a clean worktree:

```text
view.tree == root tree of view.base
```

For a dirty worktree, the implementation MUST construct an overlay tree containing the exact repository bytes that retrieval is allowed to inspect.

The selector MUST select repository evidence from `view.tree`, not directly from mutable filesystem state after the view has been captured.

This makes the tree, rather than the host worktree, the audit boundary.

### 5.3 Dirty worktrees

To capture a dirty worktree, an implementation MAY:

1. hash modified or newly included files as Git blobs;
2. apply additions, modifications, and deletions over the base tree;
3. write an overlay tree;
4. record that tree as `view.tree`.

The exact construction mechanism is implementation-defined.

The resulting tree MUST represent every repository path from which the retrieval implementation is allowed to select evidence for that invocation.

A dirty view intended for durable audit MUST keep the overlay tree and referenced blobs reachable for as long as the corresponding session provenance is retained.

### 5.4 Object identifiers

Git object identifiers are encoded as:

```text
<algorithm>:<lowercase-hex>
```

where `algorithm` matches the repository object format, currently `sha1` or `sha256`.

Consumers MUST treat Git OIDs as opaque strings and MUST NOT assume SHA-1 length.

V1 does not define a separate repository-family identity. `view.base` and `view.tree` identify the repository state required for this audit primitive.

### 5.5 Paths, symlinks, submodules, and unavailable content

Evidence resolution follows the recorded tree.

- **Paths** are required tree locators for repository evidence items. Each item path MUST resolve under `view.tree` to the recorded blob. Blob identity remains authoritative for the bytes, while the path proves membership in the claimed repository view.
- **Symlinks** are evidence as link blobs and MUST NOT be silently followed when resolving an item.
- **Submodules** are gitlinks. V1 MAY record the gitlink but does not require traversal into the submodule.
- **Missing partial-clone/LFS content** MUST be reported as unavailable rather than silently replaced with different bytes.

---

## 6. Context Pack

A Context Pack is an immutable manifest of repository evidence associated with one auditable context exposure.

Minimal example:

```json
{
  "version": 1,
  "view": {
    "base": "sha256:abc123...",
    "tree": "sha256:def456..."
  },
  "items": [
    {
      "path": "src/auth.ts",
      "blob": "sha256:111aaa...",
      "range": [1200, 1840],
      "role": "implementation",
      "reason": "reference"
    },
    {
      "path": "tests/auth.test.ts",
      "blob": "sha256:222bbb...",
      "range": [400, 920],
      "role": "test",
      "reason": "test"
    }
  ]
}
```

Only `version`, `view`, `items[].path`, `items[].blob`, and sufficient information to resolve an item's bytes are protocol-critical.

`role`, `reason`, symbols, selector metadata, and other annotations are descriptive metadata.

### 6.1 Item identity

A repository evidence item contains:

```json
{
  "path": "src/auth.ts",
  "blob": "sha256:...",
  "range": [1200, 1840]
}
```

`blob` is required.

`range` is optional. When absent, the item refers to the whole blob.

`path` is required. It is the verifiable tree locator that binds the item to `view.tree`. It does not define the immutable byte identity of the evidence; `blob` does that.

A conforming repository evidence item MUST therefore satisfy both:

```text
view.tree + path -> blob
blob + optional range -> exact evidence bytes
```

V1 does not permit a floating evidence blob that exists in the object database but cannot be resolved from the recorded `view.tree`. Future item types MAY define a different verifiable locator, but such types require an extension to this specification.

### 6.2 Byte ranges

Ranges are half-open byte offsets:

```text
[start, end)
```

into the exact blob bytes before line-ending or encoding transformation.

Rules:

1. `0 ≤ start < end ≤ blobSize`;
2. whole-blob items SHOULD omit `range`;
3. a UTF-8 text renderer MUST NOT slice through a codepoint boundary;
4. a consumer MUST verify that the blob exists and that the range is valid before rendering it as verified evidence.

### 6.3 Resolution against the view

For each item, a verifier MUST confirm that:

1. `view.tree` exists;
2. `path` resolves under `view.tree` to the recorded `blob`; and
3. `range`, when present, is valid for that blob.

If the path is absent, does not resolve, or resolves to a different object, the item is not verified repository evidence for that view. A path mismatch does not change the blob's bytes, but it is an audit failure because the manifest claims that evidence was selected from that repository view at that path.

### 6.4 Selector identity

A producer MAY record the selector implementation that produced the pack:

```json
{
  "selector": {
    "name": "repo-context",
    "version": "2.3.1"
  }
}
```

`selector.name` and `selector.version` are diagnostic metadata.

They are useful for answering questions such as:

```text
Did the missing context start after a selector release?
Did two incidents use different retrieval implementations?
Which implementation should be evaluated against this historical failure?
```

They MUST NOT be treated as a selector configuration digest, a reproducibility claim, or an input required to validate pack evidence.

A verifier MUST NOT reject a pack because selector identity is absent or unknown.

### 6.5 Descriptive item metadata

A producer MAY include item metadata such as:

```json
{
  "role": "implementation",
  "reason": "reference",
  "symbol": "Policy.checkBranchPolicy"
}
```

Such metadata exists for debugging and explanation.

A consumer MUST NOT require it to validate the underlying evidence.

No ranking score, fixed-point representation, graph digest, token estimate, or selector configuration digest is required by V1.

### 6.6 Reasons

Reasons are optional and descriptive.

Recommended core reason labels are:

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

An implementation MAY use additional namespaced labels.

A reason answers:

> "Why did this retrieval implementation include the item?"

It does not prove that an independent selector would make the same choice.

### 6.7 Omission diagnostics

A producer MAY include a top-level `omissions` array containing coarse diagnostics for evidence that the retrieval pipeline discovered or attempted to include but that was not present in the final exposure.

Example:

```json
{
  "omissions": [
    {
      "path": "tests/auth.test.ts",
      "reason": "budget"
    },
    {
      "path": "config/private.json",
      "reason": "filtered"
    }
  ]
}
```

Recommended core omission reasons are:

```text
budget
unavailable
filtered
error
other
```

An implementation MAY use additional namespaced reasons.

Omission diagnostics are deliberately **non-exhaustive and non-ranked**:

- they MUST NOT contain or imply a canonical selector score;
- their order MUST NOT imply candidate rank;
- absence from `omissions` MUST NOT be interpreted as proof that an item was never considered or did not exist;
- an omission record is a producer diagnostic claim, not independently reproducible selection evidence.

When an omitted item has a known Git blob OID, the producer MAY record `blob` in addition to `path`. A verifier MAY validate that blob against `view.tree` when possible.

The purpose is to distinguish useful operational failure classes such as:

```text
retrieval surfaced the evidence but the context budget dropped it
access or availability prevented inclusion
host policy filtered it
retrieval failed while handling it
```

without restoring deterministic ranking machinery.

If evidence is absent from both `items` and `omissions`, the protocol deliberately does not distinguish "never discovered" from "not recorded as an omission".

### 6.8 Verifiable instruction provenance

A producer MAY annotate an item with the repository source from which it claims instruction authority was derived:

```json
{
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
2. `authority.path` MUST resolve under that tree to the recorded item `blob`;
3. `authority.source` is descriptive and identifies the producer's authority class, such as `repository-instructions` or `repository-policy`;
4. a verifier SHOULD surface an invalid authority annotation as an **unverified instruction claim** while continuing to treat the item itself as ordinary Git-grounded context evidence.

If a producer uses `role: "instruction"` to claim that an item was supplied with instruction authority, it SHOULD include `authority`.

A valid `authority` annotation proves only the repository provenance of the claimed instruction source. Whether that source actually has authority for the invocation remains a decision of the harness or session policy (§4.6).

### 6.9 Encoding and identity

A Context Pack is ordinary UTF-8 JSON.

V1 does not require RFC 8785/JCS or byte-identical serialization across independent producers.

When persisted as a Git blob, the pack's identity is simply the Git blob OID of the **actual bytes persisted**.

Two semantically equivalent JSON serializations MAY therefore have different blob OIDs. That is acceptable: the audit question is which record existed, not whether another producer could independently recreate its bytes.

A consumer MUST NOT claim that reserialized bytes have the original pack OID.

---

## 7. Context Exposure Event

A Context Exposure Event binds a Context Pack to the exact ContextRender artifact handed from the context subsystem to the model-invocation subsystem for one invocation.

Example event payload:

```json
{
  "type": "context-exposure",
  "pack": "sha256:8d7ad4...",
  "renderDigest": "sha256:6e91f2..."
}
```

### 7.1 Meaning

The event means:

> The harness claims that the Context Pack identified by `pack` describes repository evidence for this invocation, and that the final ContextRender bytes handed from the context subsystem to the model-invocation subsystem hashed to `renderDigest`.

The event derives trust from the signed session event that contains it.

It does not carry a separate signature.

### 7.2 ContextRender and render digest

A **ContextRender** is the single ordered byte string produced by the context renderer and handed to the model-invocation subsystem as repository-derived context for one invocation.

`renderDigest` is:

```text
sha256(<ContextRender bytes>)
```

The ContextRender bytes are hashed exactly as handed across that boundary, including ordering, labels, line numbers, truncation markers, summaries, separators, and text encoding.

The digest boundary is deliberately **before provider-specific request serialization**. JSON envelopes, HTTP framing, SDK serialization, message IDs, transport compression, provider-added fields, and other adapter details are not part of ContextRender. This avoids making provider wire formats part of the protocol.

A provider adapter MAY wrap the ContextRender in messages or structured request fields, but it MUST NOT silently alter the repository-derived bytes after `renderDigest` is computed. All repository-derived content intentionally supplied through the adapter MUST be represented in ContextRender. The adapter MUST NOT inject additional repository-derived content outside that artifact. If the adapter must transform, truncate, reorder, summarize, or otherwise change repository context, the transformed bytes constitute a new ContextRender and the harness MUST compute the digest over those final bytes instead.

A ContextRender MAY represent repository context that is distributed across multiple provider message or content fields. In that case the harness MUST first construct one deterministic ordered ContextRender byte artifact for the repository-derived portions, then map those bytes into provider fields without changing their content. The provider envelope itself remains outside the digest boundary.

This digest is intentionally over the **rendered repository-context artifact**, not the pack JSON and not the complete provider API request.

### 7.3 Render storage and redaction

The exact ContextRender bytes SHOULD be retained by the session system when policy permits, because they are the strongest evidence of what repository-derived context crossed the harness audit boundary for the invocation.

Rendered context may contain source or other sensitive text, so it MUST follow the session's normal retention, secret-handling, access-control, and redaction lifecycle rather than being embedded permanently in the Context Pack.

If rendered bytes remain available, a verifier MUST be able to recompute `renderDigest`.

If rendered bytes have been redacted or expired, `renderDigest` remains a commitment to the prior exposure, but the exact rendering can no longer be independently inspected.

### 7.4 Binding to the agent operation

A Context Exposure Event MUST be bound unambiguously to the model invocation it describes.

The preferred history shape is:

```text
signed context-exposure event
        ↓
model invocation event
        ↓
agent/tool operation event(s)
```

The subsequent model invocation SHOULD reference the exposure event explicitly. If the session protocol already provides an unambiguous parent/sequence relationship, that relationship MAY provide the binding.

The exposure event MUST NOT include its own event identifier. This avoids circular content addressing.

### 7.5 One exposure per auditable invocation

For strongest auditability, each model invocation capable of producing repository-affecting agent actions SHOULD have one Context Exposure Event describing the repository context for that invocation.

If repository context changes between model calls, a new exposure event MUST be recorded.

This includes context acquired through interactive tool use when that tool output becomes part of a later model invocation.

The Context Pack for a later invocation SHOULD describe the repository evidence present in that later invocation, rather than relying on an auditor to reconstruct context-window retention from old turns.

---

## 8. Retrieval and rendering

### 8.1 Retrieval is outside the protocol

A conforming producer may use any method to construct `items`.

There is no canonical V1 selector.

Therefore these are implementation concerns, not protocol invariants:

```text
ranking
scoring
fixed-point arithmetic
tie-breaking
CodeGraph construction
lexical-vs-semantic weighting
embedding models
token estimation
candidate limits
history windows
resource counters
reranking
```

A producer MAY record those details for diagnostics.

A verifier MUST NOT require them to validate the pack's repository evidence.

### 8.2 Resource safety remains an implementation requirement

Removing deterministic resource limits from the protocol does not remove the need to bound attacker-controlled work.

Retrieval implementations MUST still defend against unbounded:

- source parsing;
- graph expansion;
- history scans;
- candidate generation;
- rendered context size;
- memory use;
- wall-clock execution.

The exact limits are host policy and need not be serialized into every Context Pack.

### 8.3 Rendering

A renderer transforms a pack and related session context into a ContextRender byte artifact.

Rendering is allowed to be implementation-specific.

The audit invariant is not that another renderer can reproduce the same bytes from the pack. The invariant is that the producer records `renderDigest` over the final ContextRender bytes handed to the model-invocation subsystem, before provider-specific envelope serialization.

The provider adapter MUST preserve those repository-derived bytes. If it changes them, the changed artifact becomes the ContextRender and MUST be hashed instead (§7.2).

This distinction allows renderers and provider adapters to evolve without changing Context Pack validity or making provider wire formats protocol-critical.

---

## 9. Storage and reachability

### 9.1 Persistent exposure

A persisted Context Pack intended to support durable audit MUST be reachable from the signed session history that references it.

One valid layout is:

```text
session event commit
└── tree
    ├── event.json
    ├── event.sig
    └── context.json
```

`context.json` is the exact Context Pack blob referenced by the event.

The event payload MAY duplicate its blob OID for validation.

### 9.2 Repository evidence

Evidence blobs inside the pack remain normal Git references.

A provenance replica MAY replicate the session record without automatically replicating the entire repository history, but an auditor cannot fully resolve evidence until the referenced Git objects are available.

### 9.3 Dirty-view reachability

If `view.tree` is an overlay tree that is not reachable from ordinary source history, durable audit requires preserving that tree and every blob needed to resolve its selected evidence.

An implementation MUST NOT persist an audit record that claims an exact dirty view while allowing the only copy of that view to be immediately garbage-collected.

### 9.4 Generation may remain read-only

A context-selection command MAY compute a transient pack without writing durable Git objects.

Durable objects are required only when the pack is attached to a session exposure that the system intends to preserve.

---

## 10. Security and trust

### 10.1 Prompt injection

Context selection does not create instruction authority.

A comment, README, generated summary, retrieved memory, or source string does not become an instruction because it appears in a Context Pack.

The harness MUST apply its normal trusted-instruction policy independently of pack relevance.

### 10.2 Retrieval poisoning

A malicious repository may manipulate lexical search, graph structure, comments, filenames, or generated files to influence retrieval.

V1 does not attempt to prove that retrieval was good.

Implementations SHOULD mitigate poisoning through host policy, tests, retrieval heuristics, path policy, and evaluation. Those mechanisms may evolve without changing the audit format.

### 10.3 Extractor and parser safety

Repository-controlled source can trigger expensive parser or index behavior.

Extractors and analyzers MUST be bounded and sandboxed according to host security policy.

A Context Pack does not make derived analyzer output trusted repository truth.

### 10.4 Sensitive rendered context

The Context Pack SHOULD prefer references to repository evidence over duplicated bodies.

The exact ContextRender belongs to the session's sensitive-content lifecycle because it may duplicate source, secrets, prompts, or derived text.

`renderDigest` MAY remain after render-body redaction because the digest is an integrity commitment, not sufficient by itself to recover the rendered content.

### 10.5 Visibility boundaries

A producer MUST NOT place evidence into a Context Pack that the corresponding model invocation was not authorized to receive.

Auditing context exposure does not bypass repository, path, tenant, or secret access controls.

---

## 11. Product surface

A minimal V1 product surface is:

```text
git+ context for --task "..."
git+ context why <pack> [item]
git+ context audit <operation-or-session-event>
```

### 11.1 `context for`

Generates a Context Pack using the implementation's current retrieval strategy.

It MAY display ranking, scores, graph paths, or token estimates as diagnostics, but none are required fields in the persisted pack.

It SHOULD record selector `name` and `version` when known. It MAY record coarse `omissions` when doing so helps distinguish retrieval, budget, availability, filtering, or retrieval-error failures.

The command answers:

> **What repository evidence would this implementation supply?**

### 11.2 `context why`

Explains a pack using recorded evidence and optional descriptive metadata.

Example:

```text
src/auth.ts bytes 1200-1840
blob: sha256:111aaa...
view: sha256:def456...

Reason recorded by selector:
  reference
```

If the selector identity or omission diagnostics are present, `why` SHOULD display them when relevant to the selected item or investigation.

If an item carries `authority`, `why` SHOULD report whether its repository provenance verifies against `view.tree`.

If the selector stored richer diagnostics elsewhere, `why` MAY display them.

It MUST distinguish recorded facts from recomputed or inferred explanations.

### 11.3 `context audit`

Audits the repository context bound to an agent operation or model invocation.

It SHOULD report:

```text
repository base commit
repository effective tree
Context Pack OID
render digest
whether ContextRender bytes are still available
whether each pack item resolves under the recorded tree
selector name/version, when recorded
coarse omission diagnostics, when recorded
whether any instruction-authority annotations verify against the recorded tree
whether the following operation is correctly bound to the exposure
```

When ContextRender bytes are available, it SHOULD additionally make it easy to inspect or diff the exact repository context that crossed the harness context-to-invocation boundary.

This is the primary V1 audit command.

### 11.4 `context refresh`

A selector MAY offer a `context refresh` convenience that generates a new pack for a newer view and compares it with an old pack.

Refresh behavior is not normative in V1.

Implementations may use symbol rematching, diffs, graphs, embeddings, or full reselection. A refresh result MUST NOT be presented as proof that the old selector's ranking was reproducible.

---

## 12. Audit examples

### 12.1 Missing context

An agent changes authentication behavior incorrectly.

Audit finds:

```text
view.base: commit A
view.tree: tree A

Context Pack:
  src/login.ts
  src/session.ts

Not exposed:
  src/policy.ts
  tests/policy.test.ts
```

The audit can state that the policy implementation and test were absent from the recorded repository context for that invocation.

It cannot prove that their absence caused the incorrect change.

### 12.2 Stale context

An agent claims `validate()` returns `boolean`, but the active repository returns `ValidationResult`.

Audit finds:

```text
operation view.tree: tree NEW
pack item blob: blob OLD
path resolution under tree NEW: mismatch
```

The exposure record demonstrates that stale repository evidence was supplied or that the pack was constructed incorrectly.

### 12.3 Rendering bug

A pack contains the correct range, but the renderer truncates the final branch of a function.

Audit finds:

```text
pack item: bytes 1200-1840
recorded render: contains bytes 1200-1660 only
renderDigest: valid
```

The retrieval manifest was correct; the exposure bug occurred during rendering.

### 12.4 Hallucinated repository fact

An agent claims a configuration flag exists.

Audit finds no item or rendered repository context containing that flag for the relevant invocation.

The audit can report:

> The recorded repository context does not support this claim.

It cannot distinguish hallucination from knowledge acquired through some unrecorded external channel unless the rest of the session/tool history is also audited.

---

## 13. Acceptance criteria

V1 is successful when:

1. every auditable model invocation can be associated with one exact Repository View;
2. every Context Pack item has a required path that resolves under `view.tree` to its exact Git blob, with an optional byte range identifying the exposed subset;
3. dirty-worktree context is represented by an exact effective tree rather than mislabeled as `HEAD`;
4. every repository-affecting model invocation records a Context Exposure Event before the operation it informs;
5. the exposure event records a digest of the exact ContextRender bytes handed from the context subsystem to the model-invocation subsystem;
6. when ContextRender bytes are retained, recomputing their digest matches `renderDigest`;
7. the pack and any required dirty-view objects survive normal Git reachability and GC rules for the intended audit-retention period;
8. a verifier can validate pack evidence without Tree-sitter, a CodeGraph, embeddings, selector configuration, ranking weights, or fixed-point math;
9. replacing the retrieval implementation does not change the meaning of an already-persisted pack;
10. replacing the renderer or provider adapter does not invalidate an old exposure because the old event commits to the ContextRender bytes that crossed the audit boundary at that time;
11. an auditor can distinguish at least these failure classes:
    - missing repository context;
    - stale repository context;
    - invalid blob/path/range provenance;
    - rendering mismatch or truncation;
    - missing exposure-to-operation binding;
12. selecting content as context does not grant it instruction authority;
13. deleting derived indexes does not corrupt persisted audit provenance;
14. selector name/version, when present, remains diagnostic metadata and does not imply reproducibility;
15. omission diagnostics, when present, can distinguish coarse causes such as budget, filtering, unavailability, and retrieval error without implying candidate rank;
16. an instruction-authority annotation can be verified against `view.tree`, and a failed annotation does not invalidate the underlying evidence item;
17. a Context Pack never claims that its selector is reproducible unless an implementation-specific extension explicitly provides that stronger guarantee;
18. no V1 validation rule depends on deterministic ranking, graph hashing, token estimation, or cross-platform numeric behavior.

---

# Appendix A — Optional retrieval diagnostics

V1 permits lightweight selector identity and coarse omission diagnostics in the pack. Implementations may keep richer diagnostics outside the core protocol.

Examples include:

```text
candidate scores
rank position
semantic search similarity
graph paths
Tree-sitter captures
symbol resolution quality
token cost
exhaustive candidate and omission logs
retrieval latency
resource counters
reranker identity
```

These are useful for improving retrieval quality and explaining implementation behavior.

They are deliberately not required to establish what repository evidence was exposed.

---

# Appendix B — Optional selector-specific reproducibility

A selector implementation MAY define a stronger reproducibility profile outside V1.

Such a profile may pin:

```text
extractor versions
query packs
CodeGraph representation
graph digest
ranking formula
fixed-point arithmetic
resource counters
token estimator
total ordering
selector configuration
```

If implemented, that profile answers a different question:

> **Could another conforming selector recreate this same evidence selection?**

The core Context Pack answers only:

> **What evidence was associated with this invocation, and can its repository provenance be verified?**

The two guarantees SHOULD remain separate so that retrieval experimentation does not require changing the audit protocol.

---

# Appendix C — Evaluation

Evaluate the system on historical agent failures and handoff tasks.

Auditability metrics SHOULD include:

```text
percentage of repository-affecting invocations with valid exposure records
percentage of pack items that resolve under the recorded tree
stale-context detection rate
render-mismatch detection rate
missing-context investigation time
manifest size
exposure-record overhead
```

Retrieval-quality metrics MAY separately include:

```text
required-file recall
required-symbol recall
test recall
configuration recall
irrelevant-context ratio
generation latency
```

The two groups SHOULD be reported separately. A retrieval system may have poor recall while still producing excellent provenance, or excellent recall while producing weak audit records.

A useful handoff benchmark remains:

```text
fresh agent + no recorded context
fresh agent + prose summary
fresh agent + Context Pack
fresh agent + Context Pack + exact exposure record
```

---

## Final invariant

> **A Context Pack is an immutable manifest of Git-grounded repository evidence whose required paths resolve under an exact repository tree. A Context Exposure Event commits to the exact ContextRender bytes that crossed the harness context-to-invocation audit boundary and is bound to that invocation through signed session history. Provider-specific request serialization is outside that digest boundary and MUST NOT introduce unrecorded repository-derived context. Selector identity, coarse omissions, and instruction provenance may improve diagnosis without making retrieval reproducible or granting instruction authority. Retrieval and ranking remain replaceable implementation details. Neither object proves cognition or causation.**
