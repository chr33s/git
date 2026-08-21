# Git-Native Context Packs

**Status:** Draft specification
**Project:** `@chr33s/git`
**Target version:** Experimental / pre-1.0
**Last updated:** 2026-08-21
**Spec revision:** draft-2

## 1. Summary

This specification introduces three small Git-native concepts:

1. **Repository View** — the exact repository state a selector reads.
2. **Context Pack** — an immutable, explainable selection of repository evidence for a task.
3. **Context Receipt** — a session record claiming that a harness exposed a particular Context Pack.

The model is:

```
Repository View
      ↓
   Selector
      ↓
 Context Pack
      ↓
   Harness
      ↓
Context Receipt
      ↓
Session → Commit
```

The distinction matters:

```
Context Pack
  "What repository evidence was selected?"

Context Receipt
  "What selected context does the harness claim it exposed?"
```

Neither object proves what a model read, understood, or used.

The goal is not another semantic-search platform. The goal is to make task-specific repository understanding **portable, inspectable, versioned, explainable, and linked to Git-native provenance**.

> **The repository owns its understanding of itself.**

---

## 2. Problem

Coding agents repeatedly rediscover the same repository context:

- which implementation matters;
- which tests constrain it;
- which configuration changes its behavior;
- which standing instructions apply;
- which repository learnings are still relevant.

Existing retrieval systems commonly make this context transient, provider-owned, difficult to reproduce, opaque about why items were selected, and weakly connected to the commit eventually produced.

`@chr33s/git` already stores source, sessions, decisions, memory, identity, and collaboration state as Git-native data. Context Packs extend that model with a durable answer to:

> **What should an agent know about this repository for this task, and why?**

V1 deliberately does **not** try to build a universal code-intelligence platform. It defines the smallest useful Git primitive plus a language-agnostic graph contract. Parsing and semantic enrichment remain replaceable derived machinery.

---

## 3. Core principles

### 3.1 Selection is not cognition

A Context Pack records selected evidence. A Context Receipt records a harness claim about exposure. Neither proves that the model read every item, understood it, had sufficient context, or was caused by it.

This is stated once, normatively, here. It is the reason every downstream object is described as a *claim* rather than a proof.

### 3.2 Exact inputs

A deterministic selector is reproducible only when **every** input that affects selection is pinned.

V1 pins, in the Repository View (§4):

- source commit (or overlay tree);
- standing instructions;
- repository policy;
- repository Memory projection.

V1 pins, in the selector descriptor (§5.7):

- selector name and version;
- task input (§5.1);
- ranking parameters and budget;
- token estimator identity and version;
- every extractor, grammar, query pack, graph rule set, and semantic enricher version that can affect output;
- **all resource limits** (§7.9).

Resource limits are pinned inputs, not host configuration. A host that wishes to run tighter limits produces a *different* selector configuration and therefore a different pack; it does not silently produce a different pack under the same configuration digest.

> **If an input affects deterministic selection, pin it.**

### 3.3 Evidence over prose

Context Packs SHOULD primarily reference exact repository evidence: blobs, byte ranges, symbols, tests, configuration, and pinned policy and instructions. Generated summaries are optional derived conveniences, never repository truth.

### 3.4 Explainability

Every selected item MUST state why it was selected, in machine-readable form (§5.4).

### 3.5 Derived indexes are disposable

Syntax trees, normalized code graphs, compiler indexes, embeddings, and summaries MAY accelerate or enrich selection. Deleting them MUST NOT corrupt canonical repository state.

### 3.6 Language independence

The Context Pack schema and selector MUST NOT depend on TypeScript, Tree-sitter, `tree-sitter-graph`, an LSP, SCIP, or any one parser ecosystem.

```
language source
     ↓
language extractor
     ↓
normalized CodeGraph
     ↓
language-agnostic selector
     ↓
Context Pack
```

Tree-sitter SHOULD be the default syntactic extraction substrate for languages with suitable grammars. `tree-sitter-graph` MAY implement richer declarative extraction. Neither is the protocol boundary.

### 3.7 Selection never creates instruction authority

Retrieved content does not become instruction merely because it was selected. V1 context items use four kinds:

```
instruction
evidence
narrative
derived
```

`instruction` is valid only when it is *verifiable* against pinned authority (§5.3.1). A consumer that cannot verify the authority chain MUST downgrade the item to `narrative`.

### 3.8 Persistent means Git-reachable

An OID written inside JSON is only text. It does not create Git reachability. A Context Pack claimed to replicate and survive Git GC MUST be structurally reachable from a Git ref through commits/trees (§8).

### 3.9 Bounded work

Context generation MUST bound attacker-controlled work — candidate count, extractor output, graph traversal, history scanned, manifest size, and rendered context size — using deterministic counters (§7.9).

---

## 4. Repository View

The Repository View names the complete deterministic repository state used by selection.

```json
{
  "base": "sha256:abc123...",
  "overlay": null,
  "instructions": "sha256:def456...",
  "policy": "sha256:789abc...",
  "memory": "sha256:456def..."
}
```

`base` is the exact source commit being understood. All paths and blob relationships are interpreted against it.

`instructions`, `policy`, and `memory` pin the exact standing instructions, repository policy, and bounded Repository Memory projection used for selection.

### 4.1 Object identifiers

Every object identifier in every v1 object is an **algorithm-prefixed OID string**:

```
<algorithm>:<lowercase-hex>
```

`algorithm` is `sha1` or `sha256` and MUST match the object-format of the repository the OID belongs to. Implementations MUST NOT assume SHA-1. Digests that are not Git objects (task digests, configuration digests) use the same prefixed form with a non-Git algorithm name, e.g. `sha256:`.

Consumers MUST treat OID strings as opaque and compare them literally, including the prefix.

### 4.2 Dirty worktrees and overlay trees

The most valuable moment for context is usually mid-edit. V1 therefore supports uncommitted state through an **overlay tree** rather than refusing.

When the worktree is dirty, the implementation:

1. hashes modified, added, and staged blobs into the object database;
2. constructs a tree that applies those blobs over `base`;
3. records the resulting tree OID in `view.overlay`;
4. records deletions as part of that tree.

Rules:

- `overlay` MUST be `null` for a clean worktree.
- A pack whose view has a non-null `overlay` is **reproducible only while the overlay tree's objects exist**. It MUST NOT be attached durably (§8) unless the overlay objects are themselves reachable.
- Ignored files, untracked files, and files excluded by the repository's path filters MUST NOT enter the overlay.
- Implementations MUST NOT generate context from `HEAD` while an agent edits different bytes and present it as clean.

`git+ context for` MAY be invoked with `--no-overlay` to require a clean view.

### 4.3 Path handling

The selector MUST define deterministic behavior for:

- **submodules** — gitlink entries are opaque; the selector MAY reference the submodule path and pinned commit, and MUST NOT traverse into the submodule in v1;
- **symlinks** — never followed; the link blob itself is the evidence;
- **LFS / partial-clone pointers** — if the real content is unavailable, the pointer is evidence and the item MUST carry a `content-unavailable` omission (§5.5) rather than a silent miss;
- **binary blobs** — detected via `.gitattributes` and content sniffing; never range-selected, only referenced whole, and only when explicitly anchored;
- **generated and vendored paths** — matched by `.gitattributes` (`linguist-generated`, `linguist-vendored`) or repository policy, and deprioritized rather than excluded, with the deprioritization visible as a reason weight.

`.gitattributes` at the pinned view is an input to selection and is therefore covered by `view.base`.

---

## 5. Context Pack

A Context Pack is the canonical selected-evidence manifest.

```json
{
  "version": 1,
  "repo": "sha256:...",
  "view": {
    "base": "sha256:abc123...",
    "overlay": null,
    "instructions": "sha256:def456...",
    "policy": "sha256:789abc...",
    "memory": "sha256:456def..."
  },
  "task": {
    "digest": "sha256:...",
    "terms": ["provenance", "branch-policy"]
  },
  "selector": {
    "name": "context-v1",
    "version": "1.0.0",
    "config": "sha256:...",
    "configBlob": "sha256:..."
  },
  "budget": {
    "unit": "estimated-tokens",
    "estimator": "chars4-v1",
    "limit": 20000,
    "used": 18422
  },
  "items": [
    {
      "kind": "evidence",
      "path": "src/server/Policy.ts",
      "blob": "sha256:...",
      "range": [4210, 6844],
      "cost": 658,
      "reasons": [
        {
          "kind": "reference",
          "from": "symbol:sha256:...:1204-1899",
          "resolution": "semantic"
        }
      ]
    }
  ],
  "omissions": []
}
```

### 5.1 Task binding

The pack stores a digest of the task rather than duplicating prompt text; the session already owns prompt storage, secret scanning, and redaction.

This has a consequence that MUST be stated plainly:

> A pack that stores only `task.digest` cannot be regenerated from itself. Reproduction requires the original task text from the session. If that session event is redacted (§9.4), the pack becomes verifiable-by-digest but not reproducible.

To keep `context why` and `context refresh` useful after redaction, the pack MUST also store `task.terms`: the normalized, deduplicated, deterministically ordered selection terms actually derived from the task. `task.terms` is derived from prompt text and is therefore subject to the same redaction lifecycle as any other derived prose (§9.4).

`task.digest` is computed over the canonical normalized task input, not raw prompt bytes; the normalization rule is part of the selector configuration.

### 5.2 Item identity

Exact source evidence is identified by:

```
blob OID + byte range
```

Paths and symbol names are presentation metadata, not immutable identity. Ranges are half-open byte offsets `[start, end)` into the blob's exact bytes, before any line-ending or encoding transformation.

### 5.3 Item kinds

```
instruction
  standing instructions or otherwise-verifiable scoped instruction

evidence
  source, tests, configuration, policy, Git-derived facts

narrative
  docs, session notes, Memory, comments, review text

derived
  generated summaries, labels, semantic hints
```

A selector MUST NOT promote `narrative`, `evidence`, or `derived` content to `instruction` because it is relevant, signed, or highly ranked.

#### 5.3.1 Verifying instruction authority

An item with `kind: "instruction"` MUST carry an `authority` object:

```json
{
  "kind": "instruction",
  "blob": "sha256:...",
  "authority": {
    "source": "instructions",
    "root": "sha256:def456...",
    "path": "AGENTS.md"
  }
}
```

`source` is `instructions` or `policy`. `root` MUST equal `view.instructions` or `view.policy` respectively.

A consumer MUST verify that `blob` is reachable from `root` at `path`. If it cannot — because the root is unavailable, the path does not resolve, or the blob does not match — the consumer MUST treat the item as `narrative` and SHOULD surface the downgrade. This is the enforcement point for §3.7 and §9.1; without it, `instruction` is an unverifiable assertion.

### 5.4 Reasons

Every item MUST have a non-empty `reasons` array. Each reason is:

```json
{
  "kind": "reference",
  "from": "<node-id>",
  "target": "client.get",
  "resolution": "syntax",
  "weight": 0.62
}
```

| Field | Required | Meaning |
|---|---|---|
| `kind` | yes | one of the reason kinds below |
| `from` | when the reason is relational | normalized CodeGraph node id (§7.2) of the anchor |
| `target` | when unresolved | textual target that could not be resolved to a node |
| `resolution` | for graph-derived reasons | `syntax` \| `local` \| `semantic` |
| `weight` | no | selector-assigned contribution, deterministic |

Reason kinds:

```
explicit-path      explicit-symbol   task-term
definition         reference         call
import             implementation    test
config             history           memory
policy             instruction       neighbor
```

Reasons are sorted deterministically by `(kind, from, target, resolution)`.

### 5.5 Omissions

The pack SHOULD record high-ranking evidence that was excluded, so a bounded context set does not appear exhaustive.

```json
{
  "reason": "budget",
  "path": "src/trust/Projection.ts",
  "blob": "sha256:...",
  "cost": 1240,
  "rank": 41
}
```

`reason` is one of:

```
budget                 rank survived, budget did not
extractor-limit        §7.9 counter exhausted
extractor-failure      extractor output could not be normalized
content-unavailable    LFS / partial clone / missing object
non-deterministic      wall-clock safety net fired (§7.9)
redacted               derived source is unavailable under redaction
policy                 excluded by repository policy
```

**Disclosure rule.** Omissions name paths. In sparse-checkout, partial-clone, or path-restricted deployments, this can disclose paths the reader may not otherwise see. An implementation MUST filter omissions to the reader's visible path set, and where filtering would itself be informative, MUST degrade to an aggregate:

```json
{ "reason": "policy", "count": 3, "redactedPaths": true }
```

A `non-deterministic` omission makes the pack non-reproducible by definition; consumers MUST treat such a pack as advisory and MUST NOT use it as a determinism fixture.

### 5.6 Canonical encoding

Context Pack bytes are protocol surface because the Git object ID depends on them. This is normative, not deferred:

1. Serialization is **JSON Canonicalization Scheme, RFC 8785 (JCS)**: UTF-8, no insignificant whitespace, object members sorted by UTF-16 code unit, JSON numbers in ECMAScript `Number::toString` form.
2. All numbers MUST be integers within the safe-integer range, except `weight`, which is serialized as a string decimal with exactly 4 fractional digits to avoid float divergence.
3. The canonical bytes are terminated by **exactly one** `\n`. The trailing newline is inside the hashed bytes.
4. Array order is selector-defined but MUST be deterministic; `items` sort by `(path, blob, range[0], kind)` and `omissions` by `(rank, path)`.
5. Optional fields with no value are **omitted**, never emitted as `null` — except `view.overlay`, which is always present and explicitly `null` when clean.

**Extensibility.** `version` is a single integer.

- A consumer encountering a `version` it does not implement MUST reject the pack.
- Within a known `version`, unknown fields MUST be ignored for reading and MUST be preserved byte-for-byte when re-emitting. Because canonical bytes determine identity, a consumer that cannot preserve unknown fields MUST NOT re-emit the pack.
- New required semantics require a `version` bump. New optional fields do not.

### 5.7 Selector descriptor

```json
{
  "name": "context-v1",
  "version": "1.0.0",
  "config": "sha256:...",
  "configBlob": "sha256:..."
}
```

`config` is the digest of the canonicalized selector configuration, which MUST enumerate everything listed in §3.2, including all resource limits.

`configBlob` is the OID of a Git blob containing those exact canonical bytes. It SHOULD be present, and MUST be present for any pack that is durably attached (§8). A bare digest is unverifiable: `context why` and `context refresh` cannot re-run selection against a configuration they cannot read. When both are present, `config` MUST equal the digest of `configBlob`'s contents.

---

## 6. Context Receipt

A Context Receipt is a session claim that a harness exposed a Context Pack.

```json
{
  "pack": "sha256:8d7ad4...",
  "session": "sha256:...",
  "event": "sha256:...",
  "at": "2026-08-21T09:14:22Z",
  "nonce": "sha256:..."
}
```

Its meaning is intentionally narrow:

> The signer claims this Context Pack was exposed during this session, at this point in the session's history.

**Binding is normative.** A receipt carries no signature of its own; it derives all trust from the enclosing signed session event (§8). Therefore:

- a receipt MUST appear inside a signed session event and MUST reference that event's session;
- `event` MUST identify the immediately-enclosing session event, so a receipt cannot be lifted into another session or replayed at another point in the same session;
- a receipt evaluated outside its enclosing signed event carries **no** trust and MUST be treated as an unauthenticated hint.

Without these fields, `{"pack": "..."}` is a replayable fragment rather than a claim.

The Pack is a derived selection artifact. The Receipt is historical session provenance. Neither proves cognition or causation (§3.1).

Future versions MAY add a render digest, render profile, expansions, or model-specific token counts.

---

## 7. Selection and the language-agnostic CodeGraph

V1 selection SHOULD remain deterministic and simple:

```
1. explicit task roots
2. deterministic lexical search
3. normalized CodeGraph neighbors
4. tests and configuration
5. bounded history signal
6. pinned Memory / instructions / policy
7. budget packing
```

Explicit paths, symbols, commands, and identifiers receive highest priority.

### 7.1 Stable boundary: `CodeGraph`

```ts
interface CodeGraph {
  readonly nodes: ReadonlyArray<CodeNode>
  readonly edges: ReadonlyArray<CodeEdge>
}

interface CodeNode {
  readonly id: string
  readonly kind: "file" | "symbol"
  readonly language: string
  readonly path: string
  readonly blob: string
  readonly range?: readonly [startByte: number, endByte: number]
  readonly name?: string
  readonly symbolKind?: string
}

interface CodeEdge {
  readonly kind:
    | "defines"
    | "references"
    | "calls"
    | "imports"
    | "implements"
    | "extends"
    | "tests"
    | "configures"

  readonly from: string
  readonly to?: string
  readonly target?: string
  readonly resolution: "syntax" | "local" | "semantic"
}
```

The exact in-memory types are implementation detail. The normative ideas are: nodes are grounded in Git evidence; relationships use a small language-independent vocabulary; unresolved targets may remain textual; every relationship states its resolution class.

Implementations MAY add namespaced node/edge kinds internally, but the selector SHOULD rely on the core vocabulary for portable behavior.

### 7.2 `git+` owns identity

Extractor-specific node IDs MUST NOT become durable CodeGraph identity. Identity is derived from repository evidence:

```
node-id = <kind>:<blob-oid>:<startByte>-<endByte>
```

File nodes omit the range. An extractor may produce temporary IDs, but they MUST be normalized before selection or persistence.

This prevents Context Pack semantics from depending on Tree-sitter node handles, `tree-sitter-graph` node IDs, compiler object identity, process memory, or parser lifetimes.

### 7.3 Extractor interface

```ts
interface CodeExtractor {
  readonly name: string
  readonly version: string

  extract(input: {
    readonly path: string
    readonly blob: string
    readonly source: Uint8Array
    readonly language: string
    readonly limits: ExtractorLimits
  }): CodeGraph
}
```

A new language SHOULD require a grammar/query pack or extractor, not a new selector or Context Pack schema.

### 7.4 Tree-sitter as the v1 syntactic substrate

For languages with suitable grammars, V1 SHOULD use Tree-sitter to produce syntax-level graph facts, preferring declarative files:

```
queries/tags.scm
queries/locals.scm
queries/context.scm
```

`context.scm` MAY identify additional relationships useful to selection, such as imports, exports, test declarations, routes, or configuration references.

```
Tree-sitter definition capture
        ↓
CodeNode(kind="symbol", symbolKind="function")

Tree-sitter call/reference capture
        ↓
CodeEdge(kind="calls", resolution="syntax")
```

### 7.5 Syntax is not semantic resolution

Tree-sitter is a parser, not a compiler. Syntax may identify `client.get(...)` as a call to a member named `get`, but cannot generally prove which declaration it resolves to. The graph MUST preserve that distinction:

```json
{ "kind": "calls", "from": "symbol:sha256:...:12-40", "target": "client.get", "resolution": "syntax" }
```

A resolver may later enrich it:

```json
{ "kind": "calls", "from": "symbol:sha256:...:12-40", "to": "symbol:sha256:...:88-140", "resolution": "semantic" }
```

The selector MAY prefer stronger resolution, but unresolved syntax facts remain useful and explainable.

### 7.6 `tree-sitter-graph` as an optional extraction engine

`tree-sitter-graph` MAY be used when ordinary queries become awkward for stateful, nested, or cross-stanza graph construction.

```
source → grammar → syntax tree → .tsg rules → extractor graph → normalize + validate → git+ CodeGraph
```

The protocol MUST NOT depend on `tree-sitter-graph` node IDs, in-memory graph lifetime, attribute namespace, edge identity rules, `.tsg` execution order, or host-language bindings. The stable boundary remains `CodeGraph`, so implementations may substitute direct queries, compiler APIs, SCIP, LSP, prebuilt indexes, or custom analyzers without changing Context Pack semantics.

### 7.7 Normalization requirements

Every extractor output MUST be normalized before the selector consumes it. Normalization SHOULD:

1. assign Git-grounded node identity (§7.2);
2. map extractor node kinds to the core vocabulary;
3. map relationship types to core edge kinds;
4. preserve unresolved textual targets;
5. attach `syntax`, `local`, or `semantic` resolution;
6. validate all blob/range references against the pinned view;
7. sort nodes and edges deterministically by node id, then edge `(kind, from, to, target)`.

Output that cannot be normalized safely MUST be rejected or omitted with an `extractor-failure` omission rather than treated as canonical fact.

### 7.8 Optional semantic enrichers

```
   Tree-sitter / TSG
          │
          ▼
   syntactic CodeGraph
          │
 optional enrichers
   ┌──────┼──────┐
   ▼      ▼      ▼
compiler  LSP   SCIP
   └──────┼──────┘
          ▼
    enriched graph
          │
          ▼
   Context selector
```

Enrichers are optional and their versions are pinned (§3.2). No enricher is required to read or validate a canonical Context Pack.

### 7.9 Resource bounds

Language extraction and traversal are attacker-controlled work and MUST be bounded — **deterministically**.

All limits are **deterministic counters**, pinned in the selector configuration:

```
sourceBytesParsed
queryMatches
nodesEmitted
edgesEmitted
normalizedGraphBytes
candidatesConsidered
graphTraversalSteps
historyCommitsScanned
manifestBytes
```

Exceeding a counter MUST produce a bounded partial result with an explicit omission (`extractor-limit`), and MUST NOT create unbounded memory or durable-storage growth. Because the counters are pinned inputs, two hosts running the same configuration produce identical packs.

**Wall-clock is a safety net, not a limit.** An implementation MAY additionally enforce a timeout or cancellation budget for liveness. Because elapsed time is not reproducible, firing it MUST:

1. record a `non-deterministic` omission (§5.5);
2. mark the pack as non-reproducible;
3. exclude the pack from determinism fixtures and acceptance testing.

An implementation MUST NOT rely on wall-clock as its primary bound, and MUST NOT allow a timeout to silently truncate a pack that claims reproducibility.

### 7.10 History and Memory

V1 MAY use bounded Git co-change/history signals. The history horizon is a pinned counter (`historyCommitsScanned`) and a pinned commit boundary, not "recent". The selector SHOULD consult the pinned bounded Memory projection instead of scanning the complete session corpus.

### 7.11 Budget

Selection operates under an explicit budget with a declared unit and estimator:

```json
{ "unit": "estimated-tokens", "estimator": "chars4-v1", "limit": 20000, "used": 18422 }
```

`chars4-v1` is `ceil(codepoints / 4)` over the rendered UTF-8 text. It is a stable, model-independent estimator and a poor one: it understates CJK, Devanagari, and other non-Latin scripts by roughly 2–4×, and understates dense punctuation. Implementations serving such repositories SHOULD register a better estimator; the estimator identity is pinned, so packs remain comparable only within the same estimator.

Item `cost` values are in the declared unit and MUST sum to `budget.used`. Harnesses MAY compute exact model-specific tokens when rendering; that does not change the pack.

---

## 8. Storage and reachability

A persistent Context Pack MUST be a Git-reachable attachment of the session record that references it.

```
session event commit
└── tree
    ├── event.json
    ├── event.sig
    ├── context.json        # canonical Context Pack
    └── selector.json       # canonical selector configuration (§5.7)
```

The event payload MAY also record the pack OID for validation, but the tree edge provides reachability.

Evidence blobs referenced inside `context.json` SHOULD remain logical references rather than attachments, so provenance replication does not implicitly drag source history with it.

### 8.1 Generation is a read operation

`context for` computes canonical bytes and their would-be OID without writing durable objects. A read-only caller MUST NOT gain arbitrary durable object creation merely by asking for context.

Durable writes occur only when a pack is attached through an authorized session event. The attach step MUST re-derive the pack's OID from its bytes and MUST reject a mismatch.

### 8.2 Generation/attachment skew

A pack is generated against view V and attached to a session event that may be created later, against a different `HEAD`. Implementations MUST:

- record the pack's own `view` as the authoritative statement of what was selected — never the attaching commit's state;
- surface skew when `view.base` is not an ancestor of the attaching event's commit;
- reject attachment of an overlay-based pack (§4.2) whose overlay objects are not reachable, since it would be permanently unverifiable.

### 8.3 Unattached pack storage

`context why` and `context refresh` operate on packs that may never be attached. An unattached pack lives in a local, non-replicated content-addressed cache keyed by its OID. Rules:

- the cache is disposable; a missing entry is a "not found", not corruption;
- `context why <pack>` and `context refresh <pack>` accept a cache OID, a file path, or canonical bytes on stdin;
- the cache MUST NOT be reachable from any ref, so it cannot smuggle durable objects past §8.1.

---

## 9. Security and trust

### 9.1 Prompt injection

Retrieved comments, docs, Memory, and other narrative do not gain instruction authority through relevance or signature. Enforcement is §5.3.1: an `instruction` item is only an instruction if its authority chain resolves to pinned instructions or policy.

### 9.2 Retrieval poisoning

Lexical relevance is manipulable. A contributor may add decoy files or symbols matching likely task terms to consume the budget. V1 SHOULD mitigate with:

- explicit-anchor preference;
- graph-connectedness preference;
- traversal and candidate limits (§7.9);
- budget reservations for tests and configuration;
- lower weight for comments than executable symbols;
- deprioritization of generated/vendored paths (§4.3);
- visible omission and reason data.

### 9.3 Extractor poisoning and exhaustion

Parser, query, and TSG inputs are repository-controlled. Extractors MUST obey §7.9. A pathological grammar, query, or rule set MUST NOT turn context generation into unbounded CPU, memory, or graph growth.

### 9.4 Secret duplication and redaction

Context Packs SHOULD reference existing evidence rather than duplicate source or session prose.

Prose derived from a session event — including `task.terms` and any `derived` item — MUST become unavailable under the same redaction lifecycle as its source. A redacted pack:

- retains `task.digest`, `view`, `selector`, item identities, and reasons;
- loses `task.terms` and `derived` item bodies, replaced by `redacted` omissions;
- is no longer reproducible, and MUST be reported as such by `context refresh`.

### 9.5 Derived indexes are untrusted accelerators

Remote or cached structural/semantic indexes MAY be validated, rebuilt, or ignored. A missing index is a performance problem, not repository corruption.

### 9.6 Path visibility

Selection may cross access boundaries in sparse-checkout, partial-clone, or path-restricted deployments. Items, reasons, and omissions MUST be filtered to the reader's visible path set (§5.5), and filtering itself MUST NOT be inferable from counts alone where that would leak.

---

## 10. V1 product surface and acceptance criteria

```
git+ context for --task "..."
git+ context why <pack> <item>
git+ context refresh <pack>
```

They answer:

```
What should I know?
Why is this here?
What changed in what I need to know?
```

### 10.1 `context for`

Generates a deterministic Context Pack from a pinned Repository View, clean or overlay-based (§4.2).

Returns canonical bytes, the would-be OID, and a concise selection summary. Persistence occurs only through authorized session attachment (§8.1).

### 10.2 `context why`

Explains the path or rule that caused an item to be selected, by replaying the recorded reason chain against `selector.configBlob`.

```
src/trust/Projection.ts#capabilitiesAt

Included because:
  task term "provenance"
    → Policy.checkBranchPolicy      (reference, semantic)
    → capabilitiesAt                (call, syntax)
```

Explainability is part of the product, not debugging polish. If `configBlob` is unavailable, `why` MUST report the recorded reasons and explicitly state that they could not be re-derived.

### 10.3 `context refresh`

Re-evaluates a previous pack against a new Repository View. This is the hard part of the design and is specified, not implied.

**Re-anchoring.** For each item, identity is `blob + range`. When the blob is unchanged, the item is `unchanged`. When the blob changed, the implementation attempts, in order:

1. **Symbol re-match** — if the item has a CodeGraph symbol node, re-extract the new blob and match on `(path, symbolKind, name, enclosing scope)`. On a unique match, re-anchor the range and mark `changed`.
2. **Diff-hunk mapping** — map the old range through the blob diff. If the range maps intact and outside every hunk, re-anchor and mark `unchanged`; if it maps through modified hunks, re-anchor and mark `changed`.
3. **Failure** — if neither yields a unique anchor, mark `invalidated`. Refresh MUST NOT guess.

Renames are followed only via Git's own rename detection at the pinned similarity threshold, which is part of selector configuration.

**Reported states:**

```
unchanged      re-anchored, bytes semantically identical
changed        re-anchored, bytes differ
invalidated    could not be re-anchored, or blob/path gone
newly relevant selected under the new view, absent from the old pack
removed        no longer selected under the new view
```

Refresh emits a **new pack** plus a delta; it never mutates the old pack. It MUST refuse to compare packs whose `selector.config` differs, and MUST report — rather than silently absorb — a pack that is non-reproducible (§7.9, §9.4).

### 10.4 V1 boundary

```
clean view or explicit overlay tree
language-agnostic CodeGraph
Tree-sitter queries as default syntactic extraction
optional tree-sitter-graph adapter
optional semantic enrichers
deterministic selector with pinned limits
no embeddings required
pinned source/policy/instructions/memory
blob/range evidence
Git-reachable pack attachment
receipt bound to its enclosing signed session event
no submodule traversal
no requireContext branch policy
```

### 10.5 Acceptance criteria

V1 is successful when:

1. identical deterministic inputs produce byte-identical packs **on hosts with different machine resources**, because all limits are pinned;
2. a persistent pack survives push/fetch/GC because it is structurally reachable;
3. every selected item has at least one machine-readable inclusion reason;
4. selected source ranges resolve to exact Git blobs under both `sha1` and `sha256` object formats;
5. adding a new supported language does not require changing the selector or Context Pack schema;
6. Tree-sitter and `tree-sitter-graph` can be replaced without changing Context Pack semantics;
7. syntax-only edges remain distinguishable from semantically resolved edges;
8. pathological source/query/TSG inputs remain within configured counters, and any wall-clock fallback is visibly marked non-reproducible;
9. deleting all derived indexes and the unattached pack cache does not corrupt canonical repository state;
10. retrieval-poisoning fixtures cannot trivially displace all graph-connected implementation evidence;
11. an overlay-based pack is reproducible while its overlay objects exist and is refused durable attachment otherwise;
12. an `instruction` item whose authority chain does not resolve is downgraded to `narrative` by a conforming consumer;
13. a receipt lifted out of its enclosing session event fails verification;
14. refresh re-anchors moved symbols, and reports `invalidated` rather than guessing when re-anchoring is ambiguous.

The system should also be benchmarked against historical repository tasks (Appendix D).

---

# Appendix A — Extractor architecture

```
                        ┌─ direct Tree-sitter queries
                        ├─ tree-sitter-graph
source + language ──────┼─ compiler API
                        ├─ LSP / SCIP
                        └─ custom analyzer
                                 │
                                 ▼
                         normalize + validate
                                 │
                                 ▼
                            CodeGraph
                                 │
                                 ▼
                         Context selector
```

Direct Tree-sitter queries are the preferred v1 path for common facts: definitions, references, imports, calls, tests. `tree-sitter-graph` becomes attractive when extraction needs stateful or nested graph construction awkward to reconstruct from independent captures. The protocol does not care which extractor produced the normalized graph.

```
languages/
  typescript/
    grammar
    queries/tags.scm
    queries/locals.scm
    queries/context.scm
    graph.tsg            # optional
    limits.json          # pinned counters (§7.9)

  python/
    ...

  rust/
    ...
```

Supporting a new language should be mostly an extractor/query contribution rather than a change to the context engine.

---

# Appendix B — Invalidation

Git identifies changed blobs exactly, but semantic relationships can also change when configuration changes.

```
local invalidation
  changed implementation/test/import blob

global invalidation
  compiler/module-resolution/dependency configuration changed
  .gitattributes changed
  extractor, grammar, query pack, or enricher version changed
```

A global invalidation MAY rebuild the full normalized graph. Correctness is preferred over pretending every change is incrementally local.

---

# Appendix C — Semantic ranking

Embeddings or LLM rerankers MAY improve candidate ordering later. They are optional disposable caches, not canonical truth.

A deterministic non-semantic selector MUST remain available. A pack produced by a non-reproducible ranker MUST declare that fact (§5.5, `non-deterministic`) rather than presenting itself as a determinism fixture — otherwise §10.5.1 is untestable. The Context Pack still records what was selected.

---

# Appendix D — Evaluation

Evaluate on historical tasks at multiple budgets:

```
required-file recall
required-symbol recall
test recall
configuration recall
irrelevant-context ratio
generation latency
refresh latency
refresh re-anchor accuracy
manifest size
```

Adversarial fixtures SHOULD include lexical decoys, pathological extractor inputs, and rename/refactor histories that stress re-anchoring.

A handoff benchmark SHOULD compare:

```
fresh agent + no pack
fresh agent + prose summary
fresh agent + Context Pack
fresh agent + Context Pack + refresh delta
```

---

## Final invariant

> **A Context Pack is a content-addressed, explainable selection of repository evidence. A Context Receipt is a claim, bound to a signed session event, about what a harness exposed. Neither proves cognition. The selector consumes a language-agnostic CodeGraph whose identity and semantics belong to `git+`; Tree-sitter, `tree-sitter-graph`, compiler APIs, LSP, SCIP, and other analyzers are replaceable derived machinery.**
