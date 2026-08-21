# Git-Native Context Packs

**Status:** Draft specification  
**Project:** `@chr33s/git`  
**Target version:** Experimental / pre-1.0  
**Last updated:** 2026-08-21

## 1. Summary

This specification introduces three small Git-native concepts:

1. **Repository View** — the exact repository state a selector reads.
2. **Context Pack** — an immutable, explainable selection of repository evidence for a task.
3. **Context Receipt** — a signed session record claiming that a harness exposed a particular Context Pack.

The model is:

```text
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

```text
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

A Context Pack records selected evidence. A Context Receipt records a harness claim about exposure.

Neither proves:

- the model read every item;
- the model understood it;
- the selected context was sufficient;
- the output was caused by it.

### 3.2 Exact inputs

A deterministic selector is reproducible only when all inputs that affect selection are pinned.

V1 pins:

- source commit;
- standing instructions;
- repository policy;
- repository Memory;
- task digest;
- selector version/configuration.

The selector configuration MUST identify any parser, grammar, query pack, graph extractor, or semantic enricher version that affects deterministic selection.

If a future input changes deterministic selection, it MUST be pinned by the Repository View or selector configuration.

### 3.3 Evidence over prose

Context Packs SHOULD primarily reference exact repository evidence:

- blobs;
- byte ranges;
- symbols;
- tests;
- configuration;
- pinned policy and instructions.

Generated summaries are optional derived conveniences, never repository truth.

### 3.4 Explainability

Every selected item MUST state why it was selected.

### 3.5 Derived indexes are disposable

Syntax trees, normalized code graphs, compiler indexes, embeddings, and summaries MAY accelerate or enrich selection.

Deleting them MUST NOT corrupt canonical repository state.

### 3.6 Language independence

The Context Pack schema and selector MUST NOT depend on TypeScript, Tree-sitter, `tree-sitter-graph`, an LSP, SCIP, or any one parser ecosystem.

Language-specific extractors emit a common graph. The selector consumes that graph.

```text
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

Tree-sitter SHOULD be the default syntactic extraction substrate for languages with suitable grammars. `tree-sitter-graph` MAY be used to implement richer declarative extraction, but neither is the protocol boundary.

### 3.7 Selection never creates instruction authority

Retrieved content does not become instruction merely because it was selected.

V1 context items use four kinds:

```text
instruction
evidence
narrative
derived
```

`instruction` is valid only when the repository/session model already grants that authority.

### 3.8 Persistent means Git-reachable

An OID written inside JSON is only text. It does not create Git reachability.

A Context Pack claimed to replicate and survive Git GC MUST therefore be structurally reachable from a Git ref through commits/trees.

### 3.9 Bounded work

Context generation MUST bound attacker-controlled work, including candidate count, extractor output, graph traversal, history scanned, manifest size, and rendered context size.

---

## 4. Repository View

The Repository View names the complete deterministic repository inputs used by v1 selection.

```json
{
  "base": "sha1:abc123...",
  "instructions": "sha1:def456...",
  "policy": "sha1:789abc...",
  "memory": "sha1:456def..."
}
```

`base` is the exact source commit being understood. All paths and blob relationships are interpreted against it.

`instructions`, `policy`, and `memory` pin the exact standing instructions, repository policy, and bounded Repository Memory projection used for selection.

The principle is:

> **If an input affects deterministic selection, pin it.**

### 4.1 Clean worktree requirement

V1 operates only against a clean repository state.

`git+ context for` MUST refuse a dirty worktree rather than silently generate context from stale `HEAD` while an agent is editing different bytes.

A future version MAY support an ephemeral overlay tree.

---

## 5. Context Pack

A Context Pack is the canonical selected-evidence manifest.

Conceptual v1 schema:

```json
{
  "version": 1,
  "repo": "SHA256:...",
  "view": {
    "base": "sha1:abc123...",
    "instructions": "sha1:def456...",
    "policy": "sha1:789abc...",
    "memory": "sha1:456def..."
  },
  "task": {
    "digest": "sha256:..."
  },
  "selector": {
    "name": "context-v1",
    "version": "1.0.0",
    "config": "sha256:..."
  },
  "budget": 20000,
  "items": [
    {
      "kind": "evidence",
      "path": "src/server/Policy.ts",
      "blob": "sha1:...",
      "range": [4210, 6844],
      "reason": {
        "kind": "reference",
        "from": "requireProvenance"
      }
    }
  ],
  "omissions": []
}
```

### 5.1 Task binding

When generated for a session, the pack SHOULD store a digest of the task rather than duplicate prompt text.

The session already owns prompt storage, secret scanning, and redaction semantics.

### 5.2 Item identity

Exact source evidence is identified by repository objects, preferably:

```text
blob OID + byte range
```

Paths and symbol names are presentation metadata, not immutable identity.

### 5.3 Item kinds

V1 uses:

```text
instruction
  standing instructions or otherwise-valid scoped instruction

evidence
  source, tests, configuration, policy, Git-derived facts

narrative
  docs, session notes, Memory, comments, review text

derived
  generated summaries, labels, semantic hints
```

A selector MUST NOT promote `narrative`, `evidence`, or `derived` content to `instruction` merely because it is relevant.

### 5.4 Reasons

Every item MUST have at least one machine-readable reason.

Initial reason kinds MAY include:

```text
explicit-path
explicit-symbol
task-term
definition
reference
call
import
implementation
test
config
history
memory
policy
instruction
neighbor
```

### 5.5 Omissions

When useful, the pack SHOULD record high-ranking evidence excluded by the budget so a bounded context set does not appear exhaustive.

### 5.6 Canonical encoding

Context Pack bytes are protocol surface because their Git object ID depends on them.

V1 MUST define deterministic field ordering, array ordering, UTF-8 encoding, number encoding, and terminal-newline behavior.

---

## 6. Context Receipt

A Context Receipt is a signed session claim that a harness exposed a Context Pack.

The smallest useful v1 receipt is:

```json
{
  "pack": "sha1:8d7ad4..."
}
```

Its meaning is intentionally narrow:

> The signer claims this Context Pack was exposed during the session.

It does not prove model cognition or causation.

The Pack is a derived selection artifact. The Receipt is historical session provenance.

Future versions MAY add a render digest, render profile, expansions, or model-specific token counts.

---

## 7. Selection and the language-agnostic CodeGraph

V1 selection SHOULD remain deterministic and simple:

```text
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

The selector operates on a normalized graph rather than a language-specific compiler API or extractor-specific graph.

Conceptual contract:

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

The exact in-memory TypeScript types are implementation detail. The normative ideas are:

- graph nodes are grounded in Git evidence;
- graph relationships use a small language-independent vocabulary;
- unresolved targets may remain textual;
- every relationship states its resolution class.

Implementations MAY add namespaced node/edge kinds internally, but the selector SHOULD rely on the core vocabulary for portable behavior.

### 7.2 `git+` owns identity

Extractor-specific node IDs MUST NOT become durable CodeGraph identity.

`git+` SHOULD derive graph identity from repository evidence, for example:

```text
repo + blob OID + byte range + normalized node kind
```

An extractor may produce temporary IDs, but they MUST be normalized before selection or persistence.

This prevents Context Pack semantics from depending on Tree-sitter node handles, `tree-sitter-graph` node IDs, compiler object identity, process memory, or parser lifetimes.

### 7.3 Extractor interface

Language-specific extractors translate source into CodeGraph fragments.

Conceptually:

```ts
interface CodeExtractor {
  readonly name: string
  readonly version: string

  extract(input: {
    readonly path: string
    readonly blob: string
    readonly source: Uint8Array
    readonly language: string
  }): CodeGraph
}
```

A new language SHOULD require a grammar/query pack or extractor, not a new selector or Context Pack schema.

### 7.4 Tree-sitter as the v1 syntactic substrate

For languages with suitable grammars, V1 SHOULD use Tree-sitter to produce syntax-level graph facts.

Tree-sitter query packs provide a practical cross-language convention for definitions, references, locals, and syntax patterns.

A language integration SHOULD prefer declarative files where practical:

```text
queries/tags.scm
queries/locals.scm
queries/context.scm
```

`context.scm` MAY identify additional relationships useful to selection, such as imports, exports, test declarations, routes, or configuration references.

Example normalization:

```text
Tree-sitter definition capture
        ↓
CodeNode(kind="symbol", symbolKind="function")

Tree-sitter call/reference capture
        ↓
CodeEdge(kind="calls", resolution="syntax")
```

### 7.5 Syntax is not semantic resolution

Tree-sitter is a parser, not a compiler or type checker.

Syntax may identify:

```text
client.get(...)
```

as a call to a member named `get`, but it cannot generally prove which declaration that member resolves to.

The graph MUST preserve that distinction:

```json
{
  "kind": "calls",
  "from": "symbol:a",
  "target": "client.get",
  "resolution": "syntax"
}
```

A resolver may later enrich it:

```json
{
  "kind": "calls",
  "from": "symbol:a",
  "to": "symbol:b",
  "resolution": "semantic"
}
```

The selector MAY prefer stronger resolution, but unresolved syntax facts remain useful and explainable.

### 7.6 `tree-sitter-graph` as an optional extraction engine

`tree-sitter-graph` MAY be used when ordinary Tree-sitter queries become awkward for stateful, nested, or cross-stanza graph construction.

Its role is:

```text
source
  ↓
Tree-sitter grammar
  ↓
syntax tree
  ↓
language-specific .tsg rules
  ↓
extractor graph
  ↓
normalize + validate
  ↓
git+ CodeGraph
```

The Context Pack protocol MUST NOT depend on `tree-sitter-graph` graph semantics or DSL details.

In particular, `git+` MUST NOT make canonical behavior depend on:

- `tree-sitter-graph` node IDs;
- its in-memory graph lifetime;
- its attribute namespace;
- its edge identity rules;
- a particular `.tsg` execution order;
- its host-language bindings.

The stable boundary remains `CodeGraph`.

This allows implementations to replace `tree-sitter-graph` with direct Tree-sitter queries, compiler APIs, SCIP, LSP, prebuilt indexes, or custom domain analyzers without changing Context Pack semantics.

### 7.7 Normalization requirements

Every extractor output MUST be normalized before the selector consumes it.

Normalization SHOULD:

1. assign Git-grounded node identity;
2. map language/extractor-specific node kinds to the core CodeGraph vocabulary;
3. map relationship types to core edge kinds;
4. preserve unresolved textual targets;
5. attach `syntax`, `local`, or `semantic` resolution;
6. validate all blob/range references;
7. sort nodes and edges deterministically.

Extractor output that cannot be normalized safely MUST be rejected or omitted rather than treated as canonical fact.

### 7.8 Optional semantic enrichers

A language MAY enrich CodeGraph using:

```text
compiler APIs
language servers
SCIP or equivalent indexes
build-system metadata
framework-specific analyzers
```

The architecture is:

```text
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

Semantic enrichers are optional. No enricher is required to read or validate the canonical Context Pack.

### 7.9 Extractor resource bounds

Language extraction is attacker-controlled work and MUST be bounded independently of later context selection.

Implementations MUST bound at least:

```text
source bytes parsed
query/rule matches
nodes emitted
edges emitted
normalized graph bytes
execution time or cancellation budget
```

Hosts MAY define concrete limits per language or repository size.

Exceeding a limit MUST result in a bounded failure or partial result with an explicit omission reason. It MUST NOT create unbounded memory or durable-storage growth.

### 7.10 History and Memory

V1 MAY use bounded Git co-change/history signals. The inspected history horizon MUST be deterministic and part of selector configuration.

The selector SHOULD consult the pinned bounded Memory projection instead of scanning the complete session corpus.

### 7.11 Budget

Selection operates under an explicit context budget.

V1 MAY use a stable model-independent estimator such as:

```text
estimated tokens = ceil(character count / 4)
```

Harnesses MAY compute exact model-specific tokens when rendering.

---

## 8. Storage and reachability

A persistent Context Pack MUST be a Git-reachable attachment of the session record that references it.

Recommended shape:

```text
session event commit
└── tree
    ├── event.json
    ├── event.sig
    └── context.json
```

`context.json` is the canonical Context Pack.

The event payload MAY also record the pack OID for validation, but the tree edge provides reachability.

The evidence blobs referenced inside `context.json` SHOULD remain logical references rather than attachments so provenance replication does not implicitly drag source history with it.

Context generation itself is a read operation. A read-only caller MUST NOT gain arbitrary durable object creation merely by asking for context.

---

## 9. Security and trust

### 9.1 Prompt injection

Retrieved comments, docs, Memory, and other narrative do not gain instruction authority through relevance or signature alone.

> **Selection never creates instruction authority.**

### 9.2 Retrieval poisoning

Lexical relevance is manipulable. A contributor may add decoy files or symbols matching likely task terms to consume the context budget.

V1 SHOULD mitigate this with:

- explicit-anchor preference;
- graph-connectedness preference;
- traversal limits;
- candidate limits;
- budget reservations for tests/configuration;
- lower weight for comments than executable symbols;
- visible omission/reason data.

### 9.3 Extractor poisoning and exhaustion

Parser/query/TSG inputs are repository-controlled. Extractors MUST obey the resource bounds in §7.9.

A pathological grammar/query/rule set MUST NOT be able to turn context generation into unbounded CPU, memory, or graph growth.

### 9.4 Secret duplication and redaction

Context Packs SHOULD reference existing evidence rather than duplicate source or session prose.

If an attached pack contains prose derived from a redacted session event, that derived attachment MUST become unavailable under the same redaction lifecycle.

### 9.5 Derived indexes are untrusted accelerators

Remote or cached structural/semantic indexes MAY be validated, rebuilt, or ignored.

A missing index is a performance problem, not repository corruption.

---

## 10. V1 product surface and acceptance criteria

The core CLI is intentionally only three commands:

```sh
git+ context for --task "..."
git+ context why <pack> <item>
git+ context refresh <pack>
```

They answer:

```text
What should I know?
Why is this here?
What changed in what I need to know?
```

### 10.1 `context for`

Generates a deterministic Context Pack from a clean Repository View.

It SHOULD return canonical bytes, would-be OID, and a concise selection summary.

Persistence occurs only when attached through authorized session provenance.

### 10.2 `context why`

Explains the path or rule that caused an item to be selected.

Example:

```text
src/trust/Projection.ts#capabilitiesAt

Included because:
  task term "provenance"
    → Policy.checkBranchPolicy
    → capabilitiesAt
```

Explainability is part of the product, not debugging polish.

### 10.3 `context refresh`

Re-evaluates a previous pack against a new Repository View and reports:

```text
unchanged
changed
invalidated
newly relevant
removed
```

### 10.4 V1 boundary

V1 SHOULD remain narrow:

```text
clean worktree only
language-agnostic CodeGraph
Tree-sitter queries as default syntactic extraction
optional tree-sitter-graph adapter
optional semantic enrichers
deterministic selector
no embeddings required
pinned source/policy/instructions/memory
blob/range evidence
strict extractor and selector limits
Git-reachable pack attachment
minimal Context Receipt
no requireContext branch policy
```

### 10.5 Acceptance criteria

V1 is successful when:

1. identical deterministic inputs produce byte-identical packs;
2. a persistent pack survives push/fetch/GC because it is structurally reachable;
3. every selected item has an inclusion reason;
4. selected source ranges resolve to exact Git blobs;
5. adding a new supported language does not require changing the selector or Context Pack schema;
6. Tree-sitter and `tree-sitter-graph` can be replaced without changing Context Pack semantics;
7. syntax-only edges remain distinguishable from semantically resolved edges;
8. pathological source/query/TSG inputs remain within configured resource limits;
9. deleting all derived indexes does not corrupt canonical repository state;
10. retrieval-poisoning fixtures cannot trivially displace all graph-connected implementation evidence.

The system should also be benchmarked against historical repository tasks to measure required-file/symbol recall, irrelevant-context ratio, generation latency, refresh latency, and handoff usefulness.

---

# Appendix A — Extractor architecture

The intended architecture is:

```text
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

Direct Tree-sitter queries are the preferred v1 path for common facts such as definitions, references, imports, calls, and tests.

`tree-sitter-graph` becomes attractive when extraction needs stateful or nested graph construction that is awkward to reconstruct from independent query captures.

The protocol does not care which extractor produced the normalized graph.

A language package may conceptually look like:

```text
languages/
  typescript/
    grammar
    queries/tags.scm
    queries/locals.scm
    queries/context.scm
    graph.tsg            # optional

  python/
    ...

  rust/
    ...
```

Supporting a new language should be mostly an extractor/query contribution rather than a change to the context engine.

---

# Appendix B — Invalidation

Git identifies changed blobs exactly, but semantic relationships can also change when configuration changes.

Implementations SHOULD distinguish:

```text
local invalidation
  changed implementation/test/import blob

global invalidation
  compiler/module-resolution/dependency configuration changed
```

A global invalidation MAY rebuild the full normalized graph.

Correctness is preferred over pretending every change is incrementally local.

---

# Appendix C — Semantic ranking

Embeddings or LLM rerankers MAY improve candidate ordering later.

They are optional disposable caches, not canonical truth.

A deterministic non-semantic selector MUST remain available.

The Context Pack records what was selected even if a heuristic selector cannot later be reproduced bit-for-bit.

---

# Appendix D — Evaluation

The feature should be evaluated on historical tasks at multiple context budgets.

Useful measurements include:

```text
required-file recall
required-symbol recall
test recall
configuration recall
irrelevant-context ratio
generation latency
refresh latency
manifest size
```

Adversarial fixtures SHOULD include lexical decoys and pathological extractor inputs.

A handoff benchmark SHOULD compare:

```text
fresh agent + no pack
fresh agent + prose summary
fresh agent + Context Pack
fresh agent + Context Pack + refresh delta
```

---

## Final invariant

> **A Context Pack is a content-addressed, explainable selection of repository evidence. A Context Receipt is a signed claim about what a harness exposed. The selector consumes a language-agnostic CodeGraph whose identity and semantics belong to `git+`; Tree-sitter, `tree-sitter-graph`, compiler APIs, LSP, SCIP, and other analyzers are replaceable derived machinery.**
