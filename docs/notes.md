# Code-Anchored Repository Memory

**Status:** Implemented
**Scope:** `@chr33s/git` hub extension
**Applies to:** CLI, JSON API, agent workflows, merge policy
**Primary namespace:** `refs/hub/note/*`

## 1. Summary

Add durable, code-anchored repository knowledge to `@chr33s/git`.

An anchored note records a constraint, invariant, decision, or non-obvious fact that future changes to a specific file or symbol should preserve.

Example:

```text
src/auth.ts#function verify

Comparison must remain constant-time; early return leaks token length.
```

Notes are first-class Git-native records. They replicate with the repository, retain authorship and history, and can be checked against the current source tree.

The feature introduces four related capabilities:

1. **Anchored memory** — durable constraints associated with files or symbols.
2. **Git-native note storage** — signed append-only note event DAGs under `refs/hub/note/*`.
3. **`git+ why`** — read relevant knowledge before modifying code.
4. **Drift auditing** — determine whether the code a note describes is unchanged, changed, missing, or unverifiable, and require explicit resolution rather than silently advancing its baseline.

This system complements, rather than replaces, repository-wide distilled memory.

Repository-wide memory answers:

> What generally matters in this repository?

Anchored memory answers:

> What must I know before changing this specific code?

---

# 2. Goals

The system MUST:

- attach durable knowledge to a file or semantic anchor;
- replicate entirely through Git objects and refs;
- retain verifiable authorship;
- tolerate concurrent note creation without central coordination;
- distinguish source changes from mere formatting churn where possible;
- detect when an anchored declaration or section disappears;
- never treat seeing changed code as confirmation that an existing constraint still holds;
- expose machine-readable queries suitable for coding agents;
- allow merge policy to require all relevant note drift to be resolved;
- work even when semantic parsing is unavailable;
- preserve the universal Git core's separation from platform-specific parser implementations.

The system SHOULD:

- follow source-file renames;
- support whole-file anchors before semantic anchors are available;
- let different parser implementations support different languages;
- keep note reads cheap enough to perform before editing every file;
- integrate with task, session, PR, and check workflows.

---

# 3. Non-goals

This feature is NOT:

- a replacement for session provenance;
- a transcript store;
- a documentation system;
- a generic knowledge graph;
- an AI-generated memory system;
- a requirement that every file have notes;
- an automatic claim that a changed constraint remains valid;
- a reason to embed tree-sitter or any specific parser inside the universal Git core.

A note SHOULD contain a future-facing constraint or fact, not a summary of what a commit changed.

Bad:

```text
Refactored token verification into a helper.
```

Good:

```text
Token comparison must remain constant-time.
```

---

# 4. Relationship to Existing Repository Memory

Existing repository memory remains a bounded projection distilled from session-produced observations.

It is:

- repository-wide;
- derived;
- disposable;
- bounded for context-window consumption;
- rebuilt from authoritative session records.

Anchored memory is different.

It is:

- explicitly authored;
- attached to source;
- durable;
- individually addressable;
- independently auditable;
- changed only through explicit lifecycle events.

The two MAY be returned together by `git+ why`, but MUST remain separate storage concepts.

---

# 5. Terminology

## Note

A durable constraint attached to source.

## Anchor

A stable semantic description of the source region a note applies to.

Examples:

```text
@file
function verify
class Repository
interface WorkTree
## Authentication
#script
```

## Baseline

The source fingerprint against which the note was last confirmed.

## Drift

A difference between the note's confirmed baseline and the source currently resolved by its anchor.

## Resolution

An explicit human or agent action that handles drift:

```text
confirm
replace
retire
```

## Projection

Derived state produced by folding a note's append-only events.

---

# 6. Git Namespace

Each note has a globally unique ID and its own ref:

```text
refs/hub/note/<note-id>
```

Example:

```text
refs/hub/note/01991f71-b8d7-7def-82d8-0c30c58ae122
```

`note-id` SHOULD use the same time-sortable ID mechanism already used by hub events where practical.

A note ID MUST:

- be non-empty;
- be at most 128 bytes;
- contain no `/`;
- produce a valid Git ref through the existing ref-name validator.

No secondary mutable index is authoritative.

Implementations MAY maintain projections or caches for efficient path lookup.

---

# 7. Event Model

A note is an append-only DAG using the same event machinery as other hub records.

All events MUST include the common hub envelope:

```ts
{
  version: 1;
  repo: string;
  note: string;
  id: string;
  issuedAt: string;
  trustHead: string | null;
}
```

All state-changing events MUST be signed.

## 7.1 `note.created`

Creates the note.

```ts
{
  type: "note.created"

  path: string
  anchor: string
  text: string

  baseline: {
    resolver: string
    normalization: string
    signatureHash: string | null
    contentHash: string
    rawHash: string
  } | null

  pinned: boolean
}
```

`path` MUST be repository-relative.

`anchor` MUST be canonical when an anchor resolver is available.

If no resolver is available, `@file` MUST remain supported.

A creation with no baseline is permitted. This allows a note to be written in an environment that cannot resolve its anchor.

A later audit MAY establish its initial baseline without treating that as confirmation of changed code.

## 7.2 `note.confirmed`

States explicitly that the existing text remains true for a new source baseline.

```ts
{
  type: "note.confirmed";

  baseline: {
    resolver: string;
    normalization: string;
    signatureHash: string | null;
    contentHash: string;
    rawHash: string;
  }
}
```

This event MUST only be emitted when the anchor currently resolves.

It MUST NOT modify the note text.

## 7.3 `note.replaced`

Supersedes the current note text.

```ts
{
  type: "note.replaced";

  text: string;

  baseline: {
    resolver: string;
    normalization: string;
    signatureHash: string | null;
    contentHash: string;
    rawHash: string;
  }
}
```

A replacement represents:

> The old constraint is no longer exactly correct; this is the current one.

## 7.4 `note.retired`

Marks the note inactive.

```ts
{
  type: "note.retired";

  reason: string | null;
}
```

Retirement MUST preserve history.

Retired notes MUST not create merge-blocking drift.

## 7.5 `note.restored`

Restores a retired note.

```ts
{
  type: "note.restored"

  baseline: {
    resolver: string
    normalization: string
    signatureHash: string | null
    contentHash: string
    rawHash: string
  } | null
}
```

## 7.6 `note.pinned`

```ts
{
  type: "note.pinned";
}
```

## 7.7 `note.unpinned`

```ts
{
  type: "note.unpinned";
}
```

Pinning is projection metadata and MUST NOT affect drift semantics.

---

# 8. Note Projection

Folding a note ref produces:

```ts
interface NoteProjection {
  readonly id: string;

  readonly path: string;
  readonly anchor: string;
  readonly text: string;

  readonly createdAt: string;
  readonly createdBy: string;

  readonly updatedAt: string;
  readonly updatedBy: string;

  readonly active: boolean;
  readonly pinned: boolean;

  readonly baseline: Baseline | null;
}
```

Conflicting DAG branches MUST be handled using the same deterministic convergence rules used elsewhere in the hub.

Where two concurrent events represent incompatible judgments, the projection MUST NOT silently select one semantic outcome if doing so would erase disagreement.

The fold SHOULD expose a conflict state when necessary.

Example:

```ts
{
  state: "conflicted",
  competing: [...]
}
```

A conflicted note MUST be treated as unresolved for merge-policy purposes.

---

# 9. Anchor Resolution Port

Semantic parsing MUST remain outside the universal Git domain core.

Define a service boundary:

```ts
export class AnchorResolver extends Context.Service<
  AnchorResolver,
  {
    readonly name: string;
    readonly version: string;

    readonly anchors: (
      path: string,
      content: Uint8Array,
    ) => Effect.Effect<ReadonlyArray<Anchor>, AnchorResolutionFailure>;

    readonly resolve: (
      path: string,
      content: Uint8Array,
      anchor: string,
    ) => Effect.Effect<AnchorResolution, AnchorResolutionFailure>;
  }
>()("hub/AnchorResolver") {}
```

Where:

```ts
interface Anchor {
  readonly value: string;
  readonly startLine: number;
  readonly endLine: number;
}

type AnchorResolution =
  | {
      readonly _tag: "Found";
      readonly anchor: Anchor;
      readonly fingerprint: Fingerprint;
    }
  | {
      readonly _tag: "Missing";
    }
  | {
      readonly _tag: "Unsupported";
    }
  | {
      readonly _tag: "Ambiguous";
      readonly candidates: ReadonlyArray<Anchor>;
    };
```

`@file` MUST be implementable without a language parser.

---

# 10. Canonical Anchors

Resolvers SHOULD expose stable, human-readable anchors.

Initial desired forms:

```text
@file

function verify
class Repository
interface WorkTree
type Config
const MAX_MEMORY

## Rate limiting

#script
#style
#template
```

A bare symbol MAY be accepted interactively:

```text
verify
```

but SHOULD be persisted only after qualification to a canonical anchor:

```text
function verify
```

If multiple declarations share the same bare name, creation MUST refuse ambiguity and return the candidates.

---

# 11. Fingerprints

A resolved anchor produces:

```ts
interface Fingerprint {
  readonly resolver: string;
  readonly normalization: string;

  readonly signatureHash: string | null;
  readonly contentHash: string;
  readonly rawHash: string;
}
```

## Raw hash

Hash of the exact source region.

Used to detect any byte-level change.

## Content hash

Hash after language-aware normalization.

Normalization SHOULD ignore:

- whitespace-only changes;
- formatting-only changes;
- comments where the parser can reliably identify them.

It MUST NOT normalize semantic tokens.

## Signature hash

Hash of the declaration or contract portion when supported.

Examples:

```ts
export function verify(token: string): boolean;
```

versus the function body.

Resolvers unable to distinguish declaration from body set:

```text
signatureHash = null
```

Fingerprint hashes SHOULD use SHA-256.

Storage MAY use a shortened printable representation, but equality MUST retain sufficient collision resistance for this use.

---

# 12. Normalization Versioning

Every fingerprint MUST identify:

```text
resolver
normalization
```

Example:

```text
resolver: typescript-tree-sitter@1
normalization: semantic-v1
```

A resolver or normalization upgrade MUST NOT falsely report drift.

If the stored normalization is no longer comparable to the current one, audit returns:

```text
rebaseline-required
```

A baseline MAY then be regenerated without claiming that the note was re-confirmed.

The implementation MUST distinguish:

> We changed how hashes are computed.

from:

> The code changed.

---

# 13. Drift States

Auditing a live note produces exactly one principal state.

## `fresh`

The semantically normalized anchored source is unchanged.

## `content-changed`

The declaration remains equivalent, but implementation/content changed.

Requires explicit resolution.

## `contract-changed`

The declaration/signature changed.

Requires explicit resolution.

## `anchor-missing`

The source file exists, but the anchor no longer resolves.

Requires explicit resolution.

## `source-missing`

The note's source path no longer exists and could not be followed through a rename.

Requires explicit resolution unless the note is retired.

## `unverifiable`

No resolver exists for this source/anchor.

Advisory by default.

## `rebaseline-required`

The old fingerprint and current resolver normalization are not directly comparable.

Does not itself imply semantic drift.

## `conflicted`

Concurrent note lifecycle events cannot be deterministically interpreted as a single current judgment.

Requires resolution.

---

# 14. Drift Algorithm

Given:

```text
note baseline B
current source S
anchor A
```

the checker:

1. resolves the note's path;
2. attempts rename following when the path no longer exists;
3. resolves anchor `A`;
4. computes current fingerprint `C`;
5. compares `B` and `C`.

Rules:

```text
B == null
    => establish initial baseline if possible

resolver incompatible
    => rebaseline-required

anchor unsupported
    => unverifiable

anchor missing
    => anchor-missing

signatureHash differs
    => contract-changed

contentHash differs
    => content-changed

otherwise
    => fresh
```

A differing `rawHash` alone MUST NOT create drift if normalized signature/content hashes match.

---

# 15. Critical Baseline Rule

An audit MUST NEVER advance the confirmed baseline merely because it observed new source.

Given:

```text
baseline = old implementation
current  = changed implementation
```

repeated checks MUST continue returning drift until one of:

```text
note confirm
note replace
note retire
```

is explicitly recorded.

This rule is fundamental.

Observation is not approval.

---

# 16. Rename Following

If a note's path disappears, audit SHOULD determine whether Git history identifies it as a rename.

Example:

```text
src/auth.ts
→ src/security/auth.ts
```

When confidently identified, the projection MAY report:

```text
pathMovedFrom: "src/auth.ts"
path: "src/security/auth.ts"
```

The implementation SHOULD avoid rewriting authoritative history solely to update the path.

Preferred model:

- original note creation retains its original path;
- projection follows renames;
- a dedicated re-anchor/path-move event MAY later make the new path explicit.

If rename inference is ambiguous, return `source-missing`.

---

# 17. CLI

## 17.1 Create

```bash
git+ note add src/auth.ts \
  --anchor "function verify" \
  "comparison must remain constant-time"
```

Whole file:

```bash
git+ note add src/config.ts \
  "this file must stay browser-safe"
```

Defaults to:

```text
anchor = @file
```

when no anchor is supplied.

Interactive use MAY allow anchor selection.

Machine callers SHOULD use explicit text and anchor arguments.

---

# 18. Anchor Discovery

```bash
git+ note anchors src/auth.ts
```

Example:

```text
@file                  1-240
function verify        18-42
function refresh       44-61
class TokenStore       80-160
```

JSON:

```bash
git+ note anchors src/auth.ts --json
```

```json
{
  "path": "src/auth.ts",
  "resolver": "typescript-tree-sitter@1",
  "anchors": [
    {
      "value": "@file",
      "startLine": 1,
      "endLine": 240
    },
    {
      "value": "function verify",
      "startLine": 18,
      "endLine": 42
    }
  ]
}
```

---

# 19. `git+ why`

`git+ why` is the primary read path for humans and agents.

```bash
git+ why src/auth.ts
```

It SHOULD answer:

- active anchored notes relevant to the requested path;
- their current freshness state where cheap to compute;
- relevant repository-wide memory;
- optionally relevant unresolved decisions or task/session context.

Initial implementation MAY restrict itself to:

```text
anchored notes
+ repository memory
```

to keep semantics narrow.

Example:

```text
src/auth.ts

Anchored constraints

  function verify
    comparison must remain constant-time
    status: fresh
    note: 01991f71...

  class TokenStore
    token eviction order is externally observable
    status: content-changed
    note: 01991f83...

Repository memory

  convention: authentication code uses Uint8Array internally
```

---

# 20. Directory Queries

```bash
git+ why src/auth/
```

returns active notes for all paths below the directory.

The output SHOULD group by file.

This is intended for agents that know they are about to modify a subsystem.

---

# 21. Machine-readable `why`

```bash
git+ why src/auth.ts --json
```

Response:

```json
{
  "query": "src/auth.ts",
  "notes": [
    {
      "id": "01991f71-b8d7-7def-82d8-0c30c58ae122",
      "path": "src/auth.ts",
      "anchor": "function verify",
      "text": "comparison must remain constant-time",
      "status": "fresh",
      "baseline": {
        "resolver": "typescript-tree-sitter@1",
        "normalization": "semantic-v1",
        "signatureHash": "6e73...",
        "contentHash": "02cd...",
        "rawHash": "351a..."
      }
    }
  ],
  "memory": [
    {
      "kind": "convention",
      "text": "authentication code uses Uint8Array internally"
    }
  ]
}
```

Every documented key SHOULD remain present, using `null` when a value does not apply.

Agents should not have to infer whether a missing field means:

```text
unknown
unsupported
empty
or accidentally omitted
```

---

# 22. Audit

Repository-wide:

```bash
git+ note check
```

Path-scoped:

```bash
git+ note check src/auth.ts
git+ note check src/
```

Example:

```text
notes: 14 checked

11 fresh
 1 content-changed
 1 contract-changed
 1 anchor-missing

drift  src/auth.ts#function verify
       content changed since last confirmation

drift  src/api.ts#interface Request
       declaration changed since last confirmation

missing src/legacy.ts#function parse
```

Exit codes:

```text
0 = no actionable drift
1 = operational failure
2 = actionable unresolved drift
```

`unverifiable` MAY remain exit 0 unless repository policy upgrades it to required.

---

# 23. Explicit Drift Resolution

## Confirm

```bash
git+ note confirm <note-id>
```

Meaning:

> The code changed, but this constraint still applies exactly as written.

Requirements:

- anchor MUST currently resolve;
- new fingerprint MUST be recorded;
- note text MUST remain unchanged;
- event MUST be signed.

## Replace

```bash
git+ note replace <note-id> \
  "new constraint text"
```

Meaning:

> The old constraint is no longer exactly correct.

Requirements:

- anchor SHOULD resolve;
- new text and baseline are recorded together;
- replacement history remains readable.

## Retire

```bash
git+ note retire <note-id>
```

Meaning:

> This constraint no longer applies.

Retirement requires no source anchor to exist.

## Restore

```bash
git+ note restore <note-id>
```

Restores a retired note.

If its anchor resolves, the new baseline SHOULD be recorded.

---

# 24. Merge-policy Integration

Drift checking SHOULD integrate through the existing hub check abstraction rather than hard-coded merge behavior.

Recommended check name:

```text
memory
```

A CI process or trusted agent holding:

```text
hub.check:memory
```

can report whether relevant notes are resolved.

Repository policy MAY then require:

```text
requiredChecks: ["test", "memory"]
```

The memory check SHOULD inspect the PR's effective changed paths.

A required memory check fails for:

```text
content-changed
contract-changed
anchor-missing
source-missing
conflicted
```

Policy MAY decide whether:

```text
unverifiable
rebaseline-required
```

are advisory or blocking.

Default recommendation:

```text
unverifiable        advisory
rebaseline-required advisory
```

---

# 25. PR-scoped Checking

Given base `B` and head `H`:

```bash
git+ note check --base <B> --head <H>
```

The checker SHOULD prioritize notes whose paths or resolved spans overlap source changed between `B` and `H`.

A full-repository audit remains available, but merge checking SHOULD avoid unrelated historical drift blocking an otherwise independent change unless repository policy explicitly requests global enforcement.

Recommended default:

```text
check notes associated with files touched by the PR
```

---

# 26. Agent Workflow

Recommended lifecycle:

```bash
git+ why src/auth.ts --json

# agent performs work

git+ note check src/auth.ts --json

# resolve every relevant stale note

git+ note confirm <id>
# or
git+ note replace <id> "..."
# or
git+ note retire <id>

git+ note check src/auth.ts --json
```

Agent instructions SHOULD say:

> Read `git+ why` before editing a file.
> Record only non-obvious constraints future work must preserve.
> Before submission, resolve every note whose relevant source drifted.

The system MUST NOT require the agent to create a note for every edit.

---

# 27. Session Integration

Session provenance remains authoritative for:

```text
what was requested
who performed the work
what commits/refs/PRs resulted
what the session learned
```

Anchored notes represent knowledge intended to survive future sessions.

A session MAY therefore produce both:

```text
session.produced.note
anchored note events
```

but they have different semantics.

Example:

```text
session note:
"Refactoring the parser exposed a Windows path issue."

anchored note:
src/path.ts#function normalizePath
"Drive-letter comparison must remain case-insensitive."
```

A future distillation process MAY suggest candidate anchored notes, but MUST NOT publish them as authoritative constraints without an explicit signed event.

---

# 28. Security Model

Anchored note text is untrusted data.

Reading a note MUST NOT make its contents executable instruction merely because it exists in the repository.

Agents SHOULD treat it as repository context subject to normal authorization and policy.

All note lifecycle mutations MUST record signer identity.

Permission model SHOULD eventually expose dedicated capabilities:

```text
hub.note
hub.note.confirm
hub.note.retire
```

A simpler first version MAY use one:

```text
hub.note
```

for all note mutations.

Reading notes SHOULD require only repository read access.

Repositories MUST retain the ability to redact note events containing secrets using the existing tombstone/redaction mechanism.

---

# 29. Performance

`git+ why <file>` is expected to run frequently.

Implementations SHOULD avoid scanning every note ref on every invocation.

A disposable projection MAY index:

```text
path -> active note ids
```

The projection MUST be reconstructible entirely from note refs.

It MUST NOT be authoritative.

Suggested cache shape:

```text
refs/notes/hub/note-index
```

or a local non-replicated cache where appropriate.

The exact optimization is implementation-specific.

Correctness MUST not depend on the cache.

---

# 30. Parser Deployment

The core note/event schema MUST work without a parser.

Minimum viable implementation:

```text
@file anchors only
raw + normalized whole-file fingerprint
```

Semantic resolver support can then be layered in.

Recommended first resolver:

```text
TypeScript / JavaScript
Markdown
```

because they cover this repository's own source and documentation.

Possible later languages:

```text
Rust
Go
Python
C/C++
Java
shell
Vue/Svelte component blocks
```

Parser dependencies SHOULD live behind Node/platform-specific boundaries.

---

# 31. Implementation Modules

Suggested layout:

```text
src/hub/Note.ts
src/hub/NoteProjection.ts
src/hub/NoteAudit.ts
src/hub/Anchor.ts

src/hub/Anchor.node.ts

src/cli/note.ts
src/cli/why.ts
```

Responsibilities:

```text
Note.ts
  schemas
  signing
  issue
  refs
  event encoding

NoteProjection.ts
  DAG fold
  active state
  conflict handling

Anchor.ts
  AnchorResolver service contract
  @file resolver
  fingerprint types

Anchor.node.ts
  semantic resolver implementation

NoteAudit.ts
  source lookup
  rename following
  baseline comparison
  drift states

cli/note.ts
  add
  anchors
  check
  confirm
  replace
  retire
  restore

cli/why.ts
  query composition
  human output
  JSON output
```

---

# 32. JSON API

Recommended endpoints:

```text
GET  /:repo/why?path=<path>
GET  /:repo/notes?path=<path>
GET  /:repo/notes/:id
POST /:repo/notes
POST /:repo/notes/:id/confirm
POST /:repo/notes/:id/replace
POST /:repo/notes/:id/retire
GET  /:repo/notes/check
```

Mutation endpoints use the existing authentication and capability model.

Exact routing may follow current API conventions.

---

# 33. Compatibility

Anchored-note refs are additive.

Stock Git:

```bash
git fetch
```

must replicate the underlying note records when configured to fetch hub refs.

A clone that does not understand notes MUST remain a valid Git repository.

No change to commit, tree, blob, index, or transport formats is required.

---

# 34. Testing

## Event tests

Verify:

- event encoding is deterministic;
- signatures survive round-trip;
- invalid note IDs are refused before writing;
- malformed events do not enter projections;
- concurrent DAG events converge predictably.

## Projection tests

Verify:

- create → confirm;
- create → replace;
- create → retire → restore;
- pin/unpin;
- conflicting concurrent lifecycle events;
- redacted events disappear from effective state correctly.

## Anchor tests

Verify:

- `@file`;
- canonical symbol resolution;
- ambiguous bare symbol refusal;
- Markdown heading spans;
- formatting churn does not alter normalized fingerprint;
- contract changes alter `signatureHash`;
- implementation changes alter `contentHash`.

## Audit tests

Critical cases:

```text
same source
=> fresh

formatting-only change
=> fresh

comment-only change
=> fresh where supported

body change
=> content-changed

declaration change
=> contract-changed

anchor deleted
=> anchor-missing

file deleted
=> source-missing

file renamed
=> followed

unsupported language
=> unverifiable
```

Most importantly:

```text
check changed code
check again
check again

=> remains changed every time
```

until a signed:

```text
confirm
replace
retire
```

occurs.

## Interop tests

Where source behavior depends on Git rename detection or worktree semantics, claims SHOULD be checked against the real Git binary, consistent with the repository's existing interoperability rule.

---

# 35. Rollout Plan

## Status

All four phases are implemented.

```text
phase 1  note event DAG, @file anchors, add/check/confirm/replace/retire, why
phase 2  AnchorResolver, semantic anchors, signature/content fingerprints
phase 3  PR-scoped checks, hub.note, namespace policy, JSON API
phase 4  session-start why, touched-path checking, candidate capture
```

Two things the spec asks for are answered without dedicated code.

`hub.check:memory` (§24) needs none: the check abstraction already carries an
arbitrary name, so a CI process holding `hub.check:memory` reports the exit
status of `git+ note check --base <B> --head <H>` under whatever name
`requiredChecks` lists.

The mutation endpoints §32 sketches (`POST /:repo/notes`, `.../confirm`, …) are
served by the one door the hub already has, `POST /:repo/hub/events`, which now
decodes note events. The server holds no member's key and so cannot author on
anybody's behalf; a note lifecycle event is signed wherever the key lives and
this endpoint is the transport for the result. Read endpoints follow the
`/:repo/hub/*` prefix the rest of the API uses:

```text
GET /:repo/hub/notes?path=&cursor=&limit=
GET /:repo/hub/notes/:id
GET /:repo/hub/notes/check?path=&base=&head=
GET /:repo/hub/why?path=&head=
```

### Anchor resolver coverage

Languages sit in one of three tiers, chosen by what can be read _reliably_
rather than by what a pattern can be made to match — §13's `unverifiable` is a
safe answer and a wrong anchor is not.

```text
structured  TypeScript/JavaScript, Rust, Go, Python, shell, Markdown,
            Vue/Svelte blocks (#script, #style, #template)
normalized  Java, Kotlin, Scala, Swift, C#, C, C++, Objective-C
            @file normalizes past comments; symbol anchors are Unsupported
opaque      everything else — @file is byte-exact
```

The middle tier is deliberate: C-family comment syntax is unambiguous, so
§11's "ignore formatting and comments" is honest for a whole file, while its
declarations are not something a regular expression reads correctly.

Within the structured tier, a declaration's body is found past the braces its
_signature_ holds — a parameter's object type, a generic constraint, an object
return type — because taking the first brace anchored a note to a fragment of
the signature, and a region the implementation is not in reports `fresh`
however the implementation is rewritten. That is §15's forbidden outcome
reached through the resolver rather than through the audit, and it is what the
`Anchor.languages` suite exists to keep shut.

### Deliberately not built

`task-aware why` (§35 phase 4) has no implementation because it has no agreed
meaning yet: a task names refs and pull requests rather than paths, so what a
task-scoped `why` should return is a design question rather than a missing
function.

## Phase 1 — file-level notes

Implement:

```text
note event DAG
@file anchor
note add
note check
note confirm
note replace
note retire
why
JSON output
```

No semantic parser required.

## Phase 2 — semantic anchors

Add:

```text
AnchorResolver
TypeScript/JavaScript
Markdown
anchors command
signature/content fingerprints
```

## Phase 3 — policy integration

Add:

```text
PR-scoped note checks
hub.check:memory
requiredChecks integration
```

## Phase 4 — deeper agent integration

Add:

```text
session-start why injection
task-aware why
automatic touched-path checking
candidate constraint capture from session/commit workflows
```

---

# 36. UX Principle

The repository should answer two questions before an agent begins work:

```text
git+ hub whoami
→ What am I allowed to do?

git+ why <path>
→ What must I know before changing this?
```

After the work:

```text
git+ note check
→ Did I invalidate anything somebody previously said must remain true?
```

And if so, the system requires a judgment:

```text
confirm
replace
retire
```

rather than silently converting changed code into a new truth.

That explicit judgment boundary is the core invariant of the feature.
