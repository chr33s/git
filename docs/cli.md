# CLI Reference & Design Guidelines

This document defines the current CLI surface for `@chr33s/git` (Universal Git smart-HTTP protocol server, browser client & Unix CLI built on Effect v4 and Web APIs).

The CLI uses the same repository engine as the server and browser-facing APIs. Commands operate on ordinary Git state plus Git+'s signed collaboration, session, context, telemetry, and knowledge layers.

---

## 1. Design & Authoring Principles

### Developer-first execution

- Commands are runnable locally without requiring a hosting-provider API.
- Human-readable output is the default; `--json` provides stable structured output for harnesses and scripts.
- Commands that create durable provenance print stable identifiers that later commands can consume.

### Git-native identity

- Git object and record OIDs are used when immutable identity matters.
- Human-facing session, decision, task, or concept IDs remain convenient names rather than substitutes for content identity.
- Context, telemetry, and knowledge commands preserve the distinction between Git-native evidence and descriptive external metadata.

### Explicit authority and evidence

- Capability checks remain separate from content relevance.
- Knowledge, retrieved context, OTel attributes, and imported metadata are data unless repository policy independently grants authority.
- Output distinguishes signed, observed, reported, derived, stale, partial, and unknown states rather than collapsing them into one confidence value.

### Causal rather than timestamp joins

- Session, task, PR, and trace histories preserve their DAG structure.
- Context Exposure and logical invocation records join by signed Git record identity.
- Commands do not infer causal relationships merely because two records have nearby timestamps.

---

## 2. Global Architecture & Options

The `@chr33s/git` CLI is invoked as `git+` or through `npx @chr33s/git`:

```text
git+ [global-flags] <command> [subcommand] [arguments] [command-flags]
```

### Global Flags & Options

| Flag        | Short | Type      | Default | Description                                                         |
| :---------- | :---- | :-------- | :------ | :------------------------------------------------------------------ |
| `--help`    | `-h`  | `boolean` | `false` | Display help and usage information for a command or subcommand.     |
| `--version` | `-v`  | `boolean` | `false` | Output the CLI engine version.                                      |
| `--json`    |       | `boolean` | `false` | Return structured JSON to `stdout` for programmatic parsing.        |
| `--quiet`   | `-q`  | `boolean` | `false` | Suppress non-essential informational output.                        |
| `--verbose` |       | `boolean` | `false` | Output detailed Effect trace logs and HTTP protocol frames.         |
| `--config`  | `-c`  | `string`  | `""`    | Path to custom repository/hub configuration.                        |

### Environment Variables

| Variable         | Description                                                                           |
| :--------------- | :------------------------------------------------------------------------------------ |
| `GIT_ROOT`       | Base directory for repository storage when running server or local hub operations.   |
| `GIT_AUTH_TOKEN` | Bearer/capability token for authenticated remote Git+ endpoints.                      |
| `SSH_AUTH_SOCK`  | Active SSH agent socket used for signing membership, review, session, and trace data. |

---

## 3. Core Commands

### `git+ init`

Initialize a new Git repository or convert an existing repository into a cryptographic hub.

```bash
git+ init [path] [--genesis]
```

| Flag        | Short | Type      | Default | Required | Description                                                                      |
| :---------- | :---- | :-------- | :------ | :------- | :------------------------------------------------------------------------------- |
| `--genesis` | `-g`  | `boolean` | `false` | No       | Create `refs/meta/trust/genesis` guarded by the active SSH membership key.       |
| `--bare`    |       | `boolean` | `false` | No       | Create a bare repository without a working directory.                            |

```bash
git+ init my-repo
git+ init my-secured-repo --genesis
```

### `git+ serve`

Start the embedded Smart-HTTP server, JSON API, and optional web interface.

```bash
git+ serve [--port=<port>] [--host=<host>] [--open]
```

| Flag     | Short | Type      | Default       | Required | Description                                                                          |
| :------- | :---- | :-------- | :------------ | :------- | :----------------------------------------------------------------------------------- |
| `--port` | `-p`  | `number`  | `8080`        | No       | Port to bind.                                                                        |
| `--host` | `-H`  | `string`  | `"127.0.0.1"` | No       | Network host to bind.                                                               |
| `--open` | `-o`  | `boolean` | `false`       | No       | Permit writes to unguarded repositories, primarily for local scratch use.           |

```bash
GIT_ROOT=./repos git+ serve --port=8080 --open
```

### `git+ clone`

Clone a repository over Smart-HTTP.

```bash
git+ clone <url> [destination]
```

```bash
git+ clone http://127.0.0.1:8080/my-repo ./my-copy
```

The remaining core Git-style commands (`add`, `status`, `commit`, `log`, `diff`, `branch`, `merge`, `rebase`, `push`, `fsck`, `gc`, and related porcelain/plumbing) use the same repository engine and ordinary Git object model.

---

## 4. Collaboration, Identity & Sessions

### Pull requests (`git+ pr`)

Pull requests are append-only signed event DAGs stored under `refs/meta/pull-requests/`.

```bash
git+ pr list [--state=<open|closed|all>]
git+ pr create --title=<title> --target=<branch> [--body=<body>]
git+ pr review <pr-id> --status=<approve|reject|comment> [--message=<text>]
git+ pr merge <pr-id> [--strategy=<squash|rebase|merge>]
```

Relevant capabilities include `hub:review`, `hub:check`, and `hub:merge`.

```bash
git+ pr create --title="feat: Add WebCrypto signature verification" --target=main
git+ pr review 42 --status=approve --message="Verified Effect v4 schema compatibility"
git+ pr merge 42 --strategy=rebase
```

### Capability credentials (`git+ credential`)

Capability-scoped credentials avoid long-lived bearer authority.

```bash
git+ credential mint --capability=<cap> [--ttl=<duration>] [--repo=<repo-id>]
git+ credential verify --token=<token>
git+ credential revoke --key-id=<ssh-key-fingerprint>
```

```bash
git+ credential mint --capability="hub:check:test" --ttl="30m"
git+ credential revoke --key-id="SHA256:abc123xyz789..."
```

### Tasks (`git+ task`)

Tasks coordinate agent work through signed task refs. Claims are advisory leases rather than locks.

Common operations include:

```text
open
claim
release
close
reopen
reparent
redact
list
show
```

Example:

```bash
git+ task claim --id="task-102" --ttl="15m"
```

### Sessions (`git+ session`)

Sessions record what an agent was instructed and what durable repository result the session produced.

Current subcommands are:

```text
open
produce
show
ask
answer
redact
enable
memory
```

Examples:

```bash
# Open a signed session record
git+ session open \
  --key ~/.ssh/id_ed25519 \
  --agent claude-code \
  --model model-x \
  --prompt "Fix auth policy" \
  my-repo

# Record what the session produced
git+ session produce \
  --key ~/.ssh/id_ed25519 \
  --session 0198f2aa... \
  --commit abc123 \
  --ref refs/heads/feature/auth \
  --note "Auth policy fixture is required by worker tests" \
  my-repo

# Rehydrate the session that most recently produced a branch
git+ session show --branch=refs/heads/feature/auth my-repo
```

#### Repository Memory (`git+ session memory`)

Repository Memory is a bounded, regenerable projection of cited reusable knowledge. It is data, not instruction authority.

```bash
# Read the current projection
git+ session memory my-repo

# Rebuild it from durable session provenance first
git+ session memory --distill my-repo
```

Memory eviction does not delete Durable Knowledge Concepts or signed source records.

---

## 5. Repository Context & Exposure (`git+ context`)

Context commands expose the Git-native context provenance defined in [`context-pack.md`](context-pack.md).

A Context Pack identifies one effective Repository View and typed evidence:

```text
blob
  view.tree + path → blob OID
  optional half-open byte range

gitlink
  view.tree + path → mode 160000 → submodule commit OID
```

A Context Exposure additionally commits to ContextRender, whose digest binds segment ordering, semantic placement, media type, and exact bytes.

### `git+ context for`

Build a Context Pack for a task using the current retrieval implementation.

```bash
git+ context for --task=<text>
```

Example:

```bash
git+ context for --task="fix authentication policy"
```

Human-readable output summarizes selected repository evidence. With global `--json`, the command returns the Repository View, selected typed items, selector metadata, and privacy-safe omission diagnostics.

The command does not claim that retrieval is deterministic and does not grant instruction authority to selected content.

### `git+ context why`

Explain a persisted Context Pack or one selected item.

```bash
git+ context why <pack-oid> [item]
```

Examples:

```bash
git+ context why sha256:8d7ad4...
git+ context why sha256:8d7ad4... src/auth.ts
```

For blobs, output includes path, blob OID, range when present, Repository View, and recorded selection reason. For gitlinks, output includes the mode-160000 submodule commit pointer and explicitly does not imply that submodule contents were exposed.

### `git+ context audit`

Verify a historical Context Exposure or a logical invocation bound to one.

```bash
git+ context audit <operation-or-trace-record>
```

The audit reports independent dimensions:

```text
signed trace-record validity
Context Pack blob identity
retained view.tree identity
blob path/OID/range verification
gitlink mode/commit verification
ContextRender digest verification when retained
semantic segment placements
instruction-provenance annotation validity
privacy-safe omissions
bound logical invocation
OTel correlation when present
```

A missing render body can coexist with a valid historical render digest. Missing runtime telemetry does not invalidate otherwise valid Git evidence.

---

## 6. Runtime Audit Trace (`git+ trace`)

Detailed runtime provenance is stored separately from the policy-critical session DAG:

```text
refs/hub/session/<session>
  distilled lifecycle / decisions / produced result

refs/hub/trace/<session>
  Context Exposure / logical invocations / selected tools / lifecycle / workspace / trace health
```

The trace is signed and policy-invisible: authorization and protected-branch checks do not fold high-cardinality telemetry.

Harness-native OpenTelemetry GenAI is the preferred runtime input. The normalizer interprets the declared upstream semantic-convention revision without changing its meaning, then persists stable Git+ audit fields.

### `git+ trace show`

Display the durable trace joined to a session.

```bash
git+ trace show <session-id>
```

Output preserves causal DAG structure and includes available Context Exposure, logical invocation, tool, workspace, lifecycle, and trace-health records.

OTel `TraceId`/`SpanId` values are displayed as correlation metadata. Signed Git record commit OIDs are the durable cross-record identity.

### `git+ trace record`

Record a normalized trace event directly. This is the fallback ingestion path for harnesses that do not expose suitable native OTel.

```bash
git+ trace record <repo> \
  --session <session-id> \
  --key <private-key> \
  --event <event.json>
```

The recorder:

```text
validates the normalized event
applies secret/retention handling
binds repository + session identity
signs the record
appends under refs/hub/trace/<session>
prints the resulting qualified Git record OID
```

Example:

```bash
git+ trace record my-repo \
  --session 0198f2aa... \
  --key ~/.ssh/id_ed25519 \
  --event invocation.json
```

### OTel semantics visible through the CLI

For compliant GenAI telemetry:

- one inference span becomes one logical invocation;
- automatic provider retries remain subordinate attempts unless separately exposed by instrumentation;
- `gen_ai.request.model` and `gen_ai.response.model` remain distinct;
- generation finish reasons remain separate from span errors;
- provider token usage remains provider-reported even when transported by OTel;
- `gen_ai.conversation.compacted` marks compacted input but does not fabricate a separate compaction transition;
- retrieval spans are diagnostics, not proof that retrieved material entered model context.

Trace coverage is reported as available/partial/unknown according to capture capability, sampling/transformation state, and known exporter loss. Absence of an event is never silently treated as proof that the event did not occur.

---

## 7. Durable Knowledge (`git+ knowledge`)

Durable Knowledge Concepts are Markdown/YAML files in ordinary source history, conventionally under `.gitplus/knowledge/`.

They are the curated knowledge corpus; Repository Memory remains the small projection suitable for every session start.

A Concept can carry:

```text
type / title / description / tags
lifecycle: draft | stable | deprecated
generated / verified metadata
stale_after temporal freshness
stable source IDs and per-claim footnotes
signed Git+ record citations
structured blob/gitlink evidence dependencies
external-source capture digest/snapshot metadata
```

Knowledge is data. Neither a local Concept nor imported OKF metadata grants instruction authority, membership, or capabilities.

### `git+ knowledge list`

List Concepts in the configured knowledge bundle.

```bash
git+ knowledge list [path] [--status=<draft|stable|deprecated|all>]
```

Examples:

```bash
git+ knowledge list
git+ knowledge list architecture --status=stable
```

Generated directory indexes are used for progressive disclosure when available; the command remains able to scan Concept frontmatter directly.

### `git+ knowledge show`

Show one Concept with its portable metadata and Git+ provenance projection.

```bash
git+ knowledge show <concept-id>
```

Example:

```bash
git+ knowledge show gotchas/worker-auth
```

The display separates editorial metadata (`generated`, `verified`, lifecycle) from signed Git+ citations and mechanically checkable evidence.

### `git+ knowledge check`

Revalidate a Concept against repository evidence, temporal freshness, and available external-source capture metadata.

```bash
git+ knowledge check <concept-id> [--ref=<commit-ish>]
```

Example:

```bash
git+ knowledge check gotchas/worker-auth --ref=HEAD
```

Structured repository dependencies are classified independently:

```text
unchanged
changed
missing
unknown
```

A changed/missing dependency or expired `stale_after` means **revalidate**; it does not automatically mean the prose is false.

### `git+ knowledge index`

Regenerate progressive-disclosure `index.md` files from Concept metadata.

```bash
git+ knowledge index [path]
```

Indexes are derived retrieval accelerators rather than authority or truth sources.

### `git+ knowledge export`

Export the knowledge bundle through an interoperability format.

```bash
git+ knowledge export --format=okf [path]
```

Example:

```bash
git+ knowledge export --format=okf .gitplus/knowledge
```

Git+-specific citations, evidence dependencies, and verification record IDs are preserved in namespaced extension metadata while the portable Markdown/YAML remains readable by generic OKF consumers.

### `git+ knowledge import`

Import an OKF knowledge bundle into the repository knowledge corpus.

```bash
git+ knowledge import --format=okf <path>
```

Example:

```bash
git+ knowledge import --format=okf ./external-knowledge
```

Import preserves unknown portable metadata where practical, but imported actor strings, `verified` fields, trust tiers, or source URLs never become Git+ authority. Repository trust still comes from signed identities, capabilities, policy, and reviewed Git history.

---

## 8. End-to-End Agent Audit Example

A normal agent workflow joins the command families without collapsing their guarantees:

```bash
# 1. Open or let harness hooks open the signed session
SESSION=$(git+ session open \
  --key ~/.ssh/id_ed25519 \
  --agent claude-code \
  --prompt "Fix auth policy" \
  my-repo)

# 2. Build the current Git-grounded repository context
git+ context for --task="fix auth policy"

# 3. Native OTel records logical inference/tool activity into refs/hub/trace/$SESSION
#    (trace record is used only for fallback integrations)

# 4. Inspect what was actually retained for audit
git+ trace show "$SESSION"
git+ context audit sha1:context-exposure-record...

# 5. Record the durable result
git+ session produce \
  --key ~/.ssh/id_ed25519 \
  --session "$SESSION" \
  --commit abc123 \
  --note "Worker auth tests require production policy fixture" \
  my-repo

# 6. Publish/check reusable repository knowledge
git+ knowledge check gotchas/worker-auth
git+ session memory --distill my-repo
```

The important separation is:

```text
Session             what work was requested/produced
Knowledge Concept   what reusable thing the repository claims to know
Context Pack        what Git-grounded evidence was selected
Context Exposure    what semantically framed repository context crossed the boundary
Telemetry           under what observable runtime conditions it happened
```

---

## 9. Structured Output & Errors

Commands support global `--json`. Structured output preserves the same evidence distinctions shown in human-readable output.

### Example context-audit result

```json
{
  "success": true,
  "data": {
    "record": "sha1:abc123...",
    "pack": "sha256:8d7ad4...",
    "viewTree": "sha256:def456...",
    "repositoryEvidence": "verified",
    "render": "verified",
    "runtimeCoverage": "partial",
    "otel": {
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "spanId": "00f067aa0ba902b7"
    }
  }
}
```

### Example knowledge-check result

```json
{
  "success": true,
  "data": {
    "concept": "gotchas/worker-auth",
    "lifecycle": "stable",
    "temporalFreshness": "current",
    "evidence": [
      { "path": "tests/worker/auth.test.ts", "state": "unchanged" },
      { "path": "config/policy.json", "state": "changed" }
    ],
    "needsRevalidation": true
  }
}
```

### Error Response Format & Exit Codes

| Exit Code | Meaning             | Description                                                    |
| :-------- | :------------------ | :------------------------------------------------------------- |
| `0`       | Success             | Command executed without errors.                               |
| `1`       | General Failure     | Invalid invocation, parse error, missing object, or bad input. |
| `2`       | Auth / Scope Denied | Signing key/capability is invalid for the requested operation. |
| `3`       | DAG CAS Conflict    | Compare-and-swap failed because a target ref moved.            |

```json
{
  "success": false,
  "error": {
    "code": "AUTH_SCOPE_DENIED",
    "exitCode": 2,
    "message": "Key 'SHA256:...' lacks capability 'hub:merge' for target branch 'main'.",
    "hint": "Mint a token with 'hub:merge' or request an authorized signer."
  }
}
```

---

## 10. Related Specifications

- [`agents.md`](agents.md) — membership, sessions, decisions, tasks, and Repository Memory
- [`context-pack.md`](context-pack.md) — Repository View, Context Pack, ContextRender, Context Exposure, and Git reachability
- [`telemetry.md`](telemetry.md) — OpenTelemetry GenAI ingestion, signed trace storage, runtime semantics, API projection, and Flight Recorder
- [`knowledge-durability.md`](knowledge-durability.md) — Durable Knowledge Concepts, structured evidence, freshness, Memory, and OKF interoperability
- [`hub.md`](hub.md) — collaboration refs, trust, policy, pull requests, and server behavior
