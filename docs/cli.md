# Git+ CLI

This document explains the CLI conventions and the common workflows that span Git+'s **Work**, **Knowledge**, and **Audit** planes.

Detailed flag syntax belongs to the executable itself:

```bash
git+ --help
git+ <command> --help
git+ <command> <subcommand> --help
```

The top-level help snapshot in this file is generated from the CLI with:

```bash
npm run docs:cli
```

Do not maintain a second hand-written flag reference here.

---

## 1. Product model

```text
WORK
  tasks / PRs / sessions / decisions

KNOWLEDGE
  .gitplus/knowledge
  Repository Memory

AUDIT
  Invocations
  repository context
  runtime telemetry
```

All three use the same Git object store, repository identity, signed membership, and capability model.

Users normally work with sessions and Invocations. Lower-level Context Exposure and trace records remain available when auditing protocol details but are not the primary product vocabulary.

---

## 2. Repository discovery

Repo-scoped commands follow normal Git ergonomics:

```bash
cd project

git+ session show --branch=HEAD --audit
git+ context for --task="fix authentication policy"
git+ knowledge check
```

The current checkout is the default repository context.

Explicit root/repository selection remains available for bare repositories, servers, automation, and administration.

Human-facing arguments MAY use normal Git spelling:

```text
HEAD
main
refs/heads/main
abc123
```

Commands resolve unambiguous revisions and abbreviated OIDs. Canonical serialized records continue to use algorithm-qualified OIDs such as `sha1:<hex>` or `sha256:<hex>`.

---

## 3. Global behavior

The CLI is invoked as:

```text
git+ [global-options] <command> [subcommand] [arguments]
```

General conventions:

- human-readable output is the default;
- `--json` returns structured output for harnesses and scripts;
- diagnostics go to stderr so stdout remains parseable;
- commands that create durable records print a stable identifier suitable for later commands;
- missing telemetry/usage is represented as unknown or absent, never silently as zero;
- content relevance never grants repository authority;
- causal record joins use Git identity, not timestamp proximity.

---

## 4. Generated top-level help

The block below is replaced by `npm run docs:cli` from the executable's actual `--help` output.

<!-- BEGIN GENERATED CLI HELP -->
```text
git+ --help

Core Git
  init clone fetch pull push
  add rm mv restore status switch commit
  log history show diff grep files bisect
  branch tag refs reflog reset
  merge cherry-pick rebase
  archive fsck gc

Collaboration / Work
  hub id credential credential-helper
  pr task queue session wake
  social remote webhook serve

Knowledge / Audit
  context knowledge

Harness plumbing
  trace
```
<!-- END GENERATED CLI HELP -->

---

## 5. Work

### 5.1 Tasks

Tasks coordinate work through signed task refs. Claims are leases and advisory rather than locks.

Common flow:

```bash
git+ task list
git+ task claim <task> --ttl=15m
# work
git+ task close <task> --commit=HEAD
```

The exact task verbs are available through `git+ task --help`.

### 5.2 Sessions

Sessions retain the distilled work record:

```text
what the agent was asked
what decisions were requested/resolved
what the session produced
compact reusable learning
aggregate usage when available
```

Typical harness flow:

```bash
session=$(git+ session open \
  --key ~/.ssh/id_ed25519 \
  --agent claude-code \
  --model model-x \
  --prompt "Fix auth policy")

# work

git+ session produce \
  --key ~/.ssh/id_ed25519 \
  --session "$session" \
  --commit HEAD \
  --note "Worker auth tests require the production policy fixture"
```

A person normally inspects a session by ID or by the branch it produced:

```bash
git+ session show "$session"
git+ session show --branch=feature/auth
```

### 5.3 Session audit

The normal audit entry point is the session rather than the raw trace namespace:

```bash
git+ session show "$session" --audit
git+ session show --branch=feature/auth --audit
```

`--audit` joins the policy-visible session record with its policy-invisible Invocation history.

The projected output contains user-level Invocations such as:

```text
Invocation abc123

Context
  tree      79ad…
  evidence  7 blobs · 1 gitlink
  render    ✓ verified

Runtime
  chat · anthropic / model-x
  118k input · 4.2k output
  finish stop

Workspace
  79ad… → a130…

Capture
  OTel GenAI · complete
```

Users do not need to understand the separate pre-call Context Exposure and post-call Invocation Telemetry records unless debugging the audit protocol itself.

### 5.4 Decisions

When an agent reaches a question requiring human judgement:

```bash
git+ session ask \
  --session "$session" \
  --question "Which compatibility behavior should remain?" \
  --option="strict,legacy"

git+ session answer \
  --session "$session" \
  --decision <decision-id> \
  --chose strict
```

The answer becomes signed causal provenance rather than an ephemeral chat message.

---

## 6. Knowledge

The knowledge corpus is ordinary repository content under the configured bundle, conventionally:

```text
.gitplus/knowledge/
```

It is directly OKF-compatible Markdown/YAML with optional stronger Git+ provenance under `gitplus:` frontmatter.

Normal file and Git operations remain the primary UX:

```bash
$EDITOR .gitplus/knowledge/gotchas/worker-auth.md
git diff .gitplus/knowledge
git log -- .gitplus/knowledge/gotchas/worker-auth.md
```

There is intentionally no required Git+-specific list/show/import/export layer. The directory already is the portable interchange artifact.

### 6.1 Verify knowledge

Git+ adds the operation ordinary file tools cannot perform:

```bash
# Check the entire bundle
git+ knowledge check

# Check one Concept
git+ knowledge check gotchas/worker-auth
```

A check reports separate dimensions:

```text
OKF structure
status / stale_after
signed Git+ citations
blob dependency state
gitlink dependency state
external captured-source state
signed verification provenance
```

Changed evidence means **revalidate**, not automatically **false**.

Portable OKF `verified` metadata is an editorial signal and never grants Git+ membership, capability, review authority, or instruction authority.

### 6.2 Repository Memory

Repository Memory remains the bounded projection suitable for every session start:

```bash
# Read current Memory
git+ session memory

# Rebuild from durable provenance / current knowledge first
git+ session memory --distill
```

Memory is a cache. Eviction does not delete Knowledge Concepts or their signed source records.

---

## 7. Audit: repository context

Context commands expose the Git-native repository-context protocol in [context-pack.md](context-pack.md).

The normal surface is deliberately small:

```text
git+ context for --task <text>
git+ context why <pack> [item]
git+ context audit <invocation-or-exposure>
```

### 7.1 Build context

```bash
git+ context for --task="fix authentication policy"
```

The output identifies one Repository View and typed evidence:

```text
blob
  path + blob OID + optional byte range

gitlink
  path + mode 160000 + submodule commit OID
```

Retrieval scores, indexes, or graph paths may be displayed as diagnostics but are not evidence identity.

### 7.2 Explain selection

```bash
git+ context why <pack> src/auth.ts
```

The command distinguishes verified Git evidence from descriptive selector explanations.

### 7.3 Audit exposure

```bash
git+ context audit <invocation-or-exposure>
```

The audit verifies independently:

```text
signed trace record
pack blob identity
retained view.tree
blob path/OID/range
mode-160000 gitlinks
ContextRender digest when retained
semantic segment placements
instruction-provenance annotations
bound Invocation
optional OTel correlation
```

A missing retained render body can coexist with a valid historical render commitment.

---

## 8. Harness telemetry

Harness-native OpenTelemetry GenAI is the preferred runtime capture path. Normal users consume the resulting Invocations through:

```bash
git+ session show --audit
```

Raw trace writing is harness plumbing for integrations without suitable native OTel:

```bash
git+ trace record \
  --session <session-id> \
  --key <private-key> \
  --event <event.json>
```

The recorder binds the repository/session, validates the normalized event, signs it, appends it under `refs/hub/trace/<session>`, and returns the Git record OID.

There is intentionally no normal `trace show` workflow; the session/Invocation projection is the human-facing read path.

See [telemetry.md](telemetry.md) for GenAI semantic-convention mapping, attempts, capture coverage, trace health, and retention rules.

---

## 9. JSON output

Machine consumers SHOULD prefer `--json` rather than parsing formatted terminal output.

Structured responses preserve distinctions such as:

```text
observed vs reported vs derived
known vs unknown
current vs stale
complete vs partial capture
portable editorial verification vs signed Git+ provenance
```

Unknown fields should be omitted or represented explicitly according to the command schema; they must not be fabricated as zero/false.

---

## 10. Command-documentation rule

The executable is the source of truth for syntax.

When a command definition changes:

```bash
npm run docs:cli
```

updates the generated top-level help snapshot in this document. Human-authored sections explain workflows and concepts rather than duplicating every flag table.

This keeps command discovery local and self-documenting while preventing a second manually maintained CLI schema from drifting away from the Effect `Command` definitions.
