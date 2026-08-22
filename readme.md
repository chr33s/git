> [!WARNING]  
> Pre-1.0 API: unstable and not recommended for production use.

# @chr33s/git

> **Universal AI-native Git smart-HTTP server, browser client & Unix CLI**  
> Built on Effect v4 with modern TypeScript and Web APIs.

Git+ runs the same Git core in a Cloudflare Durable Object, a Node.js process, a browser tab, or a terminal. Standard Git clients can clone and push normally; Git+ adds collaboration, agent provenance, durable repository knowledge, and auditable agent invocations without making a hosting provider the system of record.

---

## The model

Git+ adds three planes to ordinary Git history:

```text
WORK
  tasks / pull requests / sessions / decisions

KNOWLEDGE
  OKF-compatible repository knowledge
  bounded Repository Memory

AUDIT
  agent Invocations
  exact repository context
  runtime telemetry
```

All three use the same Git object store, repository identity, signed membership, and capability model.

### Why Git-native?

- **The project fetches with the repository.** Identity, collaboration, sessions, audit records, and knowledge can replicate with Git rather than depending on a provider API.
- **Agent work has durable provenance.** Signed sessions record what an agent was asked, decisions it requested, and what repository state it produced.
- **Repository context is auditable.** Context Packs bind selected blobs/gitlinks to one exact Git tree; ContextRender commits to the ordered semantic segments that crossed the invocation boundary.
- **Runtime telemetry remains separate from policy.** OpenTelemetry GenAI signals are normalized into signed `refs/hub/trace/*` audit records without putting high-cardinality telemetry in protected-branch policy folds.
- **Knowledge survives sessions.** `.gitplus/knowledge/` is an OKF-compatible Markdown corpus with optional Git+ signed citations and structured Git evidence; Repository Memory remains a small regenerable session-start projection.
- **Authority remains explicit.** Retrieved content, knowledge, OTel attributes, and editorial verification are data unless repository policy independently grants authority.

---

## Quick start

Requires **Node.js >=24.12.0** or a compatible Web API runtime.

```bash
# Run directly
npx @chr33s/git --help

# Or install globally
npm install -g @chr33s/git
```

### Run a server

```bash
GIT_ROOT=./repos git+ serve --port=8080
```

Standard Git clients work against Smart-HTTP:

```bash
git clone http://127.0.0.1:8080/my-repo.git
```

For local scratch repositories that intentionally allow unguarded writes:

```bash
GIT_ROOT=./repos git+ serve --port=8080 --open
```

---

## Work

Git+ keeps collaboration in signed Git-native records.

Common command families include:

```text
hub / id / credential
pr / task / queue
session / wake
```

A typical agent session is opened by the harness, produces normal Git commits, and closes with a compact durable result:

```bash
session=$(git+ session open \
  --key ~/.ssh/id_ed25519 \
  --agent claude-code \
  --model model-x \
  --prompt="Fix authentication policy")

# edit / test / commit

git+ session produce \
  --key ~/.ssh/id_ed25519 \
  --session "$session" \
  --commit HEAD \
  --note="Worker auth tests require the production policy fixture"
```

Inspect the resulting work by session or branch:

```bash
git+ session show "$session"
git+ session show --branch=feature/auth
```

Human-only judgement can be recorded as signed decisions with `git+ session ask` / `answer` rather than disappearing into chat.

See [`docs/agents.md`](docs/agents.md) for the collaboration/session model.

---

## Knowledge

Repository knowledge is ordinary versioned content, conventionally:

```text
.gitplus/knowledge/
  index.md
  architecture/
  conventions/
  gotchas/
  playbooks/
  decisions/
```

The directory is directly **Open Knowledge Format (OKF)** compatible Markdown/YAML. Git+ does not require an import/export translation layer; clone or copy the directory and it remains portable.

Git+ adds stronger repository provenance under a namespaced `gitplus:` frontmatter extension:

```text
signed record citations
blob/gitlink evidence dependencies
external content digests/snapshots
signed verification records
```

Normal Git/filesystem operations are the primary editing and browsing UX:

```bash
$EDITOR .gitplus/knowledge/gotchas/worker-auth.md
git diff .gitplus/knowledge
git log -- .gitplus/knowledge/gotchas/worker-auth.md
```

Git+ adds verification:

```bash
# Check the whole corpus
git+ knowledge check

# Check one Concept
git+ knowledge check gotchas/worker-auth
```

Repository, temporal (`stale_after`), and external-source freshness are reported separately. Changed evidence means **revalidate**, not automatically **false**.

Repository Memory remains the small projection suitable for every session start:

```bash
git+ session memory
git+ session memory --distill
```

Knowledge and Memory are data, not instruction authority.

See [`docs/knowledge-durability.md`](docs/knowledge-durability.md).

---

## Audit

The user-facing audit object is an **Invocation**.

Internally Git+ keeps the pre-call Context Exposure and post-call runtime record separate, but the CLI/UI projects them together:

```text
Invocation
  Context
    exact tree
    selected blobs/gitlinks
    ContextRender verification

  Runtime
    provider / requested + response model
    usage / finish / error
    attempts when explicitly observed

  Operations
    tools / workspace / context lifecycle

  Capture
    OTel correlation / coverage / known loss
```

### Audit a session

```bash
git+ session show "$session" --audit
git+ session show --branch=feature/auth --audit
```

Harness-native OpenTelemetry GenAI is the preferred runtime input. OTel IDs remain correlation metadata; signed Git record OIDs remain durable audit identity. Known sampling, transformation, or exporter loss weakens coverage claims instead of being hidden.

### Inspect repository context

```bash
# Build context for a task
git+ context for --task="fix authentication policy"

# Explain why evidence was selected
git+ context why <pack> src/auth.ts

# Verify one historical invocation/exposure
git+ context audit <invocation-or-exposure>
```

Context Packs distinguish ordinary blobs from mode-160000 gitlinks and resolve both against one exact Repository View. A durable Context Exposure retains `view.tree` through a real Git tree edge, not only an OID written in JSON.

ContextRender uses one exact V1 binary framing that binds segment order, logical placement, media type, and exact body bytes.

See [`docs/context-pack.md`](docs/context-pack.md) and [`docs/telemetry.md`](docs/telemetry.md).

---

## CLI

The CLI uses the same repository engine as the server.

```text
git+ [global-options] <command> [subcommand] [arguments]
```

Detailed syntax is self-documenting:

```bash
git+ --help
git+ session --help
git+ context --help
```

Repo-scoped commands default to the current checkout. Human-facing arguments accept normal Git revisions and unambiguous abbreviated OIDs; canonical stored records use qualified Git OIDs.

[`docs/cli.md`](docs/cli.md) contains workflow conventions. Its top-level command snapshot is generated from the executable with `npm run docs:cli` instead of maintaining a second hand-written flag schema.

---

## Web interface

The UI in `src/ui/` provides repository browsing, commits, tasks, pull requests, sessions, knowledge state, and the agent Flight Recorder.

The Flight Recorder is session-centric and presents Invocations rather than requiring users to reconstruct Context Exposure and telemetry records themselves. Concurrent trace branches remain visible as causal lanes rather than being flattened by timestamp.

```bash
npm run build:ui
git+ serve --root ./repos --ui
```

For development:

```bash
GIT_ROOT=./repos npm run dev:ui
```

---

## Development

```bash
npm install
npm run check
npm run test
```

Regenerate the CLI help snapshot after command-surface changes:

```bash
npm run docs:cli
```

Documentation map: [`docs/README.md`](docs/README.md).
