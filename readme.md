> [!WARNING]  
> Pre-1.0 API: unstable and not recommended for production use.

# @chr33s/git

> **Universal AI-native Git smart-HTTP server, browser client & Unix CLI**  
> Built on Effect v4 with modern TypeScript and Web APIs.

Git+ makes the repository the system of record for source, collaboration, agent work, reusable knowledge, and invocation audit. The same Git core runs in a Cloudflare Durable Object, a Node.js process, a browser tab, or a terminal. Standard Git clients still clone and push over Smart-HTTP.

---

## What Git+ adds

Git+ puts three planes beside ordinary Git history:

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

They share the same Git object store, repository identity, signed membership, and capability model.

- Identity, collaboration, sessions, audit records, and knowledge replicate with the repository. A hosting provider database is not the source of truth.
- Signed sessions record what an agent was asked, which decisions it requested, and which repository state it produced.
- Context Packs bind selected blobs and gitlinks to one exact Git tree. ContextRender commits to the ordered semantic segments that crossed the invocation boundary.
- OpenTelemetry GenAI signals are normalized into signed `refs/hub/trace/*` audit records. High-cardinality telemetry stays out of protected-branch policy folds.
- `.gitplus/knowledge/` is an OKF-compatible Markdown corpus with optional Git+ signed citations and structured Git evidence. Repository Memory is a small, regenerable session-start projection.
- Retrieved content, knowledge, OTel attributes, and editorial verification remain data unless repository policy grants them authority.

---

## Quick start

This repository is currently **not published to npm** (`package.json` is private).
Install [mise](https://mise.jdx.dev/) and use the checkout instead. Node 26 is the
blessed development and binary-build version; the source compatibility floor is
Node 24.12.0.

```bash
git clone https://github.com/chr33s/git.git
cd git
mise install
npm ci                 # installation does not rewrite the checkout
```

### Run a server

```bash
GIT_ROOT=./repos git+ serve --port=8080
```

Standard Git clients work against Smart-HTTP:

```bash
git clone http://127.0.0.1:8080/my-repo.git
```

For a local scratch repository that intentionally allows unguarded writes:

```bash
GIT_ROOT=./repos git+ serve --port=8080 --open
```

---

## Work

Git+ stores collaboration as signed Git-native records. Common command families are:

```text
hub / id / credential
pr / task / queue
session / wake
```

A harness opens a session, the agent produces ordinary Git commits, and the harness records the durable result:

```bash
session=$(git+ session open \
  --key="$HOME/.ssh/id_ed25519" \
  --agent=claude-code \
  --model=model-x \
  --prompt="Fix authentication policy")

# edit / test / commit

git+ session produce \
  --key="$HOME/.ssh/id_ed25519" \
  --session="$session" \
  --commit=HEAD \
  --note="Worker auth tests require the production policy fixture"
```

Inspect the work by session or by the branch it produced:

```bash
git+ session show "$session"
git+ session show --branch=feature/auth
```

When a decision needs human judgement, `git+ session ask` / `answer` records that decision as signed causal provenance so it does not disappear into chat.

See [`docs/agents.md`](docs/agents.md) for the collaboration and session model.

---

## Knowledge

Repository knowledge is ordinary versioned content under `.gitplus/knowledge/`:

```text
.gitplus/knowledge/
  index.md
  architecture/
  conventions/
  gotchas/
  playbooks/
  decisions/
```

The directory is directly **Open Knowledge Format (OKF)** compatible Markdown/YAML. Clone or copy it and it remains portable; there is no Git+-specific import/export translation layer.

Git+ adds stronger repository provenance under a namespaced `gitplus:` frontmatter extension:

```text
signed record citations
blob/gitlink evidence dependencies
external content digests/snapshots
signed verification records
```

Edit and browse the corpus with normal Git and filesystem tools:

```bash
$EDITOR .gitplus/knowledge/gotchas/worker-auth.md
git diff .gitplus/knowledge
git log -- .gitplus/knowledge/gotchas/worker-auth.md
```

Git+ adds the check ordinary file tools cannot perform:

```bash
# Check the whole corpus
git+ knowledge check

# Check one Concept
git+ knowledge check gotchas/worker-auth
```

The check reports repository, temporal (`stale_after`), and external-source freshness separately. Changed evidence means **revalidate**; it does not mean **false**.

Repository Memory is the small projection suitable for every session start:

```bash
git+ session memory
git+ session memory --distill
```

Knowledge and Memory are data, not instruction authority.

See [`docs/knowledge.md`](docs/knowledge.md).

---

## Audit

Users inspect an **Invocation**. Git+ keeps the pre-call Context Exposure and post-call runtime record separate internally, then joins them for the CLI and UI:

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

Harness-native OpenTelemetry GenAI is the preferred runtime input. OTel IDs are correlation metadata; signed Git record OIDs are durable audit identity. Known sampling, transformation, or exporter loss weakens the coverage claim instead of being hidden.

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

ContextRender uses one exact V1 binary framing. The digest binds segment order, logical placement, media type, and exact body bytes.

See [`docs/context-pack.md`](docs/context-pack.md) and [`docs/telemetry.md`](docs/telemetry.md).

---

## CLI

The executable is the syntax reference:

```bash
git+ --help
git+ session --help
git+ context --help
```

The CLI uses the same repository engine as the server. Repo-scoped commands default to the current checkout. Human-facing arguments accept normal Git revisions and unambiguous abbreviated OIDs; canonical stored records use qualified Git OIDs.

[`docs/cli.md`](docs/cli.md) explains workflow conventions. Its top-level command snapshot is generated from the executable with `npm run docs:cli`, so the docs do not maintain a second flag schema by hand.

---

## Web interface

The UI in `src/ui/` covers repository browsing, commits, tasks, pull requests, sessions, knowledge state, and the agent Flight Recorder.

The Flight Recorder groups audit history by session and Invocation, so users do not have to reconstruct Context Exposure and telemetry records themselves. Concurrent trace branches stay visible as causal lanes instead of being flattened by timestamp.

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
# Install workspace dependencies without changing source files
npm ci

# Read-only formatting, lint and type checks
npm run check

# Fast feedback loop
npm run test:unit

# Integration suite only (workerd)
npm run test:integration

# Pinned stock-Git and SEA compatibility suite
npm run test:interop

# Everything
npm test

# Build the preferred end-user executable (Node 26+)
npm run build:sea
./dist/sea/git+ --help
```

`npm run check` intentionally includes the repository's Effect and anti-slop
rules as correctness checks, not automatic style fixes. `npm run fix` is the
explicit opt-in formatter/linter repair command. Run `npm run setup` only to
regenerate build metadata and Wrangler binding types (for example, after
changing `package.json` or `wrangler.test.json`); it is never part of
installation or checking.

After changing the command surface, regenerate the CLI help snapshot:

```bash
npm run docs:cli
```

Documentation map: [`docs/readme.md`](docs/readme.md).
