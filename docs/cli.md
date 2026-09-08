# Git+ CLI

The executable is the syntax reference:

```bash
git+ --help
git+ <command> --help
git+ <command> <subcommand> --help
```

This guide covers the conventions and workflows that span Git+'s **Work**, **Knowledge**, and **Audit** planes. It does not duplicate every flag table.

The generated top-level help snapshot comes from:

```bash
npm run docs:cli
```

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

Users normally work with sessions and Invocations. Context Exposure and trace records stay visible for protocol debugging, but they are not the primary product vocabulary.

---

## 2. Repository discovery

Repo-scoped commands follow ordinary Git ergonomics:

```bash
cd project

git+ session show --branch=refs/heads/feature/auth --audit
git+ context for --task="fix authentication policy"
git+ trace record --session="$session" --key=~/.ssh/agent --event=span.json
```

The current checkout is the default repository context. Explicit root/repository selection remains available for bare repositories, servers, automation, and administration.

Human-facing arguments may use normal Git spelling:

```text
HEAD
main
refs/heads/main
abc123
```

Commands resolve unambiguous revisions and abbreviated OIDs. Canonical serialized records still use algorithm-qualified OIDs such as `sha1:<hex>` or `sha256:<hex>`.

---

## 3. Global behavior

The CLI shape is:

```text
git+ [global-options] <command> [subcommand] [arguments]
```

Conventions:

- human-readable output is the default;
- `--json` returns structured output for harnesses and scripts;
- diagnostics go to stderr so stdout stays parseable;
- commands that create durable records print a stable identifier suitable for later commands;
- missing telemetry or usage stays unknown/absent and is never silently rewritten as zero;
- content relevance never grants repository authority;
- causal record joins use Git identity, not timestamp proximity.

---

## 4. Generated top-level help

`npm run docs:cli` replaces the block below with the executable's `--help` output.

<!-- BEGIN GENERATED CLI HELP -->

```text
USAGE
  git+ <subcommand> [flags]

GLOBAL FLAGS
  --help, -h                                                          Show help information
  --version, -v                                                       Show version information
  --wizard                                                            Start wizard mode for a command
  --completions <bash|zsh|fish|sh>                                    Print shell completion script (choices: bash, zsh, fish, sh)
  --log-level <all|trace|debug|info|warn|warning|error|fatal|none>    Sets the minimum log level (choices: all, trace, debug, info, warn, warning, error, fatal, none)

SUBCOMMANDS
  add                 Stage paths as they are on disk
  archive             Write a tree as a tar, tar.gz or zip archive
  bisect              Name the next commit to test between a good and a bad one
  branch              List, create or delete branches
  cherry-pick         Replay one commit onto another
  clone               Clone a repository over smart HTTP
  commit              Commit what is staged
  context             Git-native context packs, renders and exposures
  diff                Unified diff between two revisions
  files               List the files a revision's tree holds
  fetch               Fetch a remote's branches and tags into a cloned repository
  fsck                Check every object and ref for damage
  gc                  Drop unreachable objects, optionally repacking
  grep                Search a revision's file contents
  hub                 Repository identity, membership and trust
  id                  Stable principal identity and device rotation
  history             Commits that changed one path
  init                Create an empty bare repository
  knowledge           Knowledge Concepts: check structure, provenance and freshness
  log                 Commit history, newest first
  merge               Three-way merge two revisions
  mv                  Move a tracked path, staging both halves
  pr                  Pull requests: open, review, discuss, check, merge
  pull                Fast-forward one branch from a remote
  push                Push refs to a remote over smart HTTP
  rebase              Replay a branch's commits onto another
  reflog              Where a ref has been: every move, newest first
  refs                Every ref and the object it points at
  server              Server JSON-API administration extensions
  reset               Move a ref, optionally compare-and-swap
  restore             Restore a path from the index or a commit
  rm                  Unstage a path, and delete it unless --cached
  serve               Run the node host over a directory of repositories
  show                Show one object: a commit, tree, tag or blob
  status              Working-tree status in git's porcelain format
  switch              Check out a branch, replacing index and work tree
  session             Record what an agent was told, and what came of it
  social              Social graph: follows, vouches and discovery
  tag                 List, create or delete tags
  task                What needs doing, and who is on it
  trace               Append signed runtime telemetry to a session's audit trace
  queue               Land approved pull requests as one tested batch
  wake                Run local rules for hub events since the last run
  webhook             Administer a server's webhooks over its JSON API
  credential          Mint a short-lived credential stock git can present
  credential-helper   Answer git's credential helper protocol
```

<!-- END GENERATED CLI HELP -->

---

## 5. Work

### 5.1 Tasks

Tasks coordinate work through signed task refs. Claims are advisory leases, not locks.

```bash
git+ task list
git+ task claim <task> --ttl=15m
# work
git+ task close <task> --commit=HEAD
```

Run `git+ task --help` for the exact verbs and flags.

### 5.2 Sessions

A session keeps the compact work record:

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
  --key="$HOME/.ssh/id_ed25519" \
  --agent=claude-code \
  --model=model-x \
  --prompt="Fix auth policy")

# work

git+ session produce \
  --key="$HOME/.ssh/id_ed25519" \
  --session="$session" \
  --commit=HEAD \
  --note="Worker auth tests require the production policy fixture"
```

Inspect a session by ID or by the branch it produced:

```bash
git+ session show "$session"
git+ session show --branch=feature/auth
```

`show` is the one `session` subcommand whose positional argument is the session rather than the
repository — the session is what it is _for_, and requiring a repository ahead of it made the
common call name two things to select one. The repository moves to `--repo`, which every read-only
verb in §7 already takes, and is not needed at all inside a checkout. The other seven subcommands
are unchanged. An older `git+ session show <repo> <session>` fails rather than misreading: with
`--root` set and no `--repo`, the command says so and names the flag.

### 5.3 Session audit

The session is also the normal audit entry point:

```bash
git+ session show "$session" --audit
git+ session show --branch=feature/auth --audit
```

`--audit` joins the policy-visible session record with its policy-invisible Invocation history.

Example projection:

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

The separate pre-call Context Exposure and post-call Invocation Telemetry records only matter when debugging the audit protocol itself.

### 5.4 Decisions

When the agent reaches a question that needs human judgement:

```bash
git+ session ask \
  --session="$session" \
  --question="Which compatibility behavior should remain?" \
  --option="strict,legacy"

git+ session answer \
  --session="$session" \
  --decision=<decision-id> \
  --chose=strict
```

The answer becomes signed causal provenance instead of an ephemeral chat message.

---

## 6. Knowledge

The knowledge corpus is ordinary repository content under `.gitplus/knowledge/`. Each Concept is directly OKF-compatible Markdown/YAML with optional stronger Git+ provenance under `gitplus:` frontmatter.

Use normal file and Git operations to edit and inspect it:

```bash
$EDITOR .gitplus/knowledge/gotchas/worker-auth.md
git diff .gitplus/knowledge
git log -- .gitplus/knowledge/gotchas/worker-auth.md
```

The directory already is the portable interchange artifact, so Git+ does not require a list/show/import/export layer.

### 6.1 Verify knowledge

Git+ adds the check ordinary file tools cannot perform:

```bash
# Check the entire bundle, against the permitted effective working view
git+ knowledge check

# Check one Concept, by id or by repository-relative path
git+ knowledge check gotchas/worker-auth
git+ knowledge check .gitplus/knowledge/gotchas/worker-auth.md

# A committed revision rather than the working tree, as JSON
git+ knowledge check --ref HEAD --json

# Turn selected findings into a gate
git+ knowledge check --strict

# A bare repository on a server
git+ knowledge check --root ./repos --repo project --ref main
```

Checking is read-only: it stages nothing, writes no Concept, persists no Memory and appends no exposure. Capturing a dirty view does materialize blobs for the files it reads, which is how the working view is addressed at all.

Exit **0** when the requested scope was evaluated, its structural and provenance checks hold, and the selected gate passed — a missing bundle is a successful no-op. Exit **1** for malformed input, invalid declared provenance, incomplete evaluation, or a failed gate. Warnings alone do not fail the default gate; `--strict` additionally fails on changed or missing declared dependencies, on expired or invalid freshness deadlines, and on citations this replica could not verify.

A cited record this replica does not hold is reported `unavailable`, never accepted or invalid. `hub enable` does not fetch session refs by default, so on a fresh clone every citation reads that way until they are fetched:

```bash
git+ hub enable --refs 'refs/hub/session/*'
```

The check keeps these dimensions separate:

```text
OKF structure
status / stale_after
signed Git+ citations
blob dependency state
gitlink dependency state
external captured-source state
signed verification provenance
```

Changed evidence means **revalidate**, not **false**.

Portable OKF `verified` metadata is editorial metadata. It never grants Git+ membership, capability, review authority, or instruction authority.

### 6.2 Repository Memory

Repository Memory is the bounded projection suitable for every session start:

```bash
# Read the stored note, which is a historical cache and says so
git+ session memory

# Re-derive from current Concepts and session records, without persisting
git+ session memory --derive

# Re-derive and persist
git+ session memory --distill
```

The note is where Memory is kept, not what automatic recall trusts. `--derive` and `--distill` rebuild it from the Concepts and signed session records that are eligible _now_, so a redaction, a revoked key, a changed Concept or a passed `stale_after` takes an entry out before any collection has run. The account of the derivation — what did not fit, what was excluded and why, whether the note was written — goes to stderr, so stdout stays the note.

Memory is a cache. Eviction does not delete Knowledge Concepts or their signed source records.

### 6.3 Recording what a session learned

The component that can judge the work supplies the learning; a stop hook cannot infer one from a branch name. `session produce` takes it either way:

```bash
git+ session produce --session <id> --note "gotcha: …" project
git+ session produce --session <id> --note-file ./learning.txt project
```

`--note-file` reads from a path or from `-` for stdin, and is mutually exclusive with `--note`. It exists so multiline or sensitive text never passes through process arguments; the record schema, the size bound and the secret scan are the same on both paths. No learning is a valid outcome — nothing is written when there is none.

---

## 7. Audit: repository context

Context commands expose the Git-native repository-context protocol in [context-pack.md](context-pack.md):

```text
git+ context for --task <text>
git+ context why <pack> [item]
git+ context audit <invocation-or-exposure>
```

### 7.1 Build context

```bash
git+ context for --task="fix authentication policy"

# …with eligible Knowledge Concepts competing for the same budget
git+ context for --task="fix authentication policy" --knowledge
```

With `--knowledge`, a recalled Concept is selected together with the current bytes of the repository evidence it declares. A group that does not fit is omitted whole, with a `budget` diagnostic naming the Concept: prose whose support was silently dropped reads as supported when it is not.

Concept freshness is judged at an instant, and the instant is printed (`evaluated`, or `evaluatedAt` under `--json`) because the pack does not carry it. `--at <ISO 8601 datetime>` names it explicitly, as `knowledge check` does; left out, it is the wall clock, and two runs over identical refs can then differ on a Concept whose `stale_after` fell between them.

The output identifies one Repository View and typed evidence:

```text
blob
  path + blob OID + optional byte range

gitlink
  path + mode 160000 + submodule commit OID
```

Retrieval scores, indexes, or graph paths may appear as diagnostics. They are not evidence identity.

Nothing is recorded unless recording is asked for. Naming a session and a signing key writes the
Context Exposure — pack, render commitment and retained view — and prints the Git record OID:

```bash
git+ context for --task="fix authentication policy" --session=<session-id> --key=<private-key>
```

`--retain-render=false` keeps the commitment and drops the exact render bytes, which is the shape a
retention or redaction policy leaves behind.

### 7.2 Explain selection

```bash
git+ context why <pack> src/auth.ts
```

`<pack>` is a pack blob OID, an exposure record OID — the exposure retains its own pack — an
ordinary revision naming either, or a file holding a pack that was never persisted.

The command separates verified Git evidence from descriptive selector explanations.

### 7.3 Audit exposure

```bash
git+ context audit <invocation-or-exposure>
```

The argument is a qualified Git record OID, an ordinary revision naming one, or a session id —
which audits every exposure that session recorded, oldest first. An `invocation-telemetry` OID,
which is what `git+ trace record` prints, audits the exposure that invocation names. A record
that names no exposure — a tool operation, a workspace transition, an invocation that used no
context — is refused, with a pointer to `git+ session show <session> --audit`.

`why` and `audit` read objects and nothing else, so they take `--root`/`--repo` and work on a bare
repository; inside a checkout neither flag is needed. `for` takes `--work`, because capturing a
Repository View needs the work tree.

The audit checks each dimension independently:

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

What this implementation reports today: the signature over the exact payload bytes and whether a
signer holding `hub.trace` could have written it, the repository/session binding, pack blob
identity, the retained `context/view` edge, every blob and gitlink item with its range and
instruction-provenance annotation, and the render as one of verified, absent or unreadable. The
bound Invocation arrives with the runtime records in [telemetry.md](telemetry.md).

---

## 8. Harness telemetry

Harness-native OpenTelemetry GenAI is the preferred runtime capture path. Users read the resulting Invocations through:

```bash
git+ session show <session-id> --audit
git+ session show --branch=<ref> --audit
```

One or the other: a session to read, or a branch to look one up from. `--audit` has nothing to
project without knowing which run it is about.

Raw trace writing exists for integrations without suitable native OTel:

```bash
git+ trace record \
  --session=<session-id> \
  --key=<private-key> \
  --event=<event.json>
```

The recorder binds the repository and session, validates the normalized event, signs it, appends it under `refs/hub/trace/<session>`, and returns the Git record OID. The envelope — repository, session, record id, time, trust head — is the recorder's to fill in, so a file that supplies its own is overwritten rather than believed.

A GenAI span can go through the same writer instead of a pre-normalized event:

```bash
git+ trace record \
  --session=<session-id> --key=<private-key> \
  --event=<span.json> --otel \
  --stage=sdk-export --semconv-revision=<upstream revision> \
  --exposure=<context-exposure-oid>
```

`--stage` records where the signal was captured before anything could sample it. `--semconv-revision` is what makes the record claim strict semconv adherence; without it the mapping is best-effort and says so by omitting the `semconv` block. `--exposure` names the Context Exposure this invocation was made against, which is the only join the projection uses.

Both forms discover the current checkout by default; `--root` and `--repo` select a bare repository for server use.

A record whose prompt or exposed bytes should not have replicated comes back out the same way a
session record does:

```bash
git+ trace redact --session=<session-id> --target=<record-oid> --reason="the prompt carried a token"
```

Nothing is deleted by that command. The tombstone is what replicates, and the bytes go at the next
`gc`; removing a record needs `hub.redact` beside `hub.trace`.

There is no normal `trace show` workflow. The session/Invocation projection is the human-facing read path.

See [telemetry.md](telemetry.md) for GenAI semantic-convention mapping, attempts, capture coverage, trace health, and retention rules.

---

## 9. JSON output

Machine consumers should use `--json` instead of parsing terminal formatting.

Structured responses preserve distinctions such as:

```text
observed vs reported vs derived
known vs unknown
current vs stale
complete vs partial capture
portable editorial verification vs signed Git+ provenance
```

Unknown fields are omitted or represented explicitly according to the command schema; commands must not fabricate zero/false values.

---

## 10. Command-documentation rule

The executable owns syntax. After a command definition changes, run:

```bash
npm run docs:cli
```

That command updates the generated top-level help snapshot in this file. The hand-written sections explain workflows and concepts only.
