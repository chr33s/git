# Implementation Plan: Agents v0

The build order for [agents.md](agents.md) Part II's v0 cut, grounded in
what this branch already ships. The guiding fact: the hard machinery
exists and is tested — signed event DAGs (`src/hub/Event.ts`), DAG
projection (`src/hub/Projection.ts`), redaction (`src/hub/Redaction.ts`),
the policy boundary with append-only `refs/hub/**` enforcement
(`src/server/Policy.ts`), advertisement hiding (`src/server/Protocol.ts`,
tested), delegated credentials (`src/server/Auth.ts`), the trust stack
(`src/trust/*`), LFS (`src/server/Lfs.ts`), replication with DAG
reconciliation (`src/server/Replication.ts`), and a `Hooks` service whose
`postReceive` already fires on every receive
(`src/git/Repository.ts`). The plan is mostly composition.

Every phase lands green through `npm run check` and `npm test`, follows
the conventions in [internals.md](internals.md), and interop-tests
against stock `git` where a wire surface changes. Phases 0 and 1 are
independent; everything else depends on 1.

---

## Phase 0 — `hub whoami` — **landed**

The read-only join of the trust projection and the policy document
(agents.md Part I).

```text
src/server/Whoami.ts    the join itself: standing, per-ref verdicts,
                        trust freshness, and the wire schema — one
                        implementation, because an answer that
                        disagreed with the enforcement is worse than
                        no answer
src/cli/hub.ts          `whoami <repo> --key <path>`, for a repository
                        on this disk
src/server/Api.ts       `GET /:repo/whoami`, for the credential
                        presented to a host — the form a sandboxed
                        agent needs, and it reports the credential's
                        narrowed capabilities rather than the
                        member's full grant
src/cli/shared.ts       readAnyPublicKey — a private key carries its
                        own public half, and a sandbox holds that one
src/server/Policy.ts    isProtected exported, so both answers use the
                        boundary's own rule rather than a copy
```

Seven tests: five end-to-end through the CLI (member, protected branch
naming its requirements, stranger, revoked key, no genesis) and two
through the derived API client (anonymous, and a credential narrower than
its member). `npm run check` and all 818 tests pass.

**Exit — met:** an agent can ask "what may I do here?" before doing
anything, locally or over the wire.

## Phase 1 — session capture — **landed**

Two event kinds on the existing event machinery, one capability, one
namespace.

```text
src/hub/Session.ts         session.opened (agent, prompt, role, and the
                           standing instructions as an object id) and
                           session.produced (commits, refs, pulls, note,
                           usage); 256 KiB bound; open/produced helpers
src/hub/Event.ts           `append` became `appendTo`, taking the ref
                           rather than deriving it from a pull request
                           id — pull-request behaviour unchanged
src/trust/Certificate.ts   hub.session
src/server/Policy.ts       sessions admitted as the namespace's second
                           shape, and the population bound counted per
                           class
src/cli/session.ts         open / produce / show
```

Free rides, verified rather than built: `refs/hub/*` already carries
append-only enforcement, advertisement hiding, protocol-v2 prefix fetch,
replication join and redaction, and a session ref is one of those.

**Exit — met:** a hook can record who was instructed, what was asked, and
what came of it — signed, replicated, invisible to stock clients.

## Phase 2 — projection and resume — **landed**

`Session.entries` walks the DAG the way a wake does — bounded to the
namespace, stepping over joins, treating an event it cannot decode as one
event rather than a broken session — and `Session.project` folds it into
what a reader about to continue needs. `session show` takes an id or
`--branch`, because "put me back in context for this branch" is the
question an agent has on checkout; an id is what a caller holds only if it
opened the session itself.

**Exit — met:** a second agent reads a first agent's session and continues
it.

## Phase 3 — wake — **the dispatcher has landed**

```text
src/server/Wake.node.ts   the pass itself: walk each hub ref from
                          its bookmark to the tip, decode the typed
                          signed events, match rules from wake.json,
                          spawn with the event in the environment,
                          advance. Plus the hooks decorator, and the
                          per-repository serialization that keeps two
                          pushes from waking one event twice
src/cli/wake.ts           the same pass from a terminal, and
                          --dry-run
src/host/Node.ts          `serve --wake` runs it on post-receive,
                          forked so a push never waits on a rule
src/cli/session.ts        `enable` — writes the harness hooks that
                          call the verbs above
```

Pull rather than push, which is what makes the hook optional rather than
load-bearing: the same pass serves a hook, a timer and a person, so a
missed hook is a late wake rather than a lost one. Six end-to-end tests
cover waking once and not twice, waking for what arrived since, replaying
a failed batch, the dry run, refusing unreadable rules, and — through a
real server and a real push — that the host wakes at all.

The Workers host needs nothing new: `Webhooks.ts` already delivers signed
push events with retries, which is a wake anyone can consume.

**Exit — met:** an agent is woken by a review on its pull request, and
`session enable` closes the loop back into a session — one harness, as the
v0 cut says.

## Phase 4 — the deferred layers — **landed**

Everything the v0 cut set aside is now built, each on the machinery the
phases before it left in place and none of it changing a Phase 1 wire or
event format — which is the invariant that made the ordering safe.

```text
tasks and claims     src/hub/Task.ts, hub.task, src/cli/task.ts.
                     A claim is a lease judged against the caller's
                     clock, because the events carry no trustworthy
                     one of their own, and advisory because two
                     agents reading at once can both pass any check
                     a boundary could make
requireProvenance    the trailer → session.produced check, with the
                     batch's own session commands counted before its
                     source commands, so a branch and the record of
                     what produced it travel in one receive-pack
decisions            decision.requested / decision.resolved in the
                     session DAG, with session ask|answer. The
                     answer is the one projected record a harness
                     may treat as instruction, and the carve-out is
                     narrow: a question this session asked, from a
                     key the trust graph can name
memory               a note on the genesis commit, rebuilt from the
                     sessions it cites rather than merged into, and
                     capped so eviction is forced
budgets              maxUsageTokens over what sessions report,
                     surfaced by whoami rather than enforced,
                     because what it counts is self-reported
secret scanning      src/hub/Secrets.ts over what somebody typed —
                     not over the record, whose own identifiers are
                     high-entropy by construction
provenance remote    a mirror-everything default never carries a
                     session ref; naming it is what configuring a
                     provenance remote is
```

---

## Running alongside foundation work

This branch was built beside a foundation still being fixed, and the split
that made that work is worth keeping for the next time.

```text
safe in parallel — new files, or additive edges:
  a new CLI verb, a new Api verb, a new module beside an
  existing one, a hook on a seam that already exists

worth landing in the foundation branch instead:
  edits to a file it is actively reworking — a fork of one
  is a standing rebase conflict
```

Both rules held. The one change that had to touch shared code —
generalizing the event append path from a pull request id to a
caller-supplied ref — was mechanical and left pull-request behaviour
identical, which is what made it safe to carry here. The rebase that
followed cost two conflicts, both in files this branch had extended
rather than rewritten.

## What it came to

```text
Phase 0   whoami, locally and over the wire
Phase 1   session capture
Phase 2   projection and resume
Phase 3   wake, the host hook, session enable
Phase 4   tasks, requireProvenance, decisions, memory,
          budgets, scanning, the provenance remote
```

Every phase landed green through `npm run check` and `npm test`, and every
fix carries a test that fails without it. What is _not_ built is named in
agents.md where it is proposed: session authorship and resume semantics
(§7–§8's finer rules), signer-scoped `requireProvenance`, transcript side
objects, and harnesses beyond Claude Code.
