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

## Phase 1 — session capture (the core)

Two event kinds on the existing event machinery, one capability, one
namespace.

```text
src/trust/Certificate.ts   add "hub.session" to CAPABILITIES
src/hub/Session.ts         payload schemas: session.opened (agent,
                           prompt, context.instructions, trustHead),
                           session.produced (commits, refs, pulls,
                           note, usage); 256 KiB bound; UUIDv7 ids
src/hub/Event.ts           generalize the append path's ref naming
                           from refs/hub/pr/<id> to a caller-supplied
                           refs/hub/<class>/<id> (mechanical; the
                           append/sign/join logic does not change)
src/server/Policy.ts       gate refs/hub/session/** appends on
                           hub.session (append-only rules for
                           refs/hub/** already apply)
src/cli/session.ts         open / produce / show; trailer helper
                           printing "Session: <id>" for commit -m
```

Free rides to verify, not build: advertisement hiding and protocol-v2
prefix fetch (Protocol tests already cover `refs/hub/*`), replication
join (`reconcile` is namespace-agnostic), redaction (tombstones target
events by commit, class-blind).

Tests: schema roundtrips; append refused without `hub.session`;
append-only enforcement on the session ref; interop — stock git pushes
branch + session ref in one receive-pack and both land; hidden from a
source-only clone's advertisement.

**Exit:** a hook script can record who was instructed, what was asked,
what came of it — signed, replicated, invisible to stock clients.

## Phase 2 — projection and resume (small)

```text
src/hub/Session.ts     projectSession(id): walk the DAG (reuse
                       Projection.ts helpers), causal order, §16
                       tiebreak; latestForBranch(branch): scan
                       session refs' produced events
src/cli/session.ts     show <id>; resume --branch <name> prints
                       the projection for context injection
```

Tests: divergent-heads join projects once; branch index picks the
causally latest producer.

**Exit:** a second agent reads a first agent's session and continues it.

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
later  src/cli/session.ts  `enable` — writes the harness hooks;
                          waits on Phase 1's session verbs
```

Pull rather than push, which is what makes the hook optional rather than
load-bearing: the same pass serves a hook, a timer and a person, so a
missed hook is a late wake rather than a lost one. Six end-to-end tests
cover waking once and not twice, waking for what arrived since, replaying
a failed batch, the dry run, refusing unreadable rules, and — through a
real server and a real push — that the host wakes at all.

The Workers host needs nothing new: `Webhooks.ts` already delivers signed
push events with retries, which is a wake anyone can consume.

**Exit — met:** an agent is woken by a review on its pull request. Closing
the loop back into a _session_ waits on Phase 1.

## Phase 4 — deferred layers (explicitly not v0)

Each slots into a file that exists by Phase 3, in rough value order:

```text
tasks and claims        Session.ts sibling class + hub.task
                        capability; the lease-expiry projection
requireProvenance       Policy.ts: trailer → produced check,
                        session commands judged before source
                        commands in one receive-pack
decisions               two more payload schemas + the authorship
                        exemption
memory                  a distiller job (agent-side) + notes
                        read/write helpers; no server change
budgets                 Policy.ts maxUsage + whoami surfacing
secret scanning         a Policy.ts payload filter, layers 1–6
provenance remote       Remotes.ts sync config: per-remote
                        refspecs excluding sessions by default
```

None of these changes a Phase 1 wire or event format — that invariant
(agents.md §15) is what makes this ordering safe.

---

## Running alongside foundation work

While the foundation is still being fixed on its own branch, the phases
above split cleanly into lanes by how much existing code they touch.

```text
safe in parallel — new files, or additive edges:
  Phase 0    a new CLI verb and (next) a new Api verb; every fix to
             the projection it reads makes its answers more correct
  Phase 3    wake.ts and session enable are new files against the
             Hooks.postReceive seam, which the foundation work has
             not touched
  trailers   convention only, no code

hold, or land inside the foundation branch instead:
  the Event.ts ref generalization and the Policy.ts session gate —
  both modify files the foundation work is actively reworking, so a
  fork of them is a standing rebase conflict. Both are small
  (a caller-supplied ref class; one capability in the list), which
  makes them cheaper to propose there than to carry here.
```

Two working rules follow. Branch implementation off the foundation
branch's head rather than the default branch, and rebase onto it often —
its commits are semantic fixes rather than structural moves, so additive
work rebases cheaply. And keep each phase's tests runnable in isolation,
so a rebase that breaks something says which phase it broke.

## Sizing and sequence

```text
Phase 0   ~2–3 days    independent; ship first
Phase 1   ~1–2 weeks   the only phase with a schema decision
Phase 2   ~2–3 days    pure reads
Phase 3   ~1 week      dispatcher + one harness integration
```

One month of focused work to the closed loop, with a shippable increment
at every phase boundary. The risk concentrates in Phase 1's one refactor
(generalizing Event.ts ref naming) — do it first inside the phase, keep
the PR-event tests green throughout, and everything after is addition,
not modification.
