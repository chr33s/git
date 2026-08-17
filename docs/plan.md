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

## Phase 0 — `hub whoami` (small; ships alone)

The read-only join of the trust projection and the policy document
(agents.md Part I).

```text
src/server/Api.ts      one JSON verb: answer capabilities, expiry
                       and per-branch push verdicts for the
                       presented credential or challenge key
src/cli/hub.ts         `whoami` subcommand following the existing
                       enable/status pattern; --key signs a
                       challenge, --token presents a credential
```

Reads `src/trust/Projection.ts` `project()` and `src/server/Policy.ts`
rules; invents nothing. Tests: unit against the Policy fixtures
(member/non-member/expired; protected/unprotected branch), one Api test.

**Exit:** an agent can ask "what may I do here?" before doing anything.

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

## Phase 3 — wake and `session enable` (small, node-host only)

```text
src/cli/wake.ts        `wake dispatch`: reads post-receive triples
                       (or Hooks.postReceive results), walks
                       old..new for refs/hub/**, matches rules from
                       .chr33s/wake.json (ref pattern × event kind ×
                       predicate), spawns the configured command,
                       advances a local cursor ref; `wake catchup`
                       reconciles cursor → tip on start
src/host/Node.ts       provide a Hooks layer that invokes the
                       dispatcher (replacing hooksNoop) when a
                       wake config exists
src/cli/session.ts     `enable`: writes Claude Code hooks
                       (SessionStart → whoami + open;
                       Stop → produce) into .claude/settings.json;
                       one harness only
```

The Workers host needs nothing: `Webhooks.ts` already delivers signed
push events with retries — a wake consumer there is a webhook receiver,
documented, not built.

Tests: dispatcher matches/ignores correctly; cursor survives restart;
missed events replay on catchup; enable is idempotent.

**Exit:** the loop closes — an agent is woken by a review on its PR,
resumes its session, and answers.

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
