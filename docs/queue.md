# Git-Native Merge Queue

**Status:** Implemented (revision 2)
**Scope:** `chr33s/git` — composes with [hub.md](hub.md) (§11 advancing a
protected branch, §16 event DAGs, §26 merge policy) and
[agents.md](agents.md) (§19 wake, §20 tasks and claims). Nothing here
changes a Phase 1–4 wire or event format from [plan.md](plan.md).
**Queue namespace:** `refs/hub/queue/*`
**New capability:** `hub.queue`

## 1. The problem: the serialization tax

The boundary's rule for a protected branch is exactly right and exactly
serial. A branch advances by **pushing the approved revision onto it, and
by nothing else** (hub.md §11): approvals name the exact head, checks bind
to the exact revision, and the advance is compare-and-swap against the
value the rules were judged on. What lands is literally the reviewed
bytes. Nothing stale, wrapped or untested can arrive.

Under contention that safety has a cost with a shape. When the target
moves, every open pull request behind it stops being a fast-forward; each
must rebase, which is a `pr.updated`, which stales its approvals and its
checks — by design (hub.md §18). With N approved pull requests racing one
branch, each landing forces a re-review-and-re-test cycle on the rest:
O(N²) review work for O(N) changes, and an agent fleet reaches N faster
than a human team ever did. Worse, the failure the serial rule cannot see
is the one that needs no race at all: two changes each green on their own
base can be wrong **together** — a rename on one side, a new caller on the
other — and textual mergeability says nothing about it. The only evidence
that composed changes work is a test run on the composed result.

What is missing is not safety but throughput: a way to test _this exact
combination_ of approved revisions once, and land the whole combination
in one swap — without trusting whoever built it.

## 2. Design principle: a member, not a service

Every forge builds this as a privileged service that owns the branch.
That is the wrong shape here twice over: the host does not execute CI
(hub.md §19) and does not hold authority — the repository does. So the
queue is **a member**: an ordinary agent holding ordinary capabilities,
coordinating through signed events, whose output the policy boundary can
verify without trusting its author.

The split follows the one the design already uses everywhere:

```text
enforcement    the policy boundary — one new way to satisfy the
               rules a protected branch already has, checkable by
               every replica from refs alone
coordination   signed queue events — advisory, like task claims:
               they order honest agents and restrain nobody
```

The consequences fall out. Any member holding the capabilities can run
the queue. Two running at once are safe, because landing is
compare-and-swap. Zero running degrades to exactly today's behavior —
the direct push of an approved head remains valid, and is the queue of
depth zero. And a malicious queue holder can land nothing the boundary
would not have accepted from anyone, because the boundary verifies the
candidate rather than the builder.

## 3. The verified candidate

The mechanism is one new judgement at the boundary. Hub.md §11 refuses
merge commits on protected branches because _"a merge's tree is
unconstrained — a merge commit that merely names an approved head as a
parent proves nothing about content."_ The answer is not to trust the
merge; it is to **constrain the tree**.

`mergeTrees` (`src/git/Merge.ts`) is deterministic and pure — the same
three-way decision `Repository.merge` and `Rebase.replayTree` already
share, byte-identical on every replica. That makes "is this commit
exactly the merge of an approved revision onto the target?" a question
the boundary can answer itself, with no worktree and no trust in whoever
built the commit.

### Candidate chains

A push to a protected branch proposes new value `C_k`. Walk first
parents from `C_k` back to a commit the branch already holds — call it
`C_0`, the current tip. The push is a **candidate chain** when every step
`C_i` (1 ≤ i ≤ k) satisfies:

```text
1. C_i is a merge commit with exactly the parents
   [C_{i-1}, H_i], in that order

2. H_i is the current head of an open pull request PR_i
   whose base is this branch (the same fold that judges a
   direct push — hub.md §26)

3. tree(C_i) is exactly the tree Repository.mergeTree
   produces for (C_{i-1}, H_i), and that merge is
   conflict-free

4. PR_i satisfies every rule the branch requires —
   requiredApprovals of H_i for this base, resolved
   threads, requirePullRequest — exactly as today, judged
   against H_i

5. requiredChecks name C_i itself: a valid check.completed
   with head = C_i, from a signer holding hub.check:<name>,
   for every required name
```

Rule 3 is the load-bearing one: the candidate's content is a pure
function of `(C_{i-1}, H_i)`, so the tree cannot smuggle anything a
review did not cover beyond the composition itself. Rule 5 is what makes
the composition safe: the semantic conflicts rule 3 cannot see are
exactly what a test run on `C_i` exists to catch — the checks that count
are on the **candidate**, never on the pull request's own head. Rule 4
keeps the review meaning what hub.md §18 says it means: an approval of
`H_i` for this base, staled by retargeting and by head movement, with
self-approval excluded as always.

What the five rules constrain is a candidate's **content and ancestry**,
and not its commit header. The message, author and committer are
whatever the builder wrote, exactly as they are on any merge commit a
member makes on a branch of their own. That is deliberate rather than an
oversight: they are not code, nothing downstream reads them — a
repository that requires provenance takes no candidates at all, for
reasons below — and pinning them would bake one builder's conventions
into the boundary, which is precisely what would stop somebody
hand-building a candidate and landing it with no queue running.

Rule 3 names one function rather than a definition of merging, and that
is the point: the builder and the verifier both call
`Repository.mergeTree`, so they agree about the base and about the merge
by construction rather than by two implementations happening to match.
An earlier draft of this proposal refused a step with several merge bases,
reasoning that recursive-merge virtual ancestors would put an iterated
computation on the synchronous push path. That guard turned out to guard
nothing here: this codebase's `mergeBase` builds no virtual ancestor — it
picks one best common ancestor by a deterministic walk — so a step with
several bases costs exactly what any other step costs, and both sides pick
the same one. Where a replica cannot read the objects the walk needs, it
computes a different tree or none, and both readings are a refusal.

The chain lands with one `RefStore.apply`, `expected = C_0` — the same
compare-and-swap discipline as everything else (hub.md §26). If the
target moved since the chain was built, the swap fails and the queue
rebuilds; nothing stale can land, no matter how fast the queue moves.

### What lands, and what the record says

After the swap, the queue agent appends `pr.merged` to each `PR_i`,
naming `H_i` — a revision the pull request actually proposed, which is
what hub.md §10 requires of a merged event; the branch holds `C_i`, whose
second parent _is_ `H_i`, so ancestry tells the truth without any new
event shape.

Candidates are merge commits, not rebases, and that is a decision rather
than a default: a rebase re-authors every commit, which breaks the
commit signatures Part I of agents.md establishes and orphans the
`Session` trailers that `requireProvenance` verifies. A merge candidate
carries the signed originals intact. Operators who want linear history
are choosing rewritten authorship; that trade is theirs, and a
rebase-candidate mode is deferred until someone wants it enough to argue
the signature loss.

A candidate is also a commit **the runner makes**, and that puts it at
odds with `requireProvenance`, which is a rule about every commit a push
introduces. An earlier draft of this proposal said integration would
arrive with provenance of its own, the candidate being made inside the
runner's session. Building it showed why that is not free: a candidate's
object id must be a pure function of what it merges, or a check recorded
against one pass's candidate names nothing the next pass holds and the
queue can never land at all — so the trailer cannot carry a session id
chosen per run, and a stable one means either a session ref per batch or
one session growing without bound. Both are ceilings, not details. So the
two are **not usable together** for now: `queue run` refuses a target
whose rules require provenance, naming the reason, rather than building
candidates every wake that the boundary will always refuse. Landing pull
requests directly still works there, which is what such a repository was
doing anyway.

### Cost and its ceiling

Verification per step is one merge-base walk and one `mergeTrees`
recompute, on the synchronous receive-pack path — so it is bounded the
way every fold is bounded (hub.md §23). `queueDepth` caps the chain; a
push whose first-parent walk exceeds it is refused as a candidate before
any merge is recomputed, and the cheap `proposes` pre-filter that
already spares the boundary needless folds applies per step, since each
step names a proposed revision. The rules fields, both optional so every
existing rules file still decodes (the `requireProvenance` precedent —
protection nobody configured must not arrive with an upgrade):

```jsonc
// refs/meta/policy · policy.json
{
  "queueCandidates": true, // default false: today's rule only
  "queueDepth": 8, // default 8; 0 reads as "no chains"
}
```

`queueCandidates: false` costs nothing at the boundary — the walk is
never attempted — and is the default because a candidate chain is a new
way onto a protected branch, which an operator should turn on knowingly.

## 4. Queue coordination: `refs/hub/queue/*`

Boundary verification makes landing safe; it does not make N agents
efficient. Which pull requests are queued, in what order, on which
candidate — that is coordination, and it lives where all coordination
lives: an append-only, signed event DAG on the machinery of hub.md §16,
with every rule unchanged (fast-forward or join only, no deletion,
ceilings, advertisement hygiene, replication join, redaction).

```text
refs/hub/queue/<uuid>          uuid: UUIDv7, one ref per queue

queue.opened     target ref (full name, refs/heads/main);
                 one queue per target by convention — a
                 duplicate resolves by descent, exactly as
                 competing pr.opened events do (hub.md §27)
queue.entered    pr id, at head H and base; intent to land
queue.candidate  pr id, step OID C_i, its parents, and the
                 candidate branch it is published on
queue.left       pr id, reason: landed | failed | conflict |
                 stale | withdrawn
queue.reset      the target moved outside the queue; chains
                 behind the named OID are abandoned
```

All events are SSH-signed, carry the `RepoID`, and cost the new
capability `hub.queue` at the boundary — a namespace capability, per
hub.md §23's rule that growing an append-only ref must cost a capability
of its namespace. `repo.admin` implies it, like the rest.

Two properties are deliberate and worth stating plainly:

**The boundary never reads the queue ref.** Landing is judged entirely
from the trust log, the pull requests' own events, and the rules — the
same pure function as today, with rule 3's arithmetic added. The queue
ref orders honest agents and records what the queue decided; it
authorizes nothing. This is the lesson of hub.md §23's ceilings taken
seriously: a ref that gates pushes is a ref whose corruption, oversize
or absence freezes a branch, and the queue ref must never be able to.
Order enforcement is not worth that coupling — two honest agents racing
the same landing are already safe under the swap, and a dishonest one
gains nothing by lying about order it cannot land out of anyway.

**Entries are intent, not locks.** Like a task claim (agents.md §20), a
`queue.entered` coordinates agents that want to cooperate. Concurrent
entries are ordinary DAG divergence; projection orders them causally
with §16's tiebreak, and the queue position is derived, never stored.

## 5. Candidate branches are ordinary branches

Candidate commits need to be fetchable — CI must test them — and must
not pin the object graph forever. Both point away from inventing a ref
class: candidates are published as **plain branches** under a
conventional prefix,

```text
refs/heads/queue/<target>/<pr>
```

named in `queue.candidate` events, deletable and force-pushable by
ordinary branch rules, collected by `gc` once deleted. Operators simply
do not list `queue/**` in `protected`. Hub refs never point at them —
hub payloads _name_ candidate OIDs as strings, and hub.md §23 already
forbids hub commits reaching source commits as parents — so a deleted
candidate's objects genuinely go away. No new ref class, no new
advertisement rule, no new GC root.

The name is the pull request's, not the step's position in the chain.
An earlier draft used the position, and it was wrong for a reason worth
recording: a position moves when an entry ahead of it is skipped, so one
pull request's published branch was force-moved to another's candidate
while the `queue.candidate` record for the first still named it — a
branch CI had been told to fetch, quietly holding somebody else's work.
A pass deletes exactly the branches of the entries it settled, which is
the one set it knows is finished; sweeping the prefix for anything a
keep-list did not mention deleted a concurrent runner's freshly
published candidate and any branch a person kept under the same prefix.

CI needs no new anything either: it fetches the candidate branch, runs,
and signs `check.completed` with `head = C_i` under its existing
`hub.check:<name>` grant, appended to `PR_i`'s own event DAG — where the
fold that judges the landing already looks for checks, keyed by name and
revision (hub.md §19).

## 6. The integration agent

`git+ queue run` is the dispatcher, shaped like `wake` (agents.md §19):
a pull-based pass anyone can invoke — a hook, a timer, a person — that
re-derives everything from refs and is therefore crash-safe with no
bookmark and no lease. One pass:

```text
read rules; read the queue projection; read open PRs
    ↓
ready set: open PRs on this base, approvals current,
           entered in the queue (or auto-enter, by flag)
    ↓
extend the chain: merge next H onto the chain tip with
  the same Merge.ts the boundary will re-run; a conflict
  is queue.left(conflict) — predicted, not discovered
    ↓
push candidate branches; append queue.candidate; wait
  (wake on check.completed reaching the PR refs)
    ↓
checks green on C_1..C_j → push the branch to C_j,
  expected = C_0, one swap
    ↓
swap failed → queue.reset, rebuild on the new tip
check failed on C_j → queue.left(failed) for PR_j,
  rebuild the suffix without it — the failure is
  isolated to one eviction, not a drained queue
    ↓
append pr.merged per landed PR; delete landed and
  abandoned candidate branches; queue.left(landed)
```

The wake rules that close the loop, in `wake.json`:

```json
{
  "rules": [
    {
      "ref": "refs/hub/pr/*",
      "on": ["review.submitted", "check.completed"],
      "run": ["git+", "queue", "run", "--root", ".", "repo"]
    },
    {
      "ref": "refs/hub/queue/*",
      "on": ["queue.entered", "queue.reset"],
      "run": ["git+", "queue", "run", "--root", ".", "repo"]
    }
  ]
}
```

Waking twice is a wasted start and never a double landing — the swap is
the mutex, exactly as the claim is for tasks. The refs are the truth;
the notification is a hint.

## 7. Failure modes, honestly

**The queue agent dies mid-batch.** Candidate branches linger (bounded
by `queueDepth`), queue events record how far it got, and the next run —
this agent's or any member's — re-derives and continues or resets.
Nothing waits on a lease expiring, because nothing held a lock.

**A check fails on `C_j`.** `PR_j` is evicted; `C_1..C_{j-1}` remain
valid and can land immediately; the suffix rebuilds without `PR_j`. The
per-step checks are what buy this bisection — one bad change costs one
rebuild of the work behind it, not a drained queue.

**A direct push races the queue.** Still allowed — the queue is not a
gate. The swap fails, `queue.reset` is appended, chains rebuild on the
new tip. Repositories that want queue-only landing express it socially
(grant `source.push` on the target sparingly), not through a new rule;
a rule could come later without changing any event.

**A malicious `hub.queue` holder.** Can append misleading queue events
and churn CI on junk candidates; cannot land anything, because the
boundary re-derives every fact from refs it verifies. The blast radius
is spend, bounded by the grant and by the namespace ceilings, and
revocation is the same one-line recourse as for any member.

**Flaky checks.** A re-run is the CI signer's `check.started` +
`check.completed` on the same name and revision; the fold already reads
the latest per key (hub.md §10). The queue re-lands on wake. Nothing
special.

**Two queues for one target.** Descent picks the one that counts, as
with competing openings; the loser's entries are noise, its candidates
ordinary branches to delete. Harmless by construction, so not refused.

## 8. Alternatives considered

**A privileged queue service** (the GitHub shape): a principal whose
pushes bypass the rules. Refused on the design's own axiom — _signed is
not safe_ (hub.md §33); authority would replace verification, the host
would stop being passive, and the property "any replica can check any
landing offline" would be gone. The entire point of rule 3 is that no
principal needs to be trusted with the branch.

**Approval carry by patch identity** (`git patch-id` over a rebase):
refused because patch identity is a duplicate-detection heuristic, not a
semantic guarantee — a rebased patch with identical hunks can mean
something different on a moved base. Tree equality against a
deterministic merge is exact or it is nothing, and when it is nothing
the fallback is a re-review, which is correct.

**Recycling `pr.updated`** (queue rebases the PR, re-proposes): staling
approvals on head movement is a load-bearing property of §18, not an
inconvenience to engineer around; a queue that re-proposed heads would
either drown reviewers or need an approval-carry exception that guts the
rule. The candidate chain leaves `H_i` — and everything reviewers said
about it — untouched.

## 9. What was built

All four phases landed, each green through `npm run check` and `npm test`.
The enforcement went first and alone, because it is the only part that
touches the boundary and it is independently useful: a person can
hand-build a two-pull-request candidate and land it with no queue running
at all.

```text
Phase 1 — the boundary
  src/git/Repository.ts     mergeTree: the merge decision without a
                            commit or a ref, and `merge` rewritten in
                            terms of it so a candidate's builder and
                            its verifier cannot disagree
  src/hub/Projection.ts     checksPassedAt: the revision a check ran
                            against, separated from the pull request's
                            head
  src/server/Policy.ts      authorizes (the direct path's own loop,
                            now shared) and candidateChain: parent
                            shape, per-step rules, checks against the
                            candidate, tree equality, queueDepth
                            ceiling; rules fields queueCandidates and
                            queueDepth, both optional and off

Phase 2 — queue events
  src/hub/Queue.ts          opened / entered / candidate / left /
                            reset, and the projection over them
  src/trust/Certificate.ts  hub.queue
  src/server/Policy.ts      the namespace admitted, hub.queue charged,
                            population counted as its own class
  free rides, verified      append-only enforcement, advertisement
                            hiding, replication join

Phase 3 — the runner
  src/cli/queue.ts          open | enter | leave | run | list | show,
                            with --dry-run; conflicts predicted with
                            the same merge the boundary recomputes
  src/server/Wake.node.ts   the walk decodes an event's tag rather
                            than the pull-request union — see below

Phase 4 — surfaces
  src/server/Whoami.ts      a branch says whether it takes candidates,
                            and how deep
  docs, readme              hub.md §11, the capability list, the
                            command inventory
```

### Two things the implementation settled

**Candidates must be deterministic, and that is load-bearing.** The first
runner stamped each candidate with a wall clock, and the queue could
never land anything: a candidate built, published and tested in one pass
came back with a different object id in the next, so the check bound to
the first named nothing the second held. A candidate is therefore a pure
function of what it merges — a constant identity, and a committer date
taken from the later of its two parents. The second-order benefit is
larger than the fix: two runners building the same batch now produce the
_same commit_ rather than two commits with the same content, so a
candidate one of them published and CI tested is the one the other lands.

**`wake` could not see the namespaces it was extended to serve.** The
dispatcher decoded every record as a pull-request payload, so a
`task.opened` — and any `queue.entered` — read as a record this version
"cannot read" and its rule never fired. agents.md §20's whole working
rhythm was silently impossible. What a rule matches on is the event's
type, and every hub envelope spells that field the same way, so the walk
now reads the tag alone. That was a pre-existing defect this work
uncovered rather than caused; it is fixed and covered by a test.

### Not built

A queue lane in the web UI. `queue list` and `queue show` answer the same
question in JSON, and the screens are fixture-backed in places
(`src/ui/readme.md`), so this waits for the screen work rather than
leading it.

## 10. What this buys

The research framing this answers ranks an exact-candidate tested merge
queue as one of the three capabilities to build first for agentic
development, alongside isolated agent lanes and leased/transactional ref
updates — and this repository already has the other two. The gap was
never safety machinery; it was composition. One new judgement at the
boundary (a tree that must equal a deterministic merge), one new event
namespace on machinery four other namespaces already use, and one new
verb shaped like `wake` turn N approved pull requests from N re-review
cycles into one tested swap — with every landing verifiable offline,
from refs alone, by every replica, which is the property the rest of
this design exists to keep.
