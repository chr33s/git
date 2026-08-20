# Web of Trust: a Social Graph Overlay

**Status:** Proposed (revision 1)
**Scope:** `chr33s/git` — builds on the trust and hub design in
[hub.md](hub.md) (`refs/meta/trust/*`, `refs/hub/*`, the policy boundary,
delegated credentials) and the agent identity model in
[agents.md](agents.md). Nothing here changes hub v1; every construct is an
overlay a repository or a verifier opts into.
**Social namespace:** `refs/social/*` (in a principal's identity repository)

---

## 1. The problem: trust is an island per repository

Hub v1 answers "who may do what **here**" completely: a genesis roots a
quorum, a hash-linked trust log grows grants and revocations, and every
event verifies offline against that one repository's own history. What it
deliberately does not answer is anything **between** repositories or
**between people**:

```text
identity      a member IS their SSH key; rotate the key in one
              repository and every other repository still grants
              the old one. There is no "same person" across repos.

introduction  known_repos is blind TOFU: the first connection to a
              repository is an unverifiable leap, even when people
              you already trust have been using it for years.

discovery     there is no way to find a repository, its mirrors, or
              its forks except out-of-band. A decentralized forge
              with no discovery layer quietly re-centralizes around
              whoever runs the index.

federation    a fork and its parent share objects and nothing else.
              A pull request cannot carry review weight from anyone
              the target repository has not individually granted.
```

A web of trust fills exactly these gaps: **signed, append-only statements
by principals about other principals and about repositories**, replicated
as git refs like everything else, and projected **per verifier** — never
into a global score.

The design constraint carried over from hub.md, restated once because
every section below leans on it: the authoritative representation is git
objects and refs; verification is offline; omission must be visible;
projections are deterministic folds; and no construct may widen authority
that the repository's own policy did not grant.

---

## 2. Principals: identity as a repository

The unit of the social graph is a **principal**, and a principal is an
**identity repository** — the same genesis/quorum/log machinery hub v1
already ships, reused one level up:

```text
PrincipalID = RepoID of the principal's identity repository
            = SHA-256(genesis blob bytes)
```

An individual initializes 1-of-1; an organization N-of-M. The identity
repository's own trust log (`refs/meta/trust/log`) holds the principal's
**device and agent keys** as ordinary grants:

```text
principal genesis (1-of-1, Alice's offline key)
        ↓
trust.grant  laptop key      (expires yearly)
trust.grant  phone key
trust.grant  CI-agent key    (capabilities: ["sign.check"])
trust.revoke laptop key      (reason: "compromised")
```

This buys the thing bare keys cannot: **rotation without loss of
identity, and one revocation that propagates everywhere**. A repository
that granted membership to a `PrincipalID` (§5) rather than to a key
picks up the rotation on its next trust sync; a compromise revocation in
the identity log reaches every repository that resolves through it, with
the retroactive semantics of hub.md §10 intact.

Nothing forces this. A bare SSH key remains a valid subject everywhere it
is one today; a principal is what a key holder graduates to when they
have more than one key, more than one repository, or an agent fleet
(agents.md Part I becomes "grant the agent a key **in your identity
repository**" instead of "grant it in every project").

An identity repository is fetched, pinned and verified exactly like any
other repository: `known_repos` gains entries whose value is a
PrincipalID, and the checkpoint-staleness bound (hub.md §9) bounds how
old a view of someone's key set a verifier will accept for high-value
decisions.

---

## 3. The social log

A principal's statements about the world live in their identity
repository under one append-only, hash-linked ref, shaped exactly like
the trust log — commits whose trees carry one signed payload, parents the
prior head(s), first parent the genesis commit:

```text
refs/social/log
```

Everything hub.md specifies for append-only namespaces applies verbatim:
boundary-enforced no-delete and ancestry-preserving updates, a walk
ceiling, namespace-capability charging (`social.write`, granted in the
identity repository's own trust log), duplicate-ID resolution by descent,
checkpoints, and advertisement hygiene (hidden from v0 source-only
clients, `ref-prefix` under v2).

### Statement kinds

```text
social.attest.repo        URL ↔ RepoID binding, with a role
social.attest.principal   key or name ↔ PrincipalID binding
social.vouch              delegatable, attenuated trust in a principal
social.follow             subscription / replication hint
social.revoke             withdrawal of any prior statement
social.checkpoint         signed frontier attestation
```

All payloads carry the author's PrincipalID, a UUIDv7 statement ID, and
the declared social-log head — the same self-declared-head + floor
discipline as hub events — and are SSH-signed under the existing
namespace with a distinct type field, so a signature over a vouch can
never verify as a grant.

### `social.attest.repo` — introduction

```json
{
  "type": "social.attest.repo",
  "repo": "SHA256:bJd3cN8...",
  "urls": ["https://git.example.com/acme/project"],
  "role": "origin",
  "forkOf": null,
  "issuedAt": "2026-08-20T00:00:00Z"
}
```

"I have verified that this RepoID answers at these URLs." `role` is one
of `origin | mirror | fork`; a `fork` names `forkOf`. This is the
statement that turns blind TOFU into introduction (§4) and URL moves into
recognition, and the statement whose union across a verifier's web is a
decentralized index of repositories, mirrors and fork graphs (§7).

### `social.attest.principal` — key endorsement

```json
{
  "type": "social.attest.principal",
  "subject": "principal:SHA256:9f2c...",
  "publicKey": "ssh-ed25519 AAAA...",
  "claim": "key-of",
  "issuedAt": "2026-08-20T00:00:00Z"
}
```

The PGP key-signing statement, repaid in SSH: "I verified, out of band,
that this key answers to this principal." Useful for bootstrapping a
principal whose identity repository a verifier cannot reach yet, and for
cross-checking one they can.

### `social.vouch` — attenuated, delegatable trust

```json
{
  "type": "social.vouch",
  "subject": "principal:SHA256:9f2c...",
  "scope": ["introduce.repo", "review"],
  "depth": 1,
  "issuedAt": "2026-08-20T00:00:00Z",
  "expiresAt": "2027-08-20T00:00:00Z"
}
```

"Within `scope`, I trust this principal's word — and (`depth` ≥ 1) the
word of those they vouch for, `depth` hops further." Scopes are
capability-shaped and small:

```text
introduce.repo     their repo attestations count for my TOFU decisions
introduce.key      their principal attestations count
review             their reviews may satisfy policies that opt in (§6)
vouch              they may extend my web (this is what depth spends)
```

Two rules make chains safe, and both are the delegated-credential rules
of hub.md §12 generalized:

```text
attenuation   effective scope along a chain is the INTERSECTION of
              every link's scope; effective depth is the MINIMUM of
              remaining depths. A chain can only ever narrow.

no widening   nothing reachable through the web grants a capability
              in any repository. The web supplies candidates and
              confidence; authority is only ever what a repository's
              own trust log and policy say (§6).
```

### `social.follow`

```json
{ "type": "social.follow", "subject": "principal:SHA256:9f2c..." }
```

A follow carries **no trust at all**. It is a replication hint — "fetch
this identity repository's social log when synchronizing mine" — and the
edge that makes the graph traversable for discovery. Keeping follow and
vouch separate is deliberate: conflating subscription with endorsement is
how social platforms turn reach into authority, and the projection rules
below never read follows.

### `social.revoke`

Withdraws a prior statement by ID, with the windowed semantics of
hub.md §10 — every window a statement has been out on is kept, an
ordinary revoke is forward-only from acceptance, and
`reason: "compromised"` on a vouch reaches backwards so that
introductions and reviews that flowed through a compromised link are
re-projected without it.

---

## 4. Projection: every verifier is their own root

There is **no global graph and no global score**. A projection is a fold
a verifier runs from their own configuration:

```text
roots:      my own identity repository, plus principals I have
            explicitly vouched (my direct edges are depth ∞ to me)
statements: the union of social logs I hold locally (fetched via
            follows and on demand)
fold:       breadth-first from my roots along vouch edges only,
            intersecting scopes and decrementing depth per hop,
            dropping revoked windows, stopping at depth 0
answer:     for a (subject, scope) question: the set of INDEPENDENT
            paths that reach it, with each path's effective scope
```

Deterministic given the same statement set — same discipline as hub
projection, and cacheable the same way (memoised against the social-log
heads it read, keyed per verifier, disposable).

**Independent paths are the unit of confidence.** A verifier's policy
for any decision is `minPaths: k` — paths sharing no intermediate
principal — because one bad voucher then only ever contributes one path.
This is the classic web-of-trust marginal/complete distinction made
explicit and tunable.

**Sybil resistance is structural, not statistical.** A million fabricated
principals vouching for each other are reachable from a verifier's roots
by exactly zero paths. The web is safe from inflation precisely because
it refuses to compute anything unrooted; any future ranking that sums
over the whole graph re-opens the hole and is out of scope permanently.

---

## 5. Application: introduction, and cross-repository membership

### TOFU becomes introduction

`git+ hub enable` today, meeting an unknown RepoID, can only ask the
user to leap. With the overlay it first asks the verifier's own web:

```text
The authenticity of repository
  https://git.example.com/acme/project
can be established through your web of trust:

  attested by alice (direct), bob (via alice, depth 1)
  2 independent paths · scope introduce.repo · RepoID matches

Trust this repository? [yes/no]
```

`known_repos` entries gain a provenance field — `tofu` or
`introduced(k paths)` — and a repository moving hosts stops being a
fresh leap: a new URL attested by the same web for the same RepoID is
recognition (hub.md §5's "moves" case, now signed instead of
coincidental). Conflicting attestations for one URL are surfaced as the
split-view warning, never resolved silently. A verifier with no web, or
`minPaths` unmet, falls back to exactly today's blind TOFU — the overlay
strictly adds information.

### Membership by PrincipalID

A repository's `trust.grant` may name a principal instead of a key:

```json
{
  "type": "trust.grant",
  "repo": "SHA256:bJd3cN8...",
  "subject": "principal:SHA256:9f2c...",
  "identityRepo": { "pin": "SHA256:9f2c...", "hint": ["https://..."] },
  "capabilities": ["source.push", "hub.review", "hub.approve"],
  "issuedAt": "2026-08-20T00:00:00Z",
  "expiresAt": "2027-08-20T00:00:00Z"
}
```

Resolution at the policy boundary becomes a two-log question: is the
presenting key currently granted in the subject's identity log, and is
the subject granted here? The machinery this needs already exists:

```text
pinning      the grant pins the PrincipalID; the identity repository
             is fetched and verified like any repository. The hint
             is a hint — identity is the pin, never the URL.

staleness    the checkpoint-age bound (hub.md §9) applies to the
             foreign log: a high-value operation may require a
             fresh-enough view of the member's own key set.

ordering     identity logs synchronize before the repositories that
             reference them — the "trust before hub" rule (hub.md
             §23) gains one earlier stage, and an event whose
             signer's identity log has not replicated yet is
             QUARANTINED and re-validated, not rejected.

permanence   any verdict hub.md holds permanent (tombstones) must
             pin the identity-log head it judged against, exactly
             as it pins the trust head today — a permanent answer
             may not depend on a log that moves.
```

What this buys, concretely: one `trust.revoke` of a stolen laptop key in
Alice's identity repository, and every repository that granted
`principal:alice` refuses that key at its next trust sync — today that is
one revocation per repository, and the ones nobody remembers are the
breach.

---

## 6. Application: federated forks and external review

`social.attest.repo` with `role: "fork"` makes the fork graph a
first-class, verifier-computable object, which enables cross-repository
pull requests without any shared host:

```text
fork F attests   {role: fork, forkOf: R}
origin R's maintainers attest F back (or not — the edge is
  directional, and R's policy decides what F's edges are worth)

a pr.opened in R names a head that R does not hold; the fork
  attestation tells a synchronizing client WHERE to fetch it,
  and object transfer needs nothing new — a pack is a pack
```

Review weight from outside the membership is the sharpest tool here and
the most dangerous, so it is **policy opt-in, per branch, with the web
supplying candidates and the policy supplying the bar** — never the
reverse. In `refs/meta/policy`:

```json
{
  "branch": "refs/heads/main",
  "requiredApprovals": 2,
  "externalReview": {
    "anchors": [
      "principal:SHA256:maintainer1...",
      "principal:SHA256:maintainer2..."
    ],
    "scope": "review",
    "maxDepth": 1,
    "minPaths": 2,
    "maxCount": 1
  }
}
```

read as: at most one of the required approvals may come from a principal
reachable with scope `review` within one hop from **the repository's own
named anchors** — not from the pusher's web, which would let anyone
bring their own reviewers. The projection is the anchors' vouch fold,
run by the policy boundary with the same determinism, floors and
staleness bounds as every other fold it makes; self-approval exclusion
and the capability hierarchy of hub.md §27 apply unchanged. A repository
that never writes `externalReview` never folds it.

---

## 7. Application: discovery and replication

Follows make the graph crawlable, and `attest.repo` statements make it
useful. A client synchronizing its own identity repository also fetches
the social logs of followed principals (bounded: direct follows by
default, `--depth` to go further), and the local union answers:

```text
git+ social find <name-or-RepoID>   repositories my web attests,
                                    with URLs, roles, fork graph
git+ social mirrors <repo>          attested mirrors, freshest first
git+ social who <key>               principals my web binds a key to
```

This is the forge's directory function with no directory: the index is
whatever your corner of the web has said, replicates as refs, works
offline, and degrades to nothing worse than today (out-of-band URLs)
when the web is silent. Hosts MAY additionally run open aggregate
indexes for search; those are caches over signed statements anyone can
re-verify — convenience infrastructure, never authority, exactly the
"projection caches are disposable" rule.

---

## 8. What the overlay refuses to be

Stated as sharply as the hub spec states its limits:

```text
no global reputation   any number computed over the whole graph is
                       a Sybil target and a centralization magnet.
                       Every answer is rooted at a verifier.

no automatic authority the web never satisfies a capability check.
                       It introduces, corroborates and nominates;
                       repositories decide (§6), and a repository
                       that ignores refs/social/* entirely loses
                       nothing hub v1 promised it.

no follow-as-trust     follows are plumbing for replication and
                       discovery; the fold never reads them.

no hidden edges as     the graph is public where it exists at all.
  a privacy story      A social log is replicated, signed history:
                       publishing "alice vouches bob" is a choice
                       made by writing it. Principals who want
                       unlinkable contexts run multiple identity
                       repositories — cheap by design — rather than
                       trusting an overlay to hide edges it
                       replicates. Payload redaction (tombstones,
                       hub.md §21) removes content, never the fact
                       of a statement.
```

And one honest limitation inherited from every offline-verifiable
system: **freshness is best-effort**. A withheld revocation of a vouch
is visible as a stale frontier and bounded by checkpoint age, not
prevented — same property, same mitigation, same sentence as hub.md §9.

---

## 9. Threats specific to the overlay

```text
vouch-chain laundering   attenuation (intersection + min-depth) means
                         a chain never exceeds its narrowest link;
                         minPaths means one subverted principal
                         contributes one path, not a verdict.

introduction poisoning   conflicting attest.repo for one URL is a
                         surfaced split-view, and RepoID (exact
                         genesis bytes) is what is being attested —
                         a lookalike genesis is a different RepoID,
                         so the poison has to be in the verifier's
                         own web to count at all.

identity-repo capture    a captured identity repository is a captured
                         principal — quorum thresholds, offline root
                         keys and the unrecoverable-by-design rule
                         (hub.md §6) apply; repositories that granted
                         the principal bound themselves to that
                         quorum, which is the risk stated plainly.

graph spam / log growth  writing to a social log costs social.write
                         in that identity repository — you can only
                         spam yourself. Ceilings, duplicate-ID
                         descent resolution and walk bounds carry
                         over verbatim; a verifier fetches logs it
                         follows, so unwanted logs are never even
                         held.

cross-log replay         every statement carries the author's
                         PrincipalID inside the signed bytes, as
                         every trust event carries RepoID today.
```

---

## 10. Implementation shape

The point of §2–§3 reusing existing machinery is that this is mostly
composition, in the pattern of [plan.md](plan.md):

```text
src/social/
  Statement.ts       payload schemas, signing (over src/crypto)
  Log.ts             append/read (the trust Log generalized over
                     a namespace, which §23's rules already are)
  Projection.ts      rooted fold: attenuation, depth, minPaths,
                     revocation windows, memoised per verifier
  Introduce.ts       known_repos v2: provenance, introduction
                     resolution, split-view surfacing

src/trust/           subject: "principal:" resolution — the
                     two-log membership walk, quarantine, pinned
                     identity-log heads

src/server/Policy.ts externalReview evaluation at the boundary

src/cli/
  id.ts              git+ id init | rotate | revoke | status
  social.ts          git+ follow | vouch | attest | social find
```

Phasing, each independently shippable and each useful without the next:

```text
1  identity repositories + PrincipalID grants   (rotation story)
2  social log + attest.repo + introduction      (TOFU story)
3  vouch + rooted projection + minPaths         (the web itself)
4  discovery commands + follow-driven fetch     (the forge story)
5  externalReview policy                        (federation story)
```

Phase 1 has no social graph in it at all and is worth shipping alone;
phase 5 is the only one that lets the overlay near an authorization
decision, and it arrives last, behind a policy field nobody has written
yet.
