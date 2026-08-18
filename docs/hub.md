# Git-Native Hub

**Status:** Proposed (revision 2)
**Recommendation:** Adopt
**Scope:** `chr33s/git` — this spec targets the `artifacts` branch architecture (`src/git/Store.ts` `RefStore`/`ObjectStore`, `src/server/Auth.ts`, `src/server/Protocol.ts`, Effect services). The flat `src/` layout on `main` does not contain the constructs referenced here.
**Hub namespace:** `refs/hub/*`
**Trust namespace:** `refs/meta/trust/*`

## Revision 2 changes

This revision resolves the issues raised in spec review:

```text
1. Hub events: per-event refs replaced by one append-only,
   hash-linked event DAG per PR (ref cardinality O(PRs));
   refs/hub/* hidden from source-only advertisement.
2. Trust state: independent grant/revocation refs replaced by a
   hash-linked trust log with signed checkpoints; withholding and
   split-view attacks addressed explicitly.
3. Revocation semantics defined (acceptance-time, non-retroactive
   by default; retroactive "compromised" class).
4. Membership certificates carry validity windows.
5. Quorum-loss consequences stated explicitly.
6. Delegated short-lived credentials promoted to a REQUIRED v1
   component (the stock-git compatibility path).
7. Signed request envelope binds the ref-command list, not
   streamed body bytes; nonce lifecycle specified.
8. Trust refs synchronize before hub refs; unresolved events
   quarantine rather than fail permanently.
9. Redaction tombstones added; GC and replication honor them.
10. Merge policy mandates compare-and-swap (`expected` OID) at
    RefStore.apply; append-only enforcement specified at the
    policy boundary.
11. known_repos entries keyed by URL, value RepoID only.
12. RepoID defined over exact genesis blob bytes (no canonical
    JSON, not a git OID).
13. Check capability scoped per check name.
14. Normative language fixed (MUST for required key type);
    hardware-backed sk-ssh-ed25519 explicitly permitted.
15. SHA-256 object-format support moved out of v1 into Future
    work; v1 targets SHA-1 repositories with hash-qualified,
    format-agnostic payloads.
16. Phases reordered: trust before hub, refspec generalization
    before hub enable, SHA-256 deferred.
```

---

## 1. Core principle

A repository should be a portable unit containing:

```text
source
+ source history
+ repository identity
+ membership / authority history
+ pull requests
+ reviews
+ comments
+ checks
```

The authoritative representation is Git objects and refs.

A hosting provider may maintain databases or indexes, but losing those databases MUST NOT lose repository identity, membership, PR history, review history, or check history.

---

## 2. Cryptographic model

Version 1 uses **SSH public keys and SSH signatures only**.

GPG/OpenPGP and other signature formats are explicitly deferred.

V1 MUST support:

```text
ssh-ed25519
```

as the required signing key type, and SHOULD additionally accept:

```text
sk-ssh-ed25519@openssh.com
```

so hardware-backed keys work without a policy exception. Other SSH signing algorithms MAY be accepted by an implementation.

All signed application payloads use SSH signature semantics (the `ssh-keygen -Y sign` / `-Y verify` model) with a dedicated namespace:

```text
chr33s-git/hub/v1
```

A signature MUST cover the complete canonical payload bytes, including the payload's `type` field. Because the type is inside the signed bytes, a signature over one payload kind (a review) can never verify as another (a grant); no per-type namespace is needed.

A loaded private key MUST be checked for internal agreement before it is used: an OpenSSH key file states its public point three times over — in the outer blob, in the private section, and again in the trailing half of the 64-byte key material — and a reader that takes the seed without comparing them signs under one key while advertising another. Every signature it makes then verifies nowhere, and the failure surfaces as a rejected grant or review rather than as a bad key file, which is the wrong place to go looking.

Private keys never enter the repository.

They remain:

```text
~/.ssh/...
ssh-agent
hardware-backed signer
```

or another SSH-compatible signer.

---

## 3. Repository identity

Every hub-enabled repository has an independent cryptographic identity.

At creation, bootstrap a repository authority quorum.

Example:

```text
Repository
    ↓
genesis
    ↓
3 root SSH public keys
threshold = 2
```

Conceptual genesis document:

```json
{
  "version": 1,
  "objectFormat": "sha1",
  "uuid": "0195a1b2-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
  "rootKeys": ["ssh-ed25519 AAAA... alice", "ssh-ed25519 AAAA... bob", "ssh-ed25519 AAAA... carol"],
  "threshold": 2
}
```

`uuid` is generated once, at creation, and is what makes this repository _this_
repository rather than another one like it. Without it, two repositories created
from the same root key with the same threshold — which is exactly what one
person setting up two projects does — hash to the same `RepoID`: `known_repos`
pins the wrong thing on both, and a certificate or hub event bound to one
verifies against the other, which is what binding them by `repo` was for.

The genesis is stored as a Git object: `refs/meta/trust/genesis` points at a commit whose tree contains a single `genesis.json` blob. The genesis MUST carry SSH signatures from at least `threshold` root keys, proving key possession at creation time; a verifier that cannot meet the threshold MUST refuse the document rather than treat it as an unsigned but usable identity.

The corresponding private keys remain independently controlled by their owners.

There MUST NOT be a shared root seed from which member private keys are derived.

---

## 4. Repository fingerprint

The genesis document produces a stable repository identity:

```text
RepoID = SHA-256(genesis blob bytes)
```

The input is the **exact bytes of the `genesis.json` blob** as stored — never a re-serialization, never a "canonical JSON" transform, and never the blob's Git object ID (which would vary with the repository's object format). Canonicalization is defined by storage: the bytes that were written are the bytes that are hashed.

Example display:

```text
SHA256:bJd3cN8...
```

`RepoID` is independent of:

```text
hostname
URL
Cloudflare deployment
local directory
hosting provider
Git object format
```

Therefore the same repository can move between:

```text
local
↕
self-hosted
↕
Cloudflare
```

without changing identity.

---

## 5. TOFU: SSH `known_hosts` model

Repository identity uses the same trust-on-first-use model users already understand from SSH.

However, repository identities MUST NOT be stored in `~/.ssh/known_hosts`.

Maintain a logically separate store:

```text
~/.config/chr33s-git/known_repos
```

Entries are keyed by URL; the value is the RepoID and nothing else:

```text
https://git.example.com/acme/project SHA256:bJd3cN8...
```

No SSH public key appears in an entry — a genesis holds several root keys, and none of them individually identifies the repository. The RepoID is the identity.

The exact serialization may evolve. The semantics copy SSH `known_hosts`.

### First use

On first hub enablement:

```bash
chr33s-git hub enable
```

the client retrieves the repository genesis, verifies its self-consistency, and calculates its fingerprint.

If no trusted entry exists:

```text
The authenticity of repository
  https://git.example.com/acme/project

cannot be established.

Repository fingerprint:
  SHA256:bJd3cN8kL...

Trust this repository? [yes/no]
```

Acceptance stores the URL → RepoID entry in `known_repos`.

### Subsequent connections

If the repository presents the same genesis:

```text
expected RepoID == presented RepoID
```

the client proceeds.

If it changes:

```text
WARNING: REPOSITORY IDENTITY HAS CHANGED
```

Hub operations MUST fail unless the user explicitly resolves the mismatch. Do not silently overwrite the trusted repository identity.

Editing that file MUST leave every line it does not recognise exactly as it was. It is a user's file: it has comments in it, and the occasional typo. Rewritten by reformatting what parsed, an edit to one pin silently deletes the rest — and a deleted pin is not a cosmetic loss, because the next connection to that repository reads as _first use_, so the identity-changed warning the pin existed to raise never comes.

### Repository moves

Because entries are keyed by URL, a repository reached at a new URL prompts a fresh TOFU decision. When the presented RepoID matches an existing entry under a different URL, the client SHOULD say so:

```text
This repository matches the identity you already trust as
  https://git.example.com/acme/project
```

so a move reads as recognition, not as a brand-new trust decision.

### Separation from SSH `known_hosts`

The two trust systems answer different questions.

```text
SSH known_hosts

hostname
   ↓
SSH server key
   ↓
"Is this the same server?"
```

versus:

```text
chr33s-git known_repos

repository
   ↓
repository genesis/root quorum
   ↓
"Is this the same repository?"
```

These MUST remain independent.

A repository may move to another server without changing `RepoID`. A server may host thousands of independently identified repositories.

---

## 6. Root quorum

Repository authority starts with a quorum rather than one permanent creator.

Recommended default for collaborative repositories:

```text
2 of 3
```

Example:

```text
Alice key ──┐
Bob key ────┼── repository root authority
Carol key ──┘

threshold = 2
```

Sensitive authority operations require quorum approval:

```text
add/remove root authority
change quorum threshold
recover repository authority
replace policy root
```

No individual root key is sufficient once quorum bootstrap is complete.

A single-user repository MAY explicitly use:

```text
1 of 1
```

### Quorum loss is unrecoverable — by design

If enough root keys are lost that the threshold can no longer be met, **repository authority can never change again**. There is deliberately no recovery backdoor: any recovery path weaker than the quorum _is_ the security model.

Consequences that MUST be communicated at initialization:

```text
threshold SHOULD be ≤ ceil(n / 2), so the loss of a minority
of keys does not freeze authority

root key holders SHOULD keep offline backups of root keys

rotating a root key while quorum is intact is cheap;
recovering after quorum loss is impossible
```

An existing repository with frozen authority can still serve source and hub traffic under its last valid trust state; it can also be re-founded with a new genesis — which is, correctly, a new identity requiring new TOFU decisions.

---

## 7. Member certificates

New members generate their own SSH keypairs.

```bash
ssh-keygen -t ed25519
```

Their private key never leaves their machine.

An authorized repository principal issues a signed membership certificate describing their authority.

Conceptually:

```json
{
  "version": 1,
  "type": "trust.grant",
  "repo": "SHA256:bJd3cN8...",
  "subject": "SHA256:bob-key-fingerprint...",
  "publicKey": "ssh-ed25519 AAAA...",
  "capabilities": ["source.push", "hub.comment", "hub.review", "hub.approve"],
  "issuedAt": "2026-08-16T00:00:00Z",
  "expiresAt": "2027-08-16T00:00:00Z",
  "delegation": []
}
```

The certificate is signed using SSH key signing.

### Validity windows

Certificates MUST carry `issuedAt` and SHOULD carry `expiresAt`. An expired certificate no longer authorizes new operations; events it authorized while valid remain historically valid. Renewal is a new grant for the same subject key.

An implementation MAY permit non-expiring certificates (`expiresAt` absent) for single-user repositories; collaborative repositories SHOULD set expiries so that stale members age out even if nobody remembers to revoke them.

---

## 8. SSH signing only

Version 1 supports SSH-key signatures only.

Support for `ssh-ed25519` is a MUST (see §2); other SSH signing algorithms MAY be included.

The canonical signing mechanism follows the SSH signature model used by modern Git/OpenSSH tooling.

GPG/OpenPGP support is explicitly deferred:

```text
v1:
SSH signatures

future:
GPG/OpenPGP
possibly other signing schemes
```

The wire/event formats MUST be versioned so additional signature schemes can be introduced later.

---

## 9. Membership storage: the trust log

Membership and authority records are Git-native immutable records — but they are **not** stored as independent, unordered refs. A bag of `grants/<id>` and `revocations/<id>` refs synchronized by set union has a fatal property: _omission is invisible_. A replica that withholds one revocation ref presents a trust state in which the revoked key is still authorized, and no verifier can tell the set is incomplete.

Instead, trust state is a **hash-linked log**:

```text
refs/meta/trust/genesis     the genesis commit (fixed forever)
refs/meta/trust/log         append-only DAG of trust events
```

Each trust event is a commit whose tree contains the signed event payload, and whose parents are the prior trust event(s) known to the author. The first log event's parent is the genesis commit. Event kinds:

```text
trust.grant        membership certificate issuance
trust.revoke       revocation
trust.root-change  quorum-signed root/threshold change
trust.checkpoint   signed frontier attestation (below)
```

Every trust event payload includes the `RepoID`, so a trust event can never be replayed into another repository.

### Why a log, not a set

Hash-linking means the trust state has a verifiable frontier. Two replicas can compare heads and know whether one has seen everything the other has. Withholding an event is no longer silent — it shows up as a stale frontier.

### Checkpoints

A `trust.checkpoint` event is signed by a principal holding `repo.admin` (or a root key) and attests:

```json
{
  "type": "trust.checkpoint",
  "repo": "SHA256:bJd3cN8...",
  "frontier": ["sha1:abc123..."],
  "issuedAt": "2026-08-16T12:00:00Z"
}
```

Verifiers MAY require a checkpoint newer than a configured maximum age before authorizing high-value operations (merges to protected branches, trust changes). This bounds how stale a split view can be. The bound MUST NOT apply to the two refs that lift it: a checkpoint is how a stale view stops being stale and it lands on the trust log, and the bound itself lives in the rules file. Applied to those as well, the setting is a one-way door — the repository becomes unwritable over the network and neither push that would recover it can be made. Both carry their own capability, so exempting them from this check alone opens nothing.

`issuedAt` is written by the signer, so a verifier MUST reject a checkpoint dated in the future beyond ordinary clock skew (this implementation allows five minutes). A negative age that counts as fresh means one forward-dated attestation satisfies the bound for as long as it is dated ahead — which is exactly the withheld view the bound exists to catch, defeated by typing a date.

For that reason a projection MUST carry **more than one** checkpoint — bounded, and taken from _both_ ends: the newest few by `issuedAt` and the last few in log order — and a verifier walks them newest-first to the first credible one. Both ends, because a bound taken by date alone is evicted by date: a host with a fast clock checkpointing on a schedule fills the list and pushes every credible attestation out, and keeping the tail of the log means a newly pushed checkpoint is always retained however it is dated. Keeping only the greatest `issuedAt` turns the rule above into a freeze: one attestation dated ahead, from a malicious admin or a box with a fast clock, becomes the only checkpoint on record, and refusing it refuses every write on a repository with `maxTrustAgeSeconds` set — the write to `refs/meta/policy` that would lift the bound included. The fold stays clockless; the clock lives in the verifier, which is the only place that has one.

The bound applies to every gated ref write — receive-pack and the JSON verbs alike — and is configured per repository, in the branch-rules document at `refs/meta/policy`, as `maxTrustAgeSeconds`. `0` — the default, and what a rules file written without the field means — is unbounded: a repository that has never checkpointed must keep working, so the bound is something an operator turns on rather than something that arrives with an upgrade. Any positive value refuses every ref update while the newest checkpoint is older than it, including the case of no checkpoint at all.

`refs/meta/policy` replicates with the trust log and for the same reason. A replica that holds the membership but not the rules answers "unprotected" to every question the policy boundary asks, so a mirror sharing one trust graph would let through exactly the pushes its origin refuses.

### Stated limitation

Even with the log, **freshness is best-effort**: a malicious replica can serve a consistent-but-old view up to the checkpoint-age bound. This is an inherent property of any offline-verifiable system and the spec says so rather than pretending otherwise. What the log removes is _silent, unbounded_ omission.

These objects replicate with the repository. A host database MUST NOT be the source of truth for membership.

---

## 10. Authorization graph

Authorization is derived from a chain of signed authority.

Example:

```text
root quorum
    ↓
Alice: repo.admin
    ↓
Bob: source.push + hub.review
    ↓
Bob signs review
```

Verification asks:

```text
1. Is Bob's signature valid?

2. Is Bob's membership certificate valid
   (signature, validity window)?

3. Does the certificate chain reach
   the trusted repository genesis?

4. Was the relevant grant unrevoked when
   this event was accepted?

5. Does Bob have the capability required
   for this operation?
```

Only then is the operation authorized.

### Revocation semantics

There are no trusted timestamps, so "was it revoked at the time?" needs a rule that does not depend on wall clocks:

```text
Default revocation (trust.revoke, reason != "compromised"):
  effective from the moment a replica accepts the revocation
  into its trust log. NOT retroactive. Events accepted before
  the revocation arrived remain valid; events arriving after
  are rejected.

Compromise revocation (trust.revoke, reason == "compromised"):
  retroactive from the stated compromise time (or from grant,
  if unstated). Events signed by the key are invalidated even
  if previously accepted; projections MUST recompute.
```

This makes the answer to "an event signed by a since-revoked key arrives late" deterministic per replica: valid under a default revocation if accepted first, always invalid under a compromise revocation.

A key may be revoked, re-granted and revoked again, so a projection MUST keep **every window a key has been out on**, not merely the latest. A revocation of a key that is _already_ out does not open a second window — it strengthens the one it is in, keeping that window's start, since that is when the key stopped being trusted, and keeping the stronger of the two reasons — learning afterwards that a key was compromised is the reason to send one, and a revocation may only ever be strengthened while open, so a later ordinary revoke MUST NOT relabel a compromise. Appending instead leaves a window nothing can close, because a re-grant closes the latest: live requests are then waved through while every stored event by that key is refused forever. A re-grant _closes_ the window it re-opens rather than erasing it, so events signed inside it stay refused; and a later grant to a key already back in — an ordinary renewal, a widened capability, an extended expiry — MUST NOT move a closed window's end forward, or every event signed against the first re-instatement would stop reaching it and be judged as if made while revoked. Collapsing the windows to one record is the same defect from the other side: a compromise revocation's retroactive reach would be erased by any later ordinary revoke and re-grant.

An event's declared trust head is what these questions are asked against, and it is written by the event's own signer. It is therefore held to a **floor**: the newest head any accepted ancestor of the event named in the same pull request. An event whose head falls short of the floor is read as having seen the floor — not refused. Refusing it would be permanent, since the floor comes from an append-only history, and hub refs and the trust log replicate as separate refs, so a client legitimately lagging one of them writes an older head as a matter of course. Raising it costs an honest straggler nothing and still denies a revoked member the escape the floor exists to close.

Which ancestors raise the floor MUST be decided by a reading that cannot move. "Accepted" under the ordinary reading consults the wall clock, so an ancestor written by a member in good standing dropped out of the floor the day their grant lapsed — and the floor is what a **permanent** verdict is judged against, so a redaction tombstone judged today against a late floor is judged next month against its own older declared head and refused. The host that already deleted the payload is then folding a history no replica agrees with, which is the divergence permanence exists to remove, reached with no attacker and no push. An ancestor therefore raises the floor when it is authorized ignoring expiry — the same reading a permanent verdict uses — and when the head it names is a commit this replica's trust log actually holds. Raising the floor is the conservative direction in any case: an event that falls short of it is read as having seen it, never refused for it.

The floor is the fold's whole answer to a self-declared trust head, and a pull request that has just been opened has no ancestors to raise one. That left revocation with no effect on new pull requests at all: name a pre-revocation head on the `pr.opened`, and it becomes the floor for an approval signed by the revoked key against that same head — the revocation is unreachable from there, the former membership supplies the capabilities it had, and the approval satisfies a protected branch. Nothing in the events says when they were written, so the boundary supplies what the fold cannot: a push to `refs/hub/*` MUST be refused when an event it is **adding** carries a signature from a key this repository has already revoked. Only what the push adds is held to this — a history already on the ref was judged when it arrived, and re-judging it would make a pull request unpushable for good on a namespace that only grows. "What the push adds" means the commits the ref does not already reach, not the commits on the far side of its tip: a walk stopped at a single boundary OID cuts only the chain running through it, and a join has a second parent — so an ordinary reconciling push walked back to the root and re-judged everything. The rule is deliberately about the _signer_ rather than about the declared head: refusing every head that predates a revocation would catch the honest straggler who signed a comment minutes before one landed. It is narrowed again to the events that **move authority**, listed as what is safe rather than as what is dangerous: a comment, a thread and a check say nothing about whether a branch may move, and everything else does. Granting is not the only direction — `pr.closed` takes authority away, since a pull request that is not open authorizes nothing, and `pr.updated` stales every approval by moving the head. A push is not a claim about when its events were written: a replica seeded from elsewhere, or a client that has been offline, pushes a history that is entirely honest and entirely old, and refusing all of it because one past reviewer has since been revoked would make that pull request unpushable for good. There is **no exemption**: every event the push adds is held to it. Three attempts at a safe list — "grants authority", "moves authority", then a list of families — each sprang a leak, because almost everything here feeds a branch rule by some path: checks are keyed by name and revision, so a `check.started` replaces a completed success and flips the branch's checks to failing, and a `comment.created` opens an unresolved thread that `requireResolvedThreads` reads. A rule that has to enumerate the safe cases is a rule that will be wrong again. The cost is the one the tombstone gate carries: a history that is entirely honest and entirely old stops being _pushable_ once one of its past participants is revoked, and still arrives by fetch.

`pr.merged` MUST name a revision the pull request actually proposed — one an accepted `pr.opened` or `pr.updated` set as its head — and MUST be refused otherwise. `merged` is the one state with no way back: `pr.closed` and `pr.reopened` both stop at it, deliberately, because the merge has already landed in the branch. Applied to whatever revision the event happened to carry, a single stray `pr.merged` takes an approved pull request out as the route to its protected branch permanently, on a ref that cannot be rewound — the denial `pr.closed` is guarded against, reached through the one door where closing it again does not help. The check is settled after the fold rather than during it, since "did this pull request propose that revision?" is a question about the whole history: fold order interleaves concurrent branches, so a merge can be reached before the `pr.updated` that proposed what it names, and judging it in place would turn an honest merge into a refusal on ordering alone. A merge refused there MUST also give back the event id it claimed, like any other refused event: settled late is still refused, and an id left held resolves to an event nothing accepted and can never be used by its author again.

The one event judged without the floor is the `pr.opened` that _wins_ the opening, and only that one: which opening won has to be settled before the events are folded, so asking twice would mean asking two different questions and could leave a pull request with a winner it can read no `base` from — the branch freeze this rule exists to prevent, arriving by the other door. Every other `pr.opened`, a retargeting one included, is held to the floor like anything else.

---

## 11. Capabilities

Replace generic:

```text
read
write
```

with operation-oriented capabilities.

Initial candidates:

```text
repo.read

source.push
source.force-push
source.delete

hub.create-pr
hub.comment
hub.review
hub.approve
hub.check:<name>
hub.merge
hub.redact

member.invite
member.revoke

policy.write
repo.admin
```

### Advancing a protected branch

A protected branch is advanced by **pushing the approved revision onto it**,
and by nothing else. A merge commit that merely names an approved head as a
parent proves nothing about content — a merge's tree is unconstrained — so it
is refused; whoever wants one makes it on their own branch, opens a pull
request for it, and has _that_ reviewed. The API's ref-moving verbs are refused
on a protected ref for the same reason from the other direction: they name a
branch and not the revision the rules are about, so at the point they are
gated there is nothing to evaluate those rules against, and a rule that cannot
be evaluated MUST NOT be treated as satisfied.

### What the guard charges, and what the boundary charges

Authentication is a coarse filter and authorization is the precise one, so the
two ask different questions. The guard sees a URL and a method, not a push's
commands, and MUST therefore charge receive-pack (and every other write
endpoint) **`source.push` _or_ `source.delete`** — either is enough to be let
through the door. Which one a particular command actually needs, and whether it
also needs `source.force-push`, is settled at the policy boundary, which reads
the commands. Charging `source.push` alone at the guard makes `source.delete`
unusable as a standalone capability: its holder is refused before the boundary
written to admit them ever sees the request.

The verbs that compute a ref's new value while doing the work — a merge, a
rebase or a cherry-pick with `into` — are charged before the work, so "does
this drop commits?" MUST be asked of the **bases** rather than of the result: a
replay lands on top of `onto`, and a merge commit holds both of its sides, so a
destination either of those already reaches is one the write contains. Asked as
"does the destination exist", an ordinary fast-forward is charged
`source.force-push`; asked by comparing tips, a destination a side reaches
without being it is charged the same way. Both refuse work a `source.push`
holder is entitled to do. And the value the charge was judged on MUST be
carried into the write as its compare-and-swap: the merge or replay in between
takes as long as the history is deep, so a write that re-reads the destination
compares it against itself and cannot fail — a push landing in that window is
overwritten, and a write judged a fast-forward becomes one that drops commits
without ever being charged for it. This is the same rule receive-pack already
follows, where the evaluated old OID travels with the update to `apply`. It
is the ref's **own** value that travels, not the commit it resolves to: those
differ for a symbolic destination, and the store compares the former — so
handing over the resolved OID names a value nobody wrote and fails every such
write as a conflict, for good.

### Verbs that move nothing

A few operations change what a repository _is_ without moving any ref, so the
policy boundary has nothing to judge and the guard's charge — a write being
`source.push` **or** `source.delete`, since it cannot see a push's commands —
is all that stands behind them. Writing a blob or a tree is charged
`source.push`; registering a webhook or a remote (a destination this repository
will send to) and collecting the object store are charged `repo.admin` at the
handler; left at the guard's charge, a bot scoped
to delete a branch could register a receiver for every push the repository
ever accepts, or throw the objects away.

Uploading an LFS object is charged `source.push`, for the same reason as
`blob` and `tree`. Fetching and pulling from an inline remote URL are charged
`repo.read` for a related reason: negotiation offers a `have` line for every local ref, so
pointing one at a URL of the caller's choosing discloses a read-restricted
repository's commit oids, and `source.push` does not carry `repo.read`.

A gate that cannot run yet still refuses what it can — the envelope binding
included, since which refs a signature covers needs nothing from the pack —
and refuses only the commands it is about: a mixed push carrying a create and a delete, from a
principal entitled to the delete alone, loses the create and keeps the delete. The ref rules need the
objects — a fast-forward cannot be told from a force push until the pack is
unpacked, so receive-pack judges after the object phase — but "may this
requester create or move a ref at all?" is knowable from the commands alone
and MUST be asked before the body is read. Otherwise a credential scoped to
delete a branch has its whole pack persisted before the boundary refuses it,
which is the object half of the write the refusal is about.

A gate that covers only _part_ of a verb refuses that part, not the verb: a
repository protecting `refs/tags/*` still has tracking refs to update, so a
fetch takes what it may and leaves the tags, and the answer lists what moved.
Refusing the whole operation for the half it may not do enforces a stronger
rule than the operator wrote.

A repository with no genesis has no membership to charge anything against and
is left exactly as a plain git repository has always been: whether it accepts
writes at all is the host's decision, made once.

### Scoped check capability

`hub.check` is scoped **per check name**: a CI principal granted `hub.check:test` may sign `check.completed` events only for the check named `test`. An unscoped `hub.check:*` MAY be granted but SHOULD be reserved for trusted infrastructure. Without scoping, any CI bot could sign any required check, and merge policy would be only as strong as the least-trusted bot.

Capabilities MAY be grouped into convenient roles in the UI.

Roles are presentation. Capabilities are the authorization primitive.

---

## 12. Authentication

A request MUST prove possession of the private key corresponding to the claimed SSH public key.

There are two authentication paths, and **both are required in v1**:

```text
native path     chr33s-git clients: SSH challenge-response
delegated path  stock git clients: short-lived signed credentials
```

### Native path: challenge-response

```text
server nonce / request context
          ↓
client SSH-signs envelope
          ↓
server verifies SSH signature
          ↓
member public key
          ↓
repository trust graph
          ↓
capability check
```

Authentication proves:

```text
"this request controls Bob's private key"
```

while membership proves:

```text
"Bob's key is authorized by this repository"
```

These are separate checks.

### Signed request envelope

Signatures MUST bind authentication to the request being authorized. The envelope binds the **ref-command list, not the streamed body bytes**: a receive-pack body streams, and hashing it before sending would force the client to buffer the entire pack. The pack's contents are already bound transitively — each command names the exact new OID, and the server verifies the pack against those OIDs.

Each command carries an old OID and a new OID, and **both** MUST be checked against the command line the client actually sent. Checking only the new one leaves the compare-and-swap unsigned: a signed "move `main` from A to B" replays as an unconditional "set `main` to B", which lands the push on a branch that has moved on since the client looked — the exact race the old OID exists to lose.

A repository with **no identity** has no membership to charge an administrative capability against, and the guard lets every read through on one, exactly as a plain git repository has always done. The host's own decision MUST stand in for the membership there is none of: a repository the host opened to anonymous writes may be administered by anyone, and one it did not may be administered by no one. Read as "no membership, no charge", a plain repository served read-only published everything the next paragraph is about.

Reading a repository's administrative registries — where it delivers webhooks, which remotes it pushes to and under what standing instruction — MUST cost what registering an entry costs. Listing looks like a read and is not: it hands back delivery URLs and remote addresses, often internal ones, that were never meant to be published, and charging the write while leaving the read open publishes them to everybody the repository lets in at any level.

An endpoint charged the read capability MUST be bounded by something the request names — one revision's tree, one history — and not by the size of the store. `POST` is not the same thing as "writes", and endpoints that take a body only because their inputs do not fit in a URL are properly charged a read; but a whole-store operation such as `fsck` is charged for what it costs rather than for what it changes, since charging it a read makes it anonymously drivable in a loop on exactly the repositories that most want to be readable — one whose members hold no read capability, or one with no genesis at all. The endpoint behind it is charged the same way `gc` is, and for its own reason rather than for `gc`'s: a verb that reads the whole store has no ref for a gate to hang off, so without a capability of its own any contributor who may push could drive a full scan in a loop.

Where a verb takes a destination to land on, that destination is a **branch**, and an object id MUST be refused rather than qualified. Nothing moves an object id, so qualifying one creates a branch named after it — silently, and reported as success.

The membership graph the ref boundary judges against MUST be the one in force when it runs, not the one the guard reached. Reusing the guard's fold is right and worth it — it is an Ed25519 verification per signature per record — but what makes the reuse safe is the trust log's head, not the repository's identity: between the guard and the boundary sits the whole pack upload, and on a host that serializes per repository, that queue as well. A revocation landing in that window was invisible, so an approval from a key compromised in the meantime still satisfied a protected branch. Checking the head costs one ref read, and the fold behind it is already memoised by that same head.

A nonce MUST NOT be recorded as spent until the signer has been established as a member. Single use is enforced by a bounded store of spent nonces, so a store an unauthenticated caller can fill is one whose oldest entries fall out — taking with them the record that a genuine nonce was used, inside its own lifetime. Issuing costs nothing to remember: a nonce carries its own expiry and a tag only its issuer can make, so "did I issue this, and is it still good?" needs no memory at all.

A full spent-nonce store MUST refuse the request rather than evict an unexpired entry. Evicting looks like the polite failure — one client retries — but the entry it drops is the record that a nonce has been used, and dropping it re-opens the replay window inside that nonce's own lifetime, on demand, for whoever filled the store. Refusing fails closed instead, and since every entry falls out by its own expiry the ceiling is really a rate: that many authenticated requests per nonce lifetime.

The signed envelope MUST include:

```text
repository RepoID
operation (e.g. git-receive-pack, api:pr.merge)
ref commands: (ref name, old OID, new OID) triples,
  where the operation moves refs
request digest, for small non-streamed JSON bodies
server nonce
expiration
```

Example conceptual payload:

```json
{
  "repo": "SHA256:bJd3cN8...",
  "operation": "git-receive-pack",
  "commands": [["refs/heads/main", "sha1:0123...", "sha1:89ab..."]],
  "nonce": "...",
  "expiresAt": "..."
}
```

This prevents a valid signature from being replayed against another repository, another operation, or other refs.

### Transport

The envelope and signature travel in the `Authorization` header:

```text
Authorization: Hub-SSH-v1 <base64(envelope)>.<base64(ssh signature)>
```

### Nonce lifecycle

```text
nonces are issued by the server with a short expiry
nonces are single-use: consumed on first successful
  verification, rejected thereafter
the server tracks issued nonces only until expiry
```

A Durable Object tracks nonces in its own storage; the Node backend tracks them in its state store. The tracking window is bounded by the expiry, so state stays small.

### Delegated path: short-lived scoped credentials

Stock `git` cannot perform challenge-response over smart HTTP. It can present a Basic password. Therefore v1 MUST provide credentials that stock git can present and that **derive entirely from repository authority** — no server secret, no token registry.

A delegated credential is an SSH-signed capability attestation by a member key:

```json
{
  "type": "auth.delegate",
  "repo": "SHA256:bJd3cN8...",
  "capabilities": ["source.push"],
  "expiresAt": "2026-08-16T13:00:00Z",
  "nonce": "..."
}
```

encoded compactly and presented as the Basic password (or Bearer token). Verification is stateless: check the SSH signature, walk the trust graph for the signing member, intersect the attested capabilities with the member's own, check expiry. The credential can never exceed the authority of the member who minted it, and revoking the member revokes every credential they minted.

`chr33s-git` mints one on demand:

```bash
chr33s-git credential --capability source.push --ttl 1h
```

and can act as a `git credential` helper so stock git picks it up transparently. git does not take a password on a command line: it runs a helper and speaks a line protocol at it — `key=value` on stdin, a blank line, the answer the same way — with the operation as an argument. The helper MUST answer `get` with a freshly minted credential, and MUST succeed silently on `store` and `erase`: there is nothing to store, since the credential is minted from the key on every ask and expires by itself, and exiting non-zero there reports a failure for a push that worked. Where the caller names no repository the helper MUST take the one git is asking about, from the `path` it supplies — spelled the way the server reads it, trailing `.git` stripped, or every clone URL written the ordinary way names a repository that does not exist — so a single configured helper serves every repository on a host — which requires `credential.useHttpPath`, since git's default is to identify a credential by protocol and host alone and a credential scoped to one repository cannot be minted from that. Where neither is available the helper MUST say which of the two is missing rather than fail obscurely on every push.

Properties:

```text
short TTL (minutes to hours; 24h maximum)
scoped to one repository (RepoID inside the signed bytes)
scoped to explicit capabilities
verifiable offline against the trust graph
no server-side mint secret, no registry, no revocation list
  beyond membership revocation itself
```

Within its TTL a delegated credential is a bearer credential; the TTL and scoping are the containment.

---

## 13. Eliminate generic repository tokens

The existing generic repository token model on the `artifacts` branch:

```text
credential
   ↓
read | write
```

(`hmacMint` / `hmacVerify` in `src/server/Auth.ts`, the `Tokens` registries in `src/artifacts/Namespace.ts` and `src/artifacts/Sqlite.ts`, and the `Scope` type) is removed as repository authority:

```text
Bearer repository tokens minted from a server secret
Basic-password repository tokens
HMAC-minted read/write credentials
generic read/write scopes
```

The repository no longer relies on a shared server secret to mint repository authority.

Authority comes from:

```text
repository root keys
      ↓
signed membership graph
      ↓
SSH private-key possession
```

The delegated credentials of §12 are not an exception to this rule — they are its application: they carry an SSH signature by a member key and verify against the trust graph, not against a server secret.

---

## 14. Public repositories

Public repositories MAY permit unauthenticated:

```text
clone
fetch
read-only JSON/API operations
```

according to repository policy.

Authentication is required when an operation requires a capability:

```text
anonymous
    ↓
repo.read ✓ (public repository)

anonymous
    ↓
source.push ✗
```

A repository with **no genesis** has no membership to grant anything, so nobody
holds `source.push` on it: it is readable by anyone who can reach it, exactly as
a plain Git repository has always been, and writable over the network by nobody.
Hosts MAY offer an explicit opt-in for scratch servers (`serve --open`), and it
MUST be off by default — "no policy" is not "no protection", and the operator
who wants an open write endpoint should have to say so.

An open repository is still subject to the branch rules it publishes, and so is every door onto it. Applied at receive-pack alone, the two disagreed about the same file on the same repository: `reset` and a branch or tag delete honoured `refs/meta/policy` while `commit`, `branch`, `tagCreate`, a merge, rebase or cherry-pick with `into`, `fetch`, `pull` and commit-pack ignored it. Those verbs name a branch rather than a revision, so what a protected branch has to say to them is that it does not move that way at all. The
approval half of protection cannot apply — there is no membership for a review
to come from — but "this branch may not be deleted" and "this branch may not be
force-pushed" ask nothing of trust, and a boundary that stops consulting
`refs/meta/policy` the moment the repository has no genesis leaves the file
inert on exactly the repositories that have no other protection at all. The
rules ref is itself writable there, so this guards a mistake rather than an
adversary; that is still the difference between a protected branch and no branch
protection whatsoever.

A **private** repository requires `repo.read` even for fetch. Stock git clients satisfy this with a delegated credential (§12) scoped to `repo.read`; native clients use either path.

---

## 15. Hub enable

A stock Git clone remains source-only by default.

An existing clone opts into hub synchronization with:

```bash
chr33s-git hub enable
```

The command adds:

```ini
[remote "origin"]
    fetch = +refs/heads/*:refs/remotes/origin/*
    fetch = +refs/hub/*:refs/hub/*
    fetch = +refs/meta/trust/*:refs/meta/trust/*
```

without replacing existing fetch refspecs.

The command MUST be idempotent. It SHOULD immediately fetch trust and hub refs unless `--no-fetch` is supplied.

**Dependency note:** this command requires refspec-generalized fetch (§23). The current client fetch path hard-codes heads/tags selection (`src/client/Fetch.ts`); generalization is a prerequisite, and the client configuration model must support multiple fetch refspecs per remote. The implementation phases order accordingly.

Recommended related commands:

```bash
chr33s-git hub disable
chr33s-git hub status
```

`hub disable` removes only the refspecs managed by `chr33s-git`. A client that keeps no per-remote configuration — where every fetch names its own refspecs — has no refspecs to remove, and there the same rule reads as the refs: it MUST remove exactly what `hub enable` fetches — the hub refspecs themselves, the rules file among them — plus the scratch ref a presented genesis lands in while it is still only a claim, and nothing else, never a branch or a tag. Naming the namespaces by hand instead of deriving them from the refspecs leaves the origin's branch rules behind, outliving the identity that could have changed them. Deleting those refs is the one place this client does what the policy boundary refuses a push, so it MUST be guarded by the pin: a repository this client enabled against a URL got that state from somewhere else, and one that `hub init` created has no pin naming itself. The pin is about the _URL_, and what gets deleted is a _directory_, so the two MUST also be checked against each other: the identity the directory holds has to be the one the URL was pinned as. Without that, a mistyped local name points the one command that removes an identity at somebody else's repository, and there is no undo — the genesis and the trust log are gone, and a served repository that has lost its genesis answers every read anonymously. The pin survives — dropping trust is `hub forget`, and a repository whose hub state you have stopped fetching is not one whose identity you have stopped believing.

### Trust establishment

`chr33s-git hub enable` also establishes repository trust:

```text
discover origin
    ↓
fetch repository genesis
    ↓
calculate RepoID
    ↓
check known_repos
    ↓
TOFU confirmation if new
    ↓
configure trust + hub fetch refspecs
    ↓
fetch trust log, then hub state
```

If the presented `RepoID` conflicts with the pinned one, `hub enable` MUST fail.

---

## 16. Hub representation

Canonical hub state consists of immutable Git objects reachable from **one append-only ref per pull request**:

```text
refs/hub/pr/<pr-id>
```

Do not use a central mutable PR document. Do not use one mutable Git-notes root as the authoritative store. Do not make an application database authoritative.

### Why not one ref per event

Revision 1 proposed `refs/hub/pr/<id>/events/<event-id>` — one immutable ref per event. That gives trivially convergent set-union sync, but ref cardinality grows O(total events): an active repository accumulates tens of thousands of refs, and the protocol v0 advertisement (`src/server/Protocol.ts`) sends **every ref to every fetcher**, including stock clients who never opted into hub. The advertisement cost lands on exactly the clients who benefit least.

### Event DAG per PR

Instead, each PR ref points at the head of a hash-linked event DAG:

```text
event commit
  ↓ tree
event.json  (signed payload)
```

Each event commit's parents are the prior event head(s) known to the author. The first event (`pr.opened`) has no hub parent. Appending an event is a fast-forward of the PR ref.

Properties:

```text
ref count is O(PRs), not O(events)
events carry causal ordering by construction
the DAG merges like any commit DAG when replicas diverge
event objects remain immutable — appending never rewrites
```

Concurrent appends on different replicas produce divergent heads; synchronization joins them with a **join commit** — a commit with both heads as parents and a marker tree containing no `event.json`. Join commits are synchronization artifacts, not events; projection ignores them and walks through them.

### Append-only enforcement

The policy boundary (§25) MUST enforce, for every `refs/hub/**` ref:

```text
no deletion (zero-OID commands rejected)
every update's new value MUST contain the old value
  in its ancestry (fast-forward, or a join commit)
```

History under a hub ref can therefore only grow.

### Pull request identity

Pull requests use globally unique IDs generated offline — UUIDv7:

```text
0194f59d-4b7a-7c95-a9e5-b358272cb204
```

An ID MUST be a single ref path component: it becomes `refs/hub/pr/<id>`, and
one containing a `/` lands on a ref nothing lists — so the pull request can
never be found, the branch it targets becomes unpushable (a pull request that
cannot be found approves nothing), and its tombstones are never honoured.

A host MAY separately display human-friendly numbers such as `#42`, but these are presentation metadata, not canonical identity.

Every event payload carries its own UUIDv7 event ID. Event identity binds ID to content: two objects claiming the same event ID with different content is an integrity conflict that MUST be surfaced, not merged over.

Where several commits carry one record — the same statement committed twice, which append-only replication makes possible — the one that _owns_ it (whose commit becomes the grant or revocation an event's trust head must reach) is decided by descent: most descendants, then most ancestors, then lowest OID. Copies are grouped by ID **and payload bytes only**: putting the signatures in the grouping key made a copy carrying one extra junk signature a group of its own, escaping the resolution entirely. The number of copies whose signatures are consulted is bounded, and the bound MUST be taken in that same descent order rather than in walk order: taken in walk order it becomes the attack, since parentless replays sort early and a handful of unsigned ones fill the list, leaving the genuine record folded with no signers and rejected for want of a quorum.

For the same reason authority is checked against the **union of the signatures every copy carried** — they all sign the same bytes, so they are all endorsements of the same statement, and requiring them to be in the winning copy would let a copy that _drops_ the signatures strip a revocation's authority by winning descent. Descent alone is not enough, because a replay has to arrive as a join over both copies and a join descends from both: where the targeted record is the log head the counts tie, and the decision falls to an OID whoever writes the replay can grind. Depth breaks the tie — the genuine record hangs off the log's history, and a copy grafted in beside it cannot be given that history without becoming a descendant of the record it is trying to displace.

An ID is chosen by whoever writes the record, so "already applied" MUST be keyed on the ID **together with the record's bytes**, and a hub event's ID additionally on its signer. Keyed on the bare ID, re-using one becomes a weapon rather than a mistake: on the trust log, any member holding a trust capability publishes a record they are entitled to make under the ID of a revocation naming them, and the revocation is discarded as a duplicate on a ref that can never be rewound. Two records sharing an ID but not their content are two different claims, and each is answered on its own authority; what the ID check exists for is the same statement committed twice. A cross-reference to an ID two events claim resolves to **whichever claim the others descend from** — an append is written onto the ref's head, and a push may not graft a beginning beside it, so a later duplicate descends from the original. Refusing such a reference outright instead let any `hub.comment` holder pick a thread's ID, comment under it, and leave every later `comment.resolved` for it rejected for good: a branch requiring resolved threads could never be satisfied again.

An ID slot is taken by an event the projection **accepted**, and by no other. An
event refused on its own terms — one whose capability check or whose per-type
rule turned it away — MUST leave the slot free. Taken on arrival instead, a
refusal spends somebody else's ID: replaying a pull request's own signed
`pr.opened` as a parentless commit with a ground-down OID makes it fold first,
claim the ID, and be refused for not being the winning opening — and the genuine
opening is then discarded as the duplicate, leaving the pull request with no
`base`, no `head`, and the branch behind it unpushable on a ref that only grows.

### No mutable PR head ref

Do NOT make this canonical:

```text
refs/hub/pr/<id>/head
```

Instead, `pr.opened` and `pr.updated` events record the proposed revisions, and the current head is **derived**: the causally latest `pr.opened`/`pr.updated` event in the DAG. If two such events are causally concurrent (neither is an ancestor of the other), the event with the lexicographically greatest event ID wins — UUIDv7 makes that approximately "latest created", and the rule is deterministic on every replica holding the same event set.

---

## 17. Hub events

Each event is a commit whose tree contains `event.json` and whose parents place it in the PR's event DAG (§16).

Possible event types:

```text
pr.opened
pr.updated
pr.closed
pr.reopened
pr.merged

review.submitted
review.dismissed

comment.created
comment.replied
comment.resolved
comment.reopened

check.started
check.completed

event.redacted
```

Hub events MUST be SSH-signed by their author where authorship is security-relevant:

```text
review.submitted
review.dismissed
check.completed
event.redacted
membership grant
policy change
```

and SHOULD be signed everywhere else. The signature covers the canonical `event.json` bytes, type field included (§2).

---

## 18. Reviews

A review MUST reference the exact revision reviewed.

```json
{
  "type": "review.submitted",
  "pr": "0194...",
  "head": "sha1:abc123...",
  "decision": "approve"
}
```

An approval means:

```text
Alice approved abc123
```

not:

```text
Alice permanently approved PR #42
```

Therefore when the PR head changes, the old approval remains historically valid but becomes stale for the new revision.

The **base** stales it too. A reviewer approves a revision _for a destination_, and the destination can be rewritten afterwards by a second `pr.opened` from the pull request's own author — which needs no capability beyond the one that opened it. Compared on the head alone, an approval given for `refs/heads/docs` then authorized pushing that same revision to `refs/heads/main`. So a review records the base in force when it was made, and retargeting the pull request stales every review given for the branch it left. "When it was made" is a question about **descent**, not about walk position: fold order breaks ties by OID, which whoever writes the commit grinds, so a sibling `pr.opened` ground below an existing approval folded first and the approval was recorded against the base that sibling chose. A review is given for the base named by the last opening the review **descends from**, and the pull request's own title, description and base likewise come from the opening that wins descent rather than from the last one the walk reached — the head has always been decided that way, and for the same reason.

---

## 19. Comments and checks

Inline comments SHOULD record their original revision and diff location:

```json
{
  "head": "sha1:abc123...",
  "path": "src/git/Repository.ts",
  "side": "new",
  "line": 184,
  "contextHash": "..."
}
```

Replies reference an event/thread ID. Resolution is another immutable event: `comment.resolved`.

CI/check results are immutable events bound to exact object IDs:

```json
{
  "type": "check.completed",
  "head": "sha1:abc123...",
  "name": "test",
  "provider": "buildkite",
  "status": "success"
}
```

A `check.completed` event is only valid if its signer holds `hub.check:<name>` for the event's `name` (§11). Merge policy MUST verify the capability scope, not merely the presence of a check event.

The Git host does not need to execute CI.

---

## 20. Projection

Current hub state is derived from immutable events:

```text
refs/hub/pr/<id>
        ↓
 walk the event DAG
        ↓
  validate events
 (signatures, trust)
        ↓
  project state
```

Example API:

```ts
Hub.project(prId);
```

Projection ignores join commits, orders events causally, applies the §16 tiebreak for concurrency, and drops events invalidated by compromise revocations (§10) or redaction tombstones (§21).

Projection caches MAY exist. They are disposable.

---

## 21. Moderation and redaction

Immutable-forever collides with reality: a comment can contain credentials, harassment, or content that legally must be removed. "Just delete it" does not survive replication — under any union-style sync, a deleted event resurrects from the first replica that still has it.

Redaction is therefore a first-class, signed, replicated operation:

```json
{
  "type": "event.redacted",
  "target": "0195...",
  "targetCommit": "sha1:abc123...",
  "reason": "sensitive-content",
  "redactedAt": "2026-08-16T12:00:00Z"
}
```

Both fields are signed, and `targetCommit` is the one that resolves the target.
An id alone stops resolving the moment the removal happens — the payload an id
is read from is the payload the tombstone deletes — so a projection rebuilt
afterwards would lose its own target and quietly stop excluding the blob from
packs and collection, which is the removal undoing itself. A commit is stable
across exactly the change the tombstone makes. `target` remains for display and
for the operator naming an event they want gone; a name that two events answer
to is refused before a tombstone is written.

signed by a principal holding `hub.redact` (or `repo.admin`), appended to the same event DAG as any other event.

Effect of a valid tombstone for event `E`:

```text
E's event.json blob is deleted from object storage by `gc`
E's commit and tree remain — the DAG's hashes stay intact
projections read E as absent — on every replica, whether
  or not the blob is still there
replicas MUST NOT serve, re-fetch, or re-accept E's blob
GC excludes E's blob from reachability protection (§27)
```

A tombstone excludes a payload from _the hub's_ reachability, not from the
repository's. Git dedupes by content, so a redacted payload can be the very
object a branch names — post the comment, commit the same bytes as a file,
then have the comment redacted — and treating the exclusion as "delete this"
leaves the source history dangling. The exclusion a _fetch_ takes is a different set again: it says "this is not
here, walk past it", so it holds only what a tombstone covers **and this
repository no longer has**. Handing a fetch the whole set dropped an object
the pack genuinely needs, and the client rebuilt a tree pointing at nothing.

The removal happens in one place — `gc`, which walks the refs the
exclusion is not about, `HEAD` and the reflogs included, and keeps what those
reach. Deleting the loose copy the moment a tombstone is written cannot answer
that question without reproducing the walk, and a packed copy needed `gc`
anyway, since a pack cannot give up one object without being rewritten.

Because Git is content-addressed, absence composes: the commit references the tree, the tree references the blob's hash, and the blob object is simply gone. Verifiers treat a missing blob as valid **only** when a valid tombstone covers that event; a missing blob without a tombstone is corruption. That applies to **both ends of a transfer**: a strict object closure over a hub ref fails the moment anything in it has been redacted, so the side building a pack — the server answering a fetch and the client packing a push alike — walks strictly first and retries once against what the tombstones account for. Computing the exclusion up front instead would fold the trust log on every transfer, including the overwhelming majority that touch no hub ref at all; an absence no tombstone covers fails the retry too, which is the corruption it is.

Redaction removes content, never structure: the event's existence and position in history remain visible, and the tombstone naming it is itself a permanent record that something was removed. Destroying structure would break the hash chain and is not offered.

**A redacted event contributes nothing to the projection, and MUST do so everywhere.** The host that performs a redaction deletes the payload at once; every replica keeps it until the tombstone reaches them and their next repack. A projection that decides absence by whether the _bytes_ are present therefore gives two answers to the same question — a redacted approval counting on one host and not on another — and the disagreement lands on the policy boundary, which decides whether a push is allowed. Absence is decided by the tombstone.

That makes the decision itself a fold, since a tombstone counts only if its signer held `hub.redact` and was authorized when they signed it, so a pull request carrying one is folded twice: once to settle which tombstones count, and once with the answer in hand. The first pass reads as absent every commit _named_ by a tombstone payload of this pull request whose signer has _ever_ held `hub.redact`, over a grant history that only grows — the _event's_ own authorization is what the second pass settles, but the signer's capability is a fact both hosts fold identically, so requiring it costs nothing in agreement. _Ever_ rather than _now_, because this set must be monotone: once a tombstone names a target, some host may already have deleted the payload, and an answer that later shrinks — because the redactor was revoked, or their grant lapsed — leaves that host folding a history no replica agrees with. Requiring nothing was too wide a door: a skipped event drops out of the trust floor, so decoy tombstones lower it for the next one and let a tombstone signed under a stale head through, which is how a narrowed `hub.redact` would come back. Requiring "ever held" does not close that door either — it stays open once granted, which is what makes it monotone — so the boundary closes it instead: a push MUST NOT add an `event.redacted` whose signer does not hold `hub.redact` **now**. More generally, a record a constructor can already see is invalid — a capability that is not one — MUST be refused where it is built and not only where it is folded: the fold runs after the write, on a log that only grows, so a name somebody typed wrong is pinned on a ref nothing can rewind and re-read on every membership check thereafter. Generous and monotone is the right trade inside the fold, where an answer that shrinks is a divergence, and the wrong one at the door, where "now" is knowable. The cost is deliberate: a pull request whose history already carries a once-valid tombstone stops being _pushable_ to a host that does not hold it, once its signer's `hub.redact` is narrowed away. Replication is not gated, so such a history still reaches a replica by fetch; what it cannot do is arrive by push. A commit that is itself a tombstone is excepted, and that exception is load-bearing rather than tidy: without it an event could name a valid tombstone, have it read as absent in the first pass, and undo the redaction entirely — putting a payload the operator believes is gone back under `gc`'s protection. Since tombstones are never redactable, every replica sees the same names and the two passes agree across hosts. Repositories with no tombstones, which is nearly all of them, fold once.

**A tombstone's verdict MUST hold for good.** Every other event is re-judged on each fold, and an answer that moves is the conservative reading there: an approval whose author's grant has lapsed stops counting, and nothing was destroyed to reach that. A tombstone is the one statement this repository acts on irreversibly, so the same rule breaks it — `gc` goes back to protecting and serving a payload the operator was told was gone, the fold stops reading the target as absent, and the host that already deleted the blob folds a pull request no replica agrees with, on the boundary that decides whether the branch behind it may move.

So a tombstone is judged against facts that only ever accumulate: the capabilities its signer held at the trust head the event names, and the revocations reachable from that head. Membership **expiry MUST NOT be consulted** — it is read off a wall clock, and two hosts asking at different moments would answer differently about the same repository — and a `compromised` revocation, which reaches backwards everywhere else (§10), MUST NOT un-honour a tombstone already on the record. Forward reach still applies, so a revoked key writes no new ones, and the live gate still refuses an expired or revoked member a tombstone at the moment they ask for it. A pushed tombstone is also held to an **unexpired** grant. A permanent verdict cannot consult expiry — the answer would move on a wall clock, and the host that acted on it would fold a history no replica agrees with — so the door is the only place an expired redactor is turned away — every door, the local command that mints one included, since a verdict that ignores the clock for ever is one an expired holder must never be able to create, and a relayed tombstone from a membership that lapsed would otherwise be honoured and its payload destroyed. A tombstone that records **no trust head** MUST be refused outright. `trustHead` is nullable and means "had seen everything", the conservative reading everywhere else — but a permanent verdict judged that way is not permanent at all: a revocation of its signer refuses it from then on, and a narrowing re-grant shrinks what it is taken to have held. A statement this repository acts on irreversibly has to say what it was judged against. A compromised redactor's past removals stand; the recourse is to rebuild the repository from a trusted point, which is the same recourse as for any other byte they destroyed.

Because the verdict is now a pure function of the genesis, the trust log and the pull request's own events, both exclusion sets MAY be memoised against those refs. The permissive set still differs from the strict one, and not only in the clock: replication is per-ref and arrives in no order, so a replica can hold `refs/hub/*` — trees naming payloads its source already deleted — before it holds the log entry that granted `hub.redact`. Asked the strict question there, nothing accounts for the missing bytes and every fetch of that pull request fails until the log catches up.

It also means redaction is a strong operation and not merely a cosmetic one: removing an event removes what it said, so redacting a review removes its approval and redacting a comment removes its thread. `hub.redact` is charged accordingly.

---

## 22. Git notes

Git notes MAY be used for secondary SHA-specific annotations such as:

```text
provenance
build metadata
static analysis
cached projections
```

They MUST NOT be required to reconstruct PR/review/check history.

Canonical workflow state remains `refs/hub/*`.

---

## 23. Replication

Use Git itself as the replication protocol. No separate PR synchronization protocol is needed.

The existing replication machinery already provides:

```text
ref advertisement
      ↓
want/have negotiation
      ↓
minimal pack
      ↓
object transfer
      ↓
ref update
```

`refs/hub/*` and `refs/meta/trust/*` become supported ref classes.

### Ref classes

```text
refs/heads/*        mutable source state
refs/tags/*         immutable-by-name
refs/hub/*          append-only event DAGs
refs/meta/trust/*   append-only trust log + fixed genesis
```

Append-only means five things, and all five are checked at the boundary. The ref MUST **name** something in its namespace — `refs/hub/` is undeletable as a whole, and only `refs/hub/pr/<id>` is ever counted, folded or listed, so any other name under it is a permanent entry nothing tracks and nothing can remove. Its value MUST **be** a commit of that namespace's own kind, and so MUST every parent an added commit names: `gc` treats every ref as a root, so a source commit pointed at by a hub ref, or named as one's second parent, is an object graph pinned out of reach of collection through a name that can never be deleted — a purged secret among them. All of these are asked on the push that **creates** the ref as well as on the ones that extend it: the create is where a first push could otherwise hang one event commit off a source commit, or bring several competing openings at once, and where only the tip had been inspected. A trust log's anchor — the genesis commit its first record hangs off — is the one legitimate edge out of a namespace, and on every later push it is already reachable from what the ref holds. And the three that follow. An update MUST contain what it replaces. It MUST NOT bring a **new beginning the ref did not already reach** — a commit none of whose parents are edges of _this_ history, which is not the same as a commit with no parents at all: the walk is bounded to the namespace's own commits, so a parent outside it, a fabricated OID included, is not an edge this DAG has, and a graft naming any junk OID would otherwise read as attached. every hub event is written onto the ref's current head, so an append-only history has exactly one beginning — a pull request's `pr.opened`, a trust log's first record — and a push that grafts a second one is not adding to this history but setting another beside it. A fold with two beginnings must then choose between them by something, and on a pull request with no activity yet that can only be the OID, which whoever wrote the commit ground; that is how a `hub.create-pr` holder took the authorship, title and base of a pull request they had no part in opening. And it MUST leave the ref within the **ceiling a fold will walk**: folding builds an ancestor set per commit, and how many commits a pull request has is chosen by whoever may append to it. That bound belongs here as well as at the fold — applied only at the fold, it converts a slow push into a bricked pull request, and anybody holding the lowest hub capability could take somebody else's approved one past the line and freeze the protected branch it was the only route to. A history that arrived by replication is not held to it, so a fold that refuses one pull request MUST be read as one candidate missing rather than as a refusal of the push — at the cheap pre-filter that runs in front of the fold as well as at the fold itself, and on the walks `gc` and a deepening fetch make across every pull request whether or not anything in them is redacted. Read as a failure at any of those, one over-sized pull request refuses every push to every protected branch on that replica, or takes out collection for the whole repository, which is the denial the ceiling exists to prevent reached through the ceiling itself. The ceiling is therefore part of any memoised answer: two hosts with the same refs and different ceilings hold different answers. The check applies to the push that **creates** the ref as well as to the ones that extend it — a create can otherwise bring a history of any size onto a namespace nothing can delete — and to the **trust log** as well as to `refs/hub/*`: the log is append-only, needs only `source.push` to grow, ranks every duplicate statement by a walk per copy, and is read on every membership check, every push and every collection. Its ceiling is the larger of the two, because a repository's membership history is meant to outlive its pull requests. The **number** of pull requests is bounded too: the per-pull-request ceiling bounds one fold, and this bounds how many folds a protected-branch push, a collection and a deepening fetch each have to make. `refs/hub/pr/*` is append-only, so a closed pull request costs the same as an open one and the list only ever grows, which makes opening them the cheapest way for anybody holding `hub.create-pr` to make every later push slower for good. It is set where a repository that size is already unusual, because reaching it means no new pull request can be opened. A push is judged in full before any of it is applied, so this bound MUST count the creates the batch itself carries as well as what the store already holds: read from the store alone, every create in one receive-pack sees the same pre-push count and every one of them passes, which makes a bound of 65 536 a bound on nothing. What counts is what the batch is actually **allowed**, not what has passed the rule that reads the bound: a create refused by a later rule — a protected ref, a signature from a revoked key — must give its slot back, or one refused command silently spends the allowance the command beside it was entitled to. `refs/hub/*` is undeletable, so what one such push costs every later protected-branch push, collection and deepening fetch is permanent. The log's ceiling is applied at the boundary and **only** there. A pull request past its ceiling is one candidate missing, which every caller carries on without; a membership log past its ceiling has no such reading — refusing to fold one leaves a repository nothing can be authorized against, on a ref nothing can shorten, and replication writes refs without passing the boundary at all. So the log's fold walks whatever it is given, and the bound is applied where it can be applied without turning an unreadable log into an unusable repository: the push that would grow one.

A rules file that exists and will not parse MUST fail rather than read as no rules — otherwise anybody who can corrupt it can turn branch protection off. That failure MUST NOT extend to the rules file itself. The rules are read before any per-ref decision, so failing closed on all of them refused every write on the repository including the corrective push, and the only way back was filesystem access to the host. A holder of `policy.write` may always write `refs/meta/policy` while the published rules are unreadable; everything the rules govern stays refused. On a repository with no identity that the host has opened to anonymous writes there is nobody to hold `policy.write` at all, so whoever may publish the rules there may repair them — otherwise the one client that can write them can also lock the repository against itself. And the exemption has to carry the compare-and-swap the ref actually holds: a repair aimed at a ref that exists, offered against "must not exist", is allowed by the boundary and refused by the store — reported as landed, and never landing. The rules have nothing to say about their own file in any case, which is why the staleness bound already exempts it.

Any answer memoised across requests MUST be keyed by the **repository** — the host's own identity for its storage, not merely its genesis OID — and MUST hold the ref values it depends on as state to compare rather than as part of the key. Content-derived identity is not unique per repository on a host that serves mirrors: a mirror is made by copying `refs/meta/trust/*`, so it has the same genesis bytes and the same RepoID as its origin, and right after a replication the same hub ref values too. What the two can actually _read_ need not agree, since refs are applied without a connectivity check — so under a shared key whichever folded first answers for both, and an answer computed on the replica that is missing objects is served for the origin: a revocation folded away, an exclusion set computed for the wrong repository, an approved pull request filtered out of a protected-branch push. That identity has to actually reach the code that keys on it: a composition that consumes the storage layer without re-exporting it leaves every memo keyed on nothing, and the aliasing comes back with no symptom to notice — the wrong answer is a well-formed one. Its ceiling MUST then count **repositories**, not the entries within one. A protected-branch push sweeps every pull request the repository has, so a ceiling shared flat across every repository the host serves — and set below the population a repository may reach — is smaller than a single sweep: every push misses on every key and re-walks every event DAG synchronously, which is the cost the memo exists to remove, arrived at through the memo itself, and one busy repository evicts every other one the host serves. Both bounds are kept, and they bound different things: a hard cap on what is retained in total, set at the population bound so that one sweep always fits, and a cap on how many repositories' answers are held at once. Whichever is exceeded, what is evicted is a **whole repository**, least recently used — never an entry belonging to the sweep that is running, which is the flat cap's failure. A single repository at the population bound may therefore sit over the total on its own, with nothing else kept beside it. A host serves many repositories from one process: they number their pull requests from one, so an identifier alone is not a name, and a fork and its parent point at the same commits under different refs, which an OID does not separate either. Keying on the ref value instead of comparing it is what turns a bound written in pull requests into a bound in revisions — every append leaves the answer for the head before it behind, so a repository far inside the population bound turns the memo over on ordinary activity and pays again for the walk the memo exists to avoid, on the synchronous receive-pack path. Compared, an append overwrites; and a moved ref is still a miss, so a stale answer remains impossible.

`refs/meta/trust/` holds exactly two things — the genesis and the log — and a push MUST refuse every other name under it, whether or not the repository has an identity. The log is one ref, not a namespace: a name spelled _underneath_ it is one of those other names, and treating the prefix as append-only let exactly that through the check — unbounded in count, since the population bound covers only `refs/hub/`. Only the log is append-only, so none of the rules above bound any other name; the JSON verbs refuse the whole namespace, so receive-pack is the only door such a ref can arrive through. Once there it is hidden from the advertisement, copied to every mirror by a hub refspec that only ever adds refs, and roots collection: a permanent, invisible pin on the object graph of every replica, placed by anybody holding `source.push`.

Appending to one of these namespaces MUST cost a capability of that namespace — a `hub.*` for `refs/hub/**`, a `member.*` for the trust log — and not bare `source.push`. The capability charged has to be one that exists: a prefix no capability starts with reads as a tighter rule and is in fact a lockout, since `repo.admin` implies everything and would then be the only way to grow the ref — a `member.revoke` holder could sign a revocation and never publish it. What lands on the ref is a commit, and the boundary reads commits, not the records inside them: it can tell that a push is well-formed and within the ceiling, and it cannot tell that the records it carries will fold to nothing. So a holder of `source.push` alone — the capability every contributor has, and the only one a source-only contributor needs — could push commit after well-formed commit onto a pull request or the trust log, adding no state anybody projects but permanently consuming the ceiling that keeps folds affordable, on refs nothing can shorten. Charging a namespace capability puts that cost behind the same door as the events themselves: a member who may not write hub events may not grow hub refs either. It is charged on creates as well as updates, since a create is the first append.

An event's declared trust head MUST be validated as an object id before anything uses it. It is written by the event's own signer and it is used as a **name**: the walk below reads objects by it, and a store turns a name into a location. A value that is not forty hex characters is not merely one that fails to resolve — `../HEAD` joins into a path outside the object store, which is a read oracle over the host and, when the bytes there do not inflate, a failure that takes the whole fold down for good on a ref that cannot be rewound. The store MUST refuse such a name too, and answer as it would for any object it does not hold: a brand is only as good as the casts that mint it, so the check belongs at the place that builds the path as well as at the place that let the value through.

Every walk of an append-only namespace MUST be bounded to that namespace's own commits and MUST be bounded as it runs — including the walk from the trust head an **event declares**, which is an OID its own signer chose, and which a branch of record-carrying or empty-tree commits satisfies without ever being in the log. A walk that hits that bound MUST refuse the event rather than answer "reaches nothing": read as an empty ancestry, every forward-only revocation becomes invisible and the event counts, which is the opposite of conservative. A memo in front of such a walk MUST remember the refusal as well as the answer. Remembering only success leaves the one head worth remembering — the one this host will not walk — walked again from scratch on every ask, several times per event and once per event per fold, each ask reading the whole ceiling before refusing; a chain of empty-tree commits one commit past the ceiling costs one push to write and is referenced by nothing, so no rule that inspects refs ever sees it, and naming it as an event's trust head charges every later protected-branch push, collection and deepening fetch for the walk, synchronously. Each reader turns that refusal into its own conservative answer and MUST NOT let it escape: no grant is shown, no revocation is shown to be predated, and no ancestor's trust head is shown to be newer. Escaping, it makes the pull request unfoldable for good — and a pull request the boundary cannot fold is a protected branch that can never be pushed again, and a commit the walk turns away MUST be recorded as turned away: asked again per in-edge it is the same read repeated, and — counting only what it kept — one pushed commit listing a hundred thousand fabricated parents cost a hundred thousand object reads without ever reaching the ceiling meant to refuse it. Unbounded in shape, a hub commit naming a _source_ commit as a second parent turns one small push into a walk of the whole repository, synchronously, on the receive-pack path. Bounded only in its result, the ceiling is paid for before it can be applied — the walk reads the whole history and the refusal arrives after the cost it was there to refuse.

The same tolerance applies to a commit whose **tree** never arrived, and to one whose own **commit object** never did: what a ref reaches, it reaches, and a walk that cannot read one link records it and stops descending rather than reporting that the ref holds nothing it can name. Reported that way, the rules reading the answer went opposite ways and both were wrong — the edge rule waved a graft through onto a ref that can never be deleted, and the revoked-signer rule re-judged history the ref already held and refused an ordinary reconciling join for good. Refs are applied without a connectivity check, so a replica can hold one, and the walk that decides whether a commit belongs to a hub or trust history MUST read that absence as "not part of this history" rather than as a failure — it runs first on every protected-branch push, every collection and every deepening fetch, and one missing object would otherwise take all of them out at once. That tolerance is for **absence only**. A store that failed to answer is not a store that said no, and reading a failure the same way does not skip one commit: this walk is the boundary of the history, so it empties the ref. An emptied trust log has no members and no revocations — cached under an unchanged head, every revoked key authorized again and a private repository reporting itself as public — and an emptied pull request has no events, so no tombstones, so nothing excluded, and collection re-protects and repacks a payload a valid tombstone covered. Neither raises anything anywhere. So the walk MUST distinguish the two and let a failure propagate, leaving each caller to turn it into its own conservative answer.

### Convergence

Hub and trust refs converge by DAG union: if replica A holds events `{1, 2}` and replica B holds `{2, 3}`, each fetches the other's head and joins (§16), and both reach `{1, 2, 3}`. The same event ID bound to different content is an integrity failure, surfaced rather than merged.

### Ordering: trust before hub

Hub events are validated against the trust projection, and an event may be signed under a grant the receiving replica has not seen yet. Therefore:

```text
synchronization MUST fetch refs/meta/trust/* before refs/hub/*

an event whose authorization chain does not yet resolve is
QUARANTINED — held, excluded from projection — and re-validated
when the trust log advances, rather than rejected permanently
```

The explicit `ref-prefix` a hub-aware fetch sends is built from the literal head of each refspec source, cut at the **first** wildcard. A source may put its `*` in the middle, and a prefix is compared with `startsWith` — so a probe that kept the `*` matched nothing, the ask came back empty, and the fetch reported a replication of zero refs as a success, which is the one failure this path exists to make visible.

### Advertisement hygiene

`refs/hub/*` and `refs/meta/trust/*` MUST be excluded from the protocol v0 ref advertisement served to source-only clients (the moral equivalent of `transfer.hideRefs`), except `refs/meta/trust/genesis`, which stays visible so any client can compute the RepoID. Hub-aware clients fetch hub refs explicitly via their refspecs; implementations SHOULD serve them over protocol v2 with `ref-prefix` filtering so a hub fetch names the namespaces it wants instead of receiving everything.

### Source branches remain different

Branches remain mutable. If two branch tips diverge, automatic synchronization MUST NOT invent a merge, rebase, or force push. It reports divergence and requires explicit resolution.

The rules file (§25) is the one ref that is neither append-only nor a branch, and synchronization MUST take the source's copy rather than report it. It is not a history two sides grow independently; it is one blob the repository publishes about itself, and a replica that keeps its own copy keeps enforcing rules the source has already superseded — a branch still protected after the protection was lifted, a required check after it was dropped. Fetching it is the point of fetching it. Writing it through the boundary still needs `policy.write`; arriving by replication is the source saying what it now requires.

---

## 24. Generalized ref selection

The synchronization implementation MUST be refspec-driven rather than hard-coding heads and tags:

```text
refs/heads/*        → refs/remotes/origin/*
refs/tags/*         → refs/tags/*
refs/hub/*          → refs/hub/*
refs/meta/trust/*   → refs/meta/trust/*
```

The pack/object-transfer layer should not care whether an OID was reached through a source branch, review, PR event, or trust event. (`checkRefAddress` in `src/git/Store.ts` already accepts any `refs/**` name; the generalization work is in ref _selection_ — the client fetch path currently hard-codes heads/tags.)

---

## 25. Automatic remote replication

Remote hosts MAY optionally configure automatic replication.

Conceptually:

```ts
interface Remote {
  readonly name: string;
  readonly url: string;

  readonly sync?: {
    readonly mode: "manual" | "fetch" | "push" | "mirror";
    readonly refs: ReadonlyArray<string>;
  };
}
```

Successful local ref updates may schedule asynchronous synchronization:

```text
RefStore.apply
      ↓
postReceive
      ↓
replication job
      ↓
Git push/fetch
```

Replication failure MUST NOT roll back the originating write. The trigger is `post-receive` — a ref that moved because somebody else said so — the same one webhook delivery uses, and every host that serves pushes MUST install both: a host that composes only one of them stores a standing instruction it will never act on, and two hosts reading the same configuration then disagree about what it means. A mode with no trigger behind it MUST be refused where it is written rather than stored: an implementation with no scheduler cannot honour `fetch`, and a remote configured for one that sits doing nothing is configuration that reads as working and is not. A stored instruction MUST be decoded rather than trusted, whichever store it came back from: a registry is a repository's file or table, and both get hand-edited and both get carried between versions — a shape nothing checked is one the forwarder walks into on `post-receive`, where nothing is watching. Unreadable reads as `manual`, which is what a repository that never configured one already does. Nor may one post-receive consumer cost another one its notification: forwarding and webhook delivery are two things that happen after the same push, they run under one hook service, and a failure in either MUST leave the other's work done. Post-receive has no error channel to fail through, so the only way it stops is a defect — which makes an unguarded chain fail silently. It runs detached from the response and logs, exactly as webhook delivery does. A forward carries the **values** the push applied, not the ref names it applied them to. It is detached from the push that caused it, so by the time it runs a ref may have moved again or been deleted — and resolving a name at send time both forwards the wrong value and, when the name is gone, fails the whole batch and takes every other ref in it down. What it forwards is what the push actually **applied**: a command the receive refused never happened, and sending it would tell the other side something this repository does not hold. An empty `refs` list is everything the mode carries, so `{mode: "push"}` means what it looks like it means. A forward is never forced — a standing instruction is not a licence to overwrite what the other side has, and a ref that will not fast-forward there is a divergence for a person.

The repository a forward pushes _from_ MUST be built with no hooks, and MUST NOT be a dependency of the hook layer. Both halves matter: handed the repository the hook is installed on, a forward would be its own trigger; and asking for that repository while building the hooks it depends on is a cycle the layer system resolves by handing somebody a second instance — which is silent, and which quietly cost the webhook registry its notifications when it happened.

Hub and trust events synchronize active-active (as DAG unions, trust first — §23). Source branches remain subject to directional and fast-forward policy.

---

## 26. Merge policy

Policy executes before mutable source refs move.

```text
request
   ↓
SSH key proof (native or delegated — §12)
   ↓
membership/capability verification
   ↓
Git state + hub projection
   ↓
RefUpdatePolicy
   ↓
RefStore.apply (with expected old OIDs)
```

Possible rules:

```text
member has source.push
no force pushes
no deletion of main
PR required
required approvals (of the current head — §18)
required check names (with hub.check:<name> scope — §19)
no unresolved threads
CODEOWNER approval
```

Authentication answers: `who?`
Membership answers: `what may they do?`
Policy answers: `is this particular state transition allowed now?`

### Atomicity

There is a race between evaluating the projection (head, approvals, checks) and applying the ref update. Policy-gated updates MUST pass the evaluated old OID as `expected` to `RefStore.apply` — the compare-and-swap already in the `RefUpdate` contract — so a head that moved between evaluation and application fails the swap instead of merging stale approvals.

A refused atomic batch MUST report each ref with its own reason where the boundary gave it one, and the rest with the fact that the batch failed. Stamping the first refusal onto every command tells a user their clean ref failed for something that was never about it, and throws away every other refusal's reason — which on a batch refused for two different things is the half they needed.

A Durable Object is additionally single-threaded per repository, which serializes evaluate-then-apply; the Node backend has no such guarantee and relies on the CAS. The CAS is therefore mandatory, not an optimization.

### Append-only enforcement

The same policy boundary enforces §16's rules for `refs/hub/**` and `refs/meta/trust/log`: no deletion, ancestry-preserving updates only, within the namespace's ceiling, and only for a principal holding a capability of that namespace. `refs/meta/trust/genesis` never moves at all.

All mutable source-ref mutation paths MUST converge on this one policy boundary.

---

## 27. Garbage collection

Canonical hub and trust refs participate in reachability:

```text
refs/hub/*
refs/meta/trust/*
```

MUST protect the Git objects they reference from GC — with one exception: blobs covered by a valid redaction tombstone (§21) are excluded from protection and pruned.

Closing a PR means creating `pr.closed`, not deleting its history.

Several events cost one capability to make and a higher one to make about
somebody else's work. The pull request's own author may always do these; anybody
else needs the capability named:

```text
pr.closed / pr.reopened / pr.updated / a second pr.opened   hub.merge
review.dismissed (of a review one did not write)            hub.approve
```

The author is the signer of the opening event, and which event opened a pull
request is decided by descent rather than by walk order: every honest event
descends from `pr.opened`, so a forged parentless one is an ancestor of nothing.
A pull request's `base` MAY be written unqualified (`main`) and MUST be read as
a full ref name (`refs/heads/main`); the protected-branch check matches on it,
so an unnormalised base makes the pull request stop counting toward its own
branch and leaves that branch unpushable.

Which of a reviewer's several statements counts is decided by fold order —
ancestry with a deterministic tie-break — and never by `issuedAt`, which the
reviewer writes: ordered on that, a back-dated "request changes" fails to
withdraw the approval it was meant to withdraw. Only a _verdict_ supersedes:
a `comment` review takes no position and costs `hub.review` rather than
`hub.approve`, so letting one supersede would be the lower capability
cancelling an approval — the very thing `review.dismissed` charges
`hub.approve` to stop.

Descent is measured over _events_, weighted by how many distinct members made
them — not over raw commits. Counted over the walked DAG, a forger grafts an
opening and chains empty commits under it until it outnumbers the real
conversation; counted this way, displacing one costs a member key per
participant rather than a commit per event.
Where two competing openings are structurally indistinguishable — neither
descends from the other and the weighted count above separates them by nothing —
the opening is _contested_ and establishes no author at all, so every action in
the table above needs its capability. A candidate the count _does_ separate is
not a contest but a loss, and MUST NOT strip the winner of its authorship:
treated as one, a member holding only `hub.create-pr` could erase the author of
any pull request — and with it the self-approval exclusion — by pushing a single
parentless commit at it. That costs the
honest author a shortcut and denies the forger the thing they were after: a
contested pull request can still be reviewed, approved and settled by a
`hub.merge` holder, so the protected branch behind it never freezes.

`hub.create-pr` is the lowest-privileged hub capability, and settling or
retargeting somebody else's approved pull request is not the same authority as
opening one: a projection that treated it as such would let any hub writer close
or re-point an approved pull request and, with it, block every push to the
protected branch that pull request was the only route to. `hub.review` is
likewise below `hub.approve`, and letting it dismiss an approval would let the
lower capability cancel the higher one's word.

An approval by anybody who **proposed** the pull request or the revision under
review is recorded but counts toward no requirement — its author, every other
claimant to have opened it, and whoever moved the head. Keyed on the opening
alone, a `hub.merge` holder could push a revision onto somebody else's pull
request and then approve it, which is self-approval wearing another event's
name; and an opening the descent pre-pass refused but the fold accepted — the
two judge the same event against different heads, the one it declares and the
floor its ancestors raise it to — left its signer out of the exclusion entirely
while still supplying the title, description and base.

Self-approval is not review; it is the thing review exists to be independent
of, and counting it would let one holder of `hub.approve` satisfy
`requiredApprovals` alone.

Destructive pruning beyond tombstoned blobs is a separate explicit administrative operation, and it is local: it does not replicate, and a pruned replica re-fetching from a peer will get the objects back unless the peer pruned too. Tombstones are the only replicating removal.

---

## 28. Hash-qualified references

Version 1 targets **SHA-1 repositories** (SHA-256 object-format support is future work — §31). But hub and trust payloads MUST be format-agnostic from day one, so that no payload ever needs migration:

```text
OIDs in payloads MUST NOT assume 40 hex characters
references MUST be explicitly qualified
```

```json
{
  "head": "sha1:abc...",
  "base": "sha1:def..."
}
```

`RepoID` is always SHA-256 over genesis bytes (§4), independent of the repository's object format:

```text
Git objects: sha1 (v1)
RepoID:      always sha256
signatures:  SSH
```

---

## 29. Recommended modules

Following the `artifacts` branch layout:

```text
src/crypto/
  SshSignature.ts

src/trust/
  Genesis.ts
  Certificate.ts
  Log.ts
  Checkpoint.ts
  Projection.ts
  Verify.ts

src/hub/
  Event.ts
  PullRequest.ts
  Projection.ts
  Review.ts
  Comment.ts
  Check.ts
  Redaction.ts

src/server/
  Auth.ts        (rewritten: challenge + delegated credentials)
  Policy.ts
  Replication.ts
```

`src/git/Sha1.ts` already provides streaming SHA-1. The generic repository token registry (`Tokens` in `src/artifacts/*`) and the HMAC mint/verify pair in `src/server/Auth.ts` disappear (§13).

---

## 30. Implementation phases

Ordered so that each phase's prerequisites precede it, the novel work (trust, hub) comes first, and the largest mechanical work (SHA-256) is deferred entirely.

### Phase 1 — Repository trust genesis

```text
RepoID (exact-bytes definition)
quorum genesis, 1-of-1 and N-of-M
SSH signatures (sign/verify, namespace)
known_repos TOFU (URL → RepoID)
refs/meta/trust/genesis
```

### Phase 2 — Trust log and membership

```text
refs/meta/trust/log (hash-linked event DAG)
membership grant / revocation (both classes)
certificate validity windows
root rotation
checkpoints
trust projection + capability evaluation
```

### Phase 3 — Authentication

```text
challenge-response (nonce lifecycle, signed envelope
  over command lists)
delegated short-lived credentials + git credential helper
remove: HMAC mint/verify, Tokens registries,
  generic read/write scopes
```

### Phase 4 — Generalized replication

```text
refspec-driven ref selection in client fetch/push
arbitrary refs/** namespaces through the transfer layer
advertisement hygiene (hide hub/trust from v0;
  v2 ref-prefix for hub fetches)
```

### Phase 5 — `hub enable`

```text
known_repos verification / TOFU
+refs/hub/*:refs/hub/* and trust refspecs
multiple fetch refspecs per remote in client config
initial trust-then-hub fetch
hub disable
hub status
```

### Phase 6 — Hub events

```text
per-PR append-only event DAGs
signed PRs, reviews, comments, checks
join commits, integrity-conflict detection
redaction tombstones
projection
```

### Phase 7 — Policy

```text
trust + hub projections at the common ref-update boundary
expected-OID compare-and-swap on policy-gated applies
append-only enforcement for hub/trust refs
```

### Phase 8 — Automatic replication

```text
hub/trust state as active-active DAG unions (trust first)
source refs by directional / fast-forward policy
quarantine + re-validation of unresolved events
```

---

## 31. Future work: SHA-256 object format

SHA-256 object-format support is explicitly **out of scope for v1**. Nothing in the trust or hub design depends on it — `RepoID` is already SHA-256 regardless of object format, and all payload OIDs are hash-qualified (§28) — while full conformance is the single largest engineering item in the original proposal: the `Oid` brand and `isOid`'s 40-hex regex are load-bearing across the codebase, `ZERO_OID` and pack/index/protocol widths are SHA-1-fixed, and interop must be proven against stock Git SHA-256 repositories. Sequencing it first would gate every novel feature behind months of hash plumbing.

When taken up, the work comprises:

### Hash abstraction

```ts
type ObjectFormat = "sha1" | "sha256";

interface HashFormat {
  readonly name: ObjectFormat;
  readonly rawSize: number; // sha1: 20, sha256: 32
  readonly hexSize: number; // sha1: 40, sha256: 64
  readonly digest: (bytes: Uint8Array) => Effect<Uint8Array>;
  readonly hasher: () => StreamingHasher;
}

interface StreamingHasher {
  update(bytes: Uint8Array): this;
  digest(): Uint8Array;
  digestHex(): string;
}
```

OID parsing receives repository/hash context. Scattered assumptions (`length === 40`, `slice(0, 40)`, `new Uint8Array(20)`) are replaced by the abstraction. SHA-1 behavior keeps passing unchanged throughout.

### Conformance

```text
SHA-256 loose objects, trees, commits, tags
packs and pack indexes (v3)
refs, zero OIDs, delta references
protocol advertisement: object-format capability
  (object-format=sha1 | object-format=sha256), with
  incompatible negotiated formats rejected rather than
  misinterpreted
fetch, push, fsck
tests against real stock Git SHA-1 and SHA-256 repositories
```

A repository has one primary Git object format; SHA-1 and SHA-256 objects are not mixed in one object namespace. `chr33s-git init --object-format=…` selects it; SHA-1 remains the compatibility default.

Because v1 payloads are already hash-qualified, enabling SHA-256 later changes no wire or event format.

---

## 32. Acceptance scenario

Alice, Bob, and Carol each own independent SSH Ed25519 keys.

They initialize:

```text
RepoID = SHA256(genesis blob bytes)
authorities = Alice, Bob, Carol
threshold = 2
objectFormat = sha1
```

A user first connects to:

```text
https://git.example.com/project
```

and `chr33s-git hub enable` displays the repository fingerprint. The user accepts it. That `RepoID` is pinned in:

```text
~/.config/chr33s-git/known_repos
```

Future connections presenting a different genesis fail with a repository-identity warning.

Dave generates his own SSH key. The repository authority issues Dave a membership certificate, valid for one year, with:

```text
source.push
hub.create-pr
hub.comment
hub.approve
```

recorded as a `trust.grant` in the trust log.

Dave authenticates by proving possession of that SSH private key — challenge-response from `chr33s-git`, or a delegated credential he minted for stock git. The server derives authorization from the repository's Git-native trust graph rather than from a generic read/write token.

Dave creates a PR: a `pr.opened` event at the root of a new event DAG under:

```text
refs/hub/pr/0194f59d-...
```

Bob reviews and signs an approval of the exact head OID:

```text
sha1:abc123...
```

CI, holding `hub.check:test`, signs a successful `check.completed` for `test`.

Merge policy verifies:

```text
member authorization
approval signature and that it names the current head
check event capability scope
branch rules
```

and applies the merge through `RefStore.apply` with the evaluated head as `expected` — a head moved in the meantime fails the swap instead of merging stale approvals.

The repository moves to another compatible host. Its RepoID does not change. The new host synchronizes trust refs, then hub refs, and reconstructs:

```text
repository identity
current authorities
membership and revocation state
PR history
reviews
checks
```

from Git-native state and SSH signatures.

No original hosting database or generic repository token is required.

---

## 33. Final architecture

```text
                   Repo genesis
                        │
                 SHA-256 RepoID
                        │
                 known_repos TOFU
                        │
                SSH root quorum
                        │
          trust log (hash-linked, checkpointed)
                        │
             membership certificates
                        │
                    SSH keys
                        │
          ┌─────────────┴─────────────┐
          │                           │
   authentication                signed hub events
 (challenge / delegated)     (append-only DAGs per PR)
          │                           │
          ↓                           ↓
    capabilities                  refs/hub/*
          │                           │
          └─────────────┬─────────────┘
                        ↓
                 RefUpdatePolicy
              (CAS + append-only rules)
                        ↓
                  RefStore.apply
```

The intended security model is:

```text
SSH known_hosts
    → trust the transport endpoint

known_repos
    → trust the logical repository

SSH membership chain (hash-linked log)
    → trust repository members

SSH event signatures
    → trust authorship

policy
    → authorize state transitions
```

while Git itself remains the source, storage, and replication substrate.
