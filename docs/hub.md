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
  "rootKeys": ["ssh-ed25519 AAAA... alice", "ssh-ed25519 AAAA... bob", "ssh-ed25519 AAAA... carol"],
  "threshold": 2
}
```

The genesis is stored as a Git object: `refs/meta/trust/genesis` points at a commit whose tree contains a single `genesis.json` blob. The genesis SHOULD carry SSH signatures from at least `threshold` root keys, proving key possession at creation time.

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

Verifiers MAY require a checkpoint newer than a configured maximum age before authorizing high-value operations (merges to protected branches, trust changes). This bounds how stale a split view can be.

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

and can act as a `git credential` helper so stock git picks it up transparently.

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

`hub disable` removes only the refspecs managed by `chr33s-git`.

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

A host MAY separately display human-friendly numbers such as `#42`, but these are presentation metadata, not canonical identity.

Every event payload carries its own UUIDv7 event ID. Event identity binds ID to content: two objects claiming the same event ID with different content is an integrity conflict that MUST be surfaced, not merged over.

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
  "reason": "sensitive-content",
  "redactedAt": "2026-08-16T12:00:00Z"
}
```

signed by a principal holding `hub.redact` (or `repo.admin`), appended to the same event DAG as any other event.

Effect of a valid tombstone for event `E`:

```text
E's event.json blob is deleted from object storage
E's commit and tree remain — the DAG's hashes stay intact
projections exclude E's content, showing a redaction marker
replicas MUST NOT serve, re-fetch, or re-accept E's blob
GC excludes E's blob from reachability protection (§27)
```

Because Git is content-addressed, absence composes: the commit references the tree, the tree references the blob's hash, and the blob object is simply gone. Verifiers treat a missing blob as valid **only** when a valid tombstone covers that event; a missing blob without a tombstone is corruption.

Redaction removes content, never structure: the event's existence, author, and position in history remain visible. Destroying structure would break the hash chain and is not offered.

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

### Advertisement hygiene

`refs/hub/*` and `refs/meta/trust/*` MUST be excluded from the protocol v0 ref advertisement served to source-only clients (the moral equivalent of `transfer.hideRefs`), except `refs/meta/trust/genesis`, which stays visible so any client can compute the RepoID. Hub-aware clients fetch hub refs explicitly via their refspecs; implementations SHOULD serve them over protocol v2 with `ref-prefix` filtering so a hub fetch names the namespaces it wants instead of receiving everything.

### Source branches remain different

Branches remain mutable. If two branch tips diverge, automatic synchronization MUST NOT invent a merge, rebase, or force push. It reports divergence and requires explicit resolution.

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

Replication failure MUST NOT roll back the originating write.

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

A Durable Object is additionally single-threaded per repository, which serializes evaluate-then-apply; the Node backend has no such guarantee and relies on the CAS. The CAS is therefore mandatory, not an optimization.

### Append-only enforcement

The same policy boundary enforces §16's rules for `refs/hub/**` and `refs/meta/trust/log`: no deletion, ancestry-preserving updates only. `refs/meta/trust/genesis` never moves at all.

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

`hub.create-pr` is the lowest-privileged hub capability, and settling or
retargeting somebody else's approved pull request is not the same authority as
opening one: a projection that treated it as such would let any hub writer close
or re-point an approved pull request and, with it, block every push to the
protected branch that pull request was the only route to. `hub.review` is
likewise below `hub.approve`, and letting it dismiss an approval would let the
lower capability cancel the higher one's word.

An approval by the pull request's own author is recorded but counts toward no
requirement. Self-approval is not review; it is the thing review exists to be
independent of, and counting it would let one holder of `hub.approve` satisfy
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
