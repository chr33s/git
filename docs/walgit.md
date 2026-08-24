# Bundle Delivery, Observable Operations, and Self-Healing Maintenance

**Status:** Proposed  
**Target:** `@chr33s/git`  
**Priority:** P0/P1  
**Scope:** Git hosting performance and operability

## 1. Summary

Add three related capabilities:

1. **Bundle URI delivery** — precompute immutable Git bundles so clones and stale fetches can obtain most repository data directly from static storage instead of generating and streaming a large pack through Smart HTTP.
2. **Observable operations** — represent slow server work as named operations with IDs, progress, logs, cancellation where safe, CLI output, Smart HTTP sideband narration, and SSE for the web UI.
3. **Desired-state maintenance** — replace ad-hoc scheduling of maintenance work with a deterministic planner that continually derives the next bounded unit of work from repository state and configuration.

The implementation must preserve the existing architecture:

- `Repository` remains the Git domain boundary.
- `ObjectStore`, `RefStore`, and `PackStore` remain authoritative for Git state.
- Durable Objects remain the Cloudflare serialization mechanism.
- No repository-wide WAL is introduced.
- Node, Cloudflare, CLI, and browser portability must not be weakened to support a server-only feature.

## 2. Goals

### 2.1 Bundle delivery

- Reduce CPU, memory, and origin-server byte volume for large clones.
- Allow stale clients to catch up using precomputed immutable data.
- Let R2/CDN/static storage carry large byte streams.
- Remain compatible with stock Git protocol v2.
- Degrade transparently to ordinary Smart HTTP if bundle delivery fails.
- Support full and `blob:none` bundle families.

### 2.2 Observable operations

- Make long-running work visibly progress instead of appearing hung.
- Give CLI, Git clients, API clients, and the web UI one operation model.
- Keep operational jobs distinct from agent/hub tasks.
- Preserve enough diagnostic history to explain failures.

### 2.3 Desired-state maintenance

- Make maintenance idempotent and restart-safe.
- Recover naturally after missed schedules or process crashes.
- Bound work performed by each maintenance invocation.
- Prioritize correctness work ahead of optimization work.
- Support different scheduling mechanisms on Node and Cloudflare without putting scheduling policy in the Git core.

## 3. Non-goals

This project does **not** introduce:

- an object-store WAL as repository source of truth;
- leader election or a distributed scheduler;
- another ref database;
- a replacement for Durable Object serialization;
- a new Git object or pack format;
- packfile-URI support;
- a generic workflow engine;
- a replacement for `refs/hub/task/*`;
- mandatory bundles for correctness.

The ordinary Git server remains the ultimate source of truth.

---

# 4. Feature A — Bundle URI Delivery

## 4.1 User-visible behavior

A compatible stock Git client cloning through protocol v2 may discover:

```text
bundle-uri
```

Before the normal `fetch`, it requests the bundle list and downloads one or more immutable `.bundle` files.

After those objects have been installed, normal fetch negotiation transfers only the remaining objects.

A client that does not understand `bundle-uri` behaves exactly as it does today.

If a bundle is missing, corrupt, unauthorized, stale, or unavailable, the client falls back to normal fetch.

## 4.2 Architecture

Add server-specific bundle modules without changing `Repository`'s storage contract.

Suggested module shape:

```text
src/server/
  Bundles.ts            bundle list and protocol integration
  BundleBuilder.ts      repository snapshot -> bundle artifact
  BundleStore.ts        server-side bundle metadata/artifact port
  Bundles.node.ts
  Bundles.cloudflare.ts
```

`BundleStore` is intentionally separate from `ObjectStore`.

Git objects and refs are repository truth. Bundles are derived artifacts.

### BundleStore

Conceptually:

```ts
interface BundleStore {
  list(repo: string): Effect<BundleManifest | null>;
  publish(repo: string, manifest: BundleManifest): Effect<void>;

  read(id: string): Effect<Stream<Uint8Array>>;
  write(id: string, source: Stream<Uint8Array>): Effect<BundleArtifact>;
  delete(id: string): Effect<void>;
}
```

Cloudflare implementation stores bundle bytes in R2.

Node implementation stores bundle bytes beneath server-managed repository metadata.

Browser implementations do not provide this service.

## 4.3 Bundle metadata

```ts
interface BundleArtifact {
  readonly id: string;
  readonly kind: "full" | "incremental";
  readonly filter: null | "blob:none";

  readonly creationToken: bigint;

  readonly refs: Readonly<Record<string, Oid>>;
  readonly prerequisites: ReadonlyArray<Oid>;

  readonly objectId: string;
  readonly bytes: number;
  readonly createdAt: string;
}
```

`id` must be stable and safe for Git's bundle-list identifier restrictions.

Artifact URLs are immutable.

A content checksum should appear in the object key:

```text
<repo>/bundles/<family>/<creationToken>-<checksum>.bundle
```

Never overwrite bundle contents in place.

## 4.4 Snapshot model

Do **not** reproduce walgit's historical WAL reconstruction.

A bundle is built from an explicit repository snapshot:

```ts
interface BundleSnapshot {
  readonly createdAt: Date;
  readonly refs: Readonly<Record<string, Oid>>;
  readonly filter: null | "blob:none";
}
```

The builder first captures the ref OIDs it intends to bundle.

All subsequent object traversal uses those OIDs.

A concurrent push may advance the repository, but it cannot change the meaning of the already captured snapshot.

The subsequent Smart HTTP fetch transfers the difference.

## 4.5 Bundle families

Support two parallel families:

```text
full
blob:none
```

The `blob:none` family MUST advertise:

```text
bundle.<id>.filter=blob:none
```

Do not attempt arbitrary filters in the first implementation.

## 4.6 Initial chain policy

Use a simple chain before adding calendar complexity.

Each family maintains:

- one recent full base;
- zero or more incrementals chained from previous published state.

An incremental contains objects reachable from its captured refs that are not already covered by its prerequisite snapshot.

Conceptually:

```text
FULL A
  ↓
INC B
  ↓
INC C
  ↓
INC D
```

Each incremental's prerequisite OIDs must correspond to an earlier published state.

### Compaction

When the incremental chain exceeds configurable limits, generate a new full base and retire old entries from the advertised clone list.

Initial configuration:

```ts
interface BundlePolicy {
  readonly enabled: boolean;

  readonly fullMaxAge: Duration; // default 7d
  readonly incrementalMinAge: Duration; // default 1h

  readonly maxIncrementals: number; // default 24
  readonly minNewObjects: number;
}
```

These defaults are policy, not protocol. They may change after measurements.

## 4.7 Clone list vs catch-up list

Maintain two logical lists.

### Clone list

```text
/:repo.git/bundles/clone
```

Contains the current full base plus the incremental chain above it.

It is intended to bootstrap a repository with no objects.

### Catch-up list

```text
/:repo.git/bundles/catchup
```

Contains incrementals only.

It is intended for an existing clone and MUST NOT introduce a newly-created gigantic full bundle into a routine fetch.

Both lists use:

```ini
[bundle]
    version = 1
    mode = all
    heuristic = creationToken
```

### Protocol v2 behavior

The protocol `bundle-uri` command returns the clone bundle-list entries directly.

This allows stock Git clone discovery without requiring an additional configuration step.

### `git+` client behavior

When `git+ clone` or a future setup command controls the clone workflow, configure subsequent fetches to use:

```text
/:repo.git/bundles/catchup
```

Do not configure the full clone list as the persistent fetch source.

## 4.8 Static artifact HTTP contract

Bundle artifacts MUST support:

- `GET`
- `HEAD`
- byte `Range`
- `ETag`
- `If-None-Match`
- `If-Range`
- `Content-Length`
- `Accept-Ranges: bytes`

Immutable artifact responses should include:

```text
Cache-Control: public, max-age=31536000, immutable
```

where repository visibility permits public caching.

Private repositories must preserve existing read authorization.

A backend MAY later return a short-lived authorized direct-storage URL instead of proxying bytes through the application, but direct delivery is an optimization and not required for the first implementation.

## 4.9 Authorization

Bundle-list discovery requires the same read permission as clone/fetch.

Artifact access MUST NOT create a weaker alternate path around `repo.read`.

For authenticated repositories:

- application-proxied artifact requests use normal repository authentication; or
- direct object URLs must be authorization-scoped and time-limited.

Bundle identifiers and object keys contain no secrets.

## 4.10 Generation

Bundle construction must use the existing object traversal and pack writer where practical.

It must not require collecting an entire large repository pack into one `Uint8Array`.

Before enabling large-repository bundle generation on Cloudflare, the write path MUST be capable of streaming bundle bytes to backing storage.

If the current pack writer cannot do this within the runtime's memory bound, streaming bundle output is a prerequisite.

## 4.11 Atomic publication

Artifact bytes are written first.

Only after the artifact is complete and verified is the bundle manifest updated to advertise it.

Order:

```text
capture refs
→ generate artifact
→ verify artifact
→ store immutable artifact
→ atomically publish new manifest
```

A crash before manifest publication leaves an unreferenced artifact that maintenance may later collect.

A client must never observe a manifest entry pointing to incomplete bytes.

## 4.12 Verification

Before publication:

- parse the generated bundle header;
- verify expected refs;
- verify prerequisites;
- verify object format;
- ensure the pack is structurally readable.

Interop tests should additionally run stock:

```text
git bundle verify
```

against generated bundles.

## 4.13 Bundle API

The first release does not require a user-facing JSON management API beyond observability.

The following read-only API is useful:

```text
GET /api/bundles
```

Response:

```json
{
  "enabled": true,
  "families": [
    {
      "filter": null,
      "full": "...",
      "incrementals": 4,
      "latestCreationToken": "..."
    }
  ]
}
```

Administrative bundle creation should occur through the maintenance system rather than a synchronous HTTP request.

---

# 5. Feature B — Observable Operations

## 5.1 Terminology

Use **Operation**, not **Task**.

`Task` already describes agent/hub coordination.

An Operation describes execution performed by this Git host.

Examples:

```text
bundle.build
maintenance.fsck
maintenance.gc
maintenance.repack
remote.fetch
remote.push
archive.build
```

## 5.2 Operation model

```ts
type OperationState = "queued" | "running" | "succeeded" | "failed" | "cancelled";

interface Operation {
  readonly id: string;
  readonly repo: string;
  readonly kind: string;

  readonly state: OperationState;

  readonly createdAt: string;
  readonly startedAt?: string;
  readonly finishedAt?: string;

  readonly progress?: {
    readonly current?: number;
    readonly total?: number;
    readonly unit?: string;
  };

  readonly message?: string;
  readonly result?: unknown;
  readonly error?: OperationError;
}
```

## 5.3 Events

Progress is an append-only sequence while the operation runs.

```ts
interface OperationEvent {
  readonly sequence: number;
  readonly at: string;
  readonly level: "debug" | "info" | "warning" | "error";

  readonly message: string;

  readonly progress?: {
    readonly current?: number;
    readonly total?: number;
    readonly unit?: string;
  };
}
```

Event history may be bounded after completion.

The final state must remain available longer than individual progress events.

## 5.4 Execution model

The Operation abstraction is **not** initially a durable generic queue.

It wraps work that another subsystem already knows how to execute.

Examples:

```text
maintenance planner chooses bundle.build
→ create Operation
→ execute BundleBuilder
→ emit progress
→ complete Operation
```

If the process disappears, desired-state maintenance determines whether the work still needs doing and creates a new operation.

This avoids turning operation persistence into workflow correctness.

## 5.5 API

Add:

```text
GET  /api/operations/:id
GET  /api/operations/:id/events
POST /api/operations/:id/cancel
```

`events` uses:

```text
Content-Type: text/event-stream
```

Events:

```text
event: progress
event: message
event: completed
event: failed
```

An optional repository list endpoint:

```text
GET /api/operations?state=running
```

may be added for the UI.

## 5.6 Cancellation

Cancellation is cooperative.

Every operation receives the Effect interruption signal.

Operations define whether cancellation is allowed.

Safe:

- object walk;
- fsck;
- bundle generation before publication;
- remote fetch before ref application.

Potentially unsafe or meaningless after a commit point:

- atomic ref apply;
- manifest publication;
- completed pack replacement.

After the commit point, cancellation may stop follow-up work but MUST NOT claim the committed action was undone.

## 5.7 CLI presentation

Long-running CLI commands show:

```text
* repack: walking objects 18,220 / 42,912
* repack: writing pack 61%
* repack: verifying objects
* repack: complete
```

Non-interactive output remains line oriented.

Machine-readable CLI output may emit operation IDs but does not mix progress into JSON result bodies.

## 5.8 Smart HTTP sideband

When a Git protocol operation performs meaningful server work, progress may be emitted on sideband 2.

Examples:

```text
remote: bundle catch-up unavailable; generating delta normally
remote: validating 3 ref updates
remote: indexing received pack
```

Do not flood the channel with per-object messages.

Progress messages should describe phases or meaningful thresholds.

## 5.9 Web UI

Add a reusable operation-progress component.

It should support:

- current phase;
- progress bar where total is known;
- textual stream where total is unknown;
- success/failure state;
- retry entry point when the originating feature supports retry.

The component should be reusable by maintenance, remotes, and future server operations.

---

# 6. Feature C — Desired-State Maintenance

## 6.1 Principle

Maintenance should answer:

> Given repository state and configuration now, what is the single most valuable bounded unit of missing maintenance work?

It should not depend on remembering that a timer fired.

A missed timer therefore causes delay, not permanent missing state.

## 6.2 Pure planner

Introduce a planner separate from execution:

```ts
planMaintenance(
  snapshot: MaintenanceSnapshot,
  config: MaintenanceConfig
): ReadonlyArray<MaintenanceUnit>
```

A `MaintenanceUnit` is declarative:

```ts
type MaintenanceUnit = BuildBundle | PruneBundles | RunFsck | RunGc | RunRepack;
```

Each unit contains everything necessary to identify the desired action but not mutable executor state.

## 6.3 Snapshot

```ts
interface MaintenanceSnapshot {
  readonly now: Date;

  readonly refs: Readonly<Record<string, Oid>>;

  readonly bundles: BundleManifest | null;

  readonly lastFsck?: Date;
  readonly lastGc?: Date;
  readonly lastRepack?: Date;

  readonly looseObjectCount?: number;
  readonly packCount?: number;
}
```

The planner does not perform I/O.

A separate reader gathers the snapshot.

## 6.4 Priority

Initial priority:

1. clean up invalid/incomplete derived artifacts;
2. build a required full bundle;
3. build a required incremental bundle;
4. prune retired bundle artifacts;
5. run overdue integrity check;
6. perform GC when its threshold is exceeded;
7. repack when pack/object thresholds justify it.

Correctness work must always outrank performance optimization.

If future `fsck` develops automatic repair through a configured remote, repair goes before bundle generation.

## 6.5 Bounded units

One scheduler tick executes one expensive unit by default.

Example:

```text
tick 1 → build incremental bundle
tick 2 → prune obsolete bundle
tick 3 → fsck
```

A unit may itself stream through many objects, but it has one externally meaningful outcome.

Cheap cleanup actions may be grouped when measured cost is negligible.

## 6.6 Scheduling adapters

Scheduling stays outside the planner.

### Node

Use a host timer or explicit service loop:

```text
git+ serve
→ maintenance scheduler
→ planner
→ one unit
```

### Cloudflare

Use the platform's appropriate scheduled/alarm mechanism.

The Cloudflare adapter merely invokes the same planner/executor.

### CLI

Expose:

```text
git+ maintenance plan
git+ maintenance run
```

`plan` performs no writes.

`run` executes the highest-priority planned unit.

Optional:

```text
git+ maintenance run --all
```

replans after each completed unit until no work remains.

## 6.7 Restart behavior

No maintenance step should rely on a transient "running" bit for correctness.

Example bundle build:

```text
artifact absent
→ planner says build
→ process crashes halfway
→ no manifest entry exists
→ next planner says build
```

Example successful build:

```text
artifact exists + manifest publishes it
→ planner no longer says build
```

This is the governing design pattern for all new maintenance features.

## 6.8 Failure behavior

A failed unit:

- marks its Operation failed;
- does not mark desired state satisfied;
- does not prevent unrelated later maintenance forever;
- is eligible to retry according to scheduler backoff.

Repeated failures should become visible in the UI/API.

The planner itself never returns a generic permanent "maintenance failed" state.

---

# 7. Cross-cutting Static Cache Contract

Bundle delivery should establish one reusable static-artifact HTTP helper.

Later candidates include:

- archive artifacts;
- SHA-addressed generated responses;
- immutable generated packs.

The helper owns:

```text
ETag
HEAD
Range
If-Range
Cache-Control
Content-Length
Content-Type
```

Do not apply `immutable` caching to ref-relative endpoints such as:

```text
/main/files/...
/HEAD/...
```

An API response is immutable only when all data determining that response is itself immutable and explicitly addressed.

---

# 8. Configuration

Suggested server configuration:

```text
bundles.enabled
bundles.fullMaxAge
bundles.incrementalMinAge
bundles.maxIncrementals
bundles.minNewObjects
bundles.filters

maintenance.enabled
maintenance.interval
maintenance.fsckInterval
maintenance.gcInterval
maintenance.repackThreshold
```

Application logic should consume configuration through the repository's existing Effect configuration conventions rather than reading environment variables directly.

Invalid bundle configuration fails closed at startup.

---

# 9. Observability

Add spans around:

```text
Bundles.snapshot
Bundles.build
Bundles.verify
Bundles.publish

Maintenance.plan
Maintenance.execute

Operation.start
Operation.complete
```

Useful metrics:

```text
bundle_build_seconds
bundle_bytes
bundle_objects
bundle_build_failures

git_fetch_bytes
git_fetch_objects
bundle_http_bytes

maintenance_unit_seconds
maintenance_failures

operations_running
operations_failed
```

The important product metric is:

```text
bytes served through Smart HTTP
vs
bytes served from static bundles
```

Bundle URI is successful only if it materially reduces the former.

---

# 10. Testing

## 10.1 Unit

Test:

- bundle-list serialization;
- creation-token ordering;
- prerequisite-chain construction;
- clone/catch-up list separation;
- planner determinism;
- planner priority;
- operation state transitions;
- cancellation state handling;
- static range parsing.

## 10.2 Store contracts

Add a `BundleStore` contract suite analogous to the existing storage contract.

Run against:

- memory/test backend;
- Node backend;
- Cloudflare/R2 backend.

## 10.3 Git interoperability

Use the real Git binary.

Required cases:

1. stock clone without bundle capability;
2. stock clone using server-advertised bundle URI;
3. clone followed by ordinary incremental fetch;
4. stale clone using catch-up bundle list;
5. unavailable bundle falls back successfully;
6. corrupt bundle falls back successfully;
7. private repository bundle requires authorization;
8. `blob:none` client selects only matching bundle family;
9. bundle plus final fetch produces the same reachable object graph as a normal clone.

Generated artifacts must pass:

```text
git bundle verify
```

## 10.4 Concurrency

Test:

```text
bundle snapshot captured
→ concurrent push occurs
→ bundle publishes old snapshot
→ subsequent fetch obtains new push
```

The result must be correct without rebuilding the bundle.

Also test two maintenance invocations racing to build the same desired artifact.

At most one manifest entry should become canonical; duplicate immutable bytes are harmless and collectible.

## 10.5 Crash injection

Inject failure:

- before artifact write;
- during artifact write;
- after artifact write;
- before manifest publish;
- after manifest publish;
- during obsolete-artifact cleanup.

After restart, the planner must converge to the correct state.

---

# 11. Rollout Plan

## Phase 1 — Static bundle foundation

Implement:

- `BundleStore`;
- bundle generation;
- immutable artifact serving;
- bundle verification;
- read-only bundle metadata API.

No automatic scheduling yet.

Success condition:

```text
git+ maintenance run
```

can build a bundle that stock `git bundle verify` accepts.

## Phase 2 — Protocol v2 clone acceleration

Implement:

- `bundle-uri` capability;
- `bundle-uri` command;
- clone bundle list;
- stock Git interoperability tests.

Success condition:

a fresh clone consumes the advertised bundle and the final Smart HTTP fetch transfers only the tail.

## Phase 3 — Catch-up bundles

Implement:

- incremental chain;
- `creationToken`;
- `/bundles/catchup`;
- `git+` setup of persistent fetch bundle URI;
- `blob:none` parallel family.

Success condition:

a days-stale clone downloads incrementals rather than a new full base.

## Phase 4 — Operations

Implement:

- Operation model;
- progress events;
- SSE;
- CLI rendering;
- operation API;
- cancellation.

Convert bundle generation and existing maintenance commands first.

## Phase 5 — Desired-state maintenance

Implement:

- pure planner;
- Node scheduler adapter;
- Cloudflare scheduler adapter;
- maintenance plan/run CLI;
- retry/backoff;
- UI presentation.

Bundle generation becomes maintenance-driven.

---

# 12. Acceptance Criteria

The feature set is complete when:

### Bundle URI

- Stock Git can discover a bundle through protocol v2.
- A clone remains successful when all bundle infrastructure is unavailable.
- Bundle artifacts are immutable and range-readable.
- No bundle is advertised before verification and complete storage.
- Private repository bundles enforce normal read authorization.
- `blob:none` bundles interoperate with stock Git.
- Routine catch-up never downloads a newly-created full base through the `git+` configured catch-up path.

### Operations

- Every bundle build has an operation ID.
- Progress is visible through CLI and SSE.
- Failure contains an actionable error.
- Cancellation interrupts eligible Effect work.
- Agent/hub tasks remain semantically separate.

### Maintenance

- `maintenance plan` is deterministic for identical state/configuration.
- Interrupted work is rediscovered automatically.
- One failed maintenance unit does not permanently block unrelated maintenance.
- Repeated scheduler invocations eventually converge to no planned work.
- No new repository correctness state exists solely in an in-memory scheduler.

---

# 13. Explicitly Deferred

Do not include in the initial implementation:

- WAL/time-travel repository snapshots;
- bundle generation for arbitrary historical wall-clock times;
- weekly/daily/hourly multi-tier scheduling;
- mandatory bundles for large clones;
- refusal of zero-have fetches;
- geographically distributed bundle mirrors;
- signed direct R2 URLs;
- automatic object repair;
- generic durable job queues;
- packfile URI.

Those can be evaluated after bundle effectiveness is measured.

## 14. Recommended First Slice

The first PR should establish only the protocol-independent foundation:

```text
BundleStore
+ BundleManifest
+ BundleBuilder
+ verification
+ static immutable artifact handler
+ unit/store/real-git tests
```

The second PR should add protocol v2 `bundle-uri`.

Keeping those separate proves that the derived artifact model is correct before changing Git wire behavior.
