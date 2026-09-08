# Internals

Contributor documentation: the module map, the reasoning behind the design,
code conventions, and the testing philosophy. Read [Architecture](../readme.md#architecture)
in the readme first — this document assumes it. Read this document and the
[Conventions](#conventions) section before your first change; they explain
constraints the code cannot show you.

- [Pinned dependencies](#pinned-dependencies)
- [Module map](#module-map)
- [Why it is built this way](#why-it-is-built-this-way)
- [Conventions](#conventions)
- [Testing](#testing)
- [Surfaces](#surfaces)
- [Deliberately not built](#deliberately-not-built)
- [Open questions](#open-questions)

## Pinned dependencies

Both `effect` and `alchemy` are pinned betas that break between releases; the
repo sets `save-exact=true` deliberately.

| package                 | version         | why it matters                                               |
| ----------------------- | --------------- | ------------------------------------------------------------ |
| `effect`                | `4.0.0-rc.111`  | `Schema`, `HttpApi`, `Stream` and the CLI are core           |
| `alchemy`               | `2.0.0-beta.74` | Workers stack as effects                                     |
| `@effect/platform-node` | `4.0.0-rc.111`  | node `FileSystem`/`Path`/`NodeRuntime` for the CLI           |
| `@effect/tsgo`          | `0.36.5`        | Effect diagnostics via Oxlint (`effect-tsgo patch --oxlint`) |
| `typescript`            | `7.0.2`         | TypeScript-Go                                                |

`alchemy` is behind a `patch-package` patch. Oxlint `typeCheck` and `tsc` both
run in `npm run check`; Effect rules live in Oxlint, not `tsc` (`diagnostics: false`).

## Module map

| module                       | what it is                                                           |
| ---------------------------- | -------------------------------------------------------------------- |
| `src/git/Error.ts`           | tagged errors with `httpApiStatus` annotations                       |
| `src/git/Store.ts`           | `ObjectStore` / `RefStore` ports                                     |
| `src/git/Format.ts`          | the pure/effectful seam — framing, commit and tree codecs, hashing   |
| `src/git/Repository.ts`      | the domain service                                                   |
| `src/git/Memory.ts`          | in-memory backend                                                    |
| `src/git/Node.ts`            | filesystem backend, git's own on-disk layout                         |
| `src/git/Cloudflare.ts`      | R2 + Durable Object SQLite backend                                   |
| `src/adapters/Opfs.ts`       | browser (OPFS) backend — same loose-object layout                    |
| `src/git/Durable.ts`         | the Worker entry: one Durable Object per repository                  |
| `src/git/Pack.ts`            | streaming packfile transport, platform-neutral, git-interop-tested   |
| `src/git/PackFile.ts`        | random access into a pack at rest, via `PackIndex.ts`'s `.idx` codec |
| `src/git/Packed.ts`          | `PackStore` port; decorates an `ObjectStore` with packed reads       |
| `src/git/Inflate.ts`         | pull-based zlib inflate — exact stream boundaries, no `node:*`       |
| `src/git/Inflate.zlib.ts`    | the same, on `node:zlib`, for reading packs at rest on node/workerd  |
| `src/git/Diff.ts`            | unified diff, byte-identical to `git diff --no-index`                |
| `src/git/Merge.ts`           | three-way merge, byte-identical to `git merge-file --diff3`          |
| `src/git/Index.ts`           | git's own `DIRC` v2 index codec                                      |
| `src/git/Work.ts`            | `WorkTree` / `IndexStore` ports; `Checkout.ts` is the porcelain      |
| `src/git/Rebase.ts`          | replay: cherry-pick, and rebase as a sequence of them                |
| `src/git/Bisect.ts`          | which commit first broke it, as a function of the good/bad marks     |
| `src/git/History.ts`         | `git log -- <path>`, history simplification included                 |
| `src/server/Protocol.ts`     | git smart-HTTP: advertisement, upload-pack, receive-pack, v0 and v2  |
| `src/server/Api.ts`          | JSON API as one `HttpApi` declaration; the client derives from it    |
| `src/server/Route.ts`        | URL → repository name, in one place; strips the `.git` suffix        |
| `src/server/Auth.ts`         | scoped tokens: guard on both surfaces, HMAC or revocable verifiers   |
| `src/server/Webhooks.ts`     | signed push delivery: `Schedule` retry, backgrounded, per-subscriber |
| `src/server/Lfs.ts`          | Git LFS batch API and transfer, per-platform streaming digest        |
| `src/server/Archive.ts`      | tree → tar / tar.gz / zip, streamed                                  |
| `src/server/CommitPack.ts`   | NDJSON bulk commit, parsed as a stream                               |
| `src/server/Remotes.ts`      | named remotes; `Remotes.node.ts` is the JSON-file store              |
| `src/host/Node.ts`           | node host: the same handlers behind `node:http`, self-hostable       |
| `src/host/Cloudflare.ts`     | the Workers host                                                     |
| `src/client/Fetch.ts`        | smart-HTTP fetch client: `lsRemote`, clone, incremental fetch        |
| `src/client/Push.ts`         | smart-HTTP push client                                               |
| `src/client/Client.ts`       | browser client: derived JSON client, clone, local `Repository`       |
| `src/cli/bin.ts`             | `bin`: node's compile cache on, then `main.ts`                       |
| `src/cli/main.ts`            | CLI: porcelain, hub, social, queue, session, task, wake              |
| `src/cli/sea.build.ts`       | `npm run build:sea` — the CLI as one node SEA binary (node 26+)      |
| `src/crypto/SshSignature.ts` | SSH sign / verify                                                    |
| `src/trust/`                 | genesis, trust log, certificates, PrincipalID, known_repos           |
| `src/hub/`                   | event DAGs: PRs, sessions, tasks, queue, redaction, memory           |
| `src/social/`                | identity-repo social log, introduction, inbox, external review       |
| `src/artifacts/Namespace.ts` | local Cloudflare Artifacts provider over alchemy's binding tag       |
| `src/artifacts/Sqlite.ts`    | the provider's registry + tokens on Durable Object SQLite            |
| `src/alchemy.run.ts`         | deployment stack: bucket, DO and Worker as values, not config        |
| `src/git/Store.contract.ts`  | one storage contract suite, run against all four backends            |

**Do not reach past the domain.** An early version had `Api.ts` and
`Protocol.ts` doing `yield* RefStore` directly. It compiles — and it makes
storage a _per-request_ requirement of every route, so `toWebHandler` demands a
`Context<ObjectStore | RefStore>` on every call and no host can bind an app
instance to a repository. Route reads through `Repository`. The shortcut is
tempting each time the JSON surface grows.

## Why it is built this way

Replay publication captures its destination-ref expectation before resolving
the source and onto revisions. A concurrent update during those initial reads
must fail the final compare-and-swap, just like movement during tree replay.

This is a ground-up rewrite of an earlier implementation. Four problems in that
code were structural rather than stylistic, and they are the whole argument:

1. **A push could take the isolate out.** The pack parser read the entire
   request body into one `Uint8Array` before validating the header. A Durable
   Object gets 128 MiB, so a push of a repo with a few large blobs OOM'd — and
   the failure mode was an isolate reset, not an error the client could read.
   Upload-pack had the mirror problem: the object walk completed into an array
   before the first byte went out. Everything here streams instead.

2. **Errors were strings by the time they mattered.** A `code` field, and
   handlers rediscovering meaning with `instanceof` at ~40 sites, with two
   separate error-to-status mappings. Nothing told a caller of
   `repository.commit(...)` that a ref conflict was possible. Now every failure
   is in the type, and its status comes from its own annotation.

3. **Cancellation stopped at the door.** One `throwIfAborted()` at the top;
   below it, an aborted clone kept walking objects and kept paying for it.

4. **Three storage implementations, one of them lying.** A 16-method interface
   with `applyRefChanges?` optional, provided only by Cloudflare — so the
   browser client hand-rolled read-then-write ref updates and raced itself
   across tabs. Callers branched on whether a method existed. The port split
   fixes this: `RefStore.apply` is compare-and-swap on every backend, and one
   contract suite proves it.

Effect addresses 1–3 directly; the port split addresses 4. Alchemy addresses a
smaller, separate problem: three sources of truth (`wrangler.json`, the
generated types, and a hand-written binding lookup) for one binding.

### One Durable Object per repository

Not an arbitrary mapping: the DO's input gate provides the serialization that
`RefStore.apply`'s compare-and-swap demands. The filesystem backend reserves
Git-compatible `.lock` files around comparison and atomic renames, including
`packed-refs.lock` for deletion and multi-ref rollback. The Node host also
serializes its own requests with a per-repository mutex and keeps one cached
layer per repository name.

The Node adapter propagates premature HTTP response closure through the web
request's abort signal and each direct Effect runtime. A canceled request
waiting for the repository gate does not enter its handler. Response delivery
still owns the repository lease until streaming finishes or is canceled.

Native worktree mutations reserve Git's `index.lock` through `IndexStore.withLock`
before reading the index, and retain it through ref and merge-state updates.
The callback receives index access owned by that reservation; its Effect has no
environment requirements. Checkout binds its captured services inside the callback.
Direct index saves also reserve the lock. Contending filesystem writers fail
with `StorageFailure`/`EEXIST` and can retry, while the memory index uses a semaphore.
Read-only status remains available. Index publication and individual filesystem
mutations finish before interruption releases the reservation. Failure and
interruption release it, and failed index publication removes temporary output.

Queue cleanup deletes a settled candidate branch only if it still holds the OID
that the pass observed or published. `Repository.deleteRef` forwards an optional
expectation to the ref store's atomic update, preserving a concurrent writer's
replacement while allowing the queue pass to finish its bookkeeping.

Queue settlement identifies an entry by its `queue.entered` event commit. A
conditional leave reprojects after an append conflict and preserves re-entries,
including those naming the same head. Merge bookkeeping similarly rechecks the
current PR head and appends against the raw ref that preceded that check.
`Event.appendTo` does not automatically rebase an explicitly guarded append;
the caller owns revalidation. Candidate cleanup runs only for entries the pass
actually removed, so a retained proposal keeps its candidate ref.

Native ignore discovery loads `core.excludesFile` before `info/exclude` and
per-directory `.gitignore` rules. `IgnoreConfig.node.ts` loads system, XDG, home,
repository, and enabled worktree config in order, expanding unconditional
includes at their position. Effect `Config` supplies HOME, XDG_CONFIG_HOME, and
the Git global/system overrides when the worktree layer is built. With no
configured excludes file, it reads XDG's default `git/ignore`. An explicit empty
local setting disables that fallback. Relative excludes paths resolve from the
selected worktree root; include paths resolve from the including config file.
Quoted Git values and `~/` expansion are supported. Linked worktrees use common
repository config followed by their own enabled `config.worktree`. Conditional
includes and command-scoped Git configuration are not yet interpreted. The
default system path is `/etc/gitconfig`; installations using a different build
prefix can specify `GIT_CONFIG_SYSTEM`.

`WorkTree.trustExecutableBit` exposes `core.filemode` without adding environment
requirements to worktree operations. Memory worktrees trust modes. Native status,
add, and removal safety checks preserve an indexed regular file's mode when
filemode is disabled; new regular files use 100644, while symlink/type changes
still count. Resolving an unmerged path takes the stage-2 mode when present.
Staged mode differences remain visible regardless of the filesystem setting.

Native status reports nonzero index stages in `unmerged`, with Git's two-letter
conflict code derived from the stages present. Those paths are excluded from
ordinary staged, unstaged, and untracked lists. Editing a conflicted file does
not resolve it; staging a resolution replaces its conflict stages. The CLI
prints conflict codes alongside ordinary porcelain rows, and non-forced
checkout explicitly refuses any unmerged paths before checking other changes.
Successful checkout clears abandoned merge bookkeeping after publishing the
index and HEAD. Those three completion steps are uninterruptible under the index
reservation, so cancellation cannot leave a newly published checkout with an
old merge parent queued for its next commit. Refusals preserve merge state.

File-backed remotes and webhook subscribers reserve a sibling `.lock` file
before reading and editing their JSON snapshot. Contending writers fail with
`StorageFailure`/`EEXIST` and may retry; readers continue to see the complete
published file. Failed edits remove their reservation and temporary output.

Creating an empty filesystem repository calls `Node.initializeBare` explicitly:
Git needs `objects/`, `refs/` and HEAD even before the first commit. Store layers
remain safe to open for reads without creating those paths. The native CLI and
Artifacts provider use this initializer for their creation paths.
The Node host uses `NodeStorage` to initialize on the first object, ref or pack
write. Its initialization guard is shared by concurrent writes and permits retry
after a filesystem failure; read-only requests do not create repositories.

Shallow history is repository metadata on `RefStore`: `shallow` reads the
boundaries and `updateShallow` merges additions/removals. Node uses Git's
`shallow` file and `shallow.lock`; memory, OPFS, and Cloudflare retain the same
set in their own storage. Fetch persists boundary changes after unpacking and
before publishing refs. Artifacts forks copy the boundary set, and published
snapshots carry it to stateless readers. `Repository.readCommit` preserves the
stored parent headers; `readHistoryCommit` removes parents only in the history
view, so traversal stops even if older objects are available through alternates.
Push sends the source's shallow declarations before its ref commands. The
receiver preserves its own existing boundaries and follows Git's default of
refusing refs that require new ones. `server/Shallow.ts` captures established
receiver history before unpacking, so retrying a refused pack cannot turn its
leftover objects into an accepted boundary. Partial pushes report each refusal;
an atomic push with any refusal changes no refs.

Tree revision lookup peels annotated tags until it reaches a tree or commit.
The JSON file/diff endpoints, native tree and restore commands, search, and archive export
share this resolver. Shallow fetch also peels the complete tag chain before
applying depth to the target commit, retaining the tag objects in the transfer.
Branch creation, log and path-history traversal, merge operations, ancestry
checks, cherry-pick and rebase peel tag revisions to a commit as well.
Branches store that commit's OID; logs emit commit OIDs and resolve the starting
revision separately for each stream execution. Raw `readCommit` and `readTag`
methods continue to require the requested object type.

Native worktree commits read pending merge heads through `MergeState`, alongside
the index and filesystem ports. Its Node layer uses the selected checkout's Git
directory, including linked-worktree metadata. Commit records those heads after
the existing branch parent, permits a merge with an unchanged tree, and pins the
branch expectation while publishing. Successful publication clears Git's merge
metadata; a rejected commit retains it for retry. Publication and cleanup are
not interrupted between those steps. In-memory checkouts provide `MergeState.none`.

Filesystem discovery applies repository `info/exclude` followed by root and nested
`.gitignore` files, using the pinned `ignore` matcher. Nested patterns are rebased
to the checkout root so later negations retain their precedence. Ignored directories
are not walked. Status and add consult indexed paths directly, so ignored tracked
files can still be modified or deleted. Linked worktrees read excludes from their
common repository, and matching uses the repository's `core.ignorecase` setting.

Discovery receives indexed paths so it can stop at gitlinks while still walking
ordinary tracked directories that later acquire nested repository metadata.
Embedded checkouts are staged through `WorkTree.gitlink`, which reads their HEAD
using the filesystem ref store, including git-directory files, common directories
and packed refs. The index stores mode 160000 and that commit OID; it never stores
the nested checkout's files as part of the gitlink. An unborn nested checkout
fails staging before index publication. Deinitialized submodules retain their
existing gitlinks. Status compares an initialized submodule's HEAD with its
indexed gitlink, so advancing or detaching to another commit is reported as an
unstaged modification without changing the index.

Non-recursive checkout preserves a nested repository when its gitlink disappears
from the target tree. Returning to a branch with that gitlink updates the outer
index without overwriting the preserved nested checkout. Its content and HEAD
remain unchanged in both directions, including forced checkout.

Normal checkout checks untracked path ancestry before changing files. A target
file cannot replace a directory containing untracked content, and an untracked
file cannot occupy a target directory's path. Both are refused with index, HEAD
and worktree untouched; the preflight covers more than exact filename matches.

After that preflight, `WorkTree.prepareWrites` discovers and removes files
blocking target directories and directories blocking target files. This lets
forced checkout and replacement of ignored content complete. Gitlink destinations
are excluded. Cleanup precedes target writes and finishes before interruption
releases the index reservation; arbitrary filesystem failures are still reported.

Outgoing pushes retain the object plan and stream the pack through `client/Upload`.
Each authentication retry starts a fresh body. The upload scope owns its reader
and socket cancellation, including early refusals. Browser fetch cannot send a
request stream over HTTP/1.x, so that host streams into a temporary OPFS file and
passes the resulting File to fetch. Normal completion, failure and interruption
remove it. Per-file Web Locks preserve active uploads across tabs; the next
upload reclaims files whose owning tab has closed. This browser path requires
OPFS and Web Locks. Push, client fetch, and server-side fetch reuse the final
advertisement URL after an initial redirect for subsequent protocol requests.

The local Artifacts registry reloads its JSON file for each read. Updates reserve
`.registry.json.lock` before reading and replace the file by atomic rename before
releasing that reservation. Writes through one handle queue; competing handles
or processes can retry a typed lock-contention error. `RepoStores.reserve` separately
holds each repository name through a complete lifecycle operation, including
rollback; forks reserve both source and target. Memory stores share a reservation
map, while Node uses per-name files under `.operations` to coordinate providers
and processes. A process killed without finalizers leaves locks requiring
operator recovery. Fork bookkeeping separately reserves `.forks.json.lock` and
reloads the current links before changing them. Reads refresh the document;
changed links invalidate composed stores, including cached descendants. A local
semaphore keeps refresh and composition from racing that provider's own writes.
Token snapshots still belong to individual providers and need separate review.

Node object stores cache each alternates file separately and validate every file
in the transitive chain on lookup. An intermediate repository can change its
object source without changing a child's own alternates file. Unchanged files
reuse their parsed paths; changed files refresh their outgoing links, and cache
entries outside the current chain are discarded.

### The pure/effectful seam

`src/git/Format.ts` is the seam — pure below, effectful above. Byte work with
no I/O (framing, commit and tree codecs, hashing, delta application, the index
codec) stays synchronous: Effect buys nothing there and costs an allocation per
call. Functions that would throw return a `Result`; anything that reaches for
storage moves up into `Repository`.

The codecs there round-trip: `encodeCommit(parseCommit(bytes))` is `bytes`, and
the same for trees and tags. That is a requirement rather than a nicety,
because an object's id is a hash of exactly those bytes — a codec that
paraphrased would have a replay silently publish something its author never
wrote. Two rules make it hold, and both cost nothing on an object that has
neither problem:

```text
raw      the bytes a text field was read from, kept only where
         decoding is not reversible. git stores names, messages
         and signature lines as bytes; a TextDecoder turns what
         is not UTF-8 into U+FFFD, and encoding that back writes
         different bytes. `TreeEntry`, `Signature`, `CommitInfo`
         and `TagInfo` each carry one

headers  the header lines a codec does not interpret —
         `encoding`, `gpgsig`, `mergetag` — kept in order, with
         their continuation lines, and written back after
         `committer` where git puts them
```

A rewrite decides for itself what to carry. `Rebase` copies `raw` — it is the
same message — and deliberately drops `headers`: a signature over the commit it
was made on says nothing true about the new one.

## Conventions

### Errors

`Schema.TaggedError` classes in `src/git/Error.ts`, each carrying an
`httpApiStatus` annotation. A `RefConflict` crosses the wire as
`{ "_tag": "RefConflict", ref, expected, actual }` with its status read from
the class, not from a mapping table. Do not hand-roll `_tag` classes, and do
not add a status field.

### Ports and layers

`Context.Service<Self, Shape>()("key")` plus `Layer.effect`/`Layer.sync`
returning `Service.of({ … })`. Effect v4 has no `Effect.Service` class helper.

A port with no caller is dead code — but so is a codec with no port. Both
halves of that rule have been load-bearing here; see
[Deliberately not built](#deliberately-not-built).

The storage ports are traced by one decorator rather than per-method edits:
`tracedObjectStore` and `tracedRefStore` wrap an implementation, so span names
live in one place and a new backend cannot forget to name itself. A trace reads
`Repository.commit → Cloudflare.RefStore.apply` whichever storage is loaded.

### Streaming

`Stream`/`Channel` end to end. Nothing reads a request body whole; nothing
collects an object walk before the first byte goes out. `Repository.unpack`
writes objects to the store as they resolve, so an `OFS_DELTA` base is re-read
by oid from storage and only the object being decoded is resident — no window,
no second pass.

Handlers that consume large bodies must be dispatched _before_ anything that
would buffer them. In both hosts, LFS and `commit-pack` are tried ahead of the
JSON API for exactly this reason.

One object still has to be resident while it is decoded, and how large that is
comes from the pack's own header — a number written by whoever sent it. So it
is judged before it is honoured: `Pack.MAX_OBJECT_BYTES` is what a host accepts
by default, and `Pack.MaxObject` is the layer a host with a different budget
declares instead. The four hosts differ by an order of magnitude — a Durable
Object has 128 MiB, a browser tab less — and a bound taken from the input being
validated is not a bound. Read as the declared size alone, one object could ask
for 512 MiB (`Inflate.MAX_INFLATED`, the decoder's own backstop) from about half
a megabyte of pack, since deflate reaches ~1000:1; the size check that follows
an inflate can only report a bomb that has already been built.

### Concurrency and lifetime

`RcMap` for reference-counted per-repository instances (a plain `Map` leaked —
every repository ever touched stayed resident on the node host).
`PartitionedSemaphore` is _not_ a per-key mutex: its permits are capacity
shared across keys. The node host uses a per-repository promise chain.

Wake dispatch reserves `wake.cursor.json.lock` through cursor reading, rule
execution and cursor publication. Concurrent CLI or host processes receive a
typed contention error and can retry; dry runs do not acquire the reservation.
Success, failure and interruption release it. After an unclean process exit,
an operator must confirm the owner has stopped before removing its stale lock.

Node post-receive work belongs to the host scope, including wake rules, webhook
delivery and replication. It survives the triggering request; host shutdown
interrupts it and waits for finalizers. The serve CLI owns its host as a scoped
resource, so SIGINT/SIGTERM closes it before the runtime exits. It uses
`close({ force: true })` to stop unfinished HTTP bodies before awaiting
background cleanup; programmatic close drains requests unless force is requested.

### HTTP

`HttpApi` declares the JSON surface once; the client is derived from that
declaration rather than re-declaring payload types. Handler requirements are
request-scoped and resolved from what the app layer _outputs_, which is why
hosts compose with `Layer.provideMerge` rather than `Layer.provide`.

### Portability

No `node:*` in anything below `host/` or the `*.node.ts` files. Platform
specifics live in per-platform modules — `Lfs.node.ts` / `Lfs.cloudflare.ts`,
`Subscribers.node.ts`, `Work.node.ts`, `Remotes.node.ts` — following the same
naming convention. `Inflate.ts` exists because `node:zlib` does not.

It is also 52x slower than `node:zlib`, so `PackStore` carries an optional
`inflate` and the two server backends pass `Inflate.zlib.ts`. Only reading a
pack _at rest_ can take that shortcut: `Pack.ts` reads a pack off the wire,
where objects are back to back and the decoder has to report where each stream
ended, and it keeps the portable one everywhere. The browser keeps it for both.

Alchemy's Cloudflare bindings return effects requiring `RuntimeContext`. Do not
thread it through port signatures: the typechecker will drag it into the CLI
and the test suite, neither of which runs on Workers. The Cloudflare layer
captures it with `Effect.context` and provides it inward, so ports stay
`R = never`. The cost is that the layer must be built inside the invocation
rather than memoized per instance — a cached context would pin a stale
`ExecutionContext`.

### Effect v4 in this repo

Read `.claude/skills/effect` before writing Effect code. Points that have
actually bitten:

- `Effect.catch`, not `catchAll`. `Effect.ignore`, not `ignoreLogged`.
  `Schema.Literals([...])`. There is no `Stream.mapConcat` — use
  `Stream.flatMap`.
- `Config`, not `process.env`, in application logic. A malformed `PORT` should
  fail naming the variable, not silently become `NaN`.
- Effect `HttpClient` for outgoing calls. `HttpClient.retryTransient`'s
  built-in classification (transport errors, timeouts, 408, 429, 5xx — and
  _not_ other 4xx) is the policy hand-rolled predicates reach for.
- `Effect.fn("Domain.operation")` on public and non-trivial operations, so a
  push or fetch shows its cost where it is spent.
- Do not use `as any`, a cast, or a non-null assertion to **silence a typing
  problem**. A cast is a claim the compiler cannot check, so where it stands in
  for a check it carries a `SAFETY:` comment naming the invariant that makes it
  true — `Certificate.validate` has already proved this subject is a
  fingerprint, `take` has already returned exactly twenty bytes — and two of
  them need a line-scoped diagnostic suppression besides. An `as any` or a
  widen-then-assert has no such reason available and is refused outright by the
  `anti-slop` rules in `tools/oxlint/`.

  A `!` on a value the surrounding code has just established — `stack.pop()!`
  inside `while (stack.length > 0)`, `parts[0]!` after a length check — is not
  in that class and is used freely: the invariant is on the line above, and
  spelling it out with a branch that cannot be taken buys nothing. What the
  rule is about is `!` standing in for a check nobody made. Roughly eighty of
  the former exist; an earlier version of this paragraph counted the latter and
  read as though it were counting all of them.

Two deviations are deliberate: raw `fetch` in `client/Fetch.ts` because it is
the browser transport, with Effect discipline kept inside the adapter; and no
module-namespace re-export style, because the existing style is consistent
already. Failures are `Schema.TaggedError` (or `Data.TaggedError` where the
value is thrown, not yielded).

## Testing

`npm test` runs both vitest projects — `unit` and `integration` (the workerd
harness). `npm run test:unit` is the fast loop, while `npm run test:integration`
runs the workerd project alone. The interop tests
need `git` on `PATH` and skip without it; the real-browser test needs Chromium
via Playwright and skips without it.

The governing rule: **verify against the real `git` binary wherever a claim is
about git.** A test that builds its own history and checks its own expectations
will agree with a bug. Two bugs on the working-tree branch were found only by
comparison with git, and neither would have failed a self-consistent test:

- `Repository.log` followed first parents only — indistinguishable from correct
  on a linear history, and silently omitting every commit that arrived by a
  merge on any other.
- The fetch client read exactly one pkt-line before the pack, while real
  `upload-pack` emits one `ACK` per recognised have. That surfaced only when
  the client was pointed at stock `git-http-backend`.

Four kinds of evidence, deliberately:

- **oids match real git** — the empty tree and `hello\n` are pinned to what
  `git hash-object` produces.
- **the repository is really a git repository** — `Node.interop.test.ts` writes
  one through the ports and has the binary read it: `fsck --strict`, `log`,
  `cat-file`, `ls-tree`, `show`.
- **the contract holds on the backend that ships** — `Store.contract.ts` runs
  against DO SQLite and R2 inside workerd, not against a mock.
- **behaviour matches git's, not our description of it** — `git status`
  porcelain, `git log -- <path>`, `git rev-list --bisect`, `git ls-files
--stage`, `verify-pack`.

Where git's answer is genuinely one of several equally good ones — bisect on an
even split — assert the property (`--bisect-all` distance is maximal), not
git's particular pick. Pinning a tie-break tests git's implementation detail,
not ours.

Use `it.live` for anything touching real HTTP, subprocesses, or a retry
schedule; `it.effect` otherwise. Prefer making a retry policy a parameter over
reaching for `TestClock`: `Webhooks.test.ts` passes a 1 ms base delay and
counts attempts against a real receiver, testing the schedule _and_ the HTTP
behaviour in one pass.

## Surfaces

### smart-HTTP

Protocol v0 and v2, shallow (`deepen`, `deepen-since`, `deepen-not`,
`--unshallow`), `side-band-64k` on both fetch and push, and incremental fetch
with `have` negotiation. Stock `git` clones, pushes, and fetches; `…/repo.git`
and `…/repo` are the same repository.

Negotiation speaks `multi_ack_detailed` on both sides: the client offers
haves in rounds of 32 up to a cap of 256, the server tags every common
commit (`ACK <oid> common`), and `ACK <oid> ready` ends the conversation the
moment `Repository.canServe` — git's `ok_to_give_up`, a budgeted walk from
each want down to the common set — proves a pack can be cut. The same
predicate is what makes protocol v2's `ready` honest rather than eager. A
client that never requests the capability gets baseline single-ACK, and
either way negotiation only narrows the pack: it never changes which objects
arrive.

### JSON API

One `HttpApi` declaration per repository, errors crossing as tagged values.
Covers content (`commit`, `commit-pack`, `blob`, `tree`, `files`, `file`,
`object`), history (`log`, `commits`, `diff`, `history`, `bisect`), refs
(`refs`, `branches`, `tags`, `reset`, `reflog`), rewriting (`merge`,
`cherry-pick`, `rebase`), search (`grep`), remotes (`remotes`, `fetch`,
`push`, `pull`), maintenance (`fsck`, `gc`), and webhook registration — which
is what makes a push deliver.

`commit`, `commits` and `log` answer one enriched commit view — oid, message,
subject, author, ISO date, parents — so a client never re-parses a raw object
for an author line; `object` stays the low-level endpoint underneath. Ref-ish
inputs take `main`, `refs/heads/main`, `HEAD` or an oid, normalized by one
resolver at the boundary; `file` answers UTF-8 text on request and refuses
binary content rather than corrupting it.

A repository can act as a client of another: remotes are registered per
repository with an optional credential that is _stored rather than sent_ (a
token in a request body is a token in an access log), and `pull` reports a
non-fast-forward as its own outcome rather than guessing whether a merge or a
rebase was wanted.

### Working tree

CLI invocation parsing resolves explicit `--git-dir` and `--work-tree` paths
after all `-C` options. A global `--bare` supplies the directory at that option
only if no Git directory selector is already set. Delegated Git commands and
native extensions receive the same normalized invocation.
Each nonempty `-C` resolves through the filesystem before the next option:
symlinks and `..` follow directory-change semantics, and a failed intermediate
step stops the command. Existing explicit selectors are canonicalized too;
nonexistent selectors remain available to `init` without collapsing their path
components.

Inbox submission owns its Git subprocess and, on Unix, its transport process
group. Interruption requests termination, forces exit after a short grace period
if necessary, and waits for pipes and processes to close before returning.

Installed session hooks scope their pending report to the harness's `session_id`,
so separate harnesses in one checkout retain their own prompts and reports. State
filenames use the ID's SHA-256 digest; callers without an ID use the legacy
`.chr33s/session.id`. A stop removes only its selected pending report.

`status`, `add`, `rm`, `mv`, `restore`, `switch`, `commit`, over an index at
`.git/index` in git's own `DIRC` v2 format. `WorkTree` describes files on disk,
`IndexStore` holds staged content and exclusive edit access, and `MergeState`
holds pending merge metadata. A server needs none of these; the native CLI binds
them to its selected checkout.

Both implementations can be pointed at the same checkout. `git+ status`
prints git's porcelain, `git status` reads the index we wrote, and
`git fsck --strict` accepts the history we commit.

The working-tree verbs are **not** on the HTTP API, and that is the one
boundary drawn on purpose: a bare server has no files, so serving `add` would
mean inventing a work tree behind the API. The server-side spelling is
`POST /:repo/commit-pack`, which streams an NDJSON body of file frames into a
commit without holding more than one file in memory.

### Auth

There is no server secret and no deploy binding to set. A repository's
authority is its own: `refs/meta/trust/genesis` names the root keys,
`refs/meta/trust/log` records every grant and revocation, and both hosts read
the same membership state to decide what a request may do. A repository with no
genesis is a plain git repository and is served as one.

A repository with no genesis is readable by anyone who can reach the host and
writable by nobody — it has no membership to authorize a write with. `serve
--open` (`allowAnonymousWrites`) serves writes to those repositories anyway,
which is what a scratch server wants and what a shared one should not have.

Clients present one of two things. A _delegated credential_ is a short-lived
token the holder signs with their own SSH key and `git` carries as
`http://<credential>@host/repo` — `git+ credential` mints it, and it can
never carry more than the person running it already had. A _signed envelope_ is
the native path: the server issues a nonce, the client signs the operation and
the refs it is moving, and the signature is checked against the same log.

## Deliberately not built

Recorded because the reasoning is worth more than the files would have been.

**A provider-neutral `RepoHost` seam.** The plan was one `App` value naming the
storage ports plus a host port supplying `stores`, `serialize` and
`background`. What shipped is two concrete hosts sharing `Protocol.handle` and
`Api.layer` directly. The seam was unnecessary: the handlers already require
nothing but `Repository`, so host-neutrality came free from the effect
requirements and an extra service would only have restated it. If a third host
ever needs the three capabilities as one value, the port is a twenty-line file
away.

**An `IndexStore` port — deferred, then built.** The argument for deferring was
that a port with no caller is dead code. That reasoning holds; what it missed
is that the situation was already the mirror image. The _codec_ had landed in
`git/Index.ts` — byte work the real `git` binary reads and writes, verified
both directions — with no service, no layer and no caller. A format
implementation with nothing on either side of it is not a port waiting for its
feature; it is a feature waiting for its port. `git/Work.ts` is the result, and
`IndexStore` came to about twenty lines over the codec, as predicted.

**Delta creation on the request path.** `createDelta` and an ofs-delta
sliding window live in the pack writer behind `PackOptions.deltify`, and only
`Maintenance.repack` turns it on: repack is background work whose output is
storage, so the window's CPU and pinned memory buy smaller packs at rest
without costing any fetch response its first byte. Repack also feeds the
writer `deltaOrder` — type-major, `pack_name_hash` over tree-entry names,
largest first — because a reachability walk emits a commit's blobs together,
which parks two versions of one file a whole commit apart and outside any
window. Live upload-pack responses
stay full-object until measurement says the wire savings justify serve-time
delta search — the worst case of not deltifying is a larger pack, never a
wrong one. Thin packs are read today (a `ref-delta` whose base is outside the
pack resolves from the store) but never written; writing them is the wire
half of this same trade, and additionally leans on the negotiated common set
for its bases.

## Open questions

- **Bundle size in a Worker is unmeasured.** Effect core plus `unstable/http`
  and `unstable/httpapi` is not small, and nothing has checked it against the
  3 MiB compressed limit.
- **`alchemy` is still beta** and breaks between releases. Effect is on RC.
- **Fetch-path deltas and thin packs.** Repack writes ofs-deltas; live
  upload-pack stays full-object. Measure before spending serve-time CPU on
  delta search.
