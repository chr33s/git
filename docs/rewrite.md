# Sketch: Effect v4 + alchemy@next rewrite

Status: **phases 0–4 have landed** — the git core in `src/git/`, the
smart-HTTP protocol in `src/server/Protocol.ts` (stock `git` clones from and
pushes to the Durable Object in the integration suite), and the JSON API as an
`HttpApi` in `src/server/Api.ts` with a derived client driving it in the
tests. Phase 5b's node host landed dependency-free in `src/host/Node.ts` —
self-hosting is one command. Phase 6's CLI landed in `src/cli/main.ts` on
`effect/unstable/cli`, with the smart-HTTP fetch client it needed extracted to
`src/client/Fetch.ts`. Phase 5's stack landed too: `src/alchemy.run.ts` and
`src/host/Cloudflare.ts` are real code against real alchemy types, unblocked
by a one-symbol patch (`patches/effect+4.0.0-beta.107.patch` aliases
`Schema.TaggedErrorClass`, the name `@distilled.cloud/*` is built against).
Every phase has landed, webhooks included, and the sketches are gone — each
one either shipped or is recorded under "paths not taken" below.

What is real code today, running under the repo's own test runner:

| module                       | what it is                                                               |
| ---------------------------- | ------------------------------------------------------------------------ |
| `src/git/Error.ts`           | the tagged errors, with `httpApiStatus` annotations                      |
| `src/git/Store.ts`           | `ObjectStore` / `RefStore` ports                                         |
| `src/git/Format.ts`          | the pure/effectful seam — framing, commit and tree codecs, hashing       |
| `src/git/Memory.ts`          | in-memory backend                                                        |
| `src/git/Node.ts`            | filesystem backend, git's own on-disk layout                             |
| `src/git/Repository.ts`      | the domain service                                                       |
| `src/git/Cloudflare.ts`      | R2 + Durable Object SQLite backend                                       |
| `src/git/Durable.ts`         | the repository as a Durable Object                                       |
| `src/git/Pack.ts`            | streaming packfile transport — reader (full, ofs- and ref-delta), writer |
| `src/server/Protocol.ts`     | smart-HTTP v0: advertisement, upload-pack, receive-pack                  |
| `src/server/Api.ts`          | the JSON API as one `HttpApi` declaration, client derived from it        |
| `src/host/Node.ts`           | node host: the same handlers behind `node:http`, self-hosting supported  |
| `src/server/Auth.ts`         | scoped tokens: guard on both surfaces, HMAC or revocable verifiers       |
| `src/client/Fetch.ts`        | smart-HTTP fetch client: `lsRemote` + clone, shared by import and CLI    |
| `src/cli/main.ts`            | the CLI on `effect/unstable/cli`: init, refs, log, clone, serve, token   |
| `src/artifacts/Namespace.ts` | local Cloudflare Artifacts provider over alchemy's binding tag           |
| `src/git/Store.contract.ts`  | one contract suite, run against **all three** backends                   |

171 tests pass: 160 unit (`npm test`, one of them cloning inside real Chromium) and 11 integration (`npm run test:integration`),
the latter driving a real Workers runtime and itself running a 15-case
conformance suite inside it. Three kinds of evidence, deliberately:

- **oids match real git** — the empty tree and `hello\n` are pinned to the
  values `git hash-object` produces;
- **the repository is really a git repository** — `Node.interop.test.ts` writes
  one through the ports and has the `git` binary read it: `fsck --strict`,
  `log`, `cat-file`, `ls-tree`, `show` all agree;
- **the contract holds on the backend that ships** — the same suite runs
  against DO SQLite and R2 inside workerd, not against a mock.

The legacy implementation — the flat `git.*.ts`, `server.*`, `client.*` and
`cli.*` files and their tests — has been removed; the Worker now serves the
Durable Object path (`src/git/Durable.ts`).

The sketch code (the `*.ts` files under [`src/`](../src)) is illustrative, but it is not hand-waved: it
typechecks against the real `effect@4.0.0-beta.107` and `alchemy@2.0.0-beta.70`
type definitions (`tsc --noEmit`, strict, no `any` escapes). Bodies that would
just be ported code are `declare const` stubs, so the shapes and the wiring are
checked while the git internals stay out of the way. The dependencies are not
added at first — the check ran against a scratch install — but both are
installed devDependencies now, `alchemy` behind a `patch-package` patch.

Typechecking it rather than eyeballing it was worth doing: it caught four
design errors that read fine on the page, all noted below — including one in
this sketch's own layering.

Where the sketch first reached for a hand-rolled version of something Effect
already has, it now uses the real thing: `httpApiStatus` annotations instead of
a `status` field, `Effect.fromResult` instead of a bespoke `Result` lift,
`Effect.fn` for traced operations, `RcMap` for reference-counted per-repo app
instances, and `PartitionedSemaphore` instead of a `Map` of locks. Two of those
were not cosmetic: the `RcMap` swap fixed a leak (every repository ever touched
stayed resident on the node host), and reordering the webhook combinators fixed
a retry that could never fire, because the catch sat inside it.

Pinned against what actually exists today:

| package                 | version          | notes                                              |
| ----------------------- | ---------------- | -------------------------------------------------- |
| `effect`                | `4.0.0-beta.107` | `Schema`, `HttpApi`, `Stream`, CLI all in core now |
| `alchemy`               | `2.0.0-beta.70`  | the `next` tag; v2 is "infrastructure as effects"  |
| `@effect/platform-node` | `4.0.0-beta.107` | node `FileSystem`/`Path`/`NodeRuntime` for the CLI |

Both are betas with breaking changes between releases. That is the single
biggest cost item below.

---

## Why bother

Four things in the current code are structural, not stylistic — they are the
argument for the rewrite. Everything else is taste.

**1. A push can take the isolate out.** `GitPackParser.parsePack` reads the
whole request body into one `Uint8Array` before it validates the header
(`git.pack.ts:547`, in the since-removed legacy implementation). A Durable Object gets 128 MiB. Today a push of a repo
with a few large blobs OOMs, and the failure mode is an isolate reset, not an
error the client can read. The upload-pack side has the mirror problem: the
object walk completes into an array before the first byte goes out.

**2. Errors are strings by the time they matter.** `GitError` carries a `code`
and handlers rediscover meaning with `instanceof` at ~40 sites; `worker.ts` and
`server.api.ts` each map errors to statuses with their own logic. Nothing tells
a caller of `repository.commit(...)` that a ref conflict is possible.

**3. Cancellation stops at the door.** `request.signal?.throwIfAborted()` runs
once in `worker.ts` and once in `Server.fetch`. Below that, an aborted clone
keeps walking objects, keeps reading R2, and keeps paying for it.

**4. Three storage implementations, one of them lying.** `GitStorage` is a
16-method filesystem interface with `applyRefChanges?` marked optional. Only the
Cloudflare implementation provides it, so the browser client hand-rolls
read-then-write ref updates (`client.ts#writeRefIfUnchanged`) and races itself
across tabs. Callers branch on whether a method exists.

Effect addresses 1–3 directly. The port split addresses 4. Alchemy addresses a
smaller, separate problem: three sources of truth (`wrangler.json`, the
generated `worker-configuration.d.ts`, and `worker.ts`'s hand-written binding
lookup) for one binding.

---

## What does not change

`git.object.ts`, `git.delta.ts`, `git.index.ts`, `git.merge.ts`, `git.utils.ts`
— 2,351 lines of pure, synchronous byte work with no I/O. Effect buys nothing
there and costs an allocation per call. They port over with two edits: functions
that `throw` return a `Result`, and anything that reached for storage moves up
into `Repository`. See [`src/git/Format.ts`](../src/git/Format.ts) — that
file is the seam, pure below and effectful above.

Being explicit about this is what keeps the rewrite from being a rewrite of
everything: a sixth of the non-test code moves across untouched, and it happens
to be the part with the subtlest bugs.

---

## Shape

```mermaid
flowchart TB
	subgraph ports["ports (src/git/Store.ts)"]
		OS[ObjectStore]
		RS[RefStore]
		IS[IndexStore]
	end

	subgraph domain["domain"]
		REPO[Repository]
		PACK[Pack streams]
		FMT[Format · pure]
	end

	subgraph edges["edges"]
		PROTO[HttpRouter · smart-HTTP]
		API[HttpApi · JSON + OpenAPI]
		CLI[effect/unstable/cli]
		CLIENT[HttpApiClient · derived]
	end

	subgraph layers["layers — swapped per environment"]
		CF[DO SQLite + R2]
		OPFS[OPFS]
		NODE[node FileSystem]
		MEM[in-memory · tests]
	end

	PROTO --> REPO
	API --> REPO
	CLI --> REPO
	CLIENT -.derived from.-> API
	REPO --> OS & RS & IS
	REPO --> FMT
	PACK --> OS
	CF & OPFS & NODE & MEM -.provide.-> ports
```

The point of the diagram is the dotted lines. `Repository` and both HTTP edges
name only the three ports and `RepoHost`, so the same program runs in the DO, on
node, in the tab, in the CLI and in a test. Today that same program is written
three times.

### Module map

| today | lines | becomes |
| ------------------------------------ | ----: | ------------------------------------------- | ----- | --------- | ----- | ----------------------------------- |
| `git.error.ts` | 50 | `git/Error.ts` — `Schema.TaggedError` union |
| `git.storage.ts` + 3 implementations | 1,192 | `git/Store.ts` + one layer per environment |
| `git.repository.ts` | 874 | `git/Repository.ts` service |
| `git.pack.ts`, `git.protocol.ts` | 1,153 | `git/Pack.ts` — `Stream`/`Channel` |
| `git.object                          | delta | index                                       | merge | utils.ts` | 2,351 | ported as-is behind `git/Format.ts` |
| `git.hooks.ts` | 163 | `Hooks` service |
| `server.ts` (DO + routing) | 1,130 | `server/App.ts` + `host/*` (~200) |
| `server.api.ts` | 2,515 | `server/Api.ts` — one `HttpApi` decl |
| `server.webhooks.ts` | 257 | `server/Webhooks.ts` — a `Schedule` |
| `server.lfs.ts`, `server.storage.ts` | 981 | folded into the R2 layer |
| `client.ts` | 1,007 | `Repository` + derived client (~250) |
| `cli.ts` | 1,991 | `cli/main.ts` — `Command` tree (~350) |
| `worker.ts` + `wrangler.json` | 32 | `alchemy.run.ts` + `host/Cloudflare.ts` |

13,819 non-test lines today. The plausible landing zone is 7–8k: the savings are
concentrated in `server.api.ts` (schema replaces hand validation and hand
dispatch), `cli.ts` (the CLI framework replaces the argv parser), and `client.ts`
(the derived client replaces re-declared payload types). The domain code barely
moves.

---

## Mechanics

### Errors

[`src/git/Error.ts`](../src/git/Error.ts). Six `Schema.TaggedError`
classes replace six `Error` subclasses. Being schema-backed is what matters:
the same class is the server's failure, the wire representation, and the type
the browser client matches on. `RefConflict` arrives at the client as
`{ _tag: "RefConflict", ref, expected, actual }`, not as a 409 whose body has a
`code` string the client re-parses.

The status mapping lives once, on the error, and is total by construction —
`worker.ts`'s catch block and `server.api.ts`'s per-handler `try`/`catch` both
disappear.

### Ports

[`src/git/Store.ts`](../src/git/Store.ts). Three narrow services addressed
by git concepts (`read(oid)`), not paths (`readFile(".git/refs/heads/main")`).
`RefStore.apply` is the only writer and is transactional, with `atomic` as a
parameter — so receive-pack's atomic mode stops being a separate code path, and
OPFS and node have to answer for atomicity instead of silently omitting it.

`ObjectStore` has both `read` and `readStream`. The Cloudflare layer
(`src/adapters/Cloudflare.ts`) maps
`readStream` onto R2's body stream and `writeStream` onto `bucket.put(stream)`,
which alchemy's binding accepts directly — a large blob never materializes.

### Streaming

**Landed** as [`src/git/Pack.ts`](../src/git/Pack.ts), tested against packs the
real `git` produces (both delta flavours) and validated back with
`git index-pack --strict`. Pack parse is a `Stream`
transformation that writes each object through to storage as it resolves, so
only the object being decoded is resident — delta bases are re-read from the
store by oid. Pack write is a lazy `Stream` handed to
`HttpServerResponse.stream`, so first-byte latency drops to the first object and
a client hang-up interrupts the walk.

pkt-line framing becomes a `Channel`, which means a truncated frame is a typed
`PackCorrupt` rather than a read that blocks forever.

Two constraints the typechecker made explicit here, both worth knowing before
phase 3 starts. A response body outlives the handler effect, so it cannot carry
requirements: `Repository.pack` closes over its own stores and hands back a
plain `Stream`, which is what lets it go straight to `HttpServerResponse`. And
R2's `put` only accepts an uncoloured stream, so a request body streamed into
storage must already be free of platform requirements. Both would have been
silent runtime surprises in a hand-written port.

### Concurrency and lifetime

- Ref updates: optimistic CAS with a retry `Schedule` on `RefConflict`
  ([`Repository.commit`](../src/git/Repository.ts)), rather than each caller
  reinventing read-then-write.
- Per-ref `update` hooks run concurrently; one rejection interrupts the siblings.
- `post-receive` / webhooks go through `RepoHost.background`
  ([`src/server/Webhooks.ts`](../src/server/Webhooks.ts)) — `waitUntil` on
  Workers, a detached fiber under node — so a slow subscriber no longer adds its
  latency to the push.
- Webhook backoff is `Schedule.exponential |> jittered`, capped at four
  attempts, with a per-attempt timeout inside the retry and the catch outside it
  — the same policy that today is a `for` loop, but now assertable under
  `TestClock`.
- Every fiber is tied to the request, so abort propagates all the way down.

### HTTP

Two edges, deliberately:

- **Smart-HTTP** — **landed** as [`src/server/Protocol.ts`](../src/server/Protocol.ts):
  protocol v0, stateless-rpc, web `Request` in and web `Response` out with
  `Repository` as the only requirement, so the same handlers serve from the
  Durable Object and from `node:http`. Stock `git` clones, pushes, deletes
  branches and fetches incrementally against both in the test suites. Stays
  byte-oriented — a schema buys nothing on a packfile.
- **JSON API** — **landed** as [`src/server/Api.ts`](../src/server/Api.ts): one
  `HttpApi` declaration for the current surface (create/read commit, log,
  refs), payloads decoded by schema at the boundary, errors on the wire as the
  tagged classes themselves with statuses from their `httpApiStatus`
  annotations. `Api.test.ts` drives it through a client derived from the same
  declaration — the no-drift property, demonstrated. The wider surface in
  [`src/server/Api.ts`](../src/server/Api.ts) lands endpoint by
  endpoint as the domain operations behind it do.

Both mount into one `HttpRouter.toWebHandler(...)` in
`src/server/App.ts`, which no host-specific code
touches.

One thing the group prefix `/api/:repo` forces: the `repo` path parameter has to
be declared in each endpoint's `params` schema. Leave it out and the server
still compiles — it is the _derived client_ that fails, because it cannot build
a URL for a segment nobody described. Worth knowing early, since it is the kind
of thing that gets discovered at the end of a mechanical port of 45 endpoints.

### Portability

There is no provider-neutral `Alchemy.Worker` or `Alchemy.DurableObject` to
reach for. In alchemy@next both are Cloudflare resources —
`Alchemy.Worker(...)` and `Alchemy.DurableObject(...)` come from
`alchemy/Cloudflare`, which is what [`src/alchemy.run.ts`](../src/alchemy.run.ts)
and [`src/host/Cloudflare.ts`](../src/host/Cloudflare.ts) import. What
alchemy _does_ offer across providers is the request shape: a Worker's `serve`
and `alchemy/Http`'s `NodeHttpServer` / `BunHttpServer` take the same
`HttpEffect`.

So portability is not a framework feature to switch on — it is one file,
`src/host/Host.ts`, naming the three things a git
server actually needs from a host:

| capability   | why it exists                                   | Cloudflare                  | node / bun             |
| ------------ | ----------------------------------------------- | --------------------------- | ---------------------- |
| `stores`     | one repository's objects and refs               | R2 + DO SQLite              | a directory            |
| `serialize`  | two pushes to a ref must not interleave the CAS | the DO input gate (nothing) | a `Semaphore` per repo |
| `background` | webhook delivery outliving the response         | `state.waitUntil`           | `Effect.forkDetach`    |

Above that line — `server/*`, `git/*` — nothing names a provider; `grep -l
alchemy` over the `*.ts` files hits only `host/`, `adapters/Cloudflare.ts` and the stack file.

The unit that moves between hosts is **one app instance bound to one
repository** (`App.forRepo`). That is not an
arbitrary choice: `Repository` is _constructed_ from `ObjectStore` and
`RefStore`, so storage resolves when the layer is built, not when a request
arrives — which is exactly the Durable Object model. Cloudflare gets an instance
per repo from the platform; [`host/Node.ts`](../src/host/Node.ts) reproduces
it with a `Map` keyed by repo name and a lock around dispatch.

The node host is worth having on its own merits, provider story aside: `npm run
dev` and a future e2e suite need not spawn `wrangler dev` on port 8080 (as the
removed `test.helpers.ts` did), self-hosting becomes a supported shape rather
than a fork, and the CLI can run the server in-process.

### Infrastructure

**Landed** as [`src/alchemy.run.ts`](../src/alchemy.run.ts). `Alchemy.R2.Bucket` and
`Alchemy.DurableObject` are values in the same program that uses them;
`Cloudflare.R2.ReadWriteBucket(Objects)` yields the binding, the env var and the
typed client from one call. That deletes `wrangler.json`, the generated
`worker-configuration.d.ts`, and the `postinstall` codegen step.

What it adds beyond parity: `--stage` previews (a full stack per PR, destroyed
on close) and a local runtime that runs the same program against emulated R2/DO
— the job the removed `test.helpers.ts` once did by spawning `wrangler dev` on
port 8080 for the e2e suite.

### Integration tests

`createTestHarness` from `wrangler` starts the Worker in `wrangler.test.json`
as a local server — real workerd, real Durable Objects, real R2 — and
[`src/git/Cloudflare.integration.ts`](../src/git/Cloudflare.integration.ts)
drives it from the outside over HTTP, under plain `node:test`. No second test
runner, and nothing mocked.

Because the harness is out-of-process, the test file cannot reach
`state.storage.sql`. So the storage contract runs on the _inside_: the DO
exposes a `/:repo/conformance` route (gated on a var only the test config sets)
that runs `Store.contract.ts` against its own R2 and SQLite and returns the
results as JSON. `Store.contract.ts` was already parameterised over the runner,
so the collector in [`Conformance.ts`](../src/git/Conformance.ts) is the third
runner it has been given — after `node:test` — with the suite itself unchanged.

Three things this reaches that unit tests cannot:

- the contract holds on DO SQLite + R2, so "atomic ref update" is verified
  where the input gate provides it rather than where a `Map` does;
- `worker.getDurableObjectStorage("GIT_REPO", …).exec(…)` reads the instance's
  SQLite directly from the test process, proving refs land in storage rather
  than in a field;
- `worker.evictDurableObject(…)` tears an instance down mid-test, which is how
  you catch state that was living in the instance. The layer graph is rebuilt
  on the next request and the refs are still there.

Integration files are named `*.integration.ts`, not `*.test.ts`, so node's
default discovery leaves them out of `npm test` — they boot a runtime and
belong behind their own script.

### Tests

**Landed.** The suite runs on `@effect/vitest` (`vitest.config.ts`, two
projects: `unit` and `integration`), with `HttpApiTest` driving the real
handler in-process and the environment swapped as a layer.
`--test-concurrency=1` is gone — it existed because the old suites shared
global state, and layers removed the reason, so the unit project runs in
parallel: ~21s serial became ~2.5s.

`it.live` is the variant to reach for, and the distinction is a trap worth
knowing: **`it.effect` installs a `TestClock` whose time never advances on
its own**, so any code under test that sleeps hangs until vitest's timeout
fires. `Api.test.ts` found this the hard way — `Repository.commit` retries a
`RefConflict` behind a 10ms schedule, and the suite's conflict assertion sat
there for the full 30 seconds. `it.live` keeps the real clock; reach for
`it.effect` only when the test itself drives time forward. Either way the win
is the same: the test body _is_ an Effect, so there is no `runPromise` at the
edge and a failure arrives as a `Cause` with its fiber trace. Most existing suites still call `Effect.runPromise` at
the edge, deliberately: their backends are stateful, so each test builds its
own layer rather than sharing one for the file, which is what `it.layer`
gives you. (`it.layer`'s nested `it` offers `effect` and `scoped` but not
`live`, which is the other reason `Api.test.ts` provides its layer per test.) The contract suites (`Store.contract.ts`, `Registry.contract.ts`)
stay framework-free on purpose — they are parameterised over a `Runner`, and
one of their runners is the collector that executes inside workerd, where no
test framework exists.

The 9,125 lines of legacy tests were removed with the old implementation;
their assertions — about git behaviour, not about plumbing — remain the
reference in git history for what each rewrite phase must cover.

---

## Migration

Each phase ships on its own and keeps `src/` working. With the legacy suite
removed, the ratchet is the contract suite, the git-binary interop tests and
the in-workerd conformance run.

| phase | scope                                                                                                         | risk   |
| ----- | ------------------------------------------------------------------------------------------------------------- | ------ |
| 0 ✅  | add `effect`, `Format.ts` seam, codecs ported with real tests                                                 | done   |
| 1 ✅  | `Error.ts` + `Store.ts` ports, in-memory backend, shared contract suite                                       | done   |
| 2 ✅  | `Repository` service, Cloudflare backend, `GitRepo` Durable Object, integration tests on `createTestHarness`  | done   |
| 2b ✅ | re-point the Worker at `GitRepo` and retire the old path — resolved by removing the legacy stack outright     | done   |
| 3 ✅  | `Pack.ts` streaming; this is where the OOM and the abort bugs get fixed                                       | done   |
| 4 ✅  | smart-HTTP protocol + `HttpApi` for the JSON API, client derived from the same declaration                    | done   |
| 5     | `RepoHost` seam + alchemy stack; delete `wrangler.json` + codegen; preview stages                             | medium |
| 5b ✅ | node host (`src/host/Node.ts`, dependency-free): the same handlers behind `node:http`, self-hosting supported | done   |
| 6 ✅  | CLI on `effect/unstable/cli` (`src/cli/main.ts`): init/refs/log/clone/serve/token                             | done   |

Every phase has landed. Phase 5 is the one with a caveat worth stating: the
stack builds and typechecks against real alchemy types, and a test asserts
the resource graph and that it agrees with `wrangler.json` — but deploying it
needs Cloudflare credentials, so `wrangler.json` remains the path the
integration suite drives. Two local patches hold it up (`patches/`): one
defers `RepoClient.raw` for the Artifacts provider, one aliases
`Schema.TaggedErrorClass` in `effect`. Both are upstream-shaped; delete them
when the versions catch up.

---

## Idiomatic Effect

Audited against `.claude/skills/effect` (Effect v4 production
defaults). What the audit changed:

- **Typed errors, not hand-rolled tags.** `Webhooks`'s delivery failure was a
  class with a literal `_tag` field; it is a `Schema.TaggedError` like every
  other failure in `git/Error.ts`, with an `httpApiStatus` annotation.
- **`Config`, not `process.env`.** The node host reads `GIT_ROOT`, `PORT` and
  `HOSTNAME` through `Config` recipes, so the provider is swappable in tests
  and a malformed `PORT` fails with a config error naming the variable rather
  than silently becoming `NaN`. Verified both ways against the real binary.
- **Effect `HttpClient` for outgoing calls.** Webhook delivery went from raw
  `fetch` plus a hand-written retry predicate to `HttpClient.filterStatusOk` +
  `HttpClient.retryTransient`, whose built-in classification (transport
  errors, timeouts, 408, 429, 5xx — and _not_ other 4xx) is exactly the
  policy the hand-rolled version was reaching for. The transport is now a
  layer a host or test swaps.
- **Sixteen unsafe type assertions removed.** `as Effect.Effect<never>` and
  friends were narrowing error and requirement channels to make things
  compile; deleting them made the compiler name the two that were actually
  load-bearing (alchemy's `RuntimeContext`, which cannot be constructed
  off-platform, and the router's request-scoped `Repository`). Those two keep
  a cast with a line-scoped `@effect-diagnostics-next-line` and a reason; the
  other fourteen were cargo cult.
- **Ten language-service suggestions cleared.** `Effect.mapError` for
  catch-then-fail, `yield*` on yieldable errors instead of `Effect.fail`,
  `catchTags` for consecutive `catchTag`s, `Schema.Finite` where a number is
  finite by definition, and `ObjectStore.list` as a `Stream` value rather
  than a thunk — a `Stream` is already lazy. One suggestion is suppressed by
  comment rather than followed: the nested `Effect<Effect<…>>` in
  `host/Cloudflare.ts` is alchemy's Durable Object contract, where the outer
  generator binds per instance and the inner runs per request.
- **`Effect.fn` spans on the expensive operations.** `Repository.unpack`,
  `Repository.packOf` and `Auth.guard` are traced; a push or fetch now shows
  its cost where it is spent.

Three deviations are deliberate:

- **`Schema.TaggedError`, not `TaggedErrorClass`.** The skill names the later
  spelling; the pinned `effect@4.0.0-beta.107` exports `TaggedError`. (The
  alias in `patches/effect+...` exists for a _dependency_ built against the
  newer name, not as a project convention.)
- **Raw `fetch` in `client/Fetch.ts`.** The skill's stated exception: this is
  the browser transport, bundled into Chromium by `Browser.test.ts`, and it
  keeps Effect boundary discipline inside an adapter.
- **The module-namespace style** (`export * as UserRepo from …`) is not
  adopted. The skill defers to an existing codebase style, and this one is
  consistent already.

**The storage ports are traced too**, and the careful version turned out to
be one decorator rather than fifty-two hand-edits: `tracedObjectStore` and
`tracedRefStore` in `git/Store.ts` wrap an implementation, and each backend
returns `ObjectStore.of(tracedObjectStore("Cloudflare", { … }))`. The span
names live in one place, so a trace reads with the same vocabulary whichever
storage is loaded — `Repository.commit → Cloudflare.RefStore.apply` — and a
new backend cannot forget to name itself. `Effect.fn` carries the call-site
frame as well as the span; `list` is a `Stream` and `head` a plain `Effect`,
so those take `Stream.withSpan` and `Effect.withSpan`.

The contract suite already runs every backend through the wrapper, and a
probe confirmed all thirteen operations report their own span rather than
inheriting the caller's.

---

## Paths not taken

The sketches are gone — every one either shipped or is recorded here. Three
designs were written, considered and _not_ built; the reasoning is worth more
than the files were.

**A provider-neutral `RepoHost` seam** (`host/Host.ts`,
`server/App.ts`). The plan was one `App` value that named only the
storage ports plus a `RepoHost` supplying `stores`, `serialize` and
`background`, with each host implementing that port. What shipped instead is
two concrete hosts — `host/Node.ts` and `host/Cloudflare.ts` — sharing
`Protocol.handle` and `Api.layer` directly. The seam turned out to be
unnecessary: the handlers already require nothing but `Repository`, so
host-neutrality came for free from the effect requirements, and an extra
service would only have restated it. The sketch's own header had noticed the
first half of this: alchemy has no provider-neutral `Worker`/`DurableObject`
either. If a third host ever needs the three capabilities as one value, the
port is a twenty-line file away.

**`@effect/vitest` with `TestClock` for the suite** (`testing/Repository.ts`).
The repo runs on `node:test` throughout, and the one thing `TestClock` was
wanted for — asserting webhook backoff without waiting out real seconds — is
handled by making the retry policy a parameter: `Webhooks.test.ts` passes a
1ms base delay and counts attempts against a real receiver, which tests the
schedule _and_ the HTTP behaviour in one pass.

**An `IndexStore` port** (`adapters/Local.ts`, `git/Store.ts`).
A staging area is a working-tree concept, and everything here serves bare
repositories: the server, the CLI and the browser client all commit trees
they have already built. A port with no caller is the dead code this section
exists to avoid, so it waits for the feature that needs it — a browser work
tree — rather than shipping ahead of it.

---

## Related

[`docs/artifacts-provider.md`](./artifacts-provider.md) evaluates whether this
server can implement alchemy's **Cloudflare Artifacts** binding — Artifacts is
"git for agents", so the protocol half is already done. The short version: yes,
and the architecture in this sketch maps onto the contract almost line for line,
but it needs a namespace registry, `fork`, and — the significant one — an auth
system, since nothing in `src/` reads an `Authorization` header today.

---

## Costs and open questions

- **Both dependencies are betas.** `effect@4.0.0-beta.107` and
  `alchemy@2.0.0-beta.70` both break between releases. Pinning exactly (the repo
  already sets `save-exact=true`) and budgeting for churn per upgrade is the
  honest plan; the alternative is waiting for stable and doing phase 3 by hand.
- **The edges were reaching past the domain — caught by the types.** The first
  version had `Api.ts` and `Protocol.ts` doing `yield* RefStore` directly.
  That compiles, but it makes storage a _per-request_ requirement of every
  route, so `toWebHandler` demanded a `Context<ObjectStore | RefStore>` on every
  call and no host could bind an app instance to a repository. Routing those
  reads through `Repository` (`refs`, `resolve`, `head`, `branch`, `pack`, and
  `receive` taking the pack stream) fixed the requirement and tightened the
  layering: the HTTP edges now know about one service, not three. Worth
  remembering as the JSON surface grows and the shortcut is tempting each time.

- **`RuntimeContext` colouring — resolved, with a cost.** Alchemy's Cloudflare
  bindings return effects requiring `RuntimeContext` (it holds the invocation's
  `env`/`ExecutionContext`). The first version of this sketch threaded it
  through the port signatures; the typechecker then dragged it into the CLI and
  the test suite, neither of which runs on Workers. The fix is that the
  Cloudflare layer captures the context with `Effect.context` and provides it
  inward (`git/Cloudflare.ts`), so the
  ports stay `R = never`. The cost is real and should be measured: that layer
  must be built inside the invocation rather than memoized per DO instance,
  because a cached context would pin a stale `ExecutionContext`.
- **Delta resolution against a stream — resolved.** `OFS_DELTA` bases are
  addressed by offset into the pack, and the fear was needing a bounded window
  plus a second pass. Neither was necessary: objects are written to the store
  as they resolve, so a delta base is re-read by oid from storage and only the
  object being decoded is resident.
- **Effect v4 has no `Effect.Service` class helper.** Services are
  `Context.Service<Self, Shape>()("key")` plus `Layer.effect`. Slightly more
  ceremony than v3 examples on the web suggest; the sketch uses the real v4 form
  throughout.
- **Bundle size in a Worker — still unmeasured.** Effect core plus
  `unstable/http` and `unstable/httpapi` is not small, and nothing has checked
  it against the 3 MiB (compressed) limit. The one open question that could
  still force a design change.
