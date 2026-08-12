# Feature completeness review: `artifacts` vs `main`

Reviewed: 2026-08-12, `artifacts` @ `b8632b9` against `main` @ `9a08166`.

> [!NOTE]
> **This is the review as it stood before the gap-closing work.** Everything
> below describes `artifacts` @ `b8632b9`. Every gap it names has since been
> closed, including the working tree, which an earlier revision of this
> document argued was out of scope — see "What has since closed" for the
> current position and [`docs/plan.md`](./plan.md) §0 for why that argument
> was withdrawn. The original text is kept because the reasoning is what
> justified the plan, including where it turned out to be wrong.

## What has since closed

Every phase-1 and phase-2 item in the plan, and most of the full-parity
track. Against the review below:

| gap named below                                        | now                                                                                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Git LFS entirely absent                                | batch API + basic transfer, streaming, content verified per platform (`server/Lfs*.ts`)                                                     |
| Merge engine entirely absent                           | `git/Diff.ts` + `git/Merge.ts`, byte-identical to `git diff --no-index` and `git merge-file --diff3`; `Repository.merge` does the tree walk |
| 36 of 44 JSON endpoints missing                        | commit/blob/tree/files/file/object/diff/merge/grep/tags/reflog/reset/fsck/gc/webhooks all present                                           |
| `POST /commit` could only make empty commits           | takes a tree or files, and `Repository.writeFiles` builds nested trees                                                                      |
| Webhooks unreachable                                   | wired in every host, persisted (DO SQLite / JSON file), with CRUD; a real `git push` delivers a signed body                                 |
| Nothing could originate a push                         | `client/Push.ts`, verified by a real `git clone` of what it pushed                                                                          |
| Protocol v2 gone                                       | `ls-refs` (prefix/peel/symrefs/unborn) and `fetch` (acknowledgments/shallow-info/packfile)                                                  |
| Shallow clone rejected                                 | `deepen`, `deepen-since`, `deepen-not`, plus `--unshallow`                                                                                  |
| `side-band-64k` dropped                                | multiplexed on both fetch and push                                                                                                          |
| `.git` suffix routing regression                       | one repository per name, in every router                                                                                                    |
| gc, fsck, annotated tags, archive, `.idx`, index codec | all present, the last two verified against git's own files                                                                                  |
| CLI: 6 commands                                        | 29                                                                                                                                          |
| `npm run test:integration` missing                     | the integration project runs under `npm test`                                                                                               |

The working tree, which this review recorded as dropped by design, is now
built: `git/Work.ts` (ports), `git/Checkout.ts` (`status`, `add`, `rm`, `mv`,
`restore`, `checkout`, `commit`), `git/Work.node.ts` (filesystem, index at
`.git/index`), and seven CLI commands. The interop tests settle it against
the `git` binary in both directions rather than against our own expectations.

Also landed since: cherry-pick and rebase (`git/Rebase.ts`) and a streamed
NDJSON bulk-commit endpoint (`server/CommitPack.ts`).

The `artifacts` branch is the Effect v4 + alchemy@next rewrite described in its own
`docs/rewrite.md`. This review compares the two branches feature-by-feature to answer one
question: **what does main have that artifacts does not (and vice versa)?**

## Verdict in one paragraph

`artifacts` is a **transport-and-storage rewrite, not a feature-parity port**. It rebuilds the
server core — objects, refs, packfile streaming, smart-HTTP, storage backends — with markedly
better engineering (bounded-memory streaming, contract-enforced atomic ref updates across four
backends, real-`git` interop tests, typed errors, plus net-new auth, a node self-hosting server,
and the Cloudflare Artifacts provider that gives the branch its name). But it carries roughly
**a fifth of main's application surface**: 36 of main's 44 JSON API endpoints, all of Git LFS,
the merge/rebase/cherry-pick engine, the index/staging layer, 20 of main's 26 CLI commands,
most of the browser client, protocol v2, and shallow clones are missing. Webhooks and
server-side hooks are implemented on artifacts but wired to no-ops in every host, so they never
fire. The rewrite doc's claim that "every phase has landed" is true of the _phases it defined_,
not of parity with main.

## Scale

|                               |                   main |                                 artifacts |
| ----------------------------- | ---------------------: | ----------------------------------------: |
| non-test + test LOC in `src/` |                ~22,900 |                                    ~9,600 |
| JSON API endpoints            |                     44 |                                         7 |
| smart-HTTP routes             |             4 (+3 LFS) |                                         3 |
| CLI commands                  |                     26 |                                         6 |
| browser client public methods |                    ~29 |                                        ~4 |
| storage backends              | 4 (1 with atomic refs) | 4 (all with atomic refs, contract-tested) |

## Missing on `artifacts` (present on `main`)

### Blocking gaps

1. **Git LFS — entirely absent.** Main implements the batch API
   (`POST /:repo.git/info/lfs/objects/batch`), streamed R2 upload/download, pointer
   parse/create, and a `git_lfs_objects` metadata table (`server.lfs.ts`, 272 lines + 410 test
   lines). On artifacts the only traces are comments; `docs/rewrite.md`'s "folded into the R2
   layer" is aspirational, not done.
2. **Merge engine — entirely absent.** Main's `git.merge.ts` (950 lines): diff3 three-way
   merge, four strategies, `findMergeBase`, octopus merge, conflict detection/markers/resolver,
   rename detection, `cherryPick`, `rebase`, plus `MERGE_HEAD` handling in the repository.
   Artifacts has no merge base, no ancestry test, no conflict type anywhere.
3. **JSON API: 36 of 44 endpoints missing**, including `/status`, `/tree`, `/read`, `/write`,
   `/diff`, `/branches/diff`, `/commits/diff`, `/object`, `/grep`, `/tag`, `/merge`, `/rebase`,
   `/reset`, `/checkout`, `/switch`, `/fetch`, `/pull`, `/push`, `/remote`, `/fsck`, `/gc`,
   `/reflog/:ref`, archive downloads (`.tar.gz`/`.zip`), the NDJSON `/commit-pack` streaming
   bulk-commit, repo create/delete, and webhook CRUD. Present: refs, paginated branches +
   branch-create, paginated commits, commit read, log (fixed limit 50), commit create.
4. **`POST /:repo/commit` cannot commit content**: the handler hardcodes
   `tree: EMPTY_TREE_OID` (`src/server/Api.ts:148`), so the one write endpoint only produces
   empty-tree commits. There is no HTTP path to write a blob or tree even though
   `Repository.writeBlob`/`writeTree` exist.
5. **Webhooks and hooks are dead code.** `server/Webhooks.ts` is a good delivery engine
   (HMAC `X-Signature-256`, jittered exponential retry, timeout, concurrency, background
   delivery) — in several ways better than main's — but every host provides
   `GitRepository.hooksNoop` (`host/Cloudflare.ts:62`, `host/Node.ts:67`, `git/Durable.ts:50`),
   there is no subscriber persistence (main has a `git_webhooks` table) and no management
   endpoints. No push on artifacts ever fires a webhook.
6. **CLI: 26 commands → 6.** Missing: add, rm, mv, restore, commit, status, show, branch
   (create/delete), checkout, switch, merge, rebase, reset, tag, bisect, diff, grep, backfill,
   history, fetch, pull, **push**, remote. Present (narrowed): init, clone, log; added: refs,
   serve, token.
7. **No push from client or CLI.** Main's browser client pushes (`client.ts:504`) and deletes
   remote refs. Artifacts' client can `lsRemote` and clone only — receive-pack exists solely
   server-side. Combined with the CLI gap, artifacts can serve pushes from stock `git` but
   cannot originate one.
8. **Index/staging + working tree — dropped by design.** Main has a real `DIRC` v2 index codec
   (readable by `git ls-files`), `add`/`checkoutCommit`/`createTreeFromIndex`. Artifacts is
   bare-repository-only, stated in `git/Store.ts`. Defensible for a server; it is what removes
   add/commit/status/checkout from CLI and client. (`docs/rewrite.md` still draws an
   `IndexStore` port that does not exist.)
   _Since resolved by building it, not by keeping the narrowing: the `IndexStore` port
   `docs/rewrite.md` drew now exists, and the parenthesis above is the tell — a design
   document describing a port that no code provided was a gap being read as a decision._

### Protocol narrowing

- **Protocol v2 — gone** (main: `ls-refs` with prefixes/symrefs, v2 `fetch`, `Git-Protocol`
  detection). Artifacts is v0 stateless-rpc only, by stated design.
- **Shallow clone — actively rejected**: `--depth=1` succeeds against main, fails on artifacts
  with `Invalid{field:"depth"}` (`server/Protocol.ts:161`). Main implements
  `shallow`/`deepen`/`deepen-since`/`deepen-not`.
- **Capabilities dropped**: `multi_ack_detailed`, `side-band-64k`, `thin-pack`, `ofs-delta`
  (advertised). Artifacts advertises only `report-status delete-refs atomic agent`. Costs pack
  size and progress reporting, not correctness.
- **`GET /:repo/HEAD` route** — gone.
- **`.git` suffix routing regression**: main strips `{.git}?` in every route; artifacts routers
  split on `/` verbatim (`worker.ts:55`, `host/Cloudflare.ts:82`, `git/Durable.ts:72`), so
  `git clone …/repo.git` addresses a _different_ Durable Object than `…/repo`.

### Maintenance & integrity

- **`gc`** (grace period, repack, prune) — no counterpart, and no endpoint.
- **`fsck`** (`validateObject`/`fsckAll`, per-type structural checks) — no counterpart.
  Artifacts' conformance suite tests the _storage contract_, not object integrity.
- **Pack storage at rest** — main writes packs + v2 `.idx` (fanout, CRC32, 64-bit offsets) and
  reads objects out of stored packs; artifacts explodes every pack to loose objects on ingest
  and keeps nothing packed. Delta _creation_ is also gone (though main's pack writer never
  actually used it either — both branches send full objects on the wire).
- **Annotated tags** — main validates/parses tag objects; artifacts only reads the target oid
  for reachability. No tag create/list anywhere (API or CLI).
- **499-on-abort mapping** in the worker — gone.

### Tooling

- **CLI-vs-system-`git` e2e parity suite** (main's `e2e.test.ts`, 617 lines, 26 command
  suites) — gone, consistent with the CLI shrink.
- **Stale `test:integration` references.** The integration project has merged into plain
  `npm test` (`vitest run` executes both projects), but `vitest.config.ts:11`, the readme,
  and `docs/rewrite.md` still describe an opt-in `npm run test:integration` script that no
  longer exists. Doc cleanup, not a functional gap.
- `bin` renamed `.git` → `chr33s-git` (an improvement, but breaks the documented
  `npx @chr33s/git <command>`).

## Added on `artifacts` (absent on `main`)

1. **Auth — the largest net-new server feature.** Main has _zero_ authentication; every
   endpoint including receive-pack is open. Artifacts adds scoped (`read`/`write`) stateless
   HMAC tokens with TTL and repo-bound signatures (`git1.<scope>.<expiry>.<hmac>`), correct
   401/403 + `WWW-Authenticate` semantics, Basic/Bearer extraction that matches how `git`
   sends tokens, enforcement at the worker edge, the DO, and (optionally) the node host, a
   deploy-fails-closed `GIT_AUTH_SECRET` secret, CLI `token`/`clone --token`, and real-`git`
   interop tests.
2. **Cloudflare Artifacts provider** (`src/artifacts/`, ~1,100 lines) — the branch's headline:
   a self-hostable implementation of alchemy's Artifacts binding backed by the repo's own git
   stores. Namespace registry with cursor listing, revocable digest-at-rest tokens, **fork via
   git alternates** (zero object copies), smart-HTTP `import`, on memory / JSON-file /
   `node:sqlite` / DO SQLite backends under one contract suite that also runs inside workerd.
3. **Node self-hosting** (`host/Node.ts`): the same handlers behind `node:http`, per-repo
   mutex, streaming bodies, `Config`-driven; exposed as `chr33s-git serve`. Main has no node
   server at all.
4. **Streaming correctness**: pack parse and pack write are bounded-memory streams (fixes
   main's read-whole-body-then-validate OOM path on a 128 MiB DO); gzip request bodies;
   pull-based zlib inflate and streaming SHA-1 written for the purpose.
5. **Storage rigor**: atomic `RefStore.apply` mandatory on every backend (main: optional,
   Cloudflare-only, racy fallback), reflogs on every backend, one 306-line contract suite run
   against all four including real R2+DO SQLite inside workerd, span tracing via one decorator.
6. **Better interop evidence**: stock `git` clones/pushes/deletes branches against both hosts;
   `git fsck --strict`/`index-pack --strict` validate outputs; a real-Chromium OPFS clone test.
7. Smaller additions: derived typed API client (no drift from the server declaration),
   `Repository.log`/`branch`/`receive` as first-class service ops with CAS retry on
   `RefConflict`, push connectivity check (`ng … missing necessary objects`), typed exit codes,
   alchemy-only deploy with `--stage` previews.

## Documentation drift worth fixing on `artifacts`

- `readme.md:11` ("every phase of it has landed") contradicts `readme.md:89` ("the wider JSON
  surface, LFS and webhooks arrive with the remaining rewrite phases") — the latter is the
  accurate one.
- `docs/rewrite.md`'s module map says `git.object|delta|index|merge|utils.ts` were "ported
  as-is behind `git/Format.ts`": false for index and merge (not ported), half-true for delta
  (apply only). The architecture diagram still shows an `IndexStore` port that `Store.ts` does
  not export.
- `server.webhooks` → "`server/Webhooks.ts` — a `Schedule`" reads as landed; the module exists
  but is unreachable (no wiring, no persistence, no CRUD).

## If the goal is to replace `main` with `artifacts`

Ordered by value-for-effort:

1. **Wire webhooks/hooks** — the engine is written and tested; only `hooksNoop` and missing
   subscriber persistence + CRUD endpoints stand in the way.
2. **Fix `.git` suffix stripping** in all three routers (small, user-facing correctness).
3. **Un-hardcode `EMPTY_TREE_OID`** in `POST /commit` and expose `writeBlob`/`writeTree`/
   `readTree` over HTTP — that unlocks a useful write API with code that already exists.
4. **Port LFS** onto the R2 layer (the design doc already claims this is the plan).
5. **Shallow clone + side-band-64k** — the two protocol drops most likely to bite real users
   (CI does `--depth=1` by default in many systems).
6. Decide explicitly whether merge/rebase/index/working-tree and the wide JSON + CLI surface
   are _goals_ or _non-goals_ of the rewrite, and update the readme/rewrite doc to match —
   today the branch note says "rewrite" while four-fifths of the application surface has no
   landing plan.
