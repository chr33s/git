# Plan: replace `main` with `artifacts`

Goal: make the Effect rewrite on `artifacts` the default branch, preserve the legacy
implementation's history under a new name, and close the gaps that would make the swap a
regression for anyone using the repo today.

Grounded in [`docs/artifacts-feature-completeness.md`](./artifacts-feature-completeness.md)
(review of `artifacts` @ `b8632b9` vs `main` @ `9a08166`).

## Status

**Phases 1, 2, 3, 5 and 6 are done; phase 4 — the branch rename itself — is the one
step left, and it is a repository-settings operation rather than a code change.**

| phase           | state                                                                                                   |
| --------------- | ------------------------------------------------------------------------------------------------------- |
| 1 · must-fix    | ✅ `.git` routing, webhooks wired + persisted + CRUD, commit takes real content, shallow, side-band-64k |
| 2 · should-fix  | ✅ LFS, annotated tags, fsck, 499-on-abort                                                              |
| 3 · docs        | ✅ readme/rewrite-doc contradictions and stale script references fixed                                  |
| 4 · the swap    | ⏸ **needs a human**: renaming the default branch retargets every clone, open PR and CI trigger          |
| 5 · acceptance  | ✅ `npm run check` and `npm test` green, criteria below all met                                         |
| 6 · full parity | ✅ including the working tree, which §0 no longer excludes                                              |

What landed beyond the phase list: protocol v2, gc, the `.idx` codec, the `DIRC` index
codec, diff, three-way merge, archive (tar/tar.gz/zip), client push, cherry-pick and
rebase, bisect, path history, incremental fetch with `have` negotiation, streamed bulk
commits, the working tree, and a CLI that went from 6 commands to 29.

## 0. Scope decision (revised)

This section previously drew the scope as a bare-repository git _server_ and named the
working tree a non-goal. **That narrowing has been withdrawn and the working tree is
built.** The argument for excluding it was that everything here serves bare
repositories — but `src/git/Index.ts` had already been written, git's own `DIRC` v2
codec byte-verified both directions, with no port and no caller. A format codec with
nothing on either side of it is not a deliberate non-goal; it is half of a feature.

What exists now:

- **The working tree** — `src/git/Work.ts` (the `WorkTree` and `IndexStore` ports),
  `src/git/Checkout.ts` (`status`, `add`, `remove`, `move`, `restore`, `checkout`,
  `commit`), `src/git/Work.node.ts` (the filesystem backend, with the index at
  `.git/index`). Two ports rather than one because a server has neither, a CLI has
  both, and a browser could have the index without the files.
- **Parity against `git` itself, not against expectations.** The interop tests stage
  with ours and let `git status` describe the result, stage with git and let ours
  describe it, and commit from our index into a history `git log` and
  `git fsck --strict` accept. `chr33s-git status` emits git's porcelain format, so the
  two are directly comparable rather than merely similar.
- **Everything else in the "full parity" column**: merge, diff, tags, gc, fsck, LFS,
  archive, packs at rest (`gc --repack` writes a `.pack`/`.idx` pair git verifies),
  protocol v2, client push, cherry-pick and rebase.

One boundary is still drawn deliberately, and this one is about shape rather than
effort: the working-tree verbs are not exposed over HTTP. A bare server has no files
on disk, so serving `add` or `checkout` would mean inventing a work tree behind the
API. The server-side spelling of "commit these files" is the `commit-pack` endpoint,
which streams an NDJSON body straight into the object store.

## 1. Must-fix before the swap (regressions within the rewrite's own scope)

Ordered by value-for-effort; each lands as its own commit on `artifacts`.

1. **`.git` suffix routing.** Strip an optional trailing `.git` from the repo segment in all
   three routers (`src/worker.ts:55`, `src/host/Cloudflare.ts:82`, `src/git/Durable.ts:72`).
   Today `git clone …/repo.git` silently addresses a different Durable Object than `…/repo`.
2. **Wire webhooks.** `server/Webhooks.ts` is written and tested but every host provides
   `GitRepository.hooksNoop`. Provide `Webhooks`-backed `Hooks` layers in
   `host/Cloudflare.ts`, `host/Node.ts`, and `git/Durable.ts`; add subscriber persistence
   (DO SQLite table beside refs/reflog; JSON file on node) and minimal CRUD endpoints
   (`POST/GET/DELETE /:repo/webhooks[/:id]`) in `server/Api.ts`.
3. **Un-hardcode the commit endpoint.** `POST /:repo/commit` pins `tree: EMPTY_TREE_OID`
   (`server/Api.ts:148`). Accept a tree oid and/or inline entries, and expose the existing
   `Repository.writeBlob` / `writeTree` / `readTree` over HTTP so the JSON API can create
   real content.
4. **Shallow clone (`--depth`).** CI systems default to `--depth=1`; today the server rejects
   it with `Invalid{field:"depth"}` (`server/Protocol.ts:161`). Implement `shallow`/`deepen`
   at minimum; `deepen-since`/`deepen-not` can follow. Legacy reference:
   `server.ts:524-576`, `git.repository.ts:739-767` on the legacy branch.
5. **`side-band-64k`.** Progress/error channel for fetch; cheap to add to the v0 protocol and
   visible to every user.

## 2. Should-fix before the swap (parity users will notice)

6. **Git LFS.** Port the batch API + R2 upload/download onto the Cloudflare layer —
   `docs/rewrite.md:196` already declares this the plan ("folded into the R2 layer").
   Legacy reference: `server.lfs.ts`, `server.storage.ts:613-657`.
7. **Annotated tags.** Tag object parse/encode in `git/Format.ts`, a `tag` endpoint and CLI
   verb. Small, and `refs/tags` handling already exists.
8. **fsck endpoint.** Object-integrity validation (hash + per-type structure) behind
   `POST /:repo/fsck`; the conformance suite covers the storage contract, not object health.
9. **499-on-abort** mapping in the worker (legacy `worker.ts:16-19`).

Deferred, explicitly (post-swap roadmap, not blockers): `multi_ack_detailed` and delta
compression in the pack writer. Fetch negotiation is baseline single-ACK on both sides —
the client offers `have` lines in rounds and the server acknowledges the first commit it
holds, which is what makes an incremental fetch transfer only what is missing; the
multi-ack variants would narrow the common base in fewer round trips, not change what
arrives. Everything else once listed here — gc, packs and `.idx` at rest, protocol v2,
client/CLI push, the wider JSON surface, and the working tree §0 used to rule out — has
since landed.

## 3. Documentation renames (same PR as the swap)

- `readme.md` on `artifacts`: remove the self-contradiction — line 11 claims "every phase of
  it has landed" while line 89 defers LFS/webhooks/wider API. After phases 1–2, restate what
  is in and what is deliberately out, and delete the `main`-tip note that points at the
  `artifacts` branch URL (it becomes self-referential after the rename).
- `docs/rewrite.md`: fix the module map row claiming `git.object|delta|index|merge|utils.ts`
  were "ported as-is" (index and merge were not ported; delta is apply-only), and drop the
  `IndexStore` port from the architecture diagram, or mark it future.
- Update any links of the form `github.com/chr33s/git/tree/artifacts` → default-branch links.
- Clean up stale `npm run test:integration` references (`vitest.config.ts:11` comment,
  readme, `docs/rewrite.md`) — the integration project has merged into plain `npm test`.
- Keep the npm `bin` name `chr33s-git` (already renamed from the legacy `.git`); call out the
  breaking change in the release notes.

## 4. The branch swap itself

Preferred: GitHub's branch **rename** (Settings → Branches, or API) rather than force-push —
renames retarget open PRs and branch-protection rules and leave a redirect notice for clones.

Order matters; do it in a quiet window:

0. **Check that the branch you are promoting carries the work.** A rename cannot lose
   history, but it can promote the wrong history — and as of this writing `artifacts` is
   still at `b8632b9`, the commit it had before any of the gap-closing work. Renaming it
   today would make the default a repository with no LFS, no merge and no protocol v2, with
   nothing in the git history to suggest anything had gone wrong. Land the work first, then
   confirm the modules are actually there:

   ```sh
   git fetch origin artifacts
   git ls-tree --name-only origin/artifacts src/server/Lfs.ts src/git/Merge.ts \
     src/server/Route.ts src/client/Push.ts
   ```

   Four paths back means four phases present; anything missing means the branch is not the
   one to promote.

1. Freeze: merge or close PRs targeting `main`; announce the cutover.
2. Preserve legacy history under a new name:
   - Rename `main` → `legacy` (GitHub UI/API). Existing clones keep working; GitHub shows the
     rename notice. Optionally also tag the tip: `git tag legacy/final 9a08166 && git push
origin legacy/final`.
3. Promote the rewrite:
   - Rename `artifacts` → `main` (GitHub UI/API). This makes it the default branch
     automatically if done as a rename of the default; otherwise set default branch to the
     renamed `main` in Settings.
4. Local cleanup for contributors (put this in the announcement):

   ```sh
   git fetch origin --prune
   git branch -m main legacy         # if they had legacy main checked out
   git checkout -b main origin/main
   ```

5. Re-point branch protection, required checks, and any CI triggers/environments that name
   `main` or `artifacts` explicitly.
6. Post-swap: archive stale `claude/*` and feature branches cut from the legacy `main`; new
   work rebases onto the new `main`.

Fallback (no GitHub rename available): `git push origin main:legacy`, flip the default branch
to `artifacts` in Settings, then `git push origin artifacts:main` and delete `artifacts`.
Force-pushing over `main` without flipping the default first will orphan open PRs — avoid.

## 5. Acceptance for the swap PR

- `npm run check` and `npm test` green — `npm test` runs both vitest projects (unit +
  integration, including the workerd harness).
- Stock `git`: clone, push, branch delete, incremental fetch, **`clone --depth=1`**, and
  clone of `…/repo.git` (suffix) all pass against both the node host and the workerd
  integration harness.
- A push with a registered webhook delivers (signed, retried) on both hosts.
- `POST /:repo/commit` round-trips real content: write blob → write tree → commit → read back
  over the derived client.
- Readme/rewrite doc updated per §3; feature-completeness review updated to reflect closed
  gaps.

## 6. Full-parity track (only if main's broader scope is a goal)

Everything below is _out of scope_ for the swap under §0's assumed scope decision. It is the
inventory of what porting main's broader surface actually takes, in dependency order — five
layers, gated by one architectural decision. All-in it is roughly **8–10k lines to port** of
main's ~13.8k non-test surface (the rest already has artifacts counterparts), against the
branch's current ~9.6k total: full parity approximately doubles the branch.

### 6.1 The gating piece: a working-tree/index seam (~600 lines + port design)

The artifacts stores are deliberately git-concept-shaped (`ObjectStore`/`RefStore`, oid- and
ref-keyed) with no path-level file API — which is exactly why `add`, `status`, `checkout`
and `.git/config` have no home. Required first:

- **The `IndexStore` port** `docs/rewrite.md` already sketches (the diagram draws it;
  `git/Store.ts` does not export it) — or a broader `WorkTree` port with path-level
  read/write/list.
- **The index codec** (legacy `git.index.ts`, 294 lines): real `DIRC` v2 read/write — main's
  is genuine enough that `git ls-files` reads it. Pure byte work; ports behind the
  `Format.ts` seam.
- **Repository ops that hang off it**: `add`, `checkoutCommit`, `createTreeFromIndex`,
  `findInTree`, work-tree `readFile`/`writeFile`/`deleteFile`, and a `.git/config`
  parser/serializer (legacy `client.ts:964-1010`) for remotes.

Every path-touching item in 6.3–6.5 depends on this landing first.

### 6.2 Git core engines (pure; port mostly as-is)

- **Merge engine** (legacy `git.merge.ts`, 950 lines) — the single largest gap: diff3 with
  LCS, `threeWayMerge` with four strategies, `mergeTrees`/`mergeCommits`, `findMergeBase`,
  octopus merge, conflict detection/markers/`ConflictResolver`, rename detection,
  `cherryPick`, `rebase`, `MERGE_HEAD` handling. Almost all pure byte/graph work — it ports
  behind the same pure/effectful seam as `Format.ts`; `Repository.closure` is a starting
  point for merge-base.
- **fsck** (`git.object.ts:212-260`): hash verification + per-type structural checks.
- **gc** (`git.repository.ts:579-672`): reachability, grace period, loose pruning, repack.
- **Pack `.idx` + packs at rest** (~250 lines): `buildPackIndex` (v2 fanout, CRC32, 64-bit
  offsets), `parsePackIndex`, binary-search lookup, and an object-read fallback into stored
  packs. Without this, gc's repack half has no target.
- **Annotated tag codec**: parse/encode/validate in `git/Format.ts` (today only the target
  oid is read).
- **Delta _creation_** (legacy `git.delta.ts`, ~230 lines) — optional: main never wired it
  into its pack writer either, so skipping it loses no shipped behavior.

### 6.3 Protocol width

- **Shallow**: `shallow`/`deepen`/`deepen-since`/`deepen-not` parsing, boundary computation,
  `shallow`/`unshallow` lines, plus shallow-commit state (legacy `server.ts:524-576`,
  `git.repository.ts:739-767`). (§1 already schedules the minimum `--depth` case.)
- **`side-band-64k`**, **`multi_ack_detailed`**, **thin-pack**, advertised **`ofs-delta`**.
- **Protocol v2**: `Git-Protocol` detection, v2 advertisement, `ls-refs`
  (prefixes/symrefs), v2 `fetch` (legacy `server.ts:765-1029`).
- Small: `GET /:repo/HEAD`, 499-on-abort (also §2.9).

Parallelizable with everything else — nothing here depends on 6.1.

### 6.4 Server surface (36 missing endpoints + LFS, grouped by dependency)

- **Needs nothing new** (core ops exist; just declare endpoints): `/tree`, `/object`,
  `/read`, `/write`, `/files`, `/file`, `/reflog/:ref`, repo create/delete. Cheap wins.
- **Needs 6.2**: `/tag`, `/fsck`, `/gc`, `/merge`, `/rebase`, `/reset`, `/restore-commit`.
- **Needs 6.1**: `/status`, `/add`, `/rm`, `/mv`, `/restore`, `/checkout`, `/switch`.
- **Needs a diff engine**: `/diff`, `/branches/diff`, `/commits/diff` — main reuses the
  merge module's LCS, so this falls out of 6.2.
- **Needs client-side transport on the server** (acting as a git client against another
  remote): `/fetch`, `/pull`, `/push`, `/remote`.
- **Standalone ports**, parallelizable any time: **LFS** (batch API, R2 streaming
  upload/download, pointer codec, metadata — legacy `server.lfs.ts`, 272 lines; also §2.6),
  **archive** (`.tar.gz`/`.zip` writers, ~160 lines), **`/commit-pack`** NDJSON streaming
  bulk-commit, **`/grep`**, **webhook CRUD + persistence** (already §1.2).

### 6.5 Client and CLI porcelain (the long tail)

- **Push first**: `Client.push`/`pushDelete` (send-pack over HTTP with CAS on
  remote-tracking refs, legacy `client.ts:504-648`) — today nothing on artifacts can
  originate a push. Then **incremental fetch** with real haves negotiation (the current
  `fetchRepository` does one `want…done` round), remote-tracking refs, and remotes config.
- The remaining ~20 client methods and **20 CLI commands** (add, rm, mv, restore, commit,
  status, show, branch-write, checkout, switch, merge, rebase, reset, tag, bisect, diff,
  grep, backfill, history, fetch, pull, push, remote) with their ~200 flags — mechanical
  once 6.1–6.4 exist, but the bulk of the line count (legacy `cli.ts` 1,991 +
  `client.ts` 1,007 lines).
- **The e2e parity suite** (legacy `e2e.test.ts`, 617 lines, 26 command suites against the
  system `git` binary) — the ratchet that made main's porcelain trustworthy; porting the
  commands without it would be parity in name only.

### Sequencing

**6.1 → 6.2 → 6.4/6.5 porcelain**, with 6.3 and the standalone server ports (LFS, archive,
grep, commit-pack) parallelizable throughout. The working-tree port is the first domino: if
6.1 is rejected, most of 6.4–6.5 is unreachable and the §0 narrowed scope is the honest
statement of what this codebase is.
