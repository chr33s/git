# Plan: replace `main` with `artifacts`

Goal: make the Effect rewrite on `artifacts` the default branch, preserve the legacy
implementation's history under a new name, and close the gaps that would make the swap a
regression for anyone using the repo today.

Grounded in [`docs/artifacts-feature-completeness.md`](./artifacts-feature-completeness.md)
(review of `artifacts` @ `b8632b9` vs `main` @ `9a08166`).

## 0. Scope decision (do this first)

The rewrite is a deliberate narrowing: a bare-repository git *server* (plus client/CLI enough
to drive it), not a working-tree git reimplementation. This plan assumes that scope is
accepted:

- **Non-goals after the swap** (removed with the legacy code, documented as such): index /
  staging, working-tree files, merge/rebase/cherry-pick, and the wide porcelain CLI
  (add/commit/status/checkout/…) and its e2e-vs-`git` parity suite. The legacy branch remains
  the reference if any of these come back.
- **Goals before the swap**: everything in phases 1–2 below — the items that are regressions
  even for the narrowed scope.

If instead full parity is required, this plan's phase 1–2 still apply first, but the swap
moves out until merge/index/CLI land; that is a much longer project (~13k lines of legacy
surface) and not scheduled here.

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

Deferred, explicitly (post-swap roadmap, not blockers): gc/repack, pack + `.idx` storage at
rest, protocol v2, thin-pack/`multi_ack_detailed`, client/CLI push, the wider JSON surface.

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
