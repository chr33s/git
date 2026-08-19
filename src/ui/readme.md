# git+ UI

The web interface for `@chr33s/git`, implementing the Claude Design prototypes
`git-plus.dc.html` (dark) and `git-plus light.dc.html` (light).

## What it is

Five screens, in one shell with a collapsible rail:

| Screen       | Contents                                                                      |
| ------------ | ----------------------------------------------------------------------------- |
| **Activity** | Repository history as a calendar timeline, zoomable and pageable              |
| **Code**     | Explorer, file view, editor, history and branch management                    |
| **Tasks**    | Tasks and Change Requests as one hierarchy                                    |
| **Detail**   | A Task, or a Change Request with refs, diff, commits, checks and merge state  |
| **Search**   | One query over Task titles and — through `/grep` — file contents              |
| **Settings** | Identity, branches, tags, remotes, webhooks, maintenance, policy, danger zone |

Both palettes ship. The rail's toggle pins one and remembers it; with no stored
choice the page follows `prefers-color-scheme`.

Screens are addressable — `#/tasks`, `#/detail/CR-14` — so a link survives a
refresh.

## Stack

- **[Lit](https://lit.dev)** for the components, rendering into the **light
  DOM** rather than a shadow root (`base.ts` explains why).
- **[`@chr33s/base-wc`](https://github.com/chr33s/base-wc)** for behaviour:
  `ui-tabs`, `ui-switch`, `ui-toggle-group`, `ui-search-field`. Its contract is
  light DOM and native-first form controls, which is what the light-DOM choice
  above is in service of — the settings toggles wrap real checkboxes and submit
  without JS.
- **[`@pierre/trees`](https://github.com/pierrecomputer/pierre)** for the
  explorer. It is path-first, so the `/files` response feeds it unchanged.
- **[`@pierre/diffs`](https://github.com/pierrecomputer/pierre)** for the file
  view and Change Request diffs, with Shiki highlighting.
- **esbuild** for the bundle — already a dependency of this repository.

## Data

**Code reads and writes the server.** The client in `api.ts` calls the
endpoints declared in `src/server/Api.ts`:

| Screen   | Endpoint                                                                                        | Use                                        |
| -------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------ |
| Rail     | `GET /:repo/whoami`                                                                             | Identity, resolved once and handed down    |
| Code     | `GET /:repo/refs`                                                                               | Branch list and the tip oid                |
| Code     | `GET /:repo/files`                                                                              | Tree paths for the explorer                |
| Code     | `GET /:repo/file`                                                                               | Blob content for the view, editor, history |
| Code     | `GET /:repo/object/:oid`                                                                        | Author and date, from the raw commit       |
| Code     | `GET /:repo/commits/:oid`                                                                       | The commit bar's history panel             |
| Code     | `GET /:repo/history/:oid`                                                                       | Commits touching the open file             |
| Code     | `POST /:repo/commit`                                                                            | Committing an edit, a new file, a delete   |
| Code     | `POST /:repo/branches/create`                                                                   | New branch, at the current tip             |
| Search   | `POST /:repo/grep`                                                                              | File contents matching the query           |
| Activity | `GET /:repo/commits/:oid`                                                                       | The timeline, one card per commit          |
| Detail   | `POST /:repo/diff`                                                                              | Which files a Change Request touches       |
| Detail   | `POST /:repo/merge`                                                                             | Merging a _fixture_ Change Request         |
| Detail   | `POST /:repo/hub/pulls/:id/merge`                                                               | Settling a hub Change Request atomically   |
| Settings | `GET /:repo/branches`, `DELETE /:repo/branches/:name`, `POST /:repo/reset`                      | Branch administration                      |
| Settings | `GET/POST /:repo/tags`, `DELETE /:repo/tags/:name`                                              | Tags                                       |
| Settings | `GET/POST /:repo/remotes`, `DELETE /:repo/remotes/:name`, `POST /:repo/fetch`, `/push`, `/pull` | Remotes and sync                           |
| Settings | `GET/POST /:repo/webhooks`, `DELETE /:repo/webhooks/:id`                                        | Webhooks                                   |
| Settings | `POST /:repo/fsck`, `POST /:repo/gc`, `GET /:repo/reflog`                                       | Maintenance                                |

What the API does **not** answer, the UI does not pretend to: the merge-policy
toggles say they are local to the browser, and the danger zone's buttons are
disabled with a title saying why. Offline, every write affordance disables
rather than failing on click.

Editing is the pencil on the file card and the explorer's "+": both open a
textarea over the blob, and committing sends the file with `expected` pinned
to the tip the editor opened at — a commit that lands mid-edit answers
`RefConflict` and is shown as one, never silently overwritten. Offline, the
editor disables: the sample repository is read-only because there is nothing
to write to.

**Tasks and Change Requests read the hub.** The git-native hub
(`src/hub/PullRequest.ts`, `src/hub/Projection.ts`, `src/hub/Task.ts`) models
pull requests, reviews, checks and tasks as signed events in `refs/hub/*`, and
`Api.ts` now projects them over HTTP: `GET /hub/tasks`, `GET /hub/pulls`,
`GET /hub/pulls/:id`, and one write — `POST /hub/events`, which appends a
_pre-signed_ event (the server holds nobody's key, so authorship stays where
the key lives). `hub.ts` queries those endpoints through the derived atom
client and folds the answers into `store.ts`; while the hub holds anything,
the screens show the repository's own state. A repository whose hub is empty,
absent or unreachable keeps the design's fixtures (`fixtures.ts`) — the
documented sample, never passed off as live.

**The whole review loop runs in the page.** A branch edited and committed in
OPFS is proposed from the Code screen's **Propose** dialog: the branch is
pushed, a `pr.opened` event is signed and appended, and the new Change
Request opens in Detail — where **Approve** / **Request changes** submit
reviews of the exact revision and threads resolve and take replies. Whether
**Merge** is offered is the _server's_ judgment (`mergeable` on the pull
answers, computed under the published rules), and the merge itself is one
transition: `POST /hub/pulls/:id/merge` fast-forwards the base to the exact
approved head and appends the browser's signed `pr.merged` beside it, judged
together — a refused or offline merge leaves the Change Request open with
the reason, and "Merged" appears only after the projection is re-read. Hub
tasks carry their lease: claim, release, complete or abandon from the detail
screen (task _comments_ are disabled with the reason: no task-comment event
exists in the protocol yet, and task ids must never reach the pull-request
namespace). The Activity screen lists the hub's **sessions**, and the
commits panel picks up cherry-pick, bisect marks, and a rebase entry in the
branch menu (all local in local mode, over the JSON API otherwise).
Settings shows grant expiry, trust freshness and the usage budget beside the
identity, badges remotes with their stored key and standing sync
instruction, and the **Branch policy** card reads `GET /policy` and
publishes edits back through `policy.write`'s own door.

**The browser holds a signing key.** `identity.ts` generates an Ed25519 key
on first use (WebCrypto, through the same `SshSignature` module every other
author uses) and keeps it in OPFS as **one versioned record** — seed and
public line together, the public point re-derived from the seed on every
load and repaired _from the seed_ with a visible note if the two ever
disagree, so the browser can never sign with one key while advertising
another. It signs hub events: creating a Task opens a real `task.opened`
event over `POST /hub/events`, commenting on a hub Change Request appends
`comment.created`, and both are read back from the server's projection —
never shown optimistically. When the server answers a 401, its challenge
carries the nonce _and the RepoID_ — which is what lets a key bootstrap on a
**private** repository, where the unauthenticated `/whoami` that used to
supply the identity is itself refused — and the request retries once under a
signed `auth.request` envelope, the same native scheme the CLI presents.
Every request answers the challenge the same way: the JSON verbs in
`api.ts`, the derived client's hub reads and writes, and smart HTTP itself —
clone, fetch and push hand their challenges to the browser key
(`src/client/Authorize.ts`), a push's envelope binding the exact ref
commands it was signed for. A fresh key is nobody: the Settings identity
card shows its public half so an operator can `hub grant` it, and until a
repository accepts the key (or is served `--open`), mutations report the
refusal. An authentication refusal is never dressed up as the offline
sample: a private repository that turns the key away empties the screens and
says what to grant.

When the API cannot be reached, Code and Diff fall back to the design's sample
repository and **say so** in an inline note, rather than passing fixtures off as
live data. That keeps the UI reviewable without a running Worker.

**Code goes local when the browser allows it.** After first paint the shell
opens the repository in OPFS (`local.ts`): on first load it clones over smart
HTTP with `src/client/Fetch.ts`, and from then on the Code screen's reads and
commits run against the same `Repository` service the server uses — over
`src/adapters/Opfs.ts` — with the server demoted to a remote named `origin`.
The header grows a sync control: **Push ↑n** sends the branch with
`src/client/Push.ts`, **Fetch ↓n** brings origin's movement in (one
advertisement, one pack, both refspecs), and nothing moves without being
asked. The `refs/remotes/origin/*` tracking refs are _observations_ of
origin — written at clone, after a successful push, and by a fetch, never
copied from local heads — so an unpushed commit is still ↑1 after a full
reload, and Push stays enabled over exactly the work that needs it. A
browser without OPFS (or with origin unreachable on first load) simply keeps
the HTTP client; nothing about the page changes.

**The client is derived, not written.** `client.ts` derives an atom-backed
client from `src/server/Api.ts`'s own `HttpApi` declaration
(`AtomHttpApi.Service` from `effect/unstable/reactivity`), so paths, payloads
and error unions cannot drift from the server — the compiler owns that now.
It loads lazily (like Shiki) so the entry bundle does not carry the Effect
runtime; `atoms.ts` bridges atom subscriptions into Lit's reactive-controller
lifecycle. The hand-written `api.ts` remains for the screens that predate the
derivation and migrates piecemeal.

### Pointing it at a repository

`index.html` carries two meta tags:

```html
<meta name="gp-repo" content="core" /> <meta name="gp-api-base" content="" />
```

An empty base means same-origin, which is the deployed case — the Worker serves
both this page and `/:repo/…`.

## Working on it

One command, and it starts the API too:

```bash
GIT_ROOT=/path/to/repos npm run dev:ui   # page, bundle and API on :8000
```

`GIT_ROOT` defaults to the working directory. The page asks for the repository
its `index.html` names — `core` unless you change it — so a root without that
one answers 404 and every screen falls back to the design's fixtures, each
notice naming the reason. That is a working UI showing sample data, not a
broken one, and the startup banner prints the root so the mismatch is visible.

```bash
npm run dev:ui              # watch and serve on :8000 — the one to reach for
npm run build:ui            # bundle to dist/ui
npm run verify:ui           # build, then drive it in a browser

node src/ui/build.ts --watch    # watch only, for serving dist/ui yourself
node src/ui/build.ts --debug    # unminified, for reading a stack trace
```

`--serve` and `--watch` both stay in the foreground and rebuild on change; that
is the process doing its job, not hanging. Only `--serve` puts a page at a URL.

`--serve` hands `dist/ui` to the server itself, so the page, the bundle and
`/:repo/...` all answer on one port. That matters because a browser blocks the
cross-origin alternative outright, and the UI would quietly show its fixtures
instead. `--watch` rewrites `dist/ui` on every change and the server reads it
per request, so a rebuild needs no restart.

This used to be two servers with a hand-written proxy between them — esbuild's
own on a random port, because esbuild cannot forward what it does not have
(its docs say to put a proxy in front, and that is what it was). Nothing is
between them now, and `dev:ui` runs the same code path that ships:

```bash
npm run build:ui && npx git+ serve --root /path/to/repos --ui
```

Deployed, it is the same shape again: the Worker serves both the page and the
API, and `gp-api-base` stays empty.

`verify.ts` runs four suites in Chromium: every screen mounts in both palettes
with nothing thrown; the behaviours from the design conversation still work
(nav collapse, tab switching, hierarchy navigation, theme toggle, deep links,
the kind filter, task creation, commenting, merging, ⌘K search, and the
disabled states of what has no endpoint); and the live surface really does
read and write the API — editing, branch creation, history, grep and the whole
Settings administration surface — the third suite serves the shapes
from `src/server/Api.ts`, so it fails loudly if this UI and that declaration
drift apart. The fourth drives the whole local loop against a _real_ node
host: the page clones into OPFS over smart HTTP, commits locally, pushes back
to origin, and adopts a hub task served by `GET /hub/tasks` — the deployed
shape, end to end.

## Files

| File               | Role                                  |
| ------------------ | ------------------------------------- |
| `main.ts`          | Entry point                           |
| `app.ts`           | Shell, routing                        |
| `base.ts`          | Light-DOM Lit base, navigation event  |
| `elements.ts`      | base-wc element registration          |
| `api.ts`           | Typed client for the JSON API         |
| `client.ts`        | Atom client derived from `Api.ts`     |
| `atoms.ts`         | Atom ↔ Lit reactive-controller bridge |
| `hub.ts`           | Hub queries folded into the store     |
| `identity.ts`      | The browser's signing key; hub writes |
| `local.ts`         | OPFS repository; clone, commit, push  |
| `model.ts`         | Task / Change Request domain          |
| `fixtures.ts`      | The design's Task data                |
| `store.ts`         | The mutable, observable Task store    |
| `theme.ts`         | Palette choice and persistence        |
| `icons.ts`         | Inline SVG icon set                   |
| `highlight.ts`     | Lazy `@pierre/diffs` loader           |
| `nav.sidebar.ts`   | The left rail                         |
| `screen.*.ts`      | One module per screen                 |
| `screen.search.ts` | ⌘K results: tasks and `/grep` hits    |
| `tokens.css`       | Both palettes, as custom properties   |
| `styles/*.css`     | Shell, primitives, and screen styles  |
| `build.ts`         | esbuild bundle                        |
| `verify.ts`        | Browser checks                        |

## Notes for whoever picks this up

- **`@pierre/diffs@1.3.5` cannot be imported the normal way.** Its
  `sideEffects` names `dist/components/web-components.js` — the module that
  registers `<diffs-container>` — but no `exports` subpath reaches it. `build.ts`
  aliases the specifier to the file, and `globals.d.ts` declares it. Both can go
  once the package exports it.

- **`@chr33s/base-wc` bare imports do not survive bundling.** The package marks
  only `./dist/elements.js` and its stylesheets as having side effects, and this
  build consumes the TypeScript sources behind `./src/*`, which that list does
  not cover — so a bundler drops `import "@chr33s/base-wc/src/switch"` as dead
  weight and the elements silently never register. `elements.ts` imports the
  classes and holds them instead.

- **The UI is its own TypeScript project.** `src/ui/tsconfig.json` sets
  `lib: ["ES2024", "DOM", "DOM.Iterable"]`; the root project keeps `WebWorker`
  for `src/`. Both in one program mis-resolves DOM members — it made
  `input.after(button)` typecheck against a `Response`-shaped overload, and
  `lib.dom.d.ts` itself reports duplicate index signatures — so `npm run check`
  runs `tsc` once per project.

- **`@chr33s/base-wc` is consumed as unbuilt TypeScript**, so its sources are
  type-checked as part of this project and `skipLibCheck` does not cover them.
  Three unguarded `items[i]` reads in `src/roving.ts` failed this repository's
  `noUncheckedIndexedAccess`; they were fixed upstream in `1.0.1`, which is what
  the lockfile now pins. Anything similar in a future version will surface as a
  `npm run check` failure in `node_modules/`, not in `src/ui/` — fix it upstream by
  preference, or carry it in `patches/` as the `alchemy` dependency does.

- **Reactive fields use `accessor`.** This repository sets no
  `experimentalDecorators`, so both esbuild and `tsc` compile _standard_
  decorators, and Lit's `@state` / `@property` need `accessor` under those.
  Dropping it fails at runtime, not at build time.

- **Shiki is loaded on demand.** It carries every bundled grammar, which is
  megabytes. `highlight.ts` defers `@pierre/diffs` to first use so Activity,
  Tasks and Settings never pay for it; the runtime-validated entry bundle is
  ~545 kB (~163 kB gzipped) and grammars arrive per language.
