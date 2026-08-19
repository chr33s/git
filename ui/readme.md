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
| Detail   | `POST /:repo/merge`                                                                             | Merging a Change Request whose refs exist  |
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

**The browser holds a signing key.** `identity.ts` generates an Ed25519 key
on first use (WebCrypto, through the same `SshSignature` module every other
author uses), keeps the seed in OPFS beside the clone, and signs hub events
with it: creating a Task opens a real `task.opened` event over
`POST /hub/events`, commenting on a hub Change Request appends
`comment.created`, and both are read back from the server's projection —
never shown optimistically. When the server answers a 401 nonce challenge,
the request retries once under a signed `auth.request` envelope, the same
native scheme the CLI presents. A fresh key is nobody: the Settings identity
card shows its public half so an operator can `hub grant` it, and until a
repository accepts the key (or is served `--open`), mutations fall back to
tab-local state and the dialogs say which happened. The projection half of a
fixture merge remains tab-local, as before.

When the API cannot be reached, Code and Diff fall back to the design's sample
repository and **say so** in an inline note, rather than passing fixtures off as
live data. That keeps the UI reviewable without a running Worker.

**Code goes local when the browser allows it.** After first paint the shell
opens the repository in OPFS (`local.ts`): on first load it clones over smart
HTTP with `src/client/Fetch.ts`, and from then on the Code screen's reads and
commits run against the same `Repository` service the server uses — over
`src/adapters/Opfs.ts` — with the server demoted to a remote named `origin`.
The header grows a sync control: **Push ↑n** sends the branch with
`src/client/Push.ts`, **Fetch ↓n** brings origin's movement in, and nothing
moves without being asked. A browser without OPFS (or with origin unreachable
on first load) simply keeps the HTTP client; nothing about the page changes.

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

The UI and the API are two processes. `ui:dev` starts the first and proxies to
the second; it does not start a server for you, and says so at startup if
nothing is listening:

```bash
GIT_ROOT=/path/to/repos PORT=8787 node src/host/Node.ts   # terminal 1
npm run ui:dev                                            # terminal 2
```

Without terminal 1 the UI still loads — every screen falls back to the design's
fixtures and each notice names the reason. That is a working UI showing sample
data, not a broken one.

```bash
npm run ui:dev              # watch and serve on :8000 — the one to reach for
npm run ui:build            # bundle to dist/ui
npm run ui:verify           # build, then drive it in a browser

node ui/build.ts --watch    # watch only, for serving dist/ui yourself
node ui/build.ts --debug    # unminified, for reading a stack trace
```

`--serve` and `--watch` both stay in the foreground and rebuild on change; that
is the process doing its job, not hanging. Only `--serve` puts a page at a URL.

`--serve` fronts the bundle with a proxy: anything the bundle does not have is
forwarded to the API, so the page and `/:repo/...` share an origin. That matters
because a browser blocks the cross-origin alternative outright, and the UI would
quietly show its fixtures instead. Point it anywhere with `GIT_API`:

```bash
GIT_API=http://elsewhere:9000 npm run ui:dev
```

Deployed, no proxy is involved: the Worker serves both the page and the API, and
`gp-api-base` stays empty.

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

- **The UI is its own TypeScript project.** `ui/tsconfig.json` sets
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
  `npm run check` failure in `node_modules/`, not in `ui/` — fix it upstream by
  preference, or carry it in `patches/` as the `alchemy` dependency does.

- **Reactive fields use `accessor`.** This repository sets no
  `experimentalDecorators`, so both esbuild and `tsc` compile _standard_
  decorators, and Lit's `@state` / `@property` need `accessor` under those.
  Dropping it fails at runtime, not at build time.

- **Shiki is loaded on demand.** It carries every bundled grammar, which is
  megabytes. `highlight.ts` defers `@pierre/diffs` to first use so Activity,
  Tasks and Settings never pay for it; the runtime-validated entry bundle is
  ~545 kB (~163 kB gzipped) and grammars arrive per language.
