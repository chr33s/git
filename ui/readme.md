# git+ UI

The web interface for `@chr33s/git`, implementing the Claude Design prototypes
`git-plus.dc.html` (dark) and `git-plus light.dc.html` (light).

## What it is

Five screens, in one shell with a collapsible rail:

| Screen       | Contents                                                                     |
| ------------ | ---------------------------------------------------------------------------- |
| **Activity** | A fortnight of work as a calendar timeline; every card opens its Task        |
| **Code**     | Repository explorer and file view — **wired to the JSON API**                |
| **Tasks**    | Tasks and Change Requests as one hierarchy                                   |
| **Detail**   | A Task, or a Change Request with refs, diff, commits, checks and merge state |
| **Settings** | General, merge policy, danger zone                                           |

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

**Code and Diff read the server.** The client in `api.ts` calls the endpoints
declared in `src/server/Api.ts`:

| Screen | Endpoint              | Use                                  |
| ------ | --------------------- | ------------------------------------ |
| Code   | `GET /:repo/refs`     | Branch list and the tip oid          |
| Code   | `GET /:repo/files`    | Tree paths for the explorer          |
| Code   | `GET /:repo/file`     | Blob content for the file view       |
| Code   | `GET /:repo/log/:oid` | The latest-commit bar                |
| Detail | `POST /:repo/diff`    | Which files a Change Request touches |
| Detail | `GET /:repo/file`     | Both sides of each changed file      |

**Tasks and Change Requests are fixtures.** The git-native hub
(`src/hub/PullRequest.ts`, `src/hub/Projection.ts`) already models pull
requests, reviews, comments and checks as signed events in `refs/hub/*`, but it
has no HTTP surface yet — `Api.ts` exposes git itself and nothing else. Until it
does, `fixtures.ts` carries the design's own data behind the types in
`model.ts`, so replacing it is a change to one module.

When the API cannot be reached, Code and Diff fall back to the design's sample
repository and **say so** in an inline note, rather than passing fixtures off as
live data. That keeps the UI reviewable without a running Worker.

### Pointing it at a repository

`index.html` carries two meta tags:

```html
<meta name="gp-repo" content="core" /> <meta name="gp-api-base" content="" />
```

An empty base means same-origin, which is the deployed case — the Worker serves
both this page and `/:repo/…`.

## Working on it

```bash
npm run ui:dev              # watch and serve on :8000 — the one to reach for
npm run ui:build            # bundle to dist/ui
npm run ui:verify           # build, then drive it in a browser

node ui/build.ts --watch    # watch only, for serving dist/ui yourself
node ui/build.ts --debug    # unminified, for reading a stack trace
```

`--serve` and `--watch` both stay in the foreground and rebuild on change; that
is the process doing its job, not hanging. Only `--serve` puts a page at a URL.

`verify.ts` runs three suites in Chromium: every screen mounts in both palettes
with nothing thrown; the behaviours from the design conversation still work
(nav collapse, tab switching, hierarchy navigation, theme toggle, deep links);
and Code and Diff really do read the API — that last suite serves the shapes
from `src/server/Api.ts`, so it fails loudly if this UI and that declaration
drift apart.

## Files

| File             | Role                                 |
| ---------------- | ------------------------------------ |
| `main.ts`        | Entry point                          |
| `app.ts`         | Shell, routing                       |
| `base.ts`        | Light-DOM Lit base, navigation event |
| `elements.ts`    | base-wc element registration         |
| `api.ts`         | Typed client for the JSON API        |
| `model.ts`       | Task / Change Request domain         |
| `fixtures.ts`    | The design's Task data               |
| `theme.ts`       | Palette choice and persistence       |
| `icons.ts`       | Inline SVG icon set                  |
| `highlight.ts`   | Lazy `@pierre/diffs` loader          |
| `nav.sidebar.ts` | The left rail                        |
| `screen.*.ts`    | One module per screen                |
| `tokens.css`     | Both palettes, as custom properties  |
| `app.css`        | Everything else                      |
| `build.ts`       | esbuild bundle                       |
| `verify.ts`      | Browser checks                       |

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
  Tasks and Settings never pay for it; the entry bundle is ~300 kB (~91 kB
  gzipped) and grammars arrive per language.
