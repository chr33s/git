# Foldkit UI Migration Specification

**Repository:** `chr33s/git`  
**Target framework:** [Foldkit](https://github.com/foldkit/foldkit)  
**Status:** Complete  
**Scope:** Migrate the browser UI from Lit-based application architecture to Foldkit while retaining `@chr33s/base-wc` and Pierre libraries as leaf-level UI/DOM integrations where appropriate.

---

## 0. Outcome

Every phase below is done. The browser UI is one Foldkit application: Foldkit
owns the root, the routing and every application fact; Lit is gone from the
source and from the bundle; `@chr33s/base-wc` and the Pierre libraries stayed,
reached through a typed element boundary and through Mounts.

What the migration is verified by:

| Check                         | Result                     |
| ----------------------------- | -------------------------- |
| Browser verification          | 165 checks, all passing    |
| Unit, integration and interop | 1733 passing, 20 skipped   |
| Foldkit Story coverage        | 14 transitions, no browser |
| Format, lint, typecheck       | Clean                      |

Two things changed that were not behaviour-neutral, and both were deliberate:
the addresses moved from `#/screen` to `/hub/screen` because Foldkit's router
cannot read a fragment (§3.2), and eight browser tests that drove Lit elements
were replaced by Story tests over `update` plus the existing Playwright suite.

Section 13 records what implementation found that this plan did not anticipate.

---

## 1. Objective

Move the `chr33s/git` browser UI fully onto Foldkit so that Foldkit owns:

- application state
- routing
- screen composition
- event/message flow
- asynchronous effects
- subscriptions
- resource lifecycles
- imperative DOM/library lifecycles
- framework-level testing and developer tooling

The migration should remove Lit as the application UI framework without requiring a simultaneous rewrite of stable leaf-level libraries such as:

- `@chr33s/base-wc`
- `@pierre/trees`
- `@pierre/diffs`
- Phosphor icons

"Fully onto Foldkit" means Foldkit becomes the application architecture and renderer. It does **not** mean every Web Component or third-party DOM-owning library must be rewritten in Foldkit.

One deliberate exception to "no behaviour change" is required: the UI moves from hash routing to real paths under a `/hub` prefix. Section 3.2 records that decision and its rationale.

---

## 2. Current-State Summary

The current UI is centered around Lit custom elements, in a flat `src/ui/` module tree.

Key architectural characteristics:

- `gp-app` (`src/ui/app.ts`) owns shell-level state such as route, identity, theme, search query, navigation error, and local API selection. Its default screen is `code` and its default selection is `T-12`.
- Screens use Lit `@state()` / reactive properties for local state.
- `TaskStore` (`src/ui/store.ts:55`) is an `EventTarget` subclass exported as the module singleton `store`. It owns task-domain state, derived queries, subscriptions, mutations, and remote/fallback behavior.
- Navigation is implemented through bubbling `CustomEvent` instances such as `gp-navigate`, plus a `hashchange` listener (`src/ui/app.ts:113`) and direct `location.hash` assignment (`src/ui/app.ts:182`, `src/ui/app.ts:201`).
- `@chr33s/base-wc` depends on light DOM for global CSS and cross-element ARIA relationships. It declares no dependencies of its own and does not import Lit, so removing Lit from the application removes it from the bundle.
- `gp-code` (`src/ui/screen.code.ts`, ~1800 lines) owns several imperative third-party handles and explicit cleanup logic for Pierre tree/diff/editor integrations.
- Playwright verification (`src/ui/verify.ts`, ~2270 lines) already tests the built UI in a real browser across render, interaction, and live API flows. It references hash URLs in 39 places.
- The Worker (`src/worker.ts`) serves `dist/ui` assets-first and falls through to `routeOf` (`src/server/Route.ts`), which treats the first path segment as a repository name.
- The development server (`src/ui/dev.ts`) serves `index.html` only for `/` and `/index.html`.
- `vite.config.ts` sets `base: "./"`, so built asset URLs are relative to the page's own path.

The codebase is already well aligned with several Foldkit concepts:

- immutable domain values
- centralized state
- semantic events
- Effect-based APIs
- explicit lifecycle handling
- framework-independent browser verification

Foldkit's peer dependency on Effect is an exact pin, `4.0.0-rc.112`, which is the version this repository already uses. Section 9.9 covers what that costs.

---

## 3. Target Architecture

### 3.1 Module Layout

The repository's UI convention is a flat `src/ui/` directory with dot-separated lowercase filenames (`screen.code.ts`, `nav.sidebar.ts`, `api.ts`, `hub.ts`, `local.ts`). The migration keeps that convention rather than introducing nested PascalCase directories.

```text
src/ui/
  model.ts             # top-level Model schema
  message.ts           # top-level Message union
  update.ts
  view.ts
  subscriptions.ts
  resources.ts
  route.ts             # route parsers and the /hub prefix

  screen.tasks.ts      # per-screen Model/Message/update/view/commands
  screen.activity.ts
  screen.search.ts
  screen.settings.ts
  screen.detail.ts
  screen.code.ts
  nav.sidebar.ts

  mount.pierre-tree.ts
  mount.pierre-diffs.ts
  element.base-wc.ts   # Foldkit CustomElement bindings

  api.ts               # existing: HTTP Git client
  hub.ts               # existing: hub task/CR operations
  local.ts             # existing: OPFS-backed LocalGitApi

  main.ts
  index.html
  styles/
```

A screen splits into its own `screen.<name>.model.ts` / `.update.ts` / `.view.ts` files only when the single file passes the size at which the existing Lit files became hard to read. Do not split preemptively.

### 3.2 URL Scheme: Hash to `/hub` Paths

**Decision: the UI moves from `#/screen/id` to `/hub/screen/id`.**

Foldkit's router cannot parse hash URLs. `parseUrl` in `route/parser.js` feeds `pathToSegments(url.pathname)` to the parser and never reads the hash, which the Url schema carries only as an opaque `Option<string>`. The browser runtime listens for `popstate`, link clicks, and a programmatic `foldkit:urlchange` event, and never for `hashchange`. Assigning `location.hash` — which is how the current shell navigates — fires `hashchange` and not `popstate`, so Foldkit would not observe those navigations at all.

The two ways out are a hash-to-pathname shim feeding `parseUrlWithFallback` plus a `hashchange` bridge, or real paths. Real paths are chosen: the shim would be permanent load-bearing glue around the one part of Foldkit this application depends on most, and it would leave `pushUrl`/`replaceUrl` unusable.

New URL shape:

```text
/hub/code/src/server/Api.ts
/hub/tasks
/hub/detail/CR-14
/hub/search
/hub/activity
/hub/settings
```

`/hub` with no screen resolves to `/hub/code`, preserving the current default screen.

Route parsing uses `route.restString` for the Code screen's trailing path, which is what makes multi-segment file paths work as a single route parameter. Screen ids keep their current per-segment `encodeURIComponent` encoding, and a malformed escape keeps producing a visible navigation error rather than an exception.

**Server work this requires.** All of it lands in Phase 1, before any screen migrates.

1. `vite.config.ts` sets `base: "/hub/"`. The current `base: "./"` produces relative asset URLs, which resolve wrongly from a nested path such as `/hub/code/src/server/Api.ts`.
2. `src/worker.ts` gains a `/hub` branch ahead of `routeOf`, serving `index.html` from the `ASSETS` binding for any `/hub` path that did not match a built file. Assets-first routing keeps serving the hashed bundles directly.
3. `src/ui/dev.ts` gains the same branch: any `GET` under `/hub` that Vite's middleware does not resolve returns the transformed `index.html`.
4. `hub` becomes a reserved repository name. `routeOf` treats the first segment as a repository, so `/hub/tasks` is otherwise indistinguishable from repository `hub`, route `tasks`. Reject the name at creation and assert the rejection in `src/server/Route.test.ts`.
5. `/` redirects to `/hub/code`.

Do **not** enable Cloudflare's `notFoundHandling: "single-page-application"`. It applies to every unmatched path, so it would swallow repository routes such as `/repo/info/refs` before they reach the Worker.

**Legacy hash URLs.** A bookmarked `#/detail/CR-14` must not break. `index.html` gains a synchronous pre-boot redirect, alongside the existing theme bootstrap, that rewrites a leading `#/` into the equivalent `/hub/` path via `location.replace`. It runs before the bundle loads and can be deleted once external links have aged out.

### 3.3 Top-Level Model

The top-level Foldkit Model should contain application facts only.

Conceptually:

```text
Model
 ├─ route
 ├─ theme
 ├─ identity
 ├─ localRepositoryState
 ├─ tasks
 ├─ activity
 ├─ search
 ├─ settings
 ├─ detail
 └─ code
```

Do not store live handles in the Model, including:

- DOM elements
- `GitApi` instances
- `LocalGitApi` instances
- `FileTree`
- Pierre viewers/editors
- browser subscriptions
- abort controllers
- imperative dialog handles

Those belong in Commands, Resources, ManagedResources, Subscriptions, or Mounts.

---

## 4. Architectural Rules

The finished application should follow these rules.

### 4.1 State

Every application fact lives in the Foldkit Model.

Examples:

- selected route
- selected repository ref
- currently loaded path
- loading/error state
- search query
- task filter
- dialog-open state
- identity state
- local-repository availability
- editor draft metadata

Model any remote value's loading/loaded/failed states with Foldkit's `asyncData` module rather than a hand-rolled `loadState` field, and model form validity with `fieldValidation` rather than ad-hoc booleans.

### 4.2 Messages

Every meaningful application event becomes a Message.

Messages should be fact-oriented and named for what happened.

Examples:

```text
ClickedTask
ChangedTaskFilter
SubmittedNewTask
SucceededCreateTask
FailedCreateTask
ChangedSearchQuery
SelectedBranch
SucceededLoadFile
FailedLoadFile
ClickedMerge
SucceededMerge
FailedMerge
```

### 4.3 Commands

One-shot effects belong in Commands.

Examples:

- HTTP requests
- Git writes
- task mutations
- clipboard writes
- navigation, via `navigation.pushUrl` / `navigation.replaceUrl`
- dialog show/hide calls
- focus
- local storage updates
- identity loading
- OPFS initialization steps
- branch creation
- fetch/push
- commit/save operations

### 4.4 Subscriptions

Long-running external event sources belong in Subscriptions.

Examples:

- theme/media-query changes
- document-level events
- externally emitted application events that remain relevant during a model condition

Browser history is not in this list. The Foldkit runtime owns `popstate` and link interception, and delivers them through the application's `onUrlChange` and `onUrlRequest` configuration.

### 4.5 Managed Resources

Long-lived stateful handles whose lifetime follows application state should use ManagedResources or equivalent resource/service boundaries.

Examples:

- local OPFS repository handles
- persistent repository session handles
- other stateful browser-backed resources not tied to a single DOM node

### 4.6 Mounts

Imperative integrations caused by a live DOM element belong in Mounts.

Examples:

- Pierre file tree
- Pierre diff viewer
- Pierre editor
- DOM-owned third-party widgets

The library owns its subtree, Foldkit owns its lifecycle, and the Model owns application state.

Construct the handle **inside** the Mount's acquire body, never before it. Foldkit's Mount documentation is explicit that `Effect.acquireRelease` only guarantees "acquire completed, therefore release is registered"; a handle built before the call can leak if acquisition is interrupted. Whatever `release` needs must be the success value of `acquire`.

### 4.7 Custom Elements

Use Foldkit `CustomElement` bindings for `base-wc` elements that expose declarative properties and `CustomEvent`s.

Use Commands or Mount-backed adapters for imperative custom-element methods such as dialog `.show()` / `.hide()`.

---

## 5. Migration Principles

1. Keep every migration PR releasable.
2. Change the URL scheme once, in Phase 1, exactly as specified in §3.2, and never again during the migration.
3. Preserve DOM classes wherever possible so CSS remains stable.
4. Retain the current Playwright suite as a framework-independent contract.
5. Do not rewrite `base-wc` as part of the Foldkit migration.
6. Do not rewrite Pierre libraries.
7. Do not move live handles into the Foldkit Model.
8. Do not run Lit and Foldkit as peer architectures longer than necessary.
9. Migrate simpler state-first screens before `gp-code`.
10. Migrate `gp-code` last.
11. Remove Lit only after all production routes render through Foldkit.
12. Prefer explicit Messages and Commands over direct imperative event-handler side effects.
13. Pin `foldkit`, `@foldkit/vite-plugin` and `effect` to exact versions and upgrade all three in a single dedicated PR, never inside a migration PR.

---

## 6. Migration Phases

## Phase 0 — Establish the Behavioral Baseline

### Scope

Freeze the existing browser behavior before architectural changes.

### Work

- Run the current full UI verification suite.
- Confirm all current UI routes render.
- Confirm both light and dark palettes.
- Confirm no page errors.
- Record representative screenshots.
- Record production bundle size.
- Add missing regression coverage where necessary.

### Required coverage

At minimum:

- route navigation
- deep-link reloads
- malformed route handling
- Tasks filter/create/open behavior
- dialog open/close/focus
- `base-wc` keyboard behavior
- Search
- Settings
- Activity
- Detail / CR actions
- Code browsing
- file selection
- branch selection
- file editing
- save/commit
- conflict behavior
- local repository behavior
- sync/fetch/push
- Pierre cleanup after navigation

### Exit Criterion

Every item in the required-coverage list above has at least one named assertion in `src/ui/verify.ts`, and the suite is green. A missing item is a Phase 0 blocker, not a note for later.

Record the baseline numbers — bundle size, suite runtime, assertion count — in the foundation PR description so later phases can be compared against them.

---

## Phase 1 — Add Foldkit Foundation and Change the URL Scheme

**Status: done.** What implementation changed about the plan is recorded under
"What Phase 1 found" below.

### Scope

Introduce Foldkit and land the `/hub` path migration, before any screen is rewritten. Doing routing first means every later phase is written against the final URL shape.

### Work

- Add `foldkit` and `@foldkit/vite-plugin`, both at exact versions.
- Confirm one and only one `vite` resolves, and that it satisfies the plugin's `^7.0.0 || ^8.0.0` peer range. Add this as a CI assertion (§8).
- Set `base: "/hub/"` in `vite.config.ts`.
- Add the `/hub` branch to `src/worker.ts` ahead of `routeOf`, serving `index.html` from the `ASSETS` binding.
- Add the matching `/hub` branch to `src/ui/dev.ts`.
- Reserve `hub` as a repository name in `src/server/Route.ts` and cover it in `src/server/Route.test.ts`.
- Redirect `/` to `/hub/code`.
- Add the pre-boot hash-to-path redirect to `index.html`.
- Port the existing Lit shell's hash parsing to read `location.pathname`. The Lit shell keeps working on the new URLs for the whole dual-framework period.
- Update the 39 hash URLs in `src/ui/verify.ts`.
- Add a minimal Foldkit application mounted into a development-only container.
- Enable Foldkit DevTools in development builds now, not at the end. It is the debugging tool for every phase that follows.
- Validate:
  - development server
  - HMR, including the plugin's state-preserving model bridge
  - production build
  - production minification
  - source maps
  - cleanup/disposal

### Exit Criterion

The production UI serves from `/hub/*`, deep links and reloads work under the Worker and the dev server, legacy hash links redirect, the Playwright suite is green on the new URLs, and Foldkit compiles and runs under the repository's Vite+ toolchain with no other production UI change.

### What Phase 1 found

Six things the plan did not anticipate. Later phases should assume them.

**The prefix has to be applied at manifest time, not per request.** Cloudflare
matches asset paths literally and never strips a prefix, so `assets` takes
`{ directory, base }` and the Worker serves `index.html` from the `ASSETS`
binding for what the manifest misses. `notFoundHandling` stays at its default;
`single-page-application` would answer `/:repo/info/refs` with the page.

**Three hosts serve the UI, not one.** The Worker, `host/Node.ts` and
`ui/dev.ts` all had to learn the prefix. The first two share it through
`server/Static.ts`, which now strips the prefix and falls back to the entry
page. The dev server cannot: it asks Vite first and serves the page on Vite's
fallthrough, because a rule guessing which paths are modules gets
`/hub/node_modules/...` wrong in one direction and `/hub/code/src/Api.ts` wrong
in the other.

**`base: "./"` had to become `base: "/hub/"`, and the entry script had to become
root-absolute.** A relative `./main.ts` resolves against the _route_, so it
broke at any nested address in development. The build had rewritten it
correctly, which is why this only showed up in `dev:ui`.

**Two Vite type trees, one Vite runtime.** `@foldkit/vite-plugin` declares its
plugins against the `vite` package's `Plugin`; Vite+ ships its own
`interface Plugin extends Rolldown.Plugin` inside
`@voidzero-dev/vite-plus-core`. Both describe the same installed `vite@8.2.2`,
but relating them exhausts TypeScript's instantiation depth. One documented
assertion in `vite.config.ts` settles it, backed by a CI assertion that exactly
one Vite resolves. Risk 9.5 was right that this seam needed attention, wrong
that the problem would be resolution.

**A dev-only call is not enough to keep Foldkit out of production.** The Model,
the Message union and the Commands are module-level constructor calls, so a
static import retains the module whatever guards the call. `main.ts` reaches
the probe through a dynamic `import()` behind `import.meta.env.DEV`, and the
production bundle then contains no Foldkit at all.

**DevTools needs `@foldkit/devtools` installed.** The runtime keeps the store
and the bridge; the overlay lives in that package and the Vite plugin injects
it. Without it `devTools` is configured and nothing renders.

### Behaviour that changed

Screen-to-screen navigation is now a real path change. Under the fragment it
was same-document, so a `page.goto` in the verification suite preserved the
tab-local store; now it reloads. The suite navigates through the rail where a
check depends on the previous one. Nothing in the application changed — it
navigates with `pushState` — but any test written against the old behaviour
needs the same treatment.

---

## Phase 2 — Build the `base-wc` Integration Layer

### Scope

Create a single typed Foldkit boundary for Web Components.

### New module

```text
src/ui/element.base-wc.ts
```

### Work

Define Foldkit `customElement` bindings for relevant custom elements, including representative controls such as:

- toggle
- toggle group
- dialogs
- menus
- tabs
- selects
- search/input controls
- other existing `base-wc` elements

### Declarative Controls

For property/event-based controls:

```text
Model -> custom-element property
CustomEvent -> Message
```

### Imperative Controls

For methods such as:

```text
dialog.show()
dialog.hide()
```

use:

```text
Message
  -> update
  -> Command
  -> DOM/custom-element method
  -> result Message
```

Do not call custom-element methods from `view` or mutate DOM directly from `update`.

### Light-DOM Ownership

Foldkit renders through a Snabbdom virtual DOM with keyed diffing. Any `base-wc` element that appends, moves, or removes its own light-DOM children is mutating a subtree the diff also believes it owns. Foldkit's `customElement` module carries controlled-DOM-state and reflection machinery for exactly this, but the behaviour must be verified per element rather than assumed. Audit each binding for children the element mutates itself, and fall back to a Mount for any element that does.

### Exit Criterion

Representative `base-wc` controls work correctly inside a Foldkit view with preserved light-DOM styling, accessibility, keyboard behavior, and custom events, and no element's self-managed children are disturbed by a re-render.

---

## Phase 3 — Migrate Tasks as the First Vertical Slice

### Scope

Replace the Lit implementation of Tasks with an embedded Foldkit feature while keeping the existing shell.

Use `runtime.makeElement` to publish the Foldkit Tasks slice as a custom element the Lit shell renders, or `runtime.embed` to attach it to a container the shell owns. `makeElement` is preferred: the shell's existing `gp-tasks` slot keeps working untouched, which is what makes this phase a genuine spike rather than a shell rewrite. Note that `makeApplication` is for the page-owning root and belongs to Phase 5, not here.

### Suggested files

```text
src/ui/screen.tasks.ts
```

### Move into the Tasks Model

- selected filter
- dialog-open state
- form state, via `fieldValidation`
- submission state
- error state
- any local task-screen display state

### Example Messages

```text
ChangedTaskFilter
ClickedTask
ClickedNewTask
ChangedNewTaskTitle
ChangedNewTaskDescription
ChangedNewTaskParent
SubmittedNewTask
SucceededCreateTask
FailedCreateTask
CompletedCloseNewTaskDialog
```

### Preserve

- existing CSS classes
- current visual hierarchy
- the `/hub` URLs established in Phase 1
- current remote/fallback behavior
- existing Playwright assertions

### Exit Criterion

Tasks contains zero Lit-managed screen state and passes the existing browser verification unchanged or with only selector-neutral updates.

---

## Phase 4 — Replace `TaskStore`

### Scope

Move task-domain state and effects into Foldkit.

### Pure Model Logic

Move derived state and immutable domain operations into pure functions.

Examples:

```text
taskById
ancestorsOf
taskGroups
taskRows
openTaskCount
replaceTask
attachTask
detachTask
```

These are already close to pure in `src/ui/store.ts`; the work is separating them from the `EventTarget` they currently hang off.

### Tasks Model

Conceptually:

```text
TasksModel {
  tasks
  sessions
  liveNotice
  loadState   // AsyncData
}
```

### Remote Operations Become Commands

Examples:

```text
FetchHubTasks
CreateTask
MoveTask
CommentTask
OpenChangeRequest
ReviewChangeRequest
ResolveThread
MergeChangeRequest
```

### Migration Requirement

Remote-first/local-fallback behavior must remain explicit and observable through Messages rather than hidden inside a mutable store.

### Remove

- `TaskStore.subscribe()`
- component-driven `requestUpdate()` caused by store changes
- TaskStore-owned mutable application state
- the module singleton `store`

### Exit Criterion

`TaskStore` is deleted or reduced to non-stateful domain helpers, and task rendering follows the Foldkit Model directly.

---

## Phase 5 — Move the Shell and Routing

### Scope

Make Foldkit own the page, via `runtime.makeApplication` with a routing config.

### Move into the top-level Model

- route
- identity
- theme
- global search query
- local-repository state
- navigation error
- shell-level UI state

### Replace

- `gp-app`
- `NavigateEvent`
- `gp-navigate`
- the shell's `popstate`/pathname listener added in Phase 1
- shell-managed child property propagation

### Routing

Define the route union with `route.defineRouteUnion` and parsers built from `route.literal`, `route.string` and `route.restString`. `restString` carries the Code screen's trailing file path. `parseUrlWithFallback` produces the not-found route that the current shell expresses as `navError`.

Wire `onUrlChange` and `onUrlRequest` in the application config. Navigation from update becomes `navigation.pushUrl` / `navigation.replaceUrl` Commands.

The URL contract is the one established in Phase 1 and must not change here:

```text
/hub/code/src/server/Api.ts
/hub/tasks
/hub/detail/CR-14
/hub/search
```

### Index Cutover

Replace `<gp-app></gp-app>` with a plain Foldkit root container. Keep the synchronous theme bootstrap and the legacy hash redirect in `index.html`.

### Exit Criterion

Foldkit owns the application root and route-to-screen selection, and every URL from Phase 1 still resolves identically.

---

## Phase 6 — Move Theme, Identity, API, and Local Repository Lifecycle

### Theme

Theme becomes Foldkit Model plus Commands/Subscriptions.

Retain the synchronous pre-paint theme bootstrap in `index.html`. It is what prevents a white flash for dark-mode readers and nothing in Foldkit replaces it.

### Identity

Convert startup identity loading into explicit Commands and Messages.

Example:

```text
FetchIdentity
SucceededFetchIdentity
FailedFetchIdentity
```

### HTTP API

Represent the clients in `api.ts` and `hub.ts` as Effect services provided to the runtime, not as Model values. Foldkit's runtime builds services once and reuses them for the application's lifetime.

### Local Repository

Represent only lifecycle facts in the Model.

Example:

```text
LocalRepository =
  Unavailable
  | Opening
  | Ready
  | Failed
```

Keep the `LocalGitApi` handle outside the Model behind a ManagedResource, whose lifetime follows the `Ready` condition. A plain Resource is wrong here: the handle must be released and reacquired when the repository changes, which is precisely the ManagedResource distinction.

### Exit Criterion

No shell component owns API instances, local repository handles, theme listeners, or startup identity effects.

---

## Phase 7 — Migrate Simple Screens

### Order

1. Activity
2. Search
3. Settings
4. Sidebar / shell-only views

Settings is not small: `src/ui/screen.settings.ts` is ~960 lines. Budget it separately from Activity and Search.

### Guidance

Do not create a Foldkit Submodel merely because a Lit component existed.

Use:

- a view function when the old element was only a rendering boundary
- a Submodel when the screen has meaningful independent state and update behavior, with `OutMessage` for anything the shell must react to

### Exit Criterion

Activity, Search, Settings, and sidebar composition contain no Lit application components.

---

## Phase 8 — Migrate Detail / Change Request Workflows

### Scope

Move task detail, discussion, review, resolution, and merge flows to Foldkit. `src/ui/screen.detail.ts` is ~890 lines and this phase is multi-week.

### Move into Model

- discussion hydration state
- comment drafts
- review state
- merge state
- thread state
- errors
- pending operation state

### Commands

Examples:

```text
LoadDetail
PostComment
ReviewChangeRequest
ReplyToThread
ResolveThread
MergeChangeRequest
```

### Testing

Add Foldkit Story tests for workflow transitions.

Examples:

- comment succeeds
- comment fails
- review accepted
- review rejected
- merge succeeds
- merge fails
- thread resolves
- remote fallback occurs

### Exit Criterion

Detail/CR workflows are modeled entirely as Messages, Model transitions, and Commands.

---

## Phase 9 — Migrate Code Read Path

### Scope

Migrate `gp-code` incrementally, starting with read-only functionality. `src/ui/screen.code.ts` is ~1800 lines, the largest file in the UI; Phases 9 and 10 together are the bulk of the migration.

### First Features

- load refs
- choose branch
- load file list
- select file
- load blob
- commit history
- file history
- fallback/offline states
- deep-linked path, parsed by `route.restString`

### Code Model

Represent all relevant read-state facts explicitly, using `asyncData` for each independently loaded value.

### Replace Generation Counters

Where the current implementation manually ignores stale async completions, use Foldkit interruptible Commands keyed by request identity when appropriate.

Examples:

- selected ref
- selected path
- repository identity
- operation id

### Pierre

Mount `@pierre/trees` and read-only `@pierre/diffs` integrations through dedicated Mount modules, following the acquire-body rule in §4.6. Note that `@pierre/diffs` needs the `vite.config.ts` alias to reach its web-component entry; keep that alias.

### Exit Criterion

All Code browsing and history flows work through Foldkit while Pierre remains an isolated DOM integration.

---

## Phase 10 — Migrate Code Editing and Git Operations

### Scope

Move the difficult write/edit paths after the read model is stable.

### Features

- editor attach/detach
- new file
- delete file
- save
- commit
- expected-tip conflict handling
- diff generation
- historical file view
- branch creation
- tag creation
- reset
- merge
- fetch
- push
- sync state
- bisect
- clipboard notices

### Pierre Editor Rule

Pierre must not become a second application-state store.

Either:

1. relevant draft data lives in the Foldkit Model, or
2. Pierre emits semantic change Messages back to Foldkit.

### Lifecycle

Each DOM-owned editor/viewer/tree instance uses a Foldkit Mount with paired acquire/release semantics.

### Exit Criterion

`gp-code` is deleted and all Code functionality is controlled by Foldkit.

---

## Phase 11 — Remove Lit

### Remove

- `lit`
- `lit/decorators`
- `GitPlusElement`
- `@customElement` application wrappers
- `gp-*` elements that exist only as application component boundaries
- Lit lifecycle methods used for application orchestration
- component-level `requestUpdate()` patterns
- application-level `connectedCallback` / `disconnectedCallback` state management

### Retain

Unless separately justified:

- `@chr33s/base-wc`
- `@pierre/diffs`
- `@pierre/trees`
- Phosphor icons
- existing CSS system

`@chr33s/base-wc` declares no dependencies and does not import Lit, so this removal takes Lit out of the bundle entirely rather than leaving it behind a leaf library.

### Exit Criterion

The production UI has no Lit dependency or Lit-managed application components.

---

## Phase 12 — Make the Application Foldkit-Native

Removing Lit is not enough. The final pass should ensure the application uses Foldkit idiomatically.

### Add

- Story tests for update/Command workflows
- Scene tests for views and Mounts
- explicit Command failure Messages
- explicit resource lifecycle modeling
- interruption behavior for superseded work

DevTools was enabled in Phase 1 and is not new work here. Decide its production policy: leave it development-only, or ship it with `show: 'Always'` in `'Inspect'` mode.

### Remove

- the legacy hash-to-path redirect in `index.html`, once external links have aged out

### Architecture Audit

Verify:

- no application facts outside Model
- no side effects directly in view
- no network or storage work directly in update
- no imperative DOM work outside Commands/Mounts
- no long-lived live handles inside Model
- no hidden cross-screen mutable state
- no unnecessary custom-element application wrappers
- no framework-era migration shims remaining

### Final Acceptance Criterion

The application should satisfy:

> Every application fact lives in the Foldkit Model; every application event becomes a Message; every state transition happens in update; every one-shot effect is a Command; every ongoing external source is a Subscription or ManagedResource; and every element-owned imperative library is isolated by a Mount.

---

## 6A. What the Migration Found

Beyond Phase 1's findings, six things the plan did not anticipate.

**A property is not an attribute, and `base-wc` cares which.** Foldkit's
`Value` writes a DOM property. `ui-toggle` and `ui-menu-item` expose `value` as
a getter over their own attribute, so writing the property throws and takes the
whole application down with it; `ui-tabs` accepts the property but reflects its
selection in the attribute, so a property alone leaves the DOM disagreeing with
the Model. Every `base-wc` `value` is written with `Attribute` now. This is the
concrete form of risk 9.6, and it is sharper than that entry expected: the
failure is a crash at render, not a subtle diff.

**Mount arguments are captured once, at mount.** A blob that arrives after its
pane mounted does not reach the renderer, because the Mount does not re-run.
The fix is to key the host on what the surface _is_ — the file, its revision,
the mode, the palette — so a new answer is a new element. The draft is
deliberately outside that key: in edit mode the Model tracks every keystroke,
and remounting on each one would take the caret with it.

**`Effect.tryPromise` widens the thrown value, which loses the class.** The
product's own failure wording lives in `describe()` and depends on
`instanceof ApiError`; a `catch` in the Effect sees an unknown cause instead
and answers "An error occurred in Effect.tryPromise". Every place the reason is
shown to a reader now classifies inside the async function, where the class is
still there, and hands `update` a string.

**Interruption stops a fiber, not a request already past its `await`.** Keying
a Command `interrupt: true` is necessary and not sufficient: the Model also has
to say which answer it is waiting for. `wantedRef` and `diffFor` are that, and
they are the honest replacement for the generation counters the plan expected
to delete outright.

**A module path and a route can collide.** `src/ui/code.ts` and the address
`/hub/code` are the same string to Vite's dev server, which answered a screen
with a module. The dev server now decides by the closed set of screen names
from `route.ts` rather than by guessing what a module id looks like.
Production has no such ambiguity: the asset manifest holds built file names.

**A custom-element boundary was doing layout work.** `gp-code { display:
contents }` let the explorer and the content be the shell's flex items. A view
returns one element, so the wrapper stayed and the rule moved to its class —
the same two columns, with the reason written down.

---

## 7. Recommended Pull Request Sequence

Each PR should be independently buildable and releasable.

1. `build(ui): add Foldkit runtime and Vite+ integration`
2. `feat(ui): serve the UI from /hub paths instead of hash routes`
3. `ui: add typed base-wc Foldkit adapters`
4. `ui(tasks): render Tasks with embedded Foldkit`
5. `ui(tasks): move TaskStore state into Foldkit`
6. `ui: move shell and routing to Foldkit`
7. `ui: move identity and theme lifecycle to Foldkit`
8. `ui: model local repository as Foldkit managed resource`
9. `ui(activity): migrate to Foldkit`
10. `ui(search): migrate to Foldkit`
11. `ui(settings): migrate to Foldkit`
12. `ui(detail): migrate task and CR detail`
13. `ui(code): migrate repository read model`
14. `ui(code): mount Pierre tree and file viewer`
15. `ui(code): migrate editing and commit flow`
16. `ui(code): migrate sync, branch and bisect flows`
17. `ui: remove Lit and gp custom-element framework`
18. `test(ui): add Foldkit Story and Scene coverage`
19. `ui: settle DevTools policy and remove migration shims`

PRs 2 and 12 through 16 are each multi-week. Do not plan the sequence as nineteen comparable units.

---

## 8. Acceptance Gates

Every phase must pass the following gates before proceeding.

### Build

- Vite+ development build succeeds.
- Production UI build succeeds.
- Exactly one `vite` resolves in `node_modules`, asserted in CI, and it satisfies `@foldkit/vite-plugin`'s peer range.
- `foldkit`, `@foldkit/vite-plugin` and `effect` are pinned to exact versions.
- No unexpected production warnings.

### Type Safety

- TypeScript passes.
- Existing Effect schemas remain authoritative for API boundaries.
- No new unchecked `unknown`/assertion escape hatches are introduced for Foldkit integration. The repository's `anti-slop` lint rules apply to Foldkit code without exception.

### Browser Verification

- Current Playwright verification passes.
- No new page errors.
- Both themes render correctly.
- Keyboard behavior remains correct.
- Every `/hub` deep link resolves on a cold reload, under both the Worker and the dev server.
- Every legacy `#/` URL redirects to its `/hub` equivalent.

### Routing and Server

- `hub` is rejected as a repository name.
- Repository routes are unaffected: `/:repo/info/refs` and the rest of the Git surface still reach the Worker.
- `/` redirects to `/hub/code`.

### CSS

- Existing design tokens remain intact.
- Existing global CSS still applies.
- No Shadow DOM is introduced around `base-wc` consumers.
- Existing `gp-*` classes may remain during migration even after Lit components disappear.

### Accessibility

- cross-element ARIA references remain valid
- focus order remains valid
- dialogs retain correct focus trapping/dismiss behavior
- toggle/menu/tab keyboard interactions remain intact

### Resource Safety

- no leaked Pierre handles
- no leaked event listeners
- no stale async completions updating current Model state
- no OPFS/local repository handles leaked across repository switches
- unmount/dispose paths are deterministic

---

## 9. Risk Register

### 9.1 Hash Routing Incompatibility

**Risk:** Foldkit's router reads `url.pathname` only and its runtime never listens for `hashchange`, so the current hash-based navigation cannot survive the migration.

**Mitigation:** Resolved by §3.2. The URL scheme moves to `/hub` paths in Phase 1, ahead of any screen work, with a pre-boot redirect covering legacy links. This is the single highest-risk item in the migration; if Phase 1 cannot land cleanly, the migration should stop there.

---

### 9.2 `/hub` Versus Repository Names

**Risk:** `routeOf` treats the first path segment as a repository name and `hub` matches its validation pattern, so `/hub/tasks` is ambiguous with repository `hub`, route `tasks`.

**Mitigation:** Reserve `hub` at repository creation and branch on it in the Worker ahead of `routeOf`. Cover both in `src/server/Route.test.ts`.

---

### 9.3 Foldkit Pre-1.0 Churn

**Risk:** Foldkit is at 0.158.2 with 377 published versions and releases landing weekly. Its own documentation states that breaking changes may occur in minor releases. A migration spanning nineteen PRs will cross many minors, and a breaking change mid-migration lands on half-migrated code.

**Mitigation:** Pin exact versions. Upgrade in a dedicated PR that touches nothing else, between phases and never within one. Read the changelog before each bump. Accept that a mid-migration breaking change is likely at least once and budget for it.

---

### 9.4 Effect Version Lockstep

**Risk:** `foldkit@0.158.2` peer-depends on `effect` at exactly `4.0.0-rc.112`, not a range. This repository already pins that version, so it works today, but every future Effect upgrade becomes a joint Foldkit/Effect upgrade. Effect itself is still a release candidate.

**Mitigation:** Treat `effect` and `foldkit` as one pinned pair. Do not bump either independently. `@foldkit/vite-plugin` adds a third constraint, `foldkit >= 0.153.0`.

---

### 9.5 Vite Resolution

**Risk:** Foldkit's Vite plugin peer may resolve to an unintended second Vite package.

**Mitigation:** Resolved, though not where expected. Resolution was never the
problem: one hoisted `vite@8.2.2` satisfies the plugin's `^7.0.0 || ^8.0.0`
peer, and `Sources.test.ts` now asserts that exactly one Vite resolves and that
Foldkit, its plugin, its DevTools and Effect are all pinned to exact versions.
The actual friction was in the _types_ — two independent declarations of that
one runtime, too large to relate — which `vite.config.ts` settles with a single
documented assertion.

---

### 9.6 Virtual DOM Versus Light-DOM Web Components

**Risk:** Foldkit renders through Snabbdom with keyed diffing. A `base-wc` element that mutates its own light-DOM children is editing a subtree the diff also owns, which can produce dropped nodes or lost ARIA relationships on re-render.

**Mitigation:** Audit each element in Phase 2 for self-managed children. Foldkit's `customElement` module has controlled-DOM-state handling for this, but verify it per element. Fall back to a Mount where the element genuinely owns its subtree.

---

### 9.7 Imperative `base-wc` APIs

**Risk:** Some Web Components expose methods rather than purely declarative properties/events.

**Mitigation:** Use explicit Commands for one-shot DOM/custom-element operations. Keep these integrations centralized in `element.base-wc.ts`.

---

### 9.8 Long Dual-Framework Period

**Risk:** Lit and Foldkit coexist long enough to create duplicated state, routing, and effect ownership.

**Mitigation:** `runtime.makeElement` lets each migrated screen ship as a custom element inside the existing Lit shell, so screens migrate independently without a shell rewrite. Move shell and central task state soon after the Tasks spike validates the approach.

---

### 9.9 `gp-code` Complexity

**Risk:** `src/ui/screen.code.ts` is ~1800 lines. Migrating the most complex screen too early makes framework evaluation indistinguishable from Pierre lifecycle work.

**Mitigation:** Migrate `gp-code` last and split read-path migration from write/editor migration.

---

### 9.10 CSS Drift

**Risk:** DOM structure changes accidentally alter layout despite retained class names.

**Mitigation:** Preserve classes/markup where practical and keep real-browser screenshot/behavior checks.

---

### 9.11 Hidden Mutable State

**Risk:** Existing services or library handles remain de facto application stores after Lit removal.

**Mitigation:** Final architecture audit. All application facts must be inspectable in the Foldkit Model, which DevTools makes checkable directly.

---

## 10. Definition of Done

The migration is complete when all of the following are true:

- Foldkit owns the application root.
- Foldkit owns routing.
- Foldkit owns all application state.
- The UI serves from `/hub/*` and legacy hash URLs redirect.
- `hub` is a reserved repository name and repository routes are unaffected.
- All async work is represented through Commands or resources.
- Long-running external sources use Subscriptions/ManagedResources.
- Pierre DOM instances are isolated through Mounts.
- `base-wc` is consumed through typed integration adapters.
- `TaskStore` no longer owns mutable application state.
- `gp-app` is removed.
- `gp-code` is removed.
- Lit is removed from dependencies and from the bundle.
- No Lit decorators remain.
- No application custom-element wrappers remain solely for component architecture.
- Existing Playwright verification passes.
- Foldkit Story/Scene coverage exists for important state/effect workflows.
- DevTools is available in development with a settled production policy.
- No known resource leaks or stale-request races remain.
- Theming, accessibility, and production behavior remain stable.

---

## 11. Recommended Migration Order

The recommended dependency order is:

```text
Baseline
  ↓
Foldkit + Vite+ foundation + /hub routing
  ↓
base-wc integration
  ↓
Tasks vertical slice
  ↓
Task domain state
  ↓
Shell + routing
  ↓
Identity/theme/local repository
  ↓
Activity/Search/Settings
  ↓
Detail/CR
  ↓
Code read path
  ↓
Code write/editor path
  ↓
Remove Lit
  ↓
Foldkit-native cleanup/testing
```

The most important sequencing decision is:

> **Routing → Tasks → central state → shell → simple screens → Detail → Code → remove Lit**

Routing moves first because Foldkit cannot work on hash URLs, so every later phase should be written against the final URL shape. After that, the order minimizes the period of dual architecture while deferring the highest-complexity imperative integration until Foldkit has already proven itself across the rest of the application.

---

## 12. Non-Goals

The following are explicitly outside the migration unless separately approved:

- replacing `@chr33s/base-wc`
- replacing Pierre tree/diff/editor libraries
- redesigning the visual system
- redesigning URLs beyond the single hash-to-`/hub` change in §3.2, which is a framework requirement rather than a redesign; screen names, id encoding and route shapes are unchanged
- redesigning server APIs beyond the `/hub` asset branch and the reserved repository name
- changing Git domain semantics
- replacing Effect
- introducing a new CSS framework
- changing application information architecture solely for the framework migration
- server rendering or hydration; Foldkit supports both, and this application stays a browser-only SPA

---

## 13. Follow-Up Decisions

Resolved in this document:

- **URL scheme** — §3.2, `/hub` paths with a legacy hash redirect.
- **Vite+ plugin resolution** — §9.5, a single hoisted `vite@8.2.2` already satisfies the peer range; CI asserts it.
- **Module layout** — §3.1, flat `src/ui/` with the repository's existing dot-separated naming.
- **Local repository handles** — §Phase 6, a ManagedResource rather than a Resource, because the handle must be released and reacquired on repository change.
- **DevTools timing** — Phase 1 for development; production policy settled in Phase 12.

Still to record before implementation begins:

1. exact `base-wc` dialog adapter pattern
2. whether screen state uses nested Model records or Foldkit Submodels, and where `OutMessage` is warranted
3. naming conventions for Messages and Commands
4. cancellation keys for Code loading/editing flows
5. Playwright screenshot baseline policy
6. how long the legacy hash redirect stays before Phase 12 removes it
7. when old Lit files are deleted versus retained temporarily for rollback
8. Foldkit/Effect upgrade cadence, and who reviews the changelog at each bump

These decisions should be documented in the first foundation PR or in a short migration ADR.
