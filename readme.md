> [!WARNING]  
> Experimental: API is unstable and not production-ready.

# @chr33s/git

Universal Git smart-HTTP protocol server, browser client & unix cli — built on
[Effect](https://effect.website) v4 with modern TypeScript and Web APIs.

The repository is a ground-up rewrite in progress: the git core, the
smart-HTTP server, the JSON API and the node host are real, tested code —
stock `git` clones from and pushes to it, on Workers or self-hosted — while
the wider JSON API surface and webhooks remain typechecked design sketches. See [`docs/rewrite.md`](./docs/rewrite.md) for the full plan and
rationale.

## Prerequisites

- Node.js 24+ and npm 11+
- `git` on `PATH` (the interop tests drive the real binary)
- Chromium via Playwright, optional (the real-browser test skips without it)

## Architecture

The core is ports and adapters. `Repository` — commits, trees, refs, log — is
one service written against two storage ports, and every environment is a
layer swap underneath it:

```mermaid
flowchart LR
	subgraph domain["src/git"]
		R[Repository]
		OS[ObjectStore]
		RS[RefStore]
	end

	subgraph backends
		M[Memory]
		N[Node fs]
		CF[R2 + DO SQLite]
	end

	DO[Durable Object worker] --> R
	R --> OS
	R --> RS
	OS --> M
	OS --> N
	OS --> CF
	RS --> M
	RS --> N
	RS --> CF
```

One Durable Object per repository is not an arbitrary mapping: the DO's input
gate provides the serialization that `RefStore.apply`'s compare-and-swap
demands, and the filesystem backend buys the same guarantee with `rename(2)`.

### What runs today

| module                       | what it is                                                         |
| ---------------------------- | ------------------------------------------------------------------ |
| `src/git/Error.ts`           | tagged errors with `httpApiStatus` annotations                     |
| `src/git/Store.ts`           | `ObjectStore` / `RefStore` ports                                   |
| `src/git/Format.ts`          | the pure/effectful seam — framing, commit and tree codecs, hashing |
| `src/git/Memory.ts`          | in-memory backend                                                  |
| `src/git/Node.ts`            | filesystem backend, git's own on-disk layout                       |
| `src/git/Repository.ts`      | the domain service                                                 |
| `src/git/Cloudflare.ts`      | R2 + Durable Object SQLite backend                                 |
| `src/git/Durable.ts`         | the Worker entry: one Durable Object per repository                |
| `src/git/Pack.ts`            | streaming packfile transport, platform-neutral, git-interop-tested |
| `src/server/Protocol.ts`     | git smart-HTTP: advertisement, upload-pack, receive-pack           |
| `src/server/Api.ts`          | JSON API as one `HttpApi` declaration; the client derives from it  |
| `src/host/Node.ts`           | node host: the same handlers behind `node:http`, self-hostable     |
| `src/artifacts/Namespace.ts` | local Cloudflare Artifacts provider over alchemy's binding tag     |
| `src/artifacts/Sqlite.ts`    | the provider's registry + tokens on Durable Object SQLite          |
| `src/alchemy.run.ts`         | deployment stack: bucket, DO and Worker as values, not config      |
| `src/client/Fetch.ts`        | smart-HTTP fetch client: `lsRemote` + clone, runs anywhere         |
| `src/cli/main.ts`            | CLI: init, refs, log, clone, serve, token — `npx chr33s-git`       |
| `src/adapters/Opfs.ts`       | browser (OPFS) backend — same loose-object layout, fourth backend  |
| `src/client/Client.ts`       | browser client: derived JSON client, clone, local `Repository`     |
| `src/server/Auth.ts`         | scoped tokens: guard on both surfaces, HMAC or revocable verifiers |
| `src/git/Store.contract.ts`  | one storage contract suite, run against all four backends          |
| `src/git/Inflate.ts`         | pull-based zlib inflate — exact stream boundaries, no `node:*`     |

The Worker (`wrangler.json` → `src/git/Durable.ts`) serves the git smart-HTTP
protocol — stock `git` clones from and pushes to it — plus a schema-typed JSON
API per repository (create commit, read commit, log, list refs), whose errors
cross the wire as tagged values (`{ "_tag": "RefConflict", … }`) with statuses
from their own annotations. The wider JSON surface, LFS and webhooks arrive
with the remaining rewrite phases.

The same handlers self-host on plain node — no Cloudflare account required:

```sh
GIT_ROOT=repos node src/host/Node.ts      # or: npx wrangler dev
git clone http://127.0.0.1:8080/my-repo
```

Both hosts are open by default and enforce scoped read/write tokens when told
to: set the `GIT_AUTH_SECRET` binding on the Worker (stateless HMAC tokens via
`Auth.hmacMint`), or pass `serve({ verify })` on node. `git` presents the
token as `http://<token>@host/repo`.

The CLI drives all of it — the same `Repository`, host, client and auth code:

```sh
npx chr33s-git init my-repo && npx chr33s-git serve --secret s3cret &
npx chr33s-git token my-repo --secret s3cret --scope write
npx chr33s-git clone --token <token> http://127.0.0.1:8080/my-repo my-copy
```

### The design sketches

Everything not yet landed lives beside its future home as a `*.sketch.ts`
file: illustrative code that typechecks against the real `effect` and
`alchemy@next` type definitions but is excluded from the build and checks.

| area                       | sketch                     |
| -------------------------- | -------------------------- |
| wider JSON API + webhooks  | `src/server/*.sketch.ts`   |
| host seam abstraction      | `src/host/Host.sketch.ts`  |
| OPFS/node adapter sketches | `src/adapters/*.sketch.ts` |

## Development

```sh
npm install               # postinstall applies patches/ and regenerates worker types
npm run check             # format + type-aware lint
npm run fix               # auto-fix both
npx wrangler dev          # run the Worker locally on port 8080
npx wrangler deploy       # deploy (the tested path)
npx alchemy deploy        # deploy the same stack from src/alchemy.run.ts
```

Two dependency patches ship in [`patches/`](./patches), applied on install:
one defers `RepoClient.raw` so a third-party Artifacts provider can exist, one
aliases `Schema.TaggedErrorClass` in `effect` for `alchemy`'s transitive
dependencies. Both are upstream-shaped and deletable when the versions catch
up.

## Testing

```sh
npm test                  # unit: codecs, contract suite on Memory/Node, git-binary interop
npm run test:integration  # workerd: drives the Worker via wrangler's test harness
```

Four kinds of evidence, deliberately:

- **oids match real git** — the empty tree and known blobs are pinned to the
  values `git hash-object` produces;
- **the repository is really a git repository** — `Node.interop.test.ts`
  writes one through the ports and has the `git` binary read it back
  (`fsck`, `log`, `cat-file`, `ls-tree`, `show`);
- **the contract holds on every backend** — the storage contract suite runs
  against memory, the filesystem, OPFS, and DO SQLite + R2 inside workerd,
  not against a mock;
- **the browser is a real browser** — Playwright loads the bundled client
  into Chromium, writes commits into actual OPFS, re-reads them through a
  fresh store, drives the derived JSON client same-origin, and executes a
  full smart-HTTP clone in-page over the pure-JS inflate (skipped when
  Chromium is absent);
- **the protocol is really git's protocol** — stock `git` clones, pushes,
  deletes branches and fetches incrementally against the server, over
  `node:http` in the unit suite and against the Durable Object in workerd in
  the integration suite.

## Documentation

- [`docs/rewrite.md`](./docs/rewrite.md) — the rewrite: motivation, mechanics, phases
- [`docs/artifacts-provider.md`](./docs/artifacts-provider.md) — evaluation as a Cloudflare Artifacts provider
- [`license.md`](./license.md) · [`security.md`](./security.md)
