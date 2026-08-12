# Can `@chr33s/git` be an Artifacts provider?

**Short answer: yes, and the fit is closer than you'd expect — Cloudflare
Artifacts is a git host, and so is this. Three capabilities are missing, one of
them (auth) is missing entirely rather than partially, and one upstream
interface change is needed before a third-party provider can satisfy the
binding's type.**

Evaluated against the code, not the docs (`alchemy.run` is unreachable from this
sandbox): `alchemy@2.0.0-beta.70`'s `src/Cloudflare/Artifacts/*` and the
`Artifacts` / `ArtifactsRepo` interfaces in
`@cloudflare/workers-types/experimental`.

## What the contract actually is

Cloudflare Artifacts is ["git for
agents"](https://blog.cloudflare.com/artifacts-git-for-agents-beta/): a
namespace holds git-compatible repos, each with an HTTPS remote, scoped access
tokens, and fork/import operations. Alchemy models it as a binding, not a
provisioned resource — `Namespace` is a marker (namespaces are implicit on
Cloudflare, conjured by the first repo), and everything real happens through the
runtime client.

The surface a provider has to implement:

| namespace                                                   | repo handle                                                |
| ----------------------------------------------------------- | ---------------------------------------------------------- |
| `create(name, { readOnly, description, setDefaultBranch })` | `createToken(scope?, ttl?)`                                |
| `get(name)`                                                 | `listTokens()`                                             |
| `list({ limit, cursor })`                                   | `revokeToken(tokenOrId)`                                   |
| `delete(name)`                                              | `fork(name, { description, readOnly, defaultBranchOnly })` |
| `import({ source: { url, branch, depth }, target })`        |                                                            |

Repo metadata is fixed: `id`, `name`, `description`, `defaultBranch`,
`createdAt`, `updatedAt`, `lastPushAt`, `source`, `readOnly`, `remote`.

## Fit against what exists today

| capability                                     | status               | notes                                                                                                            |
| ---------------------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------- |
| git smart-HTTP (`upload-pack`, `receive-pack`) | **have**             | `src/server/Protocol.ts` + `src/git/Pack.ts` — stock `git` clones and pushes against workerd in the test suite   |
| repo `create` / `delete`                       | **partial**          | repos are conjured by first use (one DO per name); no delete endpoint in the rewrite yet                         |
| `setDefaultBranch`                             | **partial**          | `RefStore.setHead` exists; nothing exposes it over HTTP yet                                                      |
| HTTPS `remote` URL                             | **have**             | the worker route _is_ the remote                                                                                 |
| LFS                                            | **bonus**            | Artifacts does not offer it; we do                                                                               |
| `import` from a remote                         | **partial**          | `clone`/`fetch` + shallow support exist; needs `depth`/`branch` plumbing and an async `IMPORT_IN_PROGRESS` state |
| repo metadata                                  | **partial**          | `description`, `readOnly`, `createdAt`, `lastPushAt`, `source` have nowhere to live                              |
| `list` with cursor                             | **missing**          | see below — this is architectural                                                                                |
| `fork`                                         | **missing**          | feasible cheaply; see below                                                                                      |
| tokens / any auth                              | **missing entirely** | `grep -ri 'authorization\|bearer\|token' src/` returns only commit `author` lines                                |

### The three gaps, in order of cost

**1. Auth and tokens — the big one.** There is no authentication anywhere in the
codebase today: any caller who can reach the worker can push to any repo. The
Artifacts contract needs scoped (`read`/`write`), TTL'd, revocable per-repo
tokens, returned in plaintext exactly once at creation. That means an issuance
scheme (HMAC over `repo|scope|exp` with a stack secret, or random tokens hashed
at rest), a revocation table, and verification middleware on _both_ the JSON API
and the smart-HTTP endpoints.

One detail that matters for compatibility: `git` sends credentials as HTTP Basic
over HTTPS, so the token has to be accepted as the password field of
`Authorization: Basic`, not only as a bearer token. Get that wrong and
`git clone` fails while `curl` works.

**2. A namespace registry.** Repos are addressed by
`env.GIT_REPO.idFromName(repo)` — a Durable Object per repo, and DO namespaces
cannot be enumerated. `list({ limit, cursor })` therefore has nothing to read.
The fix is a registry keyed by namespace holding one row per repo (name, id,
metadata, timestamps): a single index DO, or D1 if you want cross-region reads.
Repo creation/deletion writes through it. This also gives the metadata fields in
gap 3 somewhere to live, so the two are one piece of work.

**3. `fork`.** Naively, copying every object. Cheaply, git's own answer:
alternates. The fork's registry row points at its parent, its own object store
starts empty, and reads fall through to the parent until the first write —
`defaultBranchOnly` then only copies one ref. That is a `ObjectStore` decorator
in the [rewrite sketch](./rewrite.md)'s terms — roughly 40 lines — and nothing
above the port changes.

## The upstream blocker

`ReadWriteNamespaceClient.raw` is `Effect<Artifacts, never, RuntimeContext>`,
which a third-party provider can decline the way alchemy's own local R2 gateway
does — it notes that `raw` "cannot be satisfied … so it dies with guidance"
(`src/Cloudflare/R2/LocalR2Gateway.ts`). Fine precedent, no change needed.

`RepoClient.raw`, though, is an **eager property** of type `ArtifactsRepo`, not
an Effect:

```ts
export interface RepoClient {
  raw: ArtifactsRepo;          // ← cannot be produced off-platform
  createToken(...): Effect<...>;
  ...
}
```

A non-Cloudflare provider cannot construct one, and cannot defer the failure
either — the property is read, not called. The options are a throwing `Proxy`
(works, dishonest) or a one-line upstream change making it
`Effect<ArtifactsRepo, ArtifactsError, RuntimeContext>` like its namespace-level
sibling. That change is backwards-compatible for the native binding and is worth
proposing before building anything else.

**Patched locally in the meantime.** `alchemy@2.0.0-beta.70` is installed and
[`patches/alchemy+2.0.0-beta.70.patch`](../patches/alchemy+2.0.0-beta.70.patch)
applies exactly the change above via `patch-package` on `postinstall` — the
interface in `lib`/`src` and the native binding's `wrapRepo`, which now wraps
the handle in `Effect.succeed`. `src/artifacts/Alchemy.test.ts` pins the shape
at compile time, so a dependency bump that reverts it fails `npm run check`.
The patch file doubles as the upstream PR's diff.

## Where this provider would earn its place

Alchemy ships three implementations of the R2 binding — native worker binding,
scoped HTTP token, and local gateway. Artifacts ships **one**: the native
binding. There is no local or self-hosted implementation, which means today
`alchemy dev` against an Artifacts-using Worker talks to the real service, and
there is no way to run it anywhere else.

That is the opening, and it is a better pitch than "alternative to Cloudflare
Artifacts":

- **local dev** — `Cloudflare.Artifacts.ReadWriteNamespaceLocal`, backed by the
  node host from the rewrite sketch, so Artifacts-using Workers get an offline
  loop and CI without network;
- **tests** — the same binding over in-memory stores, which is what the sketch's
  `memory` layer already provides;
- **self-hosting** — the same program on your own Workers account or a box,
  for people who want the data in their own R2.

## Verdict and cost

The architecture does not fight this. The rewrite sketch's shape maps onto the
contract almost line for line: `RepoHost.stores(name)` is the namespace, one app
instance per repo is `ArtifactsRepo`, and the git protocol is already there. The
work is not in the git internals — it is the three things around them.

| work                                                                    | rough size                |
| ----------------------------------------------------------------------- | ------------------------- |
| upstream: make `RepoClient.raw` an Effect                               | a PR, plus review latency |
| registry + metadata (one index DO or D1)                                | ~1 week                   |
| tokens: issuance, storage, revocation, Basic + Bearer middleware, tests | ~2 weeks                  |
| `fork` via alternates, `import` with depth/branch + progress state      | ~1 week                   |
| the binding layer itself + conformance tests against the interface      | ~1 week                   |

Call it 5–6 weeks to a credible local/self-hosted Artifacts provider, of which
the git-specific part is about a day — everything else is registry, auth and
lifecycle. Auth is worth doing regardless of whether the provider ships: the
server is currently open by construction, and that is the single most important
thing to know from this evaluation.

[`src/artifacts/Namespace.sketch.ts`](../src/artifacts/Namespace.sketch.ts) sketches the
binding implementation, with the missing pieces named as ports rather than
hand-waved.
