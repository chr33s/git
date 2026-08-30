# @chr33s/git

One Effect v4 Git core: smart-HTTP server, browser client, Unix CLI. Hosts are a Cloudflare Durable Object, Node, the browser, and the CLI.

Read `docs/internals.md` before changing code. Before writing Effect, read `.claude/skills/effect` and `node_modules/effect/AGENTS.md`; search `node_modules/effect/src` for anything they don't cover.

```
npm run check    # oxfmt + oxlint
npm run fix
npm test         # unit + workerd; interop skips without git
```

- No `node:*` below `host/` except `*.node.ts`. Ports stay `R = never` — do not thread Alchemy `RuntimeContext` through them.
- Errors: `Schema.TaggedError` + `httpApiStatus` in `src/git/Error.ts`. Services: `Context.Service` + `Layer.effect` + `Service.of`. Stream; do not buffer bodies or object walks.
- `Config` not `process.env`. `Effect.catch` not `catchAll`. `Effect.fn("Domain.operation")` on public methods. No `as any` or widen-then-assert; a cast needs a `SAFETY:` reason, and `!` is for an invariant the line above establishes, never for a check nobody made.
- Verify git claims against the real `git` binary.

Specs: `docs/hub.md`, `docs/agents.md`, `docs/cli.md`.
