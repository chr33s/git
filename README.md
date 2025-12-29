> [!WARNING]  
> Experimental: API is unstable and not production-ready.

# @chr33s/git

Implements the native Git Server, Client, Cli smart-HTTP protocol for fetch and push with modern TypeScript, Web Streams, and modern Web APIs.

## Prerequisites

- Node.js 24+ and npm 11+

## Architecture

Native `GitServer` smart-HTTP protocol deployed at the edge, in‑browser `GitClient` and `GitCli`. Cloudflare Worker entrypoint that routes requests to a per‑repo Durable Object. Refs/trees/commits are kept in DO SQLite; large blobs live in R2. In the browser using standard Web APIs.

```mermaid
flowchart LR
	subgraph Clients
		GC[GitClient]
		CLI[GitCli]
	end

	subgraph GitServer
		W[Worker]
		S[Durable Object]
		DB[(DO SQLite)]
		R2[(R2 Bucket)]
	end

	GC --> W
	CLI --> W
	W --> S
	S --> GC
	S --> DB
	S --> R2

	%% Browser Web APIs used by GitClient
	Streams[Web Streams]
	Crypto[Web Crypto]
	OPFS[(OPFS)]
	GC --- Streams
	GC --- Crypto
	GC --- OPFS
```

## GitServer: Git over HTTP

### Stack

- Cloudflare Workers runtime
- Durable Objects with built‑in SQLite for refs, commits, trees, and tags
- Cloudflare R2 for Git object blobs (file contents)

### HTTP API

- Service discovery: `GET /:repo/info/refs?service=git-{upload,receive}-pack`
- Upload‑pack (fetch): `POST /:repo/git-upload-pack`
- Receive‑pack (push): `POST /:repo/git-receive-pack`

See `src/index.ts` for routing and bindings.

## GitClient: Git in the browser (and Node 22+)

`GitClient` provides Git protocol functionality using only Web standards:

- `fetch()` for HTTP
- Web Streams for efficient data processing
- Web Crypto API for SHA‑1 hashing
- TextEncoder/TextDecoder for string/binary conversion
- OPFS (Origin Private File System) for file checkout

### Features

- Repository operations: info/refs, clone/fetch, create Git objects (blob, tree, commit)
- Full pack‑file handling for upload‑pack and receive‑pack
- Browser‑first: uses Web APIs end‑to‑end
- OPFS integration: automatic checkout to the browser’s private filesystem with repo‑based directory caching

See `JsDoc` comments.

### Browser compatibility

- Chrome/Edge: 86+
- Firefox: 111+
- Safari: 15.2+

## Development

```sh
# dev
npm install
npm run check   # run lint/format checks (use: npm run fix)
npm test        # run unit tests
npm run dev

# prod
npm run build
npm run deploy
```

### Testing

Unit tests use Node.js built-in test runner and cover individual functions and components:

```sh
npm test
```

The E2E tests automatically start the development server and test the Git smart-HTTP protocol endpoints.
