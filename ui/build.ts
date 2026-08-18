/**
 * Bundles the UI.
 *
 * esbuild, because the repository already depends on it (`src/cli/sea.build.ts`
 * uses it) and the UI needs nothing a bundler-plus-plugin-stack would add: one
 * entry point, TypeScript in, one JS file and one CSS file out.
 *
 * `@chr33s/base-wc` publishes no `dist`, only TypeScript sources behind its
 * `./src/*` export — so its modules are compiled here along with ours. That is
 * the intended way to consume it; it has no runtime dependencies to hoist.
 *
 * The one alias below works around a packaging gap in `@pierre/diffs@1.3.5`:
 * its `sideEffects` field names `dist/components/web-components.js` — the
 * module that registers `<diffs-container>` and gives it the shadow root the
 * renderers draw into — but its `exports` map has no subpath that reaches it
 * (only `.`, `./edit`, `./react`, `./ssr` and `./worker`). Without it the
 * element stays unregistered and every diff renders unstyled. Pointing at the
 * file directly is the smallest fix available from this side; the alias can go
 * once the package exports it.
 *
 *   node ui/build.ts            # once
 *   node ui/build.ts --serve    # watch, and serve it on :8000
 *   node ui/build.ts --watch    # watch only, for an external server
 *   node ui/build.ts --debug    # unminified, for reading a stack trace
 *
 * `--serve` fronts the bundle with a proxy so the page and the API share an
 * origin, which is the arrangement the deployed Worker gives them and the only
 * one a browser will allow: served from :8000 and calling :8787 directly, every
 * request is cross-origin and is blocked, and the UI silently falls back to its
 * fixtures. Point it elsewhere with `GIT_API`.
 */
import { context, build as esbuild, type Plugin } from "esbuild";
import {
  createServer,
  type IncomingHttpHeaders,
  type IncomingMessage,
  type RequestListener,
} from "node:http";
import { cp } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pwd = dirname(fileURLToPath(import.meta.url));
const outdir = join(pwd, "..", "dist", "ui");

/** See the note above: reached by file path because the package does not
 * export it. Located relative to the `.` entry — which the package *does*
 * export — so it still follows however the installer hoisted the package. */
const diffsWebComponents = join(
  dirname(fileURLToPath(import.meta.resolve("@pierre/diffs"))),
  "components",
  "web-components.js",
);

/**
 * Say where the UI is, and whether the API behind it is actually up.
 *
 * Probed at startup because the alternative is what it replaced: the page loads,
 * every request 502s, and the UI quietly shows its fixtures. Any HTTP answer
 * counts as reachable — `/` is not a route, so even a 400 from the router
 * proves something is listening.
 */
const announce = async (port: number, upstream: string): Promise<void> => {
  const reachable = await fetch(new URL("/", upstream))
    .then(() => true)
    .catch(() => false);

  console.info(`\ngit+ UI on http://localhost:${String(port)}`);
  if (reachable) {
    console.info(`  API proxied from ${upstream}\n`);
    return;
  }
  console.info(`  API at ${upstream} is not answering, so the UI will show its fixtures.`);
  console.info("  Start one alongside this, in another terminal:\n");
  console.info("    GIT_ROOT=/path/to/repos PORT=8787 node src/host/Node.ts\n");
  console.info("  Already running elsewhere? Point at it with GIT_API.\n");
};

/**
 * A request body, for the methods that carry one.
 *
 * Returned as a `Uint8Array<ArrayBuffer>` rather than a `Buffer`: this project
 * compiles against the DOM lib, where `BodyInit` admits an `ArrayBufferView`
 * over a plain `ArrayBuffer` — not node's `Buffer`, and not the wider
 * `ArrayBufferLike` a bare `new Uint8Array(n)` is inferred as.
 */
const collect = async (request: IncomingMessage): Promise<Uint8Array<ArrayBuffer>> => {
  const chunks: Uint8Array[] = [];
  for await (const chunk of request) chunks.push(new Uint8Array(Buffer.from(chunk)));
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const body = new Uint8Array(new ArrayBuffer(total));
  let at = 0;
  for (const chunk of chunks) {
    body.set(chunk, at);
    at += chunk.byteLength;
  }
  return body;
};

/** node's header bag to `fetch`'s, dropping the ones a proxy must not repeat. */
const headersOf = (incoming: IncomingHttpHeaders): Headers => {
  const headers = new Headers();
  for (const [name, value] of Object.entries(incoming)) {
    if (value === undefined) continue;
    if (name === "host" || name === "connection" || name === "content-length") continue;
    headers.set(name, Array.isArray(value) ? value.join(", ") : value);
  }
  return headers;
};

const watch = process.argv.includes("--watch");
const serve = process.argv.includes("--serve");
/** Readable output, for reading a stack trace that names real functions. */
const debug = process.argv.includes("--debug");

/**
 * `index.html` is build output too.
 *
 * Copied on every build rather than once at startup, so editing the page is
 * picked up in watch mode instead of needing a restart. `fs.cp` creates the
 * parent directory, so this is also what puts the file there on a cold start.
 */
const page: Plugin = {
  name: "index-html",
  setup(build) {
    build.onEnd(async () => {
      await cp(join(pwd, "index.html"), join(outdir, "index.html"), { force: true });
    });
  },
};

/** `.ts` specifiers are the repository's import style, and esbuild needs
 * telling that they resolve to TypeScript rather than to emitted output. */
const options = {
  entryPoints: [join(pwd, "main.ts")],
  // Splitting is what lets the Shiki-bearing `@pierre/diffs` chunk stay out of
  // the entry bundle; see `highlight.ts`.
  outdir,
  bundle: true,
  format: "esm" as const,
  target: "es2022",
  platform: "browser" as const,
  splitting: true,
  sourcemap: true,
  minify: !watch && !serve && !debug,
  logLevel: "info" as const,
  loader: { ".svg": "text" as const },
  alias: { "@pierre/diffs/components/web-components": diffsWebComponents },
  plugins: [page],
  // Lit's dev build warns on every element in production; the flag picks the
  // smaller, quieter one.
  define: {
    "process.env.NODE_ENV": watch || serve || debug ? '"development"' : '"production"',
  },
};

if (watch || serve) {
  const ctx = await context(options);
  await ctx.watch();
  if (serve) {
    // esbuild serves the bundle on an ephemeral port; the proxy below is what
    // the browser talks to, so both the page and `/:repo/...` are one origin.
    const assets = await ctx.serve({ servedir: outdir, host: "127.0.0.1", port: 0 });
    const upstream = process.env["GIT_API"] ?? "http://127.0.0.1:8787";
    let warned = false;
    const port = Number(process.env["PORT"] ?? 8000);

    const forward = async (to: string, request: Parameters<RequestListener>[0]) => {
      const body =
        request.method === "GET" || request.method === "HEAD" ? undefined : await collect(request);
      return await fetch(new URL(request.url ?? "/", to), {
        method: request.method,
        headers: headersOf(request.headers),
        body,
      });
    };

    createServer((request, response) => {
      void (async () => {
        try {
          // Ask the bundle first; anything it does not have is an API path.
          // Deciding by 404 rather than by pattern means no list of routes here
          // can drift from the one the server actually serves.
          let answer = await forward(`http://127.0.0.1:${String(assets.port)}`, request);
          if (answer.status === 404) answer = await forward(upstream, request);
          response.writeHead(answer.status, Object.fromEntries(answer.headers));
          response.end(Buffer.from(await answer.arrayBuffer()));
        } catch (cause) {
          // Once, not per request: a dead upstream would otherwise scroll the
          // rebuild output away entirely.
          if (!warned) {
            warned = true;
            console.warn(`\n  API at ${upstream} refused the connection — showing fixtures.\n`);
          }
          response.writeHead(502, { "content-type": "text/plain" });
          response.end(`git+ UI proxy could not reach ${upstream}: ${String(cause)}`);
        }
      })();
    }).listen(port, () => {
      void announce(port, upstream);
    });
  } else {
    // Watch-only serves nothing, which is worth saying out loud: the process
    // then sits in the foreground by design, waiting for the next change.
    console.info("\nwatching ui/ — nothing is being served; use --serve for that\n");
  }
} else {
  await esbuild(options);
}
