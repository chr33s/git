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
import { access, cp, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pwd = dirname(fileURLToPath(import.meta.url));
const outdir = process.env["GIT_UI_OUTDIR"] ?? join(pwd, "..", "dist", "ui");

/** See the note above: reached by file path because the package does not
 * export it. Located relative to the `.` entry — which the package *does*
 * export — so it still follows however the installer hoisted the package. */
const diffsWebComponents = join(
  dirname(fileURLToPath(import.meta.resolve("@pierre/diffs"))),
  "components",
  "web-components.js",
);

/**
 * Say where everything is, and whether the API behind it is actually up.
 *
 * Three ports are in play and only `ui` is meant to be opened, so all three are
 * named together rather than left to be pieced together from esbuild's output:
 * `api` is what the proxy forwards to, and `dev` is esbuild's own asset server,
 * which is ephemeral and serves the bundle without the API beside it.
 *
 * Probed at startup because the alternative is what it replaced: the page loads,
 * every request 502s, and the UI quietly shows its fixtures. Any HTTP answer
 * counts as reachable — `/` is not a route, so even a 400 from the router
 * proves something is listening.
 */
const announce = async (port: number, upstream: string, assets: number): Promise<void> => {
  const reachable = await fetch(new URL("/", upstream))
    .then(() => true)
    .catch(() => false);

  console.info(`\nui:   http://localhost:${String(port)}`);
  console.info(
    `api:  ${new URL(upstream).origin}${reachable ? "" : "  (\u2716 :-> GIT_API | GIT_ROOT)"}`,
  );
  console.info(`dev:  http://127.0.0.1:${String(assets)}\n`);
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
/**
 * The asset layer's cache rules, shipped beside the files they govern.
 *
 * Cloudflare's asset router reads `_headers` from the upload
 * (`worker.ts` binds `dist/ui` as the Worker's static assets). Every chunk
 * except the three entry points carries a content hash in its name, so the
 * blanket rule marks everything immutable and the later, more specific rules
 * win the entry points back to revalidation — Cloudflare applies matching
 * rules top to bottom with the last repeated header winning. The entry
 * points revalidate cheaply (the asset layer serves strong ETags), so a
 * deploy is picked up on the next load without ever serving a stale shell.
 */
const HEADERS = `${[
  "/*\n  Cache-Control: public, max-age=31536000, immutable",
  ...["/index.html", "/main.js", "/main.js.map", "/main.css", "/main.css.map"].map(
    (entry) => `${entry}\n  Cache-Control: public, max-age=0, must-revalidate`,
  ),
].join("\n\n")}\n`;

let built = false;
const page: Plugin = {
  name: "index-html",
  setup(build) {
    build.onEnd(async (result) => {
      await cp(join(pwd, "index.html"), join(outdir, "index.html"), { force: true });
      await writeFile(join(outdir, "_headers"), HEADERS);
      // `--serve` turns esbuild's info logging off so its own ephemeral-port
      // line does not precede — and contradict — the boot message below. That
      // also costs the rebuild line, which is worth keeping, so it is reprinted
      // here. Not for the first build: that one is announced in full.
      //
      // Said before the check below returns, so a build that failed says so
      // rather than going quiet exactly when the notice matters most.
      if (serve && built) console.info(result.errors.length ? "  build failed" : "  rebuilt");
      built = true;
      if (result.errors.length > 0) return;
      // The deploy path serves exactly these three names (`index.html`
      // references the other two), so a build that did not produce them is a
      // deploy that would publish an API with no UI — fail it here, loudly.
      for (const expected of ["index.html", "main.js", "main.css"]) {
        await access(join(outdir, expected)).catch(() => {
          throw new Error(`ui build produced no ${expected} in ${outdir}`);
        });
      }
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
  logLevel: serve ? ("warning" as const) : ("info" as const),
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

    const forward = async (
      to: string,
      request: Parameters<RequestListener>[0],
      body: Uint8Array<ArrayBuffer> | undefined,
    ) => {
      return await fetch(new URL(request.url ?? "/", to), {
        method: request.method,
        headers: headersOf(request.headers),
        body,
      });
    };

    createServer((request, response) => {
      void (async () => {
        try {
          const assetRequest = request.method === "GET" || request.method === "HEAD";
          const body = assetRequest ? undefined : await collect(request);
          // Static assets are necessarily GET/HEAD. Mutations go directly to
          // the API, so their one-shot request streams are consumed exactly
          // once. Reads still ask the bundle first and fall through on 404,
          // avoiding a duplicated list of server routes.
          let answer = assetRequest
            ? await forward(`http://127.0.0.1:${String(assets.port)}`, request, body)
            : await forward(upstream, request, body);
          if (assetRequest && answer.status === 404) {
            answer = await forward(upstream, request, body);
          }
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
      void announce(port, upstream, assets.port);
    });
  } else {
    // Watch-only serves nothing, which is worth saying out loud: the process
    // then sits in the foreground by design, waiting for the next change.
    console.info("\nwatching ui/ — nothing is being served; use --serve for that\n");
  }
} else {
  await esbuild(options);
}
