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
 *   node src/ui/build.ts            # once
 *   node src/ui/build.ts --serve    # watch, and serve it on :8000
 *   node src/ui/build.ts --watch    # watch only, for an external server
 *   node src/ui/build.ts --debug    # unminified, for reading a stack trace
 *
 * `--serve` hands the built directory to the server itself, which is how the
 * UI is deployed and how `git+ serve --ui` runs it. The page and the API
 * must share an origin — a browser blocks the alternative outright, and the UI
 * silently falls back to its fixtures — and one process serving both is the
 * plainest way to have one.
 *
 * It used to be two servers and a proxy in front: esbuild's own on a random
 * port, forwarded to by a hand-written one, because esbuild has no way to send
 * what it does not have somewhere else (its docs say to put a proxy in front,
 * and that is what this was). None of it is needed once the API can serve a
 * directory: `--watch` writes the bundle to `dist/ui`, the host reads it from
 * there per request, and a rebuild is picked up without a restart.
 *
 * `GIT_ROOT` says which directory of repositories it serves.
 */
import { context, build as esbuild, type Plugin } from "esbuild";

import { serve as serveHost } from "../host/Node.ts";
import { access, cp, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pwd = dirname(fileURLToPath(import.meta.url));
const outdir = process.env["GIT_UI_OUTDIR"] ?? join(pwd, "..", "..", "dist", "ui");

/** See the note above: reached by file path because the package does not
 * export it. Located relative to the `.` entry — which the package *does*
 * export — so it still follows however the installer hoisted the package. */
const diffsWebComponents = join(
  dirname(fileURLToPath(import.meta.resolve("@pierre/diffs"))),
  "components",
  "web-components.js",
);

/**
 * Say where it is, and which repositories are behind it.
 *
 * One address now, where there were three: the bundle, the API and the page
 * are one server. The root is worth printing because which repository the page
 * asks for is baked into its `index.html` — a root without that one answers
 * 404 and the UI shows fixtures, which is a thing to be told rather than to
 * work out.
 */
const announce = (url: string, repositories: string): void => {
  console.info(`\nui:   ${url}`);
  console.info(`      repositories under ${repositories}\n`);
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
      // `--serve` turns esbuild's info logging down, because at `info` every
      // rebuild reprints the whole output table and buries the one line that
      // matters. That also costs the rebuild notice, which is worth keeping,
      // so it is reprinted here. Not for the first build: that one is
      // announced in full below.
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
    // The bundle is on disk, and the server that answers `/:repo/...` can hand
    // out a directory — so it hands out this one. No second server, and
    // nothing forwarding between them. `--watch` above rewrites `dist/ui` on
    // every change and the host reads it per request, so a rebuild needs no
    // restart to be picked up.
    const repositories = process.env["GIT_ROOT"] ?? process.cwd();
    const host = await serveHost({
      root: repositories,
      port: Number(process.env["PORT"] ?? 8000),
      ui: outdir,
    });
    announce(host.url, repositories);
  } else {
    // Watch-only serves nothing, which is worth saying out loud: the process
    // then sits in the foreground by design, waiting for the next change.
    console.info("\nwatching src/ui/ — nothing is being served; use --serve for that\n");
  }
} else {
  await esbuild(options);
}
