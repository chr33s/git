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
 */
import { context, build as esbuild, type Plugin } from "esbuild";
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
    // `hosts` rather than `host`: esbuild reports every interface it bound.
    const { hosts, port } = await ctx.serve({ servedir: outdir, port: 8000 });
    console.info(`\ngit+ UI on http://${hosts[0] ?? "localhost"}:${port}\n`);
  } else {
    // Watch-only serves nothing, which is worth saying out loud: the process
    // then sits in the foreground by design, waiting for the next change.
    console.info("\nwatching ui/ — nothing is being served; use --serve for that\n");
  }
} else {
  await esbuild(options);
}
