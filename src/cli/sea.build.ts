/**
 * Single-executable build (`npm run build:sea`).
 *
 * Two steps: Vite+ Pack folds the CLI and its dependencies into one minified
 * ESM file, then `node --build-sea` (Node 26+) embeds it into a copy of
 * the running node binary. The result is `dist/sea/git+` — one file
 * that needs no `node` or `node_modules` on the machine it runs on, for the
 * platform this script runs on.
 *
 * Why the knobs are set the way they are (40 interleaved `--version` runs on
 * Node 26.7: ESM plus code cache median 46.2 ms and 81.2 MiB peak RSS;
 * CommonJS plus code cache 46.3 ms and 80.8 MiB):
 * - `import.meta.main` is defined to `false`: the bundle is one module, so
 *   every entry guard in it (`main.ts`, `host/Node.ts`) would agree it is
 *   "main" and fire together; `sea.ts` calls `run()` explicitly instead.
 * - ESM is as fast as CommonJS with Node 26.7's SEA code cache, avoids Pack's
 *   CommonJS warning, and is the format it recommends. Its banner restores
 *   `require` for CommonJS dependencies (undici) that use it dynamically.
 * - `useCodeCache` embeds the V8 compile cache in the executable, skipping
 *   parse/compile of the bundle on every start. It disables dynamic `import()`
 *   — safe here because everything is bundled — and ties the executable to
 *   the building node's version and platform, which is already true of the
 *   binary itself.
 * - Minification is start-up time as much as size: less source to read and
 *   fewer bytes of code cache to load.
 *
 * One knob that is deliberately *not* here: an `onLoad` hook substituting a
 * stand-in constructor for the `globalThis.FormData` that `effect/Schema`
 * reads at module scope, which is what makes node materialize its bundled
 * `fetch` — and with it `http2` and `tls` — on every start. It is worth ~19 ms
 * and ~7 MiB of `--version`, and it is not taken: it rewrites a dependency so
 * that `Schema.FormData` in the binary rejects a real `FormData`, which is a
 * different program from the one the tests run. Nothing in a git CLI reaches
 * that schema today, and "today" is the whole of the argument for it.
 *
 * `useSnapshot` — the other half of node's SEA start-up story, which would
 * serialize the heap after module initialization instead of only the compile
 * cache — does not work here on node 26.7. Three independent blockers, in
 * increasing order of how stuck they are:
 * - `node:http`, which `host/Node.ts` imports for `serve`, creates native
 *   handles (`HTTPParser`) the serializer refuses: "global handle not
 *   serialized". Fixable by loading the host lazily.
 * - node materializes its `fetch` implementation, and with it `http2` and
 *   `tls` handles, the first time anything touches `globalThis.FormData` —
 *   which `effect/Schema` does at module scope. Fixable only by rewriting
 *   effect at build time, which is the knob above and is not taken.
 * - `Effect.fn(...)` evaluated at module scope crashes the serializer outright
 *   (`std::length_error: vector::_M_range_insert`, no JS-level error). This
 *   repository has 63 of them, and there is no user-space workaround.
 * Snapshotting is also not obviously worth wanting here: built against the
 * largest subset that does snapshot (`effect/unstable/cli` plus
 * `@effect/platform-node`, no app code), the snapshot binary starts in 39 ms
 * against the code cache's 27 ms and carries 13 MiB more RSS — deserializing
 * effect's heap costs more than compiling it from cache.
 */
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

import { build } from "vite-plus/pack";

const major = Number(process.versions.node.split(".")[0]);
if (major < 26) {
  console.error(`--build-sea needs node >= 26, this is ${process.versions.node}`);
  process.exit(1);
}

const out = path.join("dist", "sea");
fs.mkdirSync(out, { recursive: true });

await build({
  entry: ["src/cli/sea.ts"],
  outDir: out,
  clean: false,
  format: "esm",
  platform: "node",
  target: "node26",
  fixedExtension: true,
  hash: false,
  minify: true,
  // A SEA must contain every runtime dependency; it cannot load
  // `node_modules` after Node has embedded the bundle.
  deps: { alwaysBundle: /.*/, onlyBundle: false },
  // `main` would misfire the guards in `main.ts` and `host/Node.ts`; `dirname`
  // is read at module scope in `session.ts` and `main.ts`, so it must resolve
  // to the executable's directory. The banner also keeps `require` available
  // for CommonJS dependencies that call it dynamically.
  define: { "import.meta.dirname": "__SEA_DIRNAME", "import.meta.main": "false" },
  banner:
    'import { createRequire } from "node:module"; import { dirname } from "node:path"; const require=createRequire(import.meta.url); const __SEA_DIRNAME=dirname(process.execPath);',
  outputOptions: { entryFileNames: "main.mjs", codeSplitting: false },
});

const executable = path.join(out, process.platform === "win32" ? "git+.exe" : "git+");
const configuration = path.join(out, "sea.json");
fs.writeFileSync(
  configuration,
  JSON.stringify(
    {
      main: path.join(out, "main.mjs"),
      mainFormat: "module",
      output: executable,
      disableExperimentalSEAWarning: true,
      useCodeCache: true,
    },
    null,
    2,
  ),
);

execFileSync(process.execPath, ["--build-sea", configuration], { stdio: "inherit" });
// macOS kills an unsigned binary with SIGKILL before it runs a single
// instruction; an ad-hoc signature is enough.
if (process.platform === "darwin") {
  execFileSync("codesign", ["--sign", "-", executable], { stdio: "inherit" });
}
console.log(`${executable} (${(fs.statSync(executable).size / 1024 / 1024).toFixed(0)} MiB)`);
