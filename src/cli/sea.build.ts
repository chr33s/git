/**
 * Single-executable build (`npm run build:sea`).
 *
 * Two steps: Vite+ Pack folds the CLI and its dependencies into one minified
 * ESM file, then `node --build-sea` (Node 26+) embeds it into a copy of the
 * running node binary. Result: `dist/sea/git+`, needing no `node` or
 * `node_modules` on the machine it runs on, for the platform this builds on.
 *
 * Knobs (40 interleaved `--version` runs on Node 26.7: ESM+code cache median
 * 46.2 ms / 81.2 MiB peak RSS; CommonJS+code cache 46.3 ms / 80.8 MiB):
 * - `import.meta.main` is forced `false`: the bundle is one module, so every
 *   entry guard in it (`main.ts`, `host/Node.ts`) would see itself as "main"
 *   and fire together; `sea.ts` calls `run()` explicitly instead.
 * - ESM ties CommonJS on speed under Node 26.7's SEA code cache, avoids
 *   Pack's CommonJS warning, and is the format it recommends. Its banner
 *   restores `require` for CommonJS deps (undici) that use it dynamically.
 * - `useCodeCache` embeds the V8 compile cache in the executable, skipping
 *   parse/compile on every start. It disables dynamic `import()` — fine,
 *   everything is bundled — and pins the executable to the building node's
 *   version and platform, already true of the binary itself.
 * - Minification saves start-up time, not just size: less source to read and
 *   less code cache to load.
 *
 * Deliberately not a knob: an `onLoad` hook to stub `globalThis.FormData`
 * before `effect/Schema` reads it at module scope, which is what makes node
 * materialize its bundled `fetch` (and `http2`/`tls`) on every start. Worth
 * ~19 ms / ~7 MiB but not taken — it'd make `Schema.FormData` in the binary
 * reject a real `FormData`, diverging from what the tests run. Nothing in a
 * git CLI reaches that schema today, and "today" is the whole argument.
 *
 * `useSnapshot` — serializing the heap after module init instead of just the
 * compile cache — doesn't work on node 26.7, three blockers in increasing
 * order of stuck:
 * - `node:http` (imported by `host/Node.ts` for `serve`) creates native
 *   `HTTPParser` handles the serializer refuses ("global handle not
 *   serialized"). Fixable by loading the host lazily.
 * - `globalThis.FormData` access at `effect/Schema`'s module scope
 *   materializes `fetch`'s `http2`/`tls` handles, same as above. Fixable
 *   only by the rewrite declined above.
 * - `Effect.fn(...)` at module scope crashes the serializer outright
 *   (`std::length_error: vector::_M_range_insert`, no JS-level error) — 63
 *   call sites here, no user-space workaround.
 * Also not obviously worth it: built against the largest subset that does
 * snapshot (`effect/unstable/cli` + `@effect/platform-node`, no app code),
 * the snapshot binary starts in 39 ms vs the code cache's 27 ms and carries
 * 13 MiB more RSS — deserializing effect's heap costs more than compiling
 * it from cache.
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
