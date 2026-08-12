/**
 * Single-executable build (`npm run build:sea`).
 *
 * Two steps: esbuild folds the CLI and its dependencies into one minified
 * CommonJS file, then `node --build-sea` (Node 26+) embeds it into a copy of
 * the running node binary. The result is `dist/sea/chr33s-git` — one file
 * that needs no `node` or `node_modules` on the machine it runs on, for the
 * platform this script runs on.
 *
 * Why the knobs are set the way they are (measured on the benchmark harness;
 * together they took `--version` from 163 ms to 93 ms and peak RSS from
 * 131 MiB to 99 MiB):
 * - `import.meta.main` is defined to `false`: the bundle is one module, so
 *   every entry guard in it (`main.ts`, `host/Node.ts`) would agree it is
 *   "main" and fire together; `sea.ts` calls `run()` explicitly instead.
 * - CommonJS output keeps `require` alive for the CommonJS dependencies
 *   (undici) that esbuild's ESM output turns into a runtime throw, and is
 *   what `useCodeCache` needs.
 * - `useCodeCache` embeds the V8 compile cache in the executable, skipping
 *   parse/compile of the bundle on every start. It disables dynamic
 *   `import()` — safe here because everything is bundled — and ties the
 *   executable to the building node's version and platform, which is already
 *   true of the binary itself.
 * - Minification is start-up time as much as size: less source to read and
 *   fewer bytes of code cache to load.
 */
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

import { build } from "esbuild";

const major = Number(process.versions.node.split(".")[0]);
if (major < 26) {
  console.error(`--build-sea needs node >= 26, this is ${process.versions.node}`);
  process.exit(1);
}

const out = path.join("dist", "sea");
fs.mkdirSync(out, { recursive: true });

await build({
  entryPoints: ["src/cli/sea.ts"],
  outfile: path.join(out, "main.cjs"),
  bundle: true,
  minify: true,
  platform: "node",
  format: "cjs",
  target: "node26",
  define: { "import.meta.main": "false" },
});

const executable = path.join(out, process.platform === "win32" ? "chr33s-git.exe" : "chr33s-git");
const configuration = path.join(out, "sea.json");
fs.writeFileSync(
  configuration,
  JSON.stringify(
    {
      main: path.join(out, "main.cjs"),
      mainFormat: "commonjs",
      output: executable,
      disableExperimentalSEAWarning: true,
      useCodeCache: true,
    },
    null,
    2,
  ),
);

execFileSync(process.execPath, ["--build-sea", configuration], { stdio: "inherit" });
console.log(`${executable} (${(fs.statSync(executable).size / 1024 / 1024).toFixed(0)} MiB)`);
