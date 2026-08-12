/**
 * Single-executable build (`npm run build:sea`).
 *
 * Two steps: esbuild folds the CLI and its dependencies into one ESM file,
 * then `node --build-sea` (Node 26+) embeds that file into a copy of the
 * running node binary. The result is `dist/sea/chr33s-git` — one file that
 * needs no `node` or `node_modules` on the machine it runs on, for the
 * platform this script runs on.
 *
 * Why the knobs are set the way they are:
 * - `import.meta.main` is defined to `false`: the bundle is one module, so
 *   every entry guard in it (`main.ts`, `host/Node.ts`) would agree it is
 *   "main" and fire together; `sea.ts` calls `run()` explicitly instead.
 * - The banner restores `require`: CommonJS dependencies (undici) call it
 *   dynamically, which esbuild's ESM output otherwise turns into a throw.
 * - No snapshot or code cache: `mainFormat: "module"` excludes snapshots,
 *   and the code cache breaks `import()`.
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
  outfile: path.join(out, "main.js"),
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node26",
  define: { "import.meta.main": "false" },
  banner: {
    js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);",
  },
});

const executable = path.join(out, process.platform === "win32" ? "chr33s-git.exe" : "chr33s-git");
const configuration = path.join(out, "sea.json");
fs.writeFileSync(
  configuration,
  JSON.stringify(
    {
      main: path.join(out, "main.js"),
      mainFormat: "module",
      output: executable,
      disableExperimentalSEAWarning: true,
    },
    null,
    2,
  ),
);

execFileSync(process.execPath, ["--build-sea", configuration], { stdio: "inherit" });
console.log(`${executable} (${(fs.statSync(executable).size / 1024 / 1024).toFixed(0)} MiB)`);
