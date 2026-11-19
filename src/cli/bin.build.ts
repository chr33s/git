/**
 * npm `bin` build (`npm run build:bin`).
 *
 * Vite+ Pack bundles the CLI and every runtime dependency into `dist/bin`, run
 * by the installing user's node (>= 24.12) — the portable sibling of
 * `sea.build.ts`, which embeds one platform's node instead. The UI is served
 * from `dist/ui`, which `serve.ts` finds two levels above the bundled module,
 * the same as above `src/cli`.
 *
 * - Code splitting stays on: `bin.ts` turns the compile cache on and then
 *   imports the CLI dynamically, which only helps if the CLI is a separate
 *   file compiled after that call.
 * - `import.meta.main` is forced `false`: a guard bundled into the entry chunk
 *   would see itself as "main" and fire. `bin.ts` dispatches explicitly.
 * - The banner restores `require` for CommonJS deps (undici) that use it
 *   dynamically.
 * - Source maps are written for the notice inventory and left out of the
 *   package.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { build } from "vite-plus/pack";
import { writeReleaseNotices } from "../../tools/release-notices.ts";

const out = path.join("dist", "bin");
fs.rmSync(out, { recursive: true, force: true });

await build({
  entry: { cli: "src/cli/bin.ts" },
  outDir: out,
  clean: false,
  format: "esm",
  platform: "node",
  target: "node24",
  fixedExtension: true,
  hash: false,
  minify: true,
  sourcemap: true,
  dts: false,
  deps: { alwaysBundle: /.*/, onlyBundle: false },
  define: { "import.meta.main": "false" },
  banner:
    'import { createRequire as __createRequire } from "node:module"; const require=__createRequire(import.meta.url);',
});

writeReleaseNotices(undefined, out, false);
fs.chmodSync(path.join(out, "cli.mjs"), 0o755);
console.log(path.join(out, "cli.mjs"));
