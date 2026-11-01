/**
 * Single-executable entry.
 *
 * The SEA bundle compiles `import.meta.main` to `false` — inside one bundled
 * module the guards in `main.ts` and `host/Node.ts` would otherwise all see
 * themselves as "main" and fire at once — so this entry exists to call the
 * CLI unconditionally instead. See `sea.build.ts`.
 */
import { basename } from "node:path";
import { getAsset } from "node:sea";
import { run } from "./main.ts";
import { run as remote } from "./remote-id.node.ts";

// Git launches the adjacent helper symlink. execPath resolves that symlink;
// argv0 retains the invoked name and selects the remote-helper protocol.
if (basename(process.argv0).replace(/\.exe$/, "") === "git-remote-git+id") {
  await remote();
} else if (process.argv.length === 3 && process.argv[2] === "--build-info") {
  process.stdout.write(getAsset("build-info.json", "utf8"));
} else if (process.argv.length === 3 && process.argv[2] === "--licenses") {
  process.stdout.write(getAsset("THIRD_PARTY_NOTICES.txt", "utf8"));
} else if (process.argv.length === 3 && process.argv[2] === "--license") {
  process.stdout.write(getAsset("LICENSE", "utf8"));
} else if (process.argv.length === 3 && process.argv[2] === "--dependency-inventory") {
  process.stdout.write(getAsset("dependency-inventory.json", "utf8"));
} else {
  run();
}
