#!/usr/bin/env node
/**
 * `bin` entry.
 *
 * Nothing but a compile cache and a call into `main.ts`. Running from source
 * compiles ~380 files every invocation — effect, its CLI, this repository —
 * most of what `npx git+ --version` spends its time on. `enableCompileCache`
 * caches V8's compiled output keyed by node version and architecture, so
 * later runs read it back: 513 ms to 359 ms for `--version` on node 26.7.0.
 *
 * The import is dynamic on purpose: a static `import` hoists above this
 * file's own statements, so `main.ts` would compile before the cache was on
 * and store nothing — which is also why the single executable can't do this
 * to itself, and carries a build-time code cache instead (`sea.build.ts`).
 *
 * The directory is named rather than defaulted. Node's default puts the
 * cache under the temp directory, which on a shared machine anyone can
 * create first — V8 treats a code cache as trusted, so a planted blob with a
 * matching key is arbitrary code on every later run. A cache scoped to this
 * account can't be planted by another, and survives a `/tmp` sweep the
 * default does not.
 *
 * `NODE_COMPILE_CACHE` wins when set — node reads it before this file runs,
 * so there's nothing to add. Either way the first run is slower: it writes
 * what later runs read.
 */
import { enableCompileCache } from "node:module";
import * as os from "node:os";
import * as path from "node:path";

/**
 * `~/.cache`, if this process has a home at all.
 *
 * `os.homedir()` answers "" when `HOME` is set but empty, and *throws* when
 * `HOME` is unset and the user id has no passwd entry — a container running
 * as a bare uid, which is an ordinary way to run this. Throwing here would
 * take down every command, `--version` included, before it started.
 */
const homeCache = (): string | undefined => {
  try {
    return path.join(os.homedir(), ".cache");
  } catch {
    return undefined;
  }
};

// Set-but-empty is how node itself reads "unset" here, so it has to mean the
// same thing on this side: taking it as configured would leave the cache off
// entirely, which is the one thing this file exists to prevent.
if ((process.env["NODE_COMPILE_CACHE"] ?? "") === "") {
  // The XDG spec's own rule: unset, empty or relative all mean "use the
  // default". Taking a relative one at its word would put a cache directory
  // in whatever repository the CLI was run from, and share nothing between
  // them.
  const configured = process.env["XDG_CACHE_HOME"];
  const cache = configured !== undefined && path.isAbsolute(configured) ? configured : homeCache();
  // No absolute place to put it means no cache: a slower start beats one
  // anybody can write to, and beats not starting.
  if (cache !== undefined && path.isAbsolute(cache)) {
    enableCompileCache(path.join(cache, "chr33s-git"));
  }
}

const { run } = await import("./main.ts");

run();
