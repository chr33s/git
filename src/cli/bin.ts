#!/usr/bin/env node
/**
 * `bin` entry.
 *
 * Nothing but a compile cache and a call into `main.ts`. Running the CLI from
 * source compiles ~380 files on every invocation — effect, its CLI, and this
 * repository — which is most of what `npx git+ --version` spends its
 * time on. `enableCompileCache` hands V8's compiled output to a cache keyed by
 * node version and architecture, so every run after the first reads it back:
 * 513 ms to 359 ms for `--version`, measured on node 26.7.0.
 *
 * The import is dynamic on purpose. A static `import` is hoisted above this
 * file's own statements, so `main.ts` and everything under it would compile
 * before the cache was on and none of it would be stored — which is also why
 * the single executable cannot do this to itself, and carries a build-time
 * code cache instead (`sea.build.ts`).
 *
 * The directory is named rather than defaulted. Left to itself node puts the
 * cache under the temp directory, which on a shared machine is a directory
 * anyone can create first: V8 does not treat a code cache as untrusted input,
 * so a planted blob with a matching key is arbitrary code in this process, on
 * every later run. A cache that belongs to one account cannot be planted by
 * another, and it survives a `/tmp` sweep, which the default does not.
 *
 * `NODE_COMPILE_CACHE` wins when it is set — node reads it before this file
 * runs, and there is nothing to add. The first run is slower either way,
 * because it writes what the rest read.
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
