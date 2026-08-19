/**
 * `known_repos` as a file, the way `known_hosts` is a file.
 *
 * Its own module because it reaches for `node:fs` — the same split as
 * `Remotes.node.ts` — so the port stays importable from the Worker bundle and
 * the browser client.
 *
 * The file is rewritten whole on every change rather than appended to. It is a
 * handful of lines that changes when a human answers a prompt, and rewriting
 * it is what makes `remember` able to replace an entry as well as add one.
 * The write goes through a temp file and a rename, so an interrupted write
 * cannot leave a user with a truncated set of trusted repositories — the one
 * failure mode that would silently turn "identity changed" warnings into
 * "first use" prompts.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { StorageFailure } from "../git/Error.ts";
import { type KnownRepo, KnownRepos, parseFile, withEntry, withoutUrl } from "./KnownRepos.ts";

/**
 * `$XDG_CONFIG_HOME/chr33s-git/known_repos`, falling back to `~/.config`.
 *
 * The same rules `bin.ts` applies to the compile cache: set-but-empty and
 * relative both mean "use the default", and a process with no resolvable home
 * — a container running as a bare uid — gets `undefined` rather than a throw,
 * because failing to find a home must not take down a command that was not
 * going to touch the file.
 */
export const defaultPath = (): string | undefined => {
  const configured = process.env["XDG_CONFIG_HOME"];
  const base =
    configured !== undefined && path.isAbsolute(configured)
      ? configured
      : (() => {
          try {
            return path.join(os.homedir(), ".config");
          } catch {
            return undefined;
          }
        })();

  return base === undefined || !path.isAbsolute(base)
    ? undefined
    : path.join(base, "chr33s-git", "known_repos");
};

/** The file's own text, or `""` where there is no file yet. */
const contentsOf = (location: string): string => {
  try {
    return fs.readFileSync(location, "utf8");
  } catch (cause: unknown) {
    // A store nobody has written to yet is empty, not broken: the first
    // `hub enable` on a machine is the ordinary case.
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return "";
    throw cause;
  }
};

const read = (location: string): ReadonlyArray<KnownRepo> => parseFile(contentsOf(location));

const write = (location: string, contents: string): void => {
  fs.mkdirSync(path.dirname(location), { recursive: true, mode: 0o700 });
  const temporary = `${location}.${process.pid}.tmp`;
  // `0600`: this file decides which repositories a user's tooling trusts, so
  // it is written with the permissions ssh gives its own trust stores.
  fs.writeFileSync(temporary, contents, { mode: 0o600 });
  fs.renameSync(temporary, location);
};

/** The store at an explicit path. */
export const file = (location: string): Layer.Layer<KnownRepos> =>
  Layer.sync(KnownRepos, () => {
    const failed = (operation: string) => (cause: unknown) =>
      new StorageFailure({ operation, path: location, cause });

    return KnownRepos.of({
      list: Effect.try({ try: () => read(location), catch: failed("knownRepos.list") }),

      lookup: (url) =>
        Effect.try({
          try: () => read(location).find((entry) => entry.url === url)?.repoId ?? null,
          catch: failed("knownRepos.lookup"),
        }),

      remember: (entry) =>
        Effect.try({
          // Read and write inside one `try`: the CLI is the only writer and it
          // is one process, so this is as atomic as the file needs to be. The
          // edit is by line rather than by reformatting; see `withEntry`.
          try: () => {
            write(location, withEntry(contentsOf(location), entry));
          },
          catch: failed("knownRepos.remember"),
        }),

      forget: (url) =>
        Effect.try({
          try: () => {
            const next = withoutUrl(contentsOf(location), url);
            if (!next.removed) return false;
            write(location, next.contents);
            return true;
          },
          catch: failed("knownRepos.forget"),
        }),
    });
  });

/**
 * The store at its conventional location.
 *
 * A machine with no resolvable home gets a store that answers "nothing is
 * trusted" and refuses to record anything, rather than a layer that fails to
 * build: `hub status` should still run there, and say why.
 */
export const layer: Layer.Layer<KnownRepos> = Layer.suspend(() => {
  const location = defaultPath();
  return location === undefined ? homeless : file(location);
});

const homeless: Layer.Layer<KnownRepos> = Layer.sync(KnownRepos, () => {
  const nowhere = (operation: string) =>
    new StorageFailure({
      operation,
      path: "known_repos",
      cause: "no home directory: set XDG_CONFIG_HOME to record trusted repositories",
    });

  return KnownRepos.of({
    list: Effect.succeed([]),
    lookup: () => Effect.succeed(null),
    remember: () => Effect.fail(nowhere("knownRepos.remember")),
    forget: () => Effect.fail(nowhere("knownRepos.forget")),
  });
});
