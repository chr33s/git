/** Node adapters for Git-compatible invocation and repository discovery. */
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

import { Effect } from "effect";

import { coreCommandNames, type GitInvocationState } from "./GitCompat.ts";

export interface InvocationInput {
  readonly argv: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment: Readonly<Record<string, string | undefined>>;
}

export type ParsedInvocation =
  | { readonly _tag: "Invocation"; readonly invocation: GitInvocationState }
  | { readonly _tag: "InvalidInvocation"; readonly message: string };

type OptionValue =
  | { readonly _tag: "Value"; readonly value: string; readonly next: number }
  | { readonly _tag: "InvalidInvocation"; readonly message: string };

const optionValue = (argv: ReadonlyArray<string>, index: number, option: string): OptionValue => {
  const value = argv[index + 1];
  return value === undefined
    ? { _tag: "InvalidInvocation", message: `${option} requires a value` }
    : { _tag: "Value", value, next: index + 2 };
};

const fromEnvironment = (environment: InvocationInput["environment"], name: string) => {
  const value = environment[name];
  return value === undefined || value === "" ? undefined : value;
};

/**
 * Consume the Git global options that precede a command.
 *
 * The returned `argv` starts with the command, which lets Effect CLI continue
 * to own command-local parsing. Paths are resolved at the point Git would see
 * them, so repeated `-C` options compose rather than all resolving from the
 * original process directory.
 */
export const parseInvocation = (input: InvocationInput): ParsedInvocation => {
  let bare = false;
  let config: string[] = [];
  let cwd = input.cwd;
  let gitDir: string | undefined;
  let index = 0;
  let noPager = false;
  let workTree: string | undefined;

  while (index < input.argv.length) {
    const argument = input.argv[index];
    if (argument === undefined || argument === "--") break;
    if (!argument.startsWith("-")) break;

    if (argument === "-C") {
      const value = optionValue(input.argv, index, "-C");
      if (value._tag === "InvalidInvocation") return value;
      cwd = path.resolve(cwd, value.value);
      index = value.next;
      continue;
    }
    if (argument === "--git-dir" || argument === "--work-tree" || argument === "-c") {
      const value = optionValue(input.argv, index, argument);
      if (value._tag === "InvalidInvocation") return value;
      if (argument === "--git-dir") gitDir = path.resolve(cwd, value.value);
      else if (argument === "--work-tree") workTree = path.resolve(cwd, value.value);
      else config = [...config, value.value];
      index = value.next;
      continue;
    }
    if (argument.startsWith("--git-dir=")) {
      gitDir = path.resolve(cwd, argument.slice("--git-dir=".length));
      index++;
      continue;
    }
    if (argument.startsWith("--work-tree=")) {
      workTree = path.resolve(cwd, argument.slice("--work-tree=".length));
      index++;
      continue;
    }
    if (argument.startsWith("-c") && argument.length > 2) {
      config = [...config, argument.slice(2)];
      index++;
      continue;
    }
    if (argument === "--bare") {
      bare = true;
      index++;
      continue;
    }
    if (argument === "--no-pager") {
      noPager = true;
      index++;
      continue;
    }
    break;
  }

  const environmentGitDir = fromEnvironment(input.environment, "GIT_DIR");
  const environmentWorkTree = fromEnvironment(input.environment, "GIT_WORK_TREE");
  return {
    _tag: "Invocation",
    invocation: {
      argv: input.argv.slice(index),
      cwd,
      gitDir:
        gitDir ??
        (environmentGitDir === undefined ? undefined : path.resolve(cwd, environmentGitDir)),
      workTree:
        workTree ??
        (environmentWorkTree === undefined ? undefined : path.resolve(cwd, environmentWorkTree)),
      config,
      bare,
      noPager,
    },
  };
};

/**
 * Flags only the extension form of any command accepts.
 *
 * Naming one is how a caller says which implementation they meant, for a name
 * both surfaces answer to.
 */
const SELECTORS = ["--into", "--root", "--strategy", "--work"];

/**
 * The same, per command — flags stock git would reject on that command alone.
 *
 * Per command because a name can select the extension for one and be git's own
 * on another: `--branch` distinguishes the extension's `fetch`, and is stock
 * git's flag on `clone`. Listed globally it would have routed
 * `git+ clone --branch main <url>` away from the git it belongs to.
 *
 * `--root` cannot carry `fetch` on its own, which is what made these needed:
 * it has a default, so the extension's own `fetch <repo> <url>` is a complete
 * invocation with no selector in it, and every flag-bearing spelling of it —
 * `git+ fetch --token … <repo> <url>` — was handed to stock git, which reads
 * that grammar as a remote and a refspec.
 */
const COMMAND_SELECTORS: ReadonlyMap<string, ReadonlyArray<string>> = new Map([
  ["fetch", ["--token", "--branch"]],
  ["pull", ["--token"]],
]);

/**
 * Flags whose value is a separate argument, so it is not read as a flag.
 *
 * Otherwise `git commit -m "--root cause"` names a selector in its own message
 * and lands in the extension.
 */
const VALUE_FLAGS = [
  "-b",
  "--initial-branch",
  "-m",
  "--message",
  "--author",
  "-n",
  "--max-count",
  "--onto",
  "--pretty",
  "--format",
  "--date",
  "-e",
  "-d",
  "-D",
  "-M",
  "-s",
  "--source",
  "-c",
  "-C",
  "-u",
  "--untracked-files",
];

const extensionSelector = (argv: ReadonlyArray<string>) => {
  const selectors = [...SELECTORS, ...(COMMAND_SELECTORS.get(argv[0] ?? "") ?? [])];
  let takesValue = false;
  for (const argument of argv.slice(1)) {
    if (takesValue) {
      takesValue = false;
      continue;
    }
    if (argument === "--") return false;
    // Both spellings from one list. Kept as two hand-written chains, a flag
    // added to the bare one and forgotten in the other left `--flag=value`
    // routed to stock git while its spaced form reached the extension.
    if (selectors.some((flag) => argument === flag || argument.startsWith(`${flag}=`))) return true;
    takesValue = VALUE_FLAGS.includes(argument);
  }
  return false;
};

/** Whether argv belongs to the declared Git-shaped surface. */
export const isCoreCompatibilityInvocation = (invocation: GitInvocationState) => {
  const command = invocation.argv[0];
  return (
    command !== undefined && coreCommandNames.has(command) && !extensionSelector(invocation.argv)
  );
};

/**
 * Run the declared Git-compatible surface without translating command argv.
 *
 * A raw argv handoff is deliberate: it keeps Git's positional grammar,
 * stdin protocol, diagnostics, exit status, and future flags intact while the
 * native command implementations converge. Extension-only selectors stay on
 * their extension path and are not part of the compatibility manifest.
 */
export const runCoreCompatibility = (invocation: GitInvocationState) => {
  if (!isCoreCompatibilityInvocation(invocation)) return false;
  const global: string[] = [];
  for (const entry of invocation.config) global.push("-c", entry);
  if (invocation.gitDir !== undefined) global.push(`--git-dir=${invocation.gitDir}`);
  if (invocation.workTree !== undefined) global.push(`--work-tree=${invocation.workTree}`);
  if (invocation.bare) global.push("--bare");
  if (invocation.noPager) global.push("--no-pager");
  const result = spawnSync("git", [...global, ...invocation.argv], {
    cwd: invocation.cwd,
    stdio: "inherit",
  });
  if (result.error !== undefined) {
    process.stderr.write(`git+: cannot run git: ${result.error.message}\n`);
    process.exitCode = 127;
  } else {
    process.exitCode = result.status ?? 1;
  }
  return true;
};

export interface RepositoryLocation {
  readonly gitDir: string;
  readonly workTree: string | null;
}

const directory = (location: string) => {
  try {
    return fs.statSync(location).isDirectory();
  } catch {
    return false;
  }
};

const gitDirFromFile = (location: string): string | null => {
  try {
    const contents = fs.readFileSync(location, "utf8");
    const match = /^gitdir:\s*(.+)\s*$/m.exec(contents);
    return match?.[1] === undefined ? null : path.resolve(path.dirname(location), match[1]);
  } catch {
    return null;
  }
};

const bareRepository = (location: string) =>
  directory(location) &&
  fs.existsSync(path.join(location, "HEAD")) &&
  directory(path.join(location, "objects")) &&
  directory(path.join(location, "refs"));

/**
 * Locate the current repository without invoking Git.
 *
 * Commands that need an opened repository can use this result to bind
 * `Repository` and `WorkTree` without reparsing invocation state.
 */
export const discoverRepository = Effect.fn("cli.GitCompat.discoverRepository")(
  (invocation: GitInvocationState) =>
    Effect.sync(() => {
      if (invocation.gitDir !== undefined) {
        return {
          gitDir: path.resolve(invocation.cwd, invocation.gitDir),
          workTree: invocation.bare ? null : (invocation.workTree ?? invocation.cwd),
        } satisfies RepositoryLocation;
      }

      let current = invocation.workTree ?? invocation.cwd;
      for (;;) {
        const dotGit = path.join(current, ".git");
        if (directory(dotGit))
          return { gitDir: dotGit, workTree: invocation.bare ? null : current };
        const redirected = gitDirFromFile(dotGit);
        if (redirected !== null) {
          return { gitDir: redirected, workTree: invocation.bare ? null : current };
        }
        if (bareRepository(current)) return { gitDir: current, workTree: null };

        const parent = path.dirname(current);
        if (parent === current) return null;
        current = parent;
      }
    }),
);
