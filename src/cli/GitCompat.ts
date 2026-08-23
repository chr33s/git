/**
 * The declared Git-shaped CLI surface.
 *
 * This module deliberately contains declarations only: command implementations
 * and their differential cases live beside the commands that earn a manifest
 * entry. Keeping an unsupported command out of this object is what prevents a
 * familiar command name from becoming an untested compatibility promise.
 */
import { Context } from "effect";

/** The stock Git release the differential suite compares against. */
export const gitCompatibilityBaseline = "2.55.0";

export interface CompatibleCommand {
  /** Git flags whose behavior has differential coverage. */
  readonly flags: ReadonlyArray<string>;
  /** Named fixture cases that exercise the declared surface. */
  readonly cases: ReadonlyArray<string>;
}

export interface CoreCompatibilityManifest {
  readonly baseline: string;
  readonly commands: Readonly<Record<string, CompatibleCommand>>;
}

/**
 * Each command is declared here with its Git-shaped grammar and differential
 * tests; a command name in ordinary CLI help alone is never a compatibility
 * claim.
 */
export const coreCompatibility: CoreCompatibilityManifest = {
  baseline: gitCompatibilityBaseline,
  commands: {
    init: {
      flags: ["-q", "--quiet", "--bare", "-b", "--initial-branch"],
      cases: ["work-tree", "bare", "initial-branch", "reinitialize"],
    },
    add: {
      flags: ["-A", "--all", "-u", "--update", "-f", "--force", "-N", "--intent-to-add"],
      cases: ["new", "modified", "removed", "pathspec"],
    },
    status: {
      flags: ["-s", "--short", "-b", "--branch", "--porcelain", "-z"],
      cases: ["clean", "modified", "staged", "untracked", "nul"],
    },
    commit: {
      flags: [
        "-a",
        "--all",
        "-m",
        "--message",
        "--allow-empty",
        "--amend",
        "--no-edit",
        "--author",
      ],
      cases: ["message", "all", "allow-empty", "deterministic-oid"],
    },
    rm: {
      flags: ["-r", "-f", "--force", "--cached", "--ignore-unmatch"],
      cases: ["tracked", "cached", "missing"],
    },
    mv: {
      flags: ["-f", "--force", "-k"],
      cases: ["rename", "destination"],
    },
    restore: {
      flags: ["-s", "--source", "-S", "--staged", "-W", "--worktree", "--ours", "--theirs"],
      cases: ["worktree", "staged", "source"],
    },
    reset: {
      flags: ["--soft", "--mixed", "--hard"],
      cases: ["soft", "mixed", "hard", "pathspec"],
    },
    switch: {
      flags: ["-c", "--create", "-C", "--force-create", "-f", "--force", "--detach"],
      cases: ["existing", "create", "force", "detach"],
    },
    log: {
      flags: ["-n", "--max-count", "--oneline", "--pretty", "--format", "--reverse", "--all"],
      cases: ["default", "limit", "format", "pathspec"],
    },
    show: {
      flags: ["-s", "--no-patch", "--stat", "--name-only", "--name-status", "--pretty", "--format"],
      cases: ["commit", "blob", "stat"],
    },
    diff: {
      flags: ["--cached", "--staged", "--stat", "--name-only", "--name-status"],
      cases: ["worktree", "cached", "two-revisions", "pathspec"],
    },
    grep: {
      flags: ["-n", "--line-number", "-i", "--ignore-case", "-v", "--invert-match", "-F", "-E"],
      cases: ["line-number", "ignore-case", "tree", "pathspec"],
    },
    reflog: {
      flags: ["show", "--date"],
      cases: ["head", "named-ref", "date"],
    },
    branch: {
      flags: [
        "--show-current",
        "-a",
        "--all",
        "-r",
        "--remotes",
        "-d",
        "--delete",
        "-D",
        "-m",
        "-M",
      ],
      cases: ["list", "current", "create", "delete", "rename"],
    },
    tag: {
      flags: ["-l", "--list", "-f", "--force", "-d", "-a", "-m"],
      cases: ["list", "lightweight", "annotated", "delete"],
    },
    merge: {
      flags: ["--ff", "--no-ff", "--ff-only", "--squash", "--no-commit", "-m"],
      cases: ["fast-forward", "merge-commit", "conflict", "abort"],
    },
    rebase: {
      flags: ["--onto", "--continue", "--abort", "--skip"],
      cases: ["linear", "onto", "conflict", "abort"],
    },
    "cherry-pick": {
      flags: ["-n", "--no-commit", "-m", "--continue", "--abort", "--skip"],
      cases: ["clean", "no-commit", "conflict", "abort"],
    },
    clone: {
      flags: ["-b", "--branch", "--bare", "--single-branch"],
      cases: ["local", "branch", "bare", "single-branch"],
    },
    remote: {
      flags: ["-v", "--verbose", "add", "remove", "rename", "get-url", "set-url", "--push"],
      cases: ["list", "add", "remove", "rename", "get-url", "set-url"],
    },
    fetch: {
      flags: ["--all", "--prune", "--tags", "-f", "--force"],
      cases: ["configured", "refspec", "fast-forward", "prune"],
    },
    pull: {
      flags: ["--ff-only", "--rebase"],
      cases: ["fast-forward", "refspec", "diverged"],
    },
    push: {
      flags: ["-u", "--set-upstream", "-f", "--force", "--force-with-lease", "--delete", "--tags"],
      cases: ["fast-forward", "set-upstream", "force", "delete"],
    },
    archive: {
      flags: ["--format", "--output", "--prefix"],
      cases: ["tar", "prefix", "pathspec"],
    },
    bisect: {
      flags: ["start", "good", "bad", "reset"],
      cases: ["start", "good", "bad", "reset"],
    },
    fsck: {
      flags: ["--full", "--unreachable", "--no-reflogs"],
      cases: ["clean", "full", "unreachable"],
    },
    gc: {
      flags: ["--prune"],
      cases: ["default", "prune"],
    },
  },
};

/** Every command named by core-cli-compat.md's v1 scope. */
export const coreCommandNames = new Set([
  "add",
  "archive",
  "bisect",
  "branch",
  "cherry-pick",
  "clone",
  "commit",
  "diff",
  "fetch",
  "fsck",
  "gc",
  "grep",
  "init",
  "log",
  "merge",
  "mv",
  "pull",
  "push",
  "rebase",
  "reflog",
  "remote",
  "reset",
  "restore",
  "rm",
  "show",
  "status",
  "switch",
  "tag",
]);

/** A diagnostic for a command registered without its required test metadata. */
export const manifestProblems = (manifest: CoreCompatibilityManifest): ReadonlyArray<string> => {
  const problems: string[] = [];
  for (const [command, coverage] of Object.entries(manifest.commands)) {
    if (!coreCommandNames.has(command)) problems.push(`${command}: not in the core v1 surface`);
    if (coverage.flags.length === 0) problems.push(`${command}: no declared flags`);
    if (coverage.cases.length === 0) problems.push(`${command}: no differential cases`);
  }
  return problems;
};

/** Parsed Git global invocation state, available to command implementations. */
export interface GitInvocationState {
  readonly argv: ReadonlyArray<string>;
  readonly cwd: string;
  readonly gitDir: string | undefined;
  readonly workTree: string | undefined;
  readonly config: ReadonlyArray<string>;
  readonly bare: boolean;
  readonly noPager: boolean;
}

/** The command runtime receives one invocation rather than consulting globals. */
export class GitInvocation extends Context.Service<GitInvocation, GitInvocationState>()(
  "@chr33s/git/cli/GitInvocation",
) {}
