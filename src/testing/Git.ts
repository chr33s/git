/**
 * The real `git` binary, for the suites that verify against it.
 *
 * Every interop suite opens with the same two moves: ask whether a `git`
 * exists on this machine at all, and run it against a temp repository. Both
 * live here once instead of once per file.
 *
 * The identity flags are baked into the runner because these tests must not
 * depend on the machine's git config: a bare `git commit` succeeds on a
 * laptop with `user.name` configured and dies on a machine without one, which
 * would make the suite's outcome a property of the environment rather than of
 * the code under test. The values are deliberately throwaway — no assertion
 * anywhere reads them back.
 *
 * In `src/` rather than a test directory for the same reason as
 * `Store.contract.ts`: it is shared test infrastructure that test files
 * import, not a `*.test.ts` file that discovery should collect.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import * as path from "node:path";

/**
 * The environment every `git` in the suite runs under.
 *
 * The identity flags below are not enough on their own: a developer whose
 * `~/.gitconfig` sets `commit.gpgsign` with an SSH key cannot commit
 * non-interactively, so every interop suite fails with a passphrase prompt on
 * their machine and passes on everyone else's. Pointing git's config lookup at
 * a file that does not exist is how it is told to consult nothing.
 */
export const gitEnv: NodeJS.ProcessEnv = {
  ...process.env,
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_SYSTEM: "/dev/null",
  // Not merely absent: set, so nothing falls back to $HOME/.gitconfig.
  GIT_CONFIG_NOSYSTEM: "1",
};

/** Whether a `git` binary is on the PATH; suites `describe.skipIf(!hasGit)`. */
export const hasGit: boolean = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

/**
 * A synchronous `git` runner fixed to one repository.
 *
 * Synchronous on purpose: these callers drive git between assertions and want
 * its stdout as a value. Suites whose server runs on this process's event
 * loop need an async runner instead — a blocked loop can never answer the
 * client it is waiting on — and keep their own.
 */
export const gitIn =
  (cwd: string) =>
  (...args: string[]): string =>
    execFileSync("git", ["-c", "user.name=T", "-c", "user.email=t@e.com", ...args], {
      cwd,
      encoding: "utf8",
      env: gitEnv,
    });

/**
 * A whole history in one `git` process.
 *
 * Building a fixture commit by commit costs four processes per commit — write
 * the file, `add`, `commit`, `rev-parse` — and spawning `git` is ~84ms on
 * macOS against ~5ms on a Linux CI box, so a thirteen-commit fixture spent
 * over three seconds doing nothing but starting processes. `fast-import`
 * reads the whole thing on stdin and makes it in one.
 *
 * The marks are the point as much as the speed: a stream names each commit
 * `:1`, `:2`, … and `--export-marks` writes back what each became, so the
 * caller gets its oids without a `rev-parse` per commit.
 */
export const fastImport = (cwd: string, stream: string): ReadonlyMap<number, string> => {
  const marks = path.join(cwd, "fast-import-marks");
  execFileSync(
    "git",
    [
      "-c",
      "user.name=T",
      "-c",
      "user.email=t@e.com",
      "fast-import",
      "--quiet",
      `--export-marks=${marks}`,
    ],
    { cwd, input: stream, env: gitEnv },
  );

  return new Map(
    readFileSync(marks, "utf8")
      .split("\n")
      .filter((line) => line !== "")
      .map((line) => {
        const [mark, oid] = line.split(" ");
        return [Number(mark!.slice(1)), oid!] as const;
      }),
  );
};

/**
 * One commit in a `fast-import` stream.
 *
 * `from` is written out even where fast-import would infer it from the
 * branch's current head, because a fixture that says which commit it descends
 * from is a fixture whose shape can be read off the page — and the two cases
 * that *must* say so, a new branch and a merge, would otherwise look like the
 * ones that need not.
 */
export const importCommit = (input: {
  readonly branch: string;
  readonly mark: number;
  readonly message: string;
  /** Omitted for a root commit. */
  readonly from?: number;
  readonly merge?: number;
  readonly files: ReadonlyArray<{ readonly path: string; readonly content: string }>;
}): string => {
  // Fixed, increasing timestamps: a fixture whose commit times come from the
  // clock is one whose history differs between runs.
  const when = 1_700_000_000 + input.mark;
  const lines = [
    `commit ${input.branch}`,
    `mark :${input.mark}`,
    `author T <t@e.com> ${when} +0000`,
    `committer T <t@e.com> ${when} +0000`,
    `data ${Buffer.byteLength(input.message)}`,
    input.message,
    ...(input.from === undefined ? [] : [`from :${input.from}`]),
    ...(input.merge === undefined ? [] : [`merge :${input.merge}`]),
    ...input.files.flatMap((file) => [
      `M 100644 inline ${file.path}`,
      `data ${Buffer.byteLength(file.content)}`,
      file.content,
    ]),
  ];
  return `${lines.join("\n")}\n\n`;
};
