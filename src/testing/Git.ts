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
    });
