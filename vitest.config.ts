import { defineConfig } from "vitest/config";

/**
 * Two projects, because the two suites cost different things.
 *
 * `unit` is everything that runs in this process — and it runs in parallel
 * now: `node --test --test-concurrency=1` was serial because the suites
 * shared global state, and layers removed that reason.
 *
 * `integration` boots workerd through wrangler's `createTestHarness`, so it
 * is single-file and patient. It runs under `npm test` alongside the unit
 * project; `--project unit` is there for the fast half during development.
 */
/**
 * The suites drive the real `git` binary, and a developer's own config must
 * not decide whether they pass: `commit.gpgsign` with an SSH key turns every
 * `git commit` in the suite into a passphrase prompt that no non-interactive
 * run can answer. Pointing git's config lookup at a file that does not exist
 * is how it is told to consult nothing, and it reaches every child process
 * rather than only the ones that go through `testing/Git.ts`.
 */
const gitConfig = {
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_SYSTEM: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
};

export default defineConfig({
  test: {
    env: gitConfig,
    projects: [
      {
        test: {
          name: "unit",
          env: gitConfig,
          include: ["src/**/*.test.ts"],
          // Real `git`, Chromium and a bundler all appear in these suites,
          // and the git-heavy ones (PackFile, Bisect) run dozens of child
          // processes each. Fine alone, but the parallel run oversubscribes
          // the CPU by ~10x, so the allowance is for a loaded machine, not
          // an idle one.
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
      {
        test: {
          name: "integration",
          env: gitConfig,
          include: ["src/**/*.integration.ts"],
          // One workerd instance, shared by the file's tests.
          fileParallelism: false,
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
    ],
  },
});
