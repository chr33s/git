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
          // Real `git`, Chromium and a bundler all appear in these suites, and
          // the budget is sized for the slowest machine rather than the
          // fastest. Spawning `git` costs ~84ms on macOS against ~5ms on a
          // Linux CI box, and these suites spawn it in the hundreds —
          // `Bisect.test.ts` runs 21s on its own before any other file is
          // competing for a core. The worst single test observed under a full
          // parallel run was ~60s, so this leaves roughly half as much again.
          //
          // Nothing here waits on a network or a lock, so a test that reaches
          // this limit has genuinely hung, and the cost of noticing that
          // slightly later is worth a suite that does not fail by machine.
          testTimeout: 90_000,
          hookTimeout: 90_000,
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
