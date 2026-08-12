import { defineConfig } from "vitest/config";

/**
 * Two projects, because the two suites cost different things.
 *
 * `unit` is everything that runs in this process — and it runs in parallel
 * now: `node --test --test-concurrency=1` was serial because the suites
 * shared global state, and layers removed that reason.
 *
 * `integration` boots workerd through wrangler's `createTestHarness`, so it
 * is opt-in (`npm run test:integration`), single-file, and patient.
 */
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "unit",
          include: ["src/**/*.test.ts"],
          // Real `git`, Chromium and a bundler all appear in these suites.
          testTimeout: 30_000,
          hookTimeout: 30_000,
        },
      },
      {
        test: {
          name: "integration",
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
