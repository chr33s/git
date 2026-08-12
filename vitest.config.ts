/**
 * Integration tests run inside workerd, not against a mock.
 *
 * `cloudflareTest` boots the Worker defined by `wrangler.test.json` under
 * Miniflare and runs the test files in the same isolate, so a test can reach
 * into a Durable Object and use the real R2 and SQLite bindings. That is what
 * makes it possible to run the storage contract against the Cloudflare backend
 * rather than trusting that it behaves like the in-memory one.
 */
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./wrangler.test.json" } })],
  test: {
    // `.spec.ts`, deliberately: node's built-in runner claims `*.test.ts` by
    // default, and these files only run inside workerd. One pattern per runner
    // keeps `npm test` and `npm run test:integration` from tripping over each
    // other.
    include: ["src/**/*.spec.ts"],
  },
});
