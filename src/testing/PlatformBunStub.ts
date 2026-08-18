/**
 * Stands in for `@effect/platform-bun/*` when wrangler bundles the test
 * Worker.
 *
 * The `worker` export condition resolves alchemy to its TypeScript source,
 * whose `Util/PlatformServices.ts` holds `import("@effect/platform-bun/…")`
 * behind an `isBun` check. workerd never takes that branch — it is served by
 * `@effect/platform-node` — but esbuild resolves dynamic imports eagerly, so
 * the specifiers must exist on disk. `wrangler.test.json` aliases all four
 * here rather than carrying the real package as a dependency nothing runs.
 */
export {};
