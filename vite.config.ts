import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import effectRecommended from "@effect/tsgo/oxlint-presets/recommended.json" with { type: "json" };
import { defineConfig } from "vite-plus";

const effectRules = Object.fromEntries(
  Object.entries(effectRecommended.rules).map(
    ([name, severity]): readonly [string, "error" | "warn"] => [
      name,
      severity === "error" ? "error" : "warn",
    ],
  ),
);

/** Keep developer Git configuration out of subprocess-backed test results. */
const gitConfig = {
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_SYSTEM: "/dev/null",
};

const root = dirname(fileURLToPath(import.meta.url));
const ui = join(root, "src", "ui");

/**
 * `@pierre/diffs` registers its web component from a file its exports map does
 * not expose. Keep the workaround in one resolver until the package exports
 * the side-effect entry itself.
 */
const diffsWebComponents = join(
  dirname(fileURLToPath(import.meta.resolve("@pierre/diffs"))),
  "components",
  "web-components.js",
);

export default defineConfig(({ mode }) => ({
  appType: "custom",
  base: "./",
  build: {
    cssCodeSplit: false,
    emptyOutDir: true,
    minify: mode !== "development" ? "oxc" : false,
    outDir: join(root, "dist", "ui"),
    rolldownOptions: {
      output: {
        assetFileNames: (asset) =>
          asset.names.some((name) => name.endsWith(".css")) ? "main.css" : "[name]-[hash][extname]",
        chunkFileNames: "[name]-[hash].js",
        entryFileNames: "main.js",
      },
    },
    sourcemap: true,
    target: "es2022",
  },
  define: {
    "process.env.NODE_ENV": mode === "development" ? '"development"' : '"production"',
  },
  fmt: { ignorePatterns: [] },
  lint: {
    jsPlugins: [{ name: "anti-slop", specifier: "./tools/oxlint/anti-slop/index.ts" }],
    options: { typeAware: true, typeCheck: true },
    plugins: ["unicorn", "typescript", "oxc", "effecttsgo"],
    overrides: [
      {
        files: ["src/**/*.test.ts", "src/**/*.integration.ts"],
        rules: { "typescript/no-floating-promises": "off" },
      },
    ],
    rules: {
      ...effectRules,
      // Effect rule overrides
      "effecttsgo/async-function": "off",
      "effecttsgo/crypto-random-uuid": "off",
      "effecttsgo/crypto-random-uuid-in-effect": "off",
      "effecttsgo/global-console": "off",
      "effecttsgo/global-console-in-effect": "off",
      "effecttsgo/global-date": "off",
      "effecttsgo/global-date-in-effect": "off",
      "effecttsgo/global-fetch": "off",
      "effecttsgo/global-fetch-in-effect": "off",
      "effecttsgo/global-random": "off",
      "effecttsgo/global-random-in-effect": "off",
      "effecttsgo/global-timers": "off",
      "effecttsgo/global-timers-in-effect": "off",
      "effecttsgo/new-promise": "off",
      "effecttsgo/node-builtin-import": "off",
      "effecttsgo/prefer-schema-over-json": "off",
      "effecttsgo/process-env": "off",
      "effecttsgo/process-env-in-effect": "off",
      // Anti-slop rules
      "anti-slop/no-chained-type-assertions": "error",
      "anti-slop/no-conditional-empty-object-spread": "error",
      "anti-slop/no-known-value-widening": "error",
      "anti-slop/no-module-mocking": "error",
      "anti-slop/no-object-parameters": "error",
      "anti-slop/no-reflect-apply": "error",
      "anti-slop/no-reflect-get": "error",
      "anti-slop/no-runtime-typeof": "error",
      "anti-slop/no-shape-in-symbol-names": "error",
      "anti-slop/no-unknown-parameters": "error",
      "anti-slop/no-unknown-returns": "error",
      "anti-slop/no-unknown-type-aliases": "error",
      "anti-slop/no-unsafe-dictionary-type": "error",
      "anti-slop/no-widen-then-assert": "error",
      "anti-slop/require-safety-comment-for-type-assertion": "error",
    },
  },
  resolve: {
    alias: { "@pierre/diffs/components/web-components": diffsWebComponents },
  },
  // Test projects resolve `src/**` from the repository; the UI build resolves
  // its HTML entry from `src/ui`.
  root: mode === "test" ? root : ui,
  test: {
    env: gitConfig,
    projects: [
      {
        test: {
          // Real `git`, Chromium and a bundler all appear here. The suites
          // run in parallel, so this is the allowance for a loaded machine.
          exclude: ["src/**/*.interop.test.ts"],
          env: gitConfig,
          hookTimeout: 120_000,
          include: ["src/**/*.test.ts"],
          name: "unit",
          testTimeout: 120_000,
        },
      },
      {
        test: {
          // One workerd instance, shared by this file's tests.
          env: gitConfig,
          fileParallelism: false,
          hookTimeout: 120_000,
          include: ["src/**/*.integration.ts"],
          name: "integration",
          testTimeout: 120_000,
        },
      },
      {
        test: {
          env: gitConfig,
          fileParallelism: false,
          hookTimeout: 120_000,
          include: ["src/**/*.interop.test.ts"],
          name: "interop",
          testTimeout: 120_000,
        },
      },
    ],
  },
}));
