import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";

const execute = promisify(execFile);
const runner = path.resolve("node_modules/vitest/vitest.mjs");
const reporter = path.resolve("tools/release-test-reporter.ts");
const vitest = path.resolve("node_modules/vitest/dist/index.js");

const unhandled =
  'it("executes", async () => { Promise.reject(new Error("fixture unhandled")); await new Promise(resolve => setImmediate(resolve)); });';
const scenarios = {
  passed: 'it("executes", () => {});',
  skipped: 'it.skip("missing prerequisite", () => {});',
  todo: 'it.todo("unfinished");',
  dynamic: 'it("missing at runtime", (context) => context.skip());',
  mixed: 'it("executes", () => {}); it.skip("missing prerequisite", () => {});',
  empty: "",
  unhandled,
  "ignored-unhandled": unhandled,
  "after-all":
    'it("executes", () => {}); afterAll(() => { throw new Error("fixture teardown failure"); });',
};
for (const [scenario, body] of Object.entries(scenarios))
  it(`release coverage evaluates execution and runtime errors: ${scenario}`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "release-coverage-"));
    try {
      await fs.writeFile(
        path.join(root, "vite.config.mts"),
        `export default { test: { maxWorkers: 1, dangerouslyIgnoreUnhandledErrors: ${scenario === "ignored-unhandled"} } };\n`,
      );
      await fs.writeFile(
        path.join(root, "fixture.test.ts"),
        `import { it, afterAll } from ${JSON.stringify(vitest)};\n${body}\n`,
      );
      const result = await execute(
        process.execPath,
        [
          runner,
          "run",
          "--root",
          root,
          "--config",
          path.join(root, "vite.config.mts"),
          "--reporter",
          reporter,
        ],
        {
          cwd: root,
          timeout: 30_000,
        },
      ).then(
        ({ stdout, stderr }) => ({ passed: true, output: stdout + stderr }),
        (error: Error & { stdout?: string; stderr?: string }) => ({
          passed: false,
          output: `${error.stdout ?? ""}${error.stderr ?? ""}`,
        }),
      );
      assert.equal(result.passed, scenario === "passed", result.output);
      if (scenario !== "passed") assert.doesNotMatch(result.output, /Release coverage verified/);
      if (scenario === "unhandled" || scenario === "ignored-unhandled")
        assert.match(result.output, /1 unhandled runtime error\(s\)/);
      if (scenario === "after-all") assert.match(result.output, /test run ended: failed/);
      assert.match(
        result.output,
        scenario === "passed"
          ? /Release coverage verified/
          : /release verification requires every non-exempt test to pass/,
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
