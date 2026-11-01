import type { Reporter } from "vitest/node";

// These tests inspect /proc/self/fd. No prerequisite-dependent or root-only
// skips are allowed; CI must provide Git, SSH, Chromium and the SEA artifacts.
const linuxDescriptors = new Set(
  [
    "reads a pack many times through one descriptor",
    "survives more packs read at once than the pool may hold",
    "retires a repository whose objects live behind a symlink",
    "releases a removed pack whose spelling can no longer be resolved",
    "retires and re-checks through a symlinked root too",
    "keys one pack by one descriptor through a symlinked root",
    "reads the pack that is there now, not the one it opened",
    "lets go of a pack another process repacked away",
    "releases a whole repository's descriptors when the directory goes",
    "keys one pack by one descriptor however the root is spelled",
    "gives the descriptor back when the pack is deleted",
  ].map((name) => `pack descriptors > ${name}`),
);

export default class ReleaseTestReporter implements Reporter {
  onTestRunEnd(
    ...[modules, unhandledErrors, reason]: Parameters<NonNullable<Reporter["onTestRunEnd"]>>
  ) {
    const missing: string[] = [];
    if (reason !== "passed") missing.push(`test run ended: ${reason}`);
    if (unhandledErrors.length > 0)
      missing.push(`${unhandledErrors.length} unhandled runtime error(s)`);
    if (process.platform !== "linux" && process.platform !== "darwin")
      missing.push("unqualified test platform");
    let passed = 0;
    let expected = 0;
    for (const module of modules) {
      for (const test of module.children.allTests()) {
        const state = test.result().state;
        if (state === "passed") passed += 1;
        else if (
          state === "skipped" &&
          process.platform === "darwin" &&
          module.relativeModuleId === "src/git/Node.test.ts" &&
          linuxDescriptors.has(test.fullName)
        )
          expected += 1;
        else missing.push(`${module.relativeModuleId}: ${test.fullName} (${state})`);
      }
    }
    if (passed === 0) missing.push("release verification must execute tests");
    if (missing.length > 0) {
      console.error(
        "release verification requires every non-exempt test to pass:\n" + missing.join("\n"),
      );
      process.exitCode = 1;
      return;
    }
    console.info(
      `Release coverage verified: ${passed} passed, ${expected} documented platform skips.`,
    );
  }
}
