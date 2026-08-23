/** Stock-Git baseline and manifest integrity checks. */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { coreCompatibility, gitCompatibilityBaseline, manifestProblems } from "./GitCompat.ts";
import { hasGit } from "../testing/Git.ts";
import { runProcess } from "../testing/Process.ts";

const gitVersion = async () => {
  const result = await runProcess({ command: "git", args: ["--version"] });
  return new TextDecoder().decode(result.stdout).trim();
};

describe.skipIf(!hasGit)("core Git compatibility baseline", () => {
  it("runs against the pinned Git release", async () => {
    assert.equal(await gitVersion(), `git version ${gitCompatibilityBaseline}`);
  });

  it("declares every core command with coverage metadata", () => {
    assert.deepEqual(Object.keys(coreCompatibility.commands).sort(), [
      "add",
      "archive",
      "bisect",
      "branch",
      "cherry-pick",
      "clone",
      "commit",
      "diff",
      "fetch",
      "fsck",
      "gc",
      "grep",
      "init",
      "log",
      "merge",
      "mv",
      "pull",
      "push",
      "rebase",
      "reflog",
      "remote",
      "reset",
      "restore",
      "rm",
      "show",
      "status",
      "switch",
      "tag",
    ]);
    assert.deepEqual(manifestProblems(coreCompatibility), []);
  });
});
