import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { gitEnv, hasGit } from "../testing/Git.ts";
import { unified } from "./Diff.ts";

describe.skipIf(!hasGit)("unified diff paths", () => {
  for (const name of [
    "with\ttab",
    "with\nnewline",
    "with\rreturn",
    'with"quote',
    "with\\slash",
    "with space",
    "é.txt",
    "with\x01control",
  ]) {
    it.effect(`matches Git headers and applies the patch for ${JSON.stringify(name)}`, () =>
      Effect.sync(() => {
        const directory = mkdtempSync(join(tmpdir(), "diff-path-"));
        try {
          mkdirSync(join(directory, "a"));
          mkdirSync(join(directory, "b"));
          const before = "before\nshared\n";
          const after = "after\nshared\n";
          writeFileSync(join(directory, "a", name), before);
          writeFileSync(join(directory, "b", name), after);
          const reference = spawnSync(
            "git",
            ["diff", "--no-index", "--no-prefix", "--", `a/${name}`, `b/${name}`],
            { cwd: directory, env: gitEnv, encoding: "utf8" },
          );
          assert.equal(reference.status, 1, reference.stderr);
          const patch = unified(before, after, { beforeName: name });
          const headers = (value: string) =>
            value.split("\n").filter((line) => line.startsWith("--- ") || line.startsWith("+++ "));
          assert.deepEqual(headers(patch), headers(reference.stdout));
          writeFileSync(join(directory, name), before);
          const applied = spawnSync("git", ["apply", "-"], {
            cwd: directory,
            env: gitEnv,
            input: patch,
            encoding: "utf8",
          });
          assert.equal(applied.status, 0, applied.stderr);
          assert.equal(readFileSync(join(directory, name), "utf8"), after);
        } finally {
          rmSync(directory, { recursive: true, force: true });
        }
      }),
    );
  }
});
