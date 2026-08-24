import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

describe("the source tree", () => {
  it.effect("spells control characters rather than embedding them", () =>
    Effect.sync(() => {
      // A NUL byte in a string literal — the separator this codebase uses to key
      // memos on several values at once — is invisible in an editor and changes
      // what the file *is*: grep, ripgrep and `git grep` all classify a file
      // holding one as binary and skip it, so the file silently drops out of
      // every search anybody makes across the codebase, including the searches a
      // reviewer makes to find the very code that put it there. The escape reads
      // the same to the compiler and leaves the file text.
      const listed = execFileSync("git", [
        "ls-files",
        "-z",
        "*.ts",
        "*.tsx",
        "*.js",
        "*.json",
        "*.md",
      ]).toString();
      const files = listed.split("\u0000").filter((path) => path.length > 0 && existsSync(path));

      const offending = files.filter((path) => readFileSync(path).includes(0));
      assert.deepEqual(offending, [], "these files hold a raw control byte and are unsearchable");
    }),
  );
});
