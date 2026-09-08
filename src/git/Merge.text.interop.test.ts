import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { gitEnv, hasGit } from "../testing/Git.ts";
import { mergeText } from "./Merge.ts";

/**
 * `mergeText` against `git merge-file --diff3`, byte for byte.
 *
 * The labels match the defaults `mergeText` uses so the marker lines are
 * comparable; everything else is git's own answer for the same three inputs.
 */
const cases: ReadonlyArray<{
  readonly name: string;
  readonly base: string;
  readonly ours: string;
  readonly theirs: string;
}> = [
  { name: "clean on both sides", base: "a\nb\nc\n", ours: "A\nb\nc\n", theirs: "a\nb\nC\n" },
  { name: "the same change on both sides", base: "a\n", ours: "b\n", theirs: "b\n" },
  { name: "a conflict", base: "a\n", ours: "b\n", theirs: "c\n" },
  { name: "adjacent lines", base: "a\nb\n", ours: "A\nb\n", theirs: "a\nB\n" },
  { name: "no final newline on one side", base: "a\n", ours: "a\nb", theirs: "a\n" },
  { name: "no final newline on both", base: "a\n", ours: "a\nb", theirs: "a\nc" },
  { name: "an unterminated base", base: "a", ours: "a\nb\n", theirs: "a" },
  { name: "delete against modify", base: "a\nb\nc\n", ours: "a\nc\n", theirs: "a\nB\nc\n" },
  { name: "add/add agreeing", base: "", ours: "x\n", theirs: "x\n" },
  { name: "add/add differing", base: "", ours: "x\n", theirs: "y\n" },
  { name: "three empty files", base: "", ours: "", theirs: "" },
  { name: "our side emptied", base: "a\nb\n", ours: "", theirs: "a\nB\n" },
  { name: "their side emptied", base: "a\nb\n", ours: "a\nB\n", theirs: "" },
  { name: "insertions at both ends", base: "m\n", ours: "top\nm\n", theirs: "m\nbot\n" },
  { name: "a conflict at the end of file", base: "a\nb", ours: "a\nX", theirs: "a\nY" },
  { name: "both sides truncating", base: "a\nb\nc\n", ours: "a\n", theirs: "a\nb\n" },
  {
    name: "one insertion in two places",
    base: "1\n2\n3\n",
    ours: "1\nx\n2\n3\n",
    theirs: "1\n2\nx\n3\n",
  },
  { name: "whitespace-only edits", base: "a\n", ours: " a\n", theirs: "a \n" },
  {
    name: "a multi-line conflict",
    base: "1\n2\n3\n4\n5\n",
    ours: "1\nA\nB\n4\n5\n",
    theirs: "1\nC\nD\n4\n5\n",
  },
  { name: "combining marks", base: "é\n", ours: "é́\n", theirs: "é!\n" },
  // The conflict markers git writes carry the file's own line ending, and it
  // asks the question of all three inputs' first line. Every combination
  // below answered differently at some point during this comparison.
  { name: "CRLF throughout", base: "a\r\nb\r\n", ours: "A\r\nb\r\n", theirs: "a\r\nB\r\n" },
  { name: "CRLF first line only", base: "a\r\nb\n", ours: "A\r\nb\n", theirs: "a\r\nB\n" },
  { name: "CRLF last line only", base: "a\nb\r\n", ours: "A\nb\r\n", theirs: "a\nB\r\n" },
  { name: "CRLF on our side alone", base: "a\nb\n", ours: "A\r\nb\r\n", theirs: "a\nB\n" },
  { name: "CRLF on the base alone", base: "a\r\nb\r\n", ours: "A\nb\n", theirs: "a\nB\n" },
  { name: "CRLF on their side alone", base: "a\nb\n", ours: "A\nb\n", theirs: "a\r\nB\r\n" },
  { name: "CRLF without a final newline", base: "a\r\nb", ours: "A\r\nb", theirs: "a\r\nB" },
  { name: "CRLF against an empty base", base: "", ours: "x\r\n", theirs: "y\r\n" },
  {
    name: "CRLF merging cleanly",
    base: "a\r\nb\r\nc\r\n",
    ours: "A\r\nb\r\nc\r\n",
    theirs: "a\r\nb\r\nC\r\n",
  },
  {
    name: "CRLF conflicting at the end of file",
    base: "a\r\nb\r\n",
    ours: "a\r\nX\r\n",
    theirs: "a\r\nY\r\n",
  },
  { name: "single lines with no newline at all", base: "a", ours: "b", theirs: "c" },
];

describe.skipIf(!hasGit)("three-way text merge against Git", () => {
  it.live("matches git merge-file --diff3 byte for byte", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "merge-text-"));
      try {
        for (const one of cases) {
          const files = ["ours", "base", "theirs"].map((name) => path.join(root, name));
          await fs.writeFile(files[0]!, one.ours);
          await fs.writeFile(files[1]!, one.base);
          await fs.writeFile(files[2]!, one.theirs);
          let expected: string;
          let conflicted: boolean;
          try {
            expected = execFileSync(
              "git",
              ["merge-file", "--diff3", "-p", "-L", "ours", "-L", "base", "-L", "theirs", ...files],
              { encoding: "utf8", env: gitEnv },
            );
            conflicted = false;
          } catch (error) {
            // A nonzero exit is the count of conflicts, and the merged text
            // with markers still comes back on stdout.
            // SAFETY: `execFileSync` rejects with an `Error` carrying the
            // child's `stdout` and `status`; both are read defensively below.
            const failure = error as { readonly stdout?: string; readonly status?: number };
            expected = failure.stdout ?? "";
            conflicted = (failure.status ?? 0) > 0;
          }
          const merged = mergeText({ base: one.base, ours: one.ours, theirs: one.theirs });
          assert.equal(merged.content, expected, `${one.name}: merged text`);
          assert.equal(merged.conflicted, conflicted, `${one.name}: conflict verdict`);
        }
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
