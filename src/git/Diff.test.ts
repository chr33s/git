import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { diffLines, isBinary, lcs, similarity, splitLines, unified } from "./Diff.ts";
import { mergeText } from "./Merge.ts";

const numbered = (count: number, from = 1) =>
  Array.from({ length: count }, (_, index) => `line ${index + from}`);

const text = (lines: ReadonlyArray<string>) => `${lines.join("\n")}\n`;

describe("Diff", () => {
  it("matches the longest common subsequence, not the first one it finds", () => {
    // Myers' own example: the greedy prefix match ("A") is a dead end, the
    // longest run is 4 lines.
    const a = "ABCABBA".split("");
    const b = "CBABAC".split("");
    const pairs = lcs(a, b);

    assert.equal(pairs.length, 4);
    let previousA = -1;
    let previousB = -1;
    for (const [indexA, indexB] of pairs) {
      assert.equal(a[indexA], b[indexB]);
      assert.ok(indexA > previousA && indexB > previousB, "pairs must ascend on both sides");
      previousA = indexA;
      previousB = indexB;
    }
  });

  it("keeps a long unchanged file entirely matched", () => {
    const lines = numbered(500);
    assert.equal(lcs(lines, lines).length, 500);
    assert.deepEqual(diffLines(lines, lines), []);
  });

  it("still matches the untouched ends of a file whose middle is past the search bound", () => {
    // The Myers search is bounded by edit distance, and this rewrite is well
    // past it: 4,000 replaced lines is an edit distance of 8,000 against a
    // bound of 2,000. Giving up used to mean returning *no* matches, which
    // told every caller the two files had nothing in common — a 100-line
    // append came back as a whole-file rewrite, and a merge of two edits at
    // opposite ends became a whole-file conflict.
    const head = numbered(200);
    const tail = numbered(200).map((line) => `tail ${line}`);
    const before = [...head, ...numbered(4_000).map((line) => `old ${line}`), ...tail];
    const after = [...head, ...numbered(4_000).map((line) => `new ${line}`), ...tail];

    const matched = lcs(before, after);
    assert.equal(matched.length, 400);
    assert.deepEqual(matched[0], [0, 0]);
    assert.deepEqual(matched.at(-1), [before.length - 1, after.length - 1]);

    // And the diff is the middle, not the file.
    const hunks = diffLines(before, after);
    assert.equal(hunks.length, 1);
    assert.equal(hunks[0]?.oldStart, 200);
    assert.equal(hunks[0]?.oldLines.length, 4_000);
  });

  it("merges edits at opposite ends of a file too different to diff in the middle", () => {
    // The same bound, reached through `mergeText`: ours rewrites the middle,
    // theirs appends at the end, and the two do not overlap. git merges this.
    const middle = (mark: string) => numbered(3_000).map((line) => `${mark} ${line}`);
    const base = `${["head", ...middle("old"), "tail"].join("\n")}\n`;
    const ours = `${["head", ...middle("new"), "tail"].join("\n")}\n`;
    const theirs = `${["head", ...middle("old"), "tail", "appended"].join("\n")}\n`;

    const merged = mergeText({ base, ours, theirs });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, `${["head", ...middle("new"), "tail", "appended"].join("\n")}\n`);
  });

  it("groups a replacement and a trailing insertion into separate hunks", () => {
    const hunks = diffLines(["a", "b", "c", "d"], ["a", "x", "c", "d", "e"]);

    assert.deepEqual(hunks, [
      { oldStart: 1, oldLines: ["b"], newStart: 1, newLines: ["x"] },
      { oldStart: 4, oldLines: [], newStart: 4, newLines: ["e"] },
    ]);
  });

  it("renders a unified diff with git's header, range and three lines of context", () => {
    const before = text(numbered(10));
    const after = before.replace("line 5", "LINE 5");

    assert.equal(
      unified(before, after, { beforeName: "src/app.ts", afterName: "src/app.ts" }),
      text([
        "--- a/src/app.ts",
        "+++ b/src/app.ts",
        "@@ -2,7 +2,7 @@",
        " line 2",
        " line 3",
        " line 4",
        "-line 5",
        "+LINE 5",
        " line 6",
        " line 7",
        " line 8",
      ]),
    );
  });

  it("splits changes further apart than twice the context, and merges nearer ones", () => {
    const before = text(numbered(20));
    const far = before.replace("line 2\n", "line 2 edited\n").replace("line 18", "line 18 edited");
    const near = before.replace("line 2\n", "line 2 edited\n").replace("line 6", "line 6 edited");

    const headers = (diff: string) => diff.split("\n").filter((line) => line.startsWith("@@"));

    assert.deepEqual(headers(unified(before, far)), ["@@ -1,5 +1,5 @@", "@@ -15,6 +15,6 @@"]);
    assert.deepEqual(headers(unified(before, near)), ["@@ -1,9 +1,9 @@"]);
    // Nothing is printed twice, whichever way the hunks fell.
    assert.equal(
      unified(before, far)
        .split("\n")
        .filter((line) => line === " line 15").length,
      1,
    );
  });

  it("reports no diff at all for identical text", () => {
    assert.equal(unified("same\n", "same\n"), "");
    assert.equal(unified("", ""), "");
  });

  it("uses a zero-length old range when the file is created from nothing", () => {
    assert.equal(
      unified("", text(["alpha", "beta"]), { beforeName: "new.txt" }),
      text(["--- a/new.txt", "+++ b/new.txt", "@@ -0,0 +1,2 @@", "+alpha", "+beta"]),
    );
  });

  it("marks a missing final newline on the side that is missing it", () => {
    const diff = unified("one\ntwo\n", "one\ntwo");

    assert.equal(
      diff,
      text([
        "--- a/file",
        "+++ b/file",
        "@@ -1,2 +1,2 @@",
        " one",
        "-two",
        "+two",
        "\\ No newline at end of file",
      ]),
    );
  });

  it("treats a trailing newline as a terminator rather than a separator", () => {
    assert.deepEqual(splitLines("a\nb\n"), ["a", "b"]);
    assert.deepEqual(splitLines("a\nb"), ["a", "b"]);
    assert.deepEqual(splitLines(""), []);
    assert.deepEqual(splitLines("\n"), [""]);
    assert.deepEqual(splitLines("a\n\nb\n"), ["a", "", "b"]);
    // CRLF text keeps its \r, exactly as git stores it.
    assert.deepEqual(splitLines("a\r\nb\r\n"), ["a\r", "b\r"]);
  });

  it("calls a file binary only for a NUL inside the first 8000 bytes", () => {
    const clean = new Uint8Array(20_000).fill(0x61);

    assert.equal(isBinary(clean), false);
    assert.equal(isBinary(new TextEncoder().encode("plain text\n")), false);

    const early = clean.slice();
    early[7999] = 0;
    assert.equal(isBinary(early), true);

    const late = clean.slice();
    late[8000] = 0;
    assert.equal(isBinary(late), false);
  });

  it("scores a renamed-and-edited file far above an unrelated one", () => {
    const original = text(numbered(10));
    const edited = original.replace("line 5", "line five");
    const unrelated = text(numbered(10, 100).map((line) => line.replace("line", "totally other")));

    assert.equal(similarity(original, original), 1);
    assert.equal(similarity(original, edited), 0.9);
    assert.equal(similarity(original, unrelated), 0);
    // Symmetric, so rename detection does not depend on which side it starts from.
    assert.equal(similarity(edited, original), similarity(original, edited));
  });
});
