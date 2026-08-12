import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { diff3, mergeText } from "./Merge.ts";

const text = (lines: ReadonlyArray<string>) => `${lines.join("\n")}\n`;

const base = text(["one", "two", "three", "four", "five"]);

describe("Merge", () => {
  it("takes the side that changed when the other side kept the base", () => {
    const ours = base.replace("two", "TWO");
    const theirs = base.replace("five", "FIVE");

    const forward = mergeText({ base, ours, theirs });
    assert.equal(forward.conflicted, false);
    assert.equal(forward.content, text(["one", "TWO", "three", "four", "FIVE"]));

    // Symmetric: swapping the sides merges to the same file.
    const swapped = mergeText({ base, ours: theirs, theirs: ours });
    assert.equal(swapped.content, forward.content);
  });

  it("carries an insertion from one side through untouched", () => {
    const ours = base.replace("two\n", "two\ntwo and a half\n");
    const merged = mergeText({ base, ours, theirs: base.replace("four", "FOUR") });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, text(["one", "two", "two and a half", "three", "FOUR", "five"]));
  });

  it("keeps one copy when both sides made the identical change", () => {
    const changed = base.replace("three", "THREE");
    const merged = mergeText({ base, ours: changed, theirs: changed });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, changed);
  });

  it("conflicts when both sides rewrote the same region, in diff3 marker order", () => {
    const merged = mergeText({
      base,
      ours: base.replace("three", "our three"),
      theirs: base.replace("three", "their three"),
    });

    assert.equal(merged.conflicted, true);
    assert.equal(
      merged.content,
      text([
        "one",
        "two",
        "<<<<<<< ours",
        "our three",
        "||||||| base",
        "three",
        "=======",
        "their three",
        ">>>>>>> theirs",
        "four",
        "five",
      ]),
    );
  });

  it("labels the conflict markers with the refs the caller names", () => {
    const merged = mergeText({
      base,
      ours: base.replace("one", "ours one"),
      theirs: base.replace("one", "theirs one"),
      labels: { ours: "HEAD", base: "merged common ancestors", theirs: "topic" },
    });

    const markers = merged.content.split("\n").filter((line) => /^[<|=>]{7}/.test(line));
    assert.deepEqual(markers, [
      "<<<<<<< HEAD",
      "||||||| merged common ancestors",
      "=======",
      ">>>>>>> topic",
    ]);
  });

  it("resolves a conflict by picking a side under the ours and theirs strategies", () => {
    const input = {
      base,
      ours: base.replace("three", "our three"),
      theirs: base.replace("three", "their three"),
    };

    const ours = mergeText({ ...input, strategy: "ours" });
    assert.equal(ours.conflicted, false);
    assert.equal(ours.content, text(["one", "two", "our three", "four", "five"]));

    const theirs = mergeText({ ...input, strategy: "theirs" });
    assert.equal(theirs.conflicted, false);
    assert.equal(theirs.content, text(["one", "two", "their three", "four", "five"]));

    // A strategy only decides conflicts; clean regions still merge normally.
    const clean = mergeText({
      base,
      ours: base.replace("one", "ONE"),
      theirs: base.replace("five", "FIVE"),
      strategy: "ours",
    });
    assert.equal(clean.content, text(["ONE", "two", "three", "four", "FIVE"]));
  });

  it("reports regions, not just text, so a caller can resolve them itself", () => {
    const regions = diff3(
      ["one", "two", "three"],
      ["one", "ours", "three"],
      ["one", "theirs", "three"],
    );

    assert.deepEqual(regions, [
      { ok: true, lines: ["one"] },
      { ok: false, base: ["two"], ours: ["ours"], theirs: ["theirs"] },
      { ok: true, lines: ["three"] },
    ]);
  });

  it("merges a deletion on one side with an edit elsewhere on the other", () => {
    const merged = mergeText({
      base,
      ours: base.replace("two\n", ""),
      theirs: base.replace("four", "FOUR"),
    });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, text(["one", "three", "FOUR", "five"]));
  });

  it("returns the base untouched when neither side changed anything", () => {
    const merged = mergeText({ base, ours: base, theirs: base });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, base);
  });
});
