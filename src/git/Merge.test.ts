import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { diff3, mergeText, mergeTrees } from "./Merge.ts";
import type { Oid } from "./Store.ts";

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

  it("keeps a file that ends without a newline ending without one", () => {
    const merged = mergeText({
      base: "one\ntwo",
      ours: "ONE\ntwo",
      theirs: "one\ntwo",
    });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, "ONE\ntwo");
  });

  it("does not restore a trailing newline the side that changed it removed", () => {
    // Ours edited the last line and dropped the final newline; theirs edited
    // the first line and left the ending alone. `git merge-file --diff3` on
    // these three writes "A\nb\nC" with no trailing newline — an OR over all
    // three inputs sees the base's newline and puts one back, so the merge
    // commit's blob differs from git's for the same three inputs.
    const merged = mergeText({ base: "a\nb\nc\n", ours: "a\nb\nC", theirs: "A\nb\nc\n" });

    assert.equal(merged.conflicted, false);
    assert.equal(merged.content, "A\nb\nC");
  });

  it("merges a file that begins with a byte-order mark", async () => {
    const bom = "﻿";
    const oid = (suffix: string) => suffix.repeat(40).slice(0, 40) as Oid;
    const encoder = new TextEncoder();
    const content = new Map([
      [oid("1"), encoder.encode(`${bom}one\ntwo\nthree\n`)],
      [oid("2"), encoder.encode(`${bom}ONE\ntwo\nthree\n`)],
      [oid("3"), encoder.encode(`${bom}one\ntwo\nTHREE\n`)],
    ]);

    // A decoder that eats the BOM never encodes it back, so the round-trip
    // guard came up three bytes short and called every Windows-authored file
    // binary — unmergeable, forever.
    const merged = await Effect.runPromise(
      mergeTrees({
        base: new Map([["readme.md", { mode: "100644", oid: oid("1") }]]),
        ours: new Map([["readme.md", { mode: "100644", oid: oid("2") }]]),
        theirs: new Map([["readme.md", { mode: "100644", oid: oid("3") }]]),
        read: (wanted) => Effect.succeed(content.get(wanted)!),
      }) as unknown as Effect.Effect<{
        changes: ReadonlyArray<{ content: Uint8Array | null }>;
        conflicts: ReadonlyArray<{ reason: string }>;
      }>,
    );

    assert.deepEqual(merged.conflicts, []);
    // Compared as bytes: a default `TextDecoder` eats a leading BOM, so
    // reading the result back as text is exactly the mistake under test.
    assert.deepEqual(
      [...(merged.changes[0]?.content ?? [])],
      [...encoder.encode(`${bom}ONE\ntwo\nTHREE\n`)],
    );
  });

  it("carries a submodule across instead of reading it as a file", async () => {
    const oid = (digit: string) => digit.repeat(40) as Oid;
    const side = (at: string) => new Map([["vendor", { mode: "160000", oid: oid(at) }]]);

    // The commit a gitlink names lives in another repository, so `read` here
    // fails the way the real object store does.
    const read = () => Effect.fail(new Error("no such object") as never);

    const moved = await Effect.runPromise(
      mergeTrees({
        base: side("1"),
        ours: side("1"),
        theirs: side("2"),
        read,
      }) as unknown as Effect.Effect<{
        changes: ReadonlyArray<{ oid?: string; mode?: string; content: Uint8Array | null }>;
        conflicts: ReadonlyArray<{ reason: string }>;
      }>,
    );

    assert.deepEqual(moved.conflicts, []);
    assert.equal(moved.changes[0]?.oid, oid("2"));
    assert.equal(moved.changes[0]?.mode, "160000");
    assert.equal(moved.changes[0]?.content, null);

    // Both sides moving it is a question only a person can answer.
    const both = await Effect.runPromise(
      mergeTrees({
        base: side("1"),
        ours: side("2"),
        theirs: side("3"),
        read,
      }) as unknown as Effect.Effect<{ conflicts: ReadonlyArray<{ reason: string }> }>,
    );
    assert.deepEqual(
      both.conflicts.map((conflict) => conflict.reason),
      ["binary"],
    );
  });

  it("reports one conflict for a path whose content and mode both disagree", async () => {
    const oid = (digit: string) => digit.repeat(40) as Oid;
    const content = new Map([
      [oid("1"), new TextEncoder().encode("one\n")],
      [oid("2"), new TextEncoder().encode("OUR\n")],
      [oid("3"), new TextEncoder().encode("THEIR\n")],
    ]);

    // Both sides rewrote the line *and* both changed the mode differently.
    // Two entries for one path made a caller counting conflicts count this
    // file twice, and one resolving them by path meet it again with nothing
    // left to do.
    const merged = await Effect.runPromise(
      mergeTrees({
        base: new Map([["script.sh", { mode: "100644", oid: oid("1") }]]),
        ours: new Map([["script.sh", { mode: "100755", oid: oid("2") }]]),
        theirs: new Map([["script.sh", { mode: "120000", oid: oid("3") }]]),
        read: (wanted) => Effect.succeed(content.get(wanted)!),
      }) as unknown as Effect.Effect<{
        conflicts: ReadonlyArray<{ path: string; reason: string }>;
      }>,
    );

    assert.deepEqual(merged.conflicts, [{ path: "script.sh", reason: "content" }]);
  });

  it("takes a mode change made only on the incoming side", async () => {
    const oid = ("0".repeat(39) + "1") as Oid;
    const side = (mode: string) => new Map([["deploy.sh", { mode, oid }]]);

    // `theirs` ran chmod +x; `ours` changed nothing about the file. Taking
    // ours' mode leaves a deploy script that will not run, and calls the
    // merge clean.
    const merged = await Effect.runPromise(
      mergeTrees({
        base: side("100644"),
        ours: side("100644"),
        theirs: side("100755"),
        read: () => Effect.succeed(new TextEncoder().encode("#!/bin/sh\n")),
      }) as unknown as Effect.Effect<{
        changes: ReadonlyArray<{ mode?: string }>;
        conflicts: ReadonlyArray<unknown>;
      }>,
    );

    assert.deepEqual(merged.conflicts, []);
    assert.equal(merged.changes[0]?.mode, "100755");
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
