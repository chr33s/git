import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { diff3, mergeText, mergeTrees } from "./Merge.ts";
import type { Oid } from "./Store.ts";

const text = (lines: ReadonlyArray<string>) => `${lines.join("\n")}\n`;

const base = text(["one", "two", "three", "four", "five"]);

describe("Merge", () => {
  it.effect("takes the side that changed when the other side kept the base", () =>
    Effect.sync(() => {
      const ours = base.replace("two", "TWO");
      const theirs = base.replace("five", "FIVE");

      const forward = mergeText({ base, ours, theirs });
      assert.equal(forward.conflicted, false);
      assert.equal(forward.content, text(["one", "TWO", "three", "four", "FIVE"]));

      // Symmetric: swapping the sides merges to the same file.
      const swapped = mergeText({ base, ours: theirs, theirs: ours });
      assert.equal(swapped.content, forward.content);
    }),
  );

  it.effect("keeps a file that ends without a newline ending without one", () =>
    Effect.sync(() => {
      const merged = mergeText({
        base: "one\ntwo",
        ours: "ONE\ntwo",
        theirs: "one\ntwo",
      });

      assert.equal(merged.conflicted, false);
      assert.equal(merged.content, "ONE\ntwo");
    }),
  );

  it.effect("does not restore a trailing newline the side that changed it removed", () =>
    Effect.sync(() => {
      // Ours edited the last line and dropped the final newline; theirs edited
      // the first line and left the ending alone. `git merge-file --diff3` on
      // these three writes "A\nb\nC" with no trailing newline — an OR over all
      // three inputs sees the base's newline and puts one back, so the merge
      // commit's blob differs from git's for the same three inputs.
      const merged = mergeText({ base: "a\nb\nc\n", ours: "a\nb\nC", theirs: "A\nb\nc\n" });

      assert.equal(merged.conflicted, false);
      assert.equal(merged.content, "A\nb\nC");
    }),
  );

  it.effect("merges a file that begins with a byte-order mark", () =>
    Effect.promise(async () => {
      const bom = "﻿";
      // SAFETY: forty characters cut from a repeated hex digit, which is what
      // the Oid brand stands for.
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
        }),
      );

      assert.deepEqual(merged.conflicts, []);
      // Compared as bytes: a default `TextDecoder` eats a leading BOM, so
      // reading the result back as text is exactly the mistake under test.
      assert.deepEqual(
        [...(merged.changes[0]?.content ?? [])],
        [...encoder.encode(`${bom}ONE\ntwo\nTHREE\n`)],
      );
    }),
  );

  it.effect("carries a submodule across instead of reading it as a file", () =>
    Effect.promise(async () => {
      // SAFETY: forty lowercase hex characters by construction, which is exactly
      // what the Oid brand stands for.
      const oid = (digit: string) => digit.repeat(40) as Oid;
      const side = (at: string) => new Map([["vendor", { mode: "160000", oid: oid(at) }]]);

      // SAFETY: the commit a gitlink names lives in another repository, so
      // `read` fails here the way the real object store does; the error channel
      // is `never` because no caller in this test is meant to recover from it.
      const read = () => Effect.fail(new Error("no such object") as never);

      const moved = await Effect.runPromise(
        mergeTrees({
          base: side("1"),
          ours: side("1"),
          theirs: side("2"),
          read,
        }),
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
        }),
      );
      assert.deepEqual(
        both.conflicts.map((conflict) => conflict.reason),
        ["binary"],
      );
    }),
  );

  it.effect("reports one conflict for a path whose content and mode both disagree", () =>
    Effect.promise(async () => {
      // SAFETY: forty lowercase hex characters by construction, which is exactly
      // what the Oid brand stands for.
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
        }),
      );

      assert.deepEqual(merged.conflicts, [{ path: "script.sh", reason: "content" }]);
    }),
  );

  it.effect("takes a mode change made only on the incoming side", () =>
    Effect.promise(async () => {
      // SAFETY: forty lowercase hex characters by construction, which is exactly
      // what the Oid brand stands for.
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
        }),
      );

      assert.deepEqual(merged.conflicts, []);
      assert.equal(merged.changes[0]?.mode, "100755");
    }),
  );

  it.effect("carries an insertion from one side through untouched", () =>
    Effect.sync(() => {
      const ours = base.replace("two\n", "two\ntwo and a half\n");
      const merged = mergeText({ base, ours, theirs: base.replace("four", "FOUR") });

      assert.equal(merged.conflicted, false);
      assert.equal(merged.content, text(["one", "two", "two and a half", "three", "FOUR", "five"]));
    }),
  );

  it.effect("keeps one copy when both sides made the identical change", () =>
    Effect.sync(() => {
      const changed = base.replace("three", "THREE");
      const merged = mergeText({ base, ours: changed, theirs: changed });

      assert.equal(merged.conflicted, false);
      assert.equal(merged.content, changed);
    }),
  );

  it.effect("conflicts when both sides rewrote the same region, in diff3 marker order", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("labels the conflict markers with the refs the caller names", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("resolves a conflict by picking a side under the ours and theirs strategies", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("reports regions, not just text, so a caller can resolve them itself", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("merges a deletion on one side with an edit elsewhere on the other", () =>
    Effect.sync(() => {
      const merged = mergeText({
        base,
        ours: base.replace("two\n", ""),
        theirs: base.replace("four", "FOUR"),
      });

      assert.equal(merged.conflicted, false);
      assert.equal(merged.content, text(["one", "three", "FOUR", "five"]));
    }),
  );

  it.effect("returns the base untouched when neither side changed anything", () =>
    Effect.sync(() => {
      const merged = mergeText({ base, ours: base, theirs: base });

      assert.equal(merged.conflicted, false);
      assert.equal(merged.content, base);
    }),
  );

  it.effect("reads a zero-padded mode as the mode it is, not as a change", () =>
    Effect.promise(async () => {
      // SAFETY: forty lowercase hex characters by construction, which is exactly
      // what the Oid brand stands for.
      const oid = (digit: string) => digit.repeat(40) as Oid;
      const read = () => Effect.succeed(new TextEncoder().encode("one\n"));

      // Both sides added the same file; one spells the mode with git's own
      // `zeroPaddedFilemode` leading zero. Compared as strings that is not the
      // "same on both sides" shortcut, so the path fell through to an add/add
      // content merge and came back as a conflict over nothing.
      const merged = await Effect.runPromise(
        mergeTrees({
          base: new Map(),
          ours: new Map([["readme.md", { mode: "0100644", oid: oid("1") }]]),
          theirs: new Map([["readme.md", { mode: "100644", oid: oid("1") }]]),
          read,
        }),
      );

      assert.deepEqual(merged.conflicts, []);
      assert.deepEqual(merged.changes, []);
    }),
  );

  it.effect("elects a mode across spellings instead of calling one a clash", () =>
    Effect.promise(async () => {
      // SAFETY: forty lowercase hex characters by construction, which is exactly
      // what the Oid brand stands for.
      const oid = (digit: string) => digit.repeat(40) as Oid;
      const content = new Map<Oid, Uint8Array>([
        [oid("1"), new TextEncoder().encode("one\ntwo\nthree\n")],
        [oid("2"), new TextEncoder().encode("ONE\ntwo\nthree\n")],
        [oid("3"), new TextEncoder().encode("one\ntwo\nTHREE\n")],
      ]);

      // Both sides edited different lines. Ours kept the base's mode but spelled
      // it padded; theirs made it executable. Compared as strings that reads as
      // three different modes — a clash — so the merge reported a `content`
      // conflict on a file that merges cleanly and dropped the `chmod +x`.
      const merged = await Effect.runPromise(
        mergeTrees({
          base: new Map([["script.sh", { mode: "100644", oid: oid("1") }]]),
          ours: new Map([["script.sh", { mode: "0100644", oid: oid("2") }]]),
          theirs: new Map([["script.sh", { mode: "0100755", oid: oid("3") }]]),
          read: (wanted) => Effect.succeed(content.get(wanted)!),
        }),
      );

      assert.deepEqual(merged.conflicts, []);
      assert.equal(
        new TextDecoder().decode(merged.changes[0]?.content ?? new Uint8Array(0)),
        "ONE\ntwo\nTHREE\n",
      );
      // The side that actually changed the mode is the side that wins it.
      assert.equal(merged.changes[0]?.mode, "0100755");
    }),
  );
});
