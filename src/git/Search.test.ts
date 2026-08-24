import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Result } from "effect";

import * as Search from "./Search.ts";

const encode = (text: string): Uint8Array => new TextEncoder().encode(text);

/** Every line, which is what an empty-matching pattern like `^` reports. */
const everyLine = (data: string) =>
  Search.verify(encode(data), () => true, Search.MAX_MATCHES).map((match) => [
    match.line,
    match.text,
  ]);

describe("Search.verify", () => {
  it("ends a blob at its final newline rather than opening another line", () => {
    // A trailing newline terminates the last line; it does not begin an empty
    // one. Counting the remainder as a line invented a line 2 that no other
    // grep reports — visible to any pattern that accepts the empty string.
    assert.deepEqual(everyLine("a\n"), [[1, "a"]]);
  });

  it("reports no lines for an empty blob", () => {
    assert.deepEqual(everyLine(""), []);
  });

  it("keeps a genuinely empty line between two newlines", () => {
    assert.deepEqual(everyLine("a\n\nb\n"), [
      [1, "a"],
      [2, ""],
      [3, "b"],
    ]);
  });

  it("reports a final line that carries no newline", () => {
    assert.deepEqual(everyLine("a\nb"), [
      [1, "a"],
      [2, "b"],
    ]);
  });
});

describe("Search.compileMatcher", () => {
  it("accepts one repetition beside a character class of quantifier characters", () => {
    // `[*?]` is one atom whose members are literal; only `+` repeats. Counting
    // the class members as repetitions refused a pattern that repeats once,
    // which is exactly what the limit is written to allow.
    const compiled = Search.compileMatcher({ pattern: "[*?]x+" });

    assert.ok(Result.isSuccess(compiled));
    assert.equal(compiled.success("a*xx"), true);
    assert.equal(compiled.success("ax"), false);
  });

  it("does not read a bracketed parenthesis as a group", () => {
    assert.equal(Result.isSuccess(Search.compileMatcher({ pattern: "[(]x" })), true);
  });

  it("still refuses two repetitions", () => {
    assert.equal(Result.isFailure(Search.compileMatcher({ pattern: "a+b+" })), true);
  });

  it("still refuses a capturing group", () => {
    assert.equal(Result.isFailure(Search.compileMatcher({ pattern: "(ab)c" })), true);
  });
});

describe("BlobIndex.candidates", () => {
  const blob = (text: string) => new TextEncoder().encode(text);

  it("keeps a case-sensitive literal's blob, whose postings are folded", () => {
    // The prefilter is sound for a case-sensitive needle because a line
    // holding it exactly also holds its folded bigrams; the verifier decides
    // the case. Consulted only for `ignoreCase`, a plain `-F` search built
    // these postings and then read every blob in the tree regardless.
    const index = new Search.BlobIndex();
    // SAFETY: forty hex chars is a well-formed oid, and the index only uses
    // it to name the blob — it never dereferences it against a store.
    const hit = index.observe("a".repeat(40) as never, blob("Hello world\n"));
    // SAFETY: as above.
    const miss = index.observe("b".repeat(40) as never, blob("nothing here\n"));

    const candidates = index.candidates("Hello", true);

    assert.ok(candidates !== null);
    assert.equal(candidates.has(hit.ordinal), true);
    assert.equal(candidates.has(miss.ordinal), false);
  });

  it("declines to model a query it cannot fold", () => {
    const index = new Search.BlobIndex();
    assert.equal(index.candidates("hello", false), null, "not a literal search");
    assert.equal(index.candidates("é", true), null, "not printable ASCII");
    assert.equal(index.candidates("a", true), null, "shorter than one bigram");
  });
});
