/**
 * The portable format, read without being rewritten.
 *
 * K-01 through K-04 of docs/context-pack.knowledge.md §16: a minimal document
 * with unknown metadata stays readable, navigation documents are not Concepts,
 * hostile YAML fails inside its bounds, and a timestamp is an instant or it is
 * invalid — never a guess about which midnight the author meant.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Result } from "effect";

import * as Concept from "./Concept.ts";

const encode = (text: string) => new TextEncoder().encode(text);

const parse = (body: string, path = ".gitplus/knowledge/gotchas/worker-auth.md") =>
  Concept.parse({ bundle: Concept.BUNDLE, path, bytes: encode(body) });

describe("Concept parsing", () => {
  it("reads a minimal document and preserves unknown metadata", () => {
    const parsed = parse(`---
type: Gotcha
title: Worker auth needs the production fixture
unknown_field:
  nested:
    deeper: kept
okf_version: "0.2"
---
The body.
`);
    assert.equal(parsed.ok, true);
    if (!parsed.ok) return;
    assert.equal(parsed.concept.id, "gotchas/worker-auth");
    assert.equal(parsed.concept.type, "Gotcha");
    // Absent `status` is `stable` under OKF v0.2 (§9), not "unknown".
    assert.equal(parsed.concept.lifecycle, "stable");
    assert.equal(parsed.concept.body, "The body.\n");
    // SAFETY: `parse` only succeeds for a frontmatter block that decoded as a
    // mapping — the `ok: false` branch above is the one for anything else — so
    // the metadata of a parsed Concept is a record of nodes.
    const metadata = parsed.concept.metadata as Record<string, Concept.Node>;
    // Unknown portable metadata survives whole: Git+ not interpreting a field
    // is not a reason to refuse the document (§5.1).
    assert.deepEqual(metadata["unknown_field"], { nested: { deeper: "kept" } });
    assert.equal(metadata["okf_version"], "0.2");
  });

  it("reads the Git+ provenance extension", () => {
    const parsed = parse(`---
type: Gotcha
description: Use the production policy fixture.
status: draft
stale_after: "2026-12-31T00:00:00Z"
sources:
  - id: discovery
    resource: gitplus:record:sha1:1111111111111111111111111111111111111111
gitplus:
  cites:
    - record: sha1:1111111111111111111111111111111111111111
  evidence:
    - kind: blob
      path: tests/worker/auth.test.ts
      blob: sha1:2222222222222222222222222222222222222222
    - kind: gitlink
      path: vendor/policy-engine
      commit: sha1:3333333333333333333333333333333333333333
  external:
    vendor-contract:
      retrieved_at: "2026-08-22T09:10:00Z"
      content_digest: sha256:abc
  verification_records:
    - sha1:4444444444444444444444444444444444444444
---
Prose.
`);
    assert.equal(parsed.ok, true);
    if (!parsed.ok) return;
    const { concept } = parsed;
    assert.equal(concept.lifecycle, "draft");
    assert.equal(concept.staleAfter, "2026-12-31T00:00:00Z");
    assert.deepEqual(concept.sources, [
      { id: "discovery", resource: "gitplus:record:sha1:1111111111111111111111111111111111111111" },
    ]);
    assert.deepEqual(concept.cites, ["sha1:1111111111111111111111111111111111111111"]);
    assert.deepEqual(concept.evidence, [
      {
        kind: "blob",
        path: "tests/worker/auth.test.ts",
        blob: "sha1:2222222222222222222222222222222222222222",
      },
      {
        kind: "gitlink",
        path: "vendor/policy-engine",
        commit: "sha1:3333333333333333333333333333333333333333",
      },
    ]);
    // The absent field is present-and-undefined rather than missing: it is
    // what the parser knows, and `JSON.stringify` drops it in a report.
    assert.deepEqual(concept.external, [
      {
        id: "vendor-contract",
        retrievedAt: "2026-08-22T09:10:00Z",
        contentDigest: "sha256:abc",
        snapshot: undefined,
      },
    ]);
    assert.deepEqual(concept.verificationRecords, [
      "sha1:4444444444444444444444444444444444444444",
    ]);
  });

  it("K-02: index.md and log.md are navigation, not Concepts", () => {
    assert.equal(Concept.isConceptPath(Concept.BUNDLE, ".gitplus/knowledge/index.md"), false);
    assert.equal(
      Concept.isConceptPath(Concept.BUNDLE, ".gitplus/knowledge/architecture/index.md"),
      false,
    );
    assert.equal(Concept.isConceptPath(Concept.BUNDLE, ".gitplus/knowledge/gotchas/log.md"), false);
    assert.equal(
      Concept.isConceptPath(Concept.BUNDLE, ".gitplus/knowledge/gotchas/worker-auth.md"),
      true,
    );
    assert.equal(Concept.isConceptPath(Concept.BUNDLE, "src/index.md"), false);
  });

  it("K-03: refuses duplicate keys rather than letting the last one win", () => {
    const parsed = parse(`---
type: Gotcha
gitplus:
  cites:
    - record: sha1:1111111111111111111111111111111111111111
gitplus:
  cites:
    - record: sha1:2222222222222222222222222222222222222222
---
`);
    assert.equal(parsed.ok, false);
    assert.equal(parsed.diagnostics[0]?.code, "knowledge.yaml");
    assert.match(parsed.diagnostics[0]?.message ?? "", /duplicate key 'gitplus'/u);
  });

  it("K-03: refuses anchors, aliases and tags without resolving them", () => {
    for (const value of ["&anchor value", "*alias", "!!python/object x"]) {
      const parsed = Concept.parseYaml(`type: Gotcha\ntitle: ${value}\n`);
      assert.equal(Result.isFailure(parsed), true, value);
    }
  });

  it("K-03: bounds nesting depth", () => {
    let document = "root:";
    for (let depth = 0; depth < Concept.MAX_DEPTH + 4; depth += 1) {
      document += `\n${" ".repeat((depth + 1) * 2)}deeper:`;
    }
    document += " value\n";
    const parsed = Concept.parseYaml(document);
    assert.equal(Result.isFailure(parsed), true);
  });

  it("K-04: an offset-aware timestamp is an instant", () => {
    const parsed = Concept.instantOf("2026-12-31T00:00:00Z");
    assert.equal(parsed.state, "instant");
    if (parsed.state !== "instant") return;
    assert.equal(parsed.at, Date.parse("2026-12-31T00:00:00Z"));

    const offset = Concept.instantOf("2026-12-31T00:00:00+05:30");
    assert.equal(offset.state, "instant");
    if (offset.state !== "instant") return;
    assert.equal(offset.at, Date.parse("2026-12-31T00:00:00+05:30"));
  });

  it("K-04: a date-only deadline is invalid, not midnight somewhere", () => {
    const parsed = Concept.instantOf("2026-12-31");
    assert.equal(parsed.state, "invalid");
  });

  it("K-04: an offset that crosses midnight is still a real calendar date", () => {
    // The authored day is what is checked; its UTC equivalent may legitimately
    // land in the previous or next day, month or year.
    for (const value of [
      "2026-03-01T00:00:00+05:00",
      "2027-01-01T02:00:00+05:30",
      "2026-02-28T23:00:00-05:00",
      "2026-12-31T23:00:00-05:00",
    ]) {
      assert.equal(Concept.instantOf(value).state, "instant", value);
    }
  });

  it("K-04: an impossible calendar date does not roll forward", () => {
    assert.equal(Concept.instantOf("2026-02-30T00:00:00Z").state, "invalid");
    assert.equal(Concept.instantOf("2026-13-01T00:00:00Z").state, "invalid");
    // A leap day exists in 2028 and not in 2026.
    assert.equal(Concept.instantOf("2026-02-29T00:00:00Z").state, "invalid");
    assert.equal(Concept.instantOf("2028-02-29T00:00:00Z").state, "instant");
  });

  it("K-10: refuses a range that is empty, reversed or not a pair", () => {
    // `Number` would read "" as 0 and "1e1" as 10; neither is a declared bound.
    for (const range of ["[5, 5]", "[9, 4]", "[-1, 4]", "[1, 2, 3]", '["", 10]', "[1e1, 20]"]) {
      const parsed = parse(`---
type: Gotcha
gitplus:
  evidence:
    - kind: blob
      path: a.ts
      blob: sha1:2222222222222222222222222222222222222222
      range: ${range}
---
`);
      assert.equal(parsed.ok, true, range);
      if (!parsed.ok) return;
      assert.equal(parsed.concept.evidence.length, 0, range);
      assert.equal(
        parsed.diagnostics.some((d) => d.code === "knowledge.evidence.range"),
        true,
      );
    }
  });

  it("keeps a valid range", () => {
    const parsed = parse(`---
type: Gotcha
gitplus:
  evidence:
    - kind: blob
      path: a.ts
      blob: sha1:2222222222222222222222222222222222222222
      range: [0, 12]
---
`);
    assert.equal(parsed.ok, true);
    if (!parsed.ok) return;
    assert.deepEqual(parsed.concept.evidence[0], {
      kind: "blob",
      path: "a.ts",
      blob: "sha1:2222222222222222222222222222222222222222",
      range: [0, 12],
    });
  });

  it("finds the real close fence past a rule inside the frontmatter", () => {
    const parsed = parse(`---
type: Gotcha
description: "a value with --- inside it"
----
---
Body.
`);
    // The `----` is not a fence; giving up on the first candidate reported a
    // document with a perfectly good fence below as having none.
    assert.equal(parsed.ok, false);

    const closed = parse(`---
type: Gotcha
title: "--- not a fence"
---
Body.
`);
    assert.equal(closed.ok, true);
    assert.equal(closed.ok && closed.concept.body, "Body.\n");
  });

  it("refuses a document with no type and one with no frontmatter", () => {
    assert.equal(parse("---\ntitle: no type\n---\n").ok, false);
    assert.equal(parse("no frontmatter at all\n").ok, false);
  });

  it("summarizes from `description`, else from the authored body", () => {
    const described = parse(
      "---\ntype: Gotcha\ndescription: The short form.\n---\nLonger prose.\n",
    );
    assert.equal(described.ok && Concept.summarize(described.concept), "The short form.");

    const bodied = parse("---\ntype: Gotcha\n---\n# Heading\n\nThe first real paragraph.\n");
    assert.equal(bodied.ok && Concept.summarize(bodied.concept), "The first real paragraph.");
  });

  it("clips a summary between characters, never inside one", () => {
    // An astral character straddles the cut: 318 units, then a two-unit emoji.
    const description = `${"a".repeat(318)}😀${"b".repeat(10)}`;
    const parsed = parse(`---\ntype: Gotcha\ndescription: ${description}\n---\n`);
    assert.equal(parsed.ok, true);
    if (!parsed.ok) return;
    const summary = Concept.summarize(parsed.concept);
    assert.ok(summary.length <= 320);
    assert.ok(summary.endsWith("…"));
    // A lone surrogate does not survive an encode; a whole string does.
    assert.equal(new TextDecoder().decode(new TextEncoder().encode(summary)), summary);
  });
});
