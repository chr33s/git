/**
 * The kept answer, and the ways it must refuse to be trusted.
 *
 * `read` answering `null` costs one walk. `read` answering "nothing was
 * removed" when something was costs a render whose bytes an operator removed
 * being retained again under the same oid, silently. Every case below is one
 * of the second kind turned into the first.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import type { Oid } from "../git/Store.ts";
import { Answers, type Mark } from "./Redaction.ts";
import * as Cache from "./Redaction.node.ts";

describe("an answer kept beside the repository", () => {
  let gitDir = "";
  // SAFETY: forty hex characters is what the brand names, and these are
  // literals in this file rather than anything read back from a store.
  const marks = (target: string): ReadonlyArray<Mark> => [
    { target: target as Oid, bound: true, signers: [] },
  ];
  const held = () => path.join(gitDir, "gitplus", "redaction.json");

  const ask = <A>(effect: (answers: Answers["Service"]) => Effect.Effect<A>) =>
    Effect.runPromise(
      Effect.gen(function* () {
        return yield* effect(yield* Answers);
      }).pipe(Effect.provide(Cache.beside(gitDir))),
    );

  beforeEach(() => {
    gitDir = fs.mkdtempSync(path.join(os.tmpdir(), "redaction-cache-"));
  });
  afterEach(() => {
    fs.rmSync(gitDir, { recursive: true, force: true });
  });

  it("gives back what it was given, under the same key", async () => {
    await ask((answers) => answers.write("key-one", marks("a".repeat(40))));
    const read = await ask((answers) => answers.read("key-one"));
    assert.deepEqual(
      read?.map((mark) => mark.target),
      ["a".repeat(40)],
    );
  });

  it("keeps an empty answer, which is the ordinary one", async () => {
    // Most repositories have no redactions and pay the walk to learn it, so
    // "nothing is removed" is exactly the answer worth not recomputing — and a
    // layer that treated empty as absent would never save anything.
    await ask((answers) => answers.write("key-one", []));
    assert.deepEqual(await ask((answers) => answers.read("key-one")), []);
  });

  it("refuses a different key", async () => {
    // The key carries the refs the answer was walked from. A different key is
    // a different repository or a moved ref, and the answer does not carry.
    await ask((answers) => answers.write("key-one", marks("a".repeat(40))));
    assert.equal(await ask((answers) => answers.read("key-two")), null);
  });

  it("refuses when there is nothing there", async () => {
    assert.equal(await ask((answers) => answers.read("key-one")), null);
  });

  it("refuses a file it cannot parse", async () => {
    await ask((answers) => answers.write("key-one", marks("a".repeat(40))));
    fs.writeFileSync(held(), "{ not json");
    assert.equal(await ask((answers) => answers.read("key-one")), null);
  });

  it("keeps one answer per ref rather than one per file", async () => {
    // The port is asked per ref, so a file holding a single answer has each
    // ref's write clobber the last — a repository with any history would cache
    // one ref out of however many it has, which is most of the walk still paid.
    await ask((answers) => answers.write("ref-one", marks("a".repeat(40))));
    await ask((answers) => answers.write("ref-two", marks("b".repeat(40))));

    assert.deepEqual(
      (await ask((answers) => answers.read("ref-one")))?.map((mark) => mark.target),
      ["a".repeat(40)],
    );
    assert.deepEqual(
      (await ask((answers) => answers.read("ref-two")))?.map((mark) => mark.target),
      ["b".repeat(40)],
    );
  });

  it("refuses an entry whose oids are not oids", async () => {
    // The schema says these are strings; `isOid` says what kind. Asserted
    // instead of checked, a hand-edited or truncated file would put a value
    // the rest of the module believes is an object id into `gc`'s reach.
    fs.mkdirSync(path.dirname(held()), { recursive: true });
    fs.writeFileSync(
      held(),
      JSON.stringify({ entries: { "ref-one": [{ target: "nope", bound: true, signers: [] }] } }),
    );
    assert.equal(await ask((answers) => answers.read("ref-one")), null);
  });

  it("refuses a file of the wrong shape", async () => {
    // Including one that parses and looks plausible: a `blobs` of the wrong
    // type is the shape a half-migrated or hand-edited file takes.
    await ask((answers) => answers.write("key-one", marks("a".repeat(40))));
    fs.writeFileSync(held(), JSON.stringify({ entries: { "key-one": "not an array" } }));
    assert.equal(await ask((answers) => answers.read("key-one")), null);
  });

  it("refuses a file with no entries at all", async () => {
    fs.mkdirSync(path.dirname(held()), { recursive: true });
    fs.writeFileSync(held(), JSON.stringify({ nothing: {} }));
    assert.equal(await ask((answers) => answers.read("key-one")), null);
  });

  it("does not fail a caller that cannot write", async () => {
    // A repository this process may not write to recomputes, which is the same
    // answer more slowly. `context for` has real work to do; this is only an
    // optimisation and must never be the thing that stops it.
    const missing = path.join(gitDir, "gone");
    fs.rmSync(gitDir, { recursive: true, force: true });
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* (yield* Answers).write("key-one", []);
      }).pipe(Effect.provide(Cache.beside(missing))),
    );
  });
});
