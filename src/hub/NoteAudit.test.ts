/**
 * Drift: what changed under a note, and what a check is allowed to conclude.
 *
 * The matrix below is the feature's whole claim — formatting is not a change,
 * a body is, a signature is a different kind of change, and a vanished file is
 * a question rather than an answer. The last test is the one the rest exist to
 * protect: a check that runs three times reports drift three times, because
 * seeing changed source is not the same as agreeing with it.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { indexMemory, REGULAR, WorkTree, workTreeMemory } from "../git/Work.ts";
import { syntax as anchorSyntax } from "./Anchor.syntax.ts";
import { AnchorResolver, type Fingerprint } from "./Anchor.ts";
import * as Audit from "./NoteAudit.ts";
import * as Note from "./Note.ts";
import * as Notes from "./NoteProjection.ts";
import type { Projection } from "./NoteProjection.ts";

const encoder = new TextEncoder();

const scenario = <A, E>(effect: Effect.Effect<A, E, AnchorResolver | Repository | WorkTree>) =>
  effect.pipe(
    Effect.provide(
      Layer.mergeAll(
        GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop)),
        workTreeMemory,
        indexMemory,
        anchorSyntax,
      ).pipe(Layer.provideMerge(stores)),
    ),
  );

const REPO = "SHA256:test";
const VERIFY = "export function verify(token: string): boolean {\n  return token.length > 0\n}\n";

/** Files on disk, replacing whatever was there. */
const checkout = Effect.fn("test.checkout")(function* (files: Record<string, string>) {
  const work = yield* WorkTree;
  for (const path of yield* work.list([])) yield* work.remove(path);
  for (const [path, content] of Object.entries(files)) {
    yield* work.write(path, encoder.encode(content), REGULAR);
  }
});

/** The same files as a commit on `refs/heads/main`, with HEAD pointing at it. */
const commit = Effect.fn("test.commit")(function* (files: Record<string, string>, message: string) {
  const repository = yield* Repository;
  const tree = yield* repository.writeFiles({
    changes: Object.entries(files).map(([path, content]) => ({
      path,
      content: encoder.encode(content),
      mode: "100644",
    })),
  });
  const oid = yield* repository.commit({
    branch: "refs/heads/main",
    tree,
    message,
    author: { name: "A", email: "a@example.com", at: new Date(1_700_000_000_000), offset: 0 },
  });
  yield* repository.setHead("refs/heads/main");
  return oid;
});

/** A projected note, without the event log a drift check does not consult. */
const noteOn = (input: {
  readonly path: string;
  readonly anchor: string;
  readonly baseline: Fingerprint | null;
}): Projection => ({
  state: "ready",
  id: "01991f71-b8d7-7def-82d8-0c30c58ae122",
  path: input.path,
  anchor: input.anchor,
  text: "comparison must remain constant-time",
  createdAt: "2026-01-01T00:00:00.000Z",
  createdBy: "SHA256:author",
  updatedAt: "2026-01-01T00:00:00.000Z",
  updatedBy: "SHA256:author",
  active: true,
  pinned: false,
  baseline: input.baseline,
});

/** The fingerprint `source` produces for `anchor`, as creation would record it. */
const baselineOf = Effect.fn("test.baselineOf")(function* (
  path: string,
  source: string,
  anchor: string,
) {
  const resolved = yield* (yield* AnchorResolver).resolve(path, encoder.encode(source), anchor);
  assert.equal(resolved._tag, "Found");
  if (resolved._tag !== "Found") throw new Error("unreachable");
  return resolved.fingerprint;
});

/** One note against one work tree, which is what `git+ note check` does. */
const statusOf = Effect.fn("test.statusOf")(function* (note: Projection) {
  return yield* Audit.audit(note, yield* Audit.workTree());
});

const anchored = Effect.fn("test.anchored")(function* (
  path: string,
  before: string,
  after: string,
  anchor = "function verify",
) {
  const baseline = yield* baselineOf(path, before, anchor);
  yield* commit({ [path]: before }, "before\n");
  yield* checkout({ [path]: after });
  return yield* statusOf(noteOn({ path, anchor, baseline }));
});

describe("anchored note drift", () => {
  it.effect("calls unchanged source fresh", () =>
    Effect.gen(function* () {
      assert.equal((yield* anchored("src/auth.ts", VERIFY, VERIFY)).status, "fresh");
    }).pipe(scenario),
  );

  it.effect("calls formatting and comments fresh", () =>
    Effect.gen(function* () {
      const reformatted =
        "export function verify( token: string ): boolean {\n\n  // still constant-time\n  return token.length > 0\n}\n";
      assert.equal((yield* anchored("src/auth.ts", VERIFY, reformatted)).status, "fresh");
    }).pipe(scenario),
  );

  it.effect("calls a changed body content-changed", () =>
    Effect.gen(function* () {
      const body =
        "export function verify(token: string): boolean {\n  return token.length > 1\n}\n";
      assert.equal((yield* anchored("src/auth.ts", VERIFY, body)).status, "content-changed");
    }).pipe(scenario),
  );

  it.effect("calls a changed declaration contract-changed", () =>
    Effect.gen(function* () {
      const contract =
        "export function verify(token: Uint8Array): boolean {\n  return token.length > 0\n}\n";
      assert.equal((yield* anchored("src/auth.ts", VERIFY, contract)).status, "contract-changed");
    }).pipe(scenario),
  );

  it.effect("calls a deleted declaration anchor-missing", () =>
    Effect.gen(function* () {
      const gone = "export const NOTHING = 1\n";
      assert.equal((yield* anchored("src/auth.ts", VERIFY, gone)).status, "anchor-missing");
    }).pipe(scenario),
  );

  it.effect("calls an anchor two declarations answer to anchor-missing", () =>
    Effect.gen(function* () {
      // Creation refuses a bare name two declarations answer to, so a stored
      // note can only reach this by the source growing a second one later.
      // Either way the anchor no longer names one region, which is the same
      // question a deleted one asks.
      const twice = `${VERIFY}\nexport class verify {}\n`;
      yield* commit({ "src/auth.ts": twice }, "before\n");
      yield* checkout({ "src/auth.ts": twice });
      const status = yield* statusOf(
        noteOn({ path: "src/auth.ts", anchor: "verify", baseline: null }),
      );
      assert.equal(status.status, "anchor-missing");
    }).pipe(scenario),
  );

  it.effect("calls a symbol anchor the resolver will not read unverifiable", () =>
    Effect.gen(function* () {
      // Java's comments are unambiguous and its declarations are not, so the
      // resolver normalizes whole files and declines every symbol rather than
      // anchoring a constraint to whatever a pattern happened to match.
      const source = "class Repository {\n  boolean verify(String t) { return true; }\n}\n";
      yield* commit({ "src/Repository.java": source }, "before\n");
      yield* checkout({ "src/Repository.java": source });
      const status = yield* statusOf(
        noteOn({ path: "src/Repository.java", anchor: "class Repository", baseline: null }),
      );
      assert.equal(status.status, "unverifiable");

      // Its whole file still normalizes, so a comment is not drift.
      const before = yield* baselineOf("src/Repository.java", source, "@file");
      yield* checkout({ "src/Repository.java": `// added later\n${source}` });
      const whole = yield* statusOf(
        noteOn({ path: "src/Repository.java", anchor: "@file", baseline: before }),
      );
      assert.equal(whole.status, "fresh");
    }).pipe(scenario),
  );

  it.effect("gives a language it cannot read at all a byte-exact whole file", () =>
    Effect.gen(function* () {
      const before = yield* baselineOf("data/fixture.bin", "alpha\n", "@file");
      assert.equal(before.resolver, "file@1");
      assert.equal(before.normalization, "exact-v1");
      yield* commit({ "data/fixture.bin": "alpha\n" }, "before\n");
      yield* checkout({ "data/fixture.bin": "alpha \n" });
      const status = yield* statusOf(
        noteOn({ path: "data/fixture.bin", anchor: "@file", baseline: before }),
      );
      // Whitespace is not formatting where nothing knows what formatting is.
      assert.equal(status.status, "content-changed");
    }).pipe(scenario),
  );

  it.effect("calls a normalization it cannot compare rebaseline-required", () =>
    Effect.gen(function* () {
      const baseline = yield* baselineOf("src/auth.ts", VERIFY, "function verify");
      yield* commit({ "src/auth.ts": VERIFY }, "before\n");
      yield* checkout({ "src/auth.ts": VERIFY });
      const status = yield* statusOf(
        noteOn({
          path: "src/auth.ts",
          anchor: "function verify",
          baseline: {
            resolver: baseline.resolver,
            normalization: "semantic-v0",
            signatureHash: baseline.signatureHash,
            contentHash: baseline.contentHash,
            rawHash: baseline.rawHash,
          },
        }),
      );
      // Byte-identical source, and still not `fresh`: the hashes were computed
      // by rules this build no longer runs, so they compare to nothing.
      assert.equal(status.status, "rebaseline-required");
    }).pipe(scenario),
  );

  it.effect("calls a vanished file source-missing", () =>
    Effect.gen(function* () {
      const baseline = yield* baselineOf("src/legacy.ts", VERIFY, "function verify");
      yield* commit({ "src/legacy.ts": VERIFY }, "before\n");
      yield* commit({ "src/other.ts": "export const UNRELATED = 1\n" }, "delete\n");
      yield* checkout({ "src/other.ts": "export const UNRELATED = 1\n" });
      const status = yield* statusOf(
        noteOn({ path: "src/legacy.ts", anchor: "function verify", baseline }),
      );
      assert.equal(status.status, "source-missing");
      assert.equal(status.pathMovedFrom, null);
    }).pipe(scenario),
  );

  it.effect("follows a rename and keeps checking the same constraint", () =>
    Effect.gen(function* () {
      const baseline = yield* baselineOf("src/auth.ts", VERIFY, "function verify");
      yield* commit({ "src/auth.ts": VERIFY }, "before\n");
      yield* commit({ "src/security/auth.ts": VERIFY }, "move\n");
      yield* checkout({ "src/security/auth.ts": VERIFY });
      const status = yield* statusOf(
        noteOn({ path: "src/auth.ts", anchor: "function verify", baseline }),
      );
      assert.equal(status.status, "fresh");
      assert.equal(status.path, "src/security/auth.ts");
      assert.equal(status.pathMovedFrom, "src/auth.ts");
    }).pipe(scenario),
  );

  it.effect("answers a query naming the path a note's file moved to", () =>
    Effect.gen(function* () {
      // The read an agent makes before editing names the file as it is now.
      // Scoping on the note's stored path answered nothing for exactly that
      // query, so the rename following above could only ever fire for an
      // unscoped check — the one nobody runs before an edit.
      const baseline = yield* baselineOf("src/auth.ts", VERIFY, "function verify");
      yield* commit({ "src/auth.ts": VERIFY }, "before\n");
      yield* commit({ "src/security/auth.ts": VERIFY }, "move\n");
      yield* checkout({ "src/security/auth.ts": VERIFY });
      const note = noteOn({ path: "src/auth.ts", anchor: "function verify", baseline });

      const source = yield* Audit.workTree();
      const scoped = yield* Audit.auditAll([note], source, "src/security/auth.ts");
      assert.deepEqual(
        scoped.map((entry) => entry.path),
        ["src/security/auth.ts"],
      );

      // And a query naming a path it did not move to still excludes it.
      const elsewhere = yield* Audit.auditAll([note], source, "src/other");
      assert.deepEqual(elsewhere, []);
    }).pipe(scenario),
  );

  it.effect("refuses to guess when two files could be the rename", () =>
    Effect.gen(function* () {
      const baseline = yield* baselineOf("src/auth.ts", VERIFY, "function verify");
      yield* commit({ "src/auth.ts": VERIFY }, "before\n");
      yield* commit({ "a/auth.ts": VERIFY, "b/auth.ts": VERIFY }, "split\n");
      yield* checkout({ "a/auth.ts": VERIFY, "b/auth.ts": VERIFY });
      const status = yield* statusOf(
        noteOn({ path: "src/auth.ts", anchor: "function verify", baseline }),
      );
      assert.equal(status.status, "source-missing");
    }).pipe(scenario),
  );

  it.effect("reports a conflicted note without reading source at all", () =>
    Effect.gen(function* () {
      const conflicted: Projection = {
        ...noteOn({ path: "nowhere.ts", anchor: "@file", baseline: null }),
        state: "conflicted",
        competing: [],
      };
      const status = yield* statusOf(conflicted);
      assert.equal(status.status, "conflicted");
      assert.equal(Audit.actionable("conflicted"), true);
    }).pipe(scenario),
  );

  it.effect("keeps reporting drift until a signed judgment says otherwise", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const baseline = yield* baselineOf("src/auth.ts", VERIFY, "function verify");
      yield* commit({ "src/auth.ts": VERIFY }, "before\n");

      const { note } = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: "function verify",
        text: "comparison must remain constant-time",
        baseline,
        key,
      });

      const changed =
        "export function verify(token: string): boolean {\n  return token.length > 2\n}\n";
      yield* checkout({ "src/auth.ts": changed });

      const seen: string[] = [];
      for (let round = 0; round < 3; round++) {
        const projected = yield* Notes.project(note);
        assert.ok(projected !== null);
        seen.push((yield* statusOf(projected)).status);
      }
      assert.deepEqual(seen, ["content-changed", "content-changed", "content-changed"]);

      // Confirmed against what the source says now — which is the only thing
      // that ever moves a baseline.
      const current = yield* baselineOf("src/auth.ts", changed, "function verify");
      yield* Note.confirm({ repo: REPO, note, baseline: current, key });
      const after = yield* Notes.project(note);
      assert.ok(after !== null);
      assert.equal((yield* statusOf(after)).status, "fresh");
    }).pipe(scenario),
  );
});

describe("anchored notes over a range", () => {
  it.effect("checks only the notes whose paths the range touched", () =>
    Effect.gen(function* () {
      const authBaseline = yield* baselineOf("src/auth.ts", VERIFY, "function verify");
      const stale = "export function parse(input: string): number {\n  return input.length\n}\n";
      const staleBaseline = yield* baselineOf("src/parse.ts", stale, "function parse");

      const base = yield* commit({ "src/auth.ts": VERIFY, "src/parse.ts": stale }, "base\n");
      const changed =
        "export function verify(token: string): boolean {\n  return token.length > 5\n}\n";
      const drifted =
        "export function parse(input: string): number {\n  return input.length + 1\n}\n";
      // Both files differ from what the notes recorded; only one is in the range.
      yield* checkout({ "src/auth.ts": changed, "src/parse.ts": drifted });
      const head = yield* commit({ "src/auth.ts": changed, "src/parse.ts": stale }, "head\n");

      const notes = [
        {
          ...noteOn({ path: "src/auth.ts", anchor: "function verify", baseline: authBaseline }),
          id: "auth",
        },
        {
          ...noteOn({ path: "src/parse.ts", anchor: "function parse", baseline: staleBaseline }),
          id: "parse",
        },
      ];

      assert.deepEqual([...(yield* Audit.changedPaths(base, head))], ["src/auth.ts"]);
      const ranged = yield* Audit.auditRange(notes, base, head);
      assert.deepEqual(
        ranged.map((entry) => [entry.id, entry.status]),
        [["auth", "content-changed"]],
      );

      // The whole-repository audit still reports the drift the range skipped.
      const everything = yield* Audit.auditAll(notes, yield* Audit.workTree());
      assert.deepEqual(
        everything.map((entry) => [entry.id, entry.status]),
        [
          ["auth", "content-changed"],
          ["parse", "content-changed"],
        ],
      );
    }).pipe(scenario),
  );

  it.effect("scopes a query to a path or the directory beneath it", () =>
    Effect.gen(function* () {
      assert.equal(Audit.covers(undefined, "src/auth.ts"), true);
      assert.equal(Audit.covers("src/auth.ts", "src/auth.ts"), true);
      assert.equal(Audit.covers("src/", "src/auth.ts"), true);
      assert.equal(Audit.covers("src", "src/auth.ts"), true);
      assert.equal(Audit.covers("src/auth", "src/auth.ts"), false);
      assert.equal(Audit.covers("docs", "src/auth.ts"), false);
      yield* Effect.void;
    }).pipe(scenario),
  );
});
