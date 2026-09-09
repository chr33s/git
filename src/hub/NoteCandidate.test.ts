/**
 * Candidate capture, and the line §27 draws around it.
 *
 * The suggestions are worth having only if they are suggestions: every test
 * here checks both halves — that something useful comes out, and that no note
 * ref moved while it did.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Note from "./Note.ts";
import * as Candidate from "./NoteCandidate.ts";
import * as Session from "./Session.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provideMerge(stores)),
    ),
  );

const REPO = "SHA256:test";
const encoder = new TextEncoder();

/** One commit holding these files, on top of `parent`. */
const committed = Effect.fn("test.committed")(function* (
  files: Record<string, string>,
  parent: Oid | null,
) {
  const repository = yield* Repository;
  const tree = yield* repository.writeFiles({
    changes: Object.entries(files).map(([path, content]) => ({
      path,
      content: encoder.encode(content),
      mode: "100644",
    })),
  });
  return yield* repository.commitTree({
    tree,
    parents: parent === null ? [] : [parent],
    message: "work\n",
    author: { name: "A", email: "a@example.com", at: new Date(1_700_000_000_000), offset: 0 },
  });
});

const worked = Effect.fn("test.worked")(function* (input: {
  readonly key: PrivateKey;
  readonly note: string | null;
  readonly commits: ReadonlyArray<string>;
}) {
  const { session } = yield* Session.open({
    repo: REPO,
    agent: { kind: "claude", model: "opus", harness: "cli" },
    prompt: "fix the path handling",
    key: input.key,
  });
  yield* Session.produced({
    repo: REPO,
    session,
    commits: input.commits,
    note: input.note,
    key: input.key,
  });
  return session;
});

/** Every note ref and where it points — nothing here may move any of them. */
const noteRefs = Effect.fn("test.noteRefs")(function* () {
  const repository = yield* Repository;
  return (yield* repository.refs).filter(([name]) => Note.noteOf(name) !== null);
});

describe("candidate constraints", () => {
  it.effect("suggests an uncaptured observation against the files its session changed", () =>
    Effect.gen(function* () {
      const key = yield* generate("agent@example.com");
      const base = yield* committed({ "src/path.ts": "export const A = 1\n" }, null);
      const head = yield* committed(
        { "src/path.ts": "export const A = 2\n", "src/other.ts": "export const B = 1\n" },
        base,
      );
      const session = yield* worked({
        key,
        note: "gotcha: drive-letter comparison must stay case-insensitive",
        commits: [head],
      });

      const before = yield* noteRefs();
      const found = yield* Candidate.candidates();

      assert.equal(found.length, 1);
      const only = found[0]!;
      assert.equal(only.kind, "gotcha");
      assert.equal(only.text, "drive-letter comparison must stay case-insensitive");
      assert.equal(only.session, session);
      assert.deepEqual(only.paths, ["src/other.ts", "src/path.ts"]);

      // §27: suggesting is not publishing.
      assert.deepEqual(yield* noteRefs(), before);
    }).pipe(scenario),
  );

  it.effect("drops an observation an anchored note already carries", () =>
    Effect.gen(function* () {
      const key = yield* generate("agent@example.com");
      const head = yield* committed({ "src/path.ts": "export const A = 1\n" }, null);
      yield* worked({ key, note: "gotcha: paths compare case-insensitively", commits: [head] });

      assert.equal((yield* Candidate.candidates()).length, 1);

      yield* Note.create({
        repo: REPO,
        path: "src/path.ts",
        anchor: "@file",
        text: "paths compare case-insensitively",
        baseline: null,
        key,
      });

      // Captured, so no longer a candidate: the shortlist is of what is
      // missing, and re-listing what somebody wrote down is how it stops
      // being read.
      assert.deepEqual(yield* Candidate.candidates(), []);
    }).pipe(scenario),
  );

  it.effect("says nothing about a session with nowhere to anchor it", () =>
    Effect.gen(function* () {
      const key = yield* generate("agent@example.com");
      // An observation, and no commits to attach it to.
      yield* worked({ key, note: "convention: we prefer Uint8Array", commits: [] });
      assert.deepEqual(yield* Candidate.candidates(), []);

      // A commit this replica does not hold contributes no paths either, and
      // is not a reason to fail the command.
      yield* worked({
        key,
        note: "convention: we prefer Uint8Array",
        commits: ["0".repeat(40)],
      });
      assert.deepEqual(yield* Candidate.candidates(), []);
    }).pipe(scenario),
  );

  it.effect("says nothing about a session that swept the repository", () =>
    Effect.gen(function* () {
      const key = yield* generate("agent@example.com");
      const files: Record<string, string> = {};
      for (let at = 0; at <= Candidate.MAX_PATHS; at++)
        files[`src/f${at}.ts`] = `export const V = ${at}\n`;
      const head = yield* committed(files, null);
      yield* worked({ key, note: "note: renamed everything", commits: [head] });

      // A refactor pairs its observation with every file it touched, which
      // points at nothing in particular.
      assert.deepEqual(yield* Candidate.candidates(), []);
    }).pipe(scenario),
  );

  it.effect("reads an unlabelled observation as a plain note", () =>
    Effect.gen(function* () {
      const key = yield* generate("agent@example.com");
      const head = yield* committed({ "src/path.ts": "export const A = 1\n" }, null);
      yield* worked({ key, note: "the parser needs the BOM stripped first", commits: [head] });

      const found = yield* Candidate.candidates();
      assert.deepEqual(
        found.map((candidate) => [candidate.kind, candidate.text]),
        [["note", "the parser needs the BOM stripped first"]],
      );
    }).pipe(scenario),
  );
});
