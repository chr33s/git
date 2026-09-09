/**
 * Anchored notes: what the records say, and what a reader makes of them.
 *
 * The lifecycle is the point. A note's text and its confirmed baseline may
 * only move through a signed event, so what is worth checking is that the fold
 * moves them exactly there — and that two replicas judging one note
 * incompatibly produce a conflict a human has to settle rather than a winner
 * the fold picked.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import {
  fingerprint as fingerprintKey,
  generate,
  type PrivateKey,
  sign,
  NAMESPACE,
} from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Record from "../trust/Record.ts";
import type { Fingerprint } from "./Anchor.ts";
import * as Note from "./Note.ts";
import * as Notes from "./NoteProjection.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provideMerge(stores)),
    ),
  );

const REPO = "SHA256:test";

const baselineOf = (content: string, signature = "sig"): Fingerprint => ({
  resolver: "typescript-syntax@1",
  normalization: "semantic-v1",
  signatureHash: signature,
  contentHash: content,
  rawHash: `${content}-raw`,
});

const started = Effect.fn("test.started")(function* (text = "must stay constant-time") {
  const key = yield* generate("author@example.com");
  const { note } = yield* Note.create({
    repo: REPO,
    path: "src/auth.ts",
    anchor: "function verify",
    text,
    baseline: baselineOf("one"),
    key,
  });
  return { key, note } as const;
});

/** A `ready` projection, or a failure naming what came back instead. */
const ready = Effect.fn("test.ready")(function* (note: string) {
  const projected = yield* Notes.project(note);
  assert.ok(projected !== null, "the note projects");
  assert.equal(projected.state, "ready");
  if (projected.state !== "ready") throw new Error("unreachable");
  return projected;
});

/**
 * A second event written against `parent` rather than the current head.
 *
 * What a replica that never saw the first event would have written. The ref
 * keeps its own head; the two are brought together by the join below, which
 * is how a fetch delivers them.
 */
const beside = Effect.fn("test.beside")(function* (input: {
  readonly note: string;
  readonly parent: Oid;
  readonly payload: Note.NotePayload;
  readonly key: PrivateKey;
}) {
  const bytes = Note.encode(input.payload);
  return yield* Record.write({
    name: "event",
    payload: bytes,
    signatures: [yield* sign(input.key, bytes, NAMESPACE)],
    parents: [input.parent],
    message: `${input.payload.type} ${input.payload.id}\n`,
  });
});

const joined = Effect.fn("test.joined")(function* (note: string, heads: ReadonlyArray<Oid>) {
  const repository = yield* Repository;
  const commit = yield* repository.commitTree({
    tree: yield* repository.writeTree([]),
    parents: heads,
    message: "join\n",
    author: Record.identityAt(new Date(1_700_000_000_000)),
  });
  yield* repository.setRef({ name: Note.refOf(note), to: commit });
  return commit;
});

describe("anchored note records", () => {
  it("names a ref only where a ref name can hold it", () => {
    assert.equal(Note.isNoteId("01991f71-b8d7-7def-82d8-0c30c58ae122"), true);
    assert.equal(Note.isNoteId(""), false);
    assert.equal(Note.isNoteId("a/b"), false);
    assert.equal(Note.isNoteId("a".repeat(129)), false);
    assert.equal(Note.isNoteId(".hidden"), false);
    assert.equal(Note.noteOf("refs/hub/note/abc"), "abc");
    assert.equal(Note.noteOf("refs/hub/note/a/b"), null);
    assert.equal(Note.noteOf("refs/hub/task/abc"), null);
  });

  it.effect("encodes deterministically and round-trips through the store", () =>
    Effect.gen(function* () {
      const { note } = yield* started();
      const walked = yield* Note.entries(note);
      const [only] = walked.events;
      assert.ok(only !== undefined);
      assert.deepEqual(Note.encode(only.payload), only.bytes);
      const decoded = yield* Note.decode(only.bytes);
      assert.deepEqual(Note.encode(decoded), only.bytes);
    }).pipe(scenario),
  );

  it.effect("refuses a note id no ref can hold, before writing anything", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const failed = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: "@file",
        text: "constant-time",
        baseline: null,
        note: "not/a/component",
        key,
      }).pipe(Effect.flip);
      assert.equal(failed._tag, "Invalid");
      const repository = yield* Repository;
      assert.equal(yield* repository.resolve("refs/hub/note/not/a/component"), null);
    }).pipe(scenario),
  );

  it.effect("refuses an empty constraint and an empty anchor", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const blank = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: "@file",
        text: "   ",
        baseline: null,
        key,
      }).pipe(Effect.flip);
      assert.equal(blank._tag === "Invalid" ? blank.field : blank._tag, "text");
      const anchorless = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: " ",
        text: "constant-time",
        baseline: null,
        key,
      }).pipe(Effect.flip);
      assert.equal(anchorless._tag === "Invalid" ? anchorless.field : anchorless._tag, "anchor");
    }).pipe(scenario),
  );

  it.effect("refuses a constraint carrying a secret", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const failed = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: "@file",
        text: "keep using AKIAIOSFODNN7EXAMPLE for the fixture",
        baseline: null,
        key,
      }).pipe(Effect.flip);
      assert.equal(failed._tag, "Invalid");
    }).pipe(scenario),
  );
});

describe("anchored note projection", () => {
  it.effect("creates, then confirms without touching the text", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      const before = yield* ready(note);
      assert.equal(before.text, "must stay constant-time");
      assert.equal(before.baseline?.contentHash, "one");
      assert.equal(before.active, true);
      assert.equal(before.pinned, false);

      yield* Note.confirm({ repo: REPO, note, baseline: baselineOf("two"), key });
      const after = yield* ready(note);
      assert.equal(after.text, "must stay constant-time");
      assert.equal(after.baseline?.contentHash, "two");
      assert.equal(after.createdBy, after.updatedBy);
    }).pipe(scenario),
  );

  it.effect("replaces text and baseline together", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      yield* Note.replace({
        repo: REPO,
        note,
        text: "comparison must not allocate",
        baseline: baselineOf("two"),
        key,
      });
      const after = yield* ready(note);
      assert.equal(after.text, "comparison must not allocate");
      assert.equal(after.baseline?.contentHash, "two");
    }).pipe(scenario),
  );

  it.effect("retires and restores, keeping every event", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      yield* Note.retire({ repo: REPO, note, reason: "helper deleted", key });
      assert.equal((yield* ready(note)).active, false);

      yield* Note.restore({ repo: REPO, note, baseline: baselineOf("three"), key });
      const restored = yield* ready(note);
      assert.equal(restored.active, true);
      assert.equal(restored.baseline?.contentHash, "three");
      assert.equal((yield* Note.entries(note)).events.length, 3);
    }).pipe(scenario),
  );

  it.effect("pins and unpins without disturbing the baseline", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      yield* Note.setPinned({ repo: REPO, note, pinned: true, key });
      const pinned = yield* ready(note);
      assert.equal(pinned.pinned, true);
      assert.equal(pinned.baseline?.contentHash, "one");

      yield* Note.setPinned({ repo: REPO, note, pinned: false, key });
      assert.equal((yield* ready(note)).pinned, false);
    }).pipe(scenario),
  );

  it.effect("does not project an unsigned event", () =>
    Effect.gen(function* () {
      const { note } = yield* started();
      const repository = yield* Repository;
      const head = yield* repository.readRef(Note.refOf(note));
      assert.ok(head !== null);

      const base = yield* Note.context(REPO, note);
      const payload = { ...base, type: "note.retired", reason: null } as const;
      const unsigned = yield* Record.write({
        name: "event",
        payload: Note.encode(payload),
        signatures: [],
        parents: [head],
        message: "note.retired unsigned\n",
      });
      yield* repository.setRef({ name: Note.refOf(note), to: unsigned });

      // The retirement is on the ref and in the walk, and still does not move
      // the state: an unsigned judgment is not a judgment.
      assert.equal((yield* ready(note)).active, true);
    }).pipe(scenario),
  );

  it.effect("keeps a foreign note's events out of this note's fold", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      const repository = yield* Repository;
      const head = yield* repository.readRef(Note.refOf(note));
      assert.ok(head !== null);

      const elsewhere = yield* Note.context(REPO, Note.newId());
      const strayed = yield* beside({
        note,
        parent: head,
        payload: { ...elsewhere, type: "note.retired", reason: null },
        key,
      });
      yield* repository.setRef({ name: Note.refOf(note), to: strayed });
      assert.equal((yield* ready(note)).active, true);
    }).pipe(scenario),
  );

  it.effect("reports a conflict rather than choosing between two judgments", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      const repository = yield* Repository;
      const creation = yield* repository.readRef(Note.refOf(note));
      assert.ok(creation !== null);

      // One replica retires the note; another replaces its text. Neither saw
      // the other, and a fetch brings both back under one ref.
      const retired = yield* Note.retire({ repo: REPO, note, reason: "gone", key });
      const replacement = yield* Note.context(REPO, note);
      const replaced = yield* beside({
        note,
        parent: creation,
        payload: {
          ...replacement,
          type: "note.replaced",
          text: "comparison must not allocate",
          baseline: baselineOf("two"),
        },
        key,
      });
      yield* joined(note, [retired, replaced]);

      const projected = yield* Notes.project(note);
      assert.ok(projected !== null);
      assert.equal(projected.state, "conflicted");
      if (projected.state !== "conflicted") return;
      assert.deepEqual(projected.competing.map((entry) => entry.event).sort(), [
        "note.replaced",
        "note.retired",
      ]);
    }).pipe(scenario),
  );

  it.effect("resumes from a later judgment that descends both sides", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      const repository = yield* Repository;
      const creation = yield* repository.readRef(Note.refOf(note));
      assert.ok(creation !== null);

      const retired = yield* Note.retire({ repo: REPO, note, reason: "gone", key });
      const replacement = yield* Note.context(REPO, note);
      const replaced = yield* beside({
        note,
        parent: creation,
        payload: {
          ...replacement,
          type: "note.replaced",
          text: "comparison must not allocate",
          baseline: baselineOf("two"),
        },
        key,
      });
      yield* joined(note, [retired, replaced]);

      // Somebody looks at both and says what is true now.
      yield* Note.replace({
        repo: REPO,
        note,
        text: "comparison must remain constant-time and allocation-free",
        baseline: baselineOf("four"),
        key,
      });
      const settled = yield* ready(note);
      assert.equal(settled.text, "comparison must remain constant-time and allocation-free");
      assert.equal(settled.baseline?.contentHash, "four");
      // Still retired: the settling event superseded the text, and only a
      // `note.restored` says a retired constraint applies again. Both sides of
      // the disagreement are ancestors of it, so both were folded — the
      // retirement was answered, not discarded.
      assert.equal(settled.active, false);

      yield* Note.restore({ repo: REPO, note, baseline: baselineOf("five"), key });
      const live = yield* ready(note);
      assert.equal(live.active, true);
      assert.equal(live.text, "comparison must remain constant-time and allocation-free");
    }).pipe(scenario),
  );

  it.effect("converges on the same state whichever side is walked first", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      const repository = yield* Repository;
      const creation = yield* repository.readRef(Note.refOf(note));
      assert.ok(creation !== null);

      // Two confirmations of the same baseline: concurrent, but not
      // incompatible — both say the same thing about the same source.
      const first = yield* Note.confirm({ repo: REPO, note, baseline: baselineOf("two"), key });
      const context = yield* Note.context(REPO, note);
      const second = yield* beside({
        note,
        parent: creation,
        payload: { ...context, type: "note.confirmed", baseline: baselineOf("two") },
        key,
      });

      yield* joined(note, [first, second]);
      const left = yield* Notes.project(note);
      yield* joined(note, [second, first]);
      const right = yield* Notes.project(note);
      assert.deepEqual(left?.baseline, right?.baseline);
      assert.equal(left?.state, right?.state);
      assert.equal(left?.active, right?.active);
    }).pipe(scenario),
  );

  it.effect("carries a tombstone without letting it stand in for a judgment", () =>
    Effect.gen(function* () {
      const { key, note } = yield* started();
      const repository = yield* Repository;
      const head = yield* repository.readRef(Note.refOf(note));
      assert.ok(head !== null);

      // §28 keeps note text removable, and the tombstone that records the
      // removal is an event on the same ref. It says what went; it does not
      // confirm, replace or retire anything.
      const base = yield* Note.context(REPO, note);
      const stone = yield* beside({
        note,
        parent: head,
        payload: {
          ...base,
          type: "event.redacted",
          target: base.id,
          targetCommit: `sha1:${head}`,
          reason: "the constraint quoted a credential",
        },
        key,
      });
      yield* repository.setRef({ name: Note.refOf(note), to: stone });

      const after = yield* ready(note);
      assert.equal(after.text, "must stay constant-time");
      assert.equal(after.active, true);
      assert.equal(after.baseline?.contentHash, "one");
    }).pipe(scenario),
  );

  it.effect("surfaces two beginnings as a conflict rather than dropping the note", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const id = Note.newId();

      // Two replicas each begin the same note id and neither saw the other —
      // a disagreement about what the note *is*, not about what happened to
      // it. Read as "no creation", the note vanished from every listing while
      // two signed creations sat on its ref.
      const mine = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: "function verify",
        text: "must stay constant-time",
        baseline: null,
        note: id,
        key,
      });

      // A root of its own, which is what makes the two incomparable.
      const rival = { ...(yield* Note.context(REPO, id)), type: "note.created" } as const;
      const payload = Note.encode({
        ...rival,
        path: "src/token.ts",
        anchor: "@file",
        text: "this file must stay browser-safe",
        baseline: null,
        pinned: false,
      });
      const theirs = yield* Record.write({
        name: "event",
        payload,
        signatures: [yield* sign(key, payload, NAMESPACE)],
        parents: [],
        message: `note.created ${rival.id}\n`,
      });
      // A fetch brings both under one ref.
      yield* joined(id, [mine.commit, theirs]);

      const projected = yield* Notes.project(id);
      assert.ok(projected !== null, "a note with two beginnings still projects");
      assert.equal(projected.state, "conflicted");
      if (projected.state !== "conflicted") return;
      assert.deepEqual(
        projected.competing.map((entry) => entry.event),
        ["note.created", "note.created"],
      );
      // Listed, which is the point: a reader can find it in order to settle it.
      assert.deepEqual(yield* Note.notes(), [id]);
      assert.equal((yield* Notes.all()).length, 1);
    }).pipe(scenario),
  );

  it.effect("names one creation's author beside that same creation's time", () =>
    Effect.gen(function* () {
      // Every field of a conflicted note is chosen by issue time, and none of
      // them by the walk's order. `createdBy`, `updatedAt` and `updatedBy` used
      // to come from `creations[0]` — first in *topological* order, which is
      // not a function of the records: it moves with the object ids, so two
      // replicas holding the same two creations named different authors, and
      // `createdAt` could sit beside a different event's signer. Running this
      // scenario repeatedly against the old fold gave both answers.
      const key = yield* generate("author@example.com");
      const theirKey = yield* generate("rival@example.com");
      const id = Note.newId();
      const mine = yield* Note.create({
        repo: REPO,
        path: "src/auth.ts",
        anchor: "function verify",
        text: "must stay constant-time",
        baseline: null,
        note: id,
        key,
      });

      const rival = { ...(yield* Note.context(REPO, id)), type: "note.created" } as const;
      const payload = Note.encode({
        ...rival,
        // Later than the note this scenario began with, whose clock sits at
        // the epoch — so the earliest creation is that one and the
        // topologically first is this one, and the two fields must still agree
        // about which record they describe.
        issuedAt: "2030-01-01T00:00:00.000Z",
        path: "src/token.ts",
        anchor: "@file",
        text: "this file must stay browser-safe",
        baseline: null,
        pinned: false,
      });
      const theirs = yield* Record.write({
        name: "event",
        payload,
        signatures: [yield* sign(theirKey, payload, NAMESPACE)],
        parents: [],
        message: `note.created ${rival.id}\n`,
      });
      yield* joined(id, [mine.commit, theirs]);

      const projected = yield* Notes.project(id);
      assert.ok(projected !== null);
      assert.equal(projected.state, "conflicted");
      if (projected.state !== "conflicted") return;
      // Asserted together rather than field by field, because the point is
      // that each pair describes one record: the earliest creation opened the
      // note, the latest is its most recent word, and neither may borrow the
      // other's author.
      assert.deepEqual(
        {
          createdAt: projected.createdAt,
          path: projected.path,
          createdBy: projected.createdBy,
          updatedAt: projected.updatedAt,
          updatedBy: projected.updatedBy,
        },
        {
          createdAt: "1970-01-01T00:00:00.000Z",
          path: "src/auth.ts",
          createdBy: yield* fingerprintKey(key.publicKey),
          updatedAt: "2030-01-01T00:00:00.000Z",
          updatedBy: yield* fingerprintKey(theirKey.publicKey),
        },
      );
    }).pipe(scenario),
  );

  it.effect("lists every note ref and projects none from an empty namespace", () =>
    Effect.gen(function* () {
      assert.deepEqual(yield* Note.notes(), []);
      assert.equal(yield* Notes.project(Note.newId()), null);
      const { note } = yield* started();
      assert.deepEqual(yield* Note.notes(), [note]);
      assert.equal((yield* Notes.all()).length, 1);
    }).pipe(scenario),
  );
});
