/**
 * The path index, and the property that makes it safe to keep: nothing reads
 * differently because of it.
 *
 * §29 lets a cache exist here and forbids correctness from depending on it, so
 * every test below asks the same question twice — once with the index in the
 * state under test, once with no index at all — and requires the two answers
 * to agree. A cache that changes an answer is not a cache.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid } from "../git/Store.ts";
import * as Note from "./Note.ts";
import * as Index from "./NoteIndex.ts";
import * as Notes from "./NoteProjection.ts";
import { covers } from "./NoteAudit.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provideMerge(stores)),
    ),
  );

/** The same, with every object read counted: a memo that saves a walk has to. */
const counting = (reads: Array<string>) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const inner = yield* ObjectStore;
      return ObjectStore.of({
        ...inner,
        read: (oid) =>
          Effect.andThen(
            Effect.sync(() => {
              reads.push(oid);
            }),
            inner.read(oid),
          ),
      });
    }),
  ).pipe(Layer.provideMerge(stores));

const watched = <A, E>(reads: Array<string>, effect: Effect.Effect<A, E, Repository>) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provideMerge(counting(reads)),
      ),
    ),
  );

const REPO = "SHA256:test";

const noteOn = Effect.fn("test.noteOn")(function* (path: string, key: PrivateKey) {
  const { note } = yield* Note.create({
    repo: REPO,
    path,
    anchor: "@file",
    text: `${path} matters`,
    baseline: null,
    key,
  });
  return note;
});

/** What the full fold says, which is the answer the index may only match. */
const authoritative = Effect.fn("test.authoritative")(function* (query?: string) {
  return (yield* Notes.all())
    .filter((note) => covers(query, note.path))
    .map((note) => note.id)
    .sort();
});

const selected = Effect.fn("test.selected")(function* (query?: string) {
  return (yield* Index.select(query)).map((note) => note.id).sort();
});

describe("the note path index", () => {
  it.effect("answers a path query identically with and without one", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const auth = yield* noteOn("src/auth.ts", key);
      const parse = yield* noteOn("src/parse.ts", key);
      const readme = yield* noteOn("docs/readme.md", key);

      // No index yet: every answer comes from the full fold.
      assert.deepEqual(yield* selected("src/"), [auth, parse].sort());
      assert.deepEqual(yield* selected(), [auth, parse, readme].sort());

      yield* Index.refresh();
      assert.deepEqual(yield* selected("src/"), yield* authoritative("src/"));
      assert.deepEqual(yield* selected("src/auth.ts"), [auth]);
      assert.deepEqual(yield* selected("docs"), [readme]);
      assert.deepEqual(yield* selected(), yield* authoritative());
    }).pipe(scenario),
  );

  it.effect("falls back to the fold when a note ref has moved under it", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const auth = yield* noteOn("src/auth.ts", key);
      yield* Index.refresh();

      // Written after the index, so the index does not know it exists.
      const late = yield* noteOn("src/late.ts", key);
      assert.deepEqual(yield* selected("src/"), [auth, late].sort());
      assert.deepEqual(yield* selected("src/"), yield* authoritative("src/"));

      // And a note whose ref advanced without any new note appearing: the
      // count matches, the oids do not, and the answer still has to be right.
      yield* Index.refresh();
      yield* Note.retire({ repo: REPO, note: late, reason: "moved", key });
      const after = yield* Index.select("src/");
      assert.deepEqual(after.map((note) => note.id).sort(), yield* authoritative("src/"));
      assert.equal(after.find((note) => note.id === late)?.active, false);
    }).pipe(scenario),
  );

  it.effect("reads an unparseable index as no index at all", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const auth = yield* noteOn("src/auth.ts", key);
      yield* Index.refresh();

      const repository = yield* Repository;
      const blob = yield* repository.writeBlob(new TextEncoder().encode("{ not json"));
      const tree = yield* repository.writeTree([{ mode: "100644", name: "notes.json", oid: blob }]);
      const commit = yield* repository.commitTree({
        tree,
        parents: [],
        message: "corrupt\n",
        author: { name: "T", email: "t@e.com", at: new Date(0), offset: 0 },
      });
      yield* repository.setRef({ name: Index.INDEX_REF, to: commit });

      assert.equal(yield* Index.read(), null);
      assert.deepEqual(yield* selected("src/"), [auth]);
    }).pipe(scenario),
  );

  it.effect("never lets the index put a note somewhere the fold does not", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      const auth = yield* noteOn("src/auth.ts", key);
      const built = yield* Index.refresh();

      // An index that claims the note is somewhere it is not, with the ref
      // oids left untouched so the staleness check passes. `refresh` cannot
      // produce this — only a hand edit can, and a hand that can edit this
      // blob can edit the note refs beside it — but the re-check against the
      // fold means it costs a missed hit rather than a wrong answer.
      const repository = yield* Repository;
      const lying = { ...built, paths: { "docs/elsewhere.md": [auth] } };
      const blob = yield* repository.writeBlob(
        new TextEncoder().encode(`${JSON.stringify(lying)}\n`),
      );
      const tree = yield* repository.writeTree([{ mode: "100644", name: "notes.json", oid: blob }]);
      const commit = yield* repository.commitTree({
        tree,
        parents: [],
        message: "stale\n",
        author: { name: "T", email: "t@e.com", at: new Date(0), offset: 0 },
      });
      yield* repository.setRef({ name: Index.INDEX_REF, to: commit });

      // The lie cannot put the note under `docs/`, which is the direction
      // that would make a reader act on a constraint about another file.
      assert.deepEqual(yield* selected("docs/"), []);
      assert.deepEqual(yield* authoritative("docs/"), []);
    }).pipe(scenario),
  );

  it.effect("records every note ref, so a fresh index is not immediately stale", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      yield* noteOn("src/auth.ts", key);
      const built = yield* Index.refresh();
      const held = yield* Index.state();
      assert.deepEqual([...built.refs.keys()].sort(), [...held.keys()].sort());
      for (const [id, oid] of held) assert.equal(built.refs.get(id), oid);
    }).pipe(scenario),
  );

  it.effect("does not re-fold a repository whose note refs have not moved", () =>
    Effect.gen(function* () {
      const reads: string[] = [];
      yield* watched(
        reads,
        Effect.gen(function* () {
          Index.forget();
          const key = yield* generate("author@example.com");
          for (const path of ["src/a.ts", "src/b.ts", "src/c.ts"]) yield* noteOn(path, key);

          reads.length = 0;
          yield* Index.select();
          const first = reads.length;
          assert.ok(first > 0, "the first ask reads the note DAGs");

          reads.length = 0;
          yield* Index.select();
          yield* Index.select("src/");
          // `GET /hub/why` is charged only `repo.read`, so an anonymous client
          // can ask it in a loop; unmemoised, each ask re-walked every note's
          // DAG and re-verified every signature on it.
          assert.equal(reads.length, 0, "a repeated ask re-reads nothing");

          // Until something moves, which is what makes a stale answer
          // impossible rather than merely unlikely.
          const late = yield* noteOn("src/d.ts", key);
          reads.length = 0;
          const after = yield* Index.select();
          assert.ok(reads.length > 0, "a moved ref is a miss");
          assert.ok(after.some((note) => note.id === late));
        }),
      );
    }),
  );

  it.effect("re-folds only the notes whose refs moved when rebuilding", () =>
    Effect.gen(function* () {
      const reads: string[] = [];
      yield* watched(
        reads,
        Effect.gen(function* () {
          Index.forget();
          const key = yield* generate("author@example.com");
          for (const path of ["src/a.ts", "src/b.ts", "src/c.ts", "src/e.ts"]) {
            yield* noteOn(path, key);
          }

          // A full build, with no stored index to reuse.
          reads.length = 0;
          const whole = yield* Index.build();
          const full = reads.length;
          assert.ok(full > 0);
          yield* Index.refresh();

          // One more note: one ref moved, and only that one is worth folding.
          yield* noteOn("src/d.ts", key);
          reads.length = 0;
          const built = yield* Index.refresh();
          const incremental = reads.length;

          assert.ok(
            incremental < full,
            `rebuilding cost ${incremental} reads where a full fold costs ${full}`,
          );
          // And says exactly what a full fold of the same refs says.
          Index.forget();
          const authoritative = yield* Notes.all();
          assert.deepEqual(
            [...built.paths.keys()].sort(),
            [...new Set(authoritative.map((note) => note.path))].sort(),
          );
          assert.deepEqual(
            [...built.refs.keys()].sort(),
            [...(yield* Index.state()).keys()].sort(),
          );
          void whole;
        }),
      );
    }),
  );

  it.effect("survives a path that names a property of Object.prototype", () =>
    Effect.gen(function* () {
      // Paths and note ids are whoever-wrote-them strings. Kept in a plain
      // object, `paths["constructor"] ??= []` found an inherited function,
      // declined to assign and threw on `.push` — taking every note mutation
      // down with it — while `refs["__proto__"] = oid` was dropped silently
      // and left the index reading as stale for good.
      const key = yield* generate("author@example.com");
      for (const path of ["constructor", "toString", "__proto__", "src/ordinary.ts"]) {
        yield* noteOn(path, key);
      }
      const built = yield* Index.refresh();
      assert.deepEqual([...built.paths.keys()].sort(), [
        "__proto__",
        "constructor",
        "src/ordinary.ts",
        "toString",
      ]);

      // Round-tripped through JSON and still recognised as current, which is
      // the half a fallback would hide: a dropped `__proto__` key makes the
      // stored index read as stale, and `select` then answers correctly from
      // the full fold while the cache never works again.
      const reread = yield* Index.read();
      assert.ok(reread !== null);
      const byKey = (left: readonly [string, unknown], right: readonly [string, unknown]) =>
        left[0].localeCompare(right[0]);
      assert.deepEqual(
        [...reread.paths.keys()].sort((left, right) => left.localeCompare(right)),
        [...built.paths.keys()].sort((left, right) => left.localeCompare(right)),
      );
      assert.deepEqual(
        [...reread.refs.entries()].sort(byKey),
        [...built.refs.entries()].sort(byKey),
      );

      // And the cache still answers what the full fold answers.
      for (const path of ["constructor", "__proto__", "src/ordinary.ts"]) {
        Index.forget();
        assert.deepEqual(
          (yield* Index.select(path)).map((note) => note.id).sort(),
          yield* authoritative(path),
        );
      }
    }).pipe(scenario),
  );

  it.effect("falls back to the fold when the stored index points at nothing", () =>
    Effect.gen(function* () {
      const key = yield* generate("author@example.com");
      yield* noteOn("src/auth.ts", key);
      yield* Index.refresh();

      // A ref left pointing at an object the store no longer holds, which is
      // what a prune or a restore under a live ref leaves behind. `read` used
      // to fail on it, and the failure took out `git+ why` and every note read
      // over the API — a cache correctness may not depend on.
      const repository = yield* Repository;
      const real = yield* repository.writeBlob(new TextEncoder().encode("real\n"));
      // SAFETY: `Oid` is a branded hex string, and this is a real oid from this
      // store with its last character changed — same length, same alphabet, and
      // nothing written under it, which is exactly the dangling target wanted.
      const missing = `${real.slice(0, -1)}${real.endsWith("0") ? "1" : "0"}` as Oid;
      const orphan = yield* repository.commitTree({
        tree: yield* repository.writeTree([{ mode: "100644", name: "notes.json", oid: missing }]),
        parents: [],
        message: "gone\n",
        author: { name: "t", email: "t@localhost", at: new Date(0), offset: 0 },
      });
      yield* repository.setRef({
        name: Index.INDEX_REF,
        to: orphan,
        expected: yield* repository.readRef(Index.INDEX_REF),
      });

      Index.forget();
      assert.equal(yield* Index.read(), null);
      assert.deepEqual(
        (yield* Index.select("src/auth.ts")).map((note) => note.id).sort(),
        yield* authoritative("src/auth.ts"),
      );
    }).pipe(scenario),
  );
});
