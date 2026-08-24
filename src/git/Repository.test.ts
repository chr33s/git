import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { stores } from "./Memory.ts";
import * as GitRepository from "./Repository.ts";
import { Hooks, Repository } from "./Repository.ts";
import { EMPTY_TREE_OID, encodeTree, type Signature } from "./Format.ts";
import { HookRejected } from "./Error.ts";
import { ObjectStore, type Oid, RefStore } from "./Store.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** Each test gets its own stores, so there is no shared global state to reset. */
const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | RefStore | ObjectStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        // `provideMerge` so the test and `Repository` share one store instance,
        // rather than relying on layer memoization to make that true.
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ),
  );

describe("Repository", () => {
  it.effect("commits onto an empty branch and reads it back", () =>
    Effect.promise(async () => {
      const commit = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const oid = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "first",
            author: alice,
          });
          return yield* repository.readCommit(oid);
        }),
      );

      assert.equal(commit.message, "first");
      assert.equal(commit.parents.length, 0);
      assert.equal(commit.author.email, "alice@example.com");
    }),
  );

  it.effect("searches literal text through the OID index without narrowing Unicode folds", () =>
    Effect.promise(async () => {
      const found = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const tree = yield* repository.writeFiles({
            changes: [
              { path: "a.txt", content: new TextEncoder().encode("Repository\\n") },
              { path: "unicode.txt", content: new TextEncoder().encode("Kelvin\\n") },
            ],
          });
          yield* repository.commit({ branch: "main", tree, message: "search", author: alice });

          // The first query warms the ASCII posting lists. The second must
          // still scan Kelvin sign: JavaScript folds it to ASCII `k`, while a
          // byte-only prefilter cannot prove that relationship.
          yield* repository.search({
            ref: "refs/heads/main",
            pattern: "repository",
            fixed: true,
            ignoreCase: true,
          });
          return yield* repository.search({
            ref: "refs/heads/main",
            pattern: "kelvin",
            fixed: true,
            ignoreCase: true,
          });
        }),
      );

      assert.deepEqual(
        found.matches.map((match) => [match.path, match.line]),
        [["unicode.txt", 1]],
      );
    }),
  );

  it.effect("finds literal text on the first search of a cold index", () =>
    Effect.promise(async () => {
      // The prefilter's candidate set is taken before the walk, so a blob first
      // read *during* that walk holds an ordinal the set cannot contain. Judged
      // against it anyway, every blob of an unwarmed index was rejected and the
      // first search of every repository answered nothing — the default path,
      // since the UI always asks fixed and case-insensitive.
      const found = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const tree = yield* repository.writeFiles({
            changes: [{ path: "a.txt", content: new TextEncoder().encode("hello world\n") }],
          });
          yield* repository.commit({ branch: "main", tree, message: "cold", author: alice });
          return yield* repository.search({
            ref: "refs/heads/main",
            pattern: "hello",
            fixed: true,
            ignoreCase: true,
          });
        }),
      );

      assert.deepEqual(
        found.matches.map((match) => [match.path, match.line, match.text]),
        [["a.txt", 1, "hello world"]],
      );
    }),
  );

  it.effect("answers a whole list of revisions from one ancestry walk", () =>
    Effect.promise(async () => {
      // What `isAncestor` asks once, offered to a caller with a list: same
      // answers, one walk. A revision the store does not hold ends its chain
      // rather than failing the walk, which is a shallow clone's ordinary shape.
      const { one, two, seen, side } = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const one = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });
          const two = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "two",
            author: alice,
          });
          // Off the branch, and named as a parent nothing holds so the walk has a
          // missing chain to step over as well.
          // SAFETY: forty hex characters is what an oid is, and this one names
          // nothing on purpose.
          const absent = "0".repeat(40) as Oid;
          const side = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [absent],
            message: "side",
            author: alice,
          });
          return { one, two, side, seen: yield* repository.ancestry([two]) };
        }),
      );

      assert.equal(seen.has(two), true);
      assert.equal(seen.has(one), true);
      assert.equal(seen.has(side), false);
    }),
  );

  it.effect("chains commits and walks the log newest first", () =>
    Effect.promise(async () => {
      const messages = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });
          const second = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "two",
            author: alice,
          });

          const commits = yield* Stream.runCollect(repository.log(second));
          return commits.map((commit) => commit.message);
        }),
      );

      assert.deepEqual(messages, ["two", "one"]);
    }),
  );

  it.effect("logs a history whose dates are unreadable rather than throwing", () =>
    Effect.promise(async () => {
      // A commit object carries whatever a client wrote, and a timestamp far
      // enough out of range makes a `Date` that is `NaN` — which equals nothing,
      // itself included. The walk took the newest date, kept the commits equal
      // to it (none of them), and reduced an empty array: `git log` on the whole
      // repository died with `Reduce of empty array` and named no commit.
      const messages = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const objects = yield* ObjectStore;

          const good = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "readable",
            author: alice,
          });
          // Written as bytes, because nothing in this codebase would produce it:
          // the seconds field is digits, as the format demands, and larger than
          // any date can hold.
          const odd = yield* objects.write({
            type: "commit",
            data: new TextEncoder().encode(
              [
                `tree ${EMPTY_TREE_OID}`,
                `parent ${good}`,
                "author Alice <alice@example.com> 99999999999999999999 +0000",
                "committer Alice <alice@example.com> 99999999999999999999 +0000",
                "",
                "from the far future",
              ].join("\n"),
            ),
          });

          const commits = yield* Stream.runCollect(repository.log(odd));
          return commits.map((commit) => commit.message);
        }),
      );

      // The tip first and its parent after it: an unreadable date reads as the
      // epoch, which is the oldest a commit can be, so it never sorts above the
      // history it sits on.
      assert.deepEqual(messages, ["from the far future", "readable"]);
    }),
  );

  it.effect("walks a commit with more parents than a spread can carry", () =>
    Effect.promise(async () => {
      // A commit object states its own parents and a client writes as many as
      // it likes. Handed to `push` as a spread, past the argument limit — around
      // a hundred thousand — the call throws a `RangeError`, which is not a
      // refusal a caller can catch by kind: every walk that met such a commit
      // died, `gc` and merge-base with it.
      const walked = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const objects = yield* ObjectStore;

          const root = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "root",
            author: alice,
          });
          // The same parent, repeated: what matters is the count handed to the
          // spread, and a hundred thousand distinct commits would be a fixture
          // nobody could run.
          const many = yield* objects.write({
            type: "commit",
            data: new TextEncoder().encode(
              [
                `tree ${EMPTY_TREE_OID}`,
                ...Array.from({ length: 200_000 }, () => `parent ${root}`),
                "author Alice <alice@example.com> 1700000000 +0000",
                "committer Alice <alice@example.com> 1700000000 +0000",
                "",
                "an improbable octopus",
              ].join("\n"),
            ),
          });

          yield* repository.setRef({ name: "refs/heads/main", to: many });

          // `gc` rather than `log`: the walk that decides what to keep is the
          // one where a `RangeError` costs objects, and a two-hundred-thousand
          // wide frontier is a slow thing to ask `log` to order.
          yield* repository.gc();
          return yield* objects.has(root);
        }),
      );

      assert.equal(walked, true, "the parent is still reachable, so it is still here");
    }),
  );

  it.effect("honours the log limit", () =>
    Effect.promise(async () => {
      const count = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          let head = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "0",
            author: alice,
          });
          for (let index = 1; index < 5; index++) {
            head = yield* repository.commit({
              branch: "main",
              tree: EMPTY_TREE_OID,
              message: String(index),
              author: alice,
            });
          }
          const commits = yield* Stream.runCollect(repository.log(head, { limit: 2 }));
          return commits.length;
        }),
      );

      assert.equal(count, 2);
    }),
  );

  it.effect("fails with RefConflict when the caller pinned a stale head", () =>
    Effect.promise(async () => {
      const error = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const first = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          const failure = yield* Effect.flip(
            repository.commit({
              branch: "main",
              tree: EMPTY_TREE_OID,
              message: "two",
              // stale on purpose: the branch exists now
              expected: null,
              author: alice,
            }),
          );

          return { failure, first };
        }),
      );

      assert.equal(error.failure._tag, "RefConflict");
      if (error.failure._tag === "RefConflict") {
        assert.equal(error.failure.actual, error.first);
        assert.equal(error.failure.expected, null);
      }
    }),
  );

  it.effect("creates a branch from a base ref and refuses to clobber it", () =>
    Effect.promise(async () => {
      const result = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const head = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          const created = yield* repository.branch({ name: "feature", base: "refs/heads/main" });
          const again = yield* Effect.flip(
            repository.branch({ name: "feature", base: "refs/heads/main" }),
          );

          return { again, created, head };
        }),
      );

      assert.equal(result.created, result.head);
      assert.equal(result.again._tag, "RefConflict");
    }),
  );

  it.effect("rejects a branch off an unknown base", () =>
    Effect.promise(async () => {
      const error = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          return yield* Effect.flip(repository.branch({ name: "x", base: "refs/heads/nope" }));
        }),
      );

      assert.equal(error._tag, "Invalid");
    }),
  );

  it.effect("lists refs after a commit", () =>
    Effect.promise(async () => {
      const refs = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });
          return yield* repository.refs;
        }),
      );

      assert.equal(refs.length, 1);
      assert.equal(refs[0]?.[0], "refs/heads/main");
    }),
  );

  it.effect("writes and reads a tree", () =>
    Effect.promise(async () => {
      const entries = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("hello\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
          return yield* repository.readTree(tree);
        }),
      );

      assert.equal(entries.length, 1);
      assert.equal(entries[0]?.name, "a.txt");
      assert.equal(entries[0]?.oid, "ce013625030ba8dba906f756967f9e9ca394464a");
    }),
  );

  it.effect("reads the empty tree without it having been written", () =>
    Effect.promise(async () => {
      const entries = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          return yield* repository.readTree(EMPTY_TREE_OID);
        }),
      );

      assert.deepEqual(entries, []);
    }),
  );

  it.effect("reports ObjectNotFound for a commit oid that is a blob", () =>
    Effect.promise(async () => {
      const error = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x"));
          return yield* Effect.flip(repository.readCommit(blob));
        }),
      );

      assert.equal(error._tag, "ObjectNotFound");
    }),
  );
});

describe("Repository.receive", () => {
  const withHooks = <A, E>(
    hooks: Layer.Layer<Hooks>,
    effect: Effect.Effect<A, E, Repository | RefStore>,
  ) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(GitRepository.layer.pipe(Layer.provide(hooks), Layer.provideMerge(stores))),
      ),
    );

  it.effect("applies a batch and reports each ref", () =>
    Effect.promise(async () => {
      const results = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const oid = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          return yield* repository.receive(
            [
              { name: "refs/heads/a", value: oid, expected: null },
              { name: "refs/heads/b", value: oid, expected: null },
            ],
            { atomic: true },
          );
        }),
      );

      assert.equal(results.length, 2);
      assert.ok(results.every((result) => result.ok));
    }),
  );

  it.effect("applies nothing when one ref in an atomic batch is stale", () =>
    Effect.promise(async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const refs = yield* RefStore;
          const oid = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          const results = yield* repository.receive(
            [
              { name: "refs/heads/a", value: oid, expected: null },
              // stale: main already points at `oid`
              { name: "refs/heads/main", value: oid, expected: null },
            ],
            { atomic: true },
          );

          return { a: yield* refs.read("refs/heads/a"), results };
        }),
      );

      assert.ok(state.results.every((result) => !result.ok));
      assert.equal(state.a, null, "the good ref in the batch must not have been written");
    }),
  );

  it.effect("applies the good refs when the batch is not atomic", () =>
    Effect.promise(async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const refs = yield* RefStore;
          const oid = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          const results = yield* repository.receive([
            { name: "refs/heads/a", value: oid, expected: null },
            { name: "refs/heads/main", value: oid, expected: null },
          ]);

          return { a: yield* refs.read("refs/heads/a"), results };
        }),
      );

      assert.equal(state.results.filter((result) => result.ok).length, 1);
      assert.notEqual(state.a, null);
    }),
  );

  it.effect("carries a hook rejection as a typed failure and writes nothing", () =>
    Effect.promise(async () => {
      const rejecting = Layer.succeed(Hooks, {
        preReceive: () =>
          Effect.fail(new HookRejected({ hook: "pre-receive", message: "denied by policy" })),
        update: () => Effect.void,
        postReceive: () => Effect.void,
      });

      const outcome = await withHooks(
        rejecting,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const refs = yield* RefStore;
          // SAFETY: forty zeros are a well-formed oid; that it names no object is
          // the point — the rejecting hook must fire before anything reads it.
          const failure = yield* Effect.flip(
            repository.receive([
              { name: "refs/heads/a", value: "0".repeat(40) as Oid, expected: null },
            ]),
          );
          return { a: yield* refs.read("refs/heads/a"), failure };
        }),
      );

      assert.equal(outcome.failure._tag, "HookRejected");
      if (outcome.failure._tag === "HookRejected") {
        assert.equal(outcome.failure.message, "denied by policy");
      }
      assert.equal(outcome.a, null);
    }),
  );

  it.effect("replaces a directory with a file in one call", () =>
    Effect.promise(async () => {
      const files = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const base = yield* repository.writeFiles({
            changes: [
              { path: "a/b.txt", content: new TextEncoder().encode("nested\n") },
              { path: "keep.txt", content: new TextEncoder().encode("keep\n") },
            ],
          });

          // Emptying the directory and writing a file over its name in one
          // batch: the tree for `a` is loaded by the first change, and must not
          // be written back over the blob the second one puts there.
          const tree = yield* repository.writeFiles({
            base,
            changes: [
              { path: "a/b.txt", content: null },
              { path: "a", content: new TextEncoder().encode("now a file\n") },
            ],
          });

          return yield* repository.listFiles(tree);
        }),
      );

      assert.deepEqual(
        files.map((file) => file.path),
        ["a", "keep.txt"],
      );
      assert.equal(files.find((file) => file.path === "a")?.mode, "100644");
    }),
  );

  it.effect("replaces a directory with a file whichever order the changes arrive in", () =>
    Effect.promise(async () => {
      const files = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const base = yield* repository.writeFiles({
            changes: [
              { path: "a/b.txt", content: new TextEncoder().encode("nested\n") },
              { path: "keep.txt", content: new TextEncoder().encode("keep\n") },
            ],
          });

          // The replacement first, the tidy-up second — the ordering that made
          // the emptied directory's removal take the new file with it.
          const tree = yield* repository.writeFiles({
            base,
            changes: [
              { path: "a", content: new TextEncoder().encode("now a file\n") },
              { path: "a/b.txt", content: null },
            ],
          });

          return yield* repository.listFiles(tree);
        }),
      );

      assert.deepEqual(
        files.map((file) => file.path),
        ["a", "keep.txt"],
      );
    }),
  );

  it.effect("keeps two tree entries whose names are different bytes but one decode", () =>
    Effect.promise(async () => {
      const files = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x\n"));

          // Two Latin-1 names, `café.txt` and `cafè.txt`. Both decode to the
          // same U+FFFD spelling, so a tree keyed by the decoded name keeps one
          // and the next commit that rewrites the tree drops the other.
          const named = (last: number) => ({
            mode: "100644",
            name: "caf\uFFFD.txt",
            oid: blob,
            raw: Uint8Array.from([0x63, 0x61, 0x66, last, 0x2e, 0x74, 0x78, 0x74]),
          });
          const base = yield* repository.writeTree([named(0xe9), named(0xe8)]);

          // An unrelated write, of the kind a merge or an API commit makes.
          const tree = yield* repository.writeFiles({
            base,
            changes: [{ path: "other.txt", content: new TextEncoder().encode("new\n") }],
          });
          return (yield* repository.readTree(tree)).length;
        }),
      );

      assert.equal(files, 3, "a tracked file was dropped by an unrelated commit");
    }),
  );

  it.effect("replaces a non-UTF-8 entry rather than writing a second one", () =>
    Effect.promise(async () => {
      const names = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x\n"));
          const base = yield* repository.writeTree([
            {
              mode: "100644",
              name: "caf\uFFFD.txt",
              oid: blob,
              raw: Uint8Array.from([0x63, 0x61, 0x66, 0xe9, 0x2e, 0x74, 0x78, 0x74]),
            },
          ]);

          // The only spelling a caller has is the decoded one; writing it must
          // displace the entry it names, or the tree holds two entries with the
          // same encoded name — which git reads as corrupt.
          const tree = yield* repository.writeFiles({
            base,
            changes: [{ path: "caf\uFFFD.txt", content: new TextEncoder().encode("new\n") }],
          });
          return (yield* repository.readTree(tree)).map((entry) => entry.name);
        }),
      );

      assert.deepEqual(names, ["caf\uFFFD.txt"]);
    }),
  );

  it.effect("rewrites a directory holding a mode it would not have written", () =>
    Effect.promise(async () => {
      const files = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const objects = yield* ObjectStore;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x\n"));

          // `100664` is a mode git itself reads, and `receive-pack` unpacks a
          // tree carrying one without ever consulting `writeTree`. Written here
          // the same way it would arrive: as an object, not through the API that
          // validates what a caller asked for.
          const inner = yield* objects.write({
            type: "tree",
            data: encodeTree([
              { mode: "100664", name: "legacy.txt", oid: blob },
              { mode: "100644", name: "ok.txt", oid: blob },
            ]),
          });
          const base = yield* objects.write({
            type: "tree",
            data: encodeTree([{ mode: "40000", name: "dir", oid: inner }]),
          });

          // Validating the whole rebuilt directory would judge `legacy.txt` —
          // which this caller did not touch and has no way to fix — and every
          // commit, merge and rebase anywhere near `dir/` would fail from here on.
          const tree = yield* repository.writeFiles({
            base,
            changes: [{ path: "dir/ok.txt", content: new TextEncoder().encode("new\n") }],
          });

          const [directory] = yield* repository.readTree(tree);
          return yield* repository.readTree(directory!.oid);
        }),
      );

      assert.deepEqual(
        files.map((entry) => [entry.name, entry.mode]),
        [
          ["legacy.txt", "100664"],
          ["ok.txt", "100644"],
        ],
      );
    }),
  );

  it.effect("keeps a non-UTF-8 directory's bytes when a file inside it changes", () =>
    Effect.promise(async () => {
      const raw = Uint8Array.from([0x63, 0x61, 0x66, 0xe9]);
      const entries = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x\n"));
          const inner = yield* repository.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
          const base = yield* repository.writeTree([
            { mode: "40000", name: "caf\uFFFD", oid: inner, raw },
          ]);

          // Writing inside the directory must not rename the directory — every
          // file beneath it would move to a path nothing asked for.
          const tree = yield* repository.writeFiles({
            base,
            changes: [{ path: "caf\uFFFD/b.txt", content: new TextEncoder().encode("new\n") }],
          });
          return yield* repository.readTree(tree);
        }),
      );

      assert.equal(entries.length, 1);
      assert.deepEqual(entries[0]?.raw, raw);
    }),
  );

  it.effect("writes every level of a nested path, not only the one holding the file", () =>
    Effect.promise(async () => {
      const files = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("deep\n"));
          // `a/` has no direct file child, so a bottom-up pass that only knew
          // about `a/b` never wrote `a` and never linked it into the root —
          // the commit recorded the empty tree and the file was gone.
          const root = yield* repository.writePaths([
            { path: "a/b/c.txt", oid: blob, mode: "100644" },
          ]);
          return yield* repository.listFiles(root);
        }),
      );

      assert.deepEqual(
        files.map((file) => file.path),
        ["a/b/c.txt"],
      );
    }),
  );

  it.effect("refuses a tree entry that names the repository or escapes it", () =>
    Effect.promise(async () => {
      const refused = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x\n"));
          const bad = (name: string) =>
            Effect.flip(repository.writeTree([{ mode: "100644", name, oid: blob }])).pipe(
              Effect.map((error) => error._tag),
            );

          return {
            dotGit: yield* bad(".git"),
            upper: yield* bad(".GIT"),
            parent: yield* bad(".."),
            slash: yield* bad("a/b"),
            duplicate: yield* Effect.flip(
              repository.writeTree([
                { mode: "100644", name: "same", oid: blob },
                { mode: "100644", name: "same", oid: blob },
              ]),
            ).pipe(Effect.map((error) => error._tag)),
          };
        }),
      );

      assert.deepEqual(refused, {
        dotGit: "Invalid",
        duplicate: "Invalid",
        parent: "Invalid",
        slash: "Invalid",
        upper: "Invalid",
      });
    }),
  );

  it.effect("reads a zero-padded directory mode as the directory it is", () =>
    Effect.promise(async () => {
      const files = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("kept\n"));
          const inner = yield* repository.writeTree([
            { mode: "100644", name: "keep.txt", oid: blob },
          ]);

          // `040000` is what git's own fsck calls `zeroPaddedFilemode`, and real
          // history carries it. Read as a file, the directory starts empty and
          // everything under it disappears from the next commit.
          const base = yield* repository.writeTree([{ mode: "040000", name: "dir", oid: inner }]);
          const tree = yield* repository.writeFiles({
            base,
            changes: [{ path: "other.txt", content: new TextEncoder().encode("new\n") }],
          });
          return yield* repository.listFiles(tree);
        }),
      );

      assert.deepEqual(files.map((file) => file.path).sort(), ["dir/keep.txt", "other.txt"]);
    }),
  );

  it.effect(
    "finds a path through a zero-padded directory, and writes a zero-padded mode back",
    () =>
      Effect.promise(async () => {
        const found = await scenario(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const blob = yield* repository.writeBlob(new TextEncoder().encode("kept\n"));
            const inner = yield* repository.writeTree([
              // `0100644` is a file mode git also writes, and `writeFiles` refused
              // it — so a repository holding one could be read and merged but
              // never committed back.
              { mode: "0100644", name: "keep.txt", oid: blob },
            ]);
            const base = yield* repository.writeTree([{ mode: "040000", name: "dir", oid: inner }]);

            // Walking to `dir/keep.txt` stops at `dir` when `040000` is compared
            // as a string, and the file endpoint answers 404 for a file that
            // `GET /files` plainly lists.
            const entry = yield* repository.findPath(base, "dir/keep.txt");

            // And the same mode has to survive a write: the merge and rebase
            // paths carry a tree's own modes straight back into `writeFiles`.
            const rewritten = yield* repository.writeFiles({
              base,
              changes: [
                {
                  path: "dir/keep.txt",
                  content: new TextEncoder().encode("changed\n"),
                  mode: "0100644",
                },
              ],
            });

            return { entry, files: yield* repository.listFiles(rewritten) };
          }),
        );

        assert.equal(found.entry?.name, "keep.txt");
        assert.deepEqual(
          found.files.map((file) => file.path),
          ["dir/keep.txt"],
        );
      }),
  );

  it.effect("keeps a submodule the tree it rewrites never had content for", () =>
    Effect.promise(async () => {
      const result = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("top\n"));
          // SAFETY: a gitlink names a commit in *another* repository, so this
          // oid is deliberately one no object here will ever have — `never`
          // states that nothing in this store answers to it.
          const submodule = "1".repeat(40) as never;
          const base = yield* repository.writeTree([
            { mode: "100644", name: "readme.md", oid: blob },
            { mode: "160000", name: "vendor", oid: submodule },
          ]);

          const listed = yield* repository.listFiles(base);
          // Rebuilding the tree from the listing is what checkout, merge and
          // rebase all do; an entry missing from the listing is an entry
          // deleted from the next commit with no error and no conflict.
          const tree = yield* repository.writePaths(
            listed.map((file) => ({ path: file.path, oid: file.oid, mode: file.mode })),
          );

          return { listed, tree, base };
        }),
      );

      assert.deepEqual(
        result.listed.map((file) => `${file.mode} ${file.path}`),
        ["100644 readme.md", "160000 vendor"],
      );
      assert.equal(result.tree, result.base);
    }),
  );

  it.effect("packs a commit whose tree holds a zero-padded gitlink", () =>
    Effect.promise(async () => {
      const packed = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("top\n"));
          // `0160000` is a gitlink however it is spelled, and the commit it
          // names is in another repository. Walked as a local object — which
          // comparing the mode as a string did — every clone and fetch of this
          // repository fails on an object it was never supposed to have.
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "readme.md", oid: blob },
            // SAFETY: as above — a gitlink's oid belongs to another repository.
            { mode: "0160000", name: "vendor", oid: "1".repeat(40) as never },
          ]);
          const oid = yield* repository.commit({
            branch: "main",
            tree,
            message: "with a submodule",
            author: alice,
          });

          return yield* Stream.runCollect(repository.packOf([oid], []));
        }),
      );

      assert.equal(packed.length > 0, true);
    }),
  );

  it.effect("plans a shallow fetch whose want is an annotated tag", () =>
    Effect.promise(async () => {
      const planned = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const first = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });
          const tree = yield* repository.writeFiles({
            changes: [{ path: "f.txt", content: new TextEncoder().encode("x\n") }],
          });
          const second = yield* repository.commit({
            branch: "main",
            tree,
            message: "two",
            author: alice,
          });
          const tag = yield* repository.tag({
            name: "v1",
            target: "refs/heads/main",
            message: "release",
          });

          // What `git clone --depth 1` sends when the repository has tags: the
          // want is the tag object, and the walk that deepens is over commits.
          const plan = yield* repository.fetch({ wants: [tag.oid], haves: [], depth: 1 });
          return { first, plan, second, tag: tag.oid };
        }),
      );

      assert.equal(planned.plan.oids.includes(planned.tag), true, "the tag object itself");
      assert.equal(planned.plan.oids.includes(planned.second), true, "the commit it names");
      assert.equal(planned.plan.oids.includes(planned.first), false, "depth 1 cuts the parent");
      assert.deepEqual(planned.plan.shallow, [planned.second]);
    }),
  );

  it.effect("runs post-receive with the results", () =>
    Effect.promise(async () => {
      const seen: string[] = [];
      const recording = Layer.succeed(Hooks, {
        preReceive: () => Effect.void,
        update: () => Effect.void,
        postReceive: (results) =>
          Effect.sync(() => {
            for (const result of results) seen.push(result.ref);
          }),
      });

      await withHooks(
        recording,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const oid = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });
          return yield* repository.receive([{ name: "refs/heads/a", value: oid, expected: null }]);
        }),
      );

      assert.deepEqual(seen, ["refs/heads/a"]);
    }),
  );
});

describe("Repository.canServe", () => {
  /** main: a <- b <- c, and an unrelated root on `side`. */
  const history = Effect.gen(function* () {
    const repository = yield* Repository;
    const commitOn = (branch: string, message: string) =>
      repository.commit({ branch, tree: EMPTY_TREE_OID, message, author: alice });
    const a = yield* commitOn("main", "a");
    const b = yield* commitOn("main", "b");
    const c = yield* commitOn("main", "c");
    const side = yield* commitOn("side", "elsewhere");
    return { repository, a, b, c, side };
  });

  it.effect("is true when every want reaches a common commit", () =>
    Effect.promise(async () => {
      const served = await scenario(
        Effect.gen(function* () {
          const { repository, a, c } = yield* history;
          return yield* repository.canServe([c], [a]);
        }),
      );
      assert.equal(served, true);
    }),
  );

  it.effect("is false with nothing common, and false past an unreachable want", () =>
    Effect.promise(async () => {
      const [none, unreachable] = await scenario(
        Effect.gen(function* () {
          const { repository, a, b, c, side } = yield* history;
          return [
            yield* repository.canServe([c], []),
            // `side` shares no history with main: no common set built from
            // main's commits can cover it.
            yield* repository.canServe([c, side], [a, b]),
          ];
        }),
      );
      assert.equal(none, false);
      assert.equal(unreachable, false);
    }),
  );

  it.effect("treats a want that is itself common as reached", () =>
    Effect.promise(async () => {
      const served = await scenario(
        Effect.gen(function* () {
          const { repository, c } = yield* history;
          return yield* repository.canServe([c], [c]);
        }),
      );
      assert.equal(served, true);
    }),
  );

  it.effect("peels an annotated tag want to the commit it names", () =>
    Effect.promise(async () => {
      const served = await scenario(
        Effect.gen(function* () {
          const { repository, b, c } = yield* history;
          const tag = yield* repository.tag({
            name: "v1",
            target: c,
            message: "release",
            tagger: alice,
          });
          return yield* repository.canServe([tag.oid], [b]);
        }),
      );
      assert.equal(served, true);
    }),
  );
});

describe("hooks composed as one", () => {
  it.effect("refuses as soon as any of them refuses", () =>
    Effect.promise(async () => {
      // A refusal is an answer, and the rest of the chain has nothing to add to
      // it — nor should a hook that comes after a "no" get to run its side
      // effects on a push that is not happening.
      const ran: string[] = [];
      const chain = GitRepository.hooksAll([
        {
          preReceive: () => Effect.sync(() => ran.push("first")).pipe(Effect.asVoid),
          update: () => Effect.void,
          postReceive: () => Effect.void,
        },
        {
          preReceive: () => Effect.fail(new HookRejected({ hook: "pre-receive", message: "no" })),
          update: () => Effect.void,
          postReceive: () => Effect.void,
        },
        {
          preReceive: () => Effect.sync(() => ran.push("third")).pipe(Effect.asVoid),
          update: () => Effect.void,
          postReceive: () => Effect.void,
        },
      ]);

      const outcome = await Effect.runPromise(
        chain.preReceive([]).pipe(
          Effect.as("allowed"),
          Effect.catchTag("HookRejected", (error) => Effect.succeed(error.message)),
        ),
      );

      assert.equal(outcome, "no");
      assert.deepEqual(ran, ["first"], "and the one after the refusal never ran");
    }),
  );

  it.effect("tells every hook about a push even when one of them dies", () =>
    Effect.promise(async () => {
      // `postReceive` has no error channel, so the only way it stops is a defect
      // — and a defect that skipped the rest would be silent. This is what stood
      // between a webhook delivery going wrong and a mirror never being told a
      // push had landed.
      const told: string[] = [];
      const chain = GitRepository.hooksAll([
        {
          preReceive: () => Effect.void,
          update: () => Effect.void,
          postReceive: () => Effect.sync(() => told.push("first")).pipe(Effect.asVoid),
        },
        {
          preReceive: () => Effect.void,
          update: () => Effect.void,
          postReceive: () =>
            Effect.sync(() => {
              throw new Error("the receiver blew up");
            }),
        },
        {
          preReceive: () => Effect.void,
          update: () => Effect.void,
          postReceive: () => Effect.sync(() => told.push("third")).pipe(Effect.asVoid),
        },
      ]);

      await Effect.runPromise(chain.postReceive([]));

      assert.deepEqual(told, ["first", "third"]);
    }),
  );
});
