/**
 * The JSON API, driven through its own derived client.
 *
 * `HttpApiTest` builds a client from the same `HttpApi` value the server
 * implements and runs it against the handlers in-process — no socket, no
 * spawned server. What this buys is the no-drift property: a payload or
 * error-shape change that broke the client would fail to compile here, and a
 * `RefConflict` comes back as a typed value in the failure channel, not as a
 * status code to interpret.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, FileSystem, Layer, Path } from "effect";
import { Etag, HttpPlatform, HttpRouter } from "effect/unstable/http";
import { HttpApiTest } from "effect/unstable/httpapi";

import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Api from "./Api.ts";
import * as Subscribers from "./Subscribers.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const live = Layer.mergeAll(
  Api.handlers,
  HttpPlatform.layer.pipe(Layer.provide(FileSystem.layerNoop({}))),
  Etag.layerWeak,
  FileSystem.layerNoop({}),
  Path.layer,
).pipe(Layer.provideMerge(repository), Layer.provideMerge(Subscribers.memory));

/**
 * The handlers reach `Repository` and `Subscribers` through the request
 * context: a host discharges those `Request<"Requires", …>` markers by
 * handing its layer to `HttpRouter.toWebHandler`, which resolves them from
 * the layer's outputs. `HttpApiTest` dispatches in-process instead and
 * resolves the same services from the ambient context — where `live` merges
 * them — so the markers are satisfied at dispatch, just not anywhere the
 * type system can watch it happen.
 *
 * SAFETY: `live` merges `Repository` and `Subscribers` into the test context,
 * which is exactly where the in-process dispatch resolves these request-scoped
 * markers; the cast erases what every dispatched request already receives.
 */
const dispatched = <E>(
  effect: Effect.Effect<
    void,
    E,
    | HttpRouter.Request<"Requires", GitRepository.Repository>
    | HttpRouter.Request<"Requires", Subscribers.Subscribers>
  >,
): Effect.Effect<void, E> => effect as Effect.Effect<void, E>;

const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

/**
 * `it.live`, not `it.effect`: the latter installs a `TestClock` whose
 * time never advances on its own, and `Repository.commit` retries a
 * `RefConflict` behind a 10ms schedule — the conflict assertion below
 * would wait forever. The win is the same either way: the test body *is*
 * an Effect, so there is no `runPromise` at the edge and a failure is
 * reported as a `Cause` with its fiber trace.
 */
describe("Api", () => {
  it.live("drives the derived client end to end, typed errors included", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const created = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "first", author: alice },
        });
        assert.match(created.oid, /^[0-9a-f]{40}$/);

        const read = yield* client.repo.read({ params: { repo: "r", oid: created.oid } });
        assert.equal(read.message, "first");
        assert.deepEqual(read.parents, []);

        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "second", author: alice },
        });
        const log = yield* client.repo.log({ params: { repo: "r", oid: second.oid } });
        assert.deepEqual(
          log.commits.map((commit) => commit.message),
          ["second", "first"],
        );

        const refs = yield* client.repo.refs({ params: { repo: "r" } });
        assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: second.oid }]);

        // Paged endpoints: the cursor walks, `has_more` closes.
        const firstPage = yield* client.repo.commits({
          params: { repo: "r", oid: second.oid },
          query: { limit: "1" },
        });
        assert.deepEqual(
          firstPage.items.map((commit) => commit.message),
          ["second"],
        );
        assert.equal(firstPage.has_more, true);
        const nextPage = yield* client.repo.commits({
          params: { repo: "r", oid: second.oid },
          query: { limit: "1", cursor: firstPage.next_cursor! },
        });
        assert.deepEqual(
          nextPage.items.map((commit) => commit.message),
          ["first"],
        );
        assert.equal(nextPage.has_more, false);

        // `limit=0` asks for a page that advances the cursor by nothing. Left
        // unclamped it answered with no items, `has_more`, and the very cursor
        // it was handed — a client following `next_cursor` never terminates.
        const zero = yield* client.repo.commits({
          params: { repo: "r", oid: second.oid },
          query: { limit: "0" },
        });
        assert.equal(zero.items.length, 1);
        assert.notEqual(zero.next_cursor, "0");

        // Branch creation, and the paged branch list that follows it.
        const created2 = yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "feature", base: "refs/heads/main" },
        });
        assert.equal(created2.name, "refs/heads/feature");
        assert.equal(created2.oid, second.oid);

        const branches = yield* client.repo.branches({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          branches.items.map((ref) => ref.name),
          ["refs/heads/feature", "refs/heads/main"],
        );
        assert.equal(branches.has_more, false);

        // Creating it twice is a conflict, typed.
        const conflict2 = yield* client.repo
          .branch({
            params: { repo: "r" },
            payload: { name: "feature", base: "refs/heads/main" },
          })
          .pipe(Effect.flip);
        assert.equal(conflict2._tag, "RefConflict");

        // The failure channel carries the domain error, decoded.
        const conflict = yield* client.repo
          .create({
            params: { repo: "r" },
            payload: { message: "third", author: alice, expected: null },
          })
          .pipe(Effect.flip);
        assert.equal(conflict._tag, "RefConflict");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("commits real content, and reads it back", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        // The path the API exists for: files in, a commit whose tree holds
        // them out. Nested, because a flat tree would not exercise the
        // bottom-up write.
        const first = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "add sources",
            author: alice,
            files: [
              { path: "readme.md", content: "hello\n" },
              { path: "src/index.ts", content: "export const answer = 42;\n" },
              { path: "src/lib/util.ts", content: "export const noop = () => {};\n" },
            ],
          },
        });

        const root = yield* client.repo.readTree({ params: { repo: "r", oid: first.tree } });
        assert.deepEqual(
          root.entries.map((entry) => entry.name),
          ["readme.md", "src"],
        );
        const src = root.entries.find((entry) => entry.name === "src")!;
        assert.equal(src.mode, "40000");

        const inner = yield* client.repo.readTree({ params: { repo: "r", oid: src.oid } });
        assert.deepEqual(
          inner.entries.map((entry) => entry.name),
          ["index.ts", "lib"],
        );

        const blob = inner.entries.find((entry) => entry.name === "index.ts")!;
        const content = yield* client.repo.readBlob({ params: { repo: "r", oid: blob.oid } });
        assert.equal(atob(content.content), "export const answer = 42;\n");
        assert.equal(content.size, 26);

        // A second commit carries the first's tree forward: touching one path
        // must not drop the others.
        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "edit one file",
            author: alice,
            files: [{ path: "src/index.ts", content: "export const answer = 43;\n" }],
          },
        });
        const nextRoot = yield* client.repo.readTree({ params: { repo: "r", oid: second.tree } });
        assert.deepEqual(
          nextRoot.entries.map((entry) => entry.name),
          ["readme.md", "src"],
        );
        assert.equal(
          nextRoot.entries.find((entry) => entry.name === "readme.md")!.oid,
          root.entries.find((entry) => entry.name === "readme.md")!.oid,
        );

        // Removing the last file in a directory removes the directory: git
        // has no empty trees.
        const third = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "drop lib",
            author: alice,
            files: [{ path: "src/lib/util.ts", content: null }],
          },
        });
        const afterSrc = yield* client.repo.readTree({
          params: {
            repo: "r",
            oid: (yield* client.repo.readTree({
              params: { repo: "r", oid: third.tree },
            })).entries.find((entry) => entry.name === "src")!.oid,
          },
        });
        assert.deepEqual(
          afterSrc.entries.map((entry) => entry.name),
          ["index.ts"],
        );

        // Binary survives the round trip, which is what base64 is for.
        const bytes = new Uint8Array([0, 1, 2, 250, 251, 252]);
        let binary = "";
        for (const byte of bytes) binary += String.fromCharCode(byte);
        const written = yield* client.repo.blob({
          params: { repo: "r" },
          payload: { content: btoa(binary), encoding: "base64" },
        });
        const readBack = yield* client.repo.readBlob({
          params: { repo: "r", oid: written.oid },
        });
        assert.equal(readBack.content, btoa(binary));

        // A tree can also be stated outright, and a commit can name it.
        const tree = yield* client.repo.tree({
          params: { repo: "r" },
          payload: { files: [{ path: "only.txt", content: "one\n" }] },
        });
        const explicit = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "explicit tree", author: alice, tree: tree.oid },
        });
        assert.equal(explicit.tree, tree.oid);

        // A path that escapes the root is refused rather than normalised.
        const escaped = yield* client.repo
          .create({
            params: { repo: "r" },
            payload: {
              message: "nope",
              author: alice,
              files: [{ path: "../outside", content: "x" }],
            },
          })
          .pipe(Effect.flip);
        assert.equal(escaped._tag, "Invalid");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("tags, annotated and lightweight, and checks its own integrity", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const commit = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "release", author: alice, files: [{ path: "a", content: "a\n" }] },
        });

        // Annotated: a tag object of its own, so the ref points at the tag
        // rather than at the commit.
        const annotated = yield* client.repo.tagCreate({
          params: { repo: "r" },
          payload: {
            name: "v1.0.0",
            target: "refs/heads/main",
            message: "first release\n",
            tagger: alice,
          },
        });
        assert.equal(annotated.ref, "refs/tags/v1.0.0");
        assert.equal(annotated.target, commit.oid);
        assert.notEqual(annotated.oid, commit.oid);

        const read = yield* client.repo.tagRead({ params: { repo: "r", oid: annotated.oid } });
        assert.equal(read.tag, "v1.0.0");
        assert.equal(read.type, "commit");
        assert.equal(read.object, commit.oid);
        assert.equal(read.message, "first release\n");

        // Lightweight: no message, so the ref points straight at the commit.
        const light = yield* client.repo.tagCreate({
          params: { repo: "r" },
          payload: { name: "latest", target: "refs/heads/main" },
        });
        assert.equal(light.oid, commit.oid);

        const tags = yield* client.repo.tags({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          tags.items.map((tag) => tag.name),
          ["refs/tags/latest", "refs/tags/v1.0.0"],
        );

        // A tag is meant to be stable: replacing one is opt-in.
        const clash = yield* client.repo
          .tagCreate({
            params: { repo: "r" },
            payload: { name: "latest", target: "refs/heads/main" },
          })
          .pipe(Effect.flip);
        assert.equal(clash._tag, "RefConflict");

        const forced = yield* client.repo.tagCreate({
          params: { repo: "r" },
          payload: { name: "latest", target: "refs/heads/main", force: true },
        });
        assert.equal(forced.oid, commit.oid);

        // Everything written so far hashes to its own name.
        const report = yield* client.repo.fsck({ params: { repo: "r" } });
        assert.equal(report.ok, true);
        assert.deepEqual(report.problems, []);
        assert.deepEqual(report.dangling_refs, []);
        assert.equal(report.checked > 0, true);

        const removed = yield* client.repo.tagRemove({ params: { repo: "r", name: "latest" } });
        assert.equal(removed.deleted, true);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("merges: fast-forward, clean three-way, and a reported conflict", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const base = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "base",
            author: alice,
            files: [
              { path: "shared.txt", content: "one\ntwo\nthree\n" },
              { path: "untouched.txt", content: "stable\n" },
            ],
          },
        });

        yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "feature", base: "refs/heads/main" },
        });

        // Only the branch moves: main can fast-forward onto it.
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "feature",
            message: "add a file",
            author: alice,
            files: [{ path: "new.txt", content: "added\n" }],
          },
        });

        const forward = yield* client.repo.merge({
          params: { repo: "r" },
          payload: {
            ours: "refs/heads/main",
            theirs: "refs/heads/feature",
            into: "refs/heads/main",
          },
        });
        assert.equal(forward.kind, "fast-forward");
        assert.equal(forward.base, base.oid);

        const afterForward = yield* client.repo.files({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          afterForward.files.map((file) => file.path),
          ["new.txt", "shared.txt", "untouched.txt"],
        );

        // Merging again has nothing to do.
        const idempotent = yield* client.repo.merge({
          params: { repo: "r" },
          payload: { ours: "refs/heads/main", theirs: "refs/heads/feature" },
        });
        assert.equal(idempotent.kind, "up-to-date");

        // Now diverge on different files: a clean three-way merge.
        yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "side", base: "refs/heads/main" },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "side",
            message: "theirs edits shared",
            author: alice,
            files: [{ path: "shared.txt", content: "one\nTWO\nthree\n" }],
          },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "main",
            message: "ours adds another",
            author: alice,
            files: [{ path: "ours.txt", content: "mine\n" }],
          },
        });

        const merged = yield* client.repo.merge({
          params: { repo: "r" },
          payload: {
            ours: "refs/heads/main",
            theirs: "refs/heads/side",
            author: alice,
            into: "refs/heads/main",
          },
        });
        assert.equal(merged.kind, "merged");
        assert.deepEqual(merged.conflicts, []);

        // The merge commit has both parents, which is what makes it a merge.
        const commit = yield* client.repo.read({
          params: { repo: "r", oid: merged.commit! },
        });
        assert.equal(commit.parents.length, 2);

        // Their edit to shared.txt survived, and our file is still there.
        const shared = yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: "refs/heads/main", path: "shared.txt" },
        });
        assert.equal(atob(shared.content), "one\nTWO\nthree\n");
        yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: "refs/heads/main", path: "ours.txt" },
        });

        // Both sides edit the same line: a conflict, reported not thrown.
        yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "clash", base: "refs/heads/main" },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "clash",
            message: "theirs",
            author: alice,
            files: [{ path: "shared.txt", content: "one\nTHEIRS\nthree\n" }],
          },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "main",
            message: "ours",
            author: alice,
            files: [{ path: "shared.txt", content: "one\nOURS\nthree\n" }],
          },
        });

        const conflicted = yield* client.repo.merge({
          params: { repo: "r" },
          payload: { ours: "refs/heads/main", theirs: "refs/heads/clash", author: alice },
        });
        assert.equal(conflicted.kind, "conflicted");
        assert.deepEqual(
          conflicted.conflicts.map((conflict) => [conflict.path, conflict.reason]),
          [["shared.txt", "content"]],
        );
        assert.equal(conflicted.commit, null);

        // A conflicted merge still writes a tree, with the markers in it —
        // otherwise there is nothing for a caller to resolve against.
        const markers = yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: conflicted.tree!, path: "shared.txt" },
        });
        assert.match(atob(markers.content), /<<<<<<</);
        assert.match(atob(markers.content), />>>>>>>/);

        // Choosing a side resolves it without markers.
        const theirs = yield* client.repo.merge({
          params: { repo: "r" },
          payload: {
            ours: "refs/heads/main",
            theirs: "refs/heads/clash",
            author: alice,
            strategy: "theirs",
          },
        });
        assert.equal(theirs.kind, "merged");
        const resolved = yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: theirs.tree!, path: "shared.txt" },
        });
        assert.equal(atob(resolved.content), "one\nTHEIRS\nthree\n");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("diffs two revisions as unified patches", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const first = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "first",
            author: alice,
            files: [
              { path: "kept.txt", content: "same\n" },
              { path: "edited.txt", content: "one\ntwo\nthree\n" },
              { path: "removed.txt", content: "bye\n" },
            ],
          },
        });

        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "second",
            author: alice,
            files: [
              { path: "edited.txt", content: "one\nTWO\nthree\n" },
              { path: "removed.txt", content: null },
              { path: "added.txt", content: "new\n" },
            ],
          },
        });

        const diff = yield* client.repo.diff({
          params: { repo: "r" },
          payload: { from: first.oid, to: second.oid },
        });

        // Unchanged files are absent; that is what makes a diff a diff.
        assert.deepEqual(
          diff.files.map((file) => [file.path, file.status]),
          [
            ["added.txt", "added"],
            ["edited.txt", "modified"],
            ["removed.txt", "removed"],
          ],
        );

        const edited = diff.files.find((file) => file.path === "edited.txt")!;
        assert.match(edited.patch, /^--- a\/edited\.txt$/m);
        assert.match(edited.patch, /^\+\+\+ b\/edited\.txt$/m);
        assert.match(edited.patch, /^-two$/m);
        assert.match(edited.patch, /^\+TWO$/m);
        assert.match(edited.patch, /^@@ -1,3 \+1,3 @@$/m);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("reads the tree by path: files, one file, raw objects, reflog and grep", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const commit = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "seed",
            author: alice,
            files: [
              { path: "readme.md", content: "# Title\nhello world\n" },
              { path: "src/a.ts", content: "export const a = 1;\nconst hello = 2;\n" },
              { path: "src/deep/b.ts", content: "export const b = 3;\n" },
            ],
          },
        });

        // Paths, recursively, from a ref rather than a tree oid — which is
        // how a caller who has a branch name and no oids asks.
        const all = yield* client.repo.files({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          all.files.map((file) => file.path),
          ["readme.md", "src/a.ts", "src/deep/b.ts"],
        );

        const scoped = yield* client.repo.files({
          params: { repo: "r" },
          query: { path: "src" },
        });
        assert.deepEqual(
          scoped.files.map((file) => file.path),
          ["src/a.ts", "src/deep/b.ts"],
        );

        const file = yield* client.repo.file({
          params: { repo: "r" },
          query: { path: "src/a.ts" },
        });
        assert.equal(atob(file.content), "export const a = 1;\nconst hello = 2;\n");
        assert.equal(file.mode, "100644");

        const missing = yield* client.repo
          .file({ params: { repo: "r" }, query: { path: "nope.txt" } })
          .pipe(Effect.flip);
        assert.equal(missing._tag, "ObjectNotFound");

        // The raw object, whatever its type — the escape hatch for a caller
        // that knows git's model.
        const raw = yield* client.repo.object({ params: { repo: "r", oid: commit.oid } });
        assert.equal(raw.type, "commit");
        assert.match(atob(raw.content), /^tree [0-9a-f]{40}/);

        const log = yield* client.repo.reflog({
          params: { repo: "r" },
          query: { ref: "refs/heads/main" },
        });
        assert.equal(log.entries.length, 1);
        assert.equal(log.entries[0]!.to, commit.oid);
        assert.equal(log.entries[0]!.from, null);

        const found = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello" },
        });
        assert.deepEqual(
          found.matches.map((match) => [match.path, match.line]),
          [
            ["readme.md", 2],
            ["src/a.ts", 2],
          ],
        );
        assert.equal(found.truncated, false);

        const cased = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "HELLO", ignore_case: true },
        });
        assert.equal(cased.matches.length, 2);

        const scopedGrep = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello", path: "src/" },
        });
        assert.deepEqual(
          scopedGrep.matches.map((match) => match.path),
          ["src/a.ts"],
        );

        // A prefix stops at a path boundary: `src` is a directory, not the
        // first three characters of one, so `src-generated/` is not under it.
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "a sibling directory",
            author: alice,
            files: [{ path: "src-generated/g.ts", content: "const hello = 9;\n" }],
          },
        });
        const anchored = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello", path: "src" },
        });
        assert.deepEqual(
          anchored.matches.map((match) => match.path),
          ["src/a.ts"],
        );

        // A cap, and an honest flag when it bites.
        const capped = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "e", max_matches: 1 },
        });
        assert.equal(capped.matches.length, 1);
        assert.equal(capped.truncated, true);

        // A bad regex is the caller's mistake, not a 500.
        const bad = yield* client.repo
          .grep({ params: { repo: "r" }, payload: { pattern: "([unclosed" } })
          .pipe(Effect.flip);
        assert.equal(bad._tag, "Invalid");

        // …and the same string as a fixed pattern is fine.
        const literal = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "([unclosed", fixed: true },
        });
        assert.deepEqual(literal.matches, []);

        // A file too large to hold three times over — the bytes, the decoded
        // string and the array of lines — is named as skipped rather than
        // read, decoded and split inside a worker with 128 MiB.
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "a big one",
            author: alice,
            files: [{ path: "big.log", content: "hello padding\n".repeat(400_000) }],
          },
        });
        const big = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello" },
        });
        assert.deepEqual(big.skipped, ["big.log"]);
        assert.equal(
          big.matches.some((match) => match.path === "big.log"),
          false,
        );
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("collects what no ref can reach, and nothing else", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const kept = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "keep",
            author: alice,
            files: [{ path: "kept.txt", content: "k\n" }],
          },
        });

        // A blob written but never committed is exactly what gc is for.
        const orphan = yield* client.repo.blob({
          params: { repo: "r" },
          payload: { content: "nobody references this\n" },
        });

        const dry = yield* client.repo.gc({ params: { repo: "r" }, payload: { dry_run: true } });
        assert.deepEqual(dry.removed, [orphan.oid]);
        // A dry run reports and keeps.
        yield* client.repo.readBlob({ params: { repo: "r", oid: orphan.oid } });

        const swept = yield* client.repo.gc({ params: { repo: "r" }, payload: {} });
        assert.deepEqual(swept.removed, [orphan.oid]);
        // A repack that was not asked for is not a refusal, and says so — the
        // field exists so a caller can tell "nothing to pack" from "would not".
        assert.equal(swept.repack_skipped, null);

        const gone = yield* client.repo
          .readBlob({ params: { repo: "r", oid: orphan.oid } })
          .pipe(Effect.flip);
        assert.equal(gone._tag, "ObjectNotFound");

        // Everything reachable survived, and the repository still checks out.
        const commit = yield* client.repo.read({ params: { repo: "r", oid: kept.oid } });
        assert.equal(commit.message, "keep");
        const report = yield* client.repo.fsck({ params: { repo: "r" } });
        assert.equal(report.ok, true);

        // A second pass has nothing left to do.
        const again = yield* client.repo.gc({ params: { repo: "r" }, payload: {} });
        assert.deepEqual(again.removed, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("registers, lists and removes webhooks without ever echoing the secret", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const added = yield* client.repo.webhookAdd({
          params: { repo: "r" },
          payload: { url: "https://example.test/hook", secret: "a-long-enough-secret" },
        });
        assert.equal(added.url, "https://example.test/hook");
        // The response shape has no secret field at all — it cannot leak.
        assert.equal("secret" in added, false);

        const listed = yield* client.repo.webhookList({ params: { repo: "r" } });
        assert.deepEqual(
          listed.webhooks.map((hook) => hook.url),
          ["https://example.test/hook"],
        );
        assert.equal("secret" in listed.webhooks[0]!, false);

        // A receiver that cannot be reached securely is refused at
        // registration, not discovered at delivery.
        const insecure = yield* client.repo
          .webhookAdd({
            params: { repo: "r" },
            payload: { url: "http://example.test/hook", secret: "a-long-enough-secret" },
          })
          .pipe(Effect.flip);
        assert.equal(insecure._tag, "Invalid");

        const weak = yield* client.repo
          .webhookAdd({
            params: { repo: "r" },
            payload: { url: "https://example.test/hook", secret: "short" },
          })
          .pipe(Effect.flip);
        assert.equal(weak._tag, "Invalid");

        const removed = yield* client.repo.webhookRemove({
          params: { repo: "r", id: added.id },
        });
        assert.equal(removed.deleted, true);

        const again = yield* client.repo.webhookRemove({
          params: { repo: "r", id: added.id },
        });
        assert.equal(again.deleted, false);

        const empty = yield* client.repo.webhookList({ params: { repo: "r" } });
        assert.deepEqual(empty.webhooks, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );
});
