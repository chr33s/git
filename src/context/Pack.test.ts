/**
 * Repository views and the evidence that resolves from them.
 *
 * The acceptance criteria this file answers (docs/context-pack.md §15): a
 * clean checkout yields the committed tree, a dirty one yields an exact
 * overlay, and every blob and gitlink verifies against `view.tree` by its own
 * kind-specific rule. The negative cases matter as much: an object that exists
 * but is unreachable from the view, a gitlink pointed at a file, and an
 * instruction claim whose root is somebody else's tree all have to be refused
 * for their own reasons rather than for one shared one.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { indexMemory, IndexStore, workTreeMemory, WorkTree } from "../git/Work.ts";
import * as Checkout from "../git/Checkout.ts";
import * as Pack from "./Pack.ts";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const encode = (text: string) => new TextEncoder().encode(text);

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
  Layer.provideMerge(indexMemory),
  Layer.provideMerge(workTreeMemory),
);

const scenario = <A, E>(
  effect: Effect.Effect<A, E, Repository | WorkTree | IndexStore>,
): Promise<A> => Effect.runPromise(effect.pipe(Effect.provide(world)));

/** A checkout with two files committed on `main`, and HEAD pointing at it. */
const checkout = Effect.fn("test.checkout")(function* () {
  const repository = yield* Repository;
  const work = yield* WorkTree;

  yield* work.write("src/auth.ts", encode("export const authorize = () => true\n"), 0o100644);
  yield* work.write("AGENTS.md", encode("Standing instructions.\n"), 0o100644);
  yield* Checkout.add(["."]);
  const made = yield* Checkout.commit({ message: "first\n", author });
  return { repository, commit: made.oid };
});

describe("Repository View", () => {
  it.effect("names the committed tree for a clean checkout", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const view = yield* Pack.capture(commit);
          const info = yield* repository.readCommit(commit);
          // §4.2: for a clean worktree, view.tree is HEAD's own root tree —
          // and here that is a fact about how the overlay is built, not a
          // branch in the code.
          assert.equal(view.base, Pack.qualify(commit));
          assert.equal(view.tree, Pack.qualify(info.tree));
        }),
      ),
    ),
  );

  it.effect("builds an exact overlay tree for a dirty checkout", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const work = yield* WorkTree;

          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = () => false\n"),
            0o100644,
          );
          const view = yield* Pack.capture(commit);
          const info = yield* repository.readCommit(commit);

          // The base still names the commit — ancestry is unaffected — and
          // the tree is emphatically not HEAD's.
          assert.equal(view.base, Pack.qualify(commit));
          assert.notEqual(view.tree, Pack.qualify(info.tree));

          const tree = Pack.unqualify(view.tree)!;
          const entry = yield* repository.findPath(tree, "src/auth.ts");
          const bytes = yield* repository.readBlob(entry!.oid);
          assert.equal(new TextDecoder().decode(bytes), "export const authorize = () => false\n");
        }),
      ),
    ),
  );

  it.effect("peels an annotated tag to the commit it names", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const tag = yield* repository.tag({
            name: "v1.0",
            message: "release\n",
            target: commit,
            tagger: author,
          });

          // `resolveRev` hands back what the ref holds, and an annotated tag
          // holds a tag object. Read straight as a commit this failed with an
          // object-type error instead of building a view of the tagged commit.
          // The tag *object*, which is what `resolveRev` hands back for an
          // annotated tag — not the commit it resolves to.
          const view = yield* Pack.committed(tag.oid);
          const info = yield* repository.readCommit(commit);
          assert.equal(view.tree, Pack.qualify(info.tree));
          // And `base` names the commit, not the tag: ancestry is asked about
          // the commit, and a tag oid is not something it can be walked from.
          assert.equal(view.base, Pack.qualify(commit));
        }),
      ),
    ),
  );

  it.effect("peels a tag that names another tag", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const first = yield* repository.tag({
            name: "v1.0",
            message: "release\n",
            target: commit,
            tagger: author,
          });
          // `git tag -a v2 v1`, which holds a tag whose object is a tag. One
          // level of peeling handed that second tag to `readCommit`, whose
          // type check reports `ObjectNotFound` — so `context for --rev v2`
          // died saying the repository does not hold an object it does hold.
          const second = yield* repository.tag({
            name: "v2.0",
            message: "re-tagged\n",
            target: first.oid,
            tagger: author,
          });

          const view = yield* Pack.committed(second.oid);
          const info = yield* repository.readCommit(commit);
          assert.equal(view.tree, Pack.qualify(info.tree));
          assert.equal(view.base, Pack.qualify(commit));
        }),
      ),
    ),
  );

  it.effect("names no submodule pointer while the submodule is conflicted", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { commit } = yield* checkout();
          const index = yield* IndexStore;
          const at = (message: string) =>
            repository.commitTree({ tree: EMPTY_TREE_OID, parents: [], message, author });
          const base = yield* at("base\n");
          const ours = yield* at("ours\n");

          // Three stages for one path, which is what a submodule-pointer
          // conflict looks like. The loop keeps the *first* entry for an
          // unmerged path — stage 1, the merge base — and for a regular file
          // that is harmless because the bytes are re-read from disk. A
          // gitlink has no bytes to re-read, so the view named the commit the
          // submodule was at before the merge rather than the one the checkout
          // is on, and `checkItem` verified that wrong pointer forever.
          const held = yield* index.load;
          const like = held[0]!;
          yield* index.save([
            ...held,
            { ...like, path: "vendor/lib", oid: base, mode: 0o160000, stage: 1 },
            { ...like, path: "vendor/lib", oid: ours, mode: 0o160000, stage: 2 },
          ]);

          const view = yield* Pack.capture(commit);
          const found = yield* repository.findPath(Pack.unqualify(view.tree)!, "vendor/lib");
          // A pointer nobody has agreed on is not one an agent was shown.
          assert.equal(found, null);
        }),
      ),
    ),
  );

  it.effect("refuses an item carrying a field its kind does not own", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit } = yield* checkout();
          const view = yield* Pack.committed(commit);
          const of = (item: Readonly<object>) =>
            new TextEncoder().encode(JSON.stringify({ version: 1, view, items: [item] }));

          // `Schema.Union` drops an excess property silently, so this decoded
          // to a clean blob while the persisted bytes — what `payload.pack`
          // commits to, and what any other implementation reads — went on
          // asserting a submodule pointer for a path that holds a file.
          // `checkItem` never looks at a field the schema discarded, so the
          // audit reported the whole thing verified.
          const crossed = yield* Pack.decode(
            of({
              kind: "blob",
              path: "src/auth.ts",
              blob: `sha1:${"0".repeat(40)}`,
              commit: `sha1:${"1".repeat(40)}`,
            }),
          ).pipe(Effect.flip);
          assert.match(crossed.reason, /may not carry commit/);

          // And the other way, which dropped an instruction-authority claim
          // from the audit while the signed bytes kept it.
          const authority = yield* Pack.decode(
            of({
              kind: "gitlink",
              path: "vendor/lib",
              commit: `sha1:${"1".repeat(40)}`,
              authority: { source: "repository-instructions", root: view.tree, path: "AGENTS.md" },
            }),
          ).pipe(Effect.flip);
          assert.match(authority.reason, /may not carry authority/);

          // But a field this version has simply not heard of is read, not
          // refused. Rejecting every unknown key made a pack from a newer
          // producer decode nowhere — `audit` reporting no readable pack and
          // `ok: false`, permanently, on a ref nothing can remove — which is
          // the over-strictness this module avoids everywhere else. What §5.2
          // forbids is a cross-kind *claim*.
          const newer = yield* Pack.decode(
            of({
              kind: "blob",
              path: "src/auth.ts",
              blob: `sha1:${"0".repeat(40)}`,
              confidence: "high",
            }),
          );
          assert.equal(newer.items.length, 1);
        }),
      ),
    ),
  );

  it.effect("refuses evidence that does not resolve under the view", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { commit } = yield* checkout();
          const view = yield* Pack.committed(commit);

          // A blob this repository holds, at a path the view knows, that is
          // not the blob the view has there. `Select.render` turns these bytes
          // into the exact segments hashed into the signed `renderDigest`, so
          // a pack somebody else built could commit to bytes the view never
          // had — the check that would catch it lives in `checkItem`, which is
          // not on this path.
          const other = yield* repository.writeBlob(encode("something else\n"));
          const refused = yield* Pack.evidence(view, {
            kind: "blob",
            path: "src/auth.ts",
            blob: Pack.qualify(other),
          }).pipe(Effect.flip);
          assert.equal(refused._tag, "Invalid");
        }),
      ),
    ),
  );

  it.effect("names the empty tree when nothing is tracked", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const commit = yield* repository.commitTree({
            tree: yield* repository.writeTree([]),
            parents: [],
            message: "root\n",
            author,
          });
          // A checkout with nothing in it is a view with nothing in it, and
          // that has to resolve rather than fail: the empty tree is a real
          // tree, and an exposure may legitimately retain it.
          const view = yield* Pack.capture(commit);
          assert.equal(view.tree, Pack.qualify(EMPTY_TREE_OID));
          assert.equal((yield* Pack.verify({ version: 1, view, items: [] })).ok, true);
        }),
      ),
    ),
  );

  it.effect("takes the file on disk when the index holds a conflict", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const work = yield* WorkTree;
          const index = yield* IndexStore;

          // What `git merge` leaves behind: no stage 0, three sides in the
          // index, and conflict markers in the working file.
          const conflicted = "<<<<<<< ours\nmine\n=======\ntheirs\n>>>>>>>\n";
          yield* work.write("src/auth.ts", encode(conflicted), 0o100644);
          const held = yield* index.load;
          const entry = held.find((candidate) => candidate.path === "src/auth.ts")!;
          const theirs = yield* repository.writeBlob(encode("theirs\n"));
          yield* index.save([
            ...held.filter((candidate) => candidate.path !== "src/auth.ts"),
            { ...entry, stage: 1 },
            { ...entry, stage: 2 },
            { ...entry, oid: theirs, stage: 3 },
          ]);

          const view = yield* Pack.capture(commit);
          const tree = Pack.unqualify(view.tree)!;
          const found = yield* repository.findPath(tree, "src/auth.ts");
          const bytes = yield* repository.readBlob(found!.oid);
          // The bytes retrieval can actually read, not whichever stage the
          // index happened to list last.
          assert.equal(new TextDecoder().decode(bytes), conflicted);
        }),
      ),
    ),
  );

  it.effect("drops a tracked path that is not on disk", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const work = yield* WorkTree;
          yield* work.remove("AGENTS.md");

          const view = yield* Pack.capture(commit);
          const tree = Pack.unqualify(view.tree)!;
          // Retrieval cannot read it, so the view must not say that it could.
          assert.equal(yield* repository.findPath(tree, "AGENTS.md"), null);
          assert.notEqual(yield* repository.findPath(tree, "src/auth.ts"), null);
        }),
      ),
    ),
  );
});

describe("Context Pack evidence", () => {
  it.effect("verifies a blob that resolves under the view", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const view = yield* Pack.capture(commit);
          const tree = Pack.unqualify(view.tree)!;
          const entry = yield* repository.findPath(tree, "src/auth.ts");

          const report = yield* Pack.verify({
            version: 1,
            view,
            items: [{ kind: "blob", path: "src/auth.ts", blob: Pack.qualify(entry!.oid) }],
          });
          assert.equal(report.view.ok, true);
          assert.equal(report.ok, true);
        }),
      ),
    ),
  );

  it.effect("refuses a blob that exists but is unreachable from the view", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit } = yield* checkout();
          const repository = yield* Repository;
          const view = yield* Pack.capture(commit);
          // In the object database, and at no path under this tree.
          const stranger = yield* repository.writeBlob(encode("not in this view\n"));

          const report = yield* Pack.verify({
            version: 1,
            view,
            items: [{ kind: "blob", path: "src/auth.ts", blob: Pack.qualify(stranger) }],
          });
          assert.equal(report.ok, false);
          assert.match(
            report.items[0]!.evidence.ok ? "" : report.items[0]!.evidence.reason,
            /holds sha1:/,
          );
        }),
      ),
    ),
  );

  it.effect("refuses a path the view does not hold", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const view = yield* Pack.capture(commit);
          const tree = Pack.unqualify(view.tree)!;
          const entry = yield* repository.findPath(tree, "src/auth.ts");

          const report = yield* Pack.verify({
            version: 1,
            view,
            items: [{ kind: "blob", path: "src/gone.ts", blob: Pack.qualify(entry!.oid) }],
          });
          assert.equal(report.ok, false);
        }),
      ),
    ),
  );

  it.effect("holds ranges to the blob they are ranges into", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const view = yield* Pack.capture(commit);
          const tree = Pack.unqualify(view.tree)!;
          const entry = yield* repository.findPath(tree, "src/auth.ts");
          const blob = Pack.qualify(entry!.oid);

          const report = yield* Pack.verify({
            version: 1,
            view,
            items: [
              { kind: "blob", path: "src/auth.ts", blob, range: [0, 6] },
              // Past the end of the blob.
              { kind: "blob", path: "src/auth.ts", blob, range: [0, 100_000] },
            ],
          });
          assert.equal(report.items[0]!.evidence.ok, true);
          assert.equal(report.items[1]!.evidence.ok, false);

          const bytes = yield* Pack.evidence(view, {
            kind: "blob",
            path: "src/auth.ts",
            blob,
            range: [0, 6],
          });
          assert.equal(new TextDecoder().decode(bytes), "export");
        }),
      ),
    ),
  );

  it("refuses empty and inverted ranges by construction", () => {
    assert.equal(Pack.inRange(0, 1, 10), true);
    assert.equal(Pack.inRange(0, 0, 10), false);
    assert.equal(Pack.inRange(5, 4, 10), false);
    assert.equal(Pack.inRange(-1, 4, 10), false);
    assert.equal(Pack.inRange(0, 11, 10), false);
  });

  it("widens a range that would cut a codepoint", () => {
    // "é" is two bytes; a range ending between them is unrenderable as text.
    const bytes = encode("aéb");
    assert.equal(Pack.splitsCodepoint(bytes, 0, 2), true);
    assert.deepEqual(Pack.snap(bytes, 0, 2), [0, 3]);
    assert.equal(Pack.splitsCodepoint(bytes, 0, 3), false);
  });

  it.effect("verifies a gitlink only at mode 160000", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const submodule = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "submodule\n",
            author,
          });
          const blob = yield* repository.writeBlob(encode("a file\n"));
          const tree = yield* repository.writePaths([
            { path: "vendor/policy-engine", oid: submodule, mode: "160000" },
            { path: "src/auth.ts", oid: blob, mode: "100644" },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "with a submodule\n",
            author,
          });
          const view = yield* Pack.committed(commit);

          const report = yield* Pack.verify({
            version: 1,
            view,
            items: [
              {
                kind: "gitlink",
                path: "vendor/policy-engine",
                commit: Pack.qualify(submodule),
              },
              // A gitlink item pointed at an ordinary file: the path resolves,
              // and it is not a submodule, which is the distinction §5.2 keeps.
              { kind: "gitlink", path: "src/auth.ts", commit: Pack.qualify(submodule) },
              // And a blob item pointed at the gitlink, the other way round.
              {
                kind: "blob",
                path: "vendor/policy-engine",
                blob: Pack.qualify(submodule),
              },
            ],
          });
          assert.equal(report.items[0]!.evidence.ok, true);
          assert.equal(report.items[1]!.evidence.ok, false);
          assert.equal(report.items[2]!.evidence.ok, false);
        }),
      ),
    ),
  );
});

describe("instruction provenance", () => {
  it.effect("verifies a claim rooted in this pack's own view", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { commit, repository } = yield* checkout();
          const view = yield* Pack.capture(commit);
          const tree = Pack.unqualify(view.tree)!;
          const entry = yield* repository.findPath(tree, "AGENTS.md");
          const blob = Pack.qualify(entry!.oid);

          const report = yield* Pack.verify({
            version: 1,
            view,
            items: [
              {
                kind: "blob",
                path: "AGENTS.md",
                blob,
                role: "instruction",
                authority: {
                  source: "repository-instructions",
                  root: view.tree,
                  path: "AGENTS.md",
                },
              },
              {
                kind: "blob",
                path: "AGENTS.md",
                blob,
                role: "instruction",
                // Somebody else's tree: §7 requires the root to be this view.
                authority: {
                  source: "repository-instructions",
                  root: Pack.qualify(EMPTY_TREE_OID),
                  path: "AGENTS.md",
                },
              },
            ],
          });

          assert.equal(report.items[0]!.authority?.ok, true);
          assert.equal(report.items[1]!.authority?.ok, false);
          // The bad claim does not discredit the bytes underneath it: an
          // invalid annotation is an unverified instruction claim, not
          // invalidation of the evidence item.
          assert.equal(report.items[1]!.evidence.ok, true);
          assert.equal(report.ok, true);
        }),
      ),
    ),
  );
});

describe("pack serialization", () => {
  it.effect("round-trips every field it can carry", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          // Deliberately maximal. The encoder fixes field order with one
          // global property list, so a field missing from that list is
          // dropped silently — which is a pack that verifies as a pack with
          // fewer claims than the producer made. Only a value carrying every
          // field catches it.
          const pack: Pack.Pack = {
            version: 1,
            view: { base: Pack.qualify(EMPTY_TREE_OID), tree: Pack.qualify(EMPTY_TREE_OID) },
            selector: { name: "repo-context", version: "1.0.0" },
            items: [
              {
                kind: "blob",
                path: "a.ts",
                blob: Pack.qualify(EMPTY_TREE_OID),
                range: [1, 2],
                role: "implementation",
                reason: "search",
                symbol: "authorize",
                authority: {
                  source: "repository-instructions",
                  root: Pack.qualify(EMPTY_TREE_OID),
                  path: "a.ts",
                },
              },
              {
                kind: "gitlink",
                path: "vendor/x",
                commit: Pack.qualify(EMPTY_TREE_OID),
                role: "dependency",
                reason: "import",
                symbol: "policyEngine",
              },
            ],
            omissions: [
              { path: "b.ts", reason: "budget" },
              { reason: "filtered", count: 3 },
            ],
          };
          const bytes = Pack.encode(pack);
          const decoded = yield* Pack.decode(bytes);
          assert.deepEqual(decoded, pack);
          // The bytes are the identity, so re-encoding what was just decoded
          // has to reproduce them exactly.
          assert.deepEqual([...Pack.encode(decoded)], [...bytes]);
        }),
      ),
    ),
  );

  it.effect("refuses a gitlink carrying blob evidence", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const json = JSON.stringify({
            version: 1,
            view: { base: Pack.qualify(EMPTY_TREE_OID), tree: Pack.qualify(EMPTY_TREE_OID) },
            items: [
              {
                kind: "gitlink",
                path: "vendor/x",
                commit: Pack.qualify(EMPTY_TREE_OID),
                blob: Pack.qualify(EMPTY_TREE_OID),
              },
            ],
          });
          const failure = yield* Effect.result(Pack.decode(encode(json)));
          assert.equal(failure._tag, "Failure");
        }),
      ),
    ),
  );
});
