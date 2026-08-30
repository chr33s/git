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
