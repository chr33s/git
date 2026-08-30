/**
 * The default selector, judged on the only things a selector owes anyone.
 *
 * Not on whether it retrieves well — that is outside the protocol
 * (docs/context-pack.md §12) and is allowed to change without any of this
 * changing. What it owes is that everything it produces verifies against the
 * view it was given, that an instruction claim it writes actually holds, that
 * a budget it enforces is *reported* rather than silently applied, and that an
 * omission diagnostic can be reduced to an aggregate when naming a path would
 * say more than the reader may see (§6.1).
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
import * as Select from "./Select.ts";

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

/** Several files that mention the task's words to different degrees. */
const project = Effect.fn("test.project")(function* () {
  const work = yield* WorkTree;
  yield* work.write("AGENTS.md", encode("Standing instructions for this repository.\n"), 0o100644);
  yield* work.write(
    "src/auth.ts",
    encode("export const authorize = (policy: string) => policy !== ''\n"),
    0o100644,
  );
  yield* work.write(
    "src/policy.ts",
    encode("export type Policy = { authorize: boolean }\n"),
    0o100644,
  );
  yield* work.write("docs/readme.md", encode("Nothing to do with any of it.\n"), 0o100644);
  yield* work.write("assets/logo.bin", Uint8Array.from([0x89, 0x00, 0x01, 0x02, 0x00]), 0o100644);
  yield* Checkout.add(["."]);
  const made = yield* Checkout.commit({ message: "first\n", author });
  return yield* Pack.capture(made.oid);
});

describe("the default selector", () => {
  it.effect("produces a pack whose every item verifies against the view", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* project();
          const pack = yield* Select.select({ task: "authorize policy", view });
          const report = yield* Pack.verify(pack);

          assert.equal(report.ok, true);
          assert.equal(pack.view.tree, view.tree);
          assert.equal(pack.selector?.name, Select.NAME);
          assert.equal(
            pack.items.some((item) => item.path === "src/auth.ts"),
            true,
          );
          // A file that mentions none of the task's words is not evidence.
          assert.equal(
            pack.items.some((item) => item.path === "docs/readme.md"),
            false,
          );
        }),
      ),
    ),
  );

  it.effect("writes an instruction claim that verifies", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* project();
          const pack = yield* Select.select({ task: "authorize policy", view });
          const report = yield* Pack.verify(pack);

          const instructions = pack.items.findIndex((item) => item.path === "AGENTS.md");
          assert.notEqual(instructions, -1);
          // Selected whether or not the task mentions it: an agent told the
          // standing instructions was told them, so a pack that left them out
          // would describe an exposure that did not happen.
          assert.equal(report.items[instructions]?.authority?.ok, true);
        }),
      ),
    ),
  );

  it.effect("reports what the budget cut instead of dropping it silently", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* project();
          const pack = yield* Select.select({
            task: "authorize policy",
            view,
            // One item, so everything the search found after it is a cut the
            // pack has to account for.
            maxItems: 1,
          });
          assert.equal((pack.omissions ?? []).length > 0, true);
          assert.equal(
            (pack.omissions ?? []).every((omission) => omission.reason === "budget"),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("reduces omissions to an aggregate when paths may not be named", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* project();
          const pack = yield* Select.select({
            task: "authorize policy",
            view,
            maxItems: 1,
            diagnostics: "aggregate",
          });
          const omissions = pack.omissions ?? [];
          assert.equal(omissions.length, 1);
          // A count and a reason, and no repository structure at all.
          assert.equal(omissions[0]?.path, undefined);
          assert.equal((omissions[0]?.count ?? 0) > 0, true);
        }),
      ),
    ),
  );

  it.effect("records evidence a text renderer can hand over intact", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          // A file whose matches sit around a multi-byte character, and large
          // enough that the selector has to record a range rather than all of it.
          const padding = "policy filler line for authorize budget\n".repeat(200);
          yield* work.write(
            "src/wide.ts",
            encode(`${padding}// authorize é\n${padding}`),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "wide\n", author });
          const view = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({
            task: "authorize",
            view,
            maxBytes: 512,
          });
          const ranged = pack.items.find(
            (item) => item.kind === "blob" && item.range !== undefined,
          );
          assert.notEqual(ranged, undefined);
          if (ranged?.kind !== "blob" || ranged.range === undefined) return;

          const bytes = yield* Pack.evidence(view, ranged);
          // Decodable without a replacement character: the range did not cut a
          // codepoint (§5.1).
          const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
          assert.equal(text.length > 0, true);
        }),
      ),
    ),
  );

  it.effect("records a gitlink the selected evidence points at", () =>
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
          const source = yield* repository.writeBlob(
            encode("import { authorize } from '../vendor/policy-engine/index.ts'\n"),
          );
          const tree = yield* repository.writePaths([
            { path: "vendor/policy-engine", oid: submodule, mode: "160000" },
            { path: "src/auth.ts", oid: source, mode: "100644" },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "with a submodule\n",
            author,
          });
          const view = yield* Pack.committed(commit);

          const pack = yield* Select.select({ task: "authorize", view });
          const gitlink = pack.items.find((item) => item.kind === "gitlink");
          assert.equal(gitlink?.path, "vendor/policy-engine");
          assert.equal(gitlink?.reason, "import");
          // The gitlink is the parent repository's claim about a commit, and
          // nothing more: it carries no bytes, and it verifies at mode 160000.
          assert.equal((yield* Pack.verify(pack)).ok, true);
        }),
      ),
    ),
  );

  it("takes only terms long enough to discriminate", () => {
    assert.deepEqual(Select.terms("fix the auth policy"), ["fix", "the", "auth", "policy"]);
    assert.deepEqual(Select.terms("a an of"), []);
    assert.deepEqual(Select.terms("policy policy"), ["policy"]);
  });
});
