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
  // Matches the task's words and is larger than the selector will read: a
  // candidate it finds and then refuses, which is a `filtered` omission rather
  // than a budget one.
  // One match, so it does not crowd out the smaller candidates, and far past
  // the byte cap the selector reads at.
  yield* work.write(
    "assets/policy.log",
    encode(`authorize policy\n${"padding line\n".repeat(100_000)}`),
    0o100644,
  );
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
          // And the reason that actually applied. Rolling every cut into one
          // `filtered` count would state, in a signed record, that a content
          // filter removed what a budget removed.
          assert.equal(omissions[0]?.reason, "budget");
        }),
      ),
    ),
  );

  it.effect("counts an unreadable path under its own reason, not the budget's", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* project();
          const pack = yield* Select.select({
            task: "authorize policy",
            view,
            diagnostics: "aggregate",
            // Room for everything, so nothing is cut by the budget: the only
            // omission left is the binary file the selector will not read.
            maxItems: 64,
          });
          const omissions = pack.omissions ?? [];
          assert.deepEqual(
            omissions.map((omission) => omission.reason),
            ["filtered"],
          );
        }),
      ),
    ),
  );

  it.effect("counts the standing instructions against the budget it was given", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          // Small enough to fit its share, so it is included — and then has to
          // be paid for out of the same budget the search spends.
          const instructions = "authorize policy\n".repeat(50);
          yield* work.write("AGENTS.md", encode(instructions), 0o100644);
          yield* work.write(
            "src/auth.ts",
            encode(`authorize policy\n${"filler line\n".repeat(4_000)}`),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "instructions\n", author });
          const view = yield* Pack.capture(made.oid);

          const maxBytes = 4096;
          const pack = yield* Select.select({ task: "authorize policy", view, maxBytes });
          assert.equal(
            pack.items.some((item) => item.path === "AGENTS.md"),
            true,
            "an instruction file that fits is always included",
          );

          const searched = pack.items.find((item) => item.reason === "search");
          assert.notEqual(searched, undefined);
          if (searched?.kind !== "blob" || searched.range === undefined) return;
          // What the search adds is what is left, not the whole budget over
          // again: uncounted, the instructions went in *and* the search still
          // received its full allowance.
          assert.equal(
            searched.range[1] - searched.range[0] <= maxBytes - instructions.length,
            true,
          );
        }),
      ),
    ),
  );

  it.effect("does not let the instructions starve the search", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          // Larger than the whole budget: counting it without a share of its
          // own emptied the pack of everything the task asked about.
          yield* work.write("AGENTS.md", encode("standing\n".repeat(9_000)), 0o100644);
          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = (policy: string) => policy\n"),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "big instructions\n", author });
          const view = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({ task: "authorize policy", view, maxBytes: 4096 });
          assert.equal(
            pack.items.some((item) => item.path === "src/auth.ts"),
            true,
            "the task still gets evidence",
          );
          // And the file that could not fit is accounted for rather than
          // dropped: the pack says what it did with it.
          assert.equal(
            (pack.omissions ?? []).some((omission) => omission.path === "AGENTS.md"),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("never both omits and selects the same instruction file", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          yield* work.write("AGENTS.md", encode("authorize policy\n".repeat(600)), 0o100644);
          yield* work.write(
            "src/auth.ts",
            encode("export const authorize = (policy: string) => policy\n"),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "instructions\n", author });
          const view = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({ task: "authorize policy", view, maxBytes: 4096 });
          const omitted = (pack.omissions ?? []).some((entry) => entry.path === "AGENTS.md");
          const selected = pack.items.some((item) => item.path === "AGENTS.md");
          // One or the other. Both is a record contradicting itself — and the
          // selected form is worse than useless: it carries the instruction
          // file as `implementation / search`, with no authority annotation.
          assert.equal(omitted && selected, false);
          if (selected) {
            const item = pack.items.find((entry) => entry.path === "AGENTS.md");
            assert.equal(item?.kind === "blob" && item.role, "instruction");
          }
        }),
      ),
    ),
  );

  it.effect("omits an instruction file it will not read rather than exposing it", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          yield* work.write(
            "AGENTS.md",
            encode(`authorize\n${"padding line\n".repeat(100_000)}`),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "huge\n", author });
          const view = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({ task: "authorize", view });
          // Recorded as an omission rather than as evidence: `render` would
          // otherwise frame bytes the selector never looked at.
          assert.equal(
            pack.items.some((item) => item.path === "AGENTS.md"),
            false,
          );
          assert.equal(
            (pack.omissions ?? []).some(
              (omission) => omission.path === "AGENTS.md" && omission.reason === "filtered",
            ),
            true,
          );
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

  it.effect("does not let files the index never read crowd out the real omissions", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          yield* project();

          // Past `Search.MAX_FILE_BYTES`, and mentioning none of the task's
          // words. `search` reports it as skipped anyway: the size check runs
          // before the match, so this file was never a candidate for this task
          // — it is in the tree, and that is all.
          yield* work.write("vendor/blob.bin", encode("z".repeat(5 * 1024 * 1024)), 0o100644);
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "second\n", author });
          const captured = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({
            task: "authorize policy",
            view: captured,
            maxItems: 1,
          });

          // Named omissions are filled in the order they are recorded, and
          // there are only 64 slots. Recorded first, the skipped files took
          // them in tree order — so on a repository with that many large files
          // every omission a reader was asking about became an anonymous
          // count, and the signed pack named paths nobody had asked about.
          const named = (pack.omissions ?? []).filter((omission) => "path" in omission);
          assert.notEqual(named.length, 0);
          assert.equal(named[0]?.path === "vendor/blob.bin", false);
          assert.equal(
            named.some((omission) => omission.reason === "budget"),
            true,
          );

          // Still accounted for, just last: a pack that said nothing about it
          // would understate what was left out.
          assert.equal(
            named.some((omission) => omission.path === "vendor/blob.bin"),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("records a gitlink the budget cut instead of dropping it silently", () =>
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
          const tree = yield* repository.writePaths([
            { path: "vendor/policy-engine", oid: submodule, mode: "160000" },
            {
              path: "AGENTS.md",
              oid: yield* repository.writeBlob(encode("Standing.\n")),
              mode: "100644",
            },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "with a submodule\n",
            author,
          });
          const view = yield* Pack.committed(commit);

          // One item, taken by the instructions, so the submodule the task
          // names by hand does not fit. This was the only cut in the selector
          // that went unrecorded — and it `break`s — so the signed pack read
          // as though no submodule had been considered.
          const pack = yield* Select.select({
            task: "vendor/policy-engine",
            view,
            maxItems: 1,
          });
          assert.equal(
            (pack.omissions ?? []).some(
              (omission) => "path" in omission && omission.path === "vendor/policy-engine",
            ),
            true,
          );
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
          // `reference`, not `import`: what the selector established is that
          // the path is named in the evidence.
          assert.equal(gitlink?.reason, "reference");
          // The gitlink is the parent repository's claim about a commit, and
          // nothing more: it carries no bytes, and it verifies at mode 160000.
          assert.equal((yield* Pack.verify(pack)).ok, true);
        }),
      ),
    ),
  );

  it.effect("does not claim a submodule the evidence only looks like it names", () =>
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

          // A submodule at `lib`, and evidence that contains those three
          // letters three times over without referring to it once. A bare
          // `includes` recorded a signed `{kind:"gitlink", path:"lib"}` claim
          // that the exposed evidence pointed at that commit — permanent, on a
          // ref nothing can rewind, and verifying, since `checkItem` reads
          // mode and oid and has nothing to check the claim against.
          const source = yield* repository.writeBlob(
            encode(
              "#include <stdlib.h>\nimport { authorize } from './src/lib/x.ts'\n// the library loader\n",
            ),
          );
          const tree = yield* repository.writePaths([
            { path: "lib", oid: submodule, mode: "160000" },
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
          assert.equal(
            pack.items.some((item) => item.kind === "gitlink"),
            false,
          );
        }),
      ),
    ),
  );

  it.effect("keeps the matched line when the budget is exactly its context", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          const repository = yield* Repository;
          // Four leading lines of exactly a hundred bytes, the only match on
          // the fifth, and a budget of exactly four hundred. Compared against
          // the context alone the guard took the `wanted` branch and `end`
          // collapsed to the start of the matched line: the range held the
          // four lines before the match and stopped there.
          const filler = `${"x".repeat(99)}\n`;
          yield* work.write(
            "src/edge.ts",
            encode(`${filler.repeat(4)}const authorize = 1\n${filler.repeat(4)}`),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "edge\n", author });
          const view = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({ task: "authorize", view, maxBytes: 400 });
          const item = pack.items.find((entry) => entry.path === "src/edge.ts");
          if (item?.kind !== "blob" || item.range === undefined) return;
          const bytes = yield* repository.readBlob(Pack.unqualify(item.blob)!);
          const shown = new TextDecoder().decode(bytes.subarray(item.range[0], item.range[1]));
          assert.equal(shown.includes("authorize"), true);
        }),
      ),
    ),
  );

  it.effect("keeps the matched line when the budget cannot hold its context", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const work = yield* WorkTree;
          const repository = yield* Repository;
          // Long lines, with the only match well past the fourth. `rangeOf`
          // starts four lines *before* the first match and truncated the tail
          // to the budget, so a tight budget yielded a window that stopped
          // before the match — signed permanently as `reason: "search"`
          // evidence containing no search term.
          const filler = `${"x".repeat(200)}\n`;
          yield* work.write(
            "src/wide.ts",
            encode(`${filler.repeat(6)}const authorize = 1\n${filler.repeat(6)}`),
            0o100644,
          );
          yield* Checkout.add(["."]);
          const made = yield* Checkout.commit({ message: "wide\n", author });
          const view = yield* Pack.capture(made.oid);

          const pack = yield* Select.select({ task: "authorize", view, maxBytes: 200 });
          const item = pack.items.find((entry) => entry.path === "src/wide.ts");
          if (item?.kind !== "blob" || item.range === undefined) return;
          const bytes = yield* repository.readBlob(Pack.unqualify(item.blob)!);
          const shown = new TextDecoder().decode(bytes.subarray(item.range[0], item.range[1]));
          assert.equal(shown.includes("authorize"), true);
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
