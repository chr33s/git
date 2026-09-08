/**
 * Task-specific recall, and what a pack may claim about it.
 *
 * The R-series of docs/context-pack.knowledge.md §16. Three properties are load
 * bearing: a Concept that never fit in the startup note is still findable
 * (R-01, INV-09); a recalled Concept arrives with the *current* bytes of the
 * evidence it declares rather than the version it cited (R-02, R-04); and a
 * group that does not fit the budget is omitted whole, because prose with its
 * support silently dropped reads as supported when it is not (R-03).
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { qualify } from "../git/Oid.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Pack from "../context/Pack.ts";
import * as Select from "../context/Select.ts";
import * as Memory from "../hub/Memory.ts";
import * as Recall from "./Recall.ts";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const NOW = new Date("2026-09-08T00:00:00Z");
const encode = (text: string) => new TextEncoder().encode(text);

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(world)));

const viewOf = Effect.fn("test.viewOf")(function* (files: Record<string, string>) {
  const repository = yield* Repository;
  const entries: Array<{ path: string; oid: Oid; mode: string }> = [];
  for (const [path, contents] of Object.entries(files)) {
    entries.push({ path, oid: yield* repository.writeBlob(encode(contents)), mode: "100644" });
  }
  const tree = yield* repository.writePaths(entries);
  const commit = yield* repository.commitTree({ tree, parents: [], message: "files\n", author });
  return yield* Pack.committed(commit);
});

const conceptFor = (input: {
  readonly title: string;
  readonly description: string;
  readonly evidence?: { readonly path: string; readonly blob: string };
  readonly status?: string;
  readonly staleAfter?: string;
}) =>
  [
    "---",
    "type: Gotcha",
    `title: ${input.title}`,
    `description: ${input.description}`,
    ...(input.status === undefined ? [] : [`status: ${input.status}`]),
    ...(input.staleAfter === undefined ? [] : [`stale_after: "${input.staleAfter}"`]),
    ...(input.evidence === undefined
      ? []
      : [
          "gitplus:",
          "  evidence:",
          "    - kind: blob",
          `      path: ${input.evidence.path}`,
          `      blob: ${input.evidence.blob}`,
        ]),
    "---",
    "The discovery session reported this.",
    "",
  ].join("\n");

describe("task-specific recall", () => {
  it.effect("R-01: a Concept absent from startup Memory is still retrievable", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          // Enough ordinary Concepts that a small budget cannot hold them all.
          const filler = Object.fromEntries(
            Array.from({ length: 12 }, (_, at) => [
              `.gitplus/knowledge/conventions/c${at}.md`,
              conceptFor({
                title: `Convention ${at}`,
                description: `An ordinary convention number ${at} about formatting and layout.`,
              }),
            ]),
          );
          const view = yield* viewOf({
            ...filler,
            ".gitplus/knowledge/gotchas/rare-quota.md": conceptFor({
              title: "The quota endpoint rejects burst writes",
              description: "Batch quota writes; the endpoint rejects bursts above ten a second.",
            }),
          });

          // A deliberately starved startup note: the rare one does not fit.
          const derived = yield* Memory.derive({
            view,
            evaluationTime: NOW,
            limits: { bytes: 400 },
          });
          assert.equal(derived.text.includes("quota"), false);
          assert.equal(derived.omitted > 0, true);

          // The corpus is still the corpus (INV-09).
          const recalled = yield* Recall.search({
            view,
            task: "why does the quota endpoint reject writes",
            evaluationTime: NOW,
          });
          assert.equal(recalled.groups[0]?.checked.id, "gotchas/rare-quota");
        }),
      ),
    ),
  );

  it.effect("R-02: a selected Concept brings the current bytes of its support", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const fixture = "test('auth', () => { productionPolicy() })\n";
          const recorded = yield* repository.writeBlob(encode(fixture));
          const view = yield* viewOf({
            "tests/worker/auth.test.ts": fixture,
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth tests need the production policy fixture",
              description: "The worker authorization suite depends on the production fixture.",
              evidence: { path: "tests/worker/auth.test.ts", blob: qualify(recorded) },
            }),
          });

          const pack = yield* Select.select({
            task: "worker authorization fixture",
            view,
            knowledge: { evaluationTime: NOW },
          });

          const concept = pack.items.find(
            (item) => item.path === ".gitplus/knowledge/gotchas/worker-auth.md",
          );
          assert.equal(concept?.kind, "blob");
          assert.equal(concept?.role, "knowledge");
          assert.equal(concept?.reason, "memory");
          // The support rides with it, from the same one view.
          const support = pack.items.find((item) => item.path === "tests/worker/auth.test.ts");
          assert.equal(support?.kind, "blob");
          assert.equal(support?.kind === "blob" && support.blob, qualify(recorded));

          // And the whole pack verifies against that view, like any other.
          const report = yield* Pack.verify(pack);
          assert.equal(report.ok, true);
        }),
      ),
    ),
  );

  it.effect("a Concept whose support was already cut goes with it, whole", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          // Larger than the instruction share of the budget, so the selector
          // claims it and omits it before any Concept is considered.
          const standing = "standing\n".repeat(9_000);
          const recorded = yield* repository.writeBlob(encode(standing));
          const view = yield* viewOf({
            "AGENTS.md": standing,
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth tests need the production policy fixture",
              description: "The worker authorization suite depends on the production fixture.",
              evidence: { path: "AGENTS.md", blob: qualify(recorded) },
            }),
          });

          const pack = yield* Select.select({
            task: "worker authorization fixture",
            view,
            maxBytes: 4096,
            knowledge: { evaluationTime: NOW },
          });

          // Not admitted as prose standing in for the bytes the pack says it
          // left out: the group is taken whole or omitted whole (§9.2).
          assert.equal(
            pack.items.some((item) => item.path.startsWith(".gitplus/knowledge/")),
            false,
          );
          assert.deepEqual(
            (pack.omissions ?? []).filter(
              (omission) => omission.path === ".gitplus/knowledge/gotchas/worker-auth.md",
            ),
            [{ path: ".gitplus/knowledge/gotchas/worker-auth.md", reason: "budget" }],
          );
        }),
      ),
    ),
  );

  it.effect("a file a Concept declares twice is carried once", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const source = "export const fixture = 'production'\n";
          const recorded = yield* repository.writeBlob(encode(source));
          // Two ranges on one file is a legitimate shape; two items for one
          // file would charge and render the same bytes twice.
          const concept = [
            "---",
            "type: Gotcha",
            "title: Worker auth tests need the production policy fixture",
            "description: The worker authorization suite depends on the production fixture.",
            "gitplus:",
            "  evidence:",
            "    - kind: blob",
            "      path: tests/worker/auth.test.ts",
            `      blob: ${qualify(recorded)}`,
            "      range: [0, 6]",
            "    - kind: blob",
            "      path: tests/worker/auth.test.ts",
            `      blob: ${qualify(recorded)}`,
            "      range: [13, 20]",
            "---",
            "The discovery session reported this.",
            "",
          ].join("\n");
          const view = yield* viewOf({
            "tests/worker/auth.test.ts": source,
            ".gitplus/knowledge/gotchas/worker-auth.md": concept,
          });

          const pack = yield* Select.select({
            task: "worker authorization fixture",
            view,
            knowledge: { evaluationTime: NOW },
          });
          assert.equal(
            pack.items.filter((item) => item.path === "tests/worker/auth.test.ts").length,
            1,
          );
          assert.equal(
            pack.items.some((item) => item.path === ".gitplus/knowledge/gotchas/worker-auth.md"),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("R-03: a group that does not fit is an explicit budget omission", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const big = `${"x".repeat(4000)}\n`;
          const recorded = yield* repository.writeBlob(encode(big));
          const view = yield* viewOf({
            "tests/worker/auth.test.ts": big,
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth tests need the production policy fixture",
              description: "The worker authorization suite depends on the production fixture.",
              evidence: { path: "tests/worker/auth.test.ts", blob: qualify(recorded) },
            }),
          });

          const pack = yield* Select.select({
            task: "worker authorization fixture",
            view,
            maxBytes: 512,
            knowledge: { evaluationTime: NOW },
          });

          // Neither half is present: not the prose without its support, and
          // not a truncated version of either.
          assert.equal(
            pack.items.some((item) => item.path.startsWith(".gitplus/knowledge/")),
            false,
          );
          assert.deepEqual(
            (pack.omissions ?? []).filter(
              (omission) => omission.path === ".gitplus/knowledge/gotchas/worker-auth.md",
            ),
            [{ path: ".gitplus/knowledge/gotchas/worker-auth.md", reason: "budget" }],
          );
        }),
      ),
    ),
  );

  it.effect("R-04: a changed dependency is reported, never silently relabelled", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const cited = yield* repository.writeBlob(encode("the version the Concept cited\n"));
          const view = yield* viewOf({
            "tests/worker/auth.test.ts": "the version the tree holds now\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth tests need the production policy fixture",
              description: "The worker authorization suite depends on the production fixture.",
              evidence: { path: "tests/worker/auth.test.ts", blob: qualify(cited) },
            }),
          });

          const recalled = yield* Recall.search({
            view,
            task: "worker authorization fixture",
            evaluationTime: NOW,
          });
          // Excluded from automatic recall with the reason named: the prose is
          // not false, it needs revalidation.
          assert.deepEqual(recalled.groups, []);
          assert.deepEqual(recalled.excluded[0]?.reasons, ["evidence-changed"]);

          // The pack says so too, rather than quietly presenting the old prose.
          const pack = yield* Select.select({
            task: "worker authorization fixture",
            view,
            knowledge: { evaluationTime: NOW },
          });
          assert.equal(
            (pack.omissions ?? []).some(
              (omission) =>
                omission.path === ".gitplus/knowledge/gotchas/worker-auth.md" &&
                omission.reason === "filtered",
            ),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("§7: a draft Concept is not recalled automatically", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth tests need the production policy fixture",
              description: "The worker authorization suite depends on the production fixture.",
              status: "draft",
            }),
          });
          const automatic = yield* Recall.search({
            view,
            task: "worker authorization fixture",
            evaluationTime: NOW,
          });
          assert.deepEqual(automatic.groups, []);
          assert.deepEqual(automatic.excluded[0]?.reasons, ["lifecycle-draft"]);

          // Explicit inspection is a different call, and says so in its name.
          const inspected = yield* Recall.search({
            view,
            task: "worker authorization fixture",
            evaluationTime: NOW,
            includeIneligible: true,
          });
          assert.equal(inspected.groups[0]?.checked.id, "gotchas/worker-auth");
        }),
      ),
    ),
  );

  it.effect("R-05: knowledge selection leaves the source-only pack behavior intact", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            "src/auth.ts": "export const authorize = () => true\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth tests need the production policy fixture",
              description: "The worker authorization suite depends on the production fixture.",
            }),
          });
          // No `knowledge` option: the selector behaves exactly as before, and
          // the Concept is an ordinary file it did not match.
          const pack = yield* Select.select({ task: "authorize", view });
          assert.equal(
            pack.items.some((item) => item.role === "knowledge"),
            false,
          );
          const report = yield* Pack.verify(pack);
          assert.equal(report.ok, true);
        }),
      ),
    ),
  );

  it.effect("names the Concepts that ranked past the recall cut", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf(
            Object.fromEntries(
              Array.from({ length: 4 }, (_, at) => [
                `.gitplus/knowledge/gotchas/quota-${at}.md`,
                conceptFor({
                  title: `Quota rule ${at}`,
                  description: `Batch quota writes, rule number ${at}.`,
                }),
              ]),
            ),
          );

          const recalled = yield* Recall.search({
            view,
            task: "quota",
            evaluationTime: NOW,
            maxGroups: 2,
          });
          assert.equal(recalled.groups.length, 2);
          assert.equal(recalled.complete, false);
          // Left out for room, and said so: an unmentioned match is a question
          // with no answer.
          assert.deepEqual(
            recalled.excluded.map((entry) => entry.reasons),
            [[Recall.OVERFLOW], [Recall.OVERFLOW]],
          );

          const pack = yield* Select.select({
            task: "quota",
            view,
            knowledge: { evaluationTime: NOW, maxGroups: 2 },
          });
          // A budget omission, not a content filter.
          assert.equal(
            (pack.omissions ?? []).filter((omission) => omission.reason === "budget").length,
            2,
          );
        }),
      ),
    ),
  );

  it.effect("names a malformed Concept only when the task points at it", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/quota-limits.md": "no front matter here\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              title: "Worker auth",
              description: "Workers authorize against the policy ref.",
            }),
          });
          // Unrelated: not a group, and not an omission either. Named for
          // every task, a broken bundle filled the pack's named-omission slots
          // before the source search had run.
          const unrelated = yield* Recall.search({ view, task: "authorize", evaluationTime: NOW });
          assert.equal(unrelated.groups.length, 1);
          assert.deepEqual(unrelated.excluded, []);
          // Pointed at by path: still not a group, and said so.
          const pointed = yield* Recall.search({ view, task: "quota", evaluationTime: NOW });
          assert.equal(pointed.groups.length, 0);
          assert.deepEqual(
            pointed.excluded.map((entry) => entry.path),
            [".gitplus/knowledge/gotchas/quota-limits.md"],
          );
        }),
      ),
    ),
  );

  it.effect("ranks by distinct task terms, with a byte-order tiebreak", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/a.md": conceptFor({
              title: "Quota",
              description: "Something about quota only.",
            }),
            ".gitplus/knowledge/gotchas/b.md": conceptFor({
              title: "Quota and burst writes",
              description: "Both quota and burst behaviour are described here.",
            }),
          });
          const recalled = yield* Recall.search({
            view,
            task: "quota burst",
            evaluationTime: NOW,
          });
          assert.deepEqual(
            recalled.groups.map((group) => group.checked.id),
            ["gotchas/b", "gotchas/a"],
          );
        }),
      ),
    ),
  );
});
