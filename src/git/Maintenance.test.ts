/**
 * `deltaOrder`, the sort between a reachability walk and the deltifying
 * pack writer.
 *
 * The walk emits a commit's blobs together, so two versions of one file sit
 * a whole commit apart — outside any reasonable window. The claim tested
 * here is both halves of the fix: the order groups same-named objects
 * largest-first, and that grouping is worth real bytes against the same
 * writer fed walk order.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { EMPTY_TREE_OID, type Signature } from "./Format.ts";
import { deltaOrder, reachable } from "./Maintenance.ts";
import { stores } from "./Memory.ts";
import * as Pack from "./Pack.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { ObjectStore, type Oid, RefStore } from "./Store.ts";

const encoder = new TextEncoder();

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore | RefStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ),
  );

/**
 * Three commits, each rewriting `story.txt` and twelve filler files — more
 * fillers than the delta window holds, and each filler large enough to be
 * admitted to it (tiny objects are kept out as bases), so in walk order the
 * story versions can never see each other. Sizes are deliberately out of
 * commit order: middle version largest, so "largest first" is
 * distinguishable from "oldest first".
 */
const history = Effect.gen(function* () {
  const repository = yield* Repository;
  const storyOids: Oid[] = [];
  const storySizes = [2000, 6000, 4000];

  for (let index = 0; index < 3; index++) {
    const tip = yield* repository.resolve("refs/heads/main");
    const base = tip === null ? undefined : (yield* repository.readCommit(tip)).tree;
    const story = `chapter ${index}\n${"the same long-running text, revised a little each time\n".repeat(Math.ceil(storySizes[index]! / 55))}`;
    const changes = [
      { path: "story.txt", content: encoder.encode(story) },
      ...Array.from({ length: 12 }, (_, filler) => ({
        path: `filler-${String(filler).padStart(2, "0")}.txt`,
        content: encoder.encode(`filler ${filler} at commit ${index} `.padEnd(120, "x")),
      })),
    ];
    const tree = yield* base === undefined
      ? repository.writeFiles({ changes })
      : repository.writeFiles({ base, changes });
    yield* repository.commit({ branch: "main", tree, message: `commit ${index}`, author: alice });
    const files = yield* repository.listFiles(tree);
    storyOids.push(files.find((file) => file.path === "story.txt")!.oid);
  }

  const head = (yield* repository.resolve("refs/heads/main"))!;
  const objects = yield* ObjectStore;
  const walked = yield* reachable(objects, [head], { ignoreMissing: false, classify: true });
  const walk = walked.order.filter((oid) => oid !== EMPTY_TREE_OID);
  return { objects, storyOids, walk, classified: walked.classified };
});

describe("Maintenance.deltaOrder", () => {
  it.effect("groups same-named objects adjacently, largest first, types apart", () =>
    Effect.promise(async () => {
      const { ordered, storyOids, types } = await scenario(
        Effect.gen(function* () {
          const { classified, objects, storyOids } = yield* history;
          const ordered = deltaOrder(classified);
          const types: string[] = [];
          for (const oid of ordered) types.push((yield* objects.read(oid)).type);
          return { ordered, storyOids, types };
        }),
      );

      // Type-major: commits, then trees, then blobs, with no interleaving.
      const boundaries = types.filter((type, index) => types[index - 1] !== type);
      assert.deepEqual(boundaries, ["commit", "tree", "blob"]);

      // The three story versions sit in one run, sizes 6000 > 4000 > 2000 —
      // which is versions 1, 2, 0, not their commit order.
      const positions = storyOids.map((oid) => ordered.indexOf(oid));
      const run = [...positions].sort((left, right) => left - right);
      assert.deepEqual(run, [run[0]!, run[0]! + 1, run[0]! + 2]);
      assert.deepEqual(
        run.map((at) => ordered[at]),
        [storyOids[1], storyOids[2], storyOids[0]],
      );
    }),
  );

  it.effect("is worth real bytes against the same writer fed walk order", () =>
    Effect.promise(async () => {
      const { orderedSize, walkSize } = await scenario(
        Effect.gen(function* () {
          const { classified, objects, walk } = yield* history;
          const ordered = deltaOrder(classified);
          const packed = (oids: ReadonlyArray<Oid>) =>
            Stream.runCollect(
              Pack.pack(oids, { deltify: {} }).pipe(Stream.provideService(ObjectStore, objects)),
            ).pipe(Effect.map((chunks) => chunks.reduce((sum, chunk) => sum + chunk.length, 0)));
          return {
            orderedSize: yield* packed(ordered),
            walkSize: yield* packed(walk),
          };
        }),
      );

      assert.ok(
        orderedSize < walkSize,
        `ordered pack (${orderedSize}) should undercut walk order (${walkSize})`,
      );
    }),
  );
});

describe("Maintenance.gc with repack", () => {
  it.effect("tolerates a dangling ref: everything readable is packed, nothing fails", () =>
    Effect.promise(async () => {
      const { packed, refs } = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const refStore = yield* RefStore;
          yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "real",
            author: alice,
          });
          // The exact case gc's tolerance comment promises to survive: a ref
          // whose target no store holds. The walk records the oid as seen but
          // never classifies it, so repack must neither pack nor trip on it.
          // SAFETY: forty lowercase hex characters by construction, which is
          // exactly what the Oid brand stands for.
          const fake = "a".repeat(40) as Oid;
          yield* refStore.apply([{ name: "refs/heads/dangling", value: fake, reason: "test" }]);
          const report = yield* repository.gc({ repack: true });
          return { packed: report.packed, refs: yield* repository.fsck };
        }),
      );

      assert.notEqual(packed, undefined);
      assert.equal(packed!.objects, 1);
      assert.deepEqual(
        refs.danglingRefs.map((entry) => entry.ref),
        ["refs/heads/dangling"],
      );
    }),
  );
});
