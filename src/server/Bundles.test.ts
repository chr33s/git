import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Bundles from "./Bundles.ts";
import { memoryLayer } from "./BundleStore.ts";
import { defaultsLayer } from "./Features.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const liveRepo = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);
const live = Bundles.layer.pipe(
  Layer.provide(memoryLayer),
  Layer.provideMerge(liveRepo),
  Layer.provideMerge(defaultsLayer),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Bundles.Bundles | Repository>) =>
  Effect.runPromise(effect.pipe(Effect.provide(live)));

describe("Bundles publication", () => {
  it.effect("does not advertise until a verified artifact is published", () =>
    Effect.promise(async () => {
      const { before, built, after, clone } = await scenario(
        Effect.gen(function* () {
          const bundles = yield* Bundles.Bundles;
          const before = yield* bundles.summary;
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("hi\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
          yield* repository.commit({ branch: "main", tree, message: "hi", author: alice });
          const built = yield* bundles.build({ kind: "full", filter: null });
          const after = yield* bundles.summary;
          const clone = yield* bundles.protocolLines("clone", "http://host/repo");
          const catchup = yield* bundles.protocolLines("catchup", "http://host/repo");
          return { before, built, after, clone, catchup };
        }),
      );

      assert.equal(
        before.families.some((family) => family.full !== null),
        false,
      );
      assert.ok(built.bytes > 0);
      assert.match(built.id, /^full-/);
      assert.equal(
        after.families.some((family) => family.full !== null),
        true,
      );
      assert.ok(clone.some((line) => line.startsWith("bundle.version=")));
      assert.ok(clone.some((line) => line.includes(".uri=")));
    }),
  );

  it.effect("serves clone lists and range-readable artifacts", () =>
    Effect.promise(async () => {
      const { list, artifact } = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("hi\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
          yield* repository.commit({ branch: "main", tree, message: "hi", author: alice });
          const bundles = yield* Bundles.Bundles;
          const built = yield* bundles.build({ kind: "full", filter: null });
          const list = yield* bundles.handle(new Request("http://host/repo/bundles/clone"));
          const artifact = yield* bundles.handle(
            new Request(`http://host/repo/bundles/${built.objectId}`, {
              headers: { range: "bytes=0-15" },
            }),
          );
          return { list, artifact };
        }),
      );
      assert.equal(list?.status, 200);
      assert.match((await list?.text()) ?? "", /\[bundle\]/);
      assert.equal(artifact?.status, 206);
      assert.equal(artifact?.headers.get("accept-ranges"), "bytes");
    }),
  );
});
