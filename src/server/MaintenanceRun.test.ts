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
import * as Maintenance from "./MaintenanceRun.ts";
import { memoryLayer as operationsMemory } from "./Operations.ts";

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
  Layer.provideMerge(operationsMemory),
  Layer.provideMerge(Maintenance.memoryMetaLayer),
);

describe("MaintenanceRun", () => {
  it.effect("plans a full bundle for a seeded repository and run converges", () =>
    Effect.promise(async () => {
      const { planned, after } = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("x\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "x.txt", oid: blob }]);
          yield* repository.commit({ branch: "main", tree, message: "x", author: alice });
          const planned = yield* Maintenance.plan();
          yield* Maintenance.runAll();
          const after = yield* Maintenance.plan();
          return { planned, after };
        }).pipe(Effect.provide(live)),
      );
      assert.ok(planned.some((unit) => unit.kind === "build-full"));
      assert.ok(!after.some((unit) => unit.kind === "build-full"));
    }),
  );
});
