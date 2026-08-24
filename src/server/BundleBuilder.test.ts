import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Result, Stream } from "effect";

import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore } from "../git/Store.ts";
import * as BundleBuilder from "./BundleBuilder.ts";
import { parseHeader } from "./BundleFormat.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const live = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(live)));

describe("BundleBuilder", () => {
  it.effect("emits a header whose refs match the captured snapshot", () =>
    Effect.promise(async () => {
      const { header, oids } = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("hello\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "hi.txt", oid: blob }]);
          yield* repository.commit({
            branch: "main",
            tree,
            message: "hello",
            author: alice,
          });
          const head = (yield* repository.resolve("refs/heads/main"))!;
          const snapshot = {
            createdAt: new Date(),
            refs: { "refs/heads/main": head },
            filter: null,
          };
          const built = yield* BundleBuilder.build({ snapshot, kind: "full" });
          const bytes = yield* Stream.runCollect(built.stream).pipe(
            Effect.map((chunks) => {
              let total = 0;
              for (const chunk of chunks) total += chunk.length;
              const out = new Uint8Array(total);
              let offset = 0;
              for (const chunk of chunks) {
                out.set(chunk, offset);
                offset += chunk.length;
              }
              return out;
            }),
          );
          yield* BundleBuilder.verifyBundle(bytes, {
            refs: snapshot.refs,
            prerequisites: [],
            filter: null,
          });
          return { header: parseHeader(bytes), oids: built.oids };
        }),
      );
      assert.equal(Result.isSuccess(header), true);
      if (Result.isFailure(header)) return;
      assert.ok(oids.length >= 1);
      assert.equal(header.success.header.refs["refs/heads/main"] !== undefined, true);
    }),
  );

  it.effect("omits blobs from a blob:none family", () =>
    Effect.promise(async () => {
      const types = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("payload\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "p.txt", oid: blob }]);
          yield* repository.commit({
            branch: "main",
            tree,
            message: "payload",
            author: alice,
          });
          const head = (yield* repository.resolve("refs/heads/main"))!;
          const objects = yield* ObjectStore;
          const oids = yield* BundleBuilder.oidsFor({
            snapshot: {
              createdAt: new Date(),
              refs: { "refs/heads/main": head },
              filter: "blob:none",
            },
          });
          const found: string[] = [];
          for (const oid of oids) found.push((yield* objects.read(oid)).type);
          return found;
        }),
      );
      assert.ok(types.includes("commit"));
      assert.ok(types.includes("tree"));
      assert.ok(!types.includes("blob"));
    }),
  );

  it.effect("incremental oids skip the prerequisite snapshot", () =>
    Effect.promise(async () => {
      const built = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const firstBlob = yield* repository.writeBlob(new TextEncoder().encode("one\n"));
          const firstTree = yield* repository.writeTree([
            { mode: "100644", name: "a.txt", oid: firstBlob },
          ]);
          const first = yield* repository.commit({
            branch: "main",
            tree: firstTree,
            message: "one",
            author: alice,
          });
          const secondBlob = yield* repository.writeBlob(new TextEncoder().encode("two\n"));
          const secondTree = yield* repository.writeTree([
            { mode: "100644", name: "a.txt", oid: secondBlob },
          ]);
          const second = yield* repository.commit({
            branch: "main",
            tree: secondTree,
            message: "two",
            author: alice,
          });
          const incremental = yield* BundleBuilder.oidsFor({
            snapshot: {
              createdAt: new Date(),
              refs: { "refs/heads/main": second },
              filter: null,
            },
            prerequisiteRefs: { "refs/heads/main": first },
          });
          return { first, second, incremental };
        }),
      );
      assert.ok(!built.incremental.includes(built.first));
      assert.ok(built.incremental.includes(built.second));
    }),
  );
});
