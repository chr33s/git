import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { RequestLimitExceeded } from "../git/Error.ts";
import { concatBytes, EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { DELIM, FLUSH, pkt } from "../git/Pkt.ts";
import * as Repository from "../git/Repository.ts";
import * as Protocol from "./Protocol.ts";
import { defaults, RequestLimits } from "./RequestLimits.ts";

const live = Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores));
const author = { name: "Test", email: "test@example.com", at: new Date(0), offset: 0 };

describe("fetch graph budgets", () => {
  it.effect("charges ref validation even after redaction answers are cached", () =>
    Effect.gen(function* () {
      const repository = yield* Repository.Repository;
      const tip = yield* repository.commit({
        branch: "main",
        tree: EMPTY_TREE_OID,
        message: "source",
        author,
      });
      for (let i = 0; i < 25; i++)
        yield* repository.setRef({ name: `refs/hub/pr/cache-${i}`, to: tip });
      const request = {
        wants: [tip],
        haves: [],
        clientShallow: [],
        depth: 1,
        since: undefined,
        notRefs: [],
      };
      yield* Protocol.planFor(request).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 500 }),
      );
      const refused = yield* Protocol.planFor(request).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 12 }),
        Effect.flip,
      );
      assert.ok(refused instanceof RequestLimitExceeded);
    }).pipe(Effect.provide(live)),
  );

  it.effect("shares one allowance across separate hub histories", () =>
    Effect.gen(function* () {
      const repository = yield* Repository.Repository;
      const tip = yield* repository.commit({
        branch: "main",
        tree: EMPTY_TREE_OID,
        message: "source",
        author,
      });
      for (let ref = 0; ref < 3; ref++) {
        let head = tip;
        for (let i = 0; i < 4; i++)
          head = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: i === 0 ? [] : [head],
            message: `join ${ref}/${i}`,
            author,
          });
        yield* repository.setRef({ name: `refs/hub/pr/shared-${ref}`, to: head });
      }
      const request = {
        wants: [tip],
        haves: [],
        clientShallow: [],
        depth: 1,
        since: undefined,
        notRefs: [],
      };
      const refused = yield* Protocol.planFor(request).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 12 }),
        Effect.flip,
      );
      assert.ok(refused instanceof RequestLimitExceeded);
      const healthy = yield* Protocol.planFor(request).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 500 }),
      );
      assert.ok(healthy.oids.includes(tip));
    }).pipe(Effect.provide(live)),
  );

  it.effect.each(["pr", "session", "task", "trace", "note"])(
    "charges unrelated %s history needed by shallow redaction planning",
    (namespace) =>
      Effect.gen(function* () {
        const repository = yield* Repository.Repository;
        const tip = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "source",
          author,
        });
        let head = tip;
        for (let i = 0; i < 30; i++)
          head = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: i === 0 ? [] : [head],
            message: `hub join ${i}`,
            author,
          });
        yield* repository.setRef({ name: `refs/hub/${namespace}/budget`, to: head });
        const request = {
          wants: [tip],
          haves: [],
          clientShallow: [],
          depth: 1,
          since: undefined,
          notRefs: [],
        };
        const refused = yield* Protocol.planFor(request).pipe(
          Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 12 }),
          Effect.flip,
        );
        assert.ok(refused instanceof RequestLimitExceeded);
        const healthy = yield* Protocol.planFor(request).pipe(
          Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 500 }),
        );
        assert.ok(healthy.oids.includes(tip));
        assert.equal(yield* repository.resolve("refs/heads/main"), tip);
      }).pipe(Effect.provide(live)),
  );

  it.effect("shares the allowance between the have and want walks", () =>
    Effect.gen(function* () {
      const repository = yield* Repository.Repository;
      let common = EMPTY_TREE_OID;
      let tip = EMPTY_TREE_OID;
      for (let i = 0; i < 8; i++) {
        tip = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: `commit ${i}`,
          author,
        });
        if (i === 3) common = tip;
      }
      const request = {
        wants: [tip],
        haves: [common],
        clientShallow: [],
        depth: undefined,
        since: undefined,
        notRefs: [],
      };
      const error = yield* Protocol.planFor(request).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 12 }),
        Effect.flip,
      );
      assert.ok(error instanceof RequestLimitExceeded);
      const plan = yield* Protocol.planFor(request).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 20 }),
      );
      assert.equal(plan.oids.length, 4);
    }).pipe(Effect.provide(live)),
  );

  it.effect.each([false, true])("bounds final fetch planning before a response, v2=%s", (v2) =>
    Effect.gen(function* () {
      const repository = yield* Repository.Repository;
      let tip = EMPTY_TREE_OID;
      for (let i = 0; i < 8; i++) {
        tip = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: `commit ${i}`,
          author,
        });
      }
      const request = () =>
        new Request("http://host/r/git-upload-pack", {
          method: "POST",
          headers: v2 ? { "git-protocol": "version=2" } : {},
          body: concatBytes(
            v2
              ? [pkt("command=fetch\n"), DELIM, pkt(`want ${tip}\n`), pkt("done\n"), FLUSH]
              : [pkt(`want ${tip}\n`), FLUSH, pkt("done\n")],
          ),
        });
      const refused = yield* Protocol.uploadPack(request()).pipe(
        Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 5 }),
        Effect.flip,
      );
      assert.ok(refused instanceof RequestLimitExceeded);
      assert.equal(refused.budget, "fetch graph steps");
      for (let i = 0; i < 2; i++) {
        const response = yield* Protocol.uploadPack(request()).pipe(
          Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 50 }),
        );
        assert.equal(response.status, 200);
        assert.ok((yield* Effect.promise(() => response.arrayBuffer())).byteLength > 0);
      }
    }).pipe(Effect.provide(live)),
  );

  it.effect.each(["haves", "shallow", "excluded", "tags", "tree"] as const)(
    "charges %s work against the same plan budget",
    (mode) =>
      Effect.gen(function* () {
        const repository = yield* Repository.Repository;
        let tree = EMPTY_TREE_OID;
        if (mode === "tree") {
          const blob = yield* repository.writeBlob(new TextEncoder().encode("shared"));
          tree = yield* repository.writeTree(
            Array.from({ length: 40 }, (_, i) => ({
              mode: "100644",
              name: `file-${i}`,
              oid: blob,
            })),
          );
        }
        let tip = EMPTY_TREE_OID;
        for (let i = 0; i < 8; i++)
          tip = yield* repository.commit({ branch: "main", tree, message: `commit ${i}`, author });
        const before = yield* repository.resolve("refs/heads/main");
        assert.equal(before, tip);
        if (mode === "tags") {
          for (let i = 0; i < 8; i++)
            tip = (yield* repository.tag({
              name: `tag-${i}`,
              target: tip,
              message: "tag",
              tagger: author,
            })).oid;
        }
        const plan = {
          wants: [tip],
          haves: mode === "haves" ? [tip] : [],
          clientShallow: [],
          depth: mode === "shallow" || mode === "tags" || mode === "tree" ? 1 : undefined,
          since: undefined,
          notRefs: mode === "excluded" ? ["refs/heads/main"] : [],
        };
        const error = yield* Protocol.planFor(plan).pipe(
          Effect.provideService(RequestLimits, {
            ...defaults,
            fetchGraphSteps: mode === "tree" ? 20 : 4,
          }),
          Effect.flip,
        );
        assert.ok(error instanceof RequestLimitExceeded);
        const healthy = yield* Protocol.planFor(plan).pipe(
          Effect.provideService(RequestLimits, { ...defaults, fetchGraphSteps: 500 }),
        );
        assert.ok(Array.isArray(healthy.oids));
        assert.equal(yield* repository.resolve("refs/heads/main"), before);
      }).pipe(Effect.provide(live)),
  );
});
