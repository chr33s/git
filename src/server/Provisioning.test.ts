import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { concatBytes } from "../git/Format.ts";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import * as Memory from "../git/Memory.ts";
import * as Repository from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Provisioning from "./Provisioning.ts";

const live = Repository.layer.pipe(
  Layer.provide(Repository.hooksNoop),
  Layer.provide(Memory.stores),
);
const seed = Effect.fn("ProvisioningTest.seed")(function* (
  capabilities: ReadonlyArray<string>,
  expiresAt: Date | null = null,
) {
  const root = yield* generate("operator@example.com");
  const member = yield* generate("administrator@example.com");
  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(member.publicKey),
      capabilities,
      id: Log.newId(),
      at: new Date("2020-01-01T00:00:00Z"),
      expiresAt,
    }),
    [root],
  );
  return genesis;
});

describe("operator provisioning artifacts", () => {
  it.effect("rejects a structurally valid pack whose genesis lacks its root signatures", () =>
    Effect.gen(function* () {
      const root = yield* generate("unsigned@example.com");
      const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
      const commit = yield* writeGenesis(genesis, []);
      const repository = yield* Repository.Repository;
      const pack = concatBytes(yield* Stream.runCollect(repository.packOf([commit], [])));
      const error = yield* Provisioning.validate({
        version: 1,
        repo: "repo",
        repoId: genesis.repoId,
        genesis: commit,
        trust: commit,
        pack,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "Invalid");
      assert.ok("field" in error && error.field === "genesis");
    }).pipe(Effect.provide(live)),
  );

  it.effect("round-trips a verified identity without changing the source refs", () =>
    Effect.gen(function* () {
      const genesis = yield* seed(["repo.admin"]);
      const repository = yield* Repository.Repository;
      const before = yield* repository.refs;
      const bytes = yield* Provisioning.prepare("private-repo");
      const artifact = yield* Provisioning.decode(bytes, "private-repo");
      assert.equal(artifact.repoId, genesis.repoId);
      assert.ok(artifact.pack.byteLength < Provisioning.MAX_PACK_BYTES);
      yield* Provisioning.validate(artifact);
      assert.deepEqual(yield* repository.refs, before);
    }).pipe(Effect.provide(live)),
  );

  it.effect("refuses an uninitialized repository", () =>
    Effect.gen(function* () {
      const error = yield* Provisioning.prepare("repo").pipe(Effect.flip);
      assert.equal(error._tag, "Invalid");
    }).pipe(Effect.provide(live)),
  );

  for (const capability of ["repo.read", "source.push"]) {
    it.effect(`refuses provisioning with only ${capability}`, () =>
      Effect.gen(function* () {
        yield* seed([capability]);
        const error = yield* Provisioning.prepare("repo").pipe(Effect.flip);
        assert.equal(error._tag, "Invalid");
        assert.ok("reason" in error && error.reason.includes("administrator"));
      }).pipe(Effect.provide(live)),
    );
  }

  it.effect("refuses an expired administrator", () =>
    Effect.gen(function* () {
      yield* seed(["repo.admin"], new Date("2021-01-01T00:00:00Z"));
      const error = yield* Provisioning.prepare("repo").pipe(Effect.flip);
      assert.equal(error._tag, "Invalid");
    }).pipe(Effect.provide(live)),
  );

  it.effect(
    "rejects mismatched identity, corrupt packs, oversized input, and noncanonical names",
    () =>
      Effect.gen(function* () {
        yield* seed(["repo.admin"]);
        const bytes = yield* Provisioning.prepare("repo");
        const artifact = yield* Provisioning.decode(bytes, "repo");
        for (const invalid of [
          { ...artifact, repoId: "sha256:wrong" },
          { ...artifact, genesis: "not-an-oid" },
          { ...artifact, pack: new Uint8Array([1, 2, 3]) },
          { ...artifact, pack: new Uint8Array(Provisioning.MAX_PACK_BYTES + 1) },
          ...["../repo", "repo.git", "repo/other", ".hidden"].map((repo) => ({
            ...artifact,
            repo,
          })),
        ]) {
          const outcome = yield* Provisioning.validate(invalid).pipe(Effect.result);
          assert.equal(outcome._tag, "Failure");
        }
        for (const invalid of [
          new Uint8Array(Provisioning.MAX_BYTES + 1),
          new TextEncoder().encode("{}"),
        ]) {
          assert.equal(
            (yield* Provisioning.decode(invalid, "repo").pipe(Effect.result))._tag,
            "Failure",
          );
        }
        assert.equal(
          (yield* Provisioning.decode(bytes, "elsewhere").pipe(Effect.result))._tag,
          "Failure",
        );
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Provisioning.Artifact))({
          ...artifact,
          version: 1,
        });
        assert.equal(
          (yield* Provisioning.decode(new TextEncoder().encode(encoded), "repo")).repo,
          "repo",
        );
      }).pipe(Effect.provide(live)),
  );
});
