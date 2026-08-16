import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import {
  type Fingerprint,
  fingerprint,
  formatPublicKey,
  generate,
  NAMESPACE,
  type PrivateKey,
  sign,
} from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Certificate from "./Certificate.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "./Genesis.ts";
import * as Log from "./Log.ts";
import * as Record from "./Record.ts";
import { project, type Projection } from "./Projection.ts";
import * as Verify from "./Verify.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
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

interface World {
  readonly genesis: Genesis;
  readonly roots: ReadonlyArray<PrivateKey>;
}

/** A repository with three root keys and a two-of-three threshold. */
const world = Effect.fn("test.world")(function* (threshold = 2, count = 3) {
  const roots = yield* Effect.all(
    Array.from({ length: count }, (_, index) => generate(`root${index}@example.com`)),
  );
  const genesis = yield* create(
    roots.map((root) => formatPublicKey(root.publicKey)),
    threshold,
  );
  yield* writeGenesis(
    genesis,
    yield* Effect.forEach(roots.slice(0, threshold), (root) => signGenesis(genesis, root)),
  );
  return { genesis, roots } satisfies World;
});

const grantTo = Effect.fn("test.grantTo")(function* (
  where: World,
  subject: PrivateKey,
  capabilities: ReadonlyArray<string>,
  signers: ReadonlyArray<PrivateKey>,
  options?: { readonly expiresAt?: Date },
) {
  const payload = yield* Certificate.grant({
    repo: where.genesis.repoId,
    publicKey: formatPublicKey(subject.publicKey),
    capabilities,
    expiresAt: options?.expiresAt ?? null,
    id: Log.newId(),
  });
  return yield* Log.issue(payload, signers);
});

const print = (key: PrivateKey): Effect.Effect<Fingerprint> => fingerprint(key.publicKey);

const projectionOf = (where: World) => project(where.genesis);

describe("trust projection", () => {
  it("folds a grant signed by the root quorum into a member", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const bob = yield* generate("bob@example.com");
        yield* grantTo(where, bob, ["source.push", "hub.review"], where.roots.slice(0, 2));
        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    const member = state.projection.members.get(state.bob);
    assert.notEqual(member, undefined, "the grant should have produced a member");
    assert.deepEqual(member?.capabilities, ["source.push", "hub.review"]);
    assert.deepEqual(state.projection.rejected, []);
  });

  it("refuses a grant that does not meet the root threshold", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const bob = yield* generate("bob@example.com");
        // One root of the two required, and no delegated authority.
        yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 1));
        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.equal(state.projection.members.get(state.bob), undefined);
    assert.equal(state.projection.rejected.length, 1);
    assert.match(state.projection.rejected[0]!.reason, /may not invite/);
  });

  it("lets a member with member.invite grant what they hold", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const alice = yield* generate("alice@example.com");
        const bob = yield* generate("bob@example.com");

        yield* grantTo(where, alice, ["member.invite", "source.push"], where.roots.slice(0, 2));
        yield* grantTo(where, bob, ["source.push"], [alice]);

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.notEqual(state.projection.members.get(state.bob), undefined);
    assert.deepEqual(state.projection.rejected, []);
  });

  it("refuses an issuer granting more than they hold", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const alice = yield* generate("alice@example.com");
        const mallory = yield* generate("mallory@example.com");

        yield* grantTo(where, alice, ["member.invite", "source.push"], where.roots.slice(0, 2));
        // Alice may invite, but she does not hold `repo.admin` and so cannot
        // mint one — this is the privilege-escalation path.
        yield* grantTo(where, mallory, ["repo.admin"], [alice]);

        return { projection: yield* projectionOf(where), mallory: yield* print(mallory) };
      }),
    );

    assert.equal(state.projection.members.get(state.mallory), undefined);
    assert.match(state.projection.rejected[0]!.reason, /cannot grant repo\.admin/);
  });

  it("treats repo.admin as carrying every capability", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const alice = yield* generate("alice@example.com");
        const bob = yield* generate("bob@example.com");

        yield* grantTo(where, alice, ["repo.admin"], where.roots.slice(0, 2));
        yield* grantTo(where, bob, ["source.push", "hub.merge"], [alice]);

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );
    assert.notEqual(state.projection.members.get(state.bob), undefined);
  });

  describe("revocation", () => {
    it("removes a member and keeps what they held", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "left",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.equal(state.projection.members.get(state.bob), undefined);
      assert.notEqual(state.projection.revoked.get(state.bob), undefined);
      assert.notEqual(
        state.projection.former.get(state.bob),
        undefined,
        "what they held must survive, for judging what they signed",
      );
    });

    it("denies a live request from a revoked key", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "left",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          return yield* Verify.authorizeKey({
            projection: yield* projectionOf(where),
            signer: yield* print(bob),
            capability: "source.push",
          });
        }),
      );

      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /revoked/);
    });

    it("leaves an event made before the revocation was visible valid", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

          // Bob signs while the log head is still the grant: he has not seen,
          // and could not have seen, the revocation that follows.
          const repository = yield* Repository;
          const before = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("a review of an exact revision");
          const signature = yield* sign(bob, bytes, NAMESPACE);

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "left",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          return yield* Verify.authorize({
            projection: yield* projectionOf(where),
            bytes,
            signatures: [signature],
            capability: "hub.review",
            made: { at: new Date(), trustHead: before },
          });
        }),
      );

      assert.equal(outcome.ok, true, "a forward-only revocation must not unmake history");
    });

    it("refuses an event whose author had already seen the revocation", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "left",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          // Bob signs against a head that already contains his revocation.
          const repository = yield* Repository;
          const after = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("a review made too late");

          return yield* Verify.authorize({
            projection: yield* projectionOf(where),
            bytes,
            signatures: [yield* sign(bob, bytes, NAMESPACE)],
            capability: "hub.review",
            made: { at: new Date(), trustHead: after },
          });
        }),
      );

      assert.equal(outcome.ok, false);
    });

    it("reaches backwards when the key was compromised", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

          const repository = yield* Repository;
          const before = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("a review nobody should trust");
          const signature = yield* sign(bob, bytes, NAMESPACE);

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "compromised",
              compromisedAt: new Date(0),
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          return yield* Verify.authorize({
            projection: yield* projectionOf(where),
            bytes,
            signatures: [signature],
            capability: "hub.review",
            made: { at: new Date(), trustHead: before },
          });
        }),
      );

      assert.equal(outcome.ok, false, "a compromise must invalidate what came before it");
    });

    it("lets a later grant re-instate a revoked key", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "rotated",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.notEqual(state.projection.members.get(state.bob), undefined);
      assert.equal(state.projection.revoked.get(state.bob), undefined);
    });
  });

  describe("root authority", () => {
    it("rotates the root set when the quorum signs", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const replacement = yield* generate("newroot@example.com");
          yield* Log.issue(
            Certificate.rootChange({
              repo: where.genesis.repoId,
              rootKeys: [formatPublicKey(replacement.publicKey)],
              threshold: 1,
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );
          return {
            projection: yield* projectionOf(where),
            replacement: yield* print(replacement),
          };
        }),
      );

      assert.equal(state.projection.threshold, 1);
      assert.deepEqual(
        state.projection.roots.map((root) => root.fingerprint),
        [state.replacement],
      );
    });

    it("refuses a root change from an admin who is not a root", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const alice = yield* generate("alice@example.com");
          yield* grantTo(where, alice, ["repo.admin"], where.roots.slice(0, 2));

          // The loop authority cannot survive: an admin rewriting the root set
          // could replace the keys that granted them admin.
          yield* Log.issue(
            Certificate.rootChange({
              repo: where.genesis.repoId,
              rootKeys: [formatPublicKey(alice.publicKey)],
              threshold: 1,
              id: Log.newId(),
            }),
            [alice],
          );
          return yield* projectionOf(where);
        }),
      );

      assert.equal(state.threshold, 2);
      assert.match(state.rejected.at(-1)!.reason, /root quorum/);
    });

    it("uses the new roots for records that follow a rotation", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const replacement = yield* generate("newroot@example.com");
          const bob = yield* generate("bob@example.com");

          yield* Log.issue(
            Certificate.rootChange({
              repo: where.genesis.repoId,
              rootKeys: [formatPublicKey(replacement.publicKey)],
              threshold: 1,
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );
          // Signed by the new root alone, which the old threshold would refuse.
          yield* grantTo(where, bob, ["source.push"], [replacement]);

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.notEqual(state.projection.members.get(state.bob), undefined);
    });
  });

  describe("expiry", () => {
    it("denies a member whose grant has expired", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2), {
            expiresAt: new Date(Date.now() - 1000),
          });

          return yield* Verify.authorizeKey({
            projection: yield* projectionOf(where),
            signer: yield* print(bob),
            capability: "source.push",
          });
        }),
      );

      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /expired/);
    });

    it("allows a member whose grant has not expired yet", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2), {
            expiresAt: new Date(Date.now() + 60_000),
          });
          return yield* Verify.authorizeKey({
            projection: yield* projectionOf(where),
            signer: yield* print(bob),
            capability: "source.push",
          });
        }),
      );
      assert.equal(outcome.ok, true);
    });
  });

  describe("capabilities", () => {
    it("denies a capability the member does not hold", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.comment"], where.roots.slice(0, 2));
          return yield* Verify.authorizeKey({
            projection: yield* projectionOf(where),
            signer: yield* print(bob),
            capability: "hub.merge",
          });
        }),
      );
      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /does not hold hub\.merge/);
    });

    it("scopes a check capability to its own check name", () => {
      assert.ok(Certificate.permits(["hub.check:test"], Certificate.checkCapability("test")));
      assert.equal(
        Certificate.permits(["hub.check:test"], Certificate.checkCapability("deploy")),
        false,
        "a bot trusted for `test` must not be able to sign `deploy`",
      );
      assert.ok(Certificate.permits(["hub.check:*"], Certificate.checkCapability("deploy")));
    });

    it("refuses a grant naming a capability that does not exist", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.pussh"], where.roots.slice(0, 2));
          return yield* projectionOf(where);
        }),
      );
      assert.match(state.rejected[0]!.reason, /unknown capability/);
    });
  });

  describe("binding", () => {
    it("ignores a record written for another repository", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");

          // A grant harvested from another repository and replayed here.
          const stranger = yield* create([formatPublicKey(bob.publicKey)], 1);
          const payload = yield* Certificate.grant({
            repo: stranger.repoId,
            publicKey: formatPublicKey(bob.publicKey),
            capabilities: ["repo.admin"],
            id: Log.newId(),
          });
          yield* Log.issue(payload, where.roots.slice(0, 2));

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.equal(state.projection.members.get(state.bob), undefined);
      assert.match(state.projection.rejected[0]!.reason, /is for SHA256:/);
    });

    it("ignores a grant whose subject is not the key beside it", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          const mallory = yield* generate("mallory@example.com");

          const payload = yield* Certificate.grant({
            repo: where.genesis.repoId,
            publicKey: formatPublicKey(bob.publicKey),
            capabilities: ["repo.admin"],
            id: Log.newId(),
          });
          // The key stays Bob's; the subject now names Mallory.
          const forged = { ...payload, subject: yield* print(mallory) };
          yield* Log.issue(forged, where.roots.slice(0, 2));

          return { projection: yield* projectionOf(where), mallory: yield* print(mallory) };
        }),
      );

      assert.equal(state.projection.members.get(state.mallory), undefined);
      assert.match(state.projection.rejected[0]!.reason, /is not the fingerprint/);
    });

    it("ignores a record nobody signed", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          const payload = yield* Certificate.grant({
            repo: where.genesis.repoId,
            publicKey: formatPublicKey(bob.publicKey),
            capabilities: ["repo.admin"],
            id: Log.newId(),
          });
          yield* Log.issue(payload, []);
          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.equal(state.projection.members.get(state.bob), undefined);
    });

    it("survives a record it cannot read at all", async () => {
      // The log is append-only, so a record that failed the walk rather than
      // being skipped would fail every projection — and therefore every
      // request — permanently, with no way to rewind the ref. Any member with
      // `source.push` could do it.
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          // A commit under the log ref carrying an `entry.json` that is not a
          // trust payload at all.
          const repository = yield* Repository;
          const head = yield* repository.resolve(Log.LOG_REF);
          const junk = yield* Record.write({
            name: Log.RECORD,
            payload: new TextEncoder().encode("{ not a payload }\n"),
            signatures: [],
            parents: head === null ? [] : [head],
            message: "junk\n",
          });
          yield* repository.setRef({ name: Log.LOG_REF, to: junk, expected: head });

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.notEqual(
        state.projection.members.get(state.bob),
        undefined,
        "one unreadable record must not take the membership with it",
      );
    });

    it("keeps folding after an unauthorized record", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const mallory = yield* generate("mallory@example.com");
          const bob = yield* generate("bob@example.com");

          yield* grantTo(where, mallory, ["repo.admin"], [mallory]);
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.notEqual(
        state.projection.members.get(state.bob),
        undefined,
        "one bad record must not make the whole membership unreadable",
      );
      assert.equal(state.projection.rejected.length, 1);
    });
  });

  describe("checkpoints", () => {
    it("records the newest checkpoint an admin signed", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const repository = yield* Repository;
          const head = yield* repository.resolve(Log.LOG_REF);

          yield* Log.issue(
            Certificate.checkpoint({
              repo: where.genesis.repoId,
              frontier: head === null ? [] : [head],
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );
          return yield* projectionOf(where);
        }),
      );

      assert.notEqual(state.checkpoint, null);
    });

    it("calls a view with no checkpoint stale when freshness is required", async () => {
      const state = await scenario(Effect.flatMap(world(), (where) => projectionOf(where)));
      const freshness = Verify.fresh(state, 60_000);
      assert.equal(freshness.ok, false);
    });

    it("accepts a recent checkpoint and rejects an old one", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          yield* Log.issue(
            Certificate.checkpoint({
              repo: where.genesis.repoId,
              frontier: [],
              id: Log.newId(),
              at: new Date(Date.now() - 30_000),
            }),
            where.roots.slice(0, 2),
          );
          return yield* projectionOf(where);
        }),
      );

      assert.equal(Verify.fresh(state, 60_000).ok, true);
      assert.equal(Verify.fresh(state, 10_000).ok, false);
    });
  });

  it("reaches the same state on every replica, whatever the write order", async () => {
    // Determinism is what makes two hosts agree about membership. The fold
    // orders concurrent records by oid, so the answer cannot depend on which
    // replica happened to write first.
    const [first, second] = await Promise.all([
      scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));
          const projection: Projection = yield* projectionOf(where);
          return [...projection.members.keys()].sort().join(",");
        }),
      ),
      scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));
          const projection: Projection = yield* projectionOf(where);
          return [...projection.members.keys()].sort().join(",");
        }),
      ),
    ]);

    assert.equal(first.split(",").length, second.split(",").length);
  });
});
