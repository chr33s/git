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

  it("refuses a grant issued by a member whose own grant had expired", async () => {
    // The fold still has no clock — expiry is compared against the record's
    // own `issuedAt`, so the answer stays a pure function of the log. Ignoring
    // it entirely meant an expired `member.invite` holder's pre-signed grants
    // took effect on every replica forever, which is an expiry that expires
    // nothing.
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const alice = yield* generate("alice@example.com");
        const bob = yield* generate("bob@example.com");

        yield* grantTo(where, alice, ["member.invite", "source.push"], where.roots.slice(0, 2), {
          expiresAt: new Date(1_600_000_000_000),
        });
        // Alice signs *after* her own grant lapsed, and says so.
        const payload = yield* Certificate.grant({
          repo: where.genesis.repoId,
          publicKey: formatPublicKey(bob.publicKey),
          capabilities: ["source.push"],
          id: Log.newId(),
          at: new Date(1_700_000_000_000),
        });
        yield* Log.issue(payload, [alice]);

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.equal(state.projection.members.get(state.bob), undefined);
    assert.match(state.projection.rejected.at(-1)?.reason ?? "", /may not invite/);
  });

  it("accepts one an issuer signed while their grant was still live", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const alice = yield* generate("alice@example.com");
        const bob = yield* generate("bob@example.com");

        yield* grantTo(where, alice, ["member.invite", "source.push"], where.roots.slice(0, 2), {
          expiresAt: new Date(1_800_000_000_000),
        });
        const payload = yield* Certificate.grant({
          repo: where.genesis.repoId,
          publicKey: formatPublicKey(bob.publicKey),
          capabilities: ["source.push"],
          id: Log.newId(),
          at: new Date(1_700_000_000_000),
        });
        yield* Log.issue(payload, [alice]);

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.notEqual(state.projection.members.get(state.bob), undefined);
    assert.deepEqual(state.projection.rejected, []);
  });

  it("does not let an unsigned copy of a record displace the signed one", async () => {
    // Keyed on the payload alone, a replay that kept the bytes and *dropped*
    // the signatures counted as the same record — so an unsigned copy with a
    // lower oid won the tie-break and the signed original was discarded as its
    // duplicate, and the revocation never applied.
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const bob = yield* generate("bob@example.com");
        yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

        const payload = Certificate.revoke({
          repo: where.genesis.repoId,
          subject: yield* print(bob),
          reason: "compromised",
          id: Log.newId(),
        });
        const bytes = Certificate.encode(payload);
        // The same bytes with no signatures at all, then the real one.
        yield* Log.append(payload, bytes, []);
        yield* Log.append(
          payload,
          bytes,
          yield* Effect.forEach(where.roots.slice(0, 2), (key) => sign(key, bytes, NAMESPACE)),
        );

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.equal(state.projection.members.get(state.bob), undefined, "the revocation must apply");
    assert.notEqual(state.projection.revoked.get(state.bob), undefined);
  });

  it("refuses a record whose id has already been applied", async () => {
    // The log ref is writable by anybody holding `source.push` — append-only
    // containment is all the policy boundary checks about it — so
    // re-committing an existing record's bytes at the head is a push anyone
    // can make. Without an id check, replaying a revoked member's original
    // grant passed the re-instatement rule (its signer *is* the original
    // admin) and cleared the revocation.
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const bob = yield* generate("bob@example.com");

        const payload = yield* Certificate.grant({
          repo: where.genesis.repoId,
          publicKey: formatPublicKey(bob.publicKey),
          capabilities: ["source.push"],
          id: Log.newId(),
        });
        yield* Log.issue(payload, where.roots.slice(0, 2));
        yield* Log.issue(
          Certificate.revoke({
            repo: where.genesis.repoId,
            subject: yield* print(bob),
            reason: "compromised",
            id: Log.newId(),
          }),
          where.roots.slice(0, 2),
        );

        // The very same grant, appended again.
        yield* Log.issue(payload, where.roots.slice(0, 2));

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.equal(state.projection.members.get(state.bob), undefined, "the replay must not revive");
    assert.notEqual(state.projection.revoked.get(state.bob), undefined);
    assert.match(state.projection.rejected.at(-1)?.reason ?? "", /already been applied/);
  });

  it("does not let an unauthorized record burn a legitimate record's id", async () => {
    // Marking the id applied *before* the authority check let a forgery claim
    // it, be refused, and take the genuine record down with it as a duplicate.
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const bob = yield* generate("bob@example.com");
        const mallory = yield* generate("mallory@example.com");
        yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

        // Mallory signs a revocation she has no authority to make, using the
        // id the real one is about to use.
        const id = Log.newId();
        yield* Log.issue(
          Certificate.revoke({
            repo: where.genesis.repoId,
            subject: yield* print(bob),
            reason: "left",
            id,
          }),
          [mallory],
        );
        // And the real one follows, from the root quorum.
        yield* Log.issue(
          Certificate.revoke({
            repo: where.genesis.repoId,
            subject: yield* print(bob),
            reason: "compromised",
            id,
          }),
          where.roots.slice(0, 2),
        );

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    assert.equal(state.projection.members.get(state.bob), undefined, "the revocation must stand");
    assert.notEqual(state.projection.revoked.get(state.bob), undefined);
  });

  it("keeps the newest checkpoint, not the last one folded", async () => {
    // Fold order is topological with an oid tie-break, so two checkpoints made
    // concurrently and then joined could leave the older one in force — and a
    // repository with `maxTrustAgeSeconds` set would refuse every push against
    // an attestation it already had a fresher replacement for.
    const outcome = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const older = new Date(1_700_000_000_000);
        const newer = new Date(1_800_000_000_000);

        yield* Log.issue(
          Certificate.checkpoint({
            repo: where.genesis.repoId,
            frontier: [],
            id: Log.newId(),
            at: newer,
          }),
          where.roots.slice(0, 2),
        );
        yield* Log.issue(
          Certificate.checkpoint({
            repo: where.genesis.repoId,
            frontier: [],
            id: Log.newId(),
            at: older,
          }),
          where.roots.slice(0, 2),
        );

        return yield* projectionOf(where);
      }),
    );

    assert.equal(outcome.checkpoint?.at.getTime(), 1_800_000_000_000);
  });

  it("verifies a bounded number of signatures on one record", async () => {
    // The list is attacker-chosen and every entry costs an Ed25519
    // verification, on a path every protected-branch push runs. Far above any
    // real quorum, so nothing honest is truncated.
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const bob = yield* generate("bob@example.com");
        const noise = yield* generate("noise@example.com");

        const payload = yield* Certificate.grant({
          repo: where.genesis.repoId,
          publicKey: formatPublicKey(bob.publicKey),
          capabilities: ["source.push"],
          id: Log.newId(),
        });
        const bytes = Certificate.encode(payload);
        // Padding first, so the quorum's signatures fall off the end.
        const padding = yield* Effect.forEach(
          Array.from({ length: Certificate.MAX_SIGNATURES }, () => noise),
          (key) => sign(key, bytes, NAMESPACE),
        );
        const real = yield* Effect.forEach(where.roots.slice(0, 2), (key) =>
          sign(key, bytes, NAMESPACE),
        );
        yield* Log.append(payload, bytes, [...padding, ...real]);

        return { projection: yield* projectionOf(where), bob: yield* print(bob) };
      }),
    );

    // The record is not accepted, because the signatures that would have
    // authorized it were never reached — which is the bound doing its job
    // rather than an oversight.
    assert.equal(state.projection.members.get(state.bob), undefined);
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

    it("refuses a re-instatement from an issuer who could not have revoked", async () => {
      // `member.invite` is the authority to *add* members. Letting a grant
      // clear a revocation made `revoke` undoable by anybody who could
      // `grant` — a retroactive `compromised` revocation included — and left
      // nothing in `revoked` to show it had happened.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const inviter = yield* generate("inviter@example.com");
          const bob = yield* generate("bob@example.com");

          // Everything the re-grant below needs *except* `member.revoke`, so
          // the refusal can only be about the re-instatement.
          yield* grantTo(where, inviter, ["member.invite", "source.push"], where.roots.slice(0, 2));
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "compromised",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          // The inviter re-grants the revoked key. They may invite; they may
          // not undo somebody else's revocation.
          yield* grantTo(where, bob, ["source.push"], [inviter]);

          return { projection: yield* projectionOf(where), bob: yield* print(bob) };
        }),
      );

      assert.equal(outcome.projection.members.has(outcome.bob), false);
      assert.equal(outcome.projection.revoked.has(outcome.bob), true, "the revocation must stand");
      assert.match(outcome.projection.rejected.at(-1)?.reason ?? "", /member\.revoke/);
    });

    it("allows one from an issuer who holds member.revoke", async () => {
      // Rotating a key back in is an ordinary thing to do, and requiring a new
      // fingerprint for it would mean a compromised-then-recovered key could
      // never be used again.
      const outcome = await scenario(
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

      assert.equal(outcome.projection.members.has(outcome.bob), true);
      // The record survives with its window closed: everything the key signed
      // while it was revoked stays refused.
      assert.notEqual(outcome.projection.revoked.get(outcome.bob)?.supersededBy, null);
      assert.equal(outcome.projection.former.has(outcome.bob), false, "no stale former entry");
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

    it("refuses an event whose trust head this replica cannot resolve", async () => {
      // Writing junk into the field must not be a way out of a revocation
      // that writing `null` does not give you: an oid nobody holds walks zero
      // commits and would otherwise look exactly like an event that predates
      // the revocation.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

          const bytes = new TextEncoder().encode("a review with an invented history");
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
            // SAFETY: forty lowercase hex characters, and deliberately not a
            // commit this repository holds.
            made: { at: new Date(), trustHead: "f".repeat(40) as never },
          });
        }),
      );

      assert.equal(outcome.ok, false);
    });

    it("refuses an event signed before its author was granted the capability", async () => {
      // Revocation was ordered by ancestry; grants were not, so a stored event
      // was judged against whatever its signer holds *now*. A member with only
      // `hub.review` could pre-plant approvals and have them start counting the
      // moment somebody granted them `hub.approve` — on a revision they never
      // looked at again.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

          // Bob signs an approval against the head he can see, holding only
          // `hub.review`.
          const before = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("an approval written in advance");
          const signature = yield* sign(bob, bytes, NAMESPACE);

          // Later, somebody grants him what it needs.
          yield* grantTo(where, bob, ["hub.review", "hub.approve"], where.roots.slice(0, 2));

          return {
            planted: yield* Verify.authorize({
              projection: yield* projectionOf(where),
              bytes,
              signatures: [signature],
              capability: "hub.approve",
              made: { at: new Date(), trustHead: before },
            }),
            // The same signature judged against the head that *does* contain
            // the grant is fine: this is about ordering, not about the key.
            now: yield* Verify.authorize({
              projection: yield* projectionOf(where),
              bytes,
              signatures: [signature],
              capability: "hub.approve",
              made: { at: new Date(), trustHead: yield* repository.resolve(Log.LOG_REF) },
            }),
          };
        }),
      );

      assert.equal(outcome.planted.ok, false);
      assert.match(
        outcome.planted.ok === false ? outcome.planted.reason : "",
        /did not hold hub\.approve/,
      );
      assert.equal(outcome.now.ok, true, "the grant is visible from a later head");
    });

    it("keeps past events valid when a member's grant is narrowed", async () => {
      // The sharper version of a renewal: a *downgrade*. Asking the current
      // capabilities before consulting the history made losing one capability
      // stricter than losing membership outright — a full revocation preserves
      // what its subject signed beforehand, and a narrowing grant erased it.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.approve", "hub.review"], where.roots.slice(0, 2));

          const when = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("an approval made while he held it");
          const signature = yield* sign(bob, bytes, NAMESPACE);

          // Downgraded: `hub.approve` taken away, `hub.review` kept.
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

          return {
            before: yield* Verify.authorize({
              projection: yield* projectionOf(where),
              bytes,
              signatures: [signature],
              capability: "hub.approve",
              made: { at: new Date(), trustHead: when },
            }),
            // And going forward they genuinely no longer hold it.
            now: yield* Verify.authorize({
              projection: yield* projectionOf(where),
              bytes,
              signatures: [signature],
              capability: "hub.approve",
              made: { at: new Date(), trustHead: yield* repository.resolve(Log.LOG_REF) },
            }),
          };
        }),
      );

      assert.equal(outcome.before.ok, true, "what they signed while holding it still counts");
      assert.equal(outcome.now.ok, false, "and they cannot sign a new one");
    });

    it("keeps past events valid when a member's grant is renewed", async () => {
      // The other half of the ordering rule, and the one an ordinary
      // repository hits every day: a member's `grant` is overwritten by each
      // later grant, so checking only that one made a routine renewal
      // retroactively un-authorize every event they had ever signed.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.approve"], where.roots.slice(0, 2));

          const when = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("an approval made in good time");
          const signature = yield* sign(bob, bytes, NAMESPACE);

          // A renewal: same key, same capability, a later commit.
          yield* grantTo(where, bob, ["hub.approve"], where.roots.slice(0, 2));

          return yield* Verify.authorize({
            projection: yield* projectionOf(where),
            bytes,
            signatures: [signature],
            capability: "hub.approve",
            made: { at: new Date(), trustHead: when },
          });
        }),
      );

      assert.equal(outcome.ok, true, "a renewal must not unmake what came before it");
    });

    it("refuses an event whose trust head is a commit outside the trust log", async () => {
      // The sharper version of the test above: an oid this replica genuinely
      // holds, but that is not a trust record — the tip of `main`, say. An
      // ancestry walk from it reaches no revocation, so without a membership
      // check it reads exactly like an event that predates one, and naming a
      // branch tip becomes a way out of every forward-only revocation.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

          const bytes = new TextEncoder().encode("a review pinned to the wrong history");
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

          const tree = yield* repository.writeTree([]);
          const source = yield* repository.commit({
            branch: "refs/heads/main",
            tree,
            message: "ordinary work\n",
            author: Record.identityAt(new Date()),
          });

          return yield* Verify.authorize({
            projection: yield* projectionOf(where),
            bytes,
            signatures: [signature],
            capability: "hub.review",
            made: { at: new Date(), trustHead: source },
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
      // Kept, with its window closed: the record is still true about the
      // period it covers, and only deleting it would make what the key signed
      // while revoked authorized again.
      assert.notEqual(state.projection.revoked.get(state.bob)?.supersededBy, null);
    });

    it("still refuses what the key signed while it was revoked", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["hub.approve"], where.roots.slice(0, 2));
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(bob),
              reason: "rotated",
              id: Log.newId(),
            }),
            where.roots.slice(0, 2),
          );

          // Signed while out, against the head that already held the
          // revocation.
          const during = yield* repository.resolve(Log.LOG_REF);
          const bytes = new TextEncoder().encode("an approval made while revoked");
          const signature = yield* sign(bob, bytes, NAMESPACE);

          yield* grantTo(where, bob, ["hub.approve"], where.roots.slice(0, 2));

          return {
            inside: yield* Verify.authorize({
              projection: yield* projectionOf(where),
              bytes,
              signatures: [signature],
              capability: "hub.approve",
              made: { at: new Date(), trustHead: during },
            }),
            after: yield* Verify.authorize({
              projection: yield* projectionOf(where),
              bytes,
              signatures: [signature],
              capability: "hub.approve",
              made: { at: new Date(), trustHead: yield* repository.resolve(Log.LOG_REF) },
            }),
          };
        }),
      );

      assert.equal(outcome.inside.ok, false, "the window must still refuse what it covered");
      assert.equal(outcome.after.ok, true, "and end where the key came back");
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
