import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Exit, Layer } from "effect";

import {
  type Fingerprint,
  fingerprint,
  formatPublicKey,
  generate,
  NAMESPACE,
  type PrivateKey,
  sign,
} from "../crypto/SshSignature.ts";
import { type Invalid, StorageFailure } from "../git/Error.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid } from "../git/Store.ts";
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

/**
 * The same, with reads of whatever `flaky.oid` names failing after the first.
 *
 * A store that is absent and a store that is broken are different answers, and
 * the only way to tell whether a walk keeps them apart is to break one. Failing
 * only after the first read is what makes the two reads of one tree — the path
 * lookup, and the emptiness check behind it — disagree, which is the window a
 * transient failure actually arrives in. Named through a holder rather than up
 * front, because what to break is an oid the run itself writes.
 */
const flaking = (flaky: { oid: Oid | null }) => {
  let seen = 0;
  return Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const inner = yield* ObjectStore;
      return ObjectStore.of({
        ...inner,
        read: (oid) =>
          oid === flaky.oid && seen++ > 0
            ? Effect.fail(new StorageFailure({ operation: "read", path: oid }))
            : inner.read(oid),
      });
    }),
  ).pipe(Layer.provideMerge(stores));
};

const flakily = <A, E>(build: (flaky: { oid: Oid | null }) => Effect.Effect<A, E, Repository>) => {
  // SAFETY: the holder starts empty and the run fills it with an oid it
  // wrote; nothing else reads it, and `null` matches nothing.
  const flaky = { oid: null as Oid | null };
  return Effect.runPromiseExit(
    build(flaky).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(flaking(flaky)),
        ),
      ),
    ),
  );
};

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

/**
 * A record appended so that its commit oid sits in the upper half of the space.
 *
 * The tests below need a *lower* oid than this one, and searching for one is
 * only a bounded search if the target is not already near the bottom — a
 * near-minimal target turns 64 tries into a coin that lands heads once in
 * millions, which is a flake rather than a test. The id is inside the signed
 * bytes and chosen freely, so varying it is the honest issuer's own grinding
 * room, and two tries suffice on average.
 */
const highRecord = Effect.fn("test.highRecord")(function* (
  where: World,
  make: (id: string) => Certificate.TrustPayload,
) {
  const repository = yield* Repository;
  const parent = yield* repository.resolve(Log.LOG_REF);

  for (let attempt = 0; ; attempt++) {
    const payload = make(Log.newId());
    const bytes = Certificate.encode(payload);
    const signatures = yield* Effect.forEach(where.roots.slice(0, 2), (key) =>
      sign(key, bytes, NAMESPACE),
    );
    const commit = yield* Record.write({
      name: Log.RECORD,
      payload: bytes,
      signatures,
      parents: parent === null ? [] : [parent],
      message: `${payload.type} ${payload.id}\n`,
    });
    if (commit < "8".padEnd(40, "0") && attempt < 64) continue;
    yield* repository.setRef({ name: Log.LOG_REF, to: commit, expected: parent });
    return { payload, bytes, signatures, commit };
  }
});

/** The first commit a builder produces that sorts below `target`, or `null`. */
const below = Effect.fn("test.below")(function* (
  target: Oid,
  build: (attempt: number) => Effect.Effect<Oid, Invalid | StorageFailure, Repository>,
) {
  for (let attempt = 0; attempt < 64; attempt++) {
    const commit = yield* build(attempt);
    if (commit < target) return commit;
  }
  return null;
});

describe("what an event's declared trust head can cost", () => {
  it.effect("walks no further from a declared head than it would from the log", () =>
    Effect.promise(async () => {
      // The oid is written by the event's own signer, and a branch of commits
      // carrying a record — or empty trees, which is what a join looks like —
      // passes the namespace test. Unbounded, one such branch is walked again on
      // every protected-branch push, every collection and every deepening fetch
      // that reads that event.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          // A chain of joins: hub-and-trust-shaped, and none of it in the log.
          let head = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "join\n",
            author: Record.identityAt(new Date(1_700_000_000_000)),
          });
          for (let index = 0; index < 6; index++) {
            head = yield* repository.commitTree({
              tree: EMPTY_TREE_OID,
              parents: [head],
              message: `join ${index}\n`,
              author: Record.identityAt(new Date(1_700_000_000_000)),
            });
          }

          return {
            whole: (yield* Log.ancestry(head)).size,
            bounded: yield* Log.ancestry(head).pipe(
              Effect.provide(Log.ceiling(3)),
              Effect.as(null),
              Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
            ),
          };
        }),
      );

      assert.equal(outcome.whole, 7, "the whole chain is reachable when nothing bounds it");
      // Refused, not answered empty. "Reaches nothing" is not the conservative
      // reading: it makes every forward-only revocation invisible, so an event
      // naming such a head would count rather than being turned away. The
      // callers turn this failure into a denial of that one event.
      assert.match(outcome.bounded ?? "", /more than 3 commits/);
    }),
  );

  it.effect("does not read a broken store as 'not part of this history'", () =>
    Effect.promise(async () => {
      // Absent and broken are different answers. The walk that decides whether a
      // commit belongs to the trust log tolerates absence deliberately — refs are
      // applied without a connectivity check, so a replica can hold a commit
      // whose tree never arrived — and it was tolerating failure along with it.
      // That walk *is* the boundary of the history, so a failure read as "not
      // part of it" does not skip one commit: it empties the log. No members and
      // no revocations, cached under an unchanged head — every revoked key
      // authorized again, and a private repository reporting itself as public,
      // with nothing raised anywhere.
      const asked = await flakily((flaky) =>
        Effect.gen(function* () {
          const repository = yield* Repository;
          // A tree with something in it and no record: the shape whose emptiness
          // has to be read from the store rather than from the oid.
          const tree = yield* repository.writeTree([
            {
              mode: "100644",
              name: "file.txt",
              oid: yield* repository.writeBlob(new Uint8Array(1)),
            },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "not a record\n",
            author: Record.identityAt(new Date(1_700_000_000_000)),
          });
          // From here the path lookup answers and the emptiness check behind it
          // does not, which is what a transient failure looks like.
          flaky.oid = tree;
          return yield* Log.isTrustCommit(commit);
        }),
      );

      assert.ok(Exit.isFailure(asked), "a store that failed is not a store that said no");
    }),
  );

  it.effect("refuses an event whose declared head it cannot walk, rather than counting it", () =>
    Effect.promise(async () => {
      // The denial is what makes the refusal above the conservative answer. Read
      // as "reaches nothing", a revoked signer's event would show no revocation
      // in its ancestry and count; read as a refusal, the event is turned away
      // and the revocation stands.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const root = yield* generate("root@example.com");
          const member = yield* generate("member@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(member.publicKey),
              capabilities: ["hub.comment"],
              id: Log.newId(),
            }),
            [root],
          );
          const head = yield* repository.resolve(Log.LOG_REF);
          const projection = yield* project(genesis);

          const bytes = new TextEncoder().encode("a statement");
          const made = { at: new Date(1_700_000_000_000), trustHead: head };
          const signed = [yield* fingerprint(member.publicKey)];
          const judged = (ceiling: number) =>
            Verify.authorize({
              projection,
              bytes,
              signatures: [],
              signed,
              capability: "hub.comment",
              made,
            }).pipe(Effect.provide(Log.ceiling(ceiling)));

          return { walkable: yield* judged(64), beyond: yield* judged(0) };
        }),
      );

      assert.equal(outcome.walkable.ok, true, "an ordinary head is walked and the grant found");
      assert.equal(outcome.beyond.ok, false, "and one this host will not walk shows no grant");
    }),
  );
});

describe("trust projection", () => {
  it.effect("folds a grant signed by the root quorum into a member", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("refuses a grant that does not meet the root threshold", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("lets a member with member.invite grant what they hold", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("refuses an issuer granting more than they hold", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("treats repo.admin as carrying every capability", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("refuses a grant issued by a member whose own grant had expired", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("accepts one an issuer signed while their grant was still live", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("does not let an unsigned copy of a record displace the signed one", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("refuses a record whose id has already been applied", () =>
    Effect.promise(async () => {
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

      assert.equal(
        state.projection.members.get(state.bob),
        undefined,
        "the replay must not revive",
      );
      assert.notEqual(state.projection.revoked.get(state.bob), undefined);
      assert.match(state.projection.rejected.at(-1)?.reason ?? "", /already been applied/);
    }),
  );

  it.effect("does not let a grafted replay of a record displace the original", () =>
    Effect.promise(async () => {
      // Which commit owns a record decides what `Revocation.commit` is, and an
      // event's trust head has to *reach* that commit for the revocation to
      // apply. Decided by descendant count alone, the rule assumed a replay is
      // descended from by nothing — but append-only containment forces the
      // replay to arrive as a join over both copies, and a join descends from
      // both. Where the targeted record is the current head the counts came out
      // equal, the decision fell to the oid, and whoever writes the replay can
      // grind that: the revocation's commit moves onto a graft that honest trust
      // heads cannot reach, and the revocation stops applying.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          const subject = yield* print(bob);
          const record = yield* highRecord(where, (id) =>
            Certificate.revoke({ repo: where.genesis.repoId, subject, reason: "left", id }),
          );
          const genuine = record.commit;

          // The same record, byte for byte, grafted in with no history behind
          // it. The message is not part of the record's identity, so varying it
          // is free oid grinding — which is exactly the attacker's position.
          const replay = yield* below(genuine, (attempt) =>
            Record.write({
              name: Log.RECORD,
              payload: record.bytes,
              signatures: record.signatures,
              parents: [],
              message: `${record.payload.type} ${record.payload.id} ${attempt}\n`,
            }),
          );
          if (replay === null) return { ground: false, genuine, owner: null };
          yield* repository.setRef({
            name: Log.LOG_REF,
            to: yield* Log.join([genuine, replay]),
          });

          const projection = yield* projectionOf(where);
          return {
            ground: true,
            genuine,
            owner: projection.revoked.get(subject)?.[0]?.commit ?? null,
          };
        }),
      );

      assert.equal(outcome.ground, true, "the fixture must actually grind a lower oid");
      assert.equal(outcome.owner, outcome.genuine, "the record in the log's history owns it");
    }),
  );

  it.effect("does not let an extra junk signature escape the duplicate resolution", () =>
    Effect.promise(async () => {
      // Two keys that had to agree, and did not. `winner` grouped on the id, the
      // bytes *and the signatures*; the duplicate check grouped on the id and
      // the bytes. A copy carrying one extra unparseable signature was therefore
      // a group of its own — the only member of it, so it won its own descent —
      // and reached the fold on order alone, whose tie-break for parentless
      // commits is a grindable oid. The graft then owned the record: `grant`,
      // `history[].commit` and `Revocation.commit` moved onto a commit honest
      // trust heads cannot reach, which is the exact failure descent resolution
      // was added to prevent, walked around by appending a byte.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          const subject = yield* print(bob);
          const record = yield* highRecord(where, (id) =>
            Certificate.revoke({ repo: where.genesis.repoId, subject, reason: "left", id }),
          );
          const genuine = record.commit;

          const replay = yield* below(genuine, (attempt) =>
            Record.write({
              name: Log.RECORD,
              payload: record.bytes,
              // The one byte that used to be enough.
              signatures: [...record.signatures, `not a signature ${attempt}`],
              parents: [],
              message: `${record.payload.type} ${record.payload.id}\n`,
            }),
          );
          if (replay === null) return { ground: false, genuine, applied: false, owner: null };
          yield* repository.setRef({
            name: Log.LOG_REF,
            to: yield* Log.join([genuine, replay]),
          });

          const projection = yield* projectionOf(where);
          return {
            ground: true,
            genuine,
            applied: projection.members.has(subject) === false,
            owner: projection.revoked.get(subject)?.[0]?.commit ?? null,
          };
        }),
      );

      assert.equal(outcome.ground, true, "the fixture must actually grind a lower oid");
      assert.equal(outcome.applied, true, "the revocation still applies");
      assert.equal(outcome.owner, outcome.genuine, "and the record in the log's history owns it");
    }),
  );

  it.effect("does not let unsigned replays crowd the real signatures out of a record", () =>
    Effect.promise(async () => {
      // Authority for a statement is the union of the signatures every copy of
      // it carried, and verifying them is what that union costs — so the number
      // of copies consulted is bounded. Bounded in *walk* order, the bound was
      // itself the attack: parentless replays sort early, so a handful of
      // unsigned copies filled the list with empty signature sets and the
      // genuine commit — which still wins the descent — was folded with no
      // signers at all. The quorum then failed and the record was rejected,
      // which is any `source.push` holder nullifying a revocation.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const bob = yield* generate("bob@example.com");
          yield* grantTo(where, bob, ["source.push"], where.roots.slice(0, 2));

          const subject = yield* print(bob);
          const record = yield* highRecord(where, (id) =>
            Certificate.revoke({ repo: where.genesis.repoId, subject, reason: "left", id }),
          );

          // More unsigned copies than the endorsement bound holds, each grafted
          // in with no history and joined over the genuine one.
          const heads = [record.commit];
          for (let attempt = 0; attempt < 12; attempt++) {
            heads.push(
              yield* Record.write({
                name: Log.RECORD,
                payload: record.bytes,
                signatures: [],
                parents: [],
                message: `${record.payload.type} ${record.payload.id} ${attempt}\n`,
              }),
            );
          }
          yield* repository.setRef({ name: Log.LOG_REF, to: yield* Log.join(heads) });

          const projection = yield* projectionOf(where);
          return {
            applied: !projection.members.has(subject),
            owner: projection.revoked.get(subject)?.[0]?.commit ?? null,
            genuine: record.commit,
          };
        }),
      );

      assert.equal(outcome.applied, true, "the revocation must still apply");
      assert.equal(outcome.owner, outcome.genuine, "and the record in the log's history owns it");
    }),
  );

  it.effect("does not let one record's id burn a different record's", () =>
    Effect.promise(async () => {
      // Keyed on the bare id, "already applied" was a weapon: any member holding
      // a trust capability could publish a record they were perfectly entitled
      // to make — a grant to a key of their own — re-using the id of a
      // revocation naming them, and the revocation behind it was then discarded
      // as a duplicate. On an append-only ref that is permanent, and it defeats
      // the one operation membership exists to be able to perform. Two records
      // with one id but different content are two different claims, and each is
      // answered on its own authority.
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const mallory = yield* generate("mallory@example.com");
          const friend = yield* generate("friend@example.com");
          yield* grantTo(where, mallory, ["member.invite", "source.push"], where.roots.slice(0, 2));

          // The id the roots are about to use, spent first on something Mallory
          // may legitimately write.
          const burned = Log.newId();
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(friend.publicKey),
              capabilities: ["source.push"],
              id: burned,
            }),
            [mallory],
          );

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* print(mallory),
              reason: "left",
              id: burned,
            }),
            where.roots.slice(0, 2),
          );

          return { projection: yield* projectionOf(where), mallory: yield* print(mallory) };
        }),
      );

      assert.equal(
        state.projection.members.has(state.mallory),
        false,
        "the revocation must apply despite sharing an id",
      );
      assert.notEqual(state.projection.revoked.get(state.mallory), undefined);
    }),
  );

  it.effect("does not let an unauthorized record burn a legitimate record's id", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("keeps the newest checkpoint, not the last one folded", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("verifies a bounded number of signatures on one record", () =>
    Effect.promise(async () => {
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
    }),
  );

  describe("revocation", () => {
    it.effect("removes a member and keeps what they held", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("denies a live request from a revoked key", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses a re-instatement from an issuer who could not have revoked", () =>
      Effect.promise(async () => {
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
            yield* grantTo(
              where,
              inviter,
              ["member.invite", "source.push"],
              where.roots.slice(0, 2),
            );
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
        assert.equal(
          outcome.projection.revoked.has(outcome.bob),
          true,
          "the revocation must stand",
        );
        assert.match(outcome.projection.rejected.at(-1)?.reason ?? "", /member\.revoke/);
      }),
    );

    it.effect("allows one from an issuer who holds member.revoke", () =>
      Effect.promise(async () => {
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
        assert.notEqual(outcome.projection.revoked.get(outcome.bob)?.at(-1)?.supersededBy, null);
        assert.equal(outcome.projection.former.has(outcome.bob), false, "no stale former entry");
      }),
    );

    it.effect("keeps a compromise's reach when a second revocation follows it", () =>
      Effect.promise(async () => {
        // Windows are a list, not a field. Kept as one record per key, a second
        // `trust.revoke` overwrote the first — so the retroactive class, the one
        // whose whole point is that those signatures were never the subject's,
        // could be erased by anybody who could revoke: revoke the key again for
        // any ordinary reason, grant it back, and every signature made during
        // the compromise was authorized once more.
        const outcome = await scenario(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const where = yield* world();
            const bob = yield* generate("bob@example.com");
            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

            // What the attacker signed while holding the key, naming the head
            // that predates every revocation — the best case they can construct.
            const stolen = yield* repository.resolve(Log.LOG_REF);
            const bytes = new TextEncoder().encode("an approval the attacker made");
            const signature = yield* sign(bob, bytes, NAMESPACE);

            yield* Log.issue(
              Certificate.revoke({
                repo: where.genesis.repoId,
                subject: yield* print(bob),
                reason: "compromised",
                id: Log.newId(),
              }),
              where.roots.slice(0, 2),
            );
            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));
            // A second, ordinary revocation, and a second re-instatement.
            yield* Log.issue(
              Certificate.revoke({
                repo: where.genesis.repoId,
                subject: yield* print(bob),
                reason: "left",
                id: Log.newId(),
              }),
              where.roots.slice(0, 2),
            );
            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

            const projection = yield* projectionOf(where);
            return {
              windows: projection.revoked.get(yield* print(bob))?.length ?? 0,
              authorized: yield* Verify.authorize({
                projection,
                bytes,
                signatures: [signature],
                capability: "hub.review",
                made: { at: new Date(), trustHead: stolen },
              }),
            };
          }),
        );

        assert.equal(outcome.windows, 2, "both intervals stay on the record");
        assert.equal(
          outcome.authorized.ok,
          false,
          "a compromise still reaches everything the key signed",
        );
      }),
    );

    it.effect("strengthens the window a key is already out on rather than opening a second", () =>
      Effect.promise(async () => {
        // Two consecutive revocations appended two *open* windows, and a grant
        // closes the last one — so the first stayed open forever. Live pushes
        // were waved through (`openWindow` reads the last) while every stored
        // event by that key was refused against the window nothing could close,
        // on a ref that can never be rewound: a key re-instated in good faith
        // and then unable to have a single review counted.
        //
        // Learning afterwards that a key was compromised is the reason to send a
        // second revocation, so it escalates the window in place — keeping its
        // original start, because that is when the key stopped being trusted.
        const outcome = await scenario(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const where = yield* world();
            const bob = yield* generate("bob@example.com");
            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

            const subject = yield* print(bob);
            const revoke = (reason: "left" | "compromised") =>
              Log.issue(
                Certificate.revoke({
                  repo: where.genesis.repoId,
                  subject,
                  reason,
                  id: Log.newId(),
                }),
                where.roots.slice(0, 2),
              );

            yield* revoke("left");
            yield* revoke("compromised");
            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

            // Bob is back, and signs against the head that let him back in.
            const back = yield* repository.resolve(Log.LOG_REF);
            const bytes = new TextEncoder().encode("a review made after coming back");
            const signature = yield* sign(bob, bytes, NAMESPACE);

            const projection = yield* projectionOf(where);
            return {
              windows: projection.revoked.get(subject)?.length ?? 0,
              member: projection.members.has(subject),
              authorized: yield* Verify.authorize({
                projection,
                bytes,
                signatures: [signature],
                capability: "hub.review",
                made: { at: new Date(), trustHead: back },
              }),
            };
          }),
        );

        assert.equal(outcome.windows, 1, "a key already out is not put out twice");
        assert.equal(outcome.member, true, "and the re-instatement takes effect");
        assert.equal(
          outcome.authorized.ok,
          true,
          "an event made after the re-instatement must not be refused forever",
        );
      }),
    );

    it.effect("keeps the stronger reason when a weaker revocation follows it", () =>
      Effect.promise(async () => {
        // The window a key is out on is strengthened, never relabelled. Written
        // over, a compromise became "left" the moment anybody revoked the same
        // key again for an ordinary reason: `compromisedFrom` still reached
        // backwards so authorization was unaffected, but `hub members` told an
        // operator the key had merely walked away.
        const outcome = await scenario(
          Effect.gen(function* () {
            const where = yield* world();
            const bob = yield* generate("bob@example.com");
            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

            const subject = yield* print(bob);
            const revoke = (reason: "left" | "compromised") =>
              Log.issue(
                Certificate.revoke({
                  repo: where.genesis.repoId,
                  subject,
                  reason,
                  id: Log.newId(),
                }),
                where.roots.slice(0, 2),
              );
            yield* revoke("compromised");
            yield* revoke("left");

            const projection = yield* projectionOf(where);
            return projection.revoked.get(subject)?.at(-1) ?? null;
          }),
        );

        assert.equal(outcome?.reason, "compromised");
        assert.notEqual(outcome?.compromisedFrom, null, "and it still reaches backwards");
      }),
    );

    it.effect("does not move a closed window's end forward on an ordinary renewal", () =>
      Effect.promise(async () => {
        // The window ends where the key came *back*, and nowhere later. Written
        // as an unconditional overwrite, every subsequent grant to the same key
        // — a renewal, an added capability, an extended expiry — pushed the end
        // along, and every event signed against the *first* re-instatement
        // stopped reaching it and was judged as if made while revoked. A member
        // in good standing would watch their approvals vanish and their merges
        // start failing, at the moment somebody widened their capabilities.
        const outcome = await scenario(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const where = yield* world();
            const bob = yield* generate("bob@example.com");

            yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));
            yield* Log.issue(
              Certificate.revoke({
                repo: where.genesis.repoId,
                subject: yield* print(bob),
                reason: "rotated",
                id: Log.newId(),
              }),
              where.roots.slice(0, 2),
            );
            const back = yield* grantTo(where, bob, ["hub.review"], where.roots.slice(0, 2));

            // Bob signs as a member again, naming the head that re-instated him.
            const reinstated = yield* repository.resolve(Log.LOG_REF);
            const bytes = new TextEncoder().encode("a review made after coming back");
            const signature = yield* sign(bob, bytes, NAMESPACE);

            // And only then is his membership renewed with one more capability.
            yield* grantTo(where, bob, ["hub.review", "hub.comment"], where.roots.slice(0, 2));

            const projection = yield* projectionOf(where);
            return {
              back,
              ended: projection.revoked.get(yield* print(bob))?.at(-1)?.supersededBy ?? null,
              authorized: yield* Verify.authorize({
                projection,
                bytes,
                signatures: [signature],
                capability: "hub.review",
                made: { at: new Date(), trustHead: reinstated },
              }),
            };
          }),
        );

        assert.equal(outcome.ended, outcome.back, "the window ends where the key came back");
        assert.equal(outcome.authorized.ok, true, "a renewal must not invalidate signed history");
      }),
    );

    it.effect("leaves an event made before the revocation was visible valid", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses an event whose author had already seen the revocation", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses an event whose trust head this replica cannot resolve", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses an event signed before its author was granted the capability", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("keeps past events valid when a member's grant is narrowed", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("keeps past events valid when a member's grant is renewed", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses an event whose trust head is a commit outside the trust log", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("reaches backwards when the key was compromised", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("lets a later grant re-instate a revoked key", () =>
      Effect.promise(async () => {
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
        assert.notEqual(state.projection.revoked.get(state.bob)?.at(-1)?.supersededBy, null);
      }),
    );

    it.effect("still refuses what the key signed while it was revoked", () =>
      Effect.promise(async () => {
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
      }),
    );
  });

  describe("root authority", () => {
    it.effect("rotates the root set when the quorum signs", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses a root change from an admin who is not a root", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("uses the new roots for records that follow a rotation", () =>
      Effect.promise(async () => {
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
      }),
    );
  });

  describe("expiry", () => {
    it.effect("denies a member whose grant has expired", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("allows a member whose grant has not expired yet", () =>
      Effect.promise(async () => {
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
      }),
    );
  });

  describe("capabilities", () => {
    it.effect("denies a capability the member does not hold", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("scopes a check capability to its own check name", () =>
      Effect.sync(() => {
        assert.ok(Certificate.permits(["hub.check:test"], Certificate.checkCapability("test")));
        assert.equal(
          Certificate.permits(["hub.check:test"], Certificate.checkCapability("deploy")),
          false,
          "a bot trusted for `test` must not be able to sign `deploy`",
        );
        assert.ok(Certificate.permits(["hub.check:*"], Certificate.checkCapability("deploy")));
      }),
    );

    it.effect("will not even build a grant naming one", () =>
      Effect.promise(async () => {
        // The fold refuses it, and the fold runs *after* the record is written —
        // on a log that only grows. So a capability somebody typed wrong was
        // pinned on a ref nothing can rewind, rejected for ever and re-read on
        // every membership check. Refused where it is written instead.
        const refused = await scenario(
          Effect.gen(function* () {
            const where = yield* world();
            const bob = yield* generate("bob@example.com");
            return yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(bob.publicKey),
              capabilities: ["source.pussh"],
              id: Log.newId(),
            }).pipe(
              Effect.as(null),
              Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
            );
          }),
        );
        assert.match(refused ?? "", /unknown capability/);
      }),
    );

    it.effect("refuses a grant naming a capability that does not exist", () =>
      Effect.promise(async () => {
        // Built by hand rather than through `Certificate.grant`, which refuses
        // one outright so a typo cannot be pinned on an append-only ref. The
        // fold has to refuse it too: a record can arrive from another
        // implementation, or straight off a push, having gone through no
        // constructor of ours at all.
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
            yield* Log.issue(
              { ...payload, capabilities: ["source.pussh"] },
              where.roots.slice(0, 2),
            );
            return yield* projectionOf(where);
          }),
        );
        assert.match(state.rejected[0]!.reason, /unknown capability/);
      }),
    );
  });

  describe("binding", () => {
    it.effect("ignores a record written for another repository", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("ignores a grant whose subject is not the key beside it", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("ignores a record nobody signed", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("survives a record it cannot read at all", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("keeps folding after an unauthorized record", () =>
      Effect.promise(async () => {
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
      }),
    );
  });

  describe("checkpoints", () => {
    it.effect("records the newest checkpoint an admin signed", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("calls a view with no checkpoint stale when freshness is required", () =>
      Effect.promise(async () => {
        const state = await scenario(Effect.flatMap(world(), (where) => projectionOf(where)));
        const freshness = Verify.fresh(state, 60_000);
        assert.equal(freshness.ok, false);
      }),
    );

    it.effect("accepts a recent checkpoint and rejects an old one", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses one dated in the future, which would otherwise never go stale", () =>
      Effect.promise(async () => {
        // `at` is written by whoever signs the checkpoint, and `project` keeps
        // the one with the greatest `at`. An age allowed to go negative meant a
        // single forward-dated attestation satisfied `maxTrustAgeSeconds` for as
        // long as it was dated ahead — so a replica could withhold every later
        // revocation and still answer "fresh", which is the exact bound this
        // check exists to enforce.
        const state = await scenario(
          Effect.gen(function* () {
            const where = yield* world();
            yield* Log.issue(
              Certificate.checkpoint({
                repo: where.genesis.repoId,
                frontier: [],
                id: Log.newId(),
                at: new Date(Date.now() + 86_400_000),
              }),
              where.roots.slice(0, 2),
            );
            return yield* projectionOf(where);
          }),
        );

        const freshness = Verify.fresh(state, 60_000);
        assert.equal(freshness.ok, false);
        assert.match(freshness.ok ? "" : freshness.reason, /in the future/);
      }),
    );

    it.effect("looks past one dated in the future to the honest one behind it", () =>
      Effect.promise(async () => {
        // The other half of that guard. `project` used to keep only the greatest
        // `at`, so one attestation dated ahead — a malicious admin, or a CI box
        // with a fast clock — became the *only* checkpoint on record, and
        // refusing it then refused every write on a repository with
        // `maxTrustAgeSeconds` set: including the write to `refs/meta/policy`
        // that would lift the bound. A repository frozen by a typo, with no way
        // back. The fold keeps the newest few and the clock lives in `fresh`.
        const state = await scenario(
          Effect.gen(function* () {
            const where = yield* world();
            const checkpoint = (at: Date) =>
              Log.issue(
                Certificate.checkpoint({
                  repo: where.genesis.repoId,
                  frontier: [],
                  id: Log.newId(),
                  at,
                }),
                where.roots.slice(0, 2),
              );
            yield* checkpoint(new Date(Date.now() - 30_000));
            yield* checkpoint(new Date(Date.now() + 86_400_000));
            return yield* projectionOf(where);
          }),
        );

        assert.equal(state.checkpoints.length, 2, "both stay on the record");
        assert.equal(Verify.fresh(state, 60_000).ok, true, "the honest one still answers");
        assert.equal(Verify.fresh(state, 10_000).ok, false, "and it is still judged on its age");
      }),
    );

    it.effect("keeps a newly pushed one however many future-dated ones precede it", () =>
      Effect.promise(async () => {
        // The list is bounded, so sorting by `at` and truncating handed a host
        // with a fast clock a way to evict every credible checkpoint simply by
        // checkpointing on a schedule — and `fresh` skipping past them then found
        // nothing behind, refusing every write on a repository with
        // `maxTrustAgeSeconds` set, the write to `refs/meta/policy` that would
        // lift the bound included. Keeping the tail of the log as well makes the
        // recovery the obvious one: push a checkpoint.
        const state = await scenario(
          Effect.gen(function* () {
            const where = yield* world();
            const checkpoint = (at: Date) =>
              Log.issue(
                Certificate.checkpoint({
                  repo: where.genesis.repoId,
                  frontier: [],
                  id: Log.newId(),
                  at,
                }),
                where.roots.slice(0, 2),
              );
            // More forward-dated attestations than the list holds from one end.
            for (let index = 0; index < 40; index++) {
              yield* checkpoint(new Date(Date.now() + 86_400_000 + index));
            }
            // …and then the operator notices and checkpoints honestly.
            yield* checkpoint(new Date(Date.now() - 5_000));
            return yield* projectionOf(where);
          }),
        );

        assert.equal(Verify.fresh(state, 60_000).ok, true, "the recovery must actually recover");
      }),
    );

    it.effect("still allows the seconds of clock skew two honest hosts have", () =>
      Effect.promise(async () => {
        const state = await scenario(
          Effect.gen(function* () {
            const where = yield* world();
            yield* Log.issue(
              Certificate.checkpoint({
                repo: where.genesis.repoId,
                frontier: [],
                id: Log.newId(),
                at: new Date(Date.now() + 2_000),
              }),
              where.roots.slice(0, 2),
            );
            return yield* projectionOf(where);
          }),
        );

        assert.equal(Verify.fresh(state, 60_000).ok, true);
      }),
    );
  });

  it.effect("reaches the same state on every replica, whatever the write order", () =>
    Effect.promise(async () => {
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
    }),
  );
});
