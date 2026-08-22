import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Event from "../hub/Event.ts";
import { project } from "../hub/Projection.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as SocialLog from "../social/Log.ts";
import * as Statement from "../social/Statement.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { principalId } from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Policy from "./Policy.ts";
import { reconcile, stateFetchPasses } from "./Replication.ts";

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

const author: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** A repository whose one member may do everything a pull request needs. */
const world = Effect.fn("test.world")(function* () {
  const root = yield* generate("root@example.com");
  const dev = yield* generate("dev@example.com");
  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(dev.publicKey),
      capabilities: ["repo.admin"],
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, root, dev };
});

describe("Replication", () => {
  it.effect("fetches trust and policy before social state, then hub events", () =>
    Effect.sync(() => {
      assert.deepEqual(
        stateFetchPasses.map((pass) => pass.map((spec) => spec.source)),
        [["refs/meta/trust/*", "refs/meta/policy"], ["refs/social/log"], ["refs/hub/*"]],
      );
    }),
  );

  it.effect("joins two divergent histories of one pull request", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();

          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "shared",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const ref = Event.refOf(pr);
          const shared = yield* repository.resolve(ref);

          // Our side says one thing…
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "ours",
            key: where.dev,
          });
          const ours = yield* repository.resolve(ref);

          // …and another replica, from the same starting point, says another.
          yield* repository.setRef({ name: ref, to: shared!, expected: ours });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "theirs",
            key: where.dev,
          });
          const theirs = yield* repository.resolve(ref);
          yield* repository.setRef({ name: ref, to: ours!, expected: theirs });

          const divergence = yield* reconcile(ref, theirs!);
          const trust = yield* projectTrust(where.genesis);
          const state = yield* project(where.genesis, trust, pr);
          return { divergence, state };
        }),
      );

      assert.notEqual(outcome.divergence.joined, null, "a divergent hub ref must be joined");
      // Both sides survive: choosing one would have dropped what the other said.
      const bodies = outcome.state.threads.flatMap((thread) =>
        thread.comments.map((comment) => comment.body),
      );
      assert.deepEqual([...bodies].sort(), ["ours", "theirs"]);
    }),
  );

  it.effect("fast-forwards rather than joining when one side is behind", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "ahead",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const ref = Event.refOf(pr);
          const behind = yield* repository.resolve(ref);

          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "later",
            key: where.dev,
          });
          const ahead = yield* repository.resolve(ref);
          yield* repository.setRef({ name: ref, to: behind!, expected: ahead });

          const divergence = yield* reconcile(ref, ahead!);
          return { divergence, now: yield* repository.resolve(ref) };
        }),
      );

      assert.equal(outcome.divergence.joined, null, "a fast-forward needs no join commit");
      assert.equal(outcome.now, outcome.divergence.theirs);
    }),
  );

  it.effect("reports a divergent branch and refuses to invent a merge", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const ours = yield* repository.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "ours",
            author,
          });
          const theirs = yield* repository.commit({
            branch: "refs/heads/other",
            tree: EMPTY_TREE_OID,
            message: "theirs",
            author,
          });

          const divergence = yield* reconcile("refs/heads/main", theirs);
          return { divergence, ours, now: yield* repository.resolve("refs/heads/main") };
        }),
      );

      assert.equal(outcome.divergence.joined, null);
      assert.equal(outcome.now, outcome.ours, "a branch must be left exactly as it was");
    }),
  );

  it.effect("takes a ref this replica has never seen", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "new",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const ref = Event.refOf(pr);
          const theirs = yield* repository.resolve(ref);
          yield* repository.deleteRef(ref);

          yield* reconcile(ref, theirs!);
          return { theirs, now: yield* repository.resolve(ref) };
        }),
      );
      assert.equal(outcome.now, outcome.theirs);
    }),
  );

  it.effect("does not repoint the trust log when some other hub ref diverges", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          // A genesis and a trust log, so there is a membership state to lose.
          yield* world();
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // An append-only hub ref that is not a pull request. Sending it down
          // the trust-log path would land a hub commit on the membership log and
          // wipe the projection every capability check reads.
          const first = yield* repository.commit({
            branch: "refs/hub/index",
            tree: EMPTY_TREE_OID,
            message: "ours",
            author,
          });
          const second = yield* repository.commit({
            branch: "refs/heads/scratch",
            tree: EMPTY_TREE_OID,
            message: "theirs",
            author,
          });

          yield* reconcile("refs/hub/index", second);
          return {
            trustHead,
            trustNow: yield* repository.resolve(Log.LOG_REF),
            joined: yield* repository.resolve("refs/hub/index"),
            first,
          };
        }),
      );

      assert.equal(outcome.trustNow, outcome.trustHead, "the trust log must not have moved");
      assert.notEqual(outcome.joined, outcome.first, "the diverged ref should have been joined");
    }),
  );

  it.effect("joins the trust log when two replicas both granted membership", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const shared = yield* repository.resolve(Log.LOG_REF);

          const first = yield* generate("first@example.com");
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(first.publicKey),
              capabilities: ["repo.read"],
              id: Log.newId(),
            }),
            [where.root],
          );
          const ours = yield* repository.resolve(Log.LOG_REF);

          // The other replica granted somebody else from the same head.
          yield* repository.setRef({ name: Log.LOG_REF, to: shared!, expected: ours });
          const second = yield* generate("second@example.com");
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(second.publicKey),
              capabilities: ["repo.read"],
              id: Log.newId(),
            }),
            [where.root],
          );
          const theirs = yield* repository.resolve(Log.LOG_REF);
          yield* repository.setRef({ name: Log.LOG_REF, to: ours!, expected: theirs });

          yield* reconcile(Log.LOG_REF, theirs!);
          const trust = yield* projectTrust(where.genesis);
          return trust.members.size;
        }),
      );

      // The developer from `world`, plus both concurrently granted members:
      // neither grant may be lost by the join.
      assert.equal(outcome, 3);
    }),
  );

  it.effect("joins two divergent social logs without losing either statement", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const trust = yield* projectTrust(where.genesis);
          const principal = principalId(where.genesis.repoId);
          const context = (petname: string, socialHead: string | null) =>
            Statement.follow({
              author: principal,
              subject: principal,
              id: SocialLog.newId(),
              socialHead,
              trustHead: trust.head,
              petname,
            });

          yield* SocialLog.issue(context("shared", null), where.dev);
          const shared = yield* repository.resolve(SocialLog.LOG_REF);
          assert.ok(shared);
          yield* SocialLog.issue(context("ours", shared), where.dev);
          const ours = yield* repository.resolve(SocialLog.LOG_REF);
          assert.ok(ours);

          yield* repository.setRef({ name: SocialLog.LOG_REF, to: shared, expected: ours });
          yield* SocialLog.issue(context("theirs", shared), where.dev);
          const theirs = yield* repository.resolve(SocialLog.LOG_REF);
          assert.ok(theirs);
          yield* repository.setRef({ name: SocialLog.LOG_REF, to: ours, expected: theirs });

          const divergence = yield* reconcile(SocialLog.LOG_REF, theirs);
          const log = yield* SocialLog.verified(where.genesis, trust);
          return {
            divergence,
            petnames: log.statements.flatMap((entry) =>
              entry.payload.type === "social.follow" ? [entry.payload.petname] : [],
            ),
          };
        }),
      );

      assert.notEqual(outcome.divergence.joined, null);
      assert.deepEqual([...outcome.petnames].sort(), ["ours", "shared", "theirs"]);
    }),
  );

  it.effect("takes the source's rules file rather than reporting it as a divergence", () =>
    Effect.promise(async () => {
      // The rules file is neither append-only nor a branch: it is what the
      // repository publishes about itself. Lumping it in with branches left a
      // replica enforcing rules the source had already superseded — a branch
      // still protected after the protection was lifted — and reported as a
      // divergence a person was expected to resolve by hand on every replica.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;

          const ours = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "our rules",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: ours, expected: null });

          // Written independently on the source: no ancestry between the two.
          const theirs = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "their rules",
            author,
          });

          const divergence = yield* reconcile(Policy.RULES_REF, theirs);
          return {
            diverged: divergence.diverged,
            at: yield* repository.resolve(Policy.RULES_REF),
            theirs,
          };
        }),
      );

      assert.equal(outcome.diverged, false, "the source's rules are not a conflict to escalate");
      assert.equal(outcome.at, outcome.theirs, "and the replica now enforces them");
    }),
  );
});
