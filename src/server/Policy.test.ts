import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { type Member, project as projectTrust } from "../trust/Projection.ts";
import { apply, evaluate, OPEN, type Principal, type Rules } from "./Policy.ts";

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

interface World {
  readonly genesis: Genesis;
  readonly root: PrivateKey;
  readonly dev: PrivateKey;
  readonly principal: Principal;
}

const world = Effect.fn("test.world")(function* (capabilities: ReadonlyArray<string>) {
  const root = yield* generate("root@example.com");
  const dev = yield* generate("dev@example.com");

  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(dev.publicKey),
      capabilities,
      id: Log.newId(),
    }),
    [root],
  );

  const trust = yield* projectTrust(genesis);
  // SAFETY: the grant above is the only one in this repository, so the
  // projection holds exactly one member.
  const member = [...trust.members.values()].at(0) as Member;
  return {
    genesis,
    root,
    dev,
    principal: { member, capabilities },
  } satisfies World;
});

const trustOf = (where: World) => projectTrust(where.genesis);

/** Two commits on a branch, so there is something to fast-forward and to drop. */
const history = Effect.fn("test.history")(function* (branch: string) {
  const repository = yield* Repository;
  const first = yield* repository.commit({
    branch,
    tree: EMPTY_TREE_OID,
    message: "first",
    author,
  });
  const second = yield* repository.commit({
    branch,
    tree: EMPTY_TREE_OID,
    message: "second",
    author,
  });
  return { first, second };
});

const judge = (where: World, update: { name: string; value: Oid | null }, rules: Rules = OPEN) =>
  Effect.flatMap(trustOf(where), (trust) =>
    evaluate({ update, principal: where.principal, genesis: where.genesis, trust, rules }),
  );

describe("Policy", () => {
  describe("capabilities", () => {
    it("allows a fast-forward from a member who may push", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: second });
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("refuses a push from a member who may not", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.read"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /source\.push/);
    });

    it("refuses a push that drops commits without source.force-push", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { first } = yield* history("refs/heads/topic");
          // Back to the first commit: the second stops being reachable.
          return yield* judge(where, { name: "refs/heads/topic", value: first });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /drops commits/);
    });

    it("allows it from a member who holds source.force-push", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "source.force-push"]);
          const { first } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: first });
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("refuses a deletion without source.delete", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: null });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /source\.delete/);
    });

    it("refuses an anonymous write outright", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          const trust = yield* trustOf(where);
          return yield* evaluate({
            update: { name: "refs/heads/topic", value: second },
            principal: { member: null, capabilities: [] },
            genesis: where.genesis,
            trust,
            rules: OPEN,
          });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /authentication required/);
    });
  });

  describe("namespaces", () => {
    it("refuses to move the genesis", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/meta/trust/genesis", value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /written once/);
    });

    it("refuses to delete a hub ref", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: null });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /append-only/);
    });

    it("refuses a hub update that does not contain what it replaces", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          // An unrelated commit: rewriting the history rather than adding to it.
          const { second } = yield* history("refs/heads/elsewhere");
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /must contain/);
    });

    it("allows a hub update that grows the history", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: head });
        }),
      );
      assert.equal(decision.ok, true);
    });
  });

  describe("protected branches", () => {
    const guarded: Rules = {
      ...OPEN,
      protected: ["refs/heads/main"],
      requirePullRequest: true,
      requiredApprovals: 1,
    };

    it("refuses a direct push to a protected branch", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/main");
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /approved pull request/);
    });

    it("refuses a protected branch a pull request has not had approved", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr"]);
          const { second } = yield* history("refs/heads/main");
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /0 approvals/);
    });

    it("allows it once the current revision is approved", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.dev,
          });
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("refuses when the approval was of a revision that has been replaced", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { first, second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: first,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: first,
            decision: "approve",
            key: where.dev,
          });
          // The proposal moves on; the approval stays true about `first`.
          yield* PullRequest.update({
            repo: where.genesis.repoId,
            pr,
            head: second,
            key: where.dev,
          });
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /0 approvals/);
    });

    it("does not let a merged pull request unlock the branch forever", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world([
            "source.push",
            "hub.create-pr",
            "hub.review",
            "hub.approve",
            "hub.merge",
          ]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "landed",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.dev,
          });
          yield* PullRequest.merged({
            repo: where.genesis.repoId,
            pr,
            head: second,
            mergeCommit: second,
            key: where.dev,
          });

          // A later commit descends from the merged revision, and every commit
          // ever made after a merge does. If descent were enough, one merged
          // pull request would authorize direct pushes to the branch forever.
          const later = yield* repository.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "sneaked in",
            author,
          });
          return yield* judge(where, { name: "refs/heads/main", value: later }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /approved pull request/);
    });

    it("refuses when a required check has not passed", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.dev,
          });
          return yield* judge(
            where,
            { name: "refs/heads/main", value: second },
            { ...guarded, requiredChecks: ["test"] },
          );
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /has not passed test/);
    });

    it("refuses while a review thread is unresolved", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world([
            "source.push",
            "hub.create-pr",
            "hub.review",
            "hub.approve",
            "hub.comment",
          ]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.dev,
          });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "what about this",
            key: where.dev,
          });
          return yield* judge(
            where,
            { name: "refs/heads/main", value: second },
            { ...guarded, requireResolvedThreads: true },
          );
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /unresolved/);
    });
  });

  describe("the rules themselves", () => {
    it("refuses to let a pusher rewrite the branch rules", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          // Without this, anybody who may push may also publish an `OPEN`
          // policy and then push wherever they like.
          return yield* judge(where, { name: "refs/meta/policy", value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /policy\.write/);
    });

    it("allows the holder of policy.write", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/meta/policy", value: second });
        }),
      );
      assert.equal(decision.ok, true);
    });
  });

  describe("applying", () => {
    it("applies under the value it judged, so a moved ref loses", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push"]);
          const { first, second } = yield* history("refs/heads/topic");
          const trust = yield* trustOf(where);

          // Judged against `second`… and then somebody else moves the branch
          // before the update lands.
          const judged = yield* evaluate({
            update: { name: "refs/heads/topic", value: second },
            principal: where.principal,
            genesis: where.genesis,
            trust,
            rules: OPEN,
          });
          yield* repository.setRef({ name: "refs/heads/topic", to: first });

          const results = yield* repository.receive(
            judged.ok ? [{ ...judged.allowed.update, expected: judged.allowed.expected }] : [],
          );
          return results;
        }),
      );

      assert.equal(outcome.at(0)?.ok, false, "the compare-and-swap must refuse a moved ref");
    });

    it("applies nothing when an atomic batch has a refusal in it", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          const trust = yield* trustOf(where);

          const result = yield* apply({
            updates: [
              { name: "refs/heads/ok", value: second },
              { name: "refs/meta/trust/genesis", value: second },
            ],
            principal: where.principal,
            genesis: where.genesis,
            trust,
            rules: OPEN,
            atomic: true,
          });
          return { result, landed: yield* repository.resolve("refs/heads/ok") };
        }),
      );

      assert.equal(outcome.result.applied.length, 0);
      assert.equal(outcome.result.refused.length, 1);
      assert.equal(outcome.landed, null, "an atomic batch applies all or nothing");
    });

    it("leaves a repository with no genesis alone", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { second } = yield* history("refs/heads/topic");
          const result = yield* apply({
            updates: [{ name: "refs/heads/other", value: second }],
            principal: { member: null, capabilities: [] },
            genesis: null,
            trust: null,
            rules: OPEN,
          });
          return { result, landed: yield* repository.resolve("refs/heads/other") };
        }),
      );

      assert.equal(outcome.result.refused.length, 0);
      assert.equal(outcome.landed !== null, true, "a plain git repository stays servable");
    });
  });
});
