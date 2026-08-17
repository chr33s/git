import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid, RefUpdate } from "../git/Store.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { type Member, project as projectTrust } from "../trust/Projection.ts";
import * as Auth from "./Auth.ts";
import * as Policy from "./Policy.ts";
import { evaluate, OPEN, type Principal, type Rules } from "./Policy.ts";

/** A projection standing in for one nothing in these checks reads. */
const EMPTY_PROJECTION = {
  // SAFETY: `gateWrite` reads only the requester's member and capabilities;
  // this value is never compared against a real repository identity.
  repoId: "" as never,
  head: null,
  members: new Map(),
  former: new Map(),
  revoked: new Map(),
  roots: [],
  threshold: 0,
  checkpoint: null,
  checkpoints: [],
  rejected: [],
};

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
  /** A second member, so an approval can come from somebody else. */
  readonly reviewer: PrivateKey;
  readonly principal: Principal;
}

const world = Effect.fn("test.world")(function* (capabilities: ReadonlyArray<string>) {
  const root = yield* generate("root@example.com");
  const dev = yield* generate("dev@example.com");
  const reviewer = yield* generate("reviewer@example.com");

  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  const grant = (key: PrivateKey, held: ReadonlyArray<string>) =>
    Effect.flatMap(
      Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(key.publicKey),
        capabilities: held,
        id: Log.newId(),
      }),
      (payload) => Log.issue(payload, [root]),
    );
  yield* grant(dev, capabilities);
  // A pull request's own author cannot approve it, so anything that needs an
  // approval needs a second member to give one.
  yield* grant(reviewer, ["hub.review", "hub.approve"]);

  const trust = yield* projectTrust(genesis);
  const print = yield* fingerprint(dev.publicKey);
  // SAFETY: `dev` was granted above, so the fold holds them.
  const member = trust.members.get(print) as Member;
  return {
    genesis,
    root,
    dev,
    reviewer,
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

/**
 * The receive-pack path, as a request from one principal.
 *
 * `gate` reads the requester, the genesis and the rules from the repository
 * itself, so the only thing a test supplies is who is asking. `null` is the
 * anonymous caller — a plain git repository's only kind.
 */
const gateAs = (where: World | null, updates: ReadonlyArray<RefUpdate>, atomic = true) =>
  Policy.gate(updates, atomic).pipe(
    Effect.provide(
      Auth.requester({
        principal: where?.principal.member ?? null,
        signer: null,
        capabilities: where?.principal.capabilities ?? [],
        projection: EMPTY_PROJECTION,
        envelope: null,
      }),
    ),
  );

/** The JSON verbs' pre-check, as a request from one principal. */
const gateWriteAs = (where: World | null, ref: string, rewrites = false) =>
  Policy.gateWrite(ref, rewrites).pipe(
    Effect.provide(
      Auth.requester({
        principal: where?.principal.member ?? null,
        signer: null,
        capabilities: where?.principal.capabilities ?? [],
        projection: EMPTY_PROJECTION,
        envelope: null,
      }),
    ),
  );

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

    it("refuses a hub update that grafts a second beginning onto the history", async () => {
      // Every hub event is written onto the ref's current head, so the history
      // has exactly one parentless commit: the `pr.opened` that started it.
      // A join over a *second* root is not adding to that history, it is
      // adding another one beside it — and the fold then has to choose
      // between two openings by something, which on a pull request with no
      // activity yet can only be the oid, which whoever wrote the commit
      // ground. That is how a `hub.create-pr` holder took the authorship, the
      // title and the base of a pull request they had no part in opening.
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
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // A parentless commit of the same shape, joined in beside the
          // opening. The join keeps everything the ref held, so the
          // append-only rule above is satisfied.
          const forged = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "pr.opened forged\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, forged],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /second history/);
    });

    it("allows a join that keeps both heads it already had", async () => {
      // The rule is about *new* roots, not about joins: two members appending
      // concurrently is the ordinary case, and the join that reconciles them
      // brings no beginning the ref did not already have.
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
          const opened = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // A second branch off the opening, and a join over both — divergence
          // and its resolution, with no root but the one the ref started at.
          const sibling = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [opened!],
            message: "concurrent\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, sibling],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );
      assert.equal(decision.ok, true);
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
            key: where.reviewer,
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
            key: where.reviewer,
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
            key: where.reviewer,
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

    it("refuses a merge commit that merely names the approved revision", async () => {
      // A merge's tree is unconstrained, so "has the approved head as a
      // parent" says nothing at all about what is being landed.
      const decision = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { first, second } = yield* history("refs/heads/main");
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
            key: where.reviewer,
          });

          // Any content at all, with the approved head hung off it as a parent.
          const blob = yield* repository.writeBlob(new TextEncoder().encode("anything\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "x.txt", oid: blob }]);
          const wrapper = yield* repository.commitTree({
            tree,
            parents: [first, second],
            message: "merge\n",
            author,
          });
          return yield* judge(where, { name: "refs/heads/main", value: wrapper }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
    });

    it("is not blocked by an unapproved duplicate of the same proposal", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");

          // Two pull requests for the same revision; only one is approved.
          const approved = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "approved",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "duplicate",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr: approved.pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });

          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, true, "an unapproved duplicate must not block the approved one");
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
            key: where.reviewer,
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
            key: where.reviewer,
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

  describe("a repository with no identity", () => {
    it("refuses a write unless the host says otherwise", async () => {
      // §14 is unconditional that anonymous does not get `source.push`, and a
      // repository with no genesis has no membership to grant it. The previous
      // model required a server secret, so deploying this over an existing
      // installation would otherwise have made every repository accept
      // unauthenticated pushes, force-pushes and deletes included.
      const outcome = await scenario(
        Effect.gen(function* () {
          const { second } = yield* history("refs/heads/topic");
          const closed = yield* gateAs(null, [{ name: "refs/heads/other", value: second }]);
          const open = yield* gateAs(null, [{ name: "refs/heads/other", value: second }]).pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
          return { closed, open };
        }),
      );

      assert.equal(outcome.closed.updates.length, 0);
      assert.match(outcome.closed.refused.at(0)?.reason ?? "", /hub init/);
      assert.equal(outcome.open.updates.length, 1, "--open is what serves them anyway");
    });

    it("refuses the JSON verbs too, not only receive-pack", async () => {
      // `gateWrite` is the pre-check for the verbs that compute a ref's new
      // value while doing the work. Allowing there while `gate` refused meant
      // `git push` was blocked and unauthenticated `POST /commit`, `/branch`,
      // `/tags`, a merge or rebase with `into`, a pull and commit-pack all
      // still wrote refs — every door but the one that was locked.
      const outcome = await scenario(
        Effect.gen(function* () {
          const closed = yield* gateWriteAs(null, "refs/heads/topic");
          const open = yield* gateWriteAs(null, "refs/heads/topic").pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
          return { closed, open };
        }),
      );

      assert.match(outcome.closed ?? "", /hub init/);
      assert.equal(outcome.open, null, "--open is what serves them anyway");
    });

    it("never lets a push establish an identity, open or not", async () => {
      // Whoever got there first would own the repository, and its actual owner
      // would be locked out of it by a stranger.
      const refused = await scenario(
        Effect.gen(function* () {
          const { second } = yield* history("refs/heads/topic");
          return yield* gateAs(null, [{ name: "refs/meta/trust/genesis", value: second }]).pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
        }),
      );

      assert.equal(refused.updates.length, 0);
      assert.match(refused.refused.at(0)?.reason ?? "", /not by a push/);
    });

    it("still honours the branch protection it publishes", async () => {
      // The genesis-less branch returned before `input.rules` was ever
      // consulted, so a published `refs/meta/policy` was inert on exactly the
      // repositories with no other protection at all: `--open` served the
      // delete and the force-push the file refused. The approval half of
      // protection genuinely cannot apply here — there is no membership for a
      // review to come from — but "may not be deleted" and "may not be
      // force-pushed" ask nothing of trust.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { first, second } = yield* history("refs/heads/main");

          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/heads/main"] }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          // Built on the tip without moving it, so the fast-forward below is
          // a real one rather than a write of the value already there.
          const third = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [second],
            message: "third\n",
            author,
          });

          const open = Policy.anonymousWrites(true);
          return {
            deleted: yield* gateAs(null, [{ name: "refs/heads/main", value: null }]).pipe(
              Effect.provide(open),
            ),
            forced: yield* gateAs(null, [{ name: "refs/heads/main", value: first }]).pipe(
              Effect.provide(open),
            ),
            ahead: yield* gateAs(null, [{ name: "refs/heads/main", value: third }]).pipe(
              Effect.provide(open),
            ),
          };
        }),
      );

      assert.equal(outcome.deleted.updates.length, 0);
      assert.match(outcome.deleted.refused.at(0)?.reason ?? "", /may not be deleted/);
      assert.equal(outcome.forced.updates.length, 0);
      assert.match(outcome.forced.refused.at(0)?.reason ?? "", /may not be force-pushed/);
      assert.equal(outcome.ahead.updates.length, 1, "an ordinary push still lands");
    });
  });

  describe("how stale a membership view may be", () => {
    /** Publish rules with a checkpoint age bound. */
    const withMaxAge = Effect.fn("test.withMaxAge")(function* (seconds: number) {
      const repository = yield* Repository;
      const blob = yield* repository.writeBlob(
        Policy.encodeRules({ ...OPEN, maxTrustAgeSeconds: seconds }),
      );
      const tree = yield* repository.writeTree([
        { mode: "100644", name: "policy.json", oid: blob },
      ]);
      const commit = yield* repository.commitTree({
        tree,
        parents: [],
        message: "policy\n",
        author,
      });
      yield* repository.setRef({ name: Policy.RULES_REF, to: commit });
    });

    it("refuses a write when the repository has never checkpointed", async () => {
      // A hash-linked log makes withholding visible but not impossible: a
      // replica can serve a consistent view that simply stops short of a
      // revocation. A checkpoint is the signed statement that bounds it, and
      // this rule is what makes the bound mean anything.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          yield* withMaxAge(3600);
          return yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]);
        }),
      );

      assert.equal(outcome.updates.length, 0);
      assert.match(outcome.refused.at(0)?.reason ?? "", /checkpoint/);
    });

    it("allows it once a recent checkpoint exists", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          yield* Log.issue(
            Certificate.checkpoint({
              repo: where.genesis.repoId,
              frontier: [],
              id: Log.newId(),
            }),
            [where.root],
          );
          yield* withMaxAge(3600);
          return yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]);
        }),
      );

      assert.deepEqual(outcome.refused, []);
      assert.equal(outcome.updates.length, 1);
    });

    it("bounds the JSON verbs too, not only receive-pack", async () => {
      // Applied only in `gate`, the bound covered receive-pack and left
      // `commit`, `branch`, `tagCreate`, `merge`, `rebase`, `cherry-pick`,
      // `pull` and commit-pack accepting a membership view of any age — which
      // is most of the ways a ref moves.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          yield* withMaxAge(3600);
          return yield* gateWriteAs(where, "refs/heads/topic");
        }),
      );
      assert.match(refusal ?? "", /checkpoint/);
    });

    it("leaves a repository that set no bound alone", async () => {
      // The default, and it has to stay the default: a bound nobody configured
      // would refuse every push on every repository that has never checkpointed.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]);
        }),
      );
      assert.deepEqual(outcome.refused, []);
    });
  });

  describe("writing a ref whose value is not known yet", () => {
    /** The JSON verbs that compute the new value while doing the work. */
    it("lets a member write an ordinary branch", async () => {
      // The regression this guards: gating without providing the requester
      // refused *every* caller, admins included.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          return yield* gateWriteAs(where, "refs/heads/topic");
        }),
      );
      assert.equal(refusal, null);
    });

    it("refuses a protected branch, which only a pull request may move", async () => {
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({
              ...OPEN,
              protected: ["refs/heads/main"],
              requirePullRequest: true,
            }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/heads/main");
        }),
      );
      // Refused by name, before the write: these verbs name a branch and not
      // the revision the protected-branch rules are about, so there is nothing
      // here to evaluate those rules against. A protected branch moves through
      // receive-pack, where the revision arrives with the request.
      assert.match(refusal ?? "", /pushing an approved revision/);
    });

    it("refuses to establish an identity over the API", async () => {
      // A repository with no genesis must not acquire one this way: whoever
      // asked first would own somebody else's repository.
      const refusal = await scenario(gateWriteAs(null, "refs/meta/trust/genesis"));
      assert.match(refusal ?? "", /hub init/);
    });

    it("refuses the tag namespace a fetch writes, when it is protected", async () => {
      // The `fetch` and `pull` verbs write two namespaces: tracking refs, and
      // tags — `Sync` rewrites `refs/heads/*` into `refs/remotes/<name>/*` and
      // leaves tag names exactly as the remote spelled them. Gating tracking
      // alone let every `refs/tags/*` write past the policy boundary.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/tags/*"], requirePullRequest: true }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/tags/*");
        }),
      );
      assert.match(refusal ?? "", /protected/);
    });

    it("refuses a namespace write against a rule narrower than the namespace", async () => {
      // `refs/tags/*` is what a fetch asks about: it writes every tag the
      // remote has and does not know their names until the negotiation is
      // over. Compared as a literal name it matched a rule protecting
      // `refs/tags/*` and missed one protecting `refs/tags/v*` — so the
      // narrower rule was the one with the hole in it.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/tags/v*"], requirePullRequest: true }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/tags/*");
        }),
      );
      assert.match(refusal ?? "", /protected/);
    });

    it("lets a namespace write through when no rule reaches into it", async () => {
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/heads/main"] }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/tags/*");
        }),
      );
      assert.equal(refusal, null);
    });

    it("charges a force-push for the writes that behave like one", async () => {
      // Two JSON verbs move a ref to a value that need not contain what it
      // currently holds: `tagCreate --force`, which drops the create-only
      // compare-and-swap, and a merge whose `into` is a third branch, whose
      // result is a commit over `ours` and `theirs` and nothing else. Both
      // were gated as ordinary writes, so `source.push` alone did what
      // receive-pack charges `source.force-push` for.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          return {
            plain: yield* gateWriteAs(where, "refs/tags/v1"),
            forced: yield* gateWriteAs(where, "refs/tags/v1", true),
            branch: yield* gateWriteAs(where, "refs/heads/main"),
            rewrite: yield* gateWriteAs(where, "refs/heads/main", true),
          };
        }),
      );

      assert.equal(outcome.plain, null, "an ordinary tag needs only source.push");
      assert.match(outcome.forced ?? "", /source\.force-push/);
      assert.equal(outcome.branch, null);
      assert.match(outcome.rewrite ?? "", /source\.force-push/);
    });

    it("refuses a hub ref, which is appended to and never written", async () => {
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          return yield* gateWriteAs(where, "refs/hub/pr/anything");
        }),
      );
      assert.match(refusal ?? "", /appended to/);
    });
  });

  describe("applying", () => {
    it("compares against the ref's own value, not what a symref resolves to", async () => {
      // `RefStore.apply` checks `expected` against the ref's stored value, and
      // a symbolic ref stores no oid. Taking the compare-and-swap from the
      // *resolved* value named a commit the store would never agree with, so
      // every gated write to a symbolic ref failed as a conflict against a
      // value nobody had written there.
      //
      // On disk rather than in memory, because that is where a symbolic ref
      // under `refs/` can exist at all: git writes `refs/remotes/origin/HEAD`
      // as one in every repository it clones, and nothing in this codebase has
      // a way to create one.
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "policy-symref-"));
      try {
        const onDisk = GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(nodeStores(root)),
        );

        const outcome = await Effect.runPromise(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const where = yield* world(["source.push"]);
            const { second } = yield* history("refs/heads/topic");

            yield* Effect.promise(async () => {
              await fs.mkdir(path.join(root, "refs/remotes/origin"), { recursive: true });
              await fs.writeFile(
                path.join(root, "refs/remotes/origin/HEAD"),
                "ref: refs/heads/topic\n",
              );
            });

            const gated = yield* gateAs(where, [
              { name: "refs/remotes/origin/HEAD", value: second },
            ]);
            const results = yield* repository.receive(gated.updates);
            return { gated, ok: results.at(0)?.ok, reason: results.at(0)?.reason };
          }).pipe(Effect.provide(onDisk)),
        );

        assert.deepEqual(outcome.gated.refused, []);
        assert.equal(
          outcome.ok,
          true,
          `the compare-and-swap must match what the store checks: ${outcome.reason ?? ""}`,
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });

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

          const gated = yield* gateAs(where, [
            { name: "refs/heads/ok", value: second },
            { name: "refs/meta/trust/genesis", value: second },
          ]);
          yield* repository.receive(gated.updates);
          return { gated, landed: yield* repository.resolve("refs/heads/ok") };
        }),
      );

      assert.equal(outcome.gated.updates.length, 0);
      assert.equal(outcome.gated.refused.length, 1);
      assert.equal(outcome.landed, null, "an atomic batch applies all or nothing");
    });

    it("leaves a repository with no genesis alone", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { second } = yield* history("refs/heads/topic");
          const gated = yield* gateAs(null, [{ name: "refs/heads/other", value: second }]).pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
          yield* repository.receive(gated.updates);
          return { gated, landed: yield* repository.resolve("refs/heads/other") };
        }),
      );

      assert.equal(outcome.gated.refused.length, 0);
      assert.equal(outcome.landed !== null, true, "a plain git repository stays servable");
    });
  });
});
