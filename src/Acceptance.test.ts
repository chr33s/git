/**
 * The scenario §32 of `docs/hub.md` says the thing has to be able to do.
 *
 * Every other suite tests one seam. This one walks the whole thing once, in
 * the order a repository actually lives it: three people establish an
 * identity, a fourth is granted membership, opens a pull request and has it
 * approved and checked, a protected branch moves because of that approval and
 * not otherwise, and then the repository is picked up and put down on another
 * host, where all of it is read back out of nothing but git objects and SSH
 * signatures.
 *
 * The point is coverage of the *joins*: each step here works because the step
 * before it left something on a ref, and a suite of unit tests can pass while
 * the sequence does not.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate } from "./crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "./git/Format.ts";
import { stores } from "./git/Node.ts";
import * as GitRepository from "./git/Repository.ts";
import { Repository } from "./git/Repository.ts";
import type { Oid } from "./git/Store.ts";
import { project } from "./hub/Projection.ts";
import * as PullRequest from "./hub/PullRequest.ts";
import * as Auth from "./server/Auth.ts";
import * as Policy from "./server/Policy.ts";
import * as Certificate from "./trust/Certificate.ts";
import { create, readGenesis, signGenesis, writeGenesis } from "./trust/Genesis.ts";
import * as Log from "./trust/Log.ts";
import { project as projectTrust } from "./trust/Projection.ts";

const author: Signature = {
  name: "Dave",
  email: "dave@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const inside = <A, E>(directory: string, effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores(directory)),
        ),
      ),
    ),
  );

/** The rules §26 evaluates: `main` moves only through an approved, checked PR. */
const guarded: Policy.Rules = {
  ...Policy.OPEN,
  protected: ["refs/heads/main"],
  requirePullRequest: true,
  requiredApprovals: 1,
  requiredChecks: ["test"],
};

describe("the acceptance scenario", () => {
  it("goes from three root keys to a moved repository with its history intact", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "acceptance-"));
    const origin = path.join(root, "origin");
    try {
      // Alice, Bob and Carol each own an independent key; Dave and CI bring
      // their own later.
      const alice = await Effect.runPromise(generate("alice@example.com"));
      const bob = await Effect.runPromise(generate("bob@example.com"));
      const carol = await Effect.runPromise(generate("carol@example.com"));
      const dave = await Effect.runPromise(generate("dave@example.com"));
      const ci = await Effect.runPromise(generate("ci@example.com"));

      // -- identity, 2 of 3 ---------------------------------------------------
      const established = await inside(
        origin,
        Effect.gen(function* () {
          const genesis = yield* create(
            [alice, bob, carol].map((key) => formatPublicKey(key.publicKey)),
            2,
          );
          // Two of the three sign it, which is the threshold and not a
          // majority by accident: one signature would be a repository any one
          // root could have created alone.
          yield* writeGenesis(genesis, [
            yield* signGenesis(genesis, alice),
            yield* signGenesis(genesis, bob),
          ]);
          return genesis;
        }),
      );

      // -- membership ---------------------------------------------------------
      const year = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);
      await inside(
        origin,
        Effect.gen(function* () {
          const grant = (key: typeof dave, capabilities: ReadonlyArray<string>) =>
            Effect.flatMap(
              Certificate.grant({
                repo: established.repoId,
                publicKey: formatPublicKey(key.publicKey),
                capabilities,
                expiresAt: year,
                id: Log.newId(),
              }),
              // Two roots, because the threshold is two: a repository whose
              // members one root could invite alone has a quorum in name only.
              (payload) => Log.issue(payload, [alice, carol]),
            );

          // Dave's certificate, exactly as §32 spells it.
          yield* grant(dave, ["source.push", "hub.create-pr", "hub.comment", "hub.approve"]);
          // Bob reviews, and CI holds one scoped check capability and nothing
          // else — `hub.check:test` cannot sign a `lint` result.
          yield* grant(bob, ["hub.review", "hub.approve"]);
          yield* grant(ci, [Certificate.checkCapability("test")]);
        }),
      );

      // -- Dave authenticates with a credential he minted himself -------------
      const credential = await Effect.runPromise(
        Auth.mintDelegation({
          key: dave,
          repo: established.repoId,
          capabilities: ["source.push", "hub.create-pr"],
          ttlSeconds: 3600,
        }),
      );
      const delegated = await Effect.runPromise(
        Auth.openDelegation(credential, established.repoId, new Date(), null),
      );
      assert.notEqual(delegated, null, "the credential verifies against its own signature");
      assert.equal(
        delegated?.signer,
        await Effect.runPromise(fingerprint(dave.publicKey)),
        "and names the member who signed it",
      );
      assert.deepEqual(
        [...(delegated?.delegation.capabilities ?? [])].sort(),
        ["hub.create-pr", "source.push"],
        "scoped to what he asked for, and intersected with what he holds when it is used",
      );

      // -- a pull request, reviewed and checked -------------------------------
      const proposed = await inside(
        origin,
        Effect.gen(function* () {
          const repository = yield* Repository;
          // `main` exists, and the revision Dave wants on it does not yet.
          yield* repository.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "first\n",
            author,
          });
          const head = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [(yield* repository.resolve("refs/heads/main"))!],
            message: "the thing\n",
            author,
          });

          const { pr } = yield* PullRequest.open({
            repo: established.repoId,
            title: "Add a thing",
            description: "It does the thing.",
            base: "refs/heads/main",
            head,
            key: dave,
          });

          // Bob approves the *exact* revision, which is what an approval is.
          yield* PullRequest.review({
            repo: established.repoId,
            pr,
            head,
            decision: "approve",
            key: bob,
          });

          // CI signs a result for the one check it may sign for.
          yield* PullRequest.checkCompleted({
            repo: established.repoId,
            pr,
            head,
            name: "test",
            provider: "ci",
            status: "success",
            key: ci,
          });

          return { pr, head };
        }),
      );

      // -- the branch moves because of that, and not otherwise ----------------
      const judged = await inside(
        origin,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const decide = (update: { name: string; value: Oid }, who: typeof dave) =>
            Effect.gen(function* () {
              const trust = yield* projectTrust(established);
              const signer = yield* fingerprint(who.publicKey);
              const member = trust.members.get(signer);
              return yield* Policy.evaluate({
                update,
                principal: {
                  member: member ?? null,
                  capabilities: member?.capabilities ?? [],
                },
                genesis: established,
                trust,
                rules: guarded,
              });
            });

          const stranger = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [(yield* repository.resolve("refs/heads/main"))!],
            message: "unreviewed\n",
            author,
          });

          return {
            approved: yield* decide({ name: "refs/heads/main", value: proposed.head }, dave),
            direct: yield* decide({ name: "refs/heads/main", value: stranger }, dave),
            at: yield* repository.readRef("refs/heads/main"),
          };
        }),
      );

      assert.equal(
        judged.approved.ok,
        true,
        judged.approved.ok === false ? judged.approved.reason : "",
      );
      assert.equal(
        judged.approved.ok === true ? judged.approved.allowed.expected : null,
        judged.at,
        "and the decision carries the swap it was made against",
      );
      assert.equal(judged.direct.ok, false, "a revision nobody reviewed does not move it");

      // -- the repository moves host ------------------------------------------
      const elsewhere = path.join(root, "elsewhere");
      await fs.cp(origin, elsewhere, { recursive: true });

      const rebuilt = await inside(
        elsewhere,
        Effect.gen(function* () {
          const stored = yield* readGenesis();
          const trust = yield* projectTrust(stored!.genesis);
          const state = yield* project(stored!.genesis, trust, proposed.pr);
          return {
            repoId: stored!.genesis.repoId,
            members: trust.members.size,
            title: state.title,
            base: state.base,
            head: state.head,
            approvals: state.reviews.filter((review) => review.decision === "approve").length,
            checks: state.checks.map((check) => `${check.name}=${check.status}`),
            author: state.author,
          };
        }),
      );

      assert.equal(rebuilt.repoId, established.repoId, "the RepoID does not change with the host");
      assert.equal(rebuilt.members, 3, "Dave, Bob and CI are read back out of the log");
      assert.equal(rebuilt.title, "Add a thing");
      assert.equal(rebuilt.base, "refs/heads/main");
      assert.equal(rebuilt.head, proposed.head, "on the exact revision proposed");
      assert.equal(rebuilt.approvals, 1);
      assert.deepEqual(rebuilt.checks, ["test=success"]);
      assert.equal(
        rebuilt.author,
        await Effect.runPromise(fingerprint(dave.publicKey)),
        "and it still knows whose pull request it is",
      );

      // Nothing but git objects and signatures got it there: no database, no
      // token registry, nothing the copy above could have left behind.
      const carried = await fs.readdir(elsewhere);
      assert.ok(carried.includes("objects"), `objects came along: ${carried.join(", ")}`);
      assert.ok(carried.includes("refs"));
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
