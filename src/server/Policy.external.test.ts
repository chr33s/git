import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate } from "../crypto/SshSignature.ts";
import type { Fingerprint } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import type { VerifiedLog, VerifiedStatement } from "../social/Log.ts";
import { socialWebInMemory } from "../social/Projection.ts";
import type { ExternalReview } from "../social/Review.ts";
import { encode, vouch, type SocialStatement } from "../social/Statement.ts";
import * as Certificate from "../trust/Certificate.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import {
  identitiesInMemory,
  principalId,
  type PrincipalId,
  type ResolvedIdentity,
} from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { eligibleExternalApprovals, evaluate, OPEN, type ExternalReviewRule } from "./Policy.ts";

/** SAFETY: test values have the exact branded wire shapes. */
const repoId = (seed: string): RepoId => `SHA256:${seed.repeat(43).slice(0, 43)}` as RepoId;
const principal = (seed: string): PrincipalId => principalId(repoId(seed));
/** SAFETY: exactly forty lowercase hexadecimal characters. */
const oid = (seed: number): Oid => seed.toString(16).padStart(40, "0") as Oid;
/** SAFETY: the same 43-character digest spelling as a real fingerprint. */
const signer = (seed: string): Fingerprint =>
  `SHA256:${seed.repeat(43).slice(0, 43)}` as Fingerprint;

const rootA = principal("a");
const rootB = principal("b");
const reviewer = principal("r");
const reviewerKey = signer("k");

const statement = (payload: SocialStatement, commit: Oid): VerifiedStatement => ({
  commit,
  parents: [],
  payload,
  bytes: encode(payload),
  signatures: [],
  signer: signer(payload.author.slice(7, 8)),
});

const endorsement = (
  author: PrincipalId,
  index: number,
  subject: PrincipalId = reviewer,
): VerifiedLog => {
  const payload = vouch({
    author,
    subject,
    id: `018bcfe5-6800-7000-8000-${index.toString().padStart(12, "0")}`,
    socialHead: null,
    trustHead: null,
    at: new Date("2026-08-20T00:00:00Z"),
    scope: ["review"],
    depth: 0,
  });
  const commit = oid(index);
  return {
    principal: author,
    head: commit,
    statements: [statement(payload, commit)],
    rejected: [],
  };
};

const review: ExternalReview = {
  principal: reviewer,
  id: "review-1",
  author: reviewerKey,
  head: oid(100),
  base: "refs/heads/main",
  commit: oid(101),
  decision: "approve",
  body: "looks good",
  at: new Date("2026-08-20T00:00:00Z"),
  dismissed: false,
  stale: false,
};

const rule: ExternalReviewRule = {
  branch: "refs/heads/main",
  anchors: [rootA, rootB],
  scope: "review",
  maxDepth: 1,
  minPaths: 2,
  maxCount: 1,
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
  at: new Date("2026-08-20T00:00:00Z"),
  offset: 0,
};

describe("external-review policy", () => {
  it.effect("counts only reviewers meeting the rooted independent-path bar", () =>
    Effect.sync(() => {
      const both = eligibleExternalApprovals({
        rule,
        logs: [endorsement(rootA, 1), endorsement(rootB, 2)],
        reviews: [review],
      });
      const one = eligibleExternalApprovals({
        rule,
        logs: [endorsement(rootA, 1)],
        reviews: [review],
      });
      const sameKey = eligibleExternalApprovals({
        rule,
        logs: [endorsement(rootA, 1), endorsement(rootB, 2)],
        reviews: [review],
        internal: [review],
      });

      assert.equal(both.length, 1);
      assert.equal(one.length, 0);
      assert.equal(sameKey.length, 0, "one reviewer cannot count once locally and once externally");
    }),
  );

  it.effect("lets an opted-in protected branch count an identity-verified external approval", () =>
    Effect.promise(async () => {
      const identity = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("identity-root@example.com");
          const key = yield* generate("external-reviewer@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(key.publicKey),
              capabilities: [],
              id: Log.newId(),
            }),
            [root],
          );
          const projection = yield* projectTrust(genesis);
          return {
            key,
            resolved: {
              principal: principalId(genesis.repoId),
              projection,
              head: projection.head,
            } satisfies ResolvedIdentity,
          };
        }),
      );

      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const root = yield* generate("project-root@example.com");
          const contributor = yield* generate("contributor@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          const capabilities = ["source.push", "hub.create-pr"];
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(contributor.publicKey),
              capabilities,
              id: Log.newId(),
            }),
            [root],
          );
          const trust = yield* projectTrust(genesis);
          const contributorPrint = yield* fingerprint(contributor.publicKey);
          const member = trust.members.get(contributorPrint);
          if (member === undefined) assert.fail("the contributor grant must project as a member");
          const revision = yield* repository.commit({
            branch: "refs/heads/topic",
            tree: EMPTY_TREE_OID,
            message: "proposal",
            author,
          });
          const opened = yield* PullRequest.open({
            repo: genesis.repoId,
            title: "federated",
            base: "refs/heads/main",
            head: revision,
            key: contributor,
          });
          if (identity.resolved.head === null) {
            assert.fail("the identity fixture must have a trust-log head");
          }
          yield* PullRequest.review({
            repo: genesis.repoId,
            pr: opened.pr,
            head: revision,
            base: "refs/heads/main",
            principal: identity.resolved.principal,
            identityHead: identity.resolved.head,
            decision: "approve",
            key: identity.key,
          });

          const judge = (
            logs: ReadonlyArray<VerifiedLog>,
            externalBranch: ExternalReviewRule["branch"] = "refs/heads/main",
          ) =>
            evaluate({
              update: { name: "refs/heads/main", value: revision },
              principal: { member, capabilities },
              genesis,
              trust,
              rules: {
                ...OPEN,
                protected: ["refs/heads/main"],
                requirePullRequest: true,
                requiredApprovals: 1,
                externalReview: { ...rule, branch: externalBranch, anchors: [rootA, rootB] },
              },
            }).pipe(
              Effect.provide(
                Layer.merge(identitiesInMemory([identity.resolved]), socialWebInMemory(logs)),
              ),
            );

          return {
            accepted: yield* judge([
              endorsement(rootA, 1, identity.resolved.principal),
              endorsement(rootB, 2, identity.resolved.principal),
            ]),
            refused: yield* judge([endorsement(rootA, 1, identity.resolved.principal)]),
            wrongBranch: yield* judge(
              [
                endorsement(rootA, 1, identity.resolved.principal),
                endorsement(rootB, 2, identity.resolved.principal),
              ],
              "refs/heads/release",
            ),
          };
        }),
      );

      assert.equal(
        outcome.accepted.ok,
        true,
        outcome.accepted.ok ? "accepted" : outcome.accepted.reason,
      );
      assert.equal(outcome.refused.ok, false);
      assert.equal(
        outcome.wrongBranch.ok,
        false,
        "external trust policy is scoped to one exact ref",
      );
    }),
  );
});
