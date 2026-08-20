import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import { project as projectPullRequest } from "../hub/Projection.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { identitiesInMemory, principalId, type ResolvedIdentity } from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { externalReviews } from "./Review.ts";

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

describe("external reviews", () => {
  it.effect("accepts a review only after its signer resolves through the named identity", () =>
    Effect.promise(async () => {
      const identity = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("identity-root@example.com");
          const reviewer = yield* generate("reviewer@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(reviewer.publicKey),
              capabilities: [],
              id: Log.newId(),
            }),
            [root],
          );
          const projection = yield* projectTrust(genesis);
          return {
            key: reviewer,
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
          const root = yield* generate("project-root@example.com");
          const author = yield* generate("author@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(author.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            [root],
          );

          const opened = yield* PullRequest.open({
            repo: genesis.repoId,
            title: "external review",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: author,
          });
          if (identity.resolved.head === null) {
            assert.fail("the identity fixture must have a trust-log head");
          }
          yield* PullRequest.review({
            repo: genesis.repoId,
            pr: opened.pr,
            head: EMPTY_TREE_OID,
            base: "refs/heads/main",
            principal: identity.resolved.principal,
            identityHead: identity.resolved.head,
            decision: "approve",
            key: identity.key,
          });

          const trust = yield* projectTrust(genesis);
          const pullRequest = yield* projectPullRequest(genesis, trust, opened.pr);
          const absent = yield* externalReviews(genesis, pullRequest, opened.pr);
          const resolved = yield* externalReviews(genesis, pullRequest, opened.pr).pipe(
            Effect.provide(identitiesInMemory([identity.resolved])),
          );
          const movedIdentity: ResolvedIdentity = {
            ...identity.resolved,
            head: EMPTY_TREE_OID,
            projection: { ...identity.resolved.projection, head: EMPTY_TREE_OID },
          };
          const moved = yield* externalReviews(genesis, pullRequest, opened.pr).pipe(
            Effect.provide(identitiesInMemory([movedIdentity])),
          );
          return { absent, moved, resolved, pullRequest };
        }),
      );

      assert.equal(outcome.pullRequest.reviews.length, 0, "it is not local repository authority");
      assert.equal(outcome.absent.reviews.length, 0, "an unavailable identity stays quarantined");
      assert.equal(outcome.resolved.reviews.length, 1);
      assert.equal(outcome.resolved.reviews[0]?.principal, identity.resolved.principal);
      assert.equal(outcome.moved.reviews.length, 0);
      assert.match(
        outcome.moved.rejected[0]?.reason ?? "",
        /pinned identity head/,
        "an external signature is bound to the identity view it names",
      );
    }),
  );
});
