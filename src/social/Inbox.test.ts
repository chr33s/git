import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Event from "../hub/Event.ts";
import { project as projectPullRequest } from "../hub/Projection.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { adopt, pending, submit } from "./Inbox.ts";

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
  name: "Drive-by contributor",
  email: "stranger@example.com",
  at: new Date("2026-08-20T00:00:00Z"),
  offset: 0,
};

describe("social inbox", () => {
  it.effect("keeps an anonymous proposal quarantined until a member adopts it", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const root = yield* generate("root@example.com");
          const maintainer = yield* generate("maintainer@example.com");
          const outsider = yield* generate("outsider@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(maintainer.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            [root],
          );
          const proposal = yield* repository.commit({
            branch: "refs/heads/stranger-patch",
            tree: EMPTY_TREE_OID,
            message: "drive-by patch",
            author,
          });

          const offered = yield* submit({
            repo: genesis.repoId,
            head: proposal,
            base: "refs/heads/main",
            title: "Fix from a stranger",
            description: "Please consider this patch.",
          });
          const before = yield* pending();
          const beforePulls = yield* Event.pullRequests();
          const trust = yield* projectTrust(genesis);
          const denied = yield* Effect.exit(
            adopt({ genesis, trust, proposal: offered.id, key: outsider }),
          );
          const adopted = yield* adopt({
            genesis,
            trust,
            proposal: offered.id,
            key: maintainer,
          });
          const state = yield* projectPullRequest(genesis, trust, adopted.pr);

          return { before, beforePulls, denied, after: yield* pending(), state, proposal };
        }),
      );

      assert.equal(outcome.before.length, 1);
      assert.deepEqual(outcome.beforePulls, [], "quarantine contributes no hub event");
      assert.equal(outcome.denied._tag, "Failure");
      assert.equal(outcome.after.length, 0, "an adopted proposal leaves the pending queue");
      assert.equal(outcome.state.head, outcome.proposal);
    }),
  );
});
