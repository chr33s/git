import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { StorageFailure } from "../git/Error.ts";
import { RefStore, type Oid } from "../git/Store.ts";
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
import { ADOPTED_PREFIX, PENDING_PREFIX, adopt, pending, submit } from "./Inbox.ts";

const scenario = <A, E>(
  effect: Effect.Effect<A, E, Repository>,
  customize: (refs: RefStore["Service"]) => RefStore["Service"] = (refs) => refs,
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(
            Layer.effect(RefStore, Effect.map(RefStore, customize)).pipe(
              Layer.provideMerge(stores),
            ),
          ),
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

const world = Effect.fn("test.inboxWorld")(function* () {
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
  return { genesis, maintainer, outsider, proposal, offered };
});

describe("social inbox", () => {
  it.effect("preserves a proposal replaced while adoption was being prepared", () => {
    let replacement: Oid | null = null;
    return Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { genesis, maintainer, offered, proposal } = yield* world();
          const trust = yield* projectTrust(genesis);
          replacement = proposal;
          const result = yield* Effect.exit(
            adopt({ genesis, trust, proposal: offered.id, key: maintainer }),
          );
          assert.equal(result._tag, "Failure");
          assert.deepEqual(yield* Event.pullRequests(), []);
          const remaining = yield* pending();
          assert.equal(remaining.length, 1);
          assert.equal(remaining[0]?.commit, proposal);
        }),
        (refs) =>
          RefStore.of({
            ...refs,
            apply: (updates, options) =>
              Effect.gen(function* () {
                const adoption = updates.find((update) => update.name.startsWith(ADOPTED_PREFIX));
                if (adoption !== undefined && replacement !== null) {
                  const id = adoption.name.slice(ADOPTED_PREFIX.length);
                  yield* refs.apply([{ name: `${PENDING_PREFIX}${id}`, value: replacement }]);
                  replacement = null;
                }
                return yield* refs.apply(updates, options);
              }),
          }),
      ),
    );
  });

  it.effect("publishes only one pull request when two members adopt concurrently", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { genesis, maintainer, offered } = yield* world();
          const trust = yield* projectTrust(genesis);
          const results = yield* Effect.all(
            [
              Effect.exit(adopt({ genesis, trust, proposal: offered.id, key: maintainer })),
              Effect.exit(adopt({ genesis, trust, proposal: offered.id, key: maintainer })),
            ],
            { concurrency: "unbounded" },
          );
          assert.equal(results.filter((result) => result._tag === "Success").length, 1);
          assert.equal(
            (yield* Event.pullRequests()).length,
            1,
            "the losing adopter must not publish a PR",
          );
          assert.equal((yield* pending()).length, 0);
        }),
      ),
    ),
  );

  it.effect("leaves the proposal pending when recording adoption fails", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { genesis, maintainer, offered } = yield* world();
          const trust = yield* projectTrust(genesis);
          const result = yield* Effect.exit(
            adopt({ genesis, trust, proposal: offered.id, key: maintainer }),
          );
          assert.equal(result._tag, "Failure");
          assert.deepEqual(
            yield* Event.pullRequests(),
            [],
            "a failed adoption must not publish a PR",
          );
          assert.equal((yield* pending()).length, 1);
        }),
        (refs) =>
          RefStore.of({
            ...refs,
            apply: (updates, options) =>
              updates.some((update) => update.name.startsWith(ADOPTED_PREFIX))
                ? Effect.fail(
                    new StorageFailure({
                      operation: "test adoption",
                      path: ADOPTED_PREFIX,
                      cause: "disk unavailable",
                    }),
                  )
                : refs.apply(updates, options),
          }),
      ),
    ),
  );

  it.effect("keeps an anonymous proposal quarantined until a member adopts it", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, maintainer, outsider, proposal, offered } = yield* world();
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
