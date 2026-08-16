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
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { reconcile } from "./Replication.ts";

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
  it("joins two divergent histories of one pull request", async () => {
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
  });

  it("fast-forwards rather than joining when one side is behind", async () => {
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
  });

  it("reports a divergent branch and refuses to invent a merge", async () => {
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
  });

  it("takes a ref this replica has never seen", async () => {
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
  });

  it("joins the trust log when two replicas both granted membership", async () => {
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
  });
});
