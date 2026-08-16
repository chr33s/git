import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid } from "../git/Store.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Event from "./Event.ts";
import * as PullRequest from "./PullRequest.ts";
import * as Redaction from "./Redaction.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore>) =>
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

interface World {
  readonly genesis: Genesis;
  readonly author: PrivateKey;
}

/** One member who may say things and take them back. */
const world = Effect.fn("test.world")(function* () {
  const root = yield* generate("root@example.com");
  const author = yield* generate("author@example.com");

  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(author.publicKey),
      capabilities: ["hub.create-pr", "hub.comment", "hub.redact"],
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, author } satisfies World;
});

/** SAFETY: forty lowercase hex characters by construction. */
const REVISION = "a".repeat(40) as Oid;

/** A pull request carrying one comment that should never have been written. */
const withASecret = Effect.fn("test.withASecret")(function* (where: World) {
  const repository = yield* Repository;

  const { pr } = yield* PullRequest.open({
    repo: where.genesis.repoId,
    title: "Add a thing",
    base: "refs/heads/main",
    head: REVISION,
    key: where.author,
  });
  const commit = yield* PullRequest.comment({
    repo: where.genesis.repoId,
    pr,
    body: "the deploy key is hunter2",
    key: where.author,
  });

  const { events } = yield* Event.entries(pr);
  const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

  // The blob the secret actually lives in, by the name its own tree gives it.
  const info = yield* repository.readCommit(commit);
  const path = yield* repository.findPath(info.tree, `${Event.RECORD}.json`);
  return { pr, target, blob: path?.oid ?? null };
});

describe("hub redaction", () => {
  it("removes a packed payload that reachability would otherwise protect", async () => {
    // The regression: deleting an object only removes the loose copy, and a
    // pack cannot give one up without being rewritten. Redaction reported
    // success while the secret stayed clonable out of the pack — and it stayed
    // *reachable*, because the tree naming it has to survive for the hash
    // chain to hold, so no ordinary collection would ever have taken it.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const objects = yield* ObjectStore;
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);

        // Pack first, so the blob is somewhere `deleteObject` cannot reach.
        yield* repository.gc({ repack: true });
        const packed = yield* objects.has(blob!);

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });
        const afterRedact = yield* objects.has(blob!);

        const exclude = yield* Redaction.excluded();
        yield* repository.gc({ repack: true, exclude });
        return {
          packed,
          afterRedact,
          afterGc: yield* objects.has(blob!),
          excluded: exclude.has(blob!),
        };
      }),
    );

    assert.equal(outcome.packed, true, "the fixture must actually pack the payload");
    assert.equal(outcome.excluded, true, "a valid tombstone must name its payload blob");
    // Honest about the intermediate state: the tombstone replicates at once,
    // the bytes go when the pack is next rewritten.
    assert.equal(outcome.afterRedact, true, "a packed object survives a plain delete");
    assert.equal(outcome.afterGc, false, "the repack must not carry the payload forward");
  });

  it("excludes nothing in a repository that has no genesis", async () => {
    const excluded = await scenario(Redaction.excluded());
    assert.equal(excluded.size, 0);
  });

  it("keeps a payload no tombstone covers", async () => {
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const objects = yield* ObjectStore;
        const where = yield* world();
        const { blob } = yield* withASecret(where);

        const exclude = yield* Redaction.excluded();
        yield* repository.gc({ repack: true, exclude });
        return { excluded: exclude.size, held: yield* objects.has(blob!) };
      }),
    );

    assert.equal(outcome.excluded, 0);
    assert.equal(outcome.held, true, "an ordinary comment must survive collection");
  });
});
