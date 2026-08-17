import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid } from "../git/Store.ts";
import { fingerprint } from "../crypto/SshSignature.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Record from "../trust/Record.ts";
import * as Event from "./Event.ts";
import * as PullRequest from "./PullRequest.ts";
import * as Protocol from "../server/Protocol.ts";
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
  readonly root: PrivateKey;
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
  return { genesis, root, author } satisfies World;
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

  it("lets a fetch plan the pull request whose payload was redacted", async () => {
    // The tree naming a tombstoned payload survives — the hash chain depends
    // on it — so a strict pack closure fails on the blob that is gone by
    // design, and one `hub redact` made every fetch of `refs/hub/*` fail
    // outright. `Repository.fetch` stays strict, which is what keeps an
    // unexplained absence reading as corruption; what a tombstone covers is
    // handed in, and `Protocol` is the caller that works the set out.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const where = yield* world();
        const { pr, target } = yield* withASecret(where);

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });

        // Collected, which is where a redaction's bytes actually go: the
        // tombstone replicates at once, and `gc` is what stops protecting the
        // payload it covers.
        const exclusion = yield* Redaction.excluded();
        yield* repository.gc({ repack: true, exclude: exclusion });

        const head = yield* repository.resolve(Event.refOf(pr));
        const wants = [head!];
        // Recomputed from scratch, which is the case that used to come back
        // empty: the id a tombstone named was read from the payload it
        // deleted, so a projection rebuilt afterwards lost its own target.
        const exclude = yield* Redaction.excluded();

        const strict = yield* repository.fetch({ wants, haves: [] }).pipe(
          Effect.as(true),
          Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)),
        );

        const full = yield* repository.fetch({ wants, haves: [], exclude });
        // And the deepening path, which reaches the same trees by another walk.
        const shallow = yield* repository.fetch({ wants, haves: [], depth: 1, exclude });
        return {
          excluded: exclude.size,
          strict,
          full: full.oids.length,
          shallow: shallow.oids.length,
        };
      }),
    );

    assert.equal(outcome.excluded, 1, "a rebuilt projection must still find the tombstone");
    assert.equal(outcome.strict, false, "the blob really is missing without the exclusion");
    assert.ok(outcome.full > 0, "a full fetch must still produce a plan");
    assert.ok(outcome.shallow > 0, "and so must a depth-limited one");
  });

  it("keeps a redacted payload the source history also holds", async () => {
    // Git dedupes by content, so a redaction's blob can be the very object a
    // branch reaches: post the comment, commit the same bytes as a file, then
    // have the comment redacted. Applied to the whole reachability walk, the
    // exclusion deleted an object `refs/heads/*` still names and left the
    // source history dangling. An exclusion says the *hub* must not keep it
    // alive; it does not say the branch may not.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const objects = yield* ObjectStore;
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);

        // The same bytes, on a branch, by their own name.
        const payload = yield* objects.read(blob!);
        const copy = yield* repository.writeBlob(payload.data);
        const tree = yield* repository.writeTree([
          { mode: "100644", name: "leaked.json", oid: copy },
        ]);
        yield* repository.setRef({
          name: "refs/heads/main",
          to: yield* repository.commitTree({
            tree,
            parents: [],
            message: "the same bytes\n",
            author: Record.identityAt(new Date(1_700_000_000_000)),
          }),
        });

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });

        const exclude = yield* Redaction.excluded();
        yield* repository.gc({ repack: true, exclude });
        return { same: copy === blob, held: yield* objects.has(blob!) };
      }),
    );

    assert.equal(outcome.same, true, "the fixture must actually collide");
    assert.equal(outcome.held, true, "a branch still reaches it, so it must survive");
  });

  it("keeps one the source history reaches only through a reflog", async () => {
    // The protective walk has to start from the same roots the collecting one
    // does. Built from ref names alone it lost `HEAD` and the reflog entries,
    // so a blob reachable only through a branch somebody had just deleted was
    // collected anyway — the same dangling tree, by a quieter route.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const objects = yield* ObjectStore;
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);

        const payload = yield* objects.read(blob!);
        const copy = yield* repository.writeBlob(payload.data);
        const tree = yield* repository.writeTree([
          { mode: "100644", name: "leaked.json", oid: copy },
        ]);
        const commit = yield* repository.commitTree({
          tree,
          parents: [],
          message: "the same bytes\n",
          author: Record.identityAt(new Date(1_700_000_000_000)),
        });
        // On a branch, and then off it: the reflog is what still leads back.
        yield* repository.setRef({ name: "refs/heads/gone", to: commit });
        yield* repository.receive([{ name: "refs/heads/gone", value: null, expected: commit }]);

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });

        const exclude = yield* Redaction.excluded();
        yield* repository.gc({ repack: true, exclude });
        return { same: copy === blob, held: yield* objects.has(blob!) };
      }),
    );

    assert.equal(outcome.same, true, "the fixture must actually collide");
    assert.equal(outcome.held, true, "the reflog still leads back to it");
  });

  it("does not drop a redacted oid a fetch's own trees still need", async () => {
    // The exclusion `gc` takes says "stop protecting this"; the one a fetch
    // takes says "this is not here, walk past it". They are not the same set,
    // because git dedupes by content — `gc` keeps a redacted payload a branch
    // also names — and handing the whole set to a fetch dropped an object the
    // pack genuinely needs, so the client rebuilt a tree pointing at nothing.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const objects = yield* ObjectStore;
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);

        const payload = yield* objects.read(blob!);
        const tree = yield* repository.writeTree([
          { mode: "100644", name: "leaked.json", oid: yield* repository.writeBlob(payload.data) },
        ]);
        const commit = yield* repository.commitTree({
          tree,
          parents: [],
          message: "the same bytes\n",
          author: Record.identityAt(new Date(1_700_000_000_000)),
        });
        yield* repository.setRef({ name: "refs/heads/main", to: commit });

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });
        yield* repository.gc({ repack: true, exclude: yield* Redaction.excluded() });

        // The deepening path, which computes the exclusion up front.
        const plan = yield* Protocol.planFor({
          wants: [commit],
          haves: [],
          clientShallow: [],
          depth: 1,
          since: undefined,
          notRefs: [],
        });
        return { held: yield* objects.has(blob!), carried: plan.oids.includes(blob!) };
      }),
    );

    assert.equal(outcome.held, true, "a branch still reaches it, so it survives");
    assert.equal(outcome.carried, true, "and the pack the branch needs must carry it");
  });

  it("says the same thing on a dry run as on the run it predicts", async () => {
    // A dry run exists to say what the real one would do. Skipping the
    // exclusion to save a trust fold made it say something else: a tombstoned
    // payload reported as reachable and "would remove 0", and the same call
    // without the flag removing it.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });

        const exclude = yield* Redaction.excluded();
        const predicted = yield* repository.gc({ dryRun: true, repack: true, exclude });
        const actual = yield* repository.gc({ repack: true, exclude });
        return {
          predicted: predicted.removed.includes(blob!),
          actual: actual.removed.includes(blob!),
        };
      }),
    );

    assert.equal(outcome.actual, true, "the real run removes it");
    assert.equal(outcome.predicted, outcome.actual, "and the dry run says so");
  });

  it("still explains an absence once the redactor's grant has lapsed", async () => {
    // A removal is irreversible and its authorization is not: expiry is judged
    // against the clock and a compromise reaches backwards, so a tombstone
    // valid on Monday can be invalid on Friday. Judged by the strict set, the
    // bytes stay gone while nothing accounts for them — and every fetch of
    // `refs/hub/*` fails from then on, permanently. What a fetch asks is
    // whether an absence is *explained*, and a tombstone on the record
    // explains it.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const objects = yield* ObjectStore;
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);

        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });
        yield* repository.gc({ repack: true, exclude: yield* Redaction.excluded() });

        // And now the redactor is revoked, so the tombstone stops counting.
        yield* Log.issue(
          Certificate.revoke({
            repo: where.genesis.repoId,
            subject: yield* fingerprint(where.author.publicKey),
            reason: "compromised",
            id: Log.newId(),
          }),
          [where.root],
        );

        const head = yield* repository.resolve(Event.refOf(pr));
        return {
          gone: !(yield* objects.has(blob!)),
          strict: (yield* Redaction.excluded()).size,
          explained: (yield* Redaction.covered()).has(blob!),
          planned: yield* Protocol.planFor({
            wants: [head!],
            haves: [],
            clientShallow: [],
            depth: undefined,
            since: undefined,
            notRefs: [],
          }).pipe(
            Effect.as(true),
            Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)),
          ),
        };
      }),
    );

    assert.equal(outcome.gone, true, "the bytes really are gone");
    assert.equal(outcome.strict, 0, "and the tombstone no longer counts");
    assert.equal(outcome.explained, true, "but it still explains the absence");
    assert.equal(outcome.planned, true, "so a fetch of the pull request still plans");
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
