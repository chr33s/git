import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import {
  formatPublicKey,
  generate,
  NAMESPACE,
  type PrivateKey,
  sign,
} from "../crypto/SshSignature.ts";
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

  it("keeps honouring a tombstone after the redactor is revoked", async () => {
    // A removal is irreversible, so the verdict behind it has to be too.
    // Judged like every other event, a tombstone valid on Monday was invalid
    // on Friday — expiry is read off the clock and a compromise reaches
    // backwards — and then `gc` went back to protecting and serving a payload
    // the operator had been told was gone, the fold stopped reading the target
    // as absent, and the host that had already deleted the blob folded a pull
    // request no replica agreed with.
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

        // And now the redactor is revoked as compromised, which reaches
        // backwards everywhere else.
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
    assert.equal(outcome.strict, 1, "and the tombstone still counts, so gc keeps excluding it");
    assert.equal(outcome.explained, true, "the absence is still explained");
    assert.equal(outcome.planned, true, "so a fetch of the pull request still plans");
  });

  it("keeps honouring a tombstone after the redactor's grant has expired", async () => {
    // The other half of the same rule, and the one that needs no attacker at
    // all: a grant with an expiry lapses on its own, and with it — before this
    // — went the tombstone, the exclusion `gc` takes, and the agreement
    // between a host that had deleted the payload and a replica that had not.
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

        // Re-granted with an expiry already behind us, which is what a lapsed
        // membership looks like without waiting for one.
        yield* Log.issue(
          yield* Certificate.grant({
            repo: where.genesis.repoId,
            publicKey: formatPublicKey(where.author.publicKey),
            capabilities: ["hub.create-pr", "hub.comment", "hub.redact"],
            expiresAt: new Date(1_700_000_000_000),
            id: Log.newId(),
          }),
          [where.root],
        );

        return {
          gone: !(yield* objects.has(blob!)),
          strict: (yield* Redaction.excluded()).size,
        };
      }),
    );

    assert.equal(outcome.gone, true, "the bytes really are gone");
    assert.equal(outcome.strict, 1, "and the tombstone still counts");
  });

  it("explains an absence a replica cannot yet authorize", async () => {
    // The permissive set is not redundant once the strict one stops moving.
    // Replication is per-ref and arrives in no fixed order, so a replica can
    // hold `refs/hub/*` — trees naming payloads its source already deleted —
    // before it holds the trust log entry that granted `hub.redact`. Asked the
    // strict question there, nothing accounts for the missing bytes and every
    // fetch of that pull request fails until the log catches up. What a fetch
    // asks is whether an absence is *explained*, and a tombstone on the record
    // explains it whoever signed it.
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

        // The log this replica has stops short of the grant, which is what
        // being behind looks like from here.
        yield* repository.deleteRef(Log.LOG_REF);

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
    assert.equal(outcome.strict, 0, "and this replica cannot authorize the tombstone");
    assert.equal(outcome.explained, true, "but it still explains the absence");
    assert.equal(outcome.planned, true, "so a fetch of the pull request still plans");
  });

  it("works the covered set out once per repository state", async () => {
    // This is the set a *deepening* fetch takes, and a deepening fetch is a
    // request an anonymous reader makes. Unmemoised it walked every pull
    // request's event DAG twice per request — once to find the tombstones and
    // once to read them — which is the whole hub history, driveable in a loop
    // by anybody who can reach the repository. Sound to memoise because it
    // asks only what a tombstone *names*: no trust state and no clock enter
    // it, so a moved ref is the only thing that can change the answer.
    const outcome = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const first = yield* withASecret(where);
        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr: first.pr,
          target: first.target,
          reason: "sensitive-content",
          key: where.author,
        });

        const once = yield* Redaction.covered();
        const again = yield* Redaction.covered();

        // A second pull request moves a ref, which is what the memo is
        // validated against.
        const second = yield* withASecret(where);
        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr: second.pr,
          target: second.target,
          reason: "sensitive-content",
          key: where.author,
        });
        const after = yield* Redaction.covered();
        return { reused: once === again, sizes: [once.size, after.size] as const };
      }),
    );

    assert.equal(outcome.reused, true, "the same state must not be walked twice");
    assert.deepEqual(outcome.sizes, [1, 2], "and a moved ref must not be answered from the memo");
  });

  it("steps over a commit whose tree never arrived", async () => {
    // `fetchRepository` applies refs with no connectivity check, so a replica
    // can hold a hub commit whose tree object is simply absent. This walk is
    // what every protected-branch push, collection and deepening fetch runs
    // first, and it guarded the commit read but not the tree read — so one
    // missing object took all of them out at once. Read as "not part of this
    // history", it is one commit the walk steps over.
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

        // The tombstone's own commit loses its tree, so the walk steps over
        // it. Its *target* loses one too, so the two sets that read a target's
        // tree — the one `gc` takes and the one a fetch takes — each meet an
        // absence where they were looking for the payload's name.
        const head = yield* repository.resolve(Event.refOf(pr));
        const tombstone = yield* repository.readCommit(head!);
        const { events } = yield* Event.entries(pr);
        const named = events.find((entry) => entry.payload?.type === "event.redacted");
        const targeted = Event.unqualify(
          named?.payload?.type === "event.redacted" ? named.payload.targetCommit : "",
        );
        const aimed = yield* repository.readCommit(targeted!);

        // The *target's* tree, not the tombstone's. A tombstone the walk can
        // no longer read is a tombstone that never enters either set — so the
        // case that reaches the tree read is the one where the statement
        // survives and the thing it names does not, which is exactly what a
        // ref applied ahead of its objects leaves behind.
        //
        // Deleted before either set is asked: both memos are keyed on ref
        // state, which an absent object does not change, so a warm answer
        // would be the old one rather than the one under test.
        yield* objects.delete(aimed.tree);

        return {
          aimed: aimed.tree !== tombstone.tree,
          strict: (yield* Redaction.excluded()).size,
          explained: (yield* Redaction.covered()).has(blob!),
        };
      }),
    );

    assert.equal(outcome.aimed, true, "the fixture must remove two distinct trees");
    assert.equal(outcome.strict, 0, "there is nothing left for the tombstone to name");
    assert.equal(outcome.explained, false, "but nothing failed: gc and fetch both still answer");
  });

  it("does not honour a tombstone that records no trust head", async () => {
    // A permanent verdict has to say what it was judged against. `trustHead`
    // is nullable and means "had seen everything", which every other reading
    // treats as conservative — but here it makes the answer move: a revocation
    // of the signer refuses such an event outright, and a narrowing re-grant
    // shrinks what it is judged to have held. Either turns a tombstone the
    // repository already acted on into one it no longer honours, which is the
    // divergence a permanent verdict exists to remove.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const where = yield* world();
        const { pr, target } = yield* withASecret(where);

        // A second secret, for the headless tombstone to name — so honouring
        // it would show up as one more blob and not as the same one twice.
        const second = yield* PullRequest.comment({
          repo: where.genesis.repoId,
          pr,
          body: "and the other one is hunter3",
          key: where.author,
        });
        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });

        const { events } = yield* Event.entries(pr);
        const written = events.find((entry) => entry.payload?.type === "event.redacted")?.payload;
        const other = events.find((entry) => entry.commit === second)?.payload?.id ?? "";
        if (written?.type !== "event.redacted") return { honoured: -1, headless: -1 };

        const honoured = (yield* Redaction.excluded()).size;

        // The same statement, aimed at the second secret and recording no
        // trust head — a payload this schema accepts and a client may write.
        const headless = Event.encode({
          ...written,
          id: Log.newId(),
          target: other,
          targetCommit: Event.qualify(second),
          trustHead: null,
        });
        const head = yield* repository.resolve(Event.refOf(pr));
        yield* repository.setRef({
          name: Event.refOf(pr),
          to: yield* Record.write({
            name: Event.RECORD,
            payload: headless,
            signatures: [yield* sign(where.author, headless, NAMESPACE)],
            parents: [head!],
            message: "event.redacted headless\n",
          }),
        });

        return { honoured, headless: (yield* Redaction.excluded()).size };
      }),
    );

    assert.equal(outcome.honoured, 1, "the tombstone that records a head counts");
    assert.equal(outcome.headless, 1, "and the one that records none adds nothing");
  });

  it("passes over a pull request this replica cannot walk", async () => {
    // The fold's ceiling is enforced where a *push* crosses it, so a history
    // that arrived by replication may sit above it. This walk visits every
    // pull request — including ones with nothing redacted in them — so an
    // uncaught failure here took out `gc` for the whole repository and every
    // deepening fetch of it, on account of a pull request that has nothing to
    // do with either.
    const outcome = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const { pr, target, blob } = yield* withASecret(where);
        yield* PullRequest.redact({
          repo: where.genesis.repoId,
          pr,
          target,
          reason: "sensitive-content",
          key: where.author,
        });

        // Three events, and a ceiling of one: the same shape as a replicated
        // pull request larger than this host will walk.
        const blind = Event.ceiling(1);
        return {
          strict: (yield* Redaction.excluded().pipe(Effect.provide(blind))).size,
          explained: (yield* Redaction.covered().pipe(Effect.provide(blind))).has(blob!),
          // And with the ordinary ceiling nothing has changed.
          seen: (yield* Redaction.excluded()).size,
        };
      }),
    );

    assert.equal(outcome.strict, 0, "gc keeps running, protecting what it cannot read about");
    assert.equal(outcome.explained, false);
    assert.equal(outcome.seen, 1, "and a host that can walk it still honours the tombstone");
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
