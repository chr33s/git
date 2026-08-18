import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Exit, Layer } from "effect";

import {
  fingerprint,
  formatPublicKey,
  generate,
  NAMESPACE,
  type PrivateKey,
  sign,
} from "../crypto/SshSignature.ts";
import { StorageFailure } from "../git/Error.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid } from "../git/Store.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Record from "../trust/Record.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Event from "./Event.ts";
import { approvals, checksPassed, project } from "./Projection.ts";
import * as PullRequest from "./PullRequest.ts";
import * as Redaction from "./Redaction.ts";

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

/**
 * The same, with every object read recorded.
 *
 * A memo that saves a walk is only doing its job if the walk happens once, and
 * the only honest way to ask that is to count what the store was asked for.
 */
const counting = (reads: Array<string>) =>
  Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const inner = yield* ObjectStore;
      return ObjectStore.of({
        ...inner,
        read: (oid) =>
          Effect.andThen(
            Effect.sync(() => {
              reads.push(oid);
            }),
            inner.read(oid),
          ),
      });
    }),
  ).pipe(Layer.provideMerge(stores));

/**
 * The same, with reads of whatever `flaky.oid` names failing after the first.
 *
 * A store that is absent and a store that is broken are different answers, and
 * the only way to tell whether a walk keeps them apart is to break one. Failing
 * only after the first read is what makes the two reads of one tree — the path
 * lookup, and the emptiness check behind it — disagree, which is the window a
 * transient failure actually arrives in. Named through a holder rather than up
 * front, because what to break is an oid the run itself writes.
 */
const flakily = <A, E>(build: (flaky: { oid: Oid | null }) => Effect.Effect<A, E, Repository>) => {
  // SAFETY: the holder starts empty and the run fills it with an oid it
  // wrote; nothing else reads it, and `null` matches nothing.
  const flaky = { oid: null as Oid | null };
  let seen = 0;
  const flaking = Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const inner = yield* ObjectStore;
      return ObjectStore.of({
        ...inner,
        read: (oid) =>
          oid === flaky.oid && seen++ > 0
            ? Effect.fail(new StorageFailure({ operation: "read", path: oid }))
            : inner.read(oid),
      });
    }),
  ).pipe(Layer.provideMerge(stores));

  return Effect.runPromiseExit(
    build(flaky).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(flaking),
        ),
      ),
    ),
  );
};

const watched = <A, E>(effect: Effect.Effect<A, E, Repository>, reads: Array<string>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(counting(reads)),
        ),
      ),
    ),
  );

/** SAFETY: forty lowercase hex characters by construction. */
const oid = (seed: string): Oid => seed.repeat(40).slice(0, 40) as Oid;

const REVISION = oid("a");
const NEXT = oid("b");

interface World {
  readonly genesis: Genesis;
  readonly root: PrivateKey;
  readonly author: PrivateKey;
  readonly reviewer: PrivateKey;
}

/**
 * A repository with an author who may open pull requests and comment, and a
 * reviewer who may also approve and merge.
 */
const world = Effect.fn("test.world")(function* () {
  const root = yield* generate("root@example.com");
  const author = yield* generate("author@example.com");
  const reviewer = yield* generate("reviewer@example.com");

  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);

  const grant = (key: PrivateKey, capabilities: ReadonlyArray<string>) =>
    Effect.flatMap(
      Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(key.publicKey),
        capabilities,
        id: Log.newId(),
      }),
      (payload) => Log.issue(payload, [root]),
    );

  yield* grant(author, ["hub.create-pr", "hub.comment", "hub.review"]);
  yield* grant(reviewer, [
    "hub.create-pr",
    "hub.comment",
    "hub.review",
    "hub.approve",
    "hub.merge",
    "hub.redact",
  ]);

  return { genesis, root, author, reviewer } satisfies World;
});

const projectionOf = (where: World, pr: string) =>
  Effect.flatMap(projectTrust(where.genesis), (trust) => project(where.genesis, trust, pr));

const opened = Effect.fn("test.opened")(function* (where: World) {
  return yield* PullRequest.open({
    repo: where.genesis.repoId,
    title: "Add a thing",
    description: "It does the thing.",
    base: "refs/heads/main",
    head: REVISION,
    key: where.author,
  });
});

describe("hub projection", () => {
  it("refuses an id that cannot name a ref this repository can find again", async () => {
    // The id becomes a path component of `refs/hub/pr/<id>`, and `prOf`
    // refuses one with a `/` in it — so a pull request opened as `team/42`
    // landed on a ref nothing lists. `Policy.protectedBranch` could then never
    // match it, which makes the branch it targets unpushable, and
    // `Redaction.excluded` never honoured its tombstones.
    const failures = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const attempt = (id: string) =>
          PullRequest.open({
            repo: where.genesis.repoId,
            title: "Add a thing",
            base: "refs/heads/main",
            head: REVISION,
            id,
            key: where.author,
          }).pipe(
            Effect.as(null),
            Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
          );
        return {
          nested: yield* attempt("team/42"),
          empty: yield* attempt(""),
          wild: yield* attempt("a*b"),
          // The shapes a hand-written list of reserved characters missed.
          // Each of these validated, wrote a payload blob, a tree and a
          // commit, and only then failed at `setRef` with a ref-name error —
          // objects left behind, from a call that was given no ref.
          traversal: yield* attempt("a..b"),
          dotted: yield* attempt(".hidden"),
          trailing: yield* attempt("42."),
          locked: yield* attempt("42.lock"),
          reflog: yield* attempt("42@{0}"),
          plain: yield* attempt("42"),
        };
      }),
    );

    assert.match(failures.nested ?? "", /one ref path component/);
    assert.notEqual(failures.empty, null);
    assert.notEqual(failures.wild, null);
    // Refused by the *id* check, before anything was written. Matched on the
    // message rather than on failure alone: each of these failed either way,
    // one at validation and one at `setRef` after three objects had been
    // committed, and only the first is this feature working.
    for (const [spelling, reason] of [
      ["'..'", failures.traversal],
      ["a leading '.'", failures.dotted],
      ["a trailing '.'", failures.trailing],
      ["'.lock'", failures.locked],
      ["'@{'", failures.reflog],
    ] as const) {
      assert.match(
        reason ?? "",
        /cannot name a pull request/,
        `${spelling} must be refused as an id`,
      );
    }
    assert.equal(failures.plain, null, "an ordinary id still opens");
  });

  it("refuses to fold a pull request past the ceiling, and to push one there", async () => {
    // Folding builds an ancestor set per commit, which is quadratic, and how
    // many commits a pull request has is chosen by whoever may append to it —
    // the lowest hub capability there is. On the protected-branch path that
    // fold is synchronous and inside a worker with a fixed memory ceiling, so
    // an unbounded one is a push that never returns rather than a push that is
    // refused. The bound is asked in both places: at the fold, for a history
    // that arrived by replication, and at the boundary, so a pull request can
    // never *become* unfoldable — bounded only at the fold, whoever may append
    // could take somebody else's approved one past the line and freeze the
    // protected branch behind it.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const where = yield* world();
        const { pr } = yield* opened(where);
        yield* PullRequest.comment({
          repo: where.genesis.repoId,
          pr,
          body: "and another thing",
          key: where.author,
        });
        const head = yield* repository.resolve(Event.refOf(pr));
        return {
          under: (yield* Event.entries(pr)).events.length,
          over: yield* Event.entries(pr).pipe(
            Effect.as(null),
            Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
            Effect.provide(Event.ceiling(1)),
          ),
          admits: yield* Event.withinCeiling(head!),
          refuses: yield* Event.withinCeiling(head!).pipe(Effect.provide(Event.ceiling(1))),
        };
      }),
    );

    assert.equal(outcome.under, 2, "an ordinary pull request folds");
    assert.match(outcome.over ?? "", /cannot be folded/);
    assert.equal(outcome.admits, true);
    assert.equal(outcome.refuses, false, "and the boundary refuses the push that would get there");
  });

  it("qualifies a base branch spelled without its refs/heads/ prefix", async () => {
    // `base` is a string a client writes, and both spellings are natural —
    // `main` from a UI, `refs/heads/main` from a script. `protectedBranch`
    // matches a pull request to the branch being pushed by comparing this
    // against a fully qualified ref, so an unqualified one matched nothing:
    // the pull request stopped counting toward its own branch's approvals and
    // made that branch permanently unpushable, reported as missing approvals
    // rather than as a spelling.
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const { pr } = yield* PullRequest.open({
          repo: where.genesis.repoId,
          title: "Add a thing",
          base: "main",
          head: REVISION,
          key: where.author,
        });
        return yield* projectionOf(where, pr);
      }),
    );

    assert.equal(state.base, "refs/heads/main");
  });

  it("projects an opened pull request", async () => {
    const state = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const { pr } = yield* opened(where);
        return yield* projectionOf(where, pr);
      }),
    );

    assert.equal(state.title, "Add a thing");
    assert.equal(state.base, "refs/heads/main");
    assert.equal(state.head, REVISION);
    assert.equal(state.state, "open");
    assert.deepEqual(state.rejected, []);
  });

  it("derives the head from the events, with no mutable head ref", async () => {
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const where = yield* world();
        const { pr } = yield* opened(where);
        yield* PullRequest.update({
          repo: where.genesis.repoId,
          pr,
          head: NEXT,
          key: where.author,
        });

        return {
          state: yield* projectionOf(where, pr),
          headRef: yield* repository.resolve(`${Event.refOf(pr)}/head`),
        };
      }),
    );

    assert.equal(outcome.state.head, NEXT);
    assert.equal(outcome.headRef, null, "there must be no mutable head ref to disagree with");
  });

  describe("reviews", () => {
    it("counts an approval of the current revision", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(approvals(state).length, 1);
      assert.equal(state.reviews[0]?.head, REVISION);
    });

    it("makes an approval stale when the head moves, without unmaking it", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          yield* PullRequest.update({
            repo: where.genesis.repoId,
            pr,
            head: NEXT,
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(approvals(state).length, 0, "a stale approval must not count");
      assert.equal(state.reviews.length, 1, "it stays true about the revision it named");
      assert.equal(state.reviews[0]?.stale, true);
      assert.equal(state.reviews[0]?.head, REVISION);
    });

    it("makes an approval stale when the base moves, not only the head", async () => {
      // A reviewer approves a revision *for a destination*. The destination is
      // rewritten by a second `pr.opened`, which the pull request's own author
      // may make without any further capability — and staleness was computed
      // from `head` alone, so an approval given for `refs/heads/docs`
      // authorized pushing that same revision to `refs/heads/main`.
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "Add a thing",
            description: "It does the thing.",
            base: "refs/heads/docs",
            head: REVISION,
            key: where.author,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          // The same author, re-opening their own pull request at another
          // branch: no capability beyond the one that opened it.
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            id: pr,
            title: "Add a thing",
            description: "It does the thing.",
            base: "refs/heads/main",
            head: REVISION,
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.base, "refs/heads/main", "the retargeting took effect");
      assert.equal(state.reviews[0]?.base, "refs/heads/docs", "and the review remembers its own");
      assert.equal(state.reviews[0]?.stale, true);
      assert.equal(approvals(state).length, 0, "so it authorizes nothing on the new branch");
    });

    it("does not let a ground-down retarget rewrite what an approval was given for", async () => {
      // The base a review was given for was read off the walk's position
      // rather than off the review's own history. Fold order breaks ties by
      // oid, which whoever writes the commit grinds — so a sibling `pr.opened`
      // ground *below* an existing approval folds first, the approval is
      // recorded against the base that sibling chose, and it is not stale. An
      // approval given for `refs/heads/docs` then satisfied a protected
      // `refs/heads/main`. The head already uses descent for exactly this
      // reason; the base did not.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();

          // Opened until the approval's oid sorts *above* the opening's, so
          // that there is a window between them for the sibling to land in.
          // Both oids are the repository's to choose and neither is grindable
          // from here, so the fixture retries rather than steering them.
          let pr = "";
          let first: Event.Walked["events"][number] | undefined;
          // SAFETY: replaced on the first pass of the loop below, before it is
          // compared against anything; the empty string is only its shape.
          let approval = "" as Oid;
          for (let round = 0; round < 32; round++) {
            ({ pr } = yield* PullRequest.open({
              repo: where.genesis.repoId,
              title: "Add a thing",
              description: "It does the thing.",
              base: "refs/heads/docs",
              head: REVISION,
              key: where.author,
            }));
            approval = yield* PullRequest.review({
              repo: where.genesis.repoId,
              pr,
              head: REVISION,
              decision: "approve",
              key: where.reviewer,
            });
            first = (yield* Event.entries(pr)).events.find(
              (entry) => entry.payload?.type === "pr.opened",
            );
            if (first !== undefined && first.commit < approval) break;
          }

          // The author's own retarget: written the ordinary way, then moved
          // to sit *beside* the approval rather than on top of it, and ground
          // below it so the walk reaches it first. Nothing here is forged —
          // every byte of it is the author's to write.
          const ref = Event.refOf(pr);
          const approved = yield* repository.resolve(ref);
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            id: pr,
            title: "Add a thing",
            description: "It does the thing.",
            base: "refs/heads/main",
            head: REVISION,
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const retarget = events.find(
            (entry) =>
              entry.payload?.type === "pr.opened" && entry.payload.base === "refs/heads/main",
          );

          // Ground into the window *between* the two, which is the only
          // placement that demonstrates anything. Below the opening, the
          // sibling folds before any author is established and the capability
          // check refuses it on its own merits. Above the approval, the
          // approval is folded first and records the right base whether or not
          // this is fixed. In between, the sibling folds first and the
          // approval is the thing whose base is at stake.
          let sibling = first!.commit;
          const between = () => sibling > first!.commit && sibling < approval;
          for (let attempt = 0; attempt < 512 && !between(); attempt++) {
            sibling = yield* Record.write({
              name: Event.RECORD,
              payload: retarget!.bytes,
              signatures: retarget!.signatures,
              parents: [],
              message: `pr.opened moved ${attempt}\n`,
            });
          }
          yield* repository.setRef({ name: ref, to: approved! });
          yield* repository.setRef({ name: ref, to: yield* Event.join(pr, [approved!, sibling]) });

          return {
            ground: sibling > first!.commit && sibling < approval,
            state: yield* projectionOf(where, pr),
          };
        }),
      );

      assert.equal(
        outcome.ground,
        true,
        "the fixture must land the sibling between the opening and the approval",
      );
      assert.equal(
        outcome.state.reviews[0]?.base,
        "refs/heads/docs",
        "the approval was given for the branch its own history named",
      );
      assert.equal(outcome.state.reviews[0]?.stale, true);
      assert.equal(approvals(outcome.state).length, 0, "so it authorizes nothing on the new base");
    });

    it("resolves a thread whose id somebody else claimed later", async () => {
      // Two claimants on one id used to answer "ambiguous", so every later
      // `comment.resolved` naming it was rejected — for good, on a ref that
      // only grows. Any `hub.comment` holder could pick a thread's id, open a
      // comment under it, and leave a branch requiring resolved threads unable
      // ever to be satisfied. Whichever claim came *first* is the one the
      // references were written about, and first here is descent: an append is
      // written onto the ref's head, so the later duplicate descends from the
      // original.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const thread = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "this needs a look",
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const id = events.find((entry) => entry.commit === thread)?.payload?.id ?? "";

          // Somebody else's comment, under the same id, appended the ordinary
          // way — so it descends from the thread it is colliding with.
          const bytes = Event.encode({
            version: 1,
            type: "comment.created",
            repo: where.genesis.repoId,
            pr,
            id,
            issuedAt: new Date(1_700_000_001_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            body: "me too",
            head: null,
            path: null,
            side: null,
            line: null,
            contextHash: null,
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "comment.created collision\n",
            }),
          });

          yield* PullRequest.resolve({
            repo: where.genesis.repoId,
            pr,
            thread: id,
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(outcome.threads.length, 2, "both claims are folded");
      assert.equal(
        outcome.threads.filter((thread) => thread.resolved).length,
        1,
        "and the one the reference was written about is resolved",
      );
    });

    it("folds on when an event names a trust head this host will not walk", async () => {
      // A head whose ancestry exceeds the log's ceiling is *refused* rather
      // than answered empty, and that refusal must not take the fold down. One
      // commit with an empty tree and enough fabricated parents, named as an
      // event's trust head by anybody holding `hub.comment`, would otherwise
      // make this pull request unfoldable for good — and a pull request the
      // boundary cannot fold is a protected branch that can never be pushed
      // again, on a ref the event cannot be removed from.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });

          // Trust-log-shaped and enormous: an empty tree reads as a join, and
          // the parents are oids this repository has never held.
          const sprawl = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: Array.from(
              { length: 40 },
              // SAFETY: forty lowercase hex characters, which is what `Oid`
              // brands; they name nothing, which is the point.
              (_, index) => `${index}`.padStart(40, "b") as Oid,
            ),
            message: "join\n",
            author: Record.identityAt(new Date(1_700_000_000_000)),
          });

          const bytes = Event.encode({
            version: 1,
            type: "comment.created",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_001_000).toISOString(),
            trustHead: sprawl,
            body: "naming a head nobody can walk",
            head: null,
            path: null,
            side: null,
            line: null,
            contextHash: null,
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "comment.created sprawling\n",
            }),
          });

          return yield* projectionOf(where, pr).pipe(
            Effect.provide(Log.ceiling(8)),
            Effect.as(null),
            Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
          );
        }),
      );

      assert.equal(outcome, null, "the pull request still folds");
    });

    it("refuses a trust head that is not an object id", async () => {
      // The trust head is written by the event's own signer and *used as a
      // name*: the fold walks the log from it, which means reading an object
      // by that name. Never checked, `../HEAD` went straight into a path and
      // left the objects directory — a read oracle, and a decompression
      // failure that then took the whole fold down, permanently, on a ref that
      // cannot be rewound. Every protected-branch push, every collection and
      // every deepening fetch touching that pull request went with it, at the
      // cost of one comment from anybody holding `hub.comment`.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          const bytes = Event.encode({
            version: 1,
            type: "comment.created",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_001_000).toISOString(),
            trustHead: "../HEAD",
            body: "naming a file instead of a commit",
            head: null,
            path: null,
            side: null,
            line: null,
            contextHash: null,
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "comment.created escaping\n",
            }),
            expected: head,
          });

          // The fold still happens, which is the point: one bad event is one
          // rejection, not a pull request nothing can read.
          const state = yield* projectionOf(where, pr);
          return { threads: state.threads.length, reason: state.rejected.at(-1)?.reason ?? "" };
        }),
      );

      assert.equal(outcome.threads, 0, "the event counts for nothing");
      assert.match(outcome.reason, /is not an object id/);
    });

    it("does not read a broken store as 'not part of this history'", async () => {
      // Absent and broken are different answers. The walk that decides whether
      // a commit belongs to a pull request tolerates absence deliberately —
      // refs are applied without a connectivity check, so a replica can hold a
      // commit whose tree never arrived — and it was tolerating failure along
      // with it. That walk *is* the boundary of the history, so a failure read
      // as "not part of it" does not skip one commit: it empties the pull
      // request. No events, so no tombstones, so nothing excluded — and `gc`
      // re-protects and repacks a payload a valid tombstone covered, leaving
      // bytes the operator was told were gone still clonable.
      const asked = await flakily((flaky) =>
        Effect.gen(function* () {
          const repository = yield* Repository;
          // A tree with something in it and no event: the shape whose
          // emptiness has to be read from the store rather than from the oid.
          const tree = yield* repository.writeTree([
            {
              mode: "100644",
              name: "file.txt",
              oid: yield* repository.writeBlob(new Uint8Array(1)),
            },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "not an event\n",
            author: Record.identityAt(new Date(1_700_000_000_000)),
          });
          // From here the path lookup answers and the emptiness check behind
          // it does not, which is what a transient failure looks like.
          flaky.oid = tree;
          return yield* Event.isHubCommit(commit);
        }),
      );

      assert.ok(Exit.isFailure(asked), "a store that failed is not a store that said no");
    });

    it("gives back the id of a merge it refuses", async () => {
      // Every other event type holds the invariant that a rejected event does
      // not keep its author's slot for that id. A merge is settled after the
      // loop that hands the slot out, so a refused one kept it — leaving
      // `claims` resolving the id to an event nothing accepted, and the author
      // unable to ever use the id again on this pull request.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          const id = Event.newId();
          const write = (head: string, message: string) =>
            Effect.gen(function* () {
              const bytes = Event.encode({
                version: 1,
                type: "pr.merged",
                repo: where.genesis.repoId,
                pr,
                id,
                issuedAt: new Date(1_700_000_001_000).toISOString(),
                trustHead: yield* repository.resolve(Log.LOG_REF),
                head,
                mergeCommit: head,
              });
              const ref = Event.refOf(pr);
              const at = yield* repository.resolve(ref);
              yield* repository.setRef({
                name: ref,
                to: yield* Record.write({
                  name: Event.RECORD,
                  payload: bytes,
                  signatures: [yield* sign(where.reviewer, bytes, NAMESPACE)],
                  parents: [at!],
                  message,
                }),
                expected: at,
              });
            });

          // Refused: this pull request proposed `REVISION`, not `NEXT`.
          yield* write(Event.qualify(NEXT), "pr.merged of something else\n");
          const state = yield* projectionOf(where, pr);
          return { state: state.state, claimed: state.claims.has(id) };
        }),
      );

      assert.equal(outcome.state, "open");
      assert.equal(outcome.claimed, false, "the refused merge holds no id");
    });

    it("refuses a merge of a revision the pull request never proposed", async () => {
      // "Merged" is the one state with no way back — `pr.closed` and
      // `pr.reopened` both stop at it, deliberately, since the merge has
      // already landed in the branch. Applied to whatever revision the event
      // happened to name, one stray `pr.merged` took an approved pull request
      // out as the route to its protected branch, permanently, on a ref that
      // cannot be rewound: the denial `pr.closed` is guarded against, reached
      // through the one door where closing it again does not help.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          const bytes = Event.encode({
            version: 1,
            type: "pr.merged",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_001_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            // Not `REVISION`, which is what this pull request proposed.
            head: Event.qualify(NEXT),
            mergeCommit: Event.qualify(NEXT),
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.reviewer, bytes, NAMESPACE)],
              parents: [head!],
              message: "pr.merged of something else\n",
            }),
            expected: head,
          });

          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(outcome.state, "open", "the pull request is still the route to its branch");
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /never proposed/);
    });

    it("walks a head it refuses exactly once, not once per ask", async () => {
      // The memo in front of the log walk recorded successes only, so the one
      // head worth remembering — the one whose ancestry this host will not
      // walk — was walked again from scratch on every ask, several times per
      // event and once per event per fold, each ask reading the whole ceiling
      // before refusing. The chain costs one push to write and is referenced
      // by nothing, so no rule that inspects refs ever sees it; named as a
      // trust head it charged every later protected-branch push, collection
      // and deepening fetch for the walk, synchronously.
      const reads: Array<string> = [];
      const sprawl = await watched(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          const chain = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: Array.from(
              { length: 40 },
              // SAFETY: forty lowercase hex characters, which is what `Oid`
              // brands; they name nothing, which is the point.
              (_, index) => `${index}`.padStart(40, "c") as Oid,
            ),
            message: "join\n",
            author: Record.identityAt(new Date(1_700_000_000_000)),
          });

          // Several events, every one of them naming that same head.
          for (let index = 0; index < 6; index++) {
            const bytes = Event.encode({
              version: 1,
              type: "comment.created",
              repo: where.genesis.repoId,
              pr,
              id: Event.newId(),
              issuedAt: new Date(1_700_000_001_000 + index).toISOString(),
              trustHead: chain,
              body: `naming a head nobody can walk (${index})`,
              head: null,
              path: null,
              side: null,
              line: null,
              contextHash: null,
            });
            const ref = Event.refOf(pr);
            const head = yield* repository.resolve(ref);
            yield* repository.setRef({
              name: ref,
              to: yield* Record.write({
                name: Event.RECORD,
                payload: bytes,
                signatures: [yield* sign(where.author, bytes, NAMESPACE)],
                parents: [head!],
                message: "comment.created sprawling\n",
              }),
            });
          }

          // Counted from here, so writing the history above is not in it.
          reads.length = 0;
          yield* projectionOf(where, pr).pipe(Effect.provide(Log.ceiling(8)));
          return chain;
        }),
        reads,
      );

      // Two, because the redaction fold runs twice and each pass folds with a
      // memo of its own — not twelve, which is once per event per pass.
      const walks = reads.filter((oid) => oid === sprawl).length;
      assert.equal(walks, 2, "six events naming one unwalkable head is one refused walk per pass");
    });

    it("counts approvers, not approval events", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          // The same reviewer, twice. Counting events would let one member
          // satisfy a "two approvals required" rule on their own.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "still looks right",
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(approvals(state).length, 1);
      assert.equal(state.reviews.length, 2, "both statements are still on the record");
    });

    it("does not let a comment-review cancel the same author's approval", async () => {
      // A `comment` review takes no position, and it costs `hub.review` rather
      // than `hub.approve` — which is the whole difference between them. Read
      // as the reviewer's latest word, one cancelled their own approval: a
      // reviewer who approved and then said something in passing lost the
      // approval, and a `hub.review` holder could cancel a `hub.approve`
      // holder's word, which is what `review.dismissed` charges `hub.approve`
      // to prevent.
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "comment",
            body: "one more thought",
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.reviews.length, 2, "both statements are on the record");
      assert.equal(approvals(state).length, 1, "and the approval still stands");
    });

    it("withdraws an approval with a back-dated rejection too", async () => {
      // Which of a reviewer's statements counts was decided by the date the
      // statement claimed, and `issuedAt` is written by whoever signed it — so
      // a reviewer could withdraw an approval in a way that did not withdraw
      // it, by dating the withdrawal before the approval. Fold order is
      // ancestry with a deterministic tie-break, and the withdrawal is built
      // on the approval, so it is the later word wherever it is folded.
      const state = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });

          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "review.submitted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            // Well before the approval it is meant to withdraw.
            issuedAt: new Date(1_600_000_000_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            head: Event.qualify(REVISION),
            decision: "reject",
            body: "actually, no",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.reviewer, bytes, NAMESPACE)],
              parents: [head!],
              message: "review.submitted backdated\n",
            }),
            expected: head,
          });

          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.reviews.length, 2, "both statements are on the record");
      assert.equal(approvals(state).length, 0, "and the later one is the one that counts");
    });

    it("lets a later rejection withdraw the same author's approval", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          // "Request changes" after approving has to block the merge, or the
          // reviewer's latest word counts for nothing.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "reject",
            body: "actually, no",
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );
      assert.equal(approvals(state).length, 0);
    });

    it("does not count a dismissed approval", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const review = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          yield* PullRequest.dismissReview({
            repo: where.genesis.repoId,
            pr,
            review,
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(approvals(state).length, 0);
      assert.equal(state.reviews[0]?.dismissed, true);
    });

    it("refuses an approval from a member who may review but not approve", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          // The author holds `hub.review`, not `hub.approve`.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(approvals(state).length, 0);
      assert.match(state.rejected.at(-1)?.reason ?? "", /hub\.approve/);
    });
  });

  describe("comments", () => {
    it("threads a reply and resolves it", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "this line worries me",
            head: REVISION,
            path: "src/git/Repository.ts",
            side: "new",
            line: 184,
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const thread = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          yield* PullRequest.reply({
            repo: where.genesis.repoId,
            pr,
            thread,
            body: "fixed",
            key: where.author,
          });
          yield* PullRequest.resolve({
            repo: where.genesis.repoId,
            pr,
            thread,
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.threads.length, 1);
      assert.equal(state.threads[0]?.comments.length, 2);
      assert.equal(state.threads[0]?.resolved, true);
      assert.equal(state.threads[0]?.line, 184);
      assert.equal(state.threads[0]?.path, "src/git/Repository.ts");
    });

    it("reopens a resolved thread", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "still wrong",
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const thread = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          yield* PullRequest.resolve({ repo: where.genesis.repoId, pr, thread, key: where.author });
          yield* PullRequest.reopenThread({
            repo: where.genesis.repoId,
            pr,
            thread,
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );
      assert.equal(state.threads[0]?.resolved, false);
    });
  });

  describe("checks", () => {
    it("records a completed check against the revision it ran on", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const ci = yield* generate("ci@example.com");
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(ci.publicKey),
              capabilities: ["hub.check:test"],
              id: Log.newId(),
            }),
            [where.root],
          );

          const { pr } = yield* opened(where);
          yield* PullRequest.checkCompleted({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            name: "test",
            provider: "buildkite",
            status: "success",
            key: ci,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.checks.length, 1);
      assert.equal(state.checks[0]?.status, "success");
      assert.ok(checksPassed(state, ["test"]));
    });

    it("does not let a late start undo a finish for the same revision", async () => {
      // The two share one slot, keyed by check name and revision, and are
      // applied in fold order — topological with an oid tie-break. So a
      // `check.started` written on another replica and brought back by a join
      // could land *after* the completion it belongs with, turn a recorded
      // success into "started", and hold the protected branch shut on every
      // replica until somebody signed a fresh completion. Re-running a check
      // is what replaces its answer, and that arrives as another finish.
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const ci = yield* generate("ci@example.com");
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(ci.publicKey),
              capabilities: ["hub.check:test"],
              id: Log.newId(),
            }),
            [where.root],
          );

          const { pr } = yield* opened(where);
          yield* PullRequest.checkCompleted({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            name: "test",
            provider: "buildkite",
            status: "success",
            key: ci,
          });
          // The start arrives afterwards, which is what a join makes possible.
          yield* PullRequest.checkStarted({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            name: "test",
            provider: "buildkite",
            key: ci,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.checks[0]?.status, "success");
      assert.ok(checksPassed(state, ["test"]), "the branch is not held shut by a stale start");
    });

    it("refuses a check signed by a bot trusted for a different check", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const ci = yield* generate("ci@example.com");
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(ci.publicKey),
              capabilities: ["hub.check:test"],
              id: Log.newId(),
            }),
            [where.root],
          );

          const { pr } = yield* opened(where);
          // Trusted for `test`; signing `deploy` is the escalation the scoped
          // capability exists to stop.
          yield* PullRequest.checkCompleted({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            name: "deploy",
            provider: "buildkite",
            status: "success",
            key: ci,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.checks.length, 0);
      assert.equal(checksPassed(state, ["deploy"]), false);
      assert.match(state.rejected.at(-1)?.reason ?? "", /hub\.check:deploy/);
    });

    it("does not count a check that ran on a superseded revision", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const ci = yield* generate("ci@example.com");
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(ci.publicKey),
              capabilities: ["hub.check:test"],
              id: Log.newId(),
            }),
            [where.root],
          );

          const { pr } = yield* opened(where);
          yield* PullRequest.checkCompleted({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            name: "test",
            provider: "buildkite",
            status: "success",
            key: ci,
          });
          yield* PullRequest.update({
            repo: where.genesis.repoId,
            pr,
            head: NEXT,
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );
      assert.equal(checksPassed(state, ["test"]), false);
    });
  });

  describe("lifecycle", () => {
    it("closes and reopens", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: where.author });
          const closed = yield* projectionOf(where, pr);
          yield* PullRequest.reopen({ repo: where.genesis.repoId, pr, key: where.author });
          return { closed, reopened: yield* projectionOf(where, pr) };
        }),
      );
      assert.equal(outcome.closed.state, "closed");
      assert.equal(outcome.reopened.state, "open");
    });

    it("closing a merged pull request does not unmerge it", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.merged({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            mergeCommit: NEXT,
            key: where.reviewer,
          });
          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: where.author });
          return yield* projectionOf(where, pr);
        }),
      );
      assert.equal(state.state, "merged", "a merge has already landed in the branch");
      assert.equal(state.mergeCommit, NEXT);
    });

    it("refuses a merge event from somebody who may not merge", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.merged({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            mergeCommit: NEXT,
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );
      assert.equal(state.state, "open");
      assert.match(state.rejected.at(-1)?.reason ?? "", /hub\.merge/);
    });
  });

  describe("an event's declared trust head", () => {
    it("is raised to one an earlier event in the same pull request named", async () => {
      // The trust head is written by the signer, and a forward-only revocation
      // is judged by whether that head already reached it. Unconstrained, a
      // revoked member could name any pre-revocation commit and have their old
      // capabilities recovered from `former`. What they cannot do is rewrite
      // the events they are building on: an event whose own ancestors were
      // written against a later head is claiming to have seen less than the
      // conversation it is joining, and is read as having seen what they had.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          // The head as the opening event saw it, before anything else moved.
          const early = yield* repository.resolve(Log.LOG_REF);

          // The log moves on, and the reviewer is revoked.
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "left",
              id: Log.newId(),
            }),
            [where.root],
          );
          // …and a second event that honestly names the new head, so the
          // conversation has visibly moved past the revocation.
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "still here",
            key: where.author,
          });

          // The revoked reviewer backdates: an approval naming the head from
          // before their own revocation, appended after the comment.
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "review.submitted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: early,
            head: Event.qualify(REVISION),
            decision: "approve",
            body: "backdated",
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(where.reviewer, bytes, NAMESPACE)],
            parents: [head!],
            message: "review.submitted backdated\n",
          });
          yield* repository.setRef({ name: ref, to: forged, expected: head });

          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.reviews.length, 0, "a backdated approval must not count");
      // Refused by the revocation itself, which is the rule doing the work:
      // held to the floor, the backdated head reaches the revocation after all.
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /revoked/);
    });

    it("holds a retargeting pr.opened to the floor, like every other event", async () => {
      // The *winning* opening keeps the pre-pass verdict, so the two passes
      // cannot disagree and leave the pull request unreadable. Extending that
      // to every `pr.opened` was too much: a revoked author could land a
      // second one by back-dating its trust head, rewrite `base`, and freeze
      // the protected branch the approved pull request was the route to — the
      // same event as `pr.updated`, which the floor does catch.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          // The head the author's own opening named, before anything moved.
          const early = yield* repository.resolve(Log.LOG_REF);

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.author.publicKey),
              reason: "left",
              id: Log.newId(),
            }),
            [where.root],
          );
          // An honest event that names the new head, so the conversation has
          // visibly moved past the revocation.
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "still here",
            key: where.reviewer,
          });

          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "pr.opened",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: early,
            title: "Add a thing",
            description: "",
            base: "refs/heads/elsewhere",
            head: Event.qualify(REVISION),
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "pr.opened backdated retarget\n",
            }),
            expected: head,
          });

          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(
        outcome.base,
        "refs/heads/main",
        "a back-dated retarget must not move the branch this proposes to",
      );
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /revoked/);
    });

    it("may match what an earlier event named, which is the honest case", async () => {
      // Two events written against the same head is what an ordinary
      // conversation looks like; the rule bounds going *backwards*, not
      // standing still.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "fine",
            key: where.reviewer,
          });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.reviews.length, 1);
      assert.deepEqual(outcome.rejected, []);
    });

    it("does not drop an honest event from a replica whose trust log lags", async () => {
      // Hub refs and the trust log replicate as separate refs, so a client can
      // hold a conversation that has moved past a grant its own log has not
      // fetched yet, and it names the older head honestly. Refusing for that
      // dropped the comment, review or approval *permanently* — the floor
      // comes from a history that only grows, so re-folding once the log
      // caught up could not rescue it, and a slow mirror silently lost the
      // approvals it was replicating.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);

          // The head the reviewer's client is still on.
          const behind = yield* repository.resolve(Log.LOG_REF);

          // The log moves — somebody unrelated is granted membership — and an
          // event in this pull request names the newer head.
          const newcomer = yield* generate("newcomer@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(newcomer.publicKey),
              capabilities: ["hub.comment"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "seen the new member",
            key: where.author,
          });

          // And only now does the reviewer approve, against the head they had.
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "review.submitted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: behind,
            head: Event.qualify(REVISION),
            decision: "approve",
            body: "still looks right",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.reviewer, bytes, NAMESPACE)],
              parents: [head!],
              message: "review.submitted lagging\n",
            }),
            expected: head,
          });

          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(approvals(outcome).length, 1, "a lagging approval must still count");
      assert.deepEqual(outcome.rejected, []);
    });
  });

  describe("a forged duplicate event id", () => {
    it("cannot displace the authorized event that claimed it", async () => {
      // The attack: `Event.entries` used to resolve duplicate ids before any
      // signature was checked, so the winner was decided by commit order —
      // whose tie-break is the oid, which anybody able to write a hub ref can
      // grind. A member holding only `hub.comment` re-used the approval's id,
      // sorted first, and the real approval was diverted into the conflict
      // list — taking the merge's required approval with it.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const ref = Event.refOf(pr);
          const root = yield* repository.resolve(ref);

          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "looks right",
            key: where.reviewer,
          });
          const approval = yield* repository.resolve(ref);
          const { events } = yield* Event.entries(pr);
          const id = events.find((entry) => entry.commit === approval)?.payload?.id ?? "";

          // Mallory holds `hub.comment` and nothing else. She re-uses the
          // approval's id and grinds the timestamp until her commit sorts
          // below it, which is what puts her first in topological order.
          const mallory = yield* generate("mallory@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(mallory.publicKey),
              capabilities: ["hub.comment"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // Written straight into the object store as a child of the root,
          // rather than through `Event.issue`: an attacker crafts commits, and
          // the ref only has to end up naming one of them.
          // One candidate, and the assertion holds for either order. Grinding
          // for a lower oid is not a bounded loop: when the target's own oid
          // is already near-minimal, no number of attempts reliably beats it.
          const bytes = Event.encode({
            version: 1,
            type: "review.submitted",
            repo: where.genesis.repoId,
            pr,
            id,
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            head: Event.qualify(REVISION),
            decision: "approve",
            body: "not mine to give",
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(mallory, bytes, NAMESPACE)],
            parents: [root!],
            message: `review.submitted ${id}\n`,
          });

          // Both sides in one history, as a replica that fetched them would
          // have. The join is where the two claims meet.
          const joined = yield* Event.join(pr, [approval!, forged]);
          yield* repository.setRef({ name: ref, to: joined });

          const trust = yield* projectTrust(where.genesis);
          return {
            first: forged < approval! ? "forged" : "approval",
            state: yield* project(where.genesis, trust, pr),
          };
        }),
      );

      assert.equal(
        outcome.state.reviews.length,
        1,
        `the authorized approval must survive (${outcome.first} folded first)`,
      );
      assert.equal(outcome.state.reviews[0]?.decision, "approve");
      // And the forgery is refused on its own merits, by name.
      assert.match(outcome.state.rejected.map((entry) => entry.reason).join(" "), /hub\.approve/);
    });
  });

  describe("an id claimed by two authors", () => {
    it("cannot evict a stranger's event, whatever the commit order", async () => {
      // Scoping the claim to the id alone only stopped an impostor whose own
      // event type needed a capability they lacked. A member holding
      // `hub.comment` could re-use an approval's id in a `comment.created` —
      // authorized on its own terms — grind the oid below the approval's, and
      // have the genuine approval rejected as the duplicate.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const ref = Event.refOf(pr);
          const root = yield* repository.resolve(ref);

          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "looks right",
            key: where.reviewer,
          });
          const approval = yield* repository.resolve(ref);
          const { events } = yield* Event.entries(pr);
          const id = events.find((entry) => entry.commit === approval)?.payload?.id ?? "";
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // The author holds `hub.comment`, so this event is authorized — it
          // simply is not the reviewer's, and must not be able to displace it.
          // One candidate, asserted for either order; see above.
          const bytes = Event.encode({
            version: 1,
            type: "comment.created",
            repo: where.genesis.repoId,
            pr,
            id,
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            body: "mine now",
            head: null,
            path: null,
            side: null,
            line: null,
            contextHash: null,
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(where.author, bytes, NAMESPACE)],
            parents: [root!],
            message: `comment.created ${id}\n`,
          });

          const joined = yield* Event.join(pr, [approval!, forged]);
          yield* repository.setRef({ name: ref, to: joined });

          const trust = yield* projectTrust(where.genesis);
          return {
            first: forged < approval! ? "forged" : "approval",
            state: yield* project(where.genesis, trust, pr),
          };
        }),
      );

      assert.equal(
        outcome.state.reviews.length,
        1,
        `the approval must survive (${outcome.first} folded first)`,
      );
      assert.equal(outcome.state.reviews[0]?.decision, "approve");
      // Both events stand: sharing an id is not by itself a reason to drop one.
      assert.equal(outcome.state.threads.length, 1);
      assert.deepEqual(outcome.state.rejected, []);
    });
  });

  describe("an event folded before the opening one", () => {
    it("cannot re-enable self-approval by contesting the opening", async () => {
      // `contested` is computed over *accepted* openings. Computed over the
      // raw walk, an unsigned second `pr.opened` — pushable by anybody who may
      // write the ref — left `author` null, and `approvals` can only exclude
      // an author it knows: so one member could open a pull request for their
      // own commit, contest their own opening, approve it, and clear
      // `requiredApprovals` on a protected branch alone.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "mine",
            base: "refs/heads/main",
            head: REVISION,
            key: where.reviewer,
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // A second opening nobody signed at all.
          const bytes = Event.encode({
            version: 1,
            type: "pr.opened",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            title: "noise",
            description: "",
            base: "refs/heads/main",
            head: Event.qualify(REVISION),
          });
          const unsigned = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [],
            parents: [],
            message: "pr.opened unsigned\n",
          });
          const joined = yield* Event.join(pr, [head!, unsigned]);
          yield* repository.setRef({ name: ref, to: joined });

          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });

          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(approvals(outcome).length, 0, "self-approval must still count for nothing");
    });

    it("cannot escape the self-approval exclusion with a stale trust head", async () => {
      // `openers` was filled only by the pre-pass, which judges each
      // `pr.opened` against the head it *declares*; the loop judges the
      // non-winning ones against the floor their ancestors raise them to. So
      // an opening the pre-pass refused — signed against a head where its
      // signer was not yet a member — is accepted by the loop once its
      // ancestors have raised the floor past that grant, supplies the title,
      // description and base, and leaves its signer out of the set `approvals`
      // excludes. One member could then satisfy a protected branch's required
      // approval on their own.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const latecomer = yield* generate("latecomer@example.com");

          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "Add a thing",
            base: "refs/heads/main",
            head: REVISION,
            key: where.author,
          });
          // The head the opening was signed against, and the one the
          // latecomer's grant lands after.
          const stale = yield* repository.resolve(Log.LOG_REF);
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(latecomer.publicKey),
              capabilities: ["hub.create-pr", "hub.review", "hub.approve", "hub.merge"],
              id: Log.newId(),
            }),
            [where.root],
          );

          // Their approval, signed against the head that carries their grant.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: latecomer,
          });

          // And their own `pr.opened`, signed against the *stale* head — where
          // they were not a member at all — appended on top of the approval,
          // whose trust head is what raises the floor that lets the loop take
          // it.
          const bytes = Event.encode({
            version: 1,
            type: "pr.opened",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: stale,
            title: "mine now",
            description: "",
            base: "refs/heads/main",
            head: Event.qualify(REVISION),
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(latecomer, bytes, NAMESPACE)],
              parents: [head!],
              message: "pr.opened stale\n",
            }),
          });

          const state = yield* projectionOf(where, pr);
          return { title: state.title, approvals: approvals(state).length };
        }),
      );

      assert.equal(outcome.title, "mine now", "the loop did accept the second opening");
      assert.equal(outcome.approvals, 0, "so its signer approves nothing");
    });

    it("cannot approve a revision it pushed itself", async () => {
      // `approvals` excluded the openers and the author, and never whoever set
      // the head being reviewed. A `hub.merge` holder could push a revision
      // onto somebody else's pull request and then approve it — self-approval
      // wearing another event's name.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          // The reviewer holds `hub.merge`, so retargeting is theirs to do.
          yield* PullRequest.update({
            repo: where.genesis.repoId,
            pr,
            head: NEXT,
            key: where.reviewer,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: NEXT,
            decision: "approve",
            key: where.reviewer,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(outcome.head, NEXT, "the revision under review is the one they pushed");
      assert.equal(approvals(outcome).length, 0, "and approving it is not review");
    });

    it("cannot become the author by folding first", async () => {
      // Which `pr.opened` opens a pull request is decided by descent, not by
      // fold order: every honest event descends from the genuine opening, and
      // a parentless forgery is an ancestor of nothing. Decided by fold order
      // instead, an attacker could grind a low oid, become the author, and
      // then close the pull request *as* the author — the freeze the author
      // comparison exists to prevent.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);

          const meddler = yield* generate("meddler@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(meddler.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          const bytes = Event.encode({
            version: 1,
            type: "pr.opened",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            title: "mine now",
            description: "",
            base: "refs/heads/elsewhere",
            head: Event.qualify(NEXT),
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(meddler, bytes, NAMESPACE)],
            parents: [],
            message: "pr.opened forged\n",
          });

          const joined = yield* Event.join(pr, [head!, forged]);
          yield* repository.setRef({ name: ref, to: joined });

          // The point of claiming authorship: closing the pull request, which
          // freezes the protected branch behind it.
          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: meddler });

          const trust = yield* projectTrust(where.genesis);
          return {
            first: forged < head! ? "forged" : "opened",
            state: yield* project(where.genesis, trust, pr),
          };
        }),
      );

      // Whichever of the two the fold reached first, the forgery buys nothing:
      // a contested opening establishes no author, so closing still needs
      // `hub.merge` and the branch behind the pull request stays reachable.
      assert.equal(
        outcome.state.state,
        "open",
        `the pull request must still be open (${outcome.first} folded first)`,
      );
      assert.match(outcome.state.rejected.at(-1)?.reason ?? "", /needs hub\.merge/);
    });

    it("cannot burn the opening's id by replaying it parentless", async () => {
      // The id slot used to be claimed before the event's *own* branch had
      // accepted it, so an event the switch then refused still took it. A
      // `source.push` holder could replay the pull request's signed
      // `pr.opened` as a parentless commit, have it sort first, claim the id,
      // be refused for not being the winning opening — and the genuine
      // opening was then dropped as the duplicate, leaving the pull request
      // with no base and no head and its protected branch unpushable, on a ref
      // that only grows.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();

          // Opened until its commit lands in the upper half of the oid space,
          // so grinding a *lower* replay below is a bounded search rather than
          // a coin that lands heads once in millions. The id is the opener's
          // to choose, which is the attacker's position too.
          let pr = "";
          let events: Event.Walked["events"] = [];
          let original: Event.Walked["events"][number] | undefined;
          for (let attempt = 0; ; attempt++) {
            ({ pr } = yield* PullRequest.open({
              repo: where.genesis.repoId,
              title: "Add a thing",
              description: "It does the thing.",
              base: "refs/heads/main",
              head: REVISION,
              key: where.author,
            }));
            ({ events } = yield* Event.entries(pr));
            original = events.find((entry) => entry.payload?.type === "pr.opened");
            if (original!.commit >= "8".padEnd(40, "0") || attempt >= 64) break;
          }

          // Activity descending from the genuine opening, so the replay loses
          // descent rather than winning it and supplying the same content it
          // copied. Losing is the whole point: the loser is refused, and the
          // question is whether its refusal still cost the author the id.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });

          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);

          // The opening's own bytes and signatures, committed again with no
          // history behind them, and ground below it. Nothing here is forged:
          // it is a replay, and the message is not part of what identifies it.
          let replay = original!.commit;
          for (let attempt = 0; attempt < 64 && replay >= original!.commit; attempt++) {
            replay = yield* Record.write({
              name: Event.RECORD,
              payload: original!.bytes,
              signatures: original!.signatures,
              parents: [],
              message: `pr.opened replayed ${attempt}\n`,
            });
          }

          yield* repository.setRef({ name: ref, to: yield* Event.join(pr, [head!, replay]) });
          return {
            ground: replay < original!.commit,
            state: yield* projectionOf(where, pr),
          };
        }),
      );

      assert.equal(outcome.ground, true, "the fixture must actually grind a lower oid");
      assert.equal(
        outcome.state.base,
        "refs/heads/main",
        "the opening must survive its own replay folding first",
      );
      assert.equal(outcome.state.head, REVISION);
      assert.equal(approvals(outcome.state).length, 1, "the approval must still count");
    });

    it("cannot blank the base and freeze the branch behind it", async () => {
      // Withholding *all* of a contested opening's content left `base` empty,
      // and `Policy.protectedBranch` compares `pullRequest.base` against the
      // ref being pushed — so an empty base matched nothing, the pull request
      // stopped counting towards its own branch's approvals, and a member
      // holding only `hub.create-pr` could freeze a protected branch by
      // pushing one parentless `pr.opened` at every review in flight. Content
      // now comes from whichever opening wins descent, which a forgery cannot
      // do against a pull request that has any activity at all.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          // Activity, so descent has something to decide with: every honest
          // event parents on the head, and the forgery is an ancestor of none.
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "looks right",
            key: where.reviewer,
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);

          const meddler = yield* generate("meddler@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(meddler.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          const bytes = Event.encode({
            version: 1,
            type: "pr.opened",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            title: "not yours any more",
            description: "",
            base: "refs/heads/elsewhere",
            head: Event.qualify(NEXT),
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(meddler, bytes, NAMESPACE)],
            parents: [],
            message: "pr.opened forged\n",
          });

          yield* repository.setRef({
            name: ref,
            to: yield* Event.join(pr, [head!, forged]),
          });
          return {
            state: yield* projectionOf(where, pr),
            author: yield* fingerprint(where.author.publicKey),
          };
        }),
      );

      assert.equal(
        outcome.state.base,
        "refs/heads/main",
        "the pull request must still name the branch it targets",
      );
      assert.equal(outcome.state.title, "Add a thing", "and keep the content it was opened with");
      // And its author keeps it. Contestation is for the case descent cannot
      // separate; here it plainly can, and treating a parentless graft as a
      // contest anyway cost the real author the ability to update, close or
      // reopen their own work — a denial by another route.
      assert.equal(outcome.state.author, outcome.author, "the forgery takes nothing from them");
    });

    it("cannot win descent by chaining filler commits under a graft", async () => {
      // Raw descendant count is manufactured, not earned: the count was taken
      // over the walked DAG, so the commits chained under a grafted opening
      // did not even have to carry an event. Winning handed the forger `base`,
      // and a pull request whose base no longer names its branch is one
      // `Policy.protectedBranch` skips — the branch behind an approved change,
      // frozen by a member holding nothing but `hub.create-pr`.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "looks right",
            key: where.reviewer,
          });
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);

          const meddler = yield* generate("meddler@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(meddler.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          const bytes = Event.encode({
            version: 1,
            type: "pr.opened",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            title: "not yours any more",
            description: "",
            base: "refs/heads/elsewhere",
            head: Event.qualify(NEXT),
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(meddler, bytes, NAMESPACE)],
            parents: [],
            message: "pr.opened forged\n",
          });

          // Ballast: commits carrying nothing at all, chained under the graft
          // so that it outnumbers the real conversation.
          const tree = yield* repository.writeTree([]);
          let filler = forged;
          for (let index = 0; index < 12; index++) {
            filler = yield* repository.commitTree({
              tree,
              parents: [filler],
              message: `filler ${index}\n`,
              author: Record.identityAt(new Date(1_700_000_000_000)),
            });
          }

          yield* repository.setRef({
            name: ref,
            to: yield* Event.join(pr, [head!, filler]),
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(
        outcome.base,
        "refs/heads/main",
        "padding must not take the base off the real opening",
      );
      assert.equal(outcome.title, "Add a thing");
    });

    it("gets no authority from there being no author yet", async () => {
      // `Dag.topological` orders parentless commits by oid, so grinding a low
      // one folds it before the `pr.opened` that establishes the author — and
      // a guard written as `author !== null && signer !== author` was inert
      // exactly there, letting a `hub.create-pr` holder close a pull request
      // permanently and freeze the protected branch behind it.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);

          const meddler = yield* generate("meddler@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(meddler.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // Parentless, so `Dag.topological` is free to order it before the
          // opening event — which it does whenever its oid sorts lower.
          // Asserted for *either* order rather than ground into the losing
          // one: the guard has to hold whether or not an author is known yet,
          // and a test that only ever exercised one ordering would be a test
          // of the grind.
          const bytes = Event.encode({
            version: 1,
            type: "pr.closed",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
          });
          const forged = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(meddler, bytes, NAMESPACE)],
            parents: [],
            message: "pr.closed forged\n",
          });

          const joined = yield* Event.join(pr, [head!, forged]);
          yield* repository.setRef({ name: ref, to: joined });

          const trust = yield* projectTrust(where.genesis);
          return {
            first: forged < head! ? "forged" : "opened",
            state: yield* project(where.genesis, trust, pr),
          };
        }),
      );

      assert.equal(
        outcome.state.state,
        "open",
        `the pull request must still be open (${outcome.first} folded first)`,
      );
      assert.match(outcome.state.rejected.at(-1)?.reason ?? "", /needs hub\.merge/);
    });
  });

  describe("resolving a review thread", () => {
    it("is refused to somebody who neither opened it nor may review", async () => {
      // Resolving is what satisfies `requireResolvedThreads`, so leaving it to
      // any `hub.comment` holder let one clear somebody else's blocking thread
      // — or reopen a settled one to block a merge indefinitely.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "this needs work",
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const thread = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          const talker = yield* generate("talker@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(talker.publicKey),
              capabilities: ["hub.comment"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );

          yield* PullRequest.resolve({ repo: where.genesis.repoId, pr, thread, key: talker });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.threads[0]?.resolved, false, "somebody else's thread stays open");
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /needs hub\.review/);
    });

    it("is allowed to the member who opened it", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "a thought",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const thread = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          yield* PullRequest.resolve({
            repo: where.genesis.repoId,
            pr,
            thread,
            key: where.author,
          });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.threads[0]?.resolved, true);
      assert.deepEqual(outcome.rejected, []);
    });
  });

  describe("retargeting a pull request", () => {
    it("is refused to somebody who is neither its author nor a merger", async () => {
      // Moving the head stales every approval of the revision it replaces, so
      // charging it `hub.create-pr` let any hub writer retarget somebody
      // else's approved pull request and block the protected branch that pull
      // request was the only route to.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });

          const meddler = yield* generate("meddler@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(meddler.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );

          yield* PullRequest.update({
            repo: where.genesis.repoId,
            pr,
            head: NEXT,
            key: meddler,
          });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.head, REVISION, "the head must not have moved");
      assert.equal(approvals(outcome).length, 1, "and the approval must still count");
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /needs hub\.merge/);
    });
  });

  describe("an approval", () => {
    it("does not count when it is the pull request author's own", async () => {
      // Self-approval is not review — it is the thing review exists to be
      // independent of. Counted, one member holding `hub.approve` opened a
      // pull request for their own commit, approved it, and cleared
      // `requiredApprovals` on a protected branch alone.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "mine",
            base: "refs/heads/main",
            head: REVISION,
            key: where.reviewer,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      // The review is on the record — it is a true statement about what its
      // author thinks — it simply satisfies no requirement.
      assert.equal(outcome.reviews.length, 1);
      assert.equal(approvals(outcome).length, 0);
      assert.deepEqual(outcome.rejected, []);
    });

    it("cannot be dismissed by somebody who could not have made it", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const review =
            events.find((entry) => entry.payload?.type === "review.submitted")?.payload?.id ?? "";

          // The author holds `hub.review` but not `hub.approve`.
          yield* PullRequest.dismissReview({
            repo: where.genesis.repoId,
            pr,
            review,
            reason: "no",
            key: where.author,
          });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(approvals(outcome).length, 1, "the approval must still count");
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /needs hub\.approve/);
    });
  });

  describe("closing a pull request", () => {
    it("is refused to somebody who is neither its author nor a merger", async () => {
      // `hub.create-pr` is the lowest-privileged hub capability, and charging
      // closing to it let anyone holding it close somebody else's approved
      // pull request — after which `protectedBranch` skips it and the branch
      // it was approved for cannot be moved at all.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);

          const meddler = yield* generate("meddler@example.com");
          yield* Effect.flatMap(
            Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(meddler.publicKey),
              capabilities: ["hub.create-pr"],
              id: Log.newId(),
            }),
            (payload) => Log.issue(payload, [where.root]),
          );

          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: meddler });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.state, "open", "somebody else's pull request stays open");
      assert.match(outcome.rejected.at(-1)?.reason ?? "", /needs hub\.merge/);
    });

    it("is allowed to its own author", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: where.author });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.state, "closed");
      assert.deepEqual(outcome.rejected, []);
    });

    it("is allowed to somebody who may merge it", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          // The reviewer holds `hub.merge`.
          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: where.reviewer });
          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      assert.equal(outcome.state, "closed");
      assert.deepEqual(outcome.rejected, []);
    });
  });

  describe("a review dismissal", () => {
    it("drops only the review its author claimed", async () => {
      // `review.dismissed` resolved by bare event id while ids are scoped to
      // their author, so one dismissal could drop two authors' reviews — and
      // `hub.review` alone sufficed to nullify an `hub.approve` holder's
      // approval by re-using its id and dismissing that.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const ref = Event.refOf(pr);
          const root = yield* repository.resolve(ref);

          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "looks right",
            key: where.reviewer,
          });
          const approval = yield* repository.resolve(ref);
          const { events } = yield* Event.entries(pr);
          const id = events.find((entry) => entry.commit === approval)?.payload?.id ?? "";
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // The author holds `hub.review`, so their own review is authorized —
          // and it re-uses the approval's id.
          const bytes = Event.encode({
            version: 1,
            type: "review.submitted",
            repo: where.genesis.repoId,
            pr,
            id,
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            head: Event.qualify(REVISION),
            decision: "comment",
            body: "a decoy",
          });
          const decoy = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(where.author, bytes, NAMESPACE)],
            parents: [root!],
            message: `review.submitted ${id}\n`,
          });
          const joined = yield* Event.join(pr, [approval!, decoy]);
          yield* repository.setRef({ name: ref, to: joined });

          // Dismissing by that id now names two reviews, so it names neither.
          yield* PullRequest.dismissReview({
            repo: where.genesis.repoId,
            pr,
            review: id,
            reason: "no longer relevant",
            key: where.author,
          });

          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      const approval = outcome.reviews.find((review) => review.decision === "approve");
      assert.notEqual(approval, undefined, "the approval must still be there");
      assert.equal(approval?.dismissed, false, "and must not have been dismissed by proxy");
      assert.match(outcome.rejected.map((entry) => entry.reason).join(" "), /ambiguous review/);
    });
  });

  describe("a tombstone", () => {
    it("reaches only the event its author claimed, not every event sharing an id", async () => {
      // Ids are scoped to their author everywhere else in the fold, and a
      // tombstone resolved by bare id walked straight around that: a member
      // holding only `hub.comment` posts a comment re-using an approval's id,
      // redacts their *own* comment, and the approval's payload goes with it —
      // blob deleted, event unreadable, approval gone.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const ref = Event.refOf(pr);
          const root = yield* repository.resolve(ref);

          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "looks right",
            key: where.reviewer,
          });
          const approval = yield* repository.resolve(ref);
          const { events } = yield* Event.entries(pr);
          const id = events.find((entry) => entry.commit === approval)?.payload?.id ?? "";
          const trustHead = yield* repository.resolve(Log.LOG_REF);

          // The author's comment re-uses the approval's id.
          const bytes = Event.encode({
            version: 1,
            type: "comment.created",
            repo: where.genesis.repoId,
            pr,
            id,
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead,
            body: "a decoy",
            head: null,
            path: null,
            side: null,
            line: null,
            contextHash: null,
          });
          const decoy = yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(where.author, bytes, NAMESPACE)],
            parents: [root!],
            message: `comment.created ${id}\n`,
          });
          const joined = yield* Event.join(pr, [approval!, decoy]);
          yield* repository.setRef({ name: ref, to: joined });

          // The reviewer holds `hub.redact` and names that id. Two events
          // answer to it, so the tombstone identifies neither — and `redact`
          // says so rather than writing one and hoping.
          const failure = yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target: id,
            reason: "sensitive-content",
            key: where.reviewer,
          }).pipe(Effect.flip);

          const trust = yield* projectTrust(where.genesis);
          return { failure, state: yield* project(where.genesis, trust, pr) };
        }),
      );

      assert.equal(outcome.failure._tag, "Invalid");
      assert.match(outcome.failure.reason, /2 events claiming/);
      assert.equal(outcome.state.reviews.length, 1, "the approval must survive");
      assert.equal(outcome.state.reviews[0]?.body, "looks right", "and keep its content");
      assert.equal(outcome.state.redacted.size, 0, "an ambiguous target redacts nothing");
    });
  });

  describe("redaction", () => {
    it("removes the content and keeps the event's place in the chain", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "here is a password: hunter2",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          const before = yield* projectionOf(where, pr);
          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "sensitive-content",
            key: where.reviewer,
          });
          // Collected: a redaction's bytes go when the pack is next rewritten,
          // which is the one place that can tell whether anything else still
          // reaches them.
          const repository = yield* Repository;
          yield* repository.gc({ repack: true, exclude: yield* Redaction.excluded() });
          const after = yield* projectionOf(where, pr);
          const { events: walked } = yield* Event.entries(pr);

          return { before, after, walked, commit };
        }),
      );

      assert.equal(outcome.before.threads[0]?.comments[0]?.body, "here is a password: hunter2");
      // The content is gone from the projection…
      assert.equal(outcome.after.threads.length, 0);
      // …and the commit is still in the history, because every later event's
      // hash depends on it.
      assert.ok(
        outcome.walked.some((entry) => entry.commit === outcome.commit),
        "the redacted event must keep its place in the chain",
      );
      const redactedEntry = outcome.walked.find((entry) => entry.commit === outcome.commit);
      assert.equal(redactedEntry?.payload, null, "its content must be gone");
      assert.equal(redactedEntry?.summary?.type, "comment.created", "what it was survives");
    });

    it("reads the same on a replica that still holds the payload", async () => {
      // The determinism that matters most here, and the one that was missing.
      // The host that performs a redaction deletes the payload at once; every
      // replica keeps it until the tombstone reaches them and their next
      // repack. Folded on whether the *bytes* are present, those two hosts
      // reached different verdicts about the same pull request — a redacted
      // approval counting on one and not the other, a redacted comment leaving
      // a thread on one and none on the other — and the disagreement lands on
      // the policy boundary, which decides whether a push is allowed. Absence
      // is decided by the tombstone, so the answer is the same either way.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "the deploy key is hunter2",
            key: where.author,
            head: REVISION,
            path: "secrets/deploy.key",
            side: "new",
            line: 12,
          });
          // An approval too, since what a redaction must not silently change
          // is a decision the merge policy reads.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            body: "fine",
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const entry = events.find((event) => event.commit === commit);

          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target: entry?.payload?.id ?? "",
            reason: "sensitive-content",
            key: where.reviewer,
          });
          const host = yield* projectionOf(where, pr);

          // And now a replica of the same history that still holds the blob.
          yield* repository.writeBlob(entry!.bytes);
          return { host, replica: yield* projectionOf(where, pr) };
        }),
      );

      assert.equal(outcome.host.threads.length, 0, "the redacted comment is gone");
      assert.deepEqual(
        outcome.replica.threads,
        outcome.host.threads,
        "and a replica holding the payload must read it the same way",
      );
      assert.equal(approvals(outcome.host).length, 1, "an untouched approval still counts");
      assert.equal(approvals(outcome.replica).length, 1, "on both");
      assert.deepEqual(outcome.replica.redacted, outcome.host.redacted);
    });

    it("decides the opening by the tombstone too, not by the bytes", async () => {
      // The opening pass is where `author` and `openers` are settled, and
      // `approvals` excludes every opener as self-approval. Left out of the
      // redaction decision, that pass read the payload bytes directly: the
      // host that performed a redaction saw no opening and no author, and a
      // replica that still held the blob saw both — so the same approval
      // counted on one and not the other, and the two gave opposite verdicts
      // on the same protected-branch push.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "Add a thing",
            base: "refs/heads/main",
            head: REVISION,
            key: where.reviewer,
          });
          const { events } = yield* Event.entries(pr);
          const entry = events.at(0);

          // The opener redacts their own opening, and then approves.
          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target: entry?.payload?.id ?? "",
            reason: "sensitive-content",
            key: where.reviewer,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: REVISION,
            decision: "approve",
            key: where.reviewer,
          });

          const host = yield* projectionOf(where, pr);
          yield* repository.writeBlob(entry!.bytes);
          return { host, replica: yield* projectionOf(where, pr) };
        }),
      );

      assert.deepEqual(
        [...outcome.replica.openers],
        [...outcome.host.openers],
        "who opened it must not depend on which host is asking",
      );
      assert.equal(outcome.replica.author, outcome.host.author);
      assert.equal(approvals(outcome.replica).length, approvals(outcome.host).length);
      // And the content the winning opening supplies, which is what
      // `Policy.protectedBranch` matches a pull request to its branch by. The
      // contested-opening ranking counts events and their distinct member
      // signers, so an event one host reads and the other does not is a vote
      // one host counts and the other does not.
      assert.equal(outcome.replica.base, outcome.host.base);
      assert.equal(outcome.replica.title, outcome.host.title);
    });

    it("cannot be undone by a junk tombstone naming the tombstone", async () => {
      // The set of commits the first pass reads as absent is built before any
      // signature is checked — it has to be, since it is what makes two hosts
      // agree about which payloads they can still read. Built without asking
      // whether the *target* is itself a tombstone, it handed any member who
      // can append a hub event a way to undo somebody else's authorized
      // redaction: name the valid tombstone, watch the first pass skip it, and
      // the fold reports nothing redacted at all. `gc` then re-protects the
      // payload the operator believes is gone, and on the host that already
      // deleted its loose copy the fetch retry gets an empty exclusion set and
      // every clone of the repository fails.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "here is a password: hunter2",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          const tombstone = yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "sensitive-content",
            key: where.reviewer,
          });
          const before = yield* projectionOf(where, pr);

          // A member with nothing but `hub.comment` names the tombstone.
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "event.redacted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            target: "whatever",
            targetCommit: Event.qualify(tombstone),
            reason: "no",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "event.redacted over a tombstone\n",
            }),
            expected: head,
          });

          return { before, after: yield* projectionOf(where, pr) };
        }),
      );

      assert.equal(outcome.before.redacted.size, 1, "the fixture must actually redact something");
      assert.deepEqual(
        [...outcome.after.redacted],
        [...outcome.before.redacted],
        "a tombstone over a tombstone must not put the payload back in circulation",
      );
    });

    it("does not let a stranger's tombstone decide what the first pass reads", async () => {
      // The set of commits the first pass reads as absent has to be built
      // before any capability is checked — that is what makes two hosts agree
      // about which payloads they can still read — but "anybody who can append
      // a hub event" is too wide a door: a skipped event drops out of the
      // trust floor, so naming a few of them lowers it for the next tombstone.
      // Membership is a fact both hosts fold identically, so it is required.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "ordinary",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          // Signed by a key this repository never granted anything to.
          const stranger = yield* generate("stranger@example.com");
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "event.redacted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            target,
            targetCommit: Event.qualify(commit),
            reason: "no",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(stranger, bytes, NAMESPACE)],
              parents: [head!],
              message: "event.redacted from nobody\n",
            }),
            expected: head,
          });

          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(outcome.redacted.size, 0, "a stranger's tombstone redacts nothing");
      // And the comment it named is still folded, rather than read as absent.
      assert.equal(outcome.threads[0]?.comments[0]?.body, "ordinary");
    });

    it("keeps deciding it the same way once the redactor has been revoked", async () => {
      // The set the first pass reads as absent has to be *monotone*: once a
      // tombstone names a target, some host may already have deleted the
      // payload, and an answer that later shrinks leaves that host folding a
      // history no replica agrees with — the divergence the two passes exist
      // to remove. So the question is whether the signer *ever* held
      // `hub.redact`, over a grant history that only grows.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "here is a password: hunter2",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "sensitive-content",
            key: where.reviewer,
          });
          const before = yield* projectionOf(where, pr);

          // And now the redactor is revoked outright.
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "left",
              id: Log.newId(),
            }),
            [where.root],
          );

          return { before, after: yield* projectionOf(where, pr) };
        }),
      );

      assert.equal(outcome.before.threads.length, 0, "the comment was read as absent");
      assert.equal(
        outcome.after.threads.length,
        outcome.before.threads.length,
        "and still is, whatever has since become of the key that said so",
      );
    });

    it("keeps honouring a tombstone after an ancestor's grant lapses", async () => {
      // A tombstone's verdict is permanent because honouring it deletes bytes.
      // But it is judged against the *floor*, and the floor was built only
      // from events that were authorized under the ordinary reading — which
      // consults the wall clock. So an ancestor whose author's grant expired
      // silently dropped out of the floor, the floor fell back to the
      // tombstone's own older declared head, and a redaction this repository
      // had already acted on became unauthorized: `redacted` stopped naming
      // the blob, `gc` went back to protecting a payload the operator had been
      // told was gone, and the host that deleted it folded a history no
      // replica agreed with. No attacker is needed — only a grant with an
      // expiry and the passage of time.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;

          const root = yield* generate("root@example.com");
          const author = yield* generate("author@example.com");
          const lapsed = yield* generate("lapsed@example.com");
          const redactor = yield* generate("redactor@example.com");

          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);

          const grant = (key: PrivateKey, capabilities: ReadonlyArray<string>, expiresAt?: Date) =>
            Effect.flatMap(
              Certificate.grant({
                repo: genesis.repoId,
                publicKey: formatPublicKey(key.publicKey),
                capabilities,
                expiresAt: expiresAt ?? null,
                id: Log.newId(),
              }),
              (payload) => Log.issue(payload, [root]),
            );

          yield* grant(author, ["hub.create-pr", "hub.comment"]);
          // Granted with an expiry that has already passed: the fold below is
          // the one that happens "next month".
          yield* grant(lapsed, ["hub.comment"], new Date(1_700_000_500_000));

          const where = { genesis, root, author, reviewer: redactor } satisfies World;
          const { pr } = yield* PullRequest.open({
            repo: genesis.repoId,
            title: "Add a thing",
            base: "refs/heads/main",
            head: REVISION,
            key: author,
          });
          // What the tombstone names, and the head it will declare: the log as
          // it stood before `hub.redact` was granted at all.
          const stale = yield* repository.resolve(Log.LOG_REF);
          const commit = yield* PullRequest.comment({
            repo: genesis.repoId,
            pr,
            body: "ordinary",
            key: author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          yield* grant(redactor, ["hub.redact"]);
          // The ancestor that carries the floor forward — and the one whose
          // author's grant has lapsed.
          yield* PullRequest.comment({
            repo: genesis.repoId,
            pr,
            body: "carries the floor",
            key: lapsed,
          });

          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "event.redacted",
            repo: genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            // Behind the grant of `hub.redact`: only the floor lifts it.
            trustHead: stale,
            target,
            targetCommit: Event.qualify(commit),
            reason: "gone",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(redactor, bytes, NAMESPACE)],
              parents: [head!],
              message: "event.redacted under a stale head\n",
            }),
            expected: head,
          });

          const state = yield* projectionOf(where, pr);
          return { redacted: state.redacted.size, threads: state.threads.length };
        }),
      );

      assert.equal(outcome.redacted, 1, "the redaction this host may already have acted on stands");
    });

    it("does not let a member without hub.redact decide it either", async () => {
      // Membership alone was not enough. A skipped event drops out of `heads`,
      // so a member could push decoy tombstones naming events that had named
      // late trust heads, lower the floor for the *next* tombstone, and have
      // one signed under a stale head accepted — which is how a `hub.redact`
      // that had been narrowed away would come back. The capability is what
      // the decoys were being used to recover, so it is what they cost.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "ordinary",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          // The author holds `hub.comment` and `hub.review`, never `hub.redact`.
          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "event.redacted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            target,
            targetCommit: Event.qualify(commit),
            reason: "no",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "event.redacted without the capability\n",
            }),
            expected: head,
          });

          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(outcome.redacted.size, 0);
      assert.equal(outcome.threads[0]?.comments[0]?.body, "ordinary");
    });

    it("refuses a redaction from a member without hub.redact, and writes nothing", async () => {
      // Writing the tombstone and deleting the payload are two different
      // authorities. Treating the first as implying the second let anybody who
      // could write a hub ref blank another member's words: the projection
      // refused their tombstone, so nothing was marked redacted — but the blob
      // was already gone, and the event had become unreadable and stopped
      // counting.
      //
      // Refused before anything is written, too. Rebuilding the projection and
      // reporting the refusal afterwards still left the tombstone on an
      // append-only ref forever, and `Redaction` folds a pull request on every
      // collection and every retried fetch once any `event.redacted` payload
      // is present — so one refused command made every future collection pay.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "ordinary",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          const failure = yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "no",
            key: where.author,
          }).pipe(Effect.flip);

          return { failure, state: yield* projectionOf(where, pr) };
        }),
      );

      assert.equal(outcome.failure._tag, "Invalid");
      assert.match(outcome.failure.reason, /hub\.redact/);
      assert.deepEqual(outcome.state.rejected, [], "nothing was appended to be rejected");
      assert.equal(outcome.state.redacted.size, 0);
      // The words the tombstone had no authority to remove are still there.
      assert.equal(outcome.state.threads[0]?.comments[0]?.body, "ordinary");
    });

    it("refuses one pushed straight at the ref, which is what a replica sees", async () => {
      // The command refuses before writing; the fold has to refuse too, since
      // a replica receives the event and never the command that made it.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "ordinary",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          const ref = Event.refOf(pr);
          const head = yield* repository.resolve(ref);
          const bytes = Event.encode({
            version: 1,
            type: "event.redacted",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date(1_700_000_000_000).toISOString(),
            trustHead: yield* repository.resolve(Log.LOG_REF),
            target,
            targetCommit: Event.qualify(commit),
            reason: "no",
          });
          yield* repository.setRef({
            name: ref,
            to: yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(where.author, bytes, NAMESPACE)],
              parents: [head!],
              message: "event.redacted unauthorized\n",
            }),
            expected: head,
          });

          return yield* projectionOf(where, pr);
        }),
      );

      assert.match(outcome.rejected.at(-1)?.reason ?? "", /hub\.redact/);
      assert.equal(outcome.redacted.size, 0);
      assert.equal(outcome.threads[0]?.comments[0]?.body, "ordinary");
    });

    it("will not redact a tombstone", async () => {
      const failure = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "x",
            key: where.author,
          });
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === commit)?.payload?.id ?? "";

          const tombstone = yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "first",
            key: where.reviewer,
          });
          const { events: after } = yield* Event.entries(pr);
          const second = after.find((entry) => entry.commit === tombstone)?.payload?.id ?? "";

          return yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target: second,
            reason: "second",
            key: where.reviewer,
          }).pipe(Effect.flip);
        }),
      );
      // The failure is the domain one, not a storage error that happened to
      // surface: `Invalid` is what a refused redaction target produces.
      assert.equal(failure._tag, "Invalid");
      assert.match(failure._tag === "Invalid" ? failure.reason : "", /tombstone/);
    });
  });

  describe("integrity", () => {
    it("ignores an event written for another repository", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);

          // A different root key, so the genesis bytes — and therefore the
          // RepoID — genuinely differ. Reusing this repository's root would
          // produce an identical document and the same identity.
          const stranger = yield* generate("stranger-root@example.com");
          const elsewhere = yield* create([formatPublicKey(stranger.publicKey)], 1);
          yield* Event.issue(
            {
              version: 1,
              type: "pr.closed",
              repo: elsewhere.repoId,
              pr,
              id: Event.newId(),
              issuedAt: new Date().toISOString(),
              trustHead: null,
            },
            where.reviewer,
          );
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.state, "open");
      assert.match(state.rejected.at(-1)?.reason ?? "", /is for SHA256:/);
    });

    it("ignores an unsigned event", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);

          const payload = {
            version: 1,
            type: "pr.closed",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date().toISOString(),
            trustHead: null,
          } as const;
          yield* Event.append(payload, Event.encode(payload), []);
          return yield* projectionOf(where, pr);
        }),
      );
      assert.equal(state.state, "open");
    });

    it("refuses a revision that is not hash-qualified", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const payload = {
            version: 1,
            type: "pr.updated",
            repo: where.genesis.repoId,
            pr,
            id: Event.newId(),
            issuedAt: new Date().toISOString(),
            trustHead: null,
            // Bare, the way a payload would have carried one before object
            // formats had to be told apart.
            head: NEXT,
          } as const;
          yield* Event.issue(payload, where.author);
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.head, REVISION);
      assert.match(state.rejected.at(-1)?.reason ?? "", /hash-qualified/);
    });

    it("keeps folding after an event it had to reject", async () => {
      const state = await scenario(
        Effect.gen(function* () {
          const where = yield* world();
          const { pr } = yield* opened(where);
          const stranger = yield* generate("stranger@example.com");

          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "from nobody",
            key: stranger,
          });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "from a member",
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(state.threads.length, 1, "one bad event must not lose the good ones");
      assert.equal(state.rejected.length, 1);
    });
  });

  it("lists the pull requests a repository holds", async () => {
    const ids = await scenario(
      Effect.gen(function* () {
        const where = yield* world();
        const first = yield* opened(where);
        const second = yield* opened(where);
        const listed = yield* Event.pullRequests();
        return { listed, expected: [first.pr, second.pr].sort() };
      }),
    );
    assert.deepEqual([...ids.listed].sort(), ids.expected);
  });
});

describe("what a compare-and-swap compares against", () => {
  it("appends against what the hub ref holds, not what it resolves to", async () => {
    // `resolve` follows a symbolic ref to an oid; `readRef` reports the oid
    // the ref itself holds, and a symbolic ref holds none. Every writer here
    // uses one value as both the parent of the record it writes *and* the
    // expected value of the swap that publishes it — and the store compares
    // the swap against what the ref holds. Taken from `resolve`, the two are
    // the same value only for as long as no ref in these namespaces is
    // symbolic; the moment one is, every append conflicts against a value
    // nobody wrote, on a namespace with no way back, and the retry that
    // exists for concurrent authors spends itself three times over on a
    // conflict no retry can clear.
    //
    // Nothing makes a hub ref symbolic today, which is exactly why this is
    // asked of a repository that reports one rather than of a fixture that
    // has one: the writers must not be the reason it stays that way.
    const symbolic = Layer.effect(
      Repository,
      Effect.gen(function* () {
        const inner = yield* Repository;
        return Repository.of({
          ...inner,
          resolve: (name) =>
            name.startsWith("refs/hub/") ? Effect.succeed(EMPTY_TREE_OID) : inner.resolve(name),
        });
      }),
    ).pipe(
      Layer.provideMerge(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    );

    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const where = yield* world();
        const say = (type: "pr.closed" | "pr.reopened") => {
          const payload = {
            version: 1,
            type,
            repo: where.genesis.repoId,
            pr: "1",
            id: Event.newId(),
            issuedAt: new Date().toISOString(),
            trustHead: null,
          } as const;
          return Event.append(payload, Event.encode(payload), []);
        };
        const first = yield* say("pr.closed");
        const second = yield* say("pr.reopened");
        const repository = yield* Repository;
        return { first, second, held: yield* repository.readRef("refs/hub/pr/1") };
      }).pipe(Effect.provide(symbolic), Effect.exit),
    );

    assert.ok(
      Exit.isSuccess(outcome),
      `appending must not swap against a resolved value: ${JSON.stringify(outcome)}`,
    );
    assert.equal(outcome.value.held, outcome.value.second, "and the second append is what stands");
  });
});
