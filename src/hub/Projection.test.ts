import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import {
  fingerprint,
  formatPublicKey,
  generate,
  NAMESPACE,
  type PrivateKey,
  sign,
} from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Record from "../trust/Record.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Event from "./Event.ts";
import { approvals, checksPassed, project } from "./Projection.ts";
import * as PullRequest from "./PullRequest.ts";

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
          return yield* projectionOf(where, pr);
        }),
      );

      assert.equal(
        outcome.base,
        "refs/heads/main",
        "the pull request must still name the branch it targets",
      );
      assert.equal(outcome.title, "Add a thing", "and keep the content it was opened with");
      // The part a contested opening still does not confer.
      assert.equal(outcome.author, null, "and confer no authorship on the forgery");
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

    it("removes where an inline comment pointed, not only what it said", async () => {
      // On any replica that still holds the blob — which is the case redaction
      // exists for — a redacted inline comment that still names a file and a
      // line says most of what it said.
      const outcome = await scenario(
        Effect.gen(function* () {
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
          const { events } = yield* Event.entries(pr);
          const entry = events.find((event) => event.commit === commit);
          const target = entry?.payload?.id ?? "";

          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "sensitive-content",
            key: where.reviewer,
          });

          // A replica that still holds the payload. This is the case the
          // blanking exists for: locally the blob is gone and the event simply
          // reads as absent, but everywhere the tombstone has not yet been
          // acted on the content is right there to be projected.
          const repository = yield* Repository;
          yield* repository.writeBlob(entry!.bytes);

          const trust = yield* projectTrust(where.genesis);
          return yield* project(where.genesis, trust, pr);
        }),
      );

      const thread = outcome.threads.at(0);
      assert.notEqual(thread, undefined, "the thread keeps its place in the history");
      assert.equal(thread?.comments[0]?.body, "");
      assert.equal(thread?.comments[0]?.redacted, true);
      assert.equal(thread?.path, null, "the file it pointed at is content too");
      assert.equal(thread?.side, null);
      assert.equal(thread?.line, null);
      assert.equal(thread?.head, null);
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
