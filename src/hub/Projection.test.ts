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
          let forged: Oid | null = null;
          for (let attempt = 0; attempt < 64 && forged === null; attempt++) {
            const bytes = Event.encode({
              version: 1,
              type: "review.submitted",
              repo: where.genesis.repoId,
              pr,
              id,
              issuedAt: new Date(1_700_000_000_000 + attempt * 1000).toISOString(),
              trustHead,
              head: Event.qualify(REVISION),
              decision: "approve",
              body: "not mine to give",
            });
            const candidate = yield* Record.write({
              name: Event.RECORD,
              payload: bytes,
              signatures: [yield* sign(mallory, bytes, NAMESPACE)],
              parents: [root!],
              message: `review.submitted ${id}\n`,
            });
            if (candidate < approval!) forged = candidate;
          }

          // Both sides in one history, as a replica that fetched them would
          // have. The join is where the two claims meet.
          const joined = yield* Event.join(pr, [approval!, forged!]);
          yield* repository.setRef({ name: ref, to: joined });

          const trust = yield* projectTrust(where.genesis);
          return { state: yield* project(where.genesis, trust, pr), forged, approval };
        }),
      );

      assert.notEqual(outcome.forged, null, "the grind must find a lower oid to be a real test");
      assert.equal(outcome.state.reviews.length, 1, "the authorized approval must survive");
      assert.equal(outcome.state.reviews[0]?.decision, "approve");
      // And the forgery is refused on its own merits, by name.
      assert.match(outcome.state.rejected.map((entry) => entry.reason).join(" "), /hub\.approve/);
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

    it("refuses a redaction from a member without hub.redact", async () => {
      const state = await scenario(
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

          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "no",
            key: where.author,
          });
          return yield* projectionOf(where, pr);
        }),
      );

      // The tombstone was written but carries no authority, so the content
      // stays — the blob is deleted either way, which is why the projection
      // has to treat an unauthorized tombstone as the removal it physically is.
      assert.match(state.rejected.at(-1)?.reason ?? "", /hub\.redact/);
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
