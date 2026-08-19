/**
 * `git+ queue …` — landing several approved pull requests as one tested batch.
 *
 * The verbs below `run` are bookkeeping: they append the signed records that
 * let several agents cooperate on one branch (`hub/Queue.ts`). `run` is the
 * work, and its shape is `wake`'s: a pull-based pass that re-derives everything
 * from refs, so a hook can call it, and so can a timer, and so can a person. It
 * holds no lease and keeps no bookmark — a run that dies leaves the refs it had
 * already written and the next run continues from them.
 *
 * The one thing it never does is decide what may land. Every candidate it
 * builds is put to `Policy.evaluate` — the same judge a push meets — and landed
 * only if that says yes, under the compare-and-swap the judgement was made
 * against. So a runner is trusted with nothing: it proposes, and the boundary
 * disposes. That is also what makes two runners safe to have at once, and none
 * at all safe too.
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Event from "../hub/Event.ts";
import * as HubProjection from "../hub/Projection.ts";
import * as Queue from "../hub/Queue.ts";
import { fingerprint, type PrivateKey } from "../crypto/SshSignature.ts";
import * as Policy from "../server/Policy.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Verify from "../trust/Verify.ts";
import { readPrivateKey, repoArgument, rootFlag, withRepo } from "./shared.ts";

const identityOf = Effect.fn("queue.identityOf")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis; run \`git+ hub init ${repo} --key <key>\` first`,
    });
  }
  return stored.genesis;
});

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);

const queueArgument = Argument.string("queue");

/**
 * A target as somebody types it.
 *
 * `pr open --base main` takes a bare branch name and reads it as
 * `refs/heads/main`, and a sibling command that refused the same spelling
 * would be the only one here that did. The record itself still stores the full
 * name, because the protected-branch rules match on the ref being written and a
 * bare one matches nothing (`hub/Queue.ts`); this is where the two meet.
 */
const targetRef = (value: string): string => (value === "" ? value : Event.branchRef(value));

/**
 * Which queue a command is about.
 *
 * A queue is identified by what it is *for* far more often than by its id — an
 * agent woken by a push knows the branch, not the uuid — so `--target` is the
 * ordinary way in and the id is the exact one.
 */
const resolve = Effect.fn("queue.resolve")(function* (input: {
  readonly queue: string;
  readonly target: string;
}) {
  if (input.queue !== "") {
    const state = yield* Queue.project(input.queue);
    // Both named and disagreeing is a caller that thinks it is talking about
    // one branch while this talks about another — a drifted hook appending
    // records to, or landing on, a branch its invocation never named. Refused
    // rather than resolved by precedence, because either precedence is silently
    // wrong for somebody.
    if (input.target !== "" && state.target !== targetRef(input.target)) {
      return yield* new Invalid({
        field: "target",
        reason: `${input.queue} serves ${state.target ?? "no branch"}, and --target names ${targetRef(input.target)}`,
      });
    }
    // A queue nobody opened is a queue nothing reads. Refused here because
    // appending anyway would *create* `refs/hub/queue/<typo>` — on a namespace
    // that cannot be deleted, holding records the projection ignores for ever,
    // and reported as success. A mistyped id must cost an error message rather
    // than a permanent entry in every ref listing this repository ever serves.
    if (!state.exists) {
      return yield* new Invalid({
        field: "queue",
        reason: `this repository holds no queue ${input.queue}; open one with \`git+ queue open\``,
      });
    }
    // And a queue that has ended reads nothing further. Refused for the same
    // reason a mistyped id is: `forTarget` hides a closed queue and `run`
    // refuses one, so an append here is a permanent record nothing will ever
    // act on — a pull request entered into it would simply never land, and the
    // command would have said it was entered.
    if (state.closed !== null) {
      return yield* new Invalid({
        field: "queue",
        reason: `${input.queue} was closed (${state.closed}); open a fresh queue for its branch`,
      });
    }
    return state;
  }
  if (input.target === "") {
    return yield* new Invalid({
      field: "queue",
      reason: "name a queue with --queue <id> or the branch it serves with --target <ref>",
    });
  }
  const { found } = yield* Queue.forTarget(targetRef(input.target));
  if (found === null) {
    return yield* new Invalid({
      field: "target",
      reason: `this repository holds no queue for ${input.target}`,
    });
  }
  return found;
});

const open = Command.make(
  "open",
  {
    root: rootFlag,
    key: keyFlag,
    target: Flag.string("target").pipe(
      Flag.withDescription("The branch this queue lands on, as a full ref name"),
    ),
    repo: repoArgument,
  },
  ({ key, repo, root, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const queue = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          // One queue per target, refused here because it cannot be undone
          // there: `refs/hub/queue/*` is append-only, so a second queue for one
          // branch is a permanent split — `forTarget` picks between them by
          // sorted id, entries divide invisibly across the two, and two runners
          // delete each other's candidate branches.
          //
          // It catches the mistake and not the race: two `open` calls at once
          // both see nothing and both write. There is no compare-and-swap
          // across refs to have instead, and the same is true of a task claim
          // — saying so is better than implying a guarantee this cannot give.
          const existing = yield* Queue.forTarget(targetRef(target));
          if (existing.found !== null) {
            return yield* new Invalid({
              field: "target",
              reason: `${targetRef(target)} already has a queue: ${existing.found.queue}`,
            });
          }
          // A queue this replica cannot project could be for this branch, and
          // there is no way to find out from here. Said rather than refused:
          // `refs/hub/queue/*` cannot be deleted, so refusing would let one
          // unreadable queue — on any branch — block opening a queue for every
          // other branch, for good, which is a worse permanent state than the
          // one it guards against. And the duplicate it guards against is
          // largely inert: `forTarget` picks the first match in sorted id order
          // on every replica, so a second queue is a ref nobody consults unless
          // they name its id.
          if (existing.unreadable.length > 0) {
            yield* Console.error(
              `warning: ${existing.unreadable.join(", ")} cannot be read here, so this cannot tell whether one of them already serves ${targetRef(target)}`,
            );
          }
          const opened = yield* Queue.open({
            repo: (yield* identityOf(repo)).repoId,
            target: targetRef(target),
            key: signer,
          });
          return opened.queue;
        }),
      );
      // The id alone, so a hook can hand it to whatever it starts.
      yield* Console.log(queue);
    }),
);

const enter = Command.make(
  "enter",
  {
    root: rootFlag,
    key: keyFlag,
    queue: Flag.string("queue").pipe(Flag.withDefault("")),
    target: Flag.string("target").pipe(Flag.withDefault("")),
    repo: repoArgument,
    pr: Argument.string("pr"),
  },
  ({ key, pr, queue, repo, root, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const genesis = yield* identityOf(repo);
          const state = yield* resolve({ queue, target });
          const trust = yield* projectTrust(genesis);
          const pullRequest = yield* HubProjection.project(genesis, trust, pr);
          if (pullRequest.head === null || pullRequest.state !== "open") {
            return yield* new Invalid({
              field: "pr",
              reason: `${pr} is ${pullRequest.state} and proposes nothing to land`,
            });
          }
          if (state.target !== null && pullRequest.base !== state.target) {
            return yield* new Invalid({
              field: "pr",
              reason: `${pr} targets ${pullRequest.base}, and this queue lands on ${state.target}`,
            });
          }
          yield* Queue.enter({
            repo: genesis.repoId,
            queue: state.queue,
            pr,
            head: pullRequest.head,
            key: signer,
          });
        }),
      );
      yield* Console.log(`${pr} entered`);
    }),
);

const leave = Command.make(
  "leave",
  {
    root: rootFlag,
    key: keyFlag,
    queue: Flag.string("queue").pipe(Flag.withDefault("")),
    target: Flag.string("target").pipe(Flag.withDefault("")),
    reason: Flag.string("reason").pipe(
      Flag.withDefault("withdrawn"),
      Flag.withDescription("landed | failed | conflict | stale | withdrawn"),
    ),
    repo: repoArgument,
    pr: Argument.string("pr"),
  },
  ({ key, pr, queue, reason, repo, root, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const known = ["landed", "failed", "conflict", "stale", "withdrawn"] as const;
      const chosen = known.find((entry) => entry === reason);
      if (chosen === undefined) {
        return yield* new Invalid({
          field: "reason",
          reason: `'${reason}' is not one of ${known.join(", ")}`,
        });
      }
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const genesis = yield* identityOf(repo);
          const state = yield* resolve({ queue, target });
          // Refused for the reason a mistyped queue id is: a record about
          // something that is not queued is one the projection ignores for
          // ever, on a ref nothing can shorten, reported as success.
          if (!state.entries.some((entry) => entry.pr === pr)) {
            return yield* new Invalid({
              field: "pr",
              reason: `${pr} is not in ${state.queue}`,
            });
          }
          yield* Queue.leave({
            repo: genesis.repoId,
            queue: state.queue,
            pr,
            reason: chosen,
            key: signer,
          });
          // And the branch it published goes with it. A pass only deletes the
          // branches of entries *it* settled, and no later pass can name this
          // one — so left here, the candidate commit is pinned out of reach of
          // collection for as long as the repository exists.
          //
          // Derived rather than read off the record: a `queue.reset` clears an
          // entry's candidate while the branch it published stays on disk, so
          // reading the name from the projection leaked exactly the ref this is
          // here to remove, in exactly the case a queue resets.
          if (state.target !== null) {
            const repository = yield* Repository;
            yield* repository.deleteRef(Queue.candidateBranch(state.target, pr));
          }
        }),
      );
      yield* Console.log(`${pr} left: ${reason}`);
    }),
);

const close = Command.make(
  "close",
  {
    root: rootFlag,
    key: keyFlag,
    queue: Flag.string("queue").pipe(Flag.withDefault("")),
    target: Flag.string("target").pipe(Flag.withDefault("")),
    reason: Flag.string("reason").pipe(
      Flag.withDefault("rotated"),
      Flag.withDescription("Why this queue is being ended"),
    ),
    repo: repoArgument,
  },
  ({ key, queue, reason, repo, root, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const genesis = yield* identityOf(repo);
          const state = yield* resolve({ queue, target });
          // A queue is its opener's to end, as a task is its opener's to close,
          // and the fold says so by ignoring a close from anybody else. Asked
          // here as well, and before anything is destroyed: written blind, this
          // command deleted every candidate branch the queue had published and
          // reported success, while `show`, `forTarget` and `run` all went on
          // seeing the queue as live.
          const mine = yield* fingerprint(signer.publicKey);
          if (state.openedBy !== mine) {
            return yield* new Invalid({
              field: "queue",
              reason: `${state.queue} is not this key's to close; a queue is ended by whoever opened it`,
            });
          }
          yield* Queue.close({
            repo: genesis.repoId,
            queue: state.queue,
            reason,
            key: signer,
          });
          // The branches it published go with it: nothing will name them again,
          // and each pins its candidate out of reach of collection.
          if (state.target !== null) {
            const repository = yield* Repository;
            for (const entry of state.entries) {
              yield* repository.deleteRef(Queue.candidateBranch(state.target, entry.pr));
            }
          }
        }),
      );
      yield* Console.log(`closed: ${reason}`);
    }),
);

const list = Command.make("list", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  Effect.gen(function* () {
    const found = yield* withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const queues: Array<Queue.Projection> = [];
        const unreadable: Array<{ readonly queue: string; readonly reason: string }> = [];
        for (const id of yield* Queue.queues()) {
          // One queue this replica cannot walk is one entry in this list it
          // cannot fill, and not a listing that fails. A history that arrived by
          // replication was never held to this host's ceiling, so failing would
          // let whoever grew one queue hide every other one. Reported rather
          // than skipped: a queue nothing lists and a queue that is not there
          // read identically otherwise.
          const state = yield* Queue.project(id).pipe(
            Effect.catchTags({
              Invalid: (error) => Effect.succeed(error),
              ObjectNotFound: (error) => Effect.succeed(error),
              StorageFailure: (error) => Effect.succeed(error),
            }),
          );
          if ("_tag" in state) unreadable.push({ queue: id, reason: state._tag });
          else queues.push(state);
        }
        return { queues, unreadable };
      }),
    );
    yield* Console.log(JSON.stringify(found, null, 2));
  }),
);

const show = Command.make(
  "show",
  { root: rootFlag, repo: repoArgument, queue: queueArgument },
  ({ queue, repo, root }) =>
    Effect.gen(function* () {
      const state = yield* withRepo(root, repo, Queue.project(queue));
      yield* Console.log(JSON.stringify(state, null, 2));
    }),
);

/**
 * A candidate is a pure function of what it merges, its identity included.
 *
 * This matters more than it looks. A check is bound to an exact object id, so
 * if two passes over the same inputs produced two different candidate oids, a
 * check recorded against the first would never name the second — the queue
 * would rebuild, invalidate its own evidence, and never land anything. A wall
 * clock in the committer line is enough to cause that, which is why there is
 * not one here: the time is taken from the parents, and the identity is a
 * constant.
 *
 * It also makes two runners agree. Building the same batch, they produce the
 * same commit rather than two commits with the same content — so a candidate
 * one of them published and CI tested is the one the other lands.
 */
const CANDIDATE_AUTHOR = { name: "merge queue", email: "queue@chr33s-git" } as const;

const candidateSignature = Effect.fn("queue.candidateSignature")(function* (onto: Oid, head: Oid) {
  const repository = yield* Repository;
  let at = 0;
  for (const commit of [onto, head]) {
    const info = yield* repository.readCommit(commit);
    at = Math.max(at, info.committer.at.getTime());
  }
  return { ...CANDIDATE_AUTHOR, at: new Date(at), offset: 0 };
});

/** What one pass did, reported rather than narrated. */
interface Pass {
  readonly queue: string;
  readonly target: string;
  /** What the branch held when the pass started. */
  readonly from: Oid | null;
  /** What it holds now — the same value where nothing landed. */
  readonly to: Oid | null;
  readonly landed: ReadonlyArray<string>;
  /**
   * What a dry run would have landed, and nothing on a pass that landed it.
   *
   * Kept out of `landed` deliberately: a hook gating on that field would read
   * an untouched branch as a merged one, and `dryRun: true` sitting beside it
   * is not something a shell one-liner is going to check.
   */
  readonly wouldLand: ReadonlyArray<string>;
  readonly built: ReadonlyArray<{ readonly pr: string; readonly commit: Oid }>;
  readonly dropped: ReadonlyArray<{ readonly pr: string; readonly reason: string }>;
  /**
   * Entries this runner could not build, and why.
   *
   * Distinct from `dropped`, and deliberately: what these say is that *this
   * replica* could not read something, which is a fact about the runner rather
   * than about the entry. Recording a `queue.left` for one would put a claim
   * about somebody else's work on an append-only ref on the strength of a local
   * object being missing, so nothing is written — the entry stays queued and
   * another runner, or this one after a fetch, builds it.
   */
  readonly unbuilt: ReadonlyArray<{ readonly pr: string; readonly reason: string }>;
  /** Still queued and waiting on something — usually a check that has not run. */
  readonly waiting: ReadonlyArray<string>;
  /**
   * Why the chain did not land, where it did not.
   *
   * The boundary's own words. Without them a pass that refused every candidate
   * reported `landed: []` and nothing else, which reads exactly like a pass
   * with nothing to do — and the two need very different responses.
   */
  readonly refused: ReadonlyArray<{ readonly pr: string; readonly reason: string }>;
  readonly reset: boolean;
  /**
   * Whether this pass wrote anything a reader can see.
   *
   * A dry run moves no ref and records no event, which is what makes it safe.
   * It does still *write objects* — a candidate has to be built to be reported,
   * and building one writes its trees and its commit — but they are content
   * addressed and reachable from nothing, so they cost a `gc` and change no
   * answer. `git merge-tree` writes a tree for the same reason.
   */
  readonly dryRun: boolean;
}

/**
 * One pass of the queue: rebuild the chain, land what the boundary allows.
 *
 * Everything is re-derived from refs, so this is safe to run twice, safe to
 * interrupt, and safe to run beside another copy of itself. Nothing here is a
 * lock and nothing here is a bookmark.
 */
const pass = Effect.fn("queue.pass")(function* (input: {
  readonly repo: string;
  readonly queue: string;
  readonly target: string;
  readonly key: PrivateKey;
  readonly dryRun: boolean;
}) {
  const repository = yield* Repository;
  const genesis = yield* identityOf(input.repo);
  const state = yield* resolve({ queue: input.queue, target: input.target });
  if (state.target === null) {
    return yield* new Invalid({ field: "queue", reason: `${state.queue} names no target branch` });
  }
  const target = state.target;
  const rules = yield* Policy.rulesOf();

  // Said long before it is fatal. A queue ref grows for as long as its branch
  // does, and past the ceiling a fold will walk it is unreadable and — being
  // append-only — unremovable, taking `queue run` on that branch with it. This
  // is the one hub ref that can reach that bound just by doing its job, so the
  // warning is the difference between rotating a queue and losing one.
  const ceiling = yield* Event.ceilingOf();
  if (state.records * 4 > ceiling * 3) {
    yield* Console.error(
      `warning: ${state.queue} holds ${String(state.records)} of ${String(ceiling)} records; close it and open a fresh queue for ${target ?? "this branch"} before it passes the ceiling`,
    );
  }

  // A candidate is a commit this runner makes, and `requireProvenance` is a
  // rule about *every* commit a push introduces — so a candidate would have to
  // carry a `Session:` trailer naming a session that says it produced it. It
  // does not, and cannot yet: a session id is chosen when the session opens,
  // and a candidate's object id has to be a pure function of what it merges
  // (see `candidateSignature`), so an id that varied per pass would move the
  // candidate out from under the checks recorded against it — which is the one
  // failure that makes a queue unable to land anything at all.
  //
  // Refused here rather than left to the boundary, which would refuse each
  // candidate individually and correctly. The difference is what a pass costs
  // while the combination stands: building, publishing and recording a
  // `queue.candidate` every wake, for ever, on a ref that only grows. Said once
  // is better than churned indefinitely.
  if (rules.requireProvenance) {
    return yield* new Invalid({
      field: "queue",
      reason:
        `${target} requires provenance, and a queue candidate carries none: ` +
        "the two cannot be used together yet (docs/queue.md). Turn off " +
        "requireProvenance for this repository, or land pull requests directly.",
    });
  }

  // And a protected branch that does not admit candidates will refuse every one
  // this could build. Refused here for the reason above: left to the boundary,
  // each pass would publish a candidate branch and append a `queue.candidate`
  // record that can never land — and because a candidate is a pure function of
  // what it merges, every direct push that moves the branch makes a new one, so
  // the churn is unbounded on a ref that only grows. `queueCandidates` is off by
  // default, which makes this the shape a queue is most likely to be run in
  // before somebody turns the rule on.
  if (Policy.isProtected(rules, target) && Policy.needsReview(rules) && !rules.queueCandidates) {
    return yield* new Invalid({
      field: "queue",
      reason:
        `${target} is protected and does not admit queue candidates; ` +
        "set queueCandidates in the branch rules to let it take them",
    });
  }

  const from = yield* repository.resolve(target);
  // Two readings of "what the branch is now", and they differ for a symbolic
  // ref. Merging wants the commit it resolves to; the compare-and-swap wants
  // exactly what the store compares against, which is the ref's own value. The
  // same split `Policy.evaluate` and `Event.appendTo` both make, and for the
  // same reason: handing over the resolved oid names a value nobody wrote, so
  // the swap can never match and every pass records another `queue.reset` on a
  // ref that only grows.
  const held = yield* repository.readRef(target);

  const trust = yield* projectTrust(genesis);
  const print = yield* fingerprint(input.key.publicKey);
  const member = trust.members.get(print) ?? null;
  const principal: Policy.Principal = {
    member,
    capabilities: member?.capabilities ?? [],
  };

  // The staleness bound every other door applies. `Policy.evaluate` judges one
  // ref update against the rules; the bound on how old a membership view may be
  // lives in `gate` and `gateWrite`, because it is about the *request* rather
  // than about the update. A runner that judged itself with `evaluate` alone
  // was the one writer exempt from it — landing batch after batch on a branch
  // whose every `git push` was being refused for exactly that reason.
  if (rules.maxTrustAgeSeconds > 0) {
    const stale = Verify.fresh(trust, rules.maxTrustAgeSeconds * 1000);
    if (!stale.ok) {
      return yield* new Invalid({ field: "trust", reason: stale.reason });
    }
  }

  const dropped: Array<{ readonly pr: string; readonly reason: string }> = [];
  const unbuilt: Array<{ readonly pr: string; readonly reason: string }> = [];
  const refused: Array<{ readonly pr: string; readonly reason: string }> = [];
  const built: Array<{ readonly pr: string; readonly commit: Oid }> = [];
  const record = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    input.dryRun ? Effect.void : Effect.asVoid(effect);

  const drop = (pr: string, reason: Queue.QueueLeft["reason"]) =>
    Effect.gen(function* () {
      dropped.push({ pr, reason });
      yield* record(
        Queue.leave({ repo: genesis.repoId, queue: state.queue, pr, reason, key: input.key }),
      );
    });

  // A queue with nothing under it is a queue with nothing to do, and a branch
  // that does not exist is one no candidate can be merged onto.
  if (from === null) {
    return {
      queue: state.queue,
      target,
      from,
      to: from,
      landed: [],
      wouldLand: [],
      built,
      dropped,
      unbuilt,
      waiting: state.entries.map((entry) => entry.pr),
      refused: [{ pr: "", reason: `${target} does not exist, so nothing can be merged onto it` }],
      reset: false,
      dryRun: input.dryRun,
    } satisfies Pass;
  }

  // The chain on record was not built on what the branch now holds, so the
  // candidates in it are stale. Recorded before rebuilding, so a reader can
  // tell a rebuild from a first build.
  //
  // Established positively — *no* recorded candidate sits directly on the
  // branch tip — rather than by picking one entry and asking where it was
  // built. A chain's foot is the only step built on the tip, and finding it by
  // taking the first entry that has a candidate is wrong the moment an entry
  // re-enters: its candidate is cleared in place, so the search returns a
  // later step whose `onto` is another candidate, and the pass then recorded a
  // reset that had not happened on a ref nothing can shorten.
  const recorded = state.entries.flatMap((entry) =>
    entry.candidate === null ? [] : [entry.candidate],
  );
  let reset = recorded.length > 0 && !recorded.some((candidate) => candidate.onto === from);
  if (reset) {
    yield* record(
      Queue.reset({ repo: genesis.repoId, queue: state.queue, at: from, key: input.key }),
    );
  }

  /**
   * The branch a candidate is published on; ordinary, deletable, collectable.
   *
   * Named for the pull request rather than for its place in the chain. A
   * position moves when an entry ahead of it is skipped, so a positional name
   * force-moved one pull request's published branch to another's candidate
   * while the `queue.candidate` record for the first still named it — a branch
   * CI had been told to fetch, now holding somebody else's work.
   */
  const branchOf = (pr: string) => Queue.candidateBranch(target, pr);

  const chain: Array<{
    readonly pr: string;
    readonly commit: Oid;
    readonly head: Oid;
    readonly branch: string;
    readonly checks: ReadonlyArray<HubProjection.Check>;
  }> = [];
  let tip = from;
  let position = 0;

  for (const entry of state.entries) {
    // Bounded by what the boundary will actually walk. Past the ceiling
    // `candidateChain` reads a push as not a chain at all, so building beyond
    // it publishes candidates and records that can never land — work nobody
    // asked for on a ref that only grows.
    if (position >= rules.queueDepth) {
      unbuilt.push({
        pr: entry.pr,
        reason: `this branch takes chains ${String(rules.queueDepth)} deep, and this pass is full`,
      });
      continue;
    }

    // Every way a read can fail, not only `Invalid` — the same rule the merge
    // below follows, and for the same reason: a fold this replica cannot
    // complete says nothing about the entry, and letting it escape aborted the
    // whole pass.
    const pullRequest = yield* HubProjection.project(genesis, trust, entry.pr).pipe(
      Effect.catchTags({
        Invalid: (error) => Effect.succeed(error),
        ObjectNotFound: (error) => Effect.succeed(error),
        StorageFailure: (error) => Effect.succeed(error),
      }),
    );
    if ("_tag" in pullRequest) {
      unbuilt.push({ pr: entry.pr, reason: `could not be read here: ${pullRequest._tag}` });
      continue;
    }

    // Aimed somewhere else: settled, and a permanent `queue.left` is what says
    // so. Separated from the read failure above deliberately — that says what
    // this replica could not do, and this says what the pull request did.
    if (pullRequest.base !== target) {
      yield* drop(entry.pr, "stale");
      continue;
    }

    // A moved head is a different thing, and recording it as settled made the
    // projection's own re-entry rule unreachable: `Queue.project` updates an
    // entry's head *in place*, keeping the position it already had, which is
    // exactly what somebody who pushed a fix to a queued pull request should
    // get — but only if the entry is still there when they re-enter. Dropping
    // it the moment a pass noticed the push sent them to the back instead, and
    // the faster the wake, the more certain that was.
    if (pullRequest.head !== entry.head) {
      unbuilt.push({
        pr: entry.pr,
        reason: `proposes ${pullRequest.head} now and was entered at ${entry.head}; enter it again`,
      });
      continue;
    }

    // Already in the branch, whatever put it there. A pass that landed a batch
    // and died before recording it left its entries queued, and the next pass
    // then built a no-op merge for each — a candidate whose tree is the tip's
    // own, which no check names and which therefore never lands, stalling the
    // whole queue behind work that was already done. Settling it here is the
    // recovery path, and it costs one ancestry question per entry.
    //
    // Asked *after* the head check, and that order is the whole of its safety:
    // asked before, an entry whose entered revision had landed while its pull
    // request went on to propose more was closed as merged with that new work
    // unlanded. What the question has to be about is the revision the pull
    // request proposes now, which is what the check above establishes this is.
    const contained = yield* repository.isAncestor(entry.head, from).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        StorageFailure: () => Effect.succeed(null),
      }),
    );
    if (contained === null) {
      unbuilt.push({
        pr: entry.pr,
        reason: `could not tell whether ${entry.head} is in ${target}`,
      });
      continue;
    }
    if (contained) {
      // The record of the merge, where the interrupted pass did not get to it.
      // `head` is a revision the pull request proposed, which is what hub.md
      // §10 asks of a merge event; `mergeCommit` is what carried it in. Only
      // where it is still open: a pass can die *between* this record and the
      // `queue.left` beside it, and the second attempt must finish the job
      // rather than say the same thing twice.
      if (pullRequest.state === "open") {
        yield* record(
          PullRequest.merged({
            repo: genesis.repoId,
            pr: entry.pr,
            head: entry.head,
            mergeCommit: from,
            key: input.key,
          }),
        );
      }
      yield* drop(entry.pr, "landed");
      continue;
    }

    // Closed or merged without reaching this branch: it has stopped being a
    // candidate for it. Asked *after* containment, because a pass that wrote
    // `pr.merged` and died before the `queue.left` beside it leaves a pull
    // request that is merged *and* landed — and read in the other order, the
    // recovery above became unreachable the moment it had written its first
    // record, which is precisely the interruption it exists for.
    if (pullRequest.state !== "open") {
      yield* drop(entry.pr, "stale");
      continue;
    }

    // A replica that does not hold one entry's objects is a runner that cannot
    // build *that* entry. Letting the failure escape stopped the whole pass, so
    // one unfetched head blocked every other pull request in the queue, on
    // every later run, with nothing in the output naming the cause.
    const merged = yield* repository.mergeTree({ ours: tip, theirs: entry.head }).pipe(
      Effect.catchTags({
        Invalid: (error) => Effect.succeed(error),
        ObjectNotFound: (error) => Effect.succeed(error),
        StorageFailure: (error) => Effect.succeed(error),
      }),
    );
    if ("_tag" in merged) {
      // A fact about this replica rather than about the entry, so nothing is
      // written: it stays queued, and whoever holds the objects builds it.
      unbuilt.push({ pr: entry.pr, reason: `${entry.head} could not be merged: ${merged._tag}` });
      continue;
    }
    // Predicted, not discovered: the same merge the boundary will recompute,
    // asked before anything is built rather than after a test run fails.
    //
    // But *what* it conflicts with decides whether the entry is at fault. This
    // merge is onto the chain tip, which carries every entry ahead of it — so a
    // conflict here may be with the batch rather than with the branch, and the
    // batch is provisional: the entry it clashed with may never land. Dropping
    // on that was a permanent `queue.left` for a disagreement that might never
    // have existed. Only a conflict with the branch itself — what this entry
    // actually proposes to merge into — is the entry's own problem.
    if (merged.conflicts.length > 0) {
      const withBranch =
        tip === from
          ? merged
          : yield* repository.mergeTree({ ours: from, theirs: entry.head }).pipe(
              Effect.catchTags({
                Invalid: (error) => Effect.succeed(error),
                ObjectNotFound: (error) => Effect.succeed(error),
                StorageFailure: (error) => Effect.succeed(error),
              }),
            );
      if ("_tag" in withBranch) {
        // The same rule every other read failure here follows: this says what
        // *this replica* could not read, which is no basis for a permanent
        // record about somebody else's work.
        unbuilt.push({
          pr: entry.pr,
          reason: `could not be merged against ${target}: ${withBranch._tag}`,
        });
        continue;
      }
      if (withBranch.conflicts.length === 0) {
        // Clean against the branch, so it waits for a pass this batch does not
        // stand in the way of. Nothing recorded: the queue has said nothing
        // about it, because there is nothing settled to say.
        unbuilt.push({
          pr: entry.pr,
          reason: `conflicts with the batch ahead of it in ${merged.conflicts
            .map((conflict) => conflict.path)
            .join(", ")}`,
        });
        continue;
      }
      yield* drop(entry.pr, "conflict");
      continue;
    }

    position += 1;
    const branch = branchOf(entry.pr);
    const candidate = yield* repository.commitTree({
      tree: merged.tree,
      parents: [tip, entry.head],
      message: `queue: ${entry.pr} onto ${target}\n`,
      author: yield* candidateSignature(tip, entry.head),
    });
    chain.push({
      pr: entry.pr,
      commit: candidate,
      head: entry.head,
      branch,
      checks: pullRequest.checks,
    });
    built.push({ pr: entry.pr, commit: candidate });

    // Recorded only where it differs from what the queue already says. A
    // candidate is a pure function of what it merges, so an unchanged batch
    // rebuilds the identical commit — and appending that every pass would grow
    // an undeletable ref towards the ceiling a fold will walk, at which point
    // the queue becomes unreadable and unremovable at once. The branch is
    // written unconditionally, because it is cheap and may have been deleted.
    const unchanged =
      // Not after a reset this pass wrote. `state` was projected before it, so
      // an identically rebuilt candidate looked unchanged against a record the
      // reset had just cleared — and skipping it left the projection showing no
      // candidate for an entry whose branch exists, for good.
      !reset &&
      entry.candidate !== null &&
      entry.candidate.commit === candidate &&
      entry.candidate.onto === tip &&
      entry.candidate.branch === branch;
    if (!input.dryRun) {
      yield* repository.setRef({ name: branch, to: candidate });
      if (!unchanged) {
        yield* Queue.candidate({
          repo: genesis.repoId,
          queue: state.queue,
          pr: entry.pr,
          commit: candidate,
          onto: tip,
          branch,
          key: input.key,
        });
      }
    }
    tip = candidate;
  }

  // The longest prefix the boundary will take. Asked of the boundary rather
  // than re-derived here, so a runner can never believe something is landable
  // that a push of the same commit would be refused — and so the rule stays in
  // one place when it changes.
  //
  // Front to back, and it stops at the first refusal. A step is judged on
  // itself and everything before it and on nothing after, so the steps the
  // boundary accepts are a prefix — which means walking forwards finds the same
  // answer as walking backwards while paying for it very differently. Backwards
  // asked about the whole chain first, and each ask re-derives every merge
  // beneath it, so the ordinary case — a batch waiting on checks, nothing green
  // yet — was also the most expensive, on a path a wake fires repeatedly.
  //
  // The caches are shared across the asks for the same reason: a pull request
  // folded for one prefix is the same pull request in the next.
  const folds: Policy.FoldCache = new Map();
  const mentions: Policy.MentionCache = new Map();
  // And the merges, which is the expensive one: each ask re-derives the whole
  // chain beneath it, so without this a pass waiting on a check part-way up a
  // deep chain paid for the same merges again on every wake.
  const merges: Policy.MergeCache = new Map();
  let landedAt = -1;
  for (const [at, step] of chain.entries()) {
    const decision = yield* Policy.evaluate({
      update: { name: target, value: step.commit },
      principal,
      genesis,
      trust,
      rules,
      folds,
      mentions,
      merges,
    });
    if (!decision.ok) {
      // Kept, not discarded. A pass that refused every candidate used to report
      // an empty `landed` and nothing else, which reads exactly like a pass
      // with nothing to do — and the two want very different responses. This is
      // where an operator reads that a branch requires a check nothing has run.
      refused.push({ pr: step.pr, reason: decision.reason });
      break;
    }
    landedAt = at;
  }

  const landed: Array<string> = [];
  /** Whether somebody else moved the branch while this pass was building. */
  let raced = false;
  /** What a dry run would have landed; empty on a pass that actually lands. */
  const wouldLand: Array<string> = [];
  if (landedAt >= 0 && !input.dryRun) {
    const top = chain[landedAt]!;
    // The value the judgement was made against travels with the write, exactly
    // as it does on the receive-pack path: a push landing in between fails the
    // swap rather than being overwritten by this one.
    const applied = yield* repository.setRef({ name: target, to: top.commit, expected: held }).pipe(
      Effect.as(true),
      Effect.catchTag("RefConflict", () => Effect.succeed(false)),
    );
    if (applied) {
      for (const step of chain.slice(0, landedAt + 1)) {
        // Naming the revision the pull request actually proposed, which is what
        // hub.md §10 requires of a merge event; the branch holds the candidate,
        // whose second parent *is* that revision, so ancestry says the rest.
        yield* PullRequest.merged({
          repo: genesis.repoId,
          pr: step.pr,
          head: step.head,
          mergeCommit: step.commit,
          key: input.key,
        });
        yield* Queue.leave({
          repo: genesis.repoId,
          queue: state.queue,
          pr: step.pr,
          reason: "landed",
          key: input.key,
        });
        landed.push(step.pr);
      }
    } else {
      // Somebody else moved the branch while this pass was building. Nothing
      // was lost; the next pass rebuilds on what they left — but a pass that
      // reported an empty `landed` and nothing else here read exactly like a
      // pass with nothing to do, which is the ambiguity `refused` exists to
      // remove. Said out loud, and `reset` says a record was written.
      const now = yield* repository.resolve(target).pipe(Effect.map((oid) => oid ?? from));
      reset = true;
      raced = true;
      refused.push({
        pr: top.pr,
        reason: `${target} moved from ${from} to ${now} while this pass was building`,
      });
      yield* Queue.reset({ repo: genesis.repoId, queue: state.queue, at: now, key: input.key });
    }
  } else if (landedAt >= 0) {
    // A dry run moved nothing, so nothing landed. Reported separately rather
    // than in `landed`, because a hook gating on that field would read an
    // untouched branch as a merged one — and `dryRun: true` beside it is not
    // something a shell one-liner is going to check.
    for (const step of chain.slice(0, landedAt + 1)) wouldLand.push(step.pr);
  }

  // A required check that came back *failing* against a candidate is the one
  // outcome that says the entry itself is the problem, so it is evicted rather
  // than left to block everything behind it for ever. "Has not run" is not
  // failure and must not be read as one — a batch waiting on CI is the ordinary
  // case, and `checksPassedAt` cannot tell the two apart, so the status is read
  // here rather than inferred from a boolean.
  const failing = (step: (typeof chain)[number]): string | null => {
    for (const name of rules.requiredChecks) {
      const check = step.checks.find(
        (candidate) => candidate.name === name && candidate.head === step.commit,
      );
      if (check?.status === "failure") return name;
    }
    return null;
  };
  /** Whether every required check has come back successful against this step. */
  const green = (step: (typeof chain)[number]): boolean =>
    rules.requiredChecks.every((name) =>
      step.checks.some(
        (candidate) =>
          candidate.name === name &&
          candidate.head === step.commit &&
          candidate.status === "success",
      ),
    );
  //
  // And only the *first* of them. A candidate contains every step beneath it,
  // so one broken pull request fails the checks on every candidate above it as
  // well — evicting them all would take the whole batch out for one entry's
  // fault, permanently, on a ref nothing can shorten. The same distinction the
  // conflict path makes between the batch and the branch, arrived at from the
  // other side: the steps behind a failure are victims of it, not causes.
  // Skipped where the swap was lost: the branch moved, so the chain those
  // checks ran against is not a combination anybody is proposing any more, and
  // a permanent eviction on stale evidence is the same mistake as dropping an
  // entry that merely conflicted with the batch.
  for (const step of raced ? [] : chain.slice(landed.length + wouldLand.length)) {
    if (failing(step) !== null) {
      yield* drop(step.pr, "failed");
      break;
    }
    // And only where everything beneath it has actually reported. A candidate
    // contains every step under it, so a red one under a *pending* one says
    // nothing about which of them broke — blaming the red one there evicted a
    // pull request for a change that had not been tested yet, permanently.
    if (!green(step)) break;
  }

  const waiting = chain
    .slice(landed.length + wouldLand.length)
    .filter((step) => !dropped.some((entry) => entry.pr === step.pr));

  // Candidate branches are ordinary branches, so they are cleaned up like
  // ordinary branches: a candidate whose entry this pass *settled* is one
  // nothing will fetch again, and leaving it behind pins its objects out of
  // reach of collection for good.
  //
  // Only what this pass settled, and named exactly. Sweeping the prefix for
  // anything a keep-list did not mention read the queue as it stood when the
  // pass began, so it deleted a concurrent runner's freshly published candidate
  // — still named by its own `queue.candidate` record — and any branch a person
  // happened to keep under the same prefix. A pull request this pass landed or
  // dropped is the one thing it knows is finished.
  if (!input.dryRun) {
    for (const pr of [...landed, ...dropped.map((entry) => entry.pr)]) {
      yield* repository.deleteRef(branchOf(pr));
    }
  }

  return {
    queue: state.queue,
    target,
    from,
    to: yield* repository.resolve(target),
    landed,
    wouldLand,
    built,
    dropped,
    unbuilt,
    waiting: waiting.map((step) => step.pr),
    refused,
    reset,
    dryRun: input.dryRun,
  } satisfies Pass;
});

const run = Command.make(
  "run",
  {
    root: rootFlag,
    key: keyFlag,
    queue: Flag.string("queue").pipe(Flag.withDefault("")),
    target: Flag.string("target").pipe(Flag.withDefault("")),
    dryRun: Flag.boolean("dry-run").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Say what one pass would do, moving no ref and recording nothing"),
    ),
    repo: repoArgument,
  },
  ({ dryRun, key, queue, repo, root, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const outcome = yield* withRepo(
        root,
        repo,
        pass({ repo, queue, target, key: signer, dryRun }),
      );
      yield* Console.log(JSON.stringify(outcome, null, 2));
    }),
);

export const queueCommand = Command.make("queue", {}, () =>
  Console.log("git+ queue <open|enter|leave|run|close|list|show> — see --help"),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Start a queue for a branch")),
    enter.pipe(Command.withDescription("Offer a pull request for landing")),
    leave.pipe(Command.withDescription("Take a pull request back out")),
    run.pipe(Command.withDescription("Build candidates and land what the boundary allows")),
    close.pipe(Command.withDescription("End a queue, so a fresh one can take over")),
    list.pipe(Command.withDescription("Every queue this repository holds")),
    show.pipe(Command.withDescription("What one queue amounts to now")),
  ]),
);
