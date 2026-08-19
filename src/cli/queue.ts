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
import * as HubProjection from "../hub/Projection.ts";
import * as Queue from "../hub/Queue.ts";
import { fingerprint, type PrivateKey } from "../crypto/SshSignature.ts";
import * as Policy from "../server/Policy.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { project as projectTrust } from "../trust/Projection.ts";
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
    return state;
  }
  if (input.target === "") {
    return yield* new Invalid({
      field: "queue",
      reason: "name a queue with --queue <id> or the branch it serves with --target <ref>",
    });
  }
  const found = yield* Queue.forTarget(input.target);
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
          const opened = yield* Queue.open({
            repo: (yield* identityOf(repo)).repoId,
            target,
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
          yield* Queue.leave({
            repo: genesis.repoId,
            queue: state.queue,
            pr,
            reason: chosen,
            key: signer,
          });
        }),
      );
      yield* Console.log(`${pr} left: ${reason}`);
    }),
);

const list = Command.make("list", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  Effect.gen(function* () {
    const found = yield* withRepo(
      root,
      repo,
      Effect.gen(function* () {
        return yield* Effect.forEach(yield* Queue.queues(), (queue) => Queue.project(queue));
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

  const from = yield* repository.resolve(target);

  const trust = yield* projectTrust(genesis);
  const print = yield* fingerprint(input.key.publicKey);
  const member = trust.members.get(print) ?? null;
  const principal: Policy.Principal = {
    member,
    capabilities: member?.capabilities ?? [],
  };

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
      built,
      dropped,
      unbuilt,
      waiting: state.entries.map((entry) => entry.pr),
      refused: [{ pr: "", reason: `${target} does not exist, so nothing can be merged onto it` }],
      reset: false,
      dryRun: input.dryRun,
    } satisfies Pass;
  }

  // The target moved out from under the chain that was built on it. Recorded
  // before rebuilding, so a reader can tell a rebuild from a first build.
  const stale = state.entries.some(
    (entry) => entry.candidate !== null && entry.candidate.onto !== from,
  );
  const chainStart = state.entries.find((entry) => entry.candidate !== null)?.candidate?.onto;
  const reset = stale && chainStart !== from;
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
  const branchOf = (pr: string) => `refs/heads/queue/${target.replace(/^refs\/heads\//, "")}/${pr}`;

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

    // Whatever put it in the queue, what it proposes *now* is what can land.
    // Separated from the read failure above deliberately: this is a fact about
    // the pull request, which is what a permanent `queue.left` may record.
    if (
      pullRequest.state !== "open" ||
      pullRequest.base !== target ||
      pullRequest.head !== entry.head
    ) {
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
      if (!("_tag" in withBranch) && withBranch.conflicts.length === 0) {
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
  if (landedAt >= 0 && !input.dryRun) {
    const top = chain[landedAt]!;
    // The value the judgement was made against travels with the write, exactly
    // as it does on the receive-pack path: a push landing in between fails the
    // swap rather than being overwritten by this one.
    const applied = yield* repository.setRef({ name: target, to: top.commit, expected: from }).pipe(
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
      // was lost; the next pass rebuilds on what they left.
      yield* Queue.reset({
        repo: genesis.repoId,
        queue: state.queue,
        at: yield* repository.resolve(target).pipe(Effect.map((oid) => oid ?? from)),
        key: input.key,
      });
    }
  } else if (landedAt >= 0) {
    for (const step of chain.slice(0, landedAt + 1)) landed.push(step.pr);
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
  for (const step of chain.slice(landed.length)) {
    const broke = failing(step);
    if (broke !== null) yield* drop(step.pr, "failed");
  }

  const waiting = chain
    .slice(landed.length)
    .filter((step) => !dropped.some((entry) => entry.pr === step.pr));

  // Candidate branches are ordinary branches, so they are cleaned up like
  // ordinary branches: what is still waiting keeps its branch, because that is
  // what CI fetches, and everything else goes. Left behind, every candidate the
  // queue had ever built stayed a ref — pinning its objects out of reach of
  // collection for good, on a repository whose whole point is that it can be
  // collected.
  if (!input.dryRun) {
    const keep = new Set(waiting.map((step) => step.branch));
    const prefix = `refs/heads/queue/${target.replace(/^refs\/heads\//, "")}/`;
    for (const [name] of yield* repository.refs) {
      if (!name.startsWith(prefix) || keep.has(name)) continue;
      yield* repository.deleteRef(name);
    }
  }

  return {
    queue: state.queue,
    target,
    from,
    to: yield* repository.resolve(target),
    landed,
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
      Flag.withDescription("Say what one pass would do, and write nothing"),
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
  Console.log("git+ queue <open|enter|leave|run|list|show> — see --help"),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Start a queue for a branch")),
    enter.pipe(Command.withDescription("Offer a pull request for landing")),
    leave.pipe(Command.withDescription("Take a pull request back out")),
    run.pipe(Command.withDescription("Build candidates and land what the boundary allows")),
    list.pipe(Command.withDescription("Every queue this repository holds")),
    show.pipe(Command.withDescription("What one queue amounts to now")),
  ]),
);
