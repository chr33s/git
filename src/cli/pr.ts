/**
 * `chr33s-git pr …` — pull requests, from the command line.
 *
 * The verb layer `hub/PullRequest.ts` has always had, finally reachable
 * without writing a program: open a pull request for a pushed branch, move
 * its head, review a revision, talk in threads, report checks, and record
 * the merge. Every verb signs with the caller's key and appends an event;
 * the projection remains the judge of what counts, on every replica alike.
 *
 * `merge` is the one composite: it performs the three-way merge the same way
 * `chr33s-git merge --into` does, and then records `pr.merged` naming what
 * was merged and what it became — the two halves the design keeps separate,
 * done in the order that keeps them honest (no event for a merge that did
 * not land).
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import { approvals, project } from "../hub/Projection.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import {
  cliSignature,
  mustResolve,
  readPrivateKey,
  refNameOf,
  repoArgument,
  rootFlag,
  withRepo,
} from "./shared.ts";

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);

const prArgument = Argument.string("pr");

const identityOf = Effect.fn("pr.identityOf")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis; run \`chr33s-git hub init ${repo} --key <key>\` first`,
    });
  }
  return stored.genesis.repoId;
});

/** The genesis and trust view a projection folds against. */
const viewOf = Effect.fn("pr.viewOf")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis, so its pull requests cannot be judged`,
    });
  }
  return { genesis: stored.genesis, trust: yield* projectTrust(stored.genesis) };
});

const open = Command.make(
  "open",
  {
    root: rootFlag,
    key: keyFlag,
    title: Flag.string("title").pipe(Flag.withDescription("What this proposes, in one line")),
    description: Flag.string("description").pipe(Flag.withDefault("")),
    base: Flag.string("base").pipe(
      Flag.withDefault("main"),
      Flag.withDescription("The branch this asks to change"),
    ),
    head: Flag.string("head").pipe(
      Flag.withDescription("The proposed revision — a branch or a commit"),
    ),
    repo: repoArgument,
  },
  ({ base, description, head, key, repo, root, title }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const pr = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const opened = yield* PullRequest.open({
            repo: yield* identityOf(repo),
            title,
            description,
            base: refNameOf(base),
            head: yield* mustResolve(repository, head),
            key: signer,
          });
          return opened.pr;
        }),
      );
      // The id alone, so a script can hand it to the next verb.
      yield* Console.log(pr);
    }),
);

const update = Command.make(
  "update",
  {
    root: rootFlag,
    key: keyFlag,
    head: Flag.string("head").pipe(Flag.withDescription("The new revision proposed")),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ head, key, pr, repo, root }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          yield* PullRequest.update({
            repo: yield* identityOf(repo),
            pr,
            head: yield* mustResolve(repository, head),
            key: signer,
          });
        }),
      );
    }),
);

/** `close` and `reopen` differ by one event type; built from one shape. */
const lifecycle = (name: "close" | "reopen", description: string, verb: typeof PullRequest.close) =>
  Command.make(
    name,
    { root: rootFlag, key: keyFlag, repo: repoArgument, pr: prArgument },
    ({ key, pr, repo, root }) =>
      Effect.gen(function* () {
        const signer = yield* readPrivateKey(key);
        yield* withRepo(
          root,
          repo,
          Effect.gen(function* () {
            yield* verb({ repo: yield* identityOf(repo), pr, key: signer });
          }),
        );
      }),
  ).pipe(Command.withDescription(description));

const review = Command.make(
  "review",
  {
    root: rootFlag,
    key: keyFlag,
    decision: Flag.choice("decision", ["approve", "reject", "comment"]).pipe(
      Flag.withDescription("An approval is of a revision, never of a pull request"),
    ),
    head: Flag.string("head").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The revision reviewed; defaults to the pull request's head"),
    ),
    body: Flag.string("body").pipe(Flag.withDefault("")),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ body, decision, head, key, pr, repo, root }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const view = yield* viewOf(repo);
          const reviewed =
            head === ""
              ? (yield* project(view.genesis, view.trust, pr)).head
              : yield* mustResolve(repository, head);
          if (reviewed === null) {
            return yield* new Invalid({
              field: "head",
              reason: `${pr} proposes no revision yet; name one with --head`,
            });
          }
          yield* PullRequest.review({
            repo: view.genesis.repoId,
            pr,
            head: reviewed,
            decision,
            body,
            key: signer,
          });
        }),
      );
    }),
);

const dismiss = Command.make(
  "dismiss",
  {
    root: rootFlag,
    key: keyFlag,
    review: Flag.string("review").pipe(Flag.withDescription("The review event id to dismiss")),
    reason: Flag.string("reason").pipe(Flag.withDefault("")),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ key, pr, reason, repo, review: target, root }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* PullRequest.dismissReview({
            repo: yield* identityOf(repo),
            pr,
            review: target,
            reason,
            key: signer,
          });
        }),
      );
    }),
);

const comment = Command.make(
  "comment",
  {
    root: rootFlag,
    key: keyFlag,
    body: Flag.string("body"),
    path: Flag.string("path").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Make it an inline comment on this path"),
    ),
    line: Flag.integer("line").pipe(Flag.withDefault(0)),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ body, key, line, path, pr, repo, root }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repoId = yield* identityOf(repo);
          const base = { repo: repoId, pr, body, key: signer };
          yield* path === ""
            ? PullRequest.comment(base)
            : PullRequest.comment({ ...base, path, line: line === 0 ? undefined : line });
        }),
      );
    }),
);

const reply = Command.make(
  "reply",
  {
    root: rootFlag,
    key: keyFlag,
    thread: Flag.string("thread").pipe(Flag.withDescription("The thread id to answer in")),
    body: Flag.string("body"),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ body, key, pr, repo, root, thread }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          yield* PullRequest.reply({
            repo: yield* identityOf(repo),
            pr,
            thread,
            body,
            key: signer,
          });
        }),
      );
    }),
);

/** `resolve` and `reopen-thread` are one shape apart, like the lifecycle pair. */
const threadState = (
  name: "resolve" | "reopen-thread",
  description: string,
  verb: typeof PullRequest.resolve,
) =>
  Command.make(
    name,
    {
      root: rootFlag,
      key: keyFlag,
      thread: Flag.string("thread"),
      repo: repoArgument,
      pr: prArgument,
    },
    ({ key, pr, repo, root, thread }) =>
      Effect.gen(function* () {
        const signer = yield* readPrivateKey(key);
        yield* withRepo(
          root,
          repo,
          Effect.gen(function* () {
            yield* verb({ repo: yield* identityOf(repo), pr, thread, key: signer });
          }),
        );
      }),
  ).pipe(Command.withDescription(description));

const check = Command.make(
  "check",
  {
    root: rootFlag,
    key: keyFlag,
    name: Flag.string("name").pipe(
      Flag.withDescription("The check's name — what a policy requires"),
    ),
    provider: Flag.string("provider").pipe(Flag.withDefault("cli")),
    head: Flag.string("head").pipe(Flag.withDescription("The revision the check ran against")),
    status: Flag.choice("status", ["started", "success", "failure", "neutral"]),
    url: Flag.string("url").pipe(Flag.withDefault("")),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ head, key, name, pr, provider, repo, root, status, url }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const repoId = yield* identityOf(repo);
          const at = yield* mustResolve(repository, head);
          if (status === "started") {
            yield* PullRequest.checkStarted({
              repo: repoId,
              pr,
              head: at,
              name,
              provider,
              key: signer,
            });
            return;
          }
          const base = { repo: repoId, pr, head: at, name, provider, status, key: signer };
          yield* url === ""
            ? PullRequest.checkCompleted(base)
            : PullRequest.checkCompleted({ ...base, url });
        }),
      );
    }),
);

const merge = Command.make(
  "merge",
  {
    root: rootFlag,
    key: keyFlag,
    strategy: Flag.choice("strategy", ["recursive", "ours", "theirs"]).pipe(
      Flag.withDefault("recursive"),
      Flag.withAlias("s"),
    ),
    repo: repoArgument,
    pr: prArgument,
  },
  ({ key, pr, repo, root, strategy }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const outcome = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const view = yield* viewOf(repo);
          const state = yield* project(view.genesis, view.trust, pr);
          if (state.state !== "open") {
            return yield* new Invalid({ field: "pr", reason: `${pr} is ${state.state}` });
          }
          if (state.head === null) {
            return yield* new Invalid({ field: "pr", reason: `${pr} proposes no revision` });
          }

          const into = Event.branchRef(state.base);
          const merged = yield* repository.merge({
            ours: into,
            theirs: state.head,
            author: cliSignature(),
            strategy,
            into,
          });
          if (merged.kind === "conflicted" || merged.commit === null) {
            // SAFETY: widened to the union both branches of this gen return.
            return { conflicts: merged.conflicts, commit: null as string | null };
          }
          // The record, after the branch moved and not before: an event for
          // a merge that did not land would be a lie every replica repeats.
          yield* PullRequest.merged({
            repo: view.genesis.repoId,
            pr,
            head: state.head,
            mergeCommit: merged.commit,
            key: signer,
          });
          // SAFETY: same widening as the conflicted branch above.
          return { conflicts: merged.conflicts, commit: merged.commit as string | null };
        }),
      );

      if (outcome.commit === null) {
        for (const conflict of outcome.conflicts) {
          yield* Console.error(`CONFLICT (${conflict.reason}): ${conflict.path}`);
        }
        return yield* new Invalid({
          field: "merge",
          reason: `${outcome.conflicts.length} conflict(s)`,
        });
      }
      yield* Console.log(outcome.commit);
    }),
);

const list = Command.make(
  "list",
  {
    root: rootFlag,
    all: Flag.boolean("all").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Include closed and merged pull requests"),
    ),
    repo: repoArgument,
  },
  ({ all, repo, root }) =>
    Effect.gen(function* () {
      const found = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const view = yield* viewOf(repo);
          const ids = yield* Event.pullRequests();
          const projections = yield* Effect.forEach(ids.sort(), (id) =>
            project(view.genesis, view.trust, id),
          );
          return projections
            .filter((state) => all || state.state === "open")
            .map((state) => ({
              id: state.id,
              title: state.title,
              state: state.state,
              base: state.base,
              head: state.head,
              approvals: approvals(state).length,
              threads: state.threads.length,
              checks: state.checks.length,
            }));
        }),
      );
      yield* Console.log(JSON.stringify(found, null, 2));
    }),
);

const show = Command.make(
  "show",
  { root: rootFlag, repo: repoArgument, pr: prArgument },
  ({ pr, repo, root }) =>
    Effect.gen(function* () {
      const state = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const view = yield* viewOf(repo);
          return yield* project(view.genesis, view.trust, pr);
        }),
      );
      yield* Console.log(
        JSON.stringify(
          {
            ...state,
            claims: undefined,
            openers: [...state.openers],
            redacted: [...state.redacted],
          },
          null,
          2,
        ),
      );
    }),
);

export const prCommand = Command.make("pr", {}, () =>
  Console.log(
    "chr33s-git pr <open|update|close|reopen|review|dismiss|comment|reply|resolve|reopen-thread|check|merge|list|show> — see --help",
  ),
).pipe(
  Command.withSubcommands([
    open.pipe(Command.withDescription("Open a pull request for a pushed revision")),
    update.pipe(Command.withDescription("Propose a new revision")),
    lifecycle("close", "Close a pull request without merging it", PullRequest.close),
    lifecycle("reopen", "Reopen a closed pull request", PullRequest.reopen),
    review.pipe(Command.withDescription("Approve, reject, or comment on a revision")),
    dismiss.pipe(Command.withDescription("Dismiss a review, with the reason on record")),
    comment.pipe(Command.withDescription("Comment on the conversation, or inline on a path")),
    reply.pipe(Command.withDescription("Reply in an existing thread")),
    threadState("resolve", "Mark a thread resolved", PullRequest.resolve),
    threadState("reopen-thread", "Reopen a resolved thread", PullRequest.reopenThread),
    check.pipe(Command.withDescription("Report a check: started, or its outcome")),
    merge.pipe(Command.withDescription("Merge an open pull request and record pr.merged")),
    list.pipe(Command.withDescription("Open pull requests, projected")),
    show.pipe(Command.withDescription("One pull request, whole")),
  ]),
);
