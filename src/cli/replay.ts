/**
 * Replay commands.
 *
 * `cherry-pick`, `rebase` and `bisect`: the commands that re-apply or walk
 * existing commits rather than make new content. Descriptions stay at the
 * registration site in `main.ts`.
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { next as bisectNext } from "../git/Bisect.ts";
import { cherryPick, rebase } from "../git/Rebase.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import {
  cliSignature,
  mustResolve,
  refNameOf,
  repoArgument,
  rootFlag,
  withRepo,
} from "./shared.ts";

/** Both replay commands print the same ledger, so they share the printer. */
const reportReplay = (outcome: {
  readonly kind: string;
  readonly head: Oid | null;
  readonly commits: ReadonlyArray<{
    readonly original: Oid;
    readonly replayed: Oid | null;
    readonly conflicts: ReadonlyArray<{ readonly path: string; readonly reason: string }>;
  }>;
}) =>
  Effect.gen(function* () {
    for (const entry of outcome.commits) {
      for (const conflict of entry.conflicts) {
        yield* Console.log(`CONFLICT (${conflict.reason}): ${conflict.path}`);
      }
      // A replay of `null` is a commit that produced nothing: already present
      // on the target, or one whose change was empty once applied.
      yield* Console.log(
        entry.replayed === null
          ? `skipped ${entry.original}`
          : `${entry.original} -> ${entry.replayed}`,
      );
    }
    yield* Console.log(`${outcome.kind}${outcome.head === null ? "" : ` at ${outcome.head}`}`);
  });

export const cherryPickCommand = Command.make(
  "cherry-pick",
  {
    root: rootFlag,
    repo: repoArgument,
    commit: Argument.string("commit"),
    onto: Flag.string("onto").pipe(Flag.withDescription("The commit or branch to replay onto")),
    into: Flag.string("into").pipe(
      Flag.optional,
      Flag.withDescription("Ref to move on success; absent computes the replay and stops"),
    ),
  },
  ({ commit, into, onto, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const input = {
          commit: yield* mustResolve(repository, commit),
          onto: yield* mustResolve(repository, onto),
          author: yield* cliSignature(),
        };
        const outcome = yield* into._tag === "Some"
          ? cherryPick({ ...input, into: refNameOf(into.value) })
          : cherryPick(input);
        yield* reportReplay(outcome);
      }),
    ),
);

export const rebaseCommand = Command.make(
  "rebase",
  {
    root: rootFlag,
    repo: repoArgument,
    branch: Argument.string("branch"),
    onto: Flag.string("onto").pipe(Flag.withDescription("The commit or branch to replay onto")),
    into: Flag.string("into").pipe(
      Flag.optional,
      Flag.withDescription("Ref to move on success; absent computes the replay and stops"),
    ),
  },
  ({ branch, into, onto, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const outcome = yield* rebase({
          branch: yield* mustResolve(repository, branch),
          onto: yield* mustResolve(repository, onto),
          // Defaulting `--into` to the branch is what makes `rebase main`
          // move `main`, which is the only thing a caller ever means by it.
          into: refNameOf(into._tag === "Some" ? into.value : branch),
        });
        yield* reportReplay(outcome);
      }),
    ),
);

/**
 * One bisect step, stateless.
 *
 * `git bisect` keeps a session in `.git`; this takes the known state as
 * arguments and prints the next commit to test. A shell loop is then the
 * whole session, and the same call works over HTTP where there is nothing to
 * keep a session in.
 */
export const bisectCommand = Command.make(
  "bisect",
  {
    root: rootFlag,
    repo: repoArgument,
    bad: Flag.string("bad").pipe(Flag.withDescription("A revision known to have the problem")),
    good: Flag.string("good").pipe(
      Flag.atLeast(1),
      Flag.withDescription("A revision known not to; repeat for more than one"),
    ),
  },
  ({ bad, good, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const step = yield* bisectNext({
          bad: yield* mustResolve(repository, bad),
          good: yield* Effect.forEach(good, (rev) => mustResolve(repository, rev)),
        });

        if (step.kind === "found") {
          yield* Console.log(`${step.commit} is the first bad commit`);
          return;
        }
        yield* Console.log(
          `${step.commit}\t${step.remaining} revision(s) left, roughly ${step.steps} step(s)`,
        );
      }),
    ),
);
