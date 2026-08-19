/**
 * `chr33s-git wake` — run local rules for hub events since the last run.
 *
 * The walk itself lives in `server/Wake.node.ts`, because the node host runs
 * exactly the same pass from its post-receive hook and the two must not drift:
 * a wake that fired differently depending on who asked for it would be a rule
 * an operator could not reason about.
 */
import * as path from "node:path";

import { Console, Effect } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import * as Wake from "../server/Wake.node.ts";
import { repoArgument, rootFlag, withRepo } from "./shared.ts";

const wake = Command.make(
  "wake",
  {
    root: rootFlag,
    dry: Flag.boolean("dry-run").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Say what would run, run nothing, and leave the bookmark alone"),
    ),
    repo: repoArgument,
  },
  ({ dry, repo, root }) =>
    Effect.gen(function* () {
      const directory = path.join(root, repo);
      const summary = yield* withRepo(root, repo, Wake.dispatch({ directory, repo, dryRun: dry }));

      yield* Console.log(
        dry
          ? `${summary.fired} rule(s) would run`
          : `${summary.fired} rule(s) run, ${summary.failed} failed`,
      );
      // A non-zero exit, so whatever called this — a hook, a timer, a person —
      // learns that something did not run without reading the output.
      if (summary.failed > 0) {
        return yield* new Invalid({
          field: "wake",
          reason: `${summary.failed} woken command(s) failed`,
        });
      }
    }),
);

export const wakeCommand = wake.pipe(
  Command.withDescription("Run local rules for hub events since the last run"),
);
