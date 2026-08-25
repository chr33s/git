/**
 * `git+ maintenance plan` and `git+ maintenance run`.
 *
 * `plan` is read-only and deterministic for identical state. `run` executes
 * the highest-priority unit; `--all` replans until nothing remains.
 */
import { Console, Effect } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import * as Bundles from "../server/Bundles.ts";
import { fileLayer as bundleFiles, fileMetaLayer } from "../server/Bundles.node.ts";
import { defaultsLayer as featuresLayer } from "../server/Features.ts";
import * as Maintenance from "../server/MaintenanceRun.ts";
import { memoryLayer as operationsMemory, renderLine } from "../server/Operations.ts";
import { repoArgument, rootFlag } from "./shared.ts";

import { Layer } from "effect";
import * as path from "node:path";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";

const extras = (root: string, repo: string) => {
  const directory = path.join(root, repo);
  const repoStores = stores(directory);
  const bundleStore = bundleFiles(directory);
  const live = GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provideMerge(repoStores),
  );
  return Layer.mergeAll(
    live,
    bundleStore,
    fileMetaLayer(directory),
    operationsMemory,
    featuresLayer,
    Bundles.layer.pipe(Layer.provide(bundleStore), Layer.provide(live)),
  );
};

const planCommand = Command.make("plan", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  Effect.gen(function* () {
    const units = yield* Maintenance.plan();
    if (units.length === 0) {
      yield* Console.log("no maintenance needed");
      return;
    }
    for (const unit of units) {
      if (unit.kind === "build-full" || unit.kind === "build-incremental") {
        yield* Console.log(`${unit.kind} filter=${unit.filter ?? "full"}`);
      } else if (unit.kind === "prune-invalid" || unit.kind === "prune-retired") {
        yield* Console.log(`${unit.kind} ${String(unit.ids.length)} artifact(s)`);
      } else {
        yield* Console.log(unit.kind);
      }
    }
  }).pipe(Effect.provide(extras(root, repo))),
);

const runCommand = Command.make(
  "run",
  {
    root: rootFlag,
    all: Flag.boolean("all").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Replan after each unit until no work remains"),
    ),
    repo: repoArgument,
  },
  ({ all, repo, root }) =>
    Effect.gen(function* () {
      const ticks = all ? yield* Maintenance.runAll() : [yield* Maintenance.tick()];
      for (const tick of ticks) {
        if (tick.ran === null) {
          yield* Console.log("no maintenance needed");
          continue;
        }
        const label = tick.ran.kind;
        yield* Console.log(`* ${label}: complete`);
        if (tick.operationId !== undefined) {
          yield* Console.log(`operation ${tick.operationId}`);
        }
      }
    }).pipe(Effect.provide(extras(root, repo))),
);

export const maintenanceCommand = Command.make("maintenance", {}, () =>
  Console.log("git+ maintenance <plan|run> — see --help"),
).pipe(
  Command.withSubcommands([
    planCommand.pipe(Command.withDescription("Show the next maintenance units without writing")),
    runCommand.pipe(Command.withDescription("Execute the highest-priority maintenance unit")),
  ]),
);

export { renderLine };
export { extras as maintenanceLayer };
