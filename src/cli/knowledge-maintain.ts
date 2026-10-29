/** The Node worker's request/run/show surface. A timer invokes run directly. */
import * as path from "node:path";
import { Console, Effect, Option, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { Invalid, StorageFailure } from "../git/Error.ts";
import { routeOf } from "../server/Route.ts";
import * as Host from "../knowledge/Maintenance.node.ts";
import { repoFlag, rootFlag, withRepo } from "./shared.ts";

const jsonFlag = Flag.boolean("json").pipe(
  Flag.withDefault(false),
  Flag.withDescription("One versioned result on stdout; diagnostics on stderr"),
);
const repository = Effect.fn("knowledge.maintain.repository")(function* (
  root: string,
  repo: string,
) {
  if (repo.includes("/") || repo.includes("\\") || routeOf(`/${repo}`) === null)
    return yield* new Invalid({
      field: "repo",
      reason: "maintain requires a valid --repo storage name",
    });
  return path.join(root, repo);
});
const emit = <A extends { readonly version: number; readonly exitCode?: number }, E>(
  effect: Effect.Effect<A, E>,
  json: boolean,
) =>
  effect.pipe(
    Effect.matchEffect({
      onSuccess: (result) =>
        Effect.gen(function* () {
          process.exitCode = result.exitCode ?? 0;
          yield* Console.log(JSON.stringify(result, null, json ? undefined : 2));
        }),
      onFailure: (error) =>
        Effect.gen(function* () {
          const operational = Schema.is(StorageFailure)(error);
          const reason = Schema.is(Invalid)(error)
            ? error.reason
            : operational
              ? `storage failure during ${error.operation}`
              : String(error);
          process.exitCode = operational ? 4 : 1;
          yield* Console.error(`git+: ${reason}`);
          yield* Console.log(
            JSON.stringify({
              version: 1,
              state: operational ? "retryable-failure" : "invalid",
              exitCode: process.exitCode,
              reason,
            }),
          );
        }),
    }),
  );

const request = Command.make(
  "request",
  {
    root: rootFlag,
    repo: repoFlag,
    json: jsonFlag,
    trigger: Flag.choice("trigger", ["manual", "wake"]).pipe(Flag.withDefault("manual")),
  },
  ({ root, repo, json, trigger }) =>
    emit(
      Effect.gen(function* () {
        const directory = yield* repository(root, repo);
        return yield* withRepo(root, repo, Host.request({ directory, repository: repo, trigger }), {
          notify: false,
        });
      }),
      json,
    ),
);

const run = Command.make(
  "run",
  {
    root: rootFlag,
    repo: repoFlag,
    json: jsonFlag,
    ref: Flag.string("ref").pipe(
      Flag.optional,
      Flag.withDescription(
        "Full committed source branch; required for a new run unless configured",
      ),
    ),
    at: Flag.string("at").pipe(
      Flag.optional,
      Flag.withDescription(
        "Explicit ISO 8601 evaluation instant with offset, required for new jobs",
      ),
    ),
    job: Flag.string("job").pipe(
      Flag.optional,
      Flag.withDescription("Resume this job using its saved inputs and limits"),
    ),
    maximumConcepts: Flag.integer("max-concepts").pipe(
      Flag.optional,
      Flag.withDescription("Lower the Concept count ceiling for each pass"),
    ),
    maximumOutcomes: Flag.integer("max-outcomes").pipe(
      Flag.optional,
      Flag.withDescription("Lower the native-outcome count ceiling"),
    ),
    timeout: Flag.string("timeout").pipe(
      Flag.optional,
      Flag.withDescription("Lower the pass time ceiling, for example 120s, 2m or 500ms"),
    ),
    dryRun: Flag.boolean("dry-run").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Check without model calls, journal/cache/proposal writes or request acknowledgement",
      ),
    ),
    propose: Flag.boolean("propose").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Prepare a local draft branch when validated changes exist; otherwise no-proposal",
      ),
    ),
  },
  (options) =>
    emit(
      Effect.gen(function* () {
        const directory = yield* repository(options.root, options.repo);
        let timeoutMs: number | undefined;
        if (Option.isSome(options.timeout)) {
          const matched = /^([0-9]+)(ms|s|m)$/u.exec(options.timeout.value);
          if (matched === null)
            return yield* new Invalid({
              field: "timeout",
              reason: "use a duration such as 120s, 2m or 500ms",
            });
          timeoutMs =
            Number(matched[1]) * (matched[2] === "ms" ? 1 : matched[2] === "s" ? 1000 : 60_000);
        }
        return yield* withRepo(
          options.root,
          options.repo,
          Host.run({
            directory,
            sourceRef: Option.getOrUndefined(options.ref),
            evaluatedAt: Option.getOrUndefined(options.at),
            job: Option.getOrUndefined(options.job),
            maximumConcepts: Option.getOrUndefined(options.maximumConcepts),
            maximumOutcomes: Option.getOrUndefined(options.maximumOutcomes),
            timeoutMs,
            dryRun: options.dryRun,
            propose: options.propose,
          }),
          { notify: false },
        );
      }),
      options.json,
    ),
);

const show = Command.make(
  "show",
  {
    root: rootFlag,
    repo: repoFlag,
    json: jsonFlag,
    job: Flag.string("job").pipe(
      Flag.withDescription("Job identity to inspect, including failed and partial jobs"),
    ),
  },
  ({ root, repo, json, job }) =>
    emit(
      Effect.gen(function* () {
        const directory = yield* repository(root, repo);
        return yield* Host.show(directory, job);
      }),
      json,
    ),
);

export const maintainCommand = Command.make("maintain", {}, () =>
  Console.log("git+ knowledge maintain request | run | show — see --help"),
).pipe(
  Command.withSubcommands([
    request.pipe(
      Command.withDescription("Durably enqueue a manual or authoritative wake notification"),
    ),
    run.pipe(
      Command.withDescription("Check a bounded pass of captured knowledge; resume a saved job"),
    ),
    show.pipe(Command.withDescription("Inspect a durable maintenance job and its pending work")),
  ]),
);
