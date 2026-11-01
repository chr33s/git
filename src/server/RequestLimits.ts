import { Config, Context, Effect, Option, Schema } from "effect";
import { defaultImportLimits } from "../git/Pack.ts";

export interface Budgets {
  readonly jsonBodyBytes: number;
  readonly negotiationBytes: number;
  readonly negotiationLines: number;
  readonly fetchGraphSteps: number;
  readonly receiveBytes: number;
  readonly receiveCommands: number;
  readonly receiveCommandBytes: number;
  readonly receiveObjects: number;
  readonly receiveExpandedBytes: number;
  readonly lfsBatchBytes: number;
  readonly lfsBatchObjects: number;
  readonly lfsUploadBytes: number;
}

export const defaults: Budgets = {
  jsonBodyBytes: 8 * 1024 * 1024,
  negotiationBytes: 2 * 1024 * 1024,
  negotiationLines: 20_000,
  fetchGraphSteps: 250_000,
  receiveBytes: 512 * 1024 * 1024,
  receiveCommands: 10_000,
  receiveCommandBytes: 2 * 1024 * 1024,
  receiveObjects: defaultImportLimits.objects,
  receiveExpandedBytes: defaultImportLimits.expandedBytes,
  lfsBatchBytes: 1024 * 1024,
  lfsBatchObjects: 1_000,
  lfsUploadBytes: 1024 * 1024 * 1024,
};

export class RequestLimits extends Context.Service<RequestLimits, Budgets>()(
  "server/RequestLimits",
) {}

// Direct library callers retain bounded defaults; hosts load overrides once
// before admitting requests, so malformed configuration cannot disable a limit.
export const current = Effect.serviceOption(RequestLimits).pipe(
  Effect.map((limits) => Option.getOrElse(limits, () => defaults)),
);

const positive = Schema.Int.check(Schema.isGreaterThan(0));
export const configuration = Effect.fn("RequestLimits.configuration")(function* () {
  return RequestLimits.of({
    jsonBodyBytes: yield* Config.schema(positive, "GIT_JSON_BODY_BYTES").pipe(
      Config.withDefault(defaults.jsonBodyBytes),
    ),
    negotiationBytes: yield* Config.schema(positive, "GIT_NEGOTIATION_BYTES").pipe(
      Config.withDefault(defaults.negotiationBytes),
    ),
    negotiationLines: yield* Config.schema(positive, "GIT_NEGOTIATION_LINES").pipe(
      Config.withDefault(defaults.negotiationLines),
    ),
    fetchGraphSteps: yield* Config.schema(positive, "GIT_FETCH_GRAPH_STEPS").pipe(
      Config.withDefault(defaults.fetchGraphSteps),
    ),
    lfsBatchBytes: yield* Config.schema(positive, "GIT_LFS_BATCH_BYTES").pipe(
      Config.withDefault(defaults.lfsBatchBytes),
    ),
    receiveBytes: yield* Config.schema(positive, "GIT_RECEIVE_BYTES").pipe(
      Config.withDefault(defaults.receiveBytes),
    ),
    receiveObjects: yield* Config.schema(positive, "GIT_RECEIVE_OBJECTS").pipe(
      Config.withDefault(defaults.receiveObjects),
    ),
    receiveExpandedBytes: yield* Config.schema(positive, "GIT_RECEIVE_EXPANDED_BYTES").pipe(
      Config.withDefault(defaults.receiveExpandedBytes),
    ),
    receiveCommands: yield* Config.schema(positive, "GIT_RECEIVE_COMMANDS").pipe(
      Config.withDefault(defaults.receiveCommands),
    ),
    receiveCommandBytes: yield* Config.schema(positive, "GIT_RECEIVE_COMMAND_BYTES").pipe(
      Config.withDefault(defaults.receiveCommandBytes),
    ),
    lfsBatchObjects: yield* Config.schema(positive, "GIT_LFS_BATCH_OBJECTS").pipe(
      Config.withDefault(defaults.lfsBatchObjects),
    ),
    lfsUploadBytes: yield* Config.schema(positive, "GIT_LFS_UPLOAD_BYTES").pipe(
      Config.withDefault(defaults.lfsUploadBytes),
    ),
  });
});
