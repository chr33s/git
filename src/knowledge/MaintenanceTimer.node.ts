/** Operator-owned host loop. It runs maintenance directly, including when refs are quiet. */
import { DateTime, Effect, Schedule } from "effect";
import { Invalid } from "../git/Error.ts";
import * as Host from "./Maintenance.node.ts";

export type Result = Effect.Success<ReturnType<typeof Host.run>>;

export const run = Effect.fn("knowledge.MaintenanceTimer.run")(function* <E, R>(input: {
  readonly directory: string;
  readonly sourceRef?: string | undefined;
  readonly intervalMs: number;
  readonly propose?: boolean | undefined;
  readonly onResult: (result: Result) => Effect.Effect<void, E, R>;
}) {
  if (!Number.isSafeInteger(input.intervalMs) || input.intervalMs < 1)
    return yield* new Invalid({
      field: "intervalMs",
      reason: "timer interval must be a positive safe integer",
    });
  let continuation: string | undefined;
  const pass = Effect.gen(function* () {
    const result = yield* Host.run({
      directory: input.directory,
      sourceRef: input.sourceRef,
      evaluatedAt: continuation === undefined ? DateTime.formatIso(yield* DateTime.now) : undefined,
      job: continuation,
      propose: input.propose,
    });
    continuation = result.exitCode === 2 ? result.job.id : undefined;
    yield* input.onResult(result);
  });
  // Operational failures remain visible to the host supervisor; no hidden retries or timers.
  yield* pass.pipe(Effect.repeat(Schedule.spaced(input.intervalMs)));
});
