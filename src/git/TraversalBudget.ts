import { Context, Effect, Option } from "effect";
import { RequestLimitExceeded } from "./Error.ts";

/** Shared across all walks in one request, including retries and exclusions. */
export class TraversalBudget extends Context.Service<
  TraversalBudget,
  {
    readonly spend: (units: number) => Effect.Effect<void, RequestLimitExceeded>;
  }
>()("git/TraversalBudget") {}

export const make = (limit: number): TraversalBudget["Service"] => {
  let remaining = limit;
  return TraversalBudget.of({
    spend: Effect.fn("TraversalBudget.spend")((units: number) =>
      Effect.suspend(() => {
        if (units > remaining)
          return Effect.fail(new RequestLimitExceeded({ budget: "fetch graph steps", limit }));
        remaining -= units;
        return Effect.void;
      }),
    ),
  });
};

// Local maintenance/library calls are not HTTP requests. Hosts opt into a
// shared request budget without adding environment requirements to ports.
export const current = Effect.serviceOption(TraversalBudget).pipe(
  Effect.map(Option.getOrElse(() => TraversalBudget.of({ spend: () => Effect.void }))),
);
