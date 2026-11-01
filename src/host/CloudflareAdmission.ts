import { Config, Effect, Exit, Schema } from "effect";
import * as ResponseDelivery from "./ResponseDelivery.ts";

const positive = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(2_147_483_647),
);

/** One pool per runtime instance, including queued work and response delivery. */
export const make = Effect.fn("CloudflareAdmission.make")(function* (
  component: "CloudflareHost" | "CloudflareEdge" = "CloudflareHost",
) {
  const limit = yield* Config.schema(
    positive,
    component === "CloudflareHost" ? "GIT_CF_MAX_ACTIVE" : "GIT_CF_EDGE_MAX_ACTIVE",
  ).pipe(Config.withDefault(component === "CloudflareHost" ? 8 : 32));
  const timeouts = {
    idleMs: yield* Config.schema(positive, "GIT_CF_RESPONSE_IDLE_MS").pipe(
      Config.withDefault(30_000),
    ),
    deadlineMs: yield* Config.schema(positive, "GIT_CF_RESPONSE_DEADLINE_MS").pipe(
      Config.withDefault(300_000),
    ),
  };
  const requestDeadlineMs = yield* Config.schema(positive, "GIT_CF_REQUEST_DEADLINE_MS").pipe(
    Config.withDefault(300_000),
  );
  let active = 0;

  return <E, R>(
    request: Effect.Effect<Response, E, R>,
    abortRequest: () => void = () => undefined,
    uploadIdle: Effect.Effect<void> = Effect.never,
  ): Effect.Effect<Response, E, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        if (active >= limit)
          return Effect.logWarning("request admission full; retry after capacity recovers", {
            component,
            event: "admission_rejected",
            limit,
          }).pipe(
            Effect.as(
              new Response(null, {
                status: 503,
                headers: { "retry-after": "1", "cache-control": "no-store" },
              }),
            ),
          );
        active += 1;
        let released = false;
        const release = () => {
          if (released) return;
          released = true;
          active -= 1;
        };
        const expired = Effect.sleep(requestDeadlineMs).pipe(
          Effect.andThen(
            Effect.logWarning("request deadline reached; cancelling outstanding work", {
              component,
              event: "request_deadline_exceeded",
              deadlineMs: requestDeadlineMs,
            }),
          ),
          Effect.andThen(
            Effect.sync(() => {
              return new Response(null, { status: 504, headers: { "cache-control": "no-store" } });
            }),
          ),
        );
        // raceFirst waits for the losing fiber's interruption/finalizers. Never
        // detach a timed-out mutation and release its admission slot early.
        return restore(
          Effect.raceFirst(
            request,
            Effect.raceFirst(
              expired,
              uploadIdle.pipe(
                Effect.andThen(
                  Effect.logWarning("upload idle deadline reached", {
                    component,
                    event: "upload_idle_exceeded",
                  }),
                ),
                Effect.as(
                  new Response(null, { status: 408, headers: { "cache-control": "no-store" } }),
                ),
              ),
            ),
            {
              onWinner: ({ index }) => {
                // Select the timeout result before aborting the router's separate
                // runtime, so its cancellation response cannot win this race.
                if (index === 1) abortRequest();
              },
            },
          ),
        ).pipe(
          Effect.map((response) => ResponseDelivery.track(response, release, timeouts)),
          // Cancellation waits for any uninterruptible mutation to settle. Do not
          // release merely because the caller's transport has disconnected.
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Effect.sync(release) : Effect.void)),
        );
      }),
    );
});
