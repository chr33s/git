/**
 * Webhook delivery.
 *
 * Today `ServerWebhooks.deliver` (`src/server.webhooks.ts`) loops subscribers,
 * signs the body, `fetch`es with a hand-written retry (`for` loop + `setTimeout`
 * backoff), and swallows failures so a slow endpoint cannot fail a push. It
 * runs inside the receive-pack request, so a subscriber that takes 10s makes
 * the push take 10s.
 *
 * Sketch: the retry policy is a `Schedule` value, delivery is handed to
 * `RepoHost.background`, and concurrency across subscribers is a parameter.
 * Same behaviour, but the policy is testable with `TestClock` instead of a real
 * 8-second wait — and this file no longer knows whether "background" means
 * `waitUntil` or a daemon fiber.
 */
import { Duration, Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { Context } from "effect";
import { RepoHost } from "../host/Host.sketch.ts";
import type { ReceiveResult } from "../git/Repository.sketch.ts";
import { Hooks } from "../git/Repository.sketch.ts";

export class Subscribers extends Context.Service<
  Subscribers,
  {
    readonly forEvent: (event: "push") => Effect.Effect<ReadonlyArray<Subscriber>>;
  }
>()("server/Subscribers") {}

export interface Subscriber {
  readonly id: number;
  readonly url: string;
  readonly secret: string;
}

const PushEvent = Schema.Struct({
  ref: Schema.String,
  before: Schema.NullOr(Schema.String),
  after: Schema.NullOr(Schema.String),
});

/** Exponential backoff with jitter, capped at four attempts — a value, not a loop. */
const policy = {
  schedule: Schedule.exponential(Duration.seconds(1), 2).pipe(Schedule.jittered),
  times: 4,
} as const;

export const layer = Layer.effect(
  Hooks,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const subscribers = yield* Subscribers;
    const host = yield* RepoHost;

    const deliver = Effect.fn("Webhooks.deliver")(function* (
      results: ReadonlyArray<ReceiveResult>,
    ) {
      const targets = yield* subscribers.forEvent("push");
      const body = results.map((result) => ({
        ref: result.ref,
        before: result.from,
        after: result.to,
      }));

      yield* Effect.forEach(
        targets,
        (target) =>
          HttpClientRequest.post(target.url).pipe(
            HttpClientRequest.bodyJson(body),
            Effect.flatMap((request) => sign(request, target.secret)),
            Effect.flatMap(client.execute),
            // Order matters, and it is the kind of thing a hand-rolled retry
            // loop gets wrong: the timeout bounds each *attempt*, the retry
            // wraps the timeout, and the catch is outermost — inside it, the
            // retry would never see a failure to retry.
            Effect.timeout(Duration.seconds(10)),
            Effect.retry(policy),
            Effect.catchCause((cause) => Effect.logWarning("webhook failed", cause)),
          ),
        { concurrency: 8, discard: true },
      );
    });

    return {
      preReceive: () => Effect.void,
      update: () => Effect.void,
      // Delivery outlives the response without holding it: the push returns as
      // soon as the refs are durable, and the host keeps the work alive.
      postReceive: (results) => host.background(deliver(results)),
    };
  }),
).pipe(Layer.provide(FetchHttpClient.layer));

declare const sign: (
  request: HttpClientRequest.HttpClientRequest,
  secret: string,
) => Effect.Effect<HttpClientRequest.HttpClientRequest>;

export { PushEvent };
