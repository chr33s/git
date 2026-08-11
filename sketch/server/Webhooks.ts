/**
 * Webhook delivery.
 *
 * Today `ServerWebhooks.deliver` (`src/server.webhooks.ts`) loops subscribers,
 * signs the body, `fetch`es with a hand-written retry (`for` loop + `setTimeout`
 * backoff), and swallows failures so a slow endpoint cannot fail a push. It
 * runs inside the receive-pack request, so a subscriber that takes 10s makes
 * the push take 10s.
 *
 * Sketch: the retry policy is a `Schedule` value, delivery is a forked fiber
 * registered with `waitUntil`, and concurrency across subscribers is a
 * parameter. Same behaviour, but the policy is testable with `TestClock`
 * instead of a real 8-second wait.
 */
import { Duration, Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import * as Cloudflare from "alchemy/Cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context } from "effect";
import type { ReceiveResult } from "../git/Repository.ts";
import { Hooks } from "../git/Repository.ts";

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
    const state = yield* Cloudflare.DurableObjectState;
    // `waitUntil` belongs to the invocation, so its context is captured here
    // rather than leaking into the `Hooks` port — same trick as the store
    // adapter, and the reason `Hooks` is platform-agnostic.
    const runtime = yield* Effect.context<RuntimeContext>();

    const deliver = (results: ReadonlyArray<ReceiveResult>) =>
      Effect.gen(function* () {
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
              Effect.retry(policy),
              // A dead subscriber is logged, never fatal — but now that is
              // one combinator instead of a swallowed catch per call site.
              Effect.catchCause((cause) => Effect.logWarning("webhook failed", cause)),
              Effect.timeout(Duration.seconds(10)),
            ),
          { concurrency: 8, discard: true },
        );
      });

    return {
      preReceive: () => Effect.void,
      update: () => Effect.void,
      // Delivery outlives the response without holding it: the push returns
      // as soon as the refs are durable.
      postReceive: (results) =>
        state.waitUntil(deliver(results)).pipe(Effect.provideContext(runtime), Effect.ignore),
    };
  }),
).pipe(Layer.provide(FetchHttpClient.layer));

declare const sign: (
  request: HttpClientRequest.HttpClientRequest,
  secret: string,
) => Effect.Effect<HttpClientRequest.HttpClientRequest>;

export { PushEvent };
