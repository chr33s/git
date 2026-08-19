/**
 * Webhook delivery.
 *
 * Outgoing calls go through Effect's `HttpClient`, so the transport is a
 * layer a test or a host can swap, and the retry is
 * `HttpClient.retryTransient` — which already means exactly what a webhook
 * wants: transport failures, timeouts, 408, 429 and 5xx are retried, and
 * every other 4xx is the receiver saying "never send this again".
 *
 * Delivery is handed to `background`, so a push returns as soon as its refs
 * are durable and a slow subscriber cannot stall it. What "background" means
 * is the host's business: `waitUntil` on Workers, a detached fiber on node.
 *
 * Bodies are signed HMAC-SHA256 over the exact bytes sent, in the
 * `X-Signature-256: sha256=<hex>` form every git host uses, so receivers can
 * verify with the library they already have.
 */
import { bytesToHex } from "../git/Format.ts";
import { Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

import { Hooks, type ReceiveResult } from "../git/Repository.ts";
import { type Subscriber, Subscribers } from "./Subscribers.ts";

/**
 * The registry moved to `Subscribers.ts` when it grew persistence and
 * management; the names stay here because delivery is what most callers mean
 * by "webhooks".
 */
export {
  type NewSubscriber,
  none as subscribersNone,
  of as subscribersOf,
  memory as subscribersMemory,
  sql as subscribersSql,
  type Subscriber,
  Subscribers,
} from "./Subscribers.ts";

/** What a receiver is sent: one entry per ref the push moved. */
export interface PushEvent {
  readonly ref: string;
  readonly before: string | null;
  readonly after: string | null;
}

export const eventOf = (results: ReadonlyArray<ReceiveResult>): ReadonlyArray<PushEvent> =>
  results
    .filter((result) => result.ok)
    .map((result) => ({ ref: result.ref, before: result.from, after: result.to }));

const encoder = new TextEncoder();

export const sign = (body: string, secret: string): Effect.Effect<string> =>
  Effect.promise(async () => {
    const key = await crypto.subtle.importKey(
      "raw",
      encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(body));
    const hex = bytesToHex(new Uint8Array(signature));
    return `sha256=${hex}`;
  });

export interface DeliveryOptions {
  /** Attempts after the first. */
  readonly retries?: number;
  readonly baseDelay?: `${number} millis`;
  readonly timeout?: `${number} millis`;
  readonly concurrency?: number;
  /** Where delivery runs; `Effect.forkDaemon` on node, `waitUntil` on Workers. */
  readonly background?: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<void>;
}

/** Typed like every other failure in the codebase, not a hand-rolled `_tag`. */
export class DeliveryFailed extends Schema.TaggedError<DeliveryFailed>()(
  "DeliveryFailed",
  { url: Schema.String, reason: Schema.String },
  { httpApiStatus: 502 },
) {}

/** One POST, signed and bounded — the unit the retry schedule repeats. */
export const post = Effect.fn("Webhooks.post")(function* (
  subscriber: Subscriber,
  body: string,
  timeout: `${number} millis`,
) {
  const client = yield* HttpClient.HttpClient;
  const signature = yield* sign(body, subscriber.secret);

  yield* client
    .execute(
      HttpClientRequest.post(subscriber.url).pipe(
        HttpClientRequest.setHeaders({
          "content-type": "application/json",
          "x-signature-256": signature,
          "x-event": "push",
        }),
        HttpClientRequest.bodyText(body, "application/json"),
      ),
    )
    .pipe(Effect.timeout(timeout));
});

/**
 * Deliver one push to every subscriber, concurrently, retrying each
 * independently. Failures are logged, never surfaced: a webhook cannot fail
 * a push that already happened.
 */
export const deliver = Effect.fn("Webhooks.deliver")(function* (
  results: ReadonlyArray<ReceiveResult>,
  options?: DeliveryOptions,
) {
  const events = eventOf(results);
  if (events.length === 0) return;

  const subscribers = yield* Subscribers;
  const targets = yield* subscribers.forEvent("push");
  if (targets.length === 0) return;

  const body = JSON.stringify({ event: "push", refs: events });

  // Non-2xx becomes a failure, and only the transient subset is retried —
  // both are properties of the client, so `post` stays a plain request.
  const client = (yield* HttpClient.HttpClient).pipe(
    HttpClient.filterStatusOk,
    HttpClient.retryTransient({
      schedule: Schedule.exponential(options?.baseDelay ?? "200 millis", 2).pipe(Schedule.jittered),
      times: options?.retries ?? 3,
    }),
  );

  yield* Effect.forEach(
    targets,
    (target) =>
      post(target, body, options?.timeout ?? "10000 millis").pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.catchCause((cause) =>
          Effect.logWarning(`webhook delivery to ${target.url} failed`, cause),
        ),
      ),
    { concurrency: options?.concurrency ?? 8, discard: true },
  );
});

/**
 * Hooks that deliver on `post-receive` — the only hook that runs after the
 * refs are durable, which is exactly when a receiver should hear about them.
 */
export const service = (input: {
  readonly subscribers: Subscribers["Service"];
  readonly client: HttpClient.HttpClient;
  readonly options?: DeliveryOptions;
}): Hooks["Service"] => {
  const { client, options, subscribers } = input;
  const background =
    options?.background ??
    (<A, E>(effect: Effect.Effect<A, E>) => Effect.forkDetach(effect).pipe(Effect.asVoid));

  return Hooks.of({
    preReceive: () => Effect.void,
    update: () => Effect.void,
    postReceive: (results) =>
      background(
        deliver(results, options).pipe(
          Effect.provideService(Subscribers, subscribers),
          Effect.provideService(HttpClient.HttpClient, client),
        ),
      ),
  });
};

export const hooks = (
  options?: DeliveryOptions,
): Layer.Layer<Hooks, never, Subscribers | HttpClient.HttpClient> =>
  Layer.effect(
    Hooks,
    Effect.gen(function* () {
      const subscribers = yield* Subscribers;
      const client = yield* HttpClient.HttpClient;
      if (options === undefined) return service({ subscribers, client });
      return service({ subscribers, client, options });
    }),
  );

/** The same hooks with the default transport already supplied. */
export const hooksFetch = (options?: DeliveryOptions) =>
  hooks(options).pipe(Layer.provide(FetchHttpClient.layer));
