/**
 * Webhook delivery.
 *
 * The retry policy is a `Schedule` value rather than a hand-rolled loop, and
 * the combinator order is the part that is easy to get wrong: the timeout
 * bounds each *attempt*, the retry wraps the timeout, and the catch is
 * outermost — put the catch inside and the retry never sees a failure to
 * retry.
 *
 * Delivery is handed to `background`, so a push returns as soon as its refs
 * are durable and a slow subscriber cannot stall it. What "background" means
 * is the host's business: `waitUntil` on Workers, a detached fiber on node.
 *
 * Bodies are signed HMAC-SHA256 over the exact bytes sent, in the
 * `X-Signature-256: sha256=<hex>` form every git host uses, so receivers can
 * verify with the library they already have.
 */
import { Context, Effect, Layer, Schedule } from "effect";

import { Hooks, type ReceiveResult } from "../git/Repository.ts";

export interface Subscriber {
  readonly id: string;
  readonly url: string;
  /** Shared with the receiver; signs the body, never sent. */
  readonly secret: string;
}

export class Subscribers extends Context.Service<
  Subscribers,
  {
    readonly forEvent: (event: "push") => Effect.Effect<ReadonlyArray<Subscriber>>;
  }
>()("server/Subscribers") {}

/** No subscribers: the default, and what every existing test composes. */
export const subscribersNone = Layer.succeed(Subscribers, {
  forEvent: () => Effect.succeed([]),
});

export const subscribersOf = (subscribers: ReadonlyArray<Subscriber>) =>
  Layer.succeed(Subscribers, { forEvent: () => Effect.succeed(subscribers) });

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
    const hex = [...new Uint8Array(signature)]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
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

/**
 * A 4xx is the receiver saying "never send this again" — retrying is wrong.
 * Anything else (5xx, a timeout, a dropped connection) is worth another try.
 */
const worthRetrying = (error: { readonly status?: number }): boolean =>
  error.status === undefined || error.status < 400 || error.status >= 500;

class DeliveryFailure {
  readonly _tag = "DeliveryFailure";
  readonly status: number | undefined;
  readonly cause: unknown;
  constructor(status: number | undefined, cause: unknown) {
    this.status = status;
    this.cause = cause;
  }
}

/** One POST, signed, bounded — the unit the retry schedule repeats. */
export const post = (
  subscriber: Subscriber,
  body: string,
  timeout: `${number} millis`,
): Effect.Effect<void, DeliveryFailure> =>
  Effect.gen(function* () {
    const signature = yield* sign(body, subscriber.secret);
    const response = yield* Effect.tryPromise({
      try: () =>
        fetch(subscriber.url, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-signature-256": signature,
            "x-event": "push",
          },
          body,
        }),
      catch: (cause) => new DeliveryFailure(undefined, cause),
    });
    if (!response.ok) return yield* Effect.fail(new DeliveryFailure(response.status, undefined));
  }).pipe(Effect.timeout(timeout), Effect.mapError(toFailure));

const toFailure = (error: unknown): DeliveryFailure =>
  error instanceof DeliveryFailure ? error : new DeliveryFailure(undefined, error);

/**
 * Deliver one push to every subscriber, concurrently, retrying each
 * independently. Failures are logged, never surfaced: a webhook cannot fail
 * a push that already happened.
 */
export const deliver = (
  results: ReadonlyArray<ReceiveResult>,
  options?: DeliveryOptions,
): Effect.Effect<void, never, Subscribers> =>
  Effect.gen(function* () {
    const events = eventOf(results);
    if (events.length === 0) return;

    const subscribers = yield* Subscribers;
    const targets = yield* subscribers.forEvent("push");
    if (targets.length === 0) return;

    const body = JSON.stringify({ event: "push", refs: events });
    const schedule = Schedule.exponential(options?.baseDelay ?? "200 millis", 2).pipe(
      Schedule.jittered,
    );

    yield* Effect.forEach(
      targets,
      (target) =>
        post(target, body, options?.timeout ?? "10000 millis").pipe(
          Effect.retry({
            schedule,
            times: options?.retries ?? 3,
            while: worthRetrying,
          }),
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
export const hooks = (options?: DeliveryOptions): Layer.Layer<Hooks, never, Subscribers> =>
  Layer.effect(
    Hooks,
    Effect.gen(function* () {
      const subscribers = yield* Subscribers;
      const background =
        options?.background ??
        (<A, E>(effect: Effect.Effect<A, E>) => Effect.forkDetach(effect).pipe(Effect.asVoid));

      return Hooks.of({
        preReceive: () => Effect.void,
        update: () => Effect.void,
        postReceive: (results) =>
          background(
            deliver(results, options).pipe(Effect.provideService(Subscribers, subscribers)),
          ),
      });
    }),
  );
