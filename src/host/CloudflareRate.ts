import { Config, Effect, Schema } from "effect";
import { NetAddress } from "effect/net";
import { StorageFailure } from "../git/Error.ts";

export const configuration = Effect.fn("CloudflareRate.configuration")(function* () {
  const namespace = yield* Config.option(
    Config.schema(
      Schema.String.check(Schema.isPattern(/^[1-9][0-9]{0,17}$/)),
      "GIT_CF_RATE_NAMESPACE",
    ),
  );
  const limit = yield* Config.schema(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_000_000)),
    "GIT_CF_RATE_PER_MINUTE",
  ).pipe(Config.withDefault(600));
  return { namespace, limit };
});

interface Limiter {
  readonly limit: (options: { key: string }) => Promise<{ success: boolean }>;
}

/** Platform-generated client IP only; never use forwarded or credential headers. */
export const check = Effect.fn("CloudflareRate.check")(function* (
  request: Request,
  limiter: Limiter,
) {
  const ip = request.headers.get("cf-connecting-ip");
  // Missing or malformed platform metadata shares one conservative bucket.
  const address = NetAddress.ipFromString(ip !== null && ip.length <= 45 ? ip : "");
  const key = address._tag === "Success" ? `ip:${NetAddress.formatIp(address.success)}` : "unknown";
  const verdict = yield* Effect.tryPromise({
    try: () => limiter.limit({ key }),
    catch: () =>
      new StorageFailure({
        operation: "rateLimit",
        path: "binding",
        cause: "rate limiter unavailable",
      }),
  }).pipe(Effect.result);
  if (verdict._tag === "Failure") {
    yield* Effect.logError("rate limiter unavailable; refusing request", {
      component: "CloudflareEdge",
      event: "rate_limit_unavailable",
    });
    return new Response(null, {
      status: 503,
      headers: { "retry-after": "60", "cache-control": "no-store" },
    });
  }
  if (verdict.success.success) return null;
  yield* Effect.logWarning("client request rate exceeded", {
    component: "CloudflareEdge",
    event: "rate_limit_exceeded",
  });
  return new Response(null, {
    status: 429,
    headers: { "retry-after": "60", "cache-control": "no-store" },
  });
});
