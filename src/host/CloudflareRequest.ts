import { Cause, Clock, Effect, Exit, Stream } from "effect";
import { StorageFailure } from "../git/Error.ts";
import { HttpBody, HttpServerResponse } from "effect/http";

/** Fixed-field telemetry and redacted failures at both Cloudflare HTTP boundaries. */
export const respond = Effect.fn("CloudflareRequest.respond")(function* <E, R>(
  request: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
  signal: AbortSignal,
  component: "CloudflareEdge" | "CloudflareHost",
) {
  const started = yield* Clock.currentTimeMillis;
  const response = yield* request.pipe(
    Effect.catchCause(() =>
      signal.aborted
        ? Effect.succeed(HttpServerResponse.empty({ status: 499 }))
        : Effect.logError("request failed; inspect storage and service availability", {
            component,
            event: "request_failed",
          }).pipe(
            Effect.as(
              HttpServerResponse.empty({ status: 500, headers: { "cache-control": "no-store" } }),
            ),
          ),
    ),
  );
  // Alchemy passes a raw Web Response through unchanged. Its wrapper status
  // defaults to 200, so observe the actual platform response in that case.
  const status =
    response.body._tag === "Raw" && response.body.body instanceof Response
      ? response.body.body.status
      : response.status;
  yield* Effect.logInfo("response ready", {
    component,
    event: "response_ready",
    status,
    durationMs: Math.max(0, (yield* Clock.currentTimeMillis) - started),
  });
  let bytes = 0;
  const observe = (body: Stream.Stream<Uint8Array, unknown>) =>
    body.pipe(
      Stream.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Stream.failCause(cause)
          : Stream.fail(
              new StorageFailure({
                operation: "response",
                path: "stream",
                cause: "response body unavailable",
              }),
            ),
      ),
      Stream.tap((chunk) =>
        Effect.sync(() => {
          bytes += chunk.byteLength;
        }),
      ),
      Stream.onExit((exit) =>
        Effect.gen(function* () {
          const cancelled =
            Exit.isFailure(exit) && (signal.aborted || Cause.hasInterruptsOnly(exit.cause));
          const failed = Exit.isFailure(exit) && !cancelled;
          const event = failed
            ? "response_body_failed"
            : cancelled
              ? "response_body_cancelled"
              : "response_body_complete";
          const fields = {
            component,
            event,
            status,
            bytes,
            durationMs: Math.max(0, (yield* Clock.currentTimeMillis) - started),
          };
          if (failed)
            yield* Effect.logError(
              "response body failed; verify storage and retry the transfer",
              fields,
            );
          else yield* Effect.logInfo("response body settled", fields);
        }),
      ),
    );
  if (response.body._tag === "Stream") {
    return HttpServerResponse.setBody(
      response,
      HttpBody.stream(
        observe(response.body.stream),
        response.body.contentType,
        response.body.contentLength,
      ),
    );
  }
  if (response.body._tag !== "Raw" || !(response.body.body instanceof Response)) return response;
  const raw = response.body.body;
  if (raw.body === null) return response;
  const source = raw.body;
  const body = yield* Stream.toReadableStreamEffect(
    observe(Stream.fromReadableStream({ evaluate: () => source, onError: (cause) => cause })),
    { strategy: { highWaterMark: 0 } },
  );
  return HttpServerResponse.raw(new Response(body, raw));
});
