import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { TestClock } from "effect/testing";
import { Effect, Logger, Stream } from "effect";
import { HttpServerResponse } from "effect/http";
import { respond } from "./CloudflareRequest.ts";

const secret = "private-token-path-and-body";
for (const component of ["CloudflareEdge", "CloudflareHost"] as const) {
  for (const aborted of [false, true]) {
    it.effect(`${component} redacts a failing request (aborted=${aborted})`, () => {
      const messages: string[] = [];
      const logger = Logger.make<unknown, void>(({ message }) => {
        messages.push(JSON.stringify(message));
      });
      return Effect.gen(function* () {
        const controller = new AbortController();
        if (aborted) controller.abort(secret);
        const result = yield* respond(Effect.die(new Error(secret)), controller.signal, component);
        assert.equal(result.status, aborted ? 499 : 500);
        if (!aborted) assert.equal(result.headers["cache-control"], "no-store");
        assert.equal(messages.length, aborted ? 1 : 2);
        assert.match(messages.join(""), /response_ready/);
        assert.equal(messages.join("").includes("request_failed"), !aborted);
        assert.equal(messages.join("").includes(secret), false);
      }).pipe(Effect.provide(Logger.layer([logger])));
    });
  }
}

for (const raw of [false, true]) {
  it.effect(`observes actual response status without consuming the body (raw=${raw})`, () => {
    const messages: string[] = [];
    const logger = Logger.make<unknown, void>(({ message }) => {
      messages.push(JSON.stringify(message));
    });
    return Effect.gen(function* () {
      const response = raw
        ? HttpServerResponse.raw(new Response(secret, { status: 503 }))
        : HttpServerResponse.text(secret, { status: 401 });
      const result = yield* respond(
        Effect.gen(function* () {
          yield* TestClock.adjust(25);
          return response;
        }),
        new AbortController().signal,
        "CloudflareEdge",
      );
      if (!raw) assert.equal(result, response);
      if (result.body._tag === "Raw" && result.body.body instanceof Response)
        assert.equal(result.body.body.bodyUsed, false);
      assert.equal(messages.length, 1);
      assert.match(messages.join(""), new RegExp(`"status":${raw ? 503 : 401}`));
      assert.match(messages.join(""), /"durationMs":25/);
      assert.equal(messages.join("").includes(secret), false);
      if (result.body._tag === "Raw" && result.body.body instanceof Response) {
        const body = result.body.body.body;
        if (body !== null) yield* Effect.promise(() => body.cancel());
      }
    }).pipe(Effect.provide(Logger.layer([logger])));
  });
}

for (const outcome of ["complete", "failed", "cancelled"] as const) {
  it.effect(`observes streamed response ${outcome} and redacts stream errors`, () => {
    const messages: string[] = [];
    const logger = Logger.make<unknown, void>(({ message }) => {
      messages.push(JSON.stringify(message));
    });
    return Effect.gen(function* () {
      let cancelled = false;
      let pulls = 0;
      const source = new ReadableStream<Uint8Array>(
        {
          pull(controller) {
            pulls++;
            if (pulls === 1) controller.enqueue(new Uint8Array([1, 2, 3]));
            else if (outcome === "failed") controller.error(new Error(secret));
            else controller.close();
          },
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const response = yield* respond(
        Effect.succeed(HttpServerResponse.raw(new Response(source, { status: 200 }))),
        new AbortController().signal,
        "CloudflareEdge",
      );
      assert.equal(pulls, 0, "telemetry must not pull the response ahead of its consumer");
      assert.equal(response.body._tag, "Raw");
      if (response.body._tag !== "Raw" || !(response.body.body instanceof Response)) return;
      const body = response.body.body.body;
      assert.ok(body);
      const reader = body.getReader();
      assert.deepEqual(
        (yield* Effect.promise(() => reader.read())).value,
        new Uint8Array([1, 2, 3]),
      );
      if (outcome === "cancelled") {
        yield* Effect.promise(() => reader.cancel());
        assert.equal(cancelled, true);
      } else if (outcome === "failed") {
        yield* Effect.promise(() =>
          assert.rejects(reader.read(), (error: Error) => !String(error).includes(secret)),
        );
      } else assert.equal((yield* Effect.promise(() => reader.read())).done, true);
      assert.match(messages.join(""), new RegExp(`response_body_${outcome}`));
      assert.match(messages.join(""), /"bytes":3/);
      assert.equal(messages.join("").includes(secret), false);
    }).pipe(Effect.provide(Logger.layer([logger])));
  });
}

it.effect("cancels an unread response without pulling source bytes", () => {
  const messages: string[] = [];
  const logger = Logger.make<unknown, void>(({ message }) => {
    messages.push(JSON.stringify(message));
  });
  return Effect.gen(function* () {
    let cancelled = false;
    let pulls = 0;
    const source = new ReadableStream<Uint8Array>(
      {
        pull() {
          pulls++;
        },
        cancel() {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
    const response = yield* respond(
      Effect.succeed(HttpServerResponse.raw(new Response(source))),
      new AbortController().signal,
      "CloudflareHost",
    );
    assert.equal(response.body._tag, "Raw");
    if (response.body._tag !== "Raw" || !(response.body.body instanceof Response)) return;
    const body = response.body.body.body;
    assert.ok(body);
    yield* Effect.promise(() => body.cancel());
    assert.equal(cancelled, true);
    assert.equal(pulls, 0);
    assert.match(messages.join(""), /response_body_cancelled/);
  }).pipe(Effect.provide(Logger.layer([logger])));
});

for (const outcome of ["complete", "failed", "defect"] as const) {
  it.effect(`observes native Effect HTTP streaming ${outcome}`, () => {
    const messages: string[] = [];
    const logger = Logger.make<unknown, void>(({ message }) => {
      messages.push(JSON.stringify(message));
    });
    return Effect.gen(function* () {
      const stream = Stream.concat(
        Stream.succeed(new Uint8Array([1, 2, 3])),
        outcome === "complete"
          ? Stream.empty
          : outcome === "failed"
            ? Stream.fail(new Error(secret))
            : Stream.die(secret),
      );
      const response = yield* respond(
        Effect.succeed(
          HttpServerResponse.stream(stream, {
            status: 201,
            headers: { "x-fixture": "preserved" },
            contentType: "application/octet-stream",
          }),
        ),
        new AbortController().signal,
        "CloudflareEdge",
      );
      assert.equal(response.status, 201);
      assert.equal(response.headers["x-fixture"], "preserved");
      assert.equal(response.body._tag, "Stream");
      if (response.body._tag !== "Stream") return;
      const result = yield* Stream.runDrain(response.body.stream).pipe(Effect.result);
      assert.equal(result._tag, outcome === "complete" ? "Success" : "Failure");
      if (result._tag === "Failure") assert.equal(String(result.failure).includes(secret), false);
      assert.match(
        messages.join(""),
        new RegExp(outcome === "complete" ? "response_body_complete" : "response_body_failed"),
      );
      assert.match(messages.join(""), /"bytes":3/);
      assert.equal(messages.join("").includes(secret), false);
    }).pipe(Effect.provide(Logger.layer([logger])));
  });
}
