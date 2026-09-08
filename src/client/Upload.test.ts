import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Stream } from "effect";
import { StorageFailure } from "../git/Error.ts";
import { prepare } from "./Upload.ts";

it.effect("replays from the start and closes an earlier partially consumed upload", () =>
  Effect.gen(function* () {
    let closed = 0;
    const source = Stream.fromIterable([Uint8Array.of(1), Uint8Array.of(2)]).pipe(
      Stream.ensuring(
        Effect.sync(() => {
          closed++;
        }),
      ),
    );
    const upload = yield* prepare(source);
    const first = yield* Effect.promise(upload.options);
    assert.ok(first.body instanceof ReadableStream);
    const reader = first.body.getReader();
    assert.deepEqual((yield* Effect.promise(() => reader.read())).value, Uint8Array.of(1));
    const second = yield* Effect.promise(upload.options);
    assert.equal(closed, 1);
    assert.deepEqual(
      new Uint8Array(yield* Effect.promise(() => new Response(second.body).arrayBuffer())),
      Uint8Array.of(1, 2),
    );
    assert.equal(closed, 2);
  }).pipe(Effect.scoped),
);

it.effect("scope cleanup stops a source while the fetch-facing reader is locked", () =>
  Effect.gen(function* () {
    const stopped = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const upload = yield* prepare(
        Stream.make(Uint8Array.of(1)).pipe(
          Stream.concat(Stream.never),
          Stream.ensuring(Deferred.succeed(stopped, undefined)),
        ),
      );
      const request = yield* Effect.promise(upload.options);
      assert.ok(request.body instanceof ReadableStream);
      const reader = request.body.getReader();
      assert.deepEqual((yield* Effect.promise(() => reader.read())).value, Uint8Array.of(1));
      // Deliberately retain the outer lock: cleanup owns the inner reader.
    }).pipe(Effect.scoped);
    assert.equal(yield* Deferred.isDone(stopped), true);
  }),
);

it.effect("retains the source failure when Web Streams rejects the request body", () =>
  Effect.gen(function* () {
    const problem = new StorageFailure({
      operation: "read",
      path: "fixture",
      cause: "unavailable",
    });
    const upload = yield* prepare(Stream.fail(problem));
    const request = yield* Effect.promise(upload.options);
    yield* Effect.promise(() => assert.rejects(new Response(request.body).arrayBuffer()));
    const cause = upload.failure();
    assert.ok(cause !== undefined);
    assert.equal(Cause.squash(cause), problem);
  }).pipe(Effect.scoped),
);
