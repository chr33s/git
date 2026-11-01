import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Deferred, Effect, Fiber } from "effect";
import { make } from "./CloudflareAdmission.ts";
import { TestClock } from "effect/testing";

const one = ConfigProvider.layer(ConfigProvider.fromUnknown({ GIT_CF_MAX_ACTIVE: "1" }));
const healthy = Effect.sync(() => new Response(null, { status: 204 }));

for (const timeout of ["absolute", "idle"] as const)
  for (const uninterruptible of [false, true])
    it.effect(
      `${timeout} deadline cancels work and retains capacity until settlement (uninterruptible=${uninterruptible})`,
      () =>
        Effect.gen(function* () {
          const admit = yield* make();
          const entered = yield* Deferred.make<void>();
          const finish = yield* Deferred.make<void>();
          let aborted = false;
          let cleaned = false;
          const work = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(finish)),
            Effect.andThen(healthy),
            Effect.ensuring(
              Effect.sync(() => {
                cleaned = true;
              }),
            ),
          );
          const held = yield* admit(
            uninterruptible ? Effect.uninterruptible(work) : work,
            () => {
              aborted = true;
            },
            timeout === "idle" ? Effect.sleep(50) : Effect.never,
          ).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* TestClock.adjust(50);
          assert.equal(aborted, true);
          if (uninterruptible) {
            assert.equal(cleaned, false);
            assert.equal((yield* admit(healthy)).status, 503);
            yield* Deferred.succeed(finish, undefined);
          }
          const result = yield* Fiber.join(held);
          assert.equal(result.status, timeout === "idle" ? 408 : 504);
          assert.equal(result.headers.get("cache-control"), "no-store");
          assert.equal(cleaned, true);
          assert.equal((yield* admit(healthy)).status, 204);
        }).pipe(
          Effect.provide(
            ConfigProvider.layer(
              ConfigProvider.fromUnknown({
                GIT_CF_MAX_ACTIVE: "1",
                GIT_CF_REQUEST_DEADLINE_MS: timeout === "idle" ? "1000" : "50",
              }),
            ),
          ),
        ),
    );

it.effect("timeout wins even when abort synchronously resolves the separate router runtime", () =>
  Effect.gen(function* () {
    const admit = yield* make();
    const entered = yield* Deferred.make<void>();
    let finish: () => void = () => undefined;
    const response = new Promise<Response>((resolve) => {
      finish = () => resolve(new Response(null, { status: 499 }));
    });
    const held = yield* admit(
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Effect.promise(() => response)),
        Effect.uninterruptible,
      ),
      finish,
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* TestClock.adjust(50);
    assert.equal((yield* Fiber.join(held)).status, 504);
    assert.equal((yield* admit(healthy)).status, 204);
  }).pipe(
    Effect.provide(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          GIT_CF_MAX_ACTIVE: "1",
          GIT_CF_REQUEST_DEADLINE_MS: "50",
        }),
      ),
    ),
  ),
);

for (const component of ["CloudflareHost", "CloudflareEdge"] as const)
  it.effect(`${component} applies configured response expiry and recovers capacity`, () =>
    Effect.gen(function* () {
      const admit = yield* make(component);
      const response = yield* admit(Effect.succeed(new Response(new ReadableStream())));
      assert.equal((yield* admit(healthy)).status, 503);
      assert.ok(response.body);
      const reader = response.body.getReader();
      yield* Effect.promise(() => assert.rejects(reader.read(), /response idle deadline exceeded/));
      yield* Effect.yieldNow;
      assert.equal((yield* admit(healthy)).status, 204);
    }).pipe(
      Effect.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            GIT_CF_MAX_ACTIVE: "1",
            GIT_CF_EDGE_MAX_ACTIVE: "1",
            GIT_CF_RESPONSE_IDLE_MS: "10",
            GIT_CF_RESPONSE_DEADLINE_MS: "100",
          }),
        ),
      ),
    ),
  );

it.effect("uses an independent configured pool for the edge", () =>
  Effect.gen(function* () {
    const host = yield* make();
    const edge = yield* make("CloudflareEdge");
    const source = new TransformStream<Uint8Array, Uint8Array>();
    const held = yield* edge(Effect.succeed(new Response(source.readable)));
    assert.equal((yield* edge(healthy)).status, 503);
    assert.equal((yield* host(healthy)).status, 204);
    assert.ok(held.body);
    yield* Effect.promise(() => held.body?.cancel() ?? Promise.resolve());
    yield* Effect.yieldNow;
    assert.equal((yield* edge(healthy)).status, 204);
  }).pipe(
    Effect.provide(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          GIT_CF_MAX_ACTIVE: "2",
          GIT_CF_EDGE_MAX_ACTIVE: "1",
        }),
      ),
    ),
  ),
);

it.effect("refuses queued work without executing it and recovers after the handler settles", () =>
  Effect.gen(function* () {
    const admit = yield* make();
    const entered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    const held = yield* admit(
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Deferred.await(finish)),
        Effect.andThen(healthy),
      ),
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    const refused = yield* admit(Effect.die("overflow handler must not run"));
    assert.equal(refused.status, 503);
    assert.equal(refused.headers.get("retry-after"), "1");
    assert.equal(refused.headers.get("cache-control"), "no-store");
    yield* Deferred.succeed(finish, undefined);
    yield* Fiber.join(held);
    assert.equal((yield* admit(healthy)).status, 204);
  }).pipe(Effect.provide(one)),
);

it.effect("releases failed handlers", () =>
  Effect.gen(function* () {
    const admit = yield* make();
    yield* admit(Effect.fail("unavailable")).pipe(Effect.result);
    assert.equal((yield* admit(healthy)).status, 204);
    yield* admit(Effect.die("defect")).pipe(Effect.exit);
    assert.equal((yield* admit(healthy)).status, 204);
  }).pipe(Effect.provide(one)),
);

it.effect("keeps interrupted uninterruptible writes admitted until they settle", () =>
  Effect.gen(function* () {
    const admit = yield* make();
    const entered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    const held = yield* admit(
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Deferred.await(finish)),
        Effect.andThen(healthy),
        Effect.uninterruptible,
      ),
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(held).pipe(Effect.forkScoped({ startImmediately: true }));
    assert.equal((yield* admit(healthy)).status, 503);
    yield* Deferred.succeed(finish, undefined);
    yield* Fiber.await(held);
    assert.equal((yield* admit(healthy)).status, 204);
  }).pipe(Effect.provide(one)),
);

it.effect("releases an interrupted handler after its cleanup", () =>
  Effect.gen(function* () {
    const admit = yield* make();
    const entered = yield* Deferred.make<void>();
    const held = yield* admit(
      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    ).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(held);
    assert.equal((yield* admit(healthy)).status, 204);
  }).pipe(Effect.provide(one)),
);

for (const key of [
  "GIT_CF_MAX_ACTIVE",
  "GIT_CF_RESPONSE_IDLE_MS",
  "GIT_CF_RESPONSE_DEADLINE_MS",
  "GIT_CF_REQUEST_DEADLINE_MS",
])
  for (const invalid of ["0", "-1", "1.5", "2147483648", "Infinity"])
    it.effect(`rejects invalid ${key} configuration ${invalid}`, () =>
      Effect.gen(function* () {
        const result = yield* make().pipe(Effect.result);
        assert.equal(result._tag, "Failure");
      }).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ [key]: invalid })))),
    );

for (const outcome of ["complete", "failure", "cancel"] as const)
  it.effect(`holds capacity through response delivery and releases on ${outcome}`, () =>
    Effect.gen(function* () {
      const admit = yield* make();
      const source = new TransformStream<Uint8Array, Uint8Array>();
      const writer = source.writable.getWriter();
      const response = yield* admit(Effect.succeed(new Response(source.readable)));
      assert.equal((yield* admit(healthy)).status, 503);
      assert.ok(response.body);
      const reader = response.body.getReader();
      yield* Effect.promise(async () => {
        if (outcome === "cancel") await reader.cancel();
        else {
          const read = reader.read();
          if (outcome === "failure") {
            const fault = new Error("storage unavailable");
            await writer.abort(fault);
            await assert.rejects(read, fault);
          } else {
            await writer.close();
            assert.equal((await read).done, true);
          }
        }
      });
      // A reader observes EOF/cancellation before the pipe's settlement
      // callback runs. Let the transport's queued promise callbacks finish.
      yield* Effect.yieldNow;
      assert.equal((yield* admit(healthy)).status, 204);
    }).pipe(Effect.provide(one)),
  );
