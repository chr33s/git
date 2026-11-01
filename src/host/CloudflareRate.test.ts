import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Logger, Option } from "effect";
import { check, configuration } from "./CloudflareRate.ts";

it.effect("requires explicit namespace opt-in and defaults to 600 per minute", () =>
  Effect.gen(function* () {
    const config = yield* configuration();
    assert.ok(Option.isNone(config.namespace));
    assert.equal(config.limit, 600);
  }).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({})))),
);

for (const [field, values] of [
  ["GIT_CF_RATE_NAMESPACE", ["0", "-1", "1.5", "namespace", "1000000000000000000"]],
  ["GIT_CF_RATE_PER_MINUTE", ["0", "-1", "1.5", "1000001", "Infinity"]],
] as const)
  for (const value of values)
    it.effect(`rejects ${field}=${value}`, () =>
      Effect.gen(function* () {
        assert.equal((yield* configuration().pipe(Effect.result))._tag, "Failure");
      }).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ [field]: value })))),
    );

for (const [ip, key] of [
  ["192.0.2.1", "ip:192.0.2.1"],
  ["2001:0DB8:0:0:0:0:0:1", "ip:2001:db8::1"],
  ["", "unknown"],
  ["999.999.999.999", "unknown"],
  ["private-secret", "unknown"],
] as const)
  it.effect(`uses only parsed platform client identity: ${ip}`, () =>
    Effect.gen(function* () {
      const request = new Request("https://host/repo?token=private-secret", {
        headers: {
          "cf-connecting-ip": ip,
          "x-forwarded-for": "198.51.100.1",
          authorization: "private-secret",
        },
      });
      const result = yield* check(request, {
        limit: async (options) => {
          assert.equal(options.key, key);
          return { success: true };
        },
      });
      assert.equal(result, null);
    }),
  );

for (const outcome of ["limited", "unavailable"] as const)
  it.effect(`refuses ${outcome} without reflecting private input`, () => {
    const messages: string[] = [];
    const logger = Logger.make<unknown, void>(({ message }) => {
      messages.push(JSON.stringify(message));
    });
    return Effect.gen(function* () {
      const result = yield* check(new Request("https://host/private-secret"), {
        limit: async () => {
          if (outcome === "unavailable") throw new Error("private-secret");
          return { success: false };
        },
      });
      assert.ok(result);
      assert.equal(result.status, outcome === "limited" ? 429 : 503);
      assert.equal(result.headers.get("cache-control"), "no-store");
      assert.equal(result.headers.get("retry-after"), "60");
      assert.equal(yield* Effect.promise(() => result.text()), "");
      assert.equal(messages.join(" ").includes("private-secret"), false);
      assert.match(
        messages.join(" "),
        outcome === "limited" ? /rate_limit_exceeded/ : /rate_limit_unavailable/,
      );
    }).pipe(Effect.provide(Logger.layer([logger])));
  });
