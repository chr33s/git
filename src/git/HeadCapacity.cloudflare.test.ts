import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Result } from "effect";
import * as Capacity from "./HeadCapacity.cloudflare.ts";

it.effect("Cloudflare HEAD configuration validates the byte budget", () =>
  Effect.gen(function* () {
    const configured = (values: { GIT_CF_HEAD_BYTES?: number | string }) =>
      Capacity.configuration.pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
      );
    assert.equal(yield* configured({}), 1024);
    for (const value of [1, 65536])
      assert.equal(yield* configured({ GIT_CF_HEAD_BYTES: value }), value);
    for (const value of [0, -1, 0.5, 65537, "invalid"])
      assert.ok(Result.isFailure(yield* Effect.result(configured({ GIT_CF_HEAD_BYTES: value }))));
  }),
);
