import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect } from "effect";
import { configuration } from "./RegistryCapacity.ts";

it.effect("requires positive bounded SQL registry limits", () =>
  Effect.gen(function* () {
    const read = (values: Record<string, string>) =>
      configuration().pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
      );
    assert.deepEqual(yield* read({}), { bytes: 1024 ** 2, rows: 128 });
    for (const key of ["GIT_CF_REGISTRY_BYTES", "GIT_CF_REGISTRY_ROWS"])
      for (const value of ["0", "-1", "1.5", "Infinity", "9007199254740992"])
        assert.equal((yield* read({ [key]: value }).pipe(Effect.result))._tag, "Failure");
    assert.equal(
      (yield* read({ GIT_CF_REGISTRY_ROWS: "4097" }).pipe(Effect.result))._tag,
      "Failure",
    );
  }),
);

it.effect("reads independent Node registry limits", () =>
  Effect.gen(function* () {
    const read = (values: Record<string, string>) =>
      configuration("GIT").pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
      );
    assert.deepEqual(
      yield* read({
        GIT_REGISTRY_ROWS: "2",
        GIT_REGISTRY_BYTES: "300",
        GIT_CF_REGISTRY_ROWS: "99",
      }),
      { rows: 2, bytes: 300 },
    );
    for (const key of ["GIT_REGISTRY_ROWS", "GIT_REGISTRY_BYTES"])
      for (const value of ["0", "-1", "1.5", "9007199254740992"])
        assert.equal((yield* read({ [key]: value }).pipe(Effect.result))._tag, "Failure");
  }),
);
