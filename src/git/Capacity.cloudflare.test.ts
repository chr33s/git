import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect } from "effect";
import { configuration } from "./Capacity.cloudflare.ts";
import * as MetadataCapacity from "./MetadataCapacity.cloudflare.ts";

it.effect("requires positive, bounded ref metadata limits", () =>
  Effect.gen(function* () {
    const read = (values: Record<string, string>) =>
      MetadataCapacity.configuration().pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
      );
    assert.deepEqual(yield* read({}), { bytes: 8 * 1024 ** 2, rows: 100_000 });
    for (const key of ["GIT_CF_REF_METADATA_BYTES", "GIT_CF_REF_METADATA_ROWS"])
      for (const value of ["0", "-1", "1.5", "Infinity", "9007199254740992"])
        assert.equal((yield* read({ [key]: value }).pipe(Effect.result))._tag, "Failure");
    assert.equal(
      (yield* read({ GIT_CF_REF_METADATA_ROWS: "1000001" }).pipe(Effect.result))._tag,
      "Failure",
    );
  }),
);

it.effect("requires positive, bounded retained Git limits", () =>
  Effect.gen(function* () {
    const read = (values: Record<string, string>) =>
      configuration().pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
      );
    assert.deepEqual(yield* read({}), { bytes: 10 * 1024 ** 3, keys: 100_000 });
    for (const key of ["GIT_CF_RETAINED_BYTES", "GIT_CF_RETAINED_KEYS"])
      for (const value of ["0", "-1", "1.5", "Infinity", "9007199254740992"])
        assert.equal((yield* read({ [key]: value }).pipe(Effect.result))._tag, "Failure");
    assert.equal(
      (yield* read({ GIT_CF_RETAINED_KEYS: "1000001" }).pipe(Effect.result))._tag,
      "Failure",
    );
  }),
);
