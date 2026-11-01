import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { ConfigProvider, Effect } from "effect";
import { ceiling, verify } from "./StorageCapacity.node.ts";

describe("Node storage filesystem ceiling", () => {
  it.effect("is opt-in and decodes an exact positive byte ceiling", () =>
    Effect.gen(function* () {
      const read = (values: Record<string, string>) =>
        ceiling.pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))));
      assert.equal(yield* read({}), undefined);
      assert.equal(yield* read({ GIT_STORAGE_FILESYSTEM_BYTES: "33554432" }), 33554432);
      for (const value of ["0", "-1", "1.5", "Infinity", "9007199254740992", "invalid"]) {
        assert.equal(
          (yield* read({ GIT_STORAGE_FILESYSTEM_BYTES: value }).pipe(Effect.result))._tag,
          "Failure",
        );
      }
    }),
  );

  it("does not access or create storage when disabled", async () => {
    await verify("/nonexistent/git-storage-ceiling-test", undefined);
  });

  it("rejects the system filesystem or an unsupported platform", async () => {
    await assert.rejects(
      verify("/", Number.MAX_SAFE_INTEGER),
      process.platform === "linux" ? /separate filesystem/ : /requires Linux/,
    );
  });

  it("rejects invalid direct ceilings before accessing storage", async () => {
    for (const value of [0, -1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1])
      await assert.rejects(verify("/", value), /invalid storage filesystem ceiling/);
  });
});
