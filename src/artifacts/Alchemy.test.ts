/**
 * The upstream shape the local provider must satisfy.
 *
 * `RepoClient.raw` is an eager `ArtifactsRepo` that no off-platform provider can
 * construct, so `Namespace.ts` hands out a stand-in that refuses every use. If
 * alchemy defers `raw` (an `Effect`) the stand-in becomes unnecessary; this
 * compile-time assertion fails `npm run check` so it can be dropped.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import type { Artifacts } from "@alchemy.run/cloudflare";
import { Effect } from "effect";

type Raw = Artifacts.RepoClient["raw"];

/** `true` while `raw` is still an eager handle rather than an Effect. */
type Eager = Raw extends Effect.Effect<infer _A, infer _E, infer _R> ? false : true;
const rawIsEager: Eager = true;

describe("alchemy RepoClient", () => {
  it.effect("keeps RepoClient.raw an eager handle, which the local provider stubs", () =>
    Effect.sync(() => {
      assert.equal(rawIsEager, true);
    }),
  );
});
