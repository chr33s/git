/**
 * The upstream blocker, patched locally.
 *
 * One interface change is needed upstream before a third-party provider can
 * satisfy the Artifacts binding:
 * `RepoClient.raw` must be deferred (an `Effect`) rather than an eager
 * `ArtifactsRepo` no off-platform provider can construct.
 * `patches/alchemy+2.0.0-beta.74.patch` applies exactly that change — the one
 * proposed upstream — via `patch-package` on `postinstall`.
 *
 * The assertions here are compile-time: if the patch stopped applying, `raw`
 * reverts to an eager handle and this file fails `npm run check`.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs";
import { describe, it } from "@effect/vitest";

import type { Artifacts } from "alchemy/Cloudflare";
import { Effect } from "effect";

type Raw = Artifacts.RepoClient["raw"];

/** `true` only when `raw` is an Effect — the patched, provider-friendly shape. */
type Deferred = Raw extends Effect.Effect<infer _A, infer _E, infer _R> ? true : false;
const rawIsDeferred: Deferred = true;

/** And its failure channel carries `ArtifactsError`, so a local provider can decline. */
type Failure = Raw extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;
const failureIsDeclined: Failure extends { readonly _tag: string } ? true : false = true;

describe("alchemy patch", () => {
  it.effect("defers RepoClient.raw, per the local patch-package patch", () =>
    Effect.sync(() => {
      assert.equal(rawIsDeferred, true);
      assert.equal(failureIsDeclined, true);
      assert.ok(
        fs.existsSync("patches/alchemy+2.0.0-beta.74.patch"),
        "the patch file ships with the repository",
      );
    }),
  );
});
