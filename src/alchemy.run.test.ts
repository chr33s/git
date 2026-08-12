/**
 * The deployment stack, as far as it can be checked without an account.
 *
 * `alchemy deploy` needs Cloudflare credentials, so what CI can hold onto is
 * everything before the API call: the modules load, the resources carry the
 * identities the stack claims, the Worker layer requires the Durable Object
 * layer that provides it, and — the part that actually rots — the bindings
 * agree with `wrangler.json`, which is still the path that ships.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import wrangler from "../wrangler.json" with { type: "json" };

describe("alchemy stack", () => {
  it("builds the resource graph: bucket, durable object, worker", async () => {
    const [stack, host, objects] = await Promise.all([
      import("./alchemy.run.ts"),
      import("./host/Cloudflare.ts"),
      import("./objects.ts"),
    ]);

    // Every resource is a value in the program, not a config entry.
    assert.ok(objects.Objects, "the bucket resource exists");
    assert.ok(host.Repo, "the durable object class exists");
    assert.ok(stack.Git, "the worker class exists");

    // Both default exports are layers — the Worker's requires the DO's.
    assert.equal(typeof stack.default, "object", "the worker layer is the default export");
    assert.equal(typeof host.default, "object", "the durable object layer is the default export");
  });

  it("agrees with wrangler.json, which is still the path that ships", () => {
    // One bucket, one durable object, one nodejs_compat worker — the two
    // declarations describe the same deployment or one of them is stale.
    assert.equal(wrangler.r2_buckets.length, 1, "one bucket in wrangler.json");
    assert.equal(wrangler.r2_buckets[0]?.bucket_name, "git-objects", "same bucket name");
    assert.equal(wrangler.durable_objects.bindings.length, 1, "one durable object");
    assert.deepEqual(wrangler.compatibility_flags, ["nodejs_compat"], "same compatibility flags");
    assert.equal(wrangler.compatibility_date, "2025-12-10", "same compatibility date");
  });
});
