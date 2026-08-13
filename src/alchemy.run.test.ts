/**
 * The deployment stack, as far as it can be checked without an account.
 *
 * `alchemy deploy` needs Cloudflare credentials, so what CI can hold onto is
 * everything before the API call: the modules load, the resources carry the
 * identities the stack claims, the Worker layer provides the Durable Object
 * layer it hosts, and — the part that actually rots — the runtime the
 * integration suite proves (`wrangler.test.json`) is the runtime the stack
 * deploys.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Predicate } from "effect";

import harness from "../wrangler.test.json" with { type: "json" };

describe("alchemy stack", () => {
  it("builds the resource graph: bucket, durable object, worker", async () => {
    const [stack, worker, host, objects] = await Promise.all([
      import("./alchemy.run.ts"),
      import("./worker.ts"),
      import("./host/Cloudflare.ts"),
      import("./objects.ts"),
    ]);

    // Every resource is a value in the program, not a config entry.
    assert.ok(objects.Objects, "the bucket resource exists");
    assert.ok(host.Repo, "the durable object class exists");
    assert.ok(stack.Git, "the worker class exists");

    // The CLI contract: `alchemy.run.ts` default-exports the stack, and the
    // worker module default-exports its layer (the bundler resolves `main`'s
    // default export), which in turn provides the DO's layer.
    assert.ok(Predicate.isObject(stack.default), "the stack is the default export");
    assert.ok(Predicate.isObject(worker.default), "the worker layer is worker.ts's default export");
    assert.ok(Predicate.isObject(host.default), "the durable object layer is the default export");
  });

  it("deploys the runtime the integration suite proves", async () => {
    // `wrangler.test.json` pins the workerd the contract suite runs in;
    // `worker.ts` pins what alchemy deploys. If they drift, the tests pass
    // against one runtime and production runs another.
    const { compatibility } = await import("./worker.ts");
    assert.equal(harness.compatibility_date, compatibility.date, "same compatibility date");
    assert.deepEqual(harness.compatibility_flags, compatibility.flags, "same compatibility flags");
  });
});
