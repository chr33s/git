import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { HookRunner, type HookContext, type HookRefUpdate } from "./git.hooks.ts";

const ZERO_OID = "0".repeat(40);
const OID_A = "a".repeat(40);
const OID_B = "b".repeat(40);

function makeContext(overrides?: Partial<HookContext>): HookContext {
  return {
    repository: "test-repo",
    updates: [{ ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A }],
    capabilities: new Set<string>(),
    ...overrides,
  };
}

void describe("HookRunner", () => {
  void describe("pre-receive", () => {
    void it("accepts when no hooks registered", async () => {
      const runner = new HookRunner();
      const result = await runner.runPreReceive(makeContext());
      assert.equal(result.ok, true);
    });

    void it("accepts when hook returns ok", async () => {
      const runner = new HookRunner();
      runner.register("pre-receive", async () => ({ ok: true }));
      const result = await runner.runPreReceive(makeContext());
      assert.equal(result.ok, true);
    });

    void it("rejects entire push", async () => {
      const runner = new HookRunner();
      runner.register("pre-receive", async () => ({
        ok: false,
        message: "Push rejected by policy",
      }));
      const result = await runner.runPreReceive(makeContext());
      assert.equal(result.ok, false);
      assert.equal(result.message, "Push rejected by policy");
    });

    void it("short-circuits on first rejection", async () => {
      const runner = new HookRunner();
      const calls: string[] = [];

      runner.register("pre-receive", async () => {
        calls.push("hook1");
        return { ok: false, message: "Rejected" };
      });
      runner.register("pre-receive", async () => {
        calls.push("hook2");
        return { ok: true };
      });

      const result = await runner.runPreReceive(makeContext());
      assert.equal(result.ok, false);
      assert.deepEqual(calls, ["hook1"]);
    });

    void it("runs all hooks when all accept", async () => {
      const runner = new HookRunner();
      const calls: string[] = [];

      runner.register("pre-receive", async () => {
        calls.push("hook1");
        return { ok: true };
      });
      runner.register("pre-receive", async () => {
        calls.push("hook2");
        return { ok: true };
      });

      const result = await runner.runPreReceive(makeContext());
      assert.equal(result.ok, true);
      assert.deepEqual(calls, ["hook1", "hook2"]);
    });

    void it("receives correct context", async () => {
      const runner = new HookRunner();
      let receivedCtx: HookContext | null = null;

      runner.register("pre-receive", async (ctx) => {
        receivedCtx = ctx;
        return { ok: true };
      });

      const updates: HookRefUpdate[] = [
        { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        { ref: "refs/heads/feature", oldOid: OID_A, newOid: OID_B },
      ];
      const ctx = makeContext({ updates, capabilities: new Set(["atomic"]) });
      await runner.runPreReceive(ctx);

      assert.ok(receivedCtx);
      assert.equal((receivedCtx as HookContext).repository, "test-repo");
      assert.equal((receivedCtx as HookContext).updates.length, 2);
      assert.ok((receivedCtx as HookContext).capabilities.has("atomic"));
    });

    void it("can reject based on protected branch", async () => {
      const runner = new HookRunner();
      runner.register("pre-receive", async (ctx) => {
        if (ctx.updates.some((u) => u.ref === "refs/heads/protected")) {
          return { ok: false, message: "Cannot push to protected branch" };
        }
        return { ok: true };
      });

      const result1 = await runner.runPreReceive(
        makeContext({ updates: [{ ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A }] }),
      );
      assert.equal(result1.ok, true);

      const result2 = await runner.runPreReceive(
        makeContext({
          updates: [{ ref: "refs/heads/protected", oldOid: ZERO_OID, newOid: OID_A }],
        }),
      );
      assert.equal(result2.ok, false);
      assert.equal(result2.message, "Cannot push to protected branch");
    });
  });

  void describe("update (per-ref)", () => {
    void it("accepts all refs when no hooks registered", async () => {
      const runner = new HookRunner();
      const updates: HookRefUpdate[] = [
        { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        { ref: "refs/heads/feature", oldOid: OID_A, newOid: OID_B },
      ];
      const results = await runner.runUpdate(makeContext({ updates }));
      assert.equal(results.size, 2);
      assert.equal(results.get("refs/heads/main")!.ok, true);
      assert.equal(results.get("refs/heads/feature")!.ok, true);
    });

    void it("rejects individual ref", async () => {
      const runner = new HookRunner();
      runner.register("update", async (_ctx, update) => {
        if (update.ref === "refs/heads/protected") {
          return { ok: false, message: "Protected branch" };
        }
        return { ok: true };
      });

      const updates: HookRefUpdate[] = [
        { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        { ref: "refs/heads/protected", oldOid: OID_A, newOid: OID_B },
      ];
      const results = await runner.runUpdate(makeContext({ updates }));
      assert.equal(results.get("refs/heads/main")!.ok, true);
      assert.equal(results.get("refs/heads/protected")!.ok, false);
      assert.equal(results.get("refs/heads/protected")!.message, "Protected branch");
    });

    void it("short-circuits per-ref on first rejection", async () => {
      const runner = new HookRunner();
      const calls: string[] = [];

      runner.register("update", async (_ctx, update) => {
        calls.push(`hook1:${update.ref}`);
        return { ok: false, message: "Denied" };
      });
      runner.register("update", async (_ctx, update) => {
        calls.push(`hook2:${update.ref}`);
        return { ok: true };
      });

      const updates: HookRefUpdate[] = [
        { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
      ];
      const results = await runner.runUpdate(makeContext({ updates }));
      assert.equal(results.get("refs/heads/main")!.ok, false);
      assert.deepEqual(calls, ["hook1:refs/heads/main"]);
    });

    void it("can reject force pushes (non-zero old → non-zero new)", async () => {
      const runner = new HookRunner();
      runner.register("update", async (_ctx, update) => {
        // Reject deletes (newOid is zero)
        if (update.newOid === ZERO_OID) {
          return { ok: false, message: "Ref deletion not allowed" };
        }
        return { ok: true };
      });

      const updates: HookRefUpdate[] = [
        { ref: "refs/heads/main", oldOid: OID_A, newOid: ZERO_OID },
        { ref: "refs/heads/feature", oldOid: ZERO_OID, newOid: OID_B },
      ];
      const results = await runner.runUpdate(makeContext({ updates }));
      assert.equal(results.get("refs/heads/main")!.ok, false);
      assert.equal(results.get("refs/heads/feature")!.ok, true);
    });
  });

  void describe("post-receive", () => {
    void it("fires after push (no hooks)", async () => {
      const runner = new HookRunner();
      const results = await runner.runPostReceive(makeContext());
      assert.deepEqual(results, []);
    });

    void it("fires hook and returns results", async () => {
      const runner = new HookRunner();
      const fired: HookRefUpdate[][] = [];

      runner.register("post-receive", async (ctx) => {
        fired.push([...ctx.updates]);
        return { ok: true };
      });

      const updates: HookRefUpdate[] = [
        { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
      ];
      const results = await runner.runPostReceive(makeContext({ updates }));
      assert.equal(results.length, 1);
      assert.equal(results[0]!.ok, true);
      assert.equal(fired.length, 1);
      assert.equal(fired[0]![0]!.ref, "refs/heads/main");
    });

    void it("catches errors without affecting push", async () => {
      const runner = new HookRunner();

      runner.register("post-receive", async () => {
        throw new Error("Notification service down");
      });

      const results = await runner.runPostReceive(makeContext());
      assert.equal(results.length, 1);
      assert.equal(results[0]!.ok, false);
      assert.equal(results[0]!.message, "Notification service down");
    });

    void it("runs all hooks even if some fail", async () => {
      const runner = new HookRunner();
      const calls: string[] = [];

      runner.register("post-receive", async () => {
        calls.push("hook1");
        throw new Error("Failed");
      });
      runner.register("post-receive", async () => {
        calls.push("hook2");
        return { ok: true };
      });

      const results = await runner.runPostReceive(makeContext());
      assert.equal(results.length, 2);
      assert.equal(results[0]!.ok, false);
      assert.equal(results[1]!.ok, true);
      assert.deepEqual(calls, ["hook1", "hook2"]);
    });
  });

  void describe("registration", () => {
    void it("has() returns true when hooks registered", () => {
      const runner = new HookRunner();
      assert.equal(runner.has("pre-receive"), false);
      assert.equal(runner.has("update"), false);
      assert.equal(runner.has("post-receive"), false);

      runner.register("pre-receive", async () => ({ ok: true }));
      assert.equal(runner.has("pre-receive"), true);
      assert.equal(runner.has("update"), false);

      runner.register("update", async () => ({ ok: true }));
      assert.equal(runner.has("update"), true);

      runner.register("post-receive", async () => ({ ok: true }));
      assert.equal(runner.has("post-receive"), true);
    });

    void it("clear() removes all hooks", () => {
      const runner = new HookRunner();
      runner.register("pre-receive", async () => ({ ok: true }));
      runner.register("update", async () => ({ ok: true }));
      runner.register("post-receive", async () => ({ ok: true }));

      runner.clear();
      assert.equal(runner.has("pre-receive"), false);
      assert.equal(runner.has("update"), false);
      assert.equal(runner.has("post-receive"), false);
    });

    void it("clear(name) removes only specified hooks", () => {
      const runner = new HookRunner();
      runner.register("pre-receive", async () => ({ ok: true }));
      runner.register("update", async () => ({ ok: true }));
      runner.register("post-receive", async () => ({ ok: true }));

      runner.clear("pre-receive");
      assert.equal(runner.has("pre-receive"), false);
      assert.equal(runner.has("update"), true);
      assert.equal(runner.has("post-receive"), true);
    });
  });
});
