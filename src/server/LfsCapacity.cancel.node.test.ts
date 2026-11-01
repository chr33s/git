import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { it } from "@effect/vitest";

it("settles native inventory reads before interrupted quota reservation returns", async () => {
  const script = `
    import assert from "node:assert/strict";
    import { createHook } from "node:async_hooks";
    import * as fs from "node:fs/promises";
    import { tmpdir } from "node:os";
    import { join } from "node:path";
    import { setTimeout as delay } from "node:timers/promises";
    import { Effect, Fiber } from ${JSON.stringify(import.meta.resolve("effect"))};
    import { reserve } from ${JSON.stringify(new URL("./LfsCapacity.node.ts", import.meta.url).href)};
    const root = await fs.mkdtemp(join(tmpdir(), "lfs-scan-cancel-"));
    let signalStarted;
    const started = new Promise(resolve => { signalStarted = resolve; });
    let requests = 0;
    let returned = false;
    let detachedReads = 0;
    // Observe real syscalls without replacing filesystem functions. This child
    // has no other filesystem work during the observation window.
    const hook = createHook({ init(id, type) {
      if (type !== "FSREQPROMISE" && type !== "FSREQCALLBACK") return;
      if (returned) detachedReads++;
      if (++requests === 64) signalStarted();
    }});
    try {
      for (let index = 0; index < 4096; index++)
        await fs.writeFile(join(root, String(index)), "x");
      hook.enable();
      const fiber = Effect.runFork(reserve(root, { bytes: 1000000, objects: 10000 }).pipe(
        Effect.andThen(Effect.never), Effect.scoped));
      await started;
      await Effect.runPromise(Fiber.interrupt(fiber));
      returned = true;
      await delay(150);
      hook.disable();
      assert.equal(detachedReads, 0, "inventory issued new filesystem reads after cancellation returned");
      await assert.rejects(fs.stat(join(root, ".quota.lock")), { code: "ENOENT" });
    } finally {
      hook.disable();
      await fs.rm(root, { recursive: true, force: true });
    }
  `;
  const result = await promisify(execFile)(
    process.execPath,
    ["--input-type=module", "-e", script],
    {
      timeout: 20_000,
      maxBuffer: 64 * 1024,
    },
  );
  assert.equal(result.stderr, "");
}, 25_000);
