import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { serve } from "./Node.ts";

describe("Node operations listener", () => {
  it.live(
    "detects storage outage and recovery without exposing data on the Git listener",
    () =>
      Effect.promise(async () => {
        const parent = await fs.mkdtemp(join(tmpdir(), "git-operations-"));
        const root = join(parent, "private-root");
        await fs.mkdir(root);
        const server = await serve({ root, operationsPort: 0 });
        assert.ok(server.operationsUrl?.startsWith("http://127.0.0.1:"));
        const ready = async (status: number) => {
          const end = Date.now() + 12_000;
          while (true) {
            const response = await fetch(`${server.operationsUrl}/ready`);
            const body = await response.text();
            assert.equal(response.headers.get("cache-control"), "no-store");
            assert.equal(body.includes(root), false);
            if (response.status === status) return;
            assert.ok(Date.now() < end, `readiness did not become ${status}`);
            await delay(25);
          }
        };
        try {
          await ready(200);
          const live = await fetch(`${server.operationsUrl}/live`);
          assert.deepEqual(await live.json(), { live: true });
          const metrics = await fetch(`${server.operationsUrl}/metrics`);
          assert.equal(metrics.status, 200);
          assert.match(await metrics.text(), /"admitted":0/);
          const denied = await fetch(`${server.operationsUrl}/ready`, { method: "POST" });
          assert.equal(denied.status, 405);
          const publicRoute = await fetch(`${server.url}/ready`);
          assert.notEqual(
            await publicRoute.text(),
            '{"ready":true,"storage":true,"capacity":true}',
          );
          await fs.rename(root, join(parent, "offline"));
          await ready(503);
          assert.equal((await fetch(`${server.operationsUrl}/live`)).status, 200);
          await fs.rename(join(parent, "offline"), root);
          await ready(200);
        } finally {
          await server.close({ force: true });
          assert.equal(
            (await fs.readdir(root)).some((name) => name.startsWith(".git-plus-health-")),
            false,
          );
          await fs.rm(parent, { recursive: true, force: true });
        }
        await assert.rejects(fetch(`${server.operationsUrl}/live`));
      }),
    { timeout: 25_000 },
  );
});
