import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

describe("Node request failures", () => {
  it.live(
    "signals unavailable authorization storage without exposing its path or credentials",
    () =>
      Effect.promise(async () => {
        const secret = "fixture-secret-storage-and-token";
        const script = `
        import assert from "node:assert/strict";
        import * as fs from "node:fs/promises";
        import { tmpdir } from "node:os";
        import { join } from "node:path";
        import { serve } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
        const parent = await fs.mkdtemp(join(tmpdir(), "git-auth-outage-"));
        const root = join(parent, ${JSON.stringify(secret)});
        await fs.writeFile(root, "not a directory");
        const server = await serve({ root });
        try {
          const response = await fetch(server.url + "/private/refs?token=${secret}", {
            headers: { authorization: "Bearer ${secret}" },
          });
          assert.equal(response.status, 503);
          assert.equal(response.headers.get("cache-control"), "no-store");
          assert.equal(await response.text(), "authentication unavailable");
          await fs.rm(root);
          await fs.mkdir(root);
          const healthy = await fetch(server.url + "/public/refs");
          assert.equal(healthy.status, 200);
          await healthy.arrayBuffer();
        } finally {
          await server.close();
          await fs.rm(parent, { recursive: true, force: true });
        }
      `;
        const { stdout, stderr } = await promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", script],
          { timeout: 10_000, env: { ...process.env, GIT_HTTP_METRICS_INTERVAL_MS: "0" } },
        );
        const logs = stdout + stderr;
        assert.equal(logs.includes(secret), false);
        assert.match(logs, /authorization_unavailable/);
        assert.match(logs, /NodeHost/);
      }),
  );

  it.live("redacts unexpected failures from HTTP responses and operator logs", () =>
    Effect.promise(async () => {
      const secret = "fixture-private-path-and-token";
      const script = `
        import assert from "node:assert/strict";
        import * as fs from "node:fs/promises";
        import { tmpdir } from "node:os";
        import { join } from "node:path";
        import { setTimeout as delay } from "node:timers/promises";
        import { serve } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
        const root = await fs.mkdtemp(join(tmpdir(), "git-host-errors-"));
        let broken = true;
        const server = await serve({ root, ui: async () => {
          if (broken) throw new Error(${JSON.stringify(secret)});
          return new TextEncoder().encode("healthy");
        } });
        try {
          const failed = await fetch(server.url + "/hub/secret?token=${secret}", {
            headers: { authorization: "Bearer ${secret}" },
          });
          assert.equal(failed.status, 500);
          assert.equal(failed.headers.get("cache-control"), "no-store");
          assert.equal(await failed.text(), "internal server error");
          broken = false;
          const healthy = await fetch(server.url + "/hub/index.html");
          assert.equal(healthy.status, 200);
          assert.equal(await healthy.text(), "healthy");
          const until = Date.now() + 2000;
          while (server.metrics().completed < 2) {
            assert.ok(Date.now() < until, "completed requests were not recorded");
            await delay(5);
          }
          const metrics = server.metrics();
          assert.equal(metrics.admitted, 2);
          assert.equal(metrics.active, 0);
          assert.equal(metrics.serverErrors, 1);
          assert.equal(metrics.interrupted, 0);
          assert.ok(metrics.durationMsSum >= metrics.durationMsMax);
          await delay(50); // Observe at least one periodic metrics export.

        } finally {
          await server.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      `;
      const { stderr, stdout } = await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", script],
        { timeout: 10_000, env: { ...process.env, GIT_HTTP_METRICS_INTERVAL_MS: "10" } },
      );
      assert.equal((stderr + stdout).includes(secret), false);
      const lines = stderr.trim().split("\n");
      const failures = lines.filter((line) => line.includes('"event":"request_failed"'));
      assert.equal(failures.length, 1);
      assert.deepEqual(JSON.parse(failures[0] ?? "null"), {
        component: "NodeHost",
        operation: "request",
        event: "request_failed",
        responseStarted: false,
      });
      assert.ok(
        lines.some(
          (line) =>
            line.includes('"event":"request_metrics"') &&
            line.includes('"completed":2') &&
            line.includes('"serverErrors":1'),
        ),
      );
    }),
  );
});
