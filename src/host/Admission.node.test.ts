import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { ConfigProvider, Effect } from "effect";
import { configuration } from "./Admission.node.ts";

describe("Node admission and deadlines", () => {
  it.effect("rejects disabled, fractional, and overflowing timers", () =>
    Effect.gen(function* () {
      for (const invalid of ["0", "-1", "1.5", "2147483648", "Infinity"]) {
        const result = yield* configuration().pipe(
          Effect.provide(
            ConfigProvider.layer(ConfigProvider.fromUnknown({ GIT_HTTP_DEADLINE_MS: invalid })),
          ),
          Effect.result,
        );
        assert.equal(result._tag, "Failure");
      }
    }),
  );

  it.live.each([
    "capacity",
    "idle",
    "deadline",
    "headers",
    "connections",
    "output",
    "settling",
  ] as const)("enforces %s limits over real sockets and recovers capacity", (mode) =>
    Effect.promise(async () => {
      const script = `
        import assert from "node:assert/strict";
        import * as fs from "node:fs/promises";
        import * as http from "node:http";
        import * as net from "node:net";
        import { tmpdir } from "node:os";
        import { join } from "node:path";
        import { setTimeout as delay } from "node:timers/promises";
        import { serve } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
        const mode = ${JSON.stringify(mode)};
        const root = await fs.mkdtemp(join(tmpdir(), "git-admission-"));
        let finishAsset;
        const waiting = new Promise(resolve => { finishAsset = resolve; });
        const ui = mode === "output" ? async () => new Uint8Array(8 * 1024 * 1024)
          : mode === "settling" ? async () => { await waiting; return new Uint8Array(1); }
          : undefined;
        const server = await serve({ root, allowAnonymousWrites: true, ui, operationsPort: mode === "settling" ? 0 : undefined });
        const clients = [];
        const status = async repo => {
          const response = await fetch(server.url + "/" + repo + "/info/refs?service=git-upload-pack");
          await response.arrayBuffer();
          return response;
        };
        const healthy = async repo => {
          const end = Date.now() + 2000;
          while ((await status(repo)).status !== 200) {
            assert.ok(Date.now() < end, "capacity was not released");
            await delay(10);
          }
        };
        const hold = repo => new Promise((resolve, reject) => {
          const req = http.request(server.url + "/" + repo + "/blob", {
            method: "POST", headers: { expect: "100-continue", "content-type": "application/json" },
          });
          clients.push(req);
          const closed = new Promise(done => req.once("close", done));
          // Continue is the server handshake, not a sleep guessing whether
          // this request has reached admission yet.
          req.once("continue", () => { req.write('{"content":"'); resolve({ req, closed }); });
          req.on("error", reject);
          req.flushHeaders();
        });
        const socket = () => new Promise((resolve, reject) => {
          const target = new URL(server.url);
          const client = net.connect(Number(target.port), target.hostname);
          clients.push(client);
          client.once("connect", () => resolve(client));
          client.on("error", reject);
        });
        try {
          if (mode === "capacity") {
            const a = await hold("a");
            const refused = await status("a");
            assert.equal(refused.status, 503);
            assert.equal(refused.headers.get("retry-after"), "1");
            await healthy("b");
            const b = await hold("b");
            assert.equal((await status("c")).status, 503);
            a.req.destroy(); await a.closed;
            await healthy("a");
            b.req.destroy(); await b.closed;
            await healthy("b");
          } else if (mode === "idle" || mode === "deadline") {
            const start = Date.now();
            const held = await hold("a");
            const trickle = mode === "deadline" ? setInterval(() => held.req.write("x"), 25) : undefined;
            try { await held.closed; } finally { clearInterval(trickle); }
            assert.ok(Date.now() - start < 2000, "slow client was not disconnected promptly");
            await healthy("a");
          } else if (mode === "output") {
            const unread = () => new Promise((resolve, reject) => {
              const req = http.get(server.url + "/hub/large.js", res => {
                res.pause();
                res.on("error", () => {});
                const closed = new Promise(done => res.once("close", done));
                resolve({ req, closed });
              });
              clients.push(req);
              req.on("error", reject);
            });
            const a = await unread();
            const b = await unread();
            assert.equal((await status("c")).status, 503, "unread responses must still occupy capacity");
            a.req.destroy(); await a.closed;
            await healthy("a");
            b.req.destroy(); await b.closed;
          } else if (mode === "settling") {
            const req = http.get(server.url + "/hub/pending.js");
            clients.push(req);
            req.on("error", () => {});
            await new Promise(done => req.once("close", done));
            assert.equal((await status("a")).status, 503, "timeout cannot release capacity owned by unfinished work");
            assert.equal(server.metrics().active, 1, "timed-out unfinished work remains active");
            assert.equal(server.metrics().deadlines, 1);
            const readiness = await fetch(server.operationsUrl + "/ready");
            assert.equal(readiness.status, 503);
            assert.equal((await readiness.json()).capacity, false);
            assert.equal((await fetch(server.operationsUrl + "/live")).status, 200);
            finishAsset();
            await healthy("a");
          } else if (mode === "headers") {
            const client = await socket();
            let reply = "";
            client.setEncoding("utf8");
            client.on("data", data => { reply += data; });
            const closed = new Promise(done => client.once("close", done));
            client.write("POST /a/blob HTTP/1.1\\r\\nHost: localhost\\r\\n");
            await closed;
            assert.match(reply, /408 Request Timeout/);
            await healthy("a");
          } else {
            const first = await socket();
            const closed = new Promise(done => first.once("close", done));
            const second = await socket();
            const dropped = new Promise(done => second.once("close", done));
            second.resume();
            await dropped;
            first.destroy(); await closed;
            await healthy("a");
          }
          const metrics = server.metrics();
          assert.ok(metrics.admitted >= 1);
          assert.equal(metrics.admitted, metrics.completed + metrics.active);
          if (mode === "capacity") {
            assert.ok(metrics.refusedGlobal >= 1);
            assert.ok(metrics.refusedRepository >= 1);
          }
          if (mode === "deadline" || mode === "settling") assert.equal(metrics.deadlines, 1);
          console.log("admission verified: " + mode);
        } finally {
          finishAsset();
          for (const client of clients) client.destroy();
          await server.close({ force: true });
          await fs.rm(root, { recursive: true, force: true });
        }
      `;
      const result = await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", script],
        {
          env: {
            ...process.env,
            GIT_HTTP_MAX_ACTIVE: mode === "settling" ? "1" : "2",
            GIT_HTTP_MAX_PER_REPO: "1",
            GIT_HTTP_MAX_CONNECTIONS: mode === "connections" ? "1" : "32",
            GIT_HTTP_DEADLINE_MS: mode === "deadline" || mode === "settling" ? "750" : "10000",
            GIT_HTTP_IDLE_MS: mode === "idle" ? "100" : "5000",
            GIT_HTTP_HEADERS_MS: mode === "headers" ? "150" : "5000",
          },
          timeout: 10_000,
        },
      );
      assert.match(result.stdout, new RegExp("admission verified: " + mode));
    }),
  );
});
