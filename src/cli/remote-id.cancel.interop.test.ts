import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Predicate, Result } from "effect";

import { serve } from "../host/Node.ts";
import { encodeRepository } from "../social/Encode.ts";
import { gitEnv, hasGit } from "../testing/Git.ts";
import { enableHubUnder } from "../testing/Hub.ts";

const source = path.resolve("src/cli/remote-id.node.ts");

describe.skipIf(!hasGit || process.platform === "win32")("remote helper termination", () => {
  for (const phase of ["preflight", "delegate"] as const) {
    for (const signal of ["SIGINT", "SIGTERM"] as const) {
      it(`closes ${phase} Git and removes its scratch repository on ${signal}`, async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "remote-helper-cancel-"));
        const scratch = path.join(root, "scratch");
        await fs.mkdir(scratch);
        const fixture = await enableHubUnder(root, "project", []);
        const upstream = await serve({ root });
        const started = Promise.withResolvers<void>();
        const closed = Promise.withResolvers<void>();
        const server = http.createServer((request, response) => {
          void (async () => {
            if (phase === "preflight" || (await fs.readdir(scratch)).length === 0) {
              response.once("close", () => closed.resolve());
              started.resolve();
              return;
            }
            const forwarded = http.request(
              new URL(request.url ?? "/", upstream.url),
              {
                method: request.method,
                headers: request.headers,
              },
              (incoming) => {
                response.writeHead(incoming.statusCode ?? 500, incoming.headers);
                incoming.pipe(response);
              },
            );
            forwarded.once("error", (error) => {
              started.reject(error);
              response.destroy(error);
            });
            request.pipe(forwarded);
          })().catch((cause: unknown) => {
            started.reject(cause);
            response.destroy();
          });
        });
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const encoded = encodeRepository({
          id: fixture.repoId,
          hints: [`http://127.0.0.1:${address.port}/project`],
        });
        assert.ok(Result.isSuccess(encoded));
        const child = spawn(process.execPath, [source, "origin", `git+id://${encoded.success}`], {
          cwd: root,
          env: {
            ...gitEnv,
            TMPDIR: scratch,
            TMP: scratch,
            TEMP: scratch,
            XDG_CONFIG_HOME: path.join(root, "config"),
          },
          stdio: ["pipe", "pipe", "pipe"],
        });
        child.stdout.resume();
        child.stdin.write("capabilities\nlist\n");
        let stderr = "";
        child.stderr.on("data", (chunk: Buffer) => {
          stderr += chunk.toString();
        });
        const completed = new Promise<{
          readonly code: number | null;
          readonly signal: NodeJS.Signals | null;
        }>((resolve, reject) => {
          child.once("error", reject);
          child.once("close", (code, endedBy) => resolve({ code, signal: endedBy }));
        });
        void completed.then(
          () => started.reject(new Error(stderr || "helper exited before requesting the remote")),
          (cause: unknown) => started.reject(cause),
        );
        let timeout: ReturnType<typeof setTimeout> | undefined;
        try {
          await started.promise;
          assert.equal((await fs.readdir(scratch)).length, phase === "preflight" ? 1 : 0);
          child.kill(signal);
          await Promise.race([
            Promise.all([closed.promise, completed]),
            new Promise<never>((_resolve, reject) => {
              timeout = setTimeout(
                () => reject(new Error(`remote helper left its ${phase} running`)),
                3_000,
              );
            }),
          ]);
          const result = await completed;
          assert.equal(result.code, 128 + os.constants.signals[signal]);
          assert.equal(result.signal, null);
          assert.deepEqual(await fs.readdir(scratch), []);
        } finally {
          clearTimeout(timeout);
          server.closeAllConnections();
          await new Promise<void>((resolve) => server.close(() => resolve()));
          await completed;
          await upstream.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      });
    }
  }
});
