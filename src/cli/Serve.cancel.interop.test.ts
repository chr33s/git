import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Predicate } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Task from "../hub/Task.ts";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";
import { enableHubUnder } from "../testing/Hub.ts";

const exec = promisify(execFile);
const entry = path.resolve("src/cli/main.ts");
const hostUrl = pathToFileURL(path.resolve("src/host/Node.ts")).href;
const standalone = path.resolve("dist/sea/git+");
const owners = ["host", "source", ...(existsSync(standalone) ? ["standalone"] : [])];

describe.skipIf(!hasGit || process.platform === "win32")("server wake shutdown", () => {
  for (const owner of owners) {
    for (const termination of owner === "host"
      ? (["close"] as const)
      : (["SIGINT", "SIGTERM"] as const)) {
      it.live(`${owner} stops its active wake rule on ${termination}`, () =>
        Effect.promise(async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "serve-wake-stop-"));
          const project = path.join(root, "project");
          const client = path.join(root, "client");
          const fixture = await enableHubUnder(root, "project", ["hub.task", "source.push"]);
          await Effect.runPromise(
            Task.open({
              repo: fixture.repoId,
              title: "wake",
              key: fixture.member,
            }).pipe(
              Effect.provide(
                GitRepository.layer.pipe(
                  Layer.provide(GitRepository.hooksNoop),
                  Layer.provide(stores(project)),
                ),
              ),
            ),
          );
          await fs.mkdir(client);
          const git = gitIn(client);
          git("init", "-q", "-b", "main");
          await fs.writeFile(path.join(client, "a"), "wake trigger\n");
          git("add", "a");
          git("commit", "-qm", "trigger");

          const started = Promise.withResolvers<number>();
          const disconnected = Promise.withResolvers<void>();
          const sockets = new Set<net.Socket>();
          const observer = net.createServer((socket) => {
            sockets.add(socket);
            socket.once("data", (bytes) => started.resolve(Number(bytes.toString())));
            socket.once("close", () => {
              sockets.delete(socket);
              disconnected.resolve();
            });
          });
          await new Promise<void>((resolve) => observer.listen(0, "127.0.0.1", resolve));
          const address = observer.address();
          assert.ok(address !== null && !Predicate.isString(address));
          await fs.writeFile(
            path.join(project, "wake.json"),
            JSON.stringify({
              rules: [
                {
                  ref: "refs/hub/task/*",
                  on: ["task.opened"],
                  run: [
                    process.execPath,
                    "-e",
                    `const socket = require('node:net').connect(${address.port}, '127.0.0.1', () => socket.write(String(process.pid))); socket.resume();`,
                  ],
                },
              ],
            }),
          );

          const args =
            owner === "host"
              ? [
                  "--input-type=module",
                  "-e",
                  `import { serve } from ${JSON.stringify(hostUrl)}; const host = await serve({ root: process.argv[1], wake: true }); console.log('server on ' + host.url + ','); process.on('message', async () => { await host.close({ force: true }); process.disconnect(); });`,
                  root,
                ]
              : [
                  ...(owner === "source" ? [entry] : []),
                  "serve",
                  "--root",
                  root,
                  "--port",
                  "0",
                  "--hostname",
                  "127.0.0.1",
                  "--wake",
                ];
          const child = spawn(owner === "standalone" ? standalone : process.execPath, args, {
            cwd: root,
            env: { ...gitEnv, PORT: "0", GIT_HOSTS: "" },
            stdio: ["ignore", "pipe", "pipe", "ipc"],
          });
          const ready = Promise.withResolvers<string>();
          let output = "";
          assert.ok(child.stdout !== null && child.stderr !== null);
          child.stdout.on("data", (chunk: Buffer) => {
            output += chunk.toString();
            const url = /server on (http:\/\/[^\s,]+)/.exec(output)?.[1];
            if (url !== undefined) ready.resolve(url);
          });
          child.stderr.on("data", (chunk: Buffer) => {
            output += chunk.toString();
          });
          const completed = new Promise<void>((resolve, reject) => {
            child.once("error", reject);
            child.once("close", () => resolve());
          });
          let worker: number | undefined;
          let unfinished: http.ClientRequest | undefined;
          let timeout: ReturnType<typeof setTimeout> | undefined;
          try {
            const url = await Promise.race([
              ready.promise,
              completed.then((): never => {
                throw new Error(`server exited before binding: ${output}`);
              }),
            ]);
            await exec(
              "git",
              [
                "-c",
                `http.extraHeader=Authorization: Bearer ${fixture.credential}`,
                "push",
                `${url}/project`,
                "HEAD:refs/heads/topic",
              ],
              { cwd: client, env: gitEnv },
            );
            worker = await started.promise;
            assert.ok(Number.isInteger(worker) && worker > 0);
            await fs.stat(path.join(project, "wake.cursor.json.lock"));
            // A client can leave a body unfinished after its headers have
            // arrived. Signal cleanup must not wait forever for that client.
            const continued = Promise.withResolvers<void>();
            const requestClosed = Promise.withResolvers<void>();
            unfinished = http.request(`${url}/project/commit`, {
              method: "POST",
              headers: {
                authorization: `Bearer ${fixture.credential}`,
                "content-type": "application/json",
                expect: "100-continue",
              },
            });
            unfinished.once("continue", () => continued.resolve());
            unfinished.once("error", () => requestClosed.resolve());
            unfinished.once("close", () => requestClosed.resolve());
            unfinished.flushHeaders();
            await continued.promise;
            unfinished.write("{");
            if (termination === "close") child.send("close");
            else child.kill(termination);
            await Promise.race([
              Promise.all([completed, disconnected.promise, requestClosed.promise]),
              new Promise<never>((_resolve, reject) => {
                timeout = setTimeout(
                  () => reject(new Error("server left its wake rule running")),
                  3_000,
                );
              }),
            ]);
            await assert.rejects(fs.stat(path.join(project, "wake.cursor.json.lock")), {
              code: "ENOENT",
            });
            await assert.rejects(fs.stat(path.join(project, "wake.cursor.json")), {
              code: "ENOENT",
            });
            worker = undefined;
          } finally {
            clearTimeout(timeout);
            unfinished?.destroy();
            if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
            if (worker !== undefined) {
              try {
                process.kill(worker, "SIGKILL");
              } catch (cause) {
                assert.ok(Predicate.hasProperty(cause, "code") && cause.code === "ESRCH");
              }
            }
            for (const socket of sockets) socket.destroy();
            await completed;
            await new Promise<void>((resolve) => observer.close(() => resolve()));
            await fs.rm(root, { recursive: true, force: true });
          }
        }),
      );
    }
  }
});
