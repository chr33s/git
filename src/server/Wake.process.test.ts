import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import { createRequire } from "node:module";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Predicate } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Task from "../hub/Task.ts";
import { enableHubUnder } from "../testing/Hub.ts";

const entry = path.resolve("src/cli/main.ts");
const effectUrl = pathToFileURL(createRequire(import.meta.url).resolve("effect")).href;
const wakeUrl = pathToFileURL(path.resolve("src/server/Wake.node.ts")).href;

describe("wake across processes", () => {
  for (const owner of ["cli", "hook"] as const) {
    it.live(`refuses a competing dispatch while the ${owner} owns the event`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "wake-process-"));
        const directory = path.join(root, "project");
        const sockets = new Set<net.Socket>();
        const firstStarted = Promise.withResolvers<void>();
        const duplicated = Promise.withResolvers<"duplicate">();
        let starts = 0;
        const server = net.createServer((socket) => {
          sockets.add(socket);
          socket.once("close", () => sockets.delete(socket));
          socket.once("data", () => {
            starts++;
            if (starts === 1) firstStarted.resolve();
            else duplicated.resolve("duplicate");
          });
        });
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const completions: Array<Promise<{ code: number | null; output: string }>> = [];
        const dispatch = (fromHook = false) => {
          const args = fromHook
            ? [
                "--input-type=module",
                "-e",
                `import { Effect } from ${JSON.stringify(effectUrl)}; import { service } from ${JSON.stringify(wakeUrl)}; await Effect.runPromise(service(process.argv[1], 'project').postReceive([]));`,
                directory,
              ]
            : [entry, "wake", "--root", root, "project"];
          const child = spawn(process.execPath, args, {
            stdio: ["ignore", "pipe", "pipe"],
          });
          let output = "";
          child.stdout.on("data", (chunk: Buffer) => {
            output += chunk.toString();
          });
          child.stderr.on("data", (chunk: Buffer) => {
            output += chunk.toString();
          });
          const completed = new Promise<{ code: number | null; output: string }>(
            (resolve, reject) => {
              child.once("error", reject);
              child.once("close", (code) => resolve({ code, output }));
            },
          );
          completions.push(completed);
          return completed;
        };
        try {
          const fixture = await enableHubUnder(root, "project", ["hub.task"]);
          const layer = GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provide(stores(directory)),
          );
          await Effect.runPromise(
            Task.open({
              repo: fixture.repoId,
              title: "wake once",
              key: fixture.member,
            }).pipe(Effect.provide(layer)),
          );
          await fs.writeFile(
            path.join(directory, "wake.json"),
            JSON.stringify({
              rules: [
                {
                  ref: "refs/hub/task/*",
                  on: ["task.opened"],
                  run: [
                    process.execPath,
                    "-e",
                    `const socket = require('node:net').connect(${address.port}, '127.0.0.1', () => socket.write(process.env.CHR33S_GIT_COMMIT)); socket.resume();`,
                  ],
                },
              ],
            }),
          );

          const first = dispatch(owner === "hook");
          await Promise.race([
            firstStarted.promise,
            first.then((result) => {
              throw new Error(`first dispatch exited before its rule started: ${result.output}`);
            }),
          ]);
          const second = dispatch();
          const competing = await Promise.race([second, duplicated.promise]);
          assert.notEqual(competing, "duplicate", "the competing process started the same event");
          assert.ok(competing !== "duplicate");
          assert.equal(competing.code, 1, competing.output);
          assert.match(competing.output, /wake.*(lock|running|busy)/i);
          assert.equal(starts, 1);

          for (const socket of sockets) socket.end();
          const finished = await first;
          assert.equal(finished.code, 0, finished.output);
          const retried = await dispatch();
          assert.equal(retried.code, 0, retried.output);
          assert.match(retried.output, /0 rule\(s\) run/);
          assert.equal(starts, 1);
          await assert.rejects(fs.stat(path.join(directory, "wake.cursor.json.lock")), {
            code: "ENOENT",
          });
        } finally {
          for (const socket of sockets) socket.end();
          await Promise.all(completions);
          await new Promise<void>((resolve) => server.close(() => resolve()));
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
