import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

const standalone = path.resolve("dist/sea/git+");
const executables = [
  { name: "source", command: process.execPath, args: [path.resolve("src/cli/main.ts")] },
  ...(existsSync(standalone) ? [{ name: "standalone", command: standalone, args: [] }] : []),
];

describe("serve configuration precedence", () => {
  for (const executable of executables) {
    it.live(`${executable.name} starts with an explicit port despite malformed PORT`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "serve-port-"));
        const ready = Promise.withResolvers<string>();
        const child = spawn(
          executable.command,
          [...executable.args, "serve", "--root", root, "--port", "0", "--hostname", "127.0.0.1"],
          {
            cwd: root,
            env: { ...process.env, PORT: "invalid", GIT_HOSTS: "" },
            stdio: ["ignore", "pipe", "pipe"],
          },
        );
        let stdout = "";
        let stderr = "";
        child.stdout.on("data", (chunk: Buffer) => {
          stdout += chunk.toString();
          const url = /server on (http:\/\/[^\s,]+)/.exec(stdout)?.[1];
          if (url !== undefined) ready.resolve(url);
        });
        child.stderr.on("data", (chunk: Buffer) => {
          stderr += chunk.toString();
        });
        const completed = new Promise<void>((resolve, reject) => {
          child.once("error", reject);
          child.once("close", () => resolve());
        });
        let timeout: ReturnType<typeof setTimeout> | undefined;
        try {
          const url = await Promise.race([
            ready.promise,
            completed.then((): never => {
              throw new Error(`serve exited before binding: ${stderr}${stdout}`);
            }),
            new Promise<never>((_resolve, reject) => {
              timeout = setTimeout(() => reject(new Error(`serve did not bind: ${stderr}`)), 5_000);
            }),
          ]);
          assert.ok(Number(new URL(url).port) > 0);
          const response = await fetch(url, { signal: AbortSignal.timeout(3_000) });
          assert.equal(await response.text(), "bad repository name");
          assert.equal(response.status, 400);
        } finally {
          clearTimeout(timeout);
          child.kill("SIGTERM");
          await completed;
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
