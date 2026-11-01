import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import { createServer } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { Predicate } from "effect";

it("validates DO-only egress configuration before the actual Worker starts", async () => {
  const scratch = await fs.mkdtemp(path.join(os.tmpdir(), "git-config-binding-"));
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", resolve);
  });
  const address = probe.address();
  assert.ok(address !== null && !Predicate.isString(address));
  const port = address.port;
  await new Promise<void>((resolve, reject) =>
    probe.close((error) => (error ? reject(error) : resolve())),
  );
  try {
    for (const name of ["src", "node_modules", "dist", "package.json", "tsconfig.json"])
      await fs.symlink(path.resolve(name), path.join(scratch, name));
    const child = spawn(
      process.execPath,
      [
        path.resolve("node_modules/alchemy/bin/cli.js"),
        "dev",
        "src/alchemy.run.ts",
        "--stage",
        `verify-config-${crypto.randomUUID()}`,
        "--env-file",
        "/dev/null",
        "--no-input",
      ],
      {
        cwd: scratch,
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
        env: {
          ...process.env,
          CI: "true",
          NO_COLOR: "1",
          GIT_DEV_PORT: String(port),
          GIT_DEV_STRICT_PORT: "true",
          GIT_EGRESS_TRUSTED_ORIGINS: "http://localhost",
        },
      },
    );
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    let output = "";
    const rejected = () =>
      output.includes("Invalid:") &&
      output.includes("src/host/Egress.cloudflare.ts") &&
      output.includes("run failed");
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        const inspect = (chunk: Buffer) => {
          output = (output + chunk.toString()).slice(-64 * 1024);
          if (/\[git\] Started in/.test(output))
            reject(new Error("invalid DO egress configuration was ignored at deployment"));
          else if (rejected()) resolve();
        };
        child.stdout.on("data", inspect);
        child.stderr.on("data", inspect);
        child.once("error", reject);
        child.once("exit", () => {
          if (!rejected()) reject(new Error(`egress validation did not run: ${output}`));
        });
        timeout = setTimeout(
          () => reject(new Error(`egress configuration validation timed out: ${output}`)),
          30_000,
        );
      });
      assert.equal(output.includes("Started in"), false);
    } finally {
      clearTimeout(timeout);
      const terminate = (signal: NodeJS.Signals) => {
        if (child.pid !== undefined) {
          try {
            process.kill(-child.pid, signal);
          } catch (error) {
            if (!(error instanceof Error && "code" in error && error.code === "ESRCH")) throw error;
          }
        }
      };
      terminate("SIGTERM");
      const force = setTimeout(() => terminate("SIGKILL"), 10_000);
      try {
        await closed;
      } finally {
        clearTimeout(force);
      }
    }
  } finally {
    await fs.rm(scratch, { recursive: true, force: true });
  }
});
