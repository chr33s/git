import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Predicate } from "effect";

import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder } from "../testing/Hub.ts";
import { serve, type Server } from "../host/Node.ts";

const script = `
  import { Effect, Fiber } from ${JSON.stringify(createRequire(import.meta.url).resolve("effect"))};
  import { pushInbox } from ${JSON.stringify(new URL("./inbox.node.ts", import.meta.url).href)};
  const [url, mode, head = "main"] = process.argv.slice(1);
  const push = pushInbox({ url, head, id: "fixture" });
  if (mode === "cancel") {
    const cancel = new Promise(resolve => process.once("message", resolve));
    const fiber = Effect.runFork(push);
    await cancel;
    await Effect.runPromise(Fiber.interrupt(fiber));
    process.send("interrupted");
    process.disconnect();
  } else {
    console.log(await Effect.runPromise(push.pipe(Effect.catchTag("Invalid", error => Effect.succeed(error._tag)))));
  }
`;

const initialize = async (location: string) => {
  await fs.mkdir(location, { recursive: true });
  gitIn(location)("init", "--bare", "-q", "-b", "main");
  fastImport(
    location,
    importCommit({ branch: "refs/heads/main", mark: 1, message: "source", files: [] }),
  );
};

describe.skipIf(!hasGit)("inbox Git subprocess", () => {
  it("submits branches and tags through the actual inbox despite push.followTags", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "inbox-revisions-"));
    let server: Server | null = null;
    try {
      const source = path.join(root, "source");
      const target = path.join(root, "project");
      await initialize(source);
      await initialize(target);
      await enableHubUnder(root, "project", []);
      const git = gitIn(source);
      git("tag", "-am", "release", "release", "main");
      git("-c", "advice.nestedTag=false", "tag", "-am", "nested", "nested", "release");
      git("config", "push.followTags", "true");
      server = await serve({ root });
      const inboxUrl = `${server.url}/project`;
      const cli = path.resolve("src/cli/main.ts");
      const head = git("rev-parse", "main").trim();
      for (const [index, revision] of ["main", "release", "nested"].entries()) {
        const id = `0198f2aa-71c4-7d2e-9a3b-${String(index).padStart(12, "0")}`;
        const result = await promisify(execFile)(
          process.execPath,
          [cli, "social", "inbox-submit", "--url", inboxUrl, "--id", id, revision],
          { cwd: source, env: gitEnv, encoding: "utf8" },
        );
        assert.equal(result.stdout.trim(), `refs/quarantine/inbox/${id}`);
        assert.equal(gitIn(target)("rev-parse", `refs/quarantine/inbox/${id}`).trim(), head);
      }
      const listed = await promisify(execFile)(
        process.execPath,
        [cli, "social", "inbox-list", "--root", root, "project"],
        { env: gitEnv },
      );
      assert.equal(listed.stdout.trim().split("\n").length, 3);
      for (const line of listed.stdout.trim().split("\n")) assert.equal(line.split("\t")[1], head);
      assert.equal(gitIn(target)("for-each-ref", "refs/tags"), "");
    } finally {
      if (server !== null) await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  for (const resistant of process.platform === "win32" ? [false] : [false, true]) {
    it(
      resistant
        ? "finishes cancellation when a Git wrapper ignores termination"
        : "interrupts the Git transport and releases its HTTP connection",
      async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "inbox-cancel-"));
        await initialize(root);
        const environment = { ...gitEnv };
        if (resistant) {
          const bin = path.join(root, "bin");
          await fs.mkdir(bin);
          await fs.writeFile(
            path.join(bin, "git"),
            `#!${process.execPath}
        const fs = require("node:fs");
        fs.writeFileSync("wrapper.pid", String(process.pid));
        process.on("SIGTERM", () => fs.writeFileSync("terminated", "yes"));
        require("node:child_process").spawn("git", process.argv.slice(2), {
          env: { ...process.env, PATH: ${JSON.stringify(gitEnv.PATH)} }, stdio: "inherit"
        });
        setInterval(() => {}, 1000);
      `,
            { mode: 0o755 },
          );
          environment.PATH = `${bin}${path.delimiter}${gitEnv.PATH ?? ""}`;
        }
        const started = Promise.withResolvers<void>();
        const closed = Promise.withResolvers<void>();
        const interrupted = Promise.withResolvers<void>();
        const server = http.createServer((_request, response) => {
          response.once("close", () => closed.resolve());
          started.resolve();
        });
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const child = spawn(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            script,
            `http://127.0.0.1:${address.port}/remote`,
            "cancel",
          ],
          {
            cwd: root,
            env: environment,
            stdio: ["ignore", "pipe", "pipe", "ipc"],
          },
        );
        let stderr = "";
        assert.ok(child.stderr !== null);
        child.stderr.on("data", (chunk: Buffer) => {
          stderr += chunk.toString();
        });
        child.on("message", (message: "interrupted") => {
          if (message === "interrupted") interrupted.resolve();
        });
        let finished = false;
        const completed = new Promise<void>((resolve, reject) => {
          child.once("error", reject);
          child.once("close", (code) => {
            finished = true;
            if (code === 0) resolve();
            else reject(new Error(stderr));
          });
        });
        void completed.catch((cause: unknown) => {
          started.reject(cause);
          interrupted.reject(cause);
        });
        let timeout: ReturnType<typeof setTimeout> | undefined;
        try {
          await started.promise;
          child.send("cancel");
          await Promise.race([
            Promise.all([interrupted.promise, closed.promise, completed]),
            new Promise<never>((_resolve, reject) => {
              timeout = setTimeout(
                () => reject(new Error("interrupted Git transport is still running")),
                3_000,
              );
            }),
          ]);
          if (resistant)
            assert.equal(await fs.readFile(path.join(root, "terminated"), "utf8"), "yes");
        } finally {
          clearTimeout(timeout);
          server.closeAllConnections();
          await new Promise<void>((resolve) => server.close(() => resolve()));
          if (resistant && !finished) {
            const pid = Number(await fs.readFile(path.join(root, "wrapper.pid"), "utf8"));
            assert.ok(Number.isSafeInteger(pid) && pid > 0);
            try {
              process.kill(pid, "SIGKILL");
            } catch (cause) {
              assert.equal(Predicate.hasProperty(cause, "code") ? cause.code : null, "ESRCH");
            }
          }
          await completed;
          await fs.rm(root, { recursive: true, force: true });
        }
      },
    );
  }

  it("publishes the requested ref and retains typed process failures", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "inbox-result-"));
    try {
      const source = path.join(root, "source");
      const target = path.join(root, "target");
      await initialize(source);
      await initialize(target);
      const run = (head: string, env = gitEnv) =>
        promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", script, target, "run", head],
          { cwd: source, env, timeout: 10_000 },
        );
      assert.equal((await run("main")).stdout.trim(), "refs/quarantine/inbox/fixture");
      assert.equal(
        gitIn(target)("rev-parse", "refs/quarantine/inbox/fixture"),
        gitIn(source)("rev-parse", "main"),
      );
      assert.equal((await run("missing")).stdout.trim(), "Invalid");
      assert.equal(
        (await run("main", { ...gitEnv, PATH: path.join(root, "no-git") })).stdout.trim(),
        "Invalid",
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
