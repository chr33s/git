import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder } from "../testing/Hub.ts";

const start = async (root: string) => {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
    import { serve } from ${JSON.stringify(new URL("../host/Node.ts", import.meta.url).href)};
    const server = await serve({ root: process.argv[1] });
    console.log("READY " + server.url);
  `,
      root,
    ],
    {
      env: { ...gitEnv, GIT_HTTP_METRICS_INTERVAL_MS: "0" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const closed = once(child, "close");
  try {
    const url = await new Promise<string>((resolve, reject) => {
      let output = "";
      const timer = setTimeout(
        () => reject(new Error(`host startup timed out: ${output}`)),
        15_000,
      );
      const read = (bytes: Buffer) => {
        output = (output + bytes.toString()).slice(-16_384);
        const match = /READY (http:\/\/[^\s]+)/.exec(output);
        if (match?.[1] !== undefined) {
          clearTimeout(timer);
          resolve(match[1]);
        }
      };
      child.stdout.on("data", read);
      child.stderr.on("data", read);
      child.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once("exit", (code) => {
        clearTimeout(timer);
        reject(new Error(`host exited ${code}: ${output}`));
      });
    });
    return { child, closed, url };
  } catch (error) {
    child.kill("SIGKILL");
    await closed;
    throw error;
  }
};

describe.skipIf(!hasGit)("acknowledged Node HTTP writes after process death", () => {
  it("preserves an atomic push and private LFS upload after SIGKILL and permits another push", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-host-crash-"));
    const data = path.join(root, "data");
    const repository = path.join(data, "r");
    let host: Awaited<ReturnType<typeof start>> | undefined;
    try {
      await fs.mkdir(repository, { recursive: true });
      gitIn(repository)("init", "--bare", "-q", "-b", "main");
      fastImport(
        repository,
        importCommit({
          branch: "refs/heads/main",
          mark: 1,
          message: "base",
          files: [{ path: "file", content: "before" }],
        }),
      );
      const member = await enableHubUnder(data, "r", ["repo.admin"]);
      const git = (...args: string[]) =>
        promisify(execFile)(
          "git",
          [
            "-c",
            "credential.helper=",
            "-c",
            'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
            ...args,
          ],
          {
            cwd: root,
            env: { ...gitEnv, GIT_AUTH_TOKEN: member.credential, GIT_TERMINAL_PROMPT: "0" },
            timeout: 20_000,
          },
        );
      host = await start(data);
      await git("clone", "-q", `${host.url}/r`, "writer");
      const writer = path.join(root, "writer");
      await fs.writeFile(path.join(writer, "file"), "acknowledged before SIGKILL\n");
      gitIn(writer)("commit", "-qam", "accepted");
      const accepted = gitIn(writer)("rev-parse", "HEAD").trim();
      await git(
        "-C",
        writer,
        "push",
        "--atomic",
        `${host.url}/r`,
        "HEAD:refs/heads/main",
        "HEAD:refs/heads/side",
      );
      const content = "acknowledged private LFS bytes\n";
      const oid = createHash("sha256").update(content).digest("hex");
      const authorization = `Bearer ${member.credential}`;
      const upload = await fetch(`${host.url}/r/info/lfs/objects/${oid}`, {
        method: "PUT",
        headers: { authorization },
        body: content,
        signal: AbortSignal.timeout(10_000),
      });
      assert.equal(upload.status, 200);
      await upload.arrayBuffer();
      host.child.kill("SIGKILL");
      const [, signal] = await host.closed;
      assert.equal(signal, "SIGKILL", "the writer must die without graceful host cleanup");
      host = undefined;
      gitIn(repository)("fsck", "--strict");
      await assert.rejects(fs.access(path.join(repository, ".git-plus-ref-transaction.json")), {
        code: "ENOENT",
      });
      host = await start(data);
      const anonymous = await fetch(`${host.url}/r/refs`, { signal: AbortSignal.timeout(10_000) });
      assert.equal(anonymous.status, 401);
      await anonymous.arrayBuffer();
      await git("clone", "-q", `${host.url}/r`, "recovered");
      const recovered = path.join(root, "recovered");
      assert.equal(gitIn(recovered)("rev-parse", "HEAD").trim(), accepted);
      assert.equal(gitIn(recovered)("rev-parse", "refs/remotes/origin/side").trim(), accepted);
      assert.equal(gitIn(recovered)("show", "HEAD:file"), "acknowledged before SIGKILL\n");
      gitIn(recovered)("fsck", "--strict");
      const download = await fetch(`${host.url}/r/info/lfs/objects/${oid}`, {
        headers: { authorization },
        signal: AbortSignal.timeout(10_000),
      });
      assert.equal(download.status, 200);
      const received = new Uint8Array(await download.arrayBuffer());
      assert.equal(createHash("sha256").update(received).digest("hex"), oid);
      assert.equal(new TextDecoder().decode(received), content);
      await fs.writeFile(path.join(recovered, "file"), "accepted after restart\n");
      gitIn(recovered)("commit", "-qam", "recovered writer");
      await git(
        "-C",
        recovered,
        "push",
        "--atomic",
        `${host.url}/r`,
        "HEAD:refs/heads/main",
        "HEAD:refs/heads/side",
      );
      assert.equal(gitIn(repository)("show", "main:file"), "accepted after restart\n");
      assert.equal(
        gitIn(repository)("rev-parse", "main").trim(),
        gitIn(repository)("rev-parse", "side").trim(),
      );
      gitIn(repository)("fsck", "--strict");
    } finally {
      host?.child.kill("SIGKILL");
      await host?.closed;
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
