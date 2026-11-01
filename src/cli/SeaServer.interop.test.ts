import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder, grantMemberUnder } from "../testing/Hub.ts";

const executable = path.resolve("dist/sea/git+");
describe.skipIf(!hasGit || !existsSync(executable))("installed standalone HTTP host", () => {
  it("authenticates flush probes and accepts a chunked atomic push with no Node or source checkout", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "sea-host-"));
    let child: ReturnType<typeof spawn> | undefined;
    let closed: ReturnType<typeof once> | undefined;
    try {
      const install = path.join(root, "install");
      const data = path.join(root, "data");
      const repository = path.join(data, "r");
      await fs.mkdir(install);
      await fs.mkdir(repository, { recursive: true });
      gitIn(repository)("init", "--bare", "-q", "-b", "main");
      fastImport(
        repository,
        importCommit({
          branch: "refs/heads/main",
          mark: 1,
          message: "base",
          files: [{ path: "file", content: "retained" }],
        }),
      );
      const member = await enableHubUnder(data, "r", ["repo.admin"]);
      const reader = await grantMemberUnder(data, "r", member.root, member.repoId, ["repo.read"]);
      const before = gitIn(repository)("rev-parse", "main").trim();
      const installed = path.join(install, "git+");
      await fs.copyFile(executable, installed);
      await fs.chmod(installed, 0o755);
      child = spawn(
        installed,
        ["serve", "--root", data, "--hostname", "127.0.0.1", "--port", "0"],
        {
          cwd: install,
          env: { ...gitEnv, PATH: install, NO_COLOR: "1", GIT_HTTP_METRICS_INTERVAL_MS: "0" },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      closed = once(child, "close");
      const host = child;
      const url = await new Promise<string>((resolve, reject) => {
        let output = "";
        const timer = setTimeout(
          () => reject(new Error(`standalone startup timed out: ${output}`)),
          15_000,
        );
        const read = (bytes: Buffer) => {
          output = (output + bytes.toString()).slice(-16_384);
          const match = /server on (http:\/\/[^,\s]+)/.exec(output);
          if (match?.[1] !== undefined) {
            clearTimeout(timer);
            resolve(match[1]);
          }
        };
        host.stdout?.on("data", read);
        host.stderr?.on("data", read);
        host.once("error", (error) => {
          clearTimeout(timer);
          reject(error);
        });
        host.once("exit", (code) => {
          clearTimeout(timer);
          reject(new Error(`standalone exited ${code}: ${output}`));
        });
      });
      for (const [token, status] of [
        [null, 401],
        [reader.credential, 403],
        [member.credential, 200],
      ] as const) {
        const headers = new Headers();
        if (token !== null)
          headers.set("authorization", `Basic ${Buffer.from(`git:${token}`).toString("base64")}`);
        const probe = await fetch(`${url}/r/git-receive-pack`, {
          method: "POST",
          headers,
          body: "0000",
          signal: AbortSignal.timeout(10_000),
        });
        assert.equal(probe.status, status);
        if (status === 200) assert.equal((await probe.arrayBuffer()).byteLength, 0);
        else await probe.arrayBuffer();
      }
      for (const version of ["version=0", "version=2"]) {
        for (const token of [null, reader.credential]) {
          const headers = new Headers({ "git-protocol": version });
          if (token !== null) headers.set("authorization", `Bearer ${token}`);
          const probe = await fetch(`${url}/r/git-upload-pack`, {
            method: "POST",
            headers,
            body: "0000",
            signal: AbortSignal.timeout(10_000),
          });
          assert.equal(probe.status, token === null ? 401 : 200);
          const bytes = await probe.arrayBuffer();
          if (token !== null) assert.equal(bytes.byteLength, 0);
        }
      }
      assert.equal(gitIn(repository)("rev-parse", "main").trim(), before);
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
      await git("-c", "http.postBuffer=65536", "clone", "-q", `${url}/r`, "client");
      const client = path.join(root, "client");
      const content = randomBytes(256 * 1024);
      await fs.writeFile(path.join(client, "streamed.bin"), content);
      gitIn(client)("add", ".");
      gitIn(client)("commit", "-qm", "chunked candidate");
      const candidate = gitIn(client)("rev-parse", "HEAD").trim();
      await git(
        "-C",
        client,
        "-c",
        "http.postBuffer=65536",
        "push",
        "--atomic",
        "origin",
        "HEAD:refs/heads/main",
        "HEAD:refs/heads/side",
      );
      for (const ref of ["main", "side"])
        assert.equal(gitIn(repository)("rev-parse", ref).trim(), candidate);
      gitIn(repository)("fsck", "--strict");
      await git("clone", "-q", `${url}/r`, "verified");
      assert.deepEqual(await fs.readFile(path.join(root, "verified", "streamed.bin")), content);
      gitIn(path.join(root, "verified"))("fsck", "--strict");
    } finally {
      child?.kill("SIGTERM");
      const force = setTimeout(() => child?.kill("SIGKILL"), 10_000);
      try {
        await closed;
      } finally {
        clearTimeout(force);
        await fs.rm(root, { recursive: true, force: true });
      }
    }
  });
});
