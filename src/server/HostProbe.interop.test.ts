import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Predicate } from "effect";
import { probe } from "../../tools/probe-host.ts";
import { serve } from "../host/Node.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder, grantMemberUnder } from "../testing/Hub.ts";

const bind = async (server: http.Server) => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address !== null && !Predicate.isString(address));
  return `http://127.0.0.1:${address.port}`;
};
const close = (server: http.Server) =>
  new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );

describe.skipIf(!hasGit)("private host health probe", () => {
  it("verifies private Git/LFS data, rejects bad credentials and storage failures, and redacts CLI output", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-host-probe-"));
    let server: Awaited<ReturnType<typeof serve>> | undefined;
    try {
      const repository = path.join(root, "private-canary");
      await fs.mkdir(repository);
      gitIn(repository)("init", "--bare", "-q", "-b", "main");
      fastImport(
        repository,
        importCommit({
          branch: "refs/heads/main",
          mark: 1,
          message: "canary",
          files: [{ path: "private-file", content: "private-probe-content" }],
        }),
      );
      const oid = gitIn(repository)("rev-parse", "main").trim();
      const admin = await enableHubUnder(root, "private-canary", ["repo.admin"]);
      const reader = await grantMemberUnder(root, "private-canary", admin.root, admin.repoId, [
        "repo.read",
      ]);
      server = await serve({ root });
      const bytes = "private-lfs-content";
      const lfsOid = createHash("sha256").update(bytes).digest("hex");
      const url = `${server.url}/private-canary`;
      const uploaded = await fetch(`${url}/info/lfs/objects/${lfsOid}`, {
        method: "PUT",
        headers: { authorization: `Bearer ${admin.credential}` },
        body: bytes,
      });
      assert.equal(uploaded.status, 200);
      await uploaded.arrayBuffer();
      const config = { url, token: reader.credential, ref: "refs/heads/main", oid, lfsOid };
      const healthy = await probe(config);
      assert.equal(healthy.healthy, true);
      assert.equal(healthy.checks.length, 4);
      const unavailable = await probe({ ...config, token: "private-invalid-token" });
      assert.equal(unavailable.healthy, false);
      assert.equal(unavailable.checks.find((check) => check.name === "refs")?.status, 401);
      const drift = await probe({ ...config, oid: "a".repeat(40) });
      assert.equal(drift.checks.find((check) => check.name === "refs")?.outcome, "content");
      const missing = await probe({ ...config, lfsOid: "a".repeat(64) });
      assert.equal(missing.checks.find((check) => check.name === "lfs")?.status, 404);
      const run = promisify(execFile);
      const env = {
        ...process.env,
        GIT_PROBE_URL: url,
        GIT_PROBE_TOKEN: reader.credential,
        GIT_PROBE_REF: config.ref,
        GIT_PROBE_OID: oid,
        GIT_PROBE_LFS_OID: lfsOid,
      };
      const result = await run(process.execPath, ["tools/probe-host.ts"], { env, timeout: 15_000 });
      assert.match(result.stdout, /"healthy":true/);
      for (const secret of [reader.credential, url, oid, lfsOid, "private-canary", bytes])
        assert.equal((result.stdout + result.stderr).includes(secret), false);
      await assert.rejects(
        run(process.execPath, ["tools/probe-host.ts"], {
          env: { ...env, GIT_PROBE_URL: "https://private-user:private-password@example.com/repo" },
          timeout: 15_000,
        }),
        {
          code: 2,
          stderr: '{"component":"HostProbe","event":"probe_failed","reason":"configuration"}\n',
        },
      );
      await fs.rename(path.join(repository, "objects"), path.join(root, "saved-objects"));
      await fs.writeFile(path.join(repository, "objects"), "storage unavailable");
      assert.equal((await probe(config)).healthy, false);
    } finally {
      await server?.close({ force: true });
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("does not follow redirects carrying the probe credential", async () => {
    let received = 0;
    const destination = http.createServer((_, response) => {
      received++;
      response.end("unexpected");
    });
    const target = await bind(destination);
    const redirect = http.createServer((_, response) => {
      response.writeHead(302, { location: `${target}/private` });
      response.end();
    });
    try {
      const origin = await bind(redirect);
      const result = await probe({
        url: `${origin}/canary`,
        token: "private-token",
        ref: "refs/heads/main",
        oid: "a".repeat(40),
      });
      assert.equal(result.healthy, false);
      assert.ok(result.checks.every((check) => check.outcome === "status" && check.status === 302));
      assert.equal(received, 0);
    } finally {
      await close(redirect);
      await close(destination);
    }
  });

  it("refuses oversized or malformed canary responses", async () => {
    const server = http.createServer((request, response) => {
      if (request.url?.includes("info/refs")) {
        response.writeHead(401);
        response.end();
      } else if (request.url?.endsWith("/refs")) response.end(" ".repeat(1024 * 1024 + 1));
      else response.end("not JSON");
    });
    try {
      const origin = await bind(server);
      const result = await probe({
        url: `${origin}/canary`,
        token: "private-token",
        ref: "refs/heads/main",
        oid: "a".repeat(40),
      });
      assert.equal(result.healthy, false);
      assert.equal(result.checks.find((check) => check.name === "anonymous")?.outcome, "healthy");
      assert.equal(result.checks.find((check) => check.name === "refs")?.outcome, "unavailable");
      assert.equal(result.checks.find((check) => check.name === "object")?.outcome, "unavailable");
    } finally {
      await close(server);
    }
  });
});
