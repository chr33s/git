/** Real ENOSPC rehearsal. Only accepts the dedicated disposable Linux tmpfs mount. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { serve } from "../src/host/Node.ts";
import { enableHubUnder } from "../src/testing/Hub.ts";
import { fastImport, gitEnv, gitIn, importCommit } from "../src/testing/Git.ts";

const mount = "/git-capacity-rehearsal";
const report = process.argv[2];
const resource = process.argv[3] ?? "bytes";
assert.ok(
  process.argv.length === 3 || process.argv.length === 4,
  "usage: node tools/verify-storage-capacity.ts <new-report.json> [bytes|inodes]",
);
assert.ok(resource === "bytes" || resource === "inodes", "expected bytes or inodes");
assert.ok(report);
assert.equal(process.platform, "linux", "requires a disposable Linux container");
assert.ok(
  !path.resolve(report).startsWith(mount + "/"),
  "report must be outside the full filesystem",
);
assert.deepEqual(await fs.readdir(mount), [], "dedicated mount must be empty");
const filesystem = await fs.statfs(mount);
assert.equal(filesystem.type, 0x01021994, "requires tmpfs, never fill a persistent filesystem");
assert.notEqual(
  (await fs.stat(mount)).dev,
  (await fs.stat(path.dirname(mount))).dev,
  "requires a separate mount",
);
const capacityBytes = filesystem.blocks * filesystem.bsize;
if (resource === "inodes")
  assert.ok(filesystem.files >= 128 && filesystem.files <= 4096, "mount must have 128–4096 inodes");
assert.ok(
  capacityBytes >= 16 * 1024 * 1024 && capacityBytes <= 64 * 1024 * 1024,
  "mount must be 16–64 MiB",
);
const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-full-volume-"));
const data = await fs.mkdtemp(path.join(mount, "data-"));
const filler = path.join(mount, `filler-${crypto.randomUUID()}`);
let server: Awaited<ReturnType<typeof serve>> | undefined;
const ready = async (origin: string | undefined, expected: number): Promise<void> => {
  assert.ok(origin, "operations listener must be enabled");
  const deadline = performance.now() + 15_000;
  for (;;) {
    const response: Response = await fetch(`${origin}/ready`, {
      signal: AbortSignal.timeout(2000),
    });
    const body = await response.text();
    if (response.status === expected) {
      assert.equal(
        body,
        JSON.stringify({ ready: expected === 200, storage: expected === 200, capacity: true }),
      );
      return;
    }
    assert.ok(performance.now() < deadline, `readiness did not become ${expected}`);
    await delay(50);
  }
};
try {
  // Exercise the real host startup boundary before any repository is created.
  process.env.GIT_STORAGE_FILESYSTEM_BYTES = String(capacityBytes);
  await assert.rejects(serve({ root: "/tmp" }), /separate filesystem/);
  // Config's environment provider is process-scoped. A different deployment
  // configuration needs a fresh process, not an in-process environment edit.
  await promisify(execFile)(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import assert from "node:assert/strict";
       import { serve } from "./src/host/Node.ts";
       await assert.rejects(serve({ root: process.argv[1] }), /exceeds configured ceiling/);`,
      data,
    ],
    {
      env: { ...process.env, GIT_STORAGE_FILESYSTEM_BYTES: String(capacityBytes - 1) },
      timeout: 30_000,
    },
  );
  await assert.rejects(serve({ root: data, repository: "/tmp" }), /separate filesystem/);
  assert.deepEqual(await fs.readdir(data), [], "startup refusal must not create state");
  const repository = path.join(data, "r");
  await fs.mkdir(repository);
  gitIn(repository)("init", "--bare", "-q", "-b", "main");
  fastImport(
    repository,
    importCommit({
      branch: "refs/heads/main",
      mark: 1,
      message: "accepted",
      files: [{ path: "file", content: "acknowledged history" }],
    }),
  );
  const before = gitIn(repository)("rev-parse", "main").trim();
  gitIn(repository)("update-ref", "refs/heads/side", before);
  const member = await enableHubUnder(data, "r", ["repo.admin"]);
  const run = promisify(execFile);
  const git = (...args: string[]) =>
    run(
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
        timeout: 30_000,
      },
    );
  server = await serve({ root: data, operationsPort: 0 });
  await ready(server.operationsUrl, 200);
  await git("clone", "-q", `${server.url}/r`, "client");
  const client = path.join(root, "client");
  await fs.writeFile(path.join(client, "file"), randomBytes(1024 * 1024));
  gitIn(client)("commit", "-qam", "candidate larger than remaining capacity");
  const candidate = gitIn(client)("rev-parse", "HEAD").trim();
  let exhausted = false;
  if (resource === "inodes") {
    await fs.mkdir(filler, { mode: 0o700 });
    for (let count = 0; count <= filesystem.files; count++) {
      try {
        await fs.writeFile(path.join(filler, String(count)), "", { flag: "wx", mode: 0o600 });
      } catch (cause) {
        if (!(cause instanceof Error && "code" in cause && cause.code === "ENOSPC")) throw cause;
        exhausted = true;
        break;
      }
    }
  } else {
    const allocation = await fs.open(filler, "wx", 0o600);
    try {
      const block = Buffer.alloc(1024 * 1024, 1);
      for (let count = 0; count <= 64; count++) {
        try {
          await allocation.writeFile(block);
        } catch (cause) {
          if (!(cause instanceof Error && "code" in cause && cause.code === "ENOSPC")) throw cause;
          exhausted = true;
          break;
        }
      }
      assert.ok(exhausted, "the kernel must actually report ENOSPC");
      // Leave room for reservations and metadata, but not the incoming blob.
      const size = (await allocation.stat()).size;
      await allocation.truncate(size - 512 * 1024);
      await allocation.sync();
    } finally {
      await allocation.close();
    }
  }
  assert.ok(exhausted, "the kernel must actually report ENOSPC");
  const limited = await fs.statfs(mount);
  if (resource === "inodes") {
    assert.equal(limited.ffree, 0, "must exhaust inodes");
    assert.ok(limited.bavail * limited.bsize > 16 * 1024 * 1024, "byte capacity must remain");
    await ready(server.operationsUrl, 503);
  } else assert.ok(limited.bavail * limited.bsize <= 512 * 1024);
  const push = (url: string) =>
    git(
      "-C",
      client,
      "push",
      "--atomic",
      `${url}/r`,
      "HEAD:refs/heads/main",
      "HEAD:refs/heads/side",
    );
  await assert.rejects(push(server.url));
  for (const ref of ["main", "side"])
    assert.equal(gitIn(repository)("rev-parse", ref).trim(), before);
  assert.equal(gitIn(repository)("show", "main:file"), "acknowledged history");
  gitIn(repository)("fsck", "--strict");
  const bytes = randomBytes(1024 * 1024);
  const oid = createHash("sha256").update(bytes).digest("hex");
  const authorization = `Bearer ${member.credential}`;
  const upload = (url: string) =>
    fetch(`${url}/r/info/lfs/objects/${oid}`, {
      method: "PUT",
      headers: { authorization },
      body: bytes,
      signal: AbortSignal.timeout(15_000),
    });
  const refused = await upload(server.url);
  assert.equal(refused.status, 500, "full filesystem must refuse LFS publication");
  await refused.arrayBuffer();
  const missing = await fetch(`${server.url}/r/info/lfs/objects/${oid}`, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(missing.status, 404, "partial LFS bytes must not be published");
  await missing.arrayBuffer();
  const remaining = await fs.readdir(data, { recursive: true });
  assert.equal(
    remaining.some(
      (name) =>
        name.endsWith(".tmp") ||
        name.endsWith(".lock") ||
        name.endsWith(".git-plus-ref-transaction.json"),
    ),
    false,
  );
  await server.close({ force: true });
  server = undefined;
  await fs.rm(filler, { recursive: true });
  server = await serve({ root: data, operationsPort: 0 });
  await ready(server.operationsUrl, 200);
  await push(server.url);
  for (const ref of ["main", "side"])
    assert.equal(gitIn(repository)("rev-parse", ref).trim(), candidate);
  gitIn(repository)("fsck", "--strict");
  const accepted = await upload(server.url);
  assert.equal(accepted.status, 200);
  await accepted.arrayBuffer();
  const downloaded = await fetch(`${server.url}/r/info/lfs/objects/${oid}`, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(downloaded.status, 200);
  assert.equal(
    createHash("sha256")
      .update(new Uint8Array(await downloaded.arrayBuffer()))
      .digest("hex"),
    oid,
  );
  await git("clone", "-q", `${server.url}/r`, "recovered");
  assert.equal(gitIn(path.join(root, "recovered"))("rev-parse", "HEAD").trim(), candidate);
  gitIn(path.join(root, "recovered"))("fsck", "--strict");
  const recovered = await fs.statfs(mount);
  await fs.writeFile(
    report,
    JSON.stringify(
      {
        schemaVersion: 1,
        measuredAt: new Date().toISOString(),
        node: process.version,
        platform: process.platform,
        arch: process.arch,
        kernel: os.release(),
        git: (await run("git", ["--version"])).stdout.trim(),
        revision: (
          await run("git", ["-c", `safe.directory=${process.cwd()}`, "rev-parse", "HEAD"])
        ).stdout.trim(),
        dirty:
          (await run("git", ["-c", `safe.directory=${process.cwd()}`, "status", "--porcelain"]))
            .stdout.length > 0,
        filesystem: "disposable tmpfs",
        exhaustedResource: resource,
        capacityBytes,
        capacityInodes: filesystem.files,
        startupFilesystemCeilingVerified: true,
        missingMountRefused: true,
        oversizedMountRefused: true,
        singleRepositoryFilesystemChecked: true,
        availableBeforeRequests: limited.bavail * limited.bsize,
        availableInodesBeforeRequests: limited.ffree,
        availableAfterRecovery: recovered.bavail * recovered.bsize,
        availableInodesAfterRecovery: recovered.ffree,
        inodeOutageReadinessVerified: resource === "inodes",
        kernelEnospcObserved: true,
        refusedPushPreservedBothRefs: true,
        refusedLfsStatus: refused.status,
        partialLfsAbsent: true,
        temporaryFilesCleaned: true,
        retryAfterHostRestartVerified: true,
        recoveredCloneFsckPassed: true,
        limitsOfEvidence:
          "Real filesystem capacity enforcement on volatile tmpfs; not persistent-volume quotas, physical-disk exhaustion, or power-loss durability.",
      },
      null,
      2,
    ) + "\n",
    { flag: "wx", mode: 0o600 },
  );
  console.log(`Verified filesystem-capacity refusal and recovery: ${report}`);
} finally {
  await server?.close({ force: true });
  await fs.rm(data, { recursive: true, force: true });
  await fs.rm(filler, { recursive: true, force: true });
  await fs.rm(root, { recursive: true, force: true });
}
