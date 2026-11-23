/** Exercise the actual Alchemy Worker/DO entry locally, without cloud credentials. */
import assert from "node:assert/strict";
import { measureWorkerBundle } from "./measure-worker-bundle.ts";
import { spawn, execFile } from "node:child_process";
import { once } from "node:events";
import * as fs from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { chromium } from "playwright";
import { createHash } from "node:crypto";
import { Effect, Layer, Predicate, Schema, Stream } from "effect";
import { getPlatformProxy } from "@alchemy.run/cloudflare-runtime/core/platform-proxy";
import type { R2Bucket } from "@cloudflare/workers-types";
import { R2Bucket as LocalR2Bucket } from "@alchemy.run/cloudflare-runtime/core/bindings";
import { enableHub } from "../src/testing/Hub.ts";
import * as Provisioning from "../src/server/Provisioning.ts";
import * as Repository from "../src/git/Repository.ts";
import { stores } from "../src/git/Node.ts";
import { objectStore as r2Objects, packStore as r2Packs } from "../src/git/Cloudflare.ts";
import { nonceOf, repoOf, operationOf } from "../src/client/Authorize.ts";
import { mintDelegation, signEnvelope } from "../src/server/Auth.ts";
import { fingerprint, generate, type PrivateKey } from "../src/crypto/SshSignature.ts";
import * as Certificate from "../src/trust/Certificate.ts";
import * as Log from "../src/trust/Log.ts";
import * as HubEvent from "../src/hub/Event.ts";
import * as PullRequest from "../src/hub/PullRequest.ts";
import { fetchRepository } from "../src/client/Fetch.ts";
import { ObjectStore, RefStore } from "../src/git/Store.ts";
import * as Refspec from "../src/git/Refspec.ts";
import { reconcileGitCapacity, snapshotFault } from "./worker-storage-fault.ts";
import { seedLegacyRef } from "./worker-legacy-fixture.ts";
import { verifyBackupAdministration } from "./worker-backup-fixture.ts";
import { restoreLocalState } from "./worker-restore-fixture.ts";
import { sourceVersions } from "./worker-upgrade-fixture.ts";
import {
  verifyAdmission,
  verifyDeadline,
  verifyLfsDeadline,
  verifyMutationDeadlines,
} from "./worker-admission-fixture.ts";
import { probe as probeHost } from "./probe-host.ts";
import * as Snapshot from "../src/server/Snapshot.ts";
import { WebhookWire, RemoteWire } from "../src/server/ApiContract.ts";
import { defaults as requestLimits } from "../src/server/RequestLimits.ts";

const bundleReportPath = process.argv[2];
assert.ok(process.argv.length <= 3, "usage: node tools/verify-worker.ts [new-bundle-report.json]");
if (bundleReportPath !== undefined)
  assert.equal(
    await fs.stat(bundleReportPath).then(
      () => true,
      (error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT") return false;
        throw error;
      },
    ),
    false,
    "bundle report must be a new file",
  );

for (const file of [
  "_headers",
  "hub/index.html",
  "hub/bootstrap.js",
  "hub/main.js",
  "hub/main.css",
])
  await fs.access(path.join("dist/worker-assets", file));

const portProbe = createServer();
await new Promise<void>((resolve, reject) => {
  portProbe.once("error", reject);
  portProbe.listen(0, "127.0.0.1", resolve);
});
const address = portProbe.address();
assert.ok(address !== null && !Predicate.isString(address));
const port = address.port;
await new Promise<void>((resolve, reject) =>
  portProbe.close((error) => (error ? reject(error) : resolve())),
);

const stage = `verify-worker-${crypto.randomUUID()}`;
const scratch = await fs.mkdtemp(path.join(tmpdir(), "git-worker-verify-"));
// Keep all emulator/deployment state owned by this verifier, so offline restore
// can remove it without touching another development stage's storage.
const installation = path.join(scratch, "installation");
await fs.mkdir(installation);
for (const name of ["src", "tools", "node_modules", "dist", "package.json", "tsconfig.json"])
  await fs.symlink(path.resolve(name), path.join(installation, name));
const alchemyRoot = path.join(installation, ".alchemy");
const localStorage = path.join(alchemyRoot, "local");
let uploadIdleMs = "30000";
let clientRate = "100000";
let retainedLfsBytes = "10737418240";
let retainedGitBytes = "10737418240";
let refMetadataRows = "100000";
let registryRows = "128";
let doCapacity = "8";
let allowAnonymousWrites = "false";
const spawnWorker = () =>
  spawn(
    process.execPath,
    [
      path.resolve("node_modules/alchemy/bin/cli.js"),
      "dev",
      "src/alchemy.run.ts",
      "--stage",
      stage,
      "--env-file",
      "/dev/null",
      "--no-input",
    ],
    {
      cwd: installation,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        CI: "true",
        NO_COLOR: "1",
        GIT_DEV_PORT: String(port),
        GIT_DEV_STRICT_PORT: "true",
        GIT_CF_REQUEST_DEADLINE_MS: "10000",
        GIT_CF_UPLOAD_IDLE_MS: uploadIdleMs,
        GIT_CF_RATE_NAMESPACE: "1",
        GIT_CF_RATE_PER_MINUTE: clientRate,
        GIT_LFS_RETAINED_BYTES: retainedLfsBytes,
        GIT_CF_RETAINED_BYTES: retainedGitBytes,
        GIT_CF_REF_METADATA_ROWS: refMetadataRows,
        GIT_CF_REGISTRY_ROWS: registryRows,
        GIT_CF_MAX_ACTIVE: doCapacity,
        ALLOW_ANONYMOUS_WRITES: allowAnonymousWrites,
        GIT_EGRESS_TRUSTED_ORIGINS: "",
      },
    },
  );
let child = spawnWorker();
let closed = once(child, "close");
let output = "";
let workerOrigin: string | undefined;
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
let clearStorageFault: (() => Promise<void>) | undefined;
const readyWorker = () =>
  new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Worker startup timed out:\n${output}`)),
      60_000,
    );
    const append = (chunk: Buffer) => {
      output = (output + chunk.toString()).slice(-128 * 1024);
      // Alchemy can select a different local port after its initial bind probe.
      // Only trust the loopback endpoint reported by this verifier's own child.
      const origin = /\[git\] Started in[^\n]* → (http:\/\/localhost:\d+)\//.exec(output)?.[1];
      if (origin !== undefined) {
        clearTimeout(timer);
        if (workerOrigin !== undefined && workerOrigin !== origin) {
          reject(
            new Error("Worker changed its local origin during a restart; rerun with stable ports"),
          );
          return;
        }
        workerOrigin = origin;
        resolve();
      }
    };
    child.stdout.on("data", append);
    child.stderr.on("data", append);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`Worker exited ${code}:\n${output}`));
    });
  });
const stopWorker = async () => {
  const stopping = child;
  if (stopping.exitCode === null && stopping.signalCode === null) stopping.kill("SIGINT");
  const force = setTimeout(() => {
    if (stopping.pid !== undefined) {
      try {
        process.kill(-stopping.pid, "SIGKILL");
      } catch {
        /* already exited */
      }
    }
  }, 10_000);
  try {
    await closed;
  } finally {
    clearTimeout(force);
  }
};
// Worker logs reach this process through alchemy's piped stdout, which can lag
// the HTTP response that caused them; wait for a line rather than sampling once.
const logged = async (pattern: RegExp, message = `worker never logged ${pattern}`) => {
  const deadline = Date.now() + 15_000;
  while (!pattern.test(output) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.match(output, pattern, message);
};
const startWorker = async () => {
  output = "";
  child = spawnWorker();
  closed = once(child, "close");
  await readyWorker();
};
try {
  await readyWorker();
  assert.ok(workerOrigin);
  const origin = workerOrigin;
  const repo = `${origin}/release-proof`;
  for (const route of ["/hub/tasks", "/hub/main.js", "/hub/bootstrap.js"]) {
    const response = await fetch(origin + route, { signal: AbortSignal.timeout(10_000) });
    if (response.status !== 200)
      assert.fail(`${route}: HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`);
    assert.match(
      response.headers.get("content-type") ?? "",
      route.endsWith(".js") ? /javascript/ : /text\/html/,
    );
    assert.match(response.headers.get("content-security-policy") ?? "", /frame-ancestors 'none'/);
    assert.ok((await response.arrayBuffer()).byteLength > 0);
  }
  const refs = async () => {
    const response = await fetch(`${repo}/refs`, { signal: AbortSignal.timeout(10_000) });
    assert.equal(response.status, 200);
    return await response.json();
  };
  assert.deepEqual(await refs(), { refs: [], head: "refs/heads/main" });
  const denied = await fetch(`${repo}/commit`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ branch: "main", message: "must be refused" }),
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(denied.status, 403);
  await denied.arrayBuffer();
  for (const route of ["conformance", "registry-conformance"]) {
    const response = await fetch(`${repo}/${route}`, { signal: AbortSignal.timeout(10_000) });
    assert.equal(response.status, 404, "production entry must not expose conformance routes");
    await response.arrayBuffer();
  }
  const execute = promisify(execFile);
  const git = (...args: string[]) =>
    execute("git", args, {
      cwd: scratch,
      timeout: 20_000,
      env: {
        ...process.env,
        GIT_CONFIG_GLOBAL: "/dev/null",
        GIT_CONFIG_NOSYSTEM: "1",
        GIT_TERMINAL_PROMPT: "0",
      },
    });
  await git("clone", "-q", repo, "client");
  const legacyContent = "legacy public source content";
  await fs.writeFile(path.join(scratch, "client", "legacy.txt"), legacyContent);
  await git("-C", "client", "add", "legacy.txt");
  await git(
    "-C",
    "client",
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.com",
    "commit",
    "--allow-empty",
    "-qm",
    "refused push",
  );
  await assert.rejects(git("-C", "client", "push", "origin", "HEAD:refs/heads/main"), /403/);
  assert.deepEqual(await refs(), { refs: [], head: "refs/heads/main" });
  const lfs = `${repo}/info/lfs/objects`;
  const oid = "a".repeat(64);
  const batch = async (body: string) =>
    await fetch(`${lfs}/batch`, {
      method: "POST",
      headers: { "content-type": "application/vnd.git-lfs+json" },
      body,
      signal: AbortSignal.timeout(10_000),
    });
  const missing = await batch(
    JSON.stringify({ operation: "download", objects: [{ oid, size: 4 }] }),
  );
  assert.equal(missing.status, 200);
  assert.deepEqual(await missing.json(), {
    transfer: "basic",
    objects: [{ oid, size: 4, error: { code: 404, message: "object not found" } }],
  });
  const upload = await fetch(`${lfs}/${oid}`, {
    method: "PUT",
    body: "data",
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(upload.status, 403, "the actual Worker must deny anonymous LFS writes");
  await upload.arrayBuffer();
  const direct = await fetch(`${lfs}/${oid}`, { signal: AbortSignal.timeout(10_000) });
  assert.equal(direct.status, 404, "a refused upload must not create an LFS object");
  await direct.arrayBuffer();
  const excessiveObjects = await batch(
    JSON.stringify({
      operation: "download",
      objects: Array.from({ length: requestLimits.lfsBatchObjects + 1 }, () => ({ oid, size: 4 })),
    }),
  );
  assert.equal(
    excessiveObjects.status,
    413,
    "the production DO must enforce the LFS object budget",
  );
  assert.match(await excessiveObjects.text(), /LFS batch object limit exceeded/);
  // Valid JSON whose trailing whitespace crosses the streaming byte ceiling.
  const excessiveBytes = await batch(
    JSON.stringify({ operation: "download", objects: [] }) +
      " ".repeat(requestLimits.lfsBatchBytes),
  );
  assert.equal(excessiveBytes.status, 413, "the production DO must enforce the LFS byte budget");
  assert.match(await excessiveBytes.text(), /LFS batch byte limit exceeded/);
  const responsive = await batch(JSON.stringify({ operation: "download", objects: [] }));
  assert.equal(responsive.status, 200, "LFS remains responsive after both budget refusals");
  assert.deepEqual(await responsive.json(), { transfer: "basic", objects: [] });

  // Seed through the operator's local R2 binding, never a public/test route.
  // This same repo has already published an anonymous empty snapshot above.
  // Seed an offline legacy repository next, with real content and matching refs.
  const identity = path.join(scratch, "identity.git");
  await git("init", "-q", "--bare", "-b", "main", identity);
  const identityFixture = await enableHub(identity, ["repo.admin"]);
  const member = {
    ...identityFixture,
    // Multiple source versions and restart/restore phases can exceed five minutes.
    credential: await Effect.runPromise(
      mintDelegation({
        key: identityFixture.member,
        repo: identityFixture.repoId,
        capabilities: ["repo.admin"],
        ttlSeconds: 3600,
      }),
    ),
  };
  const artifact = await Effect.runPromise(
    Provisioning.prepare("release-proof").pipe(
      Effect.provide(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(identity))),
      ),
    ),
  );
  const bucketState = Schema.decodeUnknownSync(
    Schema.Struct({
      providerMode: Schema.Literal("local"),
      attr: Schema.Struct({ bucketName: Schema.String.check(Schema.isPattern(/^dev:/)) }),
    }),
  )(
    JSON.parse(
      await fs.readFile(path.join(alchemyRoot, "state/git", stage, "git-objects.json"), "utf8"),
    ),
  );
  const operatorBucket = async (action: (bucket: R2Bucket) => Promise<void>) => {
    const proxy = await getPlatformProxy<{ R2: R2Bucket }>({
      name: `provisioning-${crypto.randomUUID()}`,
      persist: { directory: localStorage },
      bindings: [LocalR2Bucket.local({ binding: "R2", id: bucketState.attr.bucketName })],
    });
    try {
      await action(proxy.env.R2);
    } finally {
      await proxy.dispose();
    }
  };
  const sentinel = `_operator/verification/${stage}`;
  await stopWorker();
  const localWorker = Schema.decodeSync(
    Schema.fromJsonString(
      Schema.Struct({
        providerMode: Schema.Literal("local"),
        attr: Schema.Struct({
          workerName: Schema.String,
          durableObjectNamespaces: Schema.Struct({ Repo: Schema.String }),
        }),
      }),
    ),
  )(await fs.readFile(path.join(alchemyRoot, "state/git", stage, "git.json"), "utf8"));
  const legacyTip = (await git("-C", "client", "rev-parse", "HEAD")).stdout.trim();
  const removeLegacyRef = await seedLegacyRef(
    localStorage,
    localWorker.attr.durableObjectNamespaces.Repo,
    "release-proof",
    legacyTip,
  );
  await operatorBucket(async (bucket) => {
    await bucket.put(sentinel, stage);
    const legacy = await Effect.runPromise(
      Effect.gen(function* () {
        const source = yield* ObjectStore;
        const destination = yield* ObjectStore.pipe(
          Effect.provide(
            r2Objects(bucket, "release-proof").pipe(
              Layer.provide(r2Packs(bucket, "release-proof")),
            ),
          ),
        );
        yield* Stream.runForEach(source.list, (oid) =>
          Effect.gen(function* () {
            assert.equal(yield* destination.write(yield* source.read(oid)), oid);
          }),
        );
        return yield* Snapshot.capture();
      }).pipe(
        Effect.provide(
          Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provideMerge(stores(path.join(scratch, "client", ".git"))),
          ),
        ),
      ),
    );
    assert.equal(legacy.anonymousRead, true);
    assert.ok(legacy.refs.some((ref) => ref.name === "refs/heads/main" && ref.oid === legacyTip));
    await bucket.put(Snapshot.keyOf("release-proof"), Snapshot.encode(legacy));
  });
  await startWorker();
  await verifyBackupAdministration(
    localStorage,
    localWorker.attr.workerName,
    localWorker.attr.durableObjectNamespaces.Repo,
  );
  assert.deepEqual(await refs(), {
    refs: [{ name: "refs/heads/main", oid: legacyTip }],
    head: "refs/heads/main",
  });
  await git("clone", "-q", repo, "legacy-public-client");
  assert.equal(
    await fs.readFile(path.join(scratch, "legacy-public-client", "legacy.txt"), "utf8"),
    legacyContent,
  );
  await git("-C", "legacy-public-client", "fsck", "--strict");
  await logged(/snapshot age observed/, "actual edge must report loaded snapshot age");
  await verifyAdmission(repo);
  await verifyDeadline(repo);
  await operatorBucket(async (bucket) => {
    await bucket.put(Provisioning.keyOf("release-proof"), artifact);
  });
  clearStorageFault = () =>
    snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "none");
  await stopWorker();
  await snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "delete");
  await startWorker();
  const blocked = await Promise.all(
    Array.from({ length: 3 }, () =>
      fetch(`${repo}/info/refs?service=git-upload-pack`, { signal: AbortSignal.timeout(20_000) }),
    ),
  );
  for (const response of blocked) {
    assert.equal(
      response.status,
      503,
      "failed snapshot invalidation must refuse installation and every concurrent anonymous read",
    );
    await response.arrayBuffer();
  }
  await logged(/snapshot invalidation failed/);
  await operatorBucket(async (bucket) => {
    const previous = await bucket.get(Snapshot.keyOf("release-proof"));
    assert.ok(previous, "the injected delete failure must leave the previous snapshot present");
    const retained = Snapshot.decode(new Uint8Array(await previous.arrayBuffer()));
    assert.ok(retained);
    assert.ok(retained.refs.some((ref) => ref.oid === legacyTip));
    assert.equal(retained.anonymousRead, true);
  });
  await stopWorker();
  // Bootstrap intentionally requires empty authoritative refs. Remove only the
  // offline fixture ref; retain the populated historical snapshot and objects
  // to prove that successful bootstrap cannot revive its old public authority.
  removeLegacyRef();
  await snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "write");
  await startWorker();
  const privateRead = await fetch(`${repo}/info/refs?service=git-upload-pack`, {
    signal: AbortSignal.timeout(20_000),
  });
  assert.equal(privateRead.status, 401, "provisioning must fence the previous anonymous snapshot");
  await privateRead.arrayBuffer();
  await logged(/snapshot publication failed/);
  await operatorBucket(async (bucket) => {
    assert.equal(
      await bucket.get(Snapshot.keyOf("release-proof")),
      null,
      "publication failure must not resurrect the previous anonymous snapshot",
    );
  });
  const authorization = `Basic ${Buffer.from(`git:${member.credential}`).toString("base64")}`;
  const nativeNonce = async (method: string, url: string) => {
    const response = await fetch(url, { method, signal: AbortSignal.timeout(10_000) });
    assert.equal(response.status, 401);
    assert.equal(repoOf(response), member.repoId);
    const nonce = nonceOf(response);
    assert.ok(nonce, "the production DO must issue a native challenge");
    await response.arrayBuffer();
    return nonce;
  };
  const nativeHeader = (
    nonce: string,
    method: string,
    url: string,
    key: PrivateKey = member.member,
  ) =>
    Effect.runPromise(
      signEnvelope(key, {
        type: "auth.request",
        version: 1,
        repo: member.repoId,
        operation: operationOf(method, url),
        commands: [],
        nonce,
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      }),
    );
  const nativeReadUrl = `${repo}/refs`;
  const sharedNonce = await nativeNonce("GET", nativeReadUrl);
  const stranger = await Effect.runPromise(generate("ungranted-worker-fixture@example.com"));
  const ungranted = await fetch(nativeReadUrl, {
    headers: { authorization: await nativeHeader(sharedNonce, "GET", nativeReadUrl, stranger) },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(ungranted.status, 403, "a signature alone grants no membership");
  await ungranted.arrayBuffer();
  const signedRead = await nativeHeader(sharedNonce, "GET", nativeReadUrl);
  const wrongOperation = await fetch(`${repo}/whoami`, {
    headers: { authorization: signedRead },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(wrongOperation.status, 401, "a native envelope is bound to its operation");
  await wrongOperation.arrayBuffer();
  const nativeRead = await fetch(nativeReadUrl, {
    headers: { authorization: signedRead },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(nativeRead.status, 200, "invalid attempts must not consume the member's nonce");
  await nativeRead.arrayBuffer();
  const replayed = await fetch(nativeReadUrl, {
    headers: { authorization: signedRead },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(replayed.status, 401, "accepted native requests cannot be replayed");
  await replayed.arrayBuffer();
  const authedGit = (...args: string[]) =>
    execute(
      "git",
      [
        "-c",
        "credential.helper=",
        "-c",
        'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
        ...args,
      ],
      {
        cwd: scratch,
        timeout: 20_000,
        env: {
          ...process.env,
          GIT_AUTH_TOKEN: member.credential,
          GIT_CONFIG_GLOBAL: "/dev/null",
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_TERMINAL_PROMPT: "0",
        },
      },
    );
  await authedGit("-C", "client", "push", "origin", "HEAD:refs/heads/main");
  await authedGit("clone", "-q", repo, "private-client");
  assert.equal(
    (await git("-C", "private-client", "rev-parse", "HEAD")).stdout,
    (await git("-C", "client", "rev-parse", "HEAD")).stdout,
  );
  const lfsContent = "authenticated Worker LFS fixture";
  const lfsOid = createHash("sha256").update(lfsContent).digest("hex");
  const nativeUpload = await nativeHeader(
    await nativeNonce("PUT", `${lfs}/${lfsOid}`),
    "PUT",
    `${lfs}/${lfsOid}`,
  );
  const storedLfs = await fetch(`${lfs}/${lfsOid}`, {
    method: "PUT",
    body: lfsContent,
    headers: { authorization: nativeUpload },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(storedLfs.status, 200);
  await storedLfs.arrayBuffer();
  const downloaded = await fetch(`${lfs}/${lfsOid}`, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(downloaded.status, 200);
  assert.equal(await downloaded.text(), lfsContent);
  const deniedLfs = await fetch(`${lfs}/${lfsOid}`, { signal: AbortSignal.timeout(10_000) });
  assert.equal(deniedLfs.status, 401);
  await deniedLfs.arrayBuffer();
  const healthConfig = {
    url: repo,
    token: member.credential,
    ref: "refs/heads/main",
    oid: (await git("-C", "client", "rev-parse", "HEAD")).stdout.trim(),
    lfsOid,
  };
  assert.equal(
    (await probeHost(healthConfig)).healthy,
    true,
    "private canary probe must verify Git and LFS",
  );

  // Retaining the original operator artifact across a full process restart
  // must not replay initial refs over the branch created after provisioning.
  const beforeRestart = await nativeHeader(
    await nativeNonce("GET", nativeReadUrl),
    "GET",
    nativeReadUrl,
  );
  await stopWorker();
  await startWorker();
  const staleNonce = await fetch(nativeReadUrl, {
    headers: { authorization: beforeRestart },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(
    staleNonce.status,
    401,
    "a nonce from the previous DO instance must not be accepted",
  );
  await staleNonce.arrayBuffer();
  const freshRead = await fetch(nativeReadUrl, {
    headers: {
      authorization: await nativeHeader(
        await nativeNonce("GET", nativeReadUrl),
        "GET",
        nativeReadUrl,
      ),
    },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(freshRead.status, 200);
  await freshRead.arrayBuffer();
  const restarted = await authedGit("ls-remote", repo, "refs/heads/main");
  assert.equal(
    restarted.stdout.split("\t")[0],
    (await git("-C", "client", "rev-parse", "HEAD")).stdout.trim(),
  );
  const restartDenied = await fetch(`${repo}/refs`, { signal: AbortSignal.timeout(10_000) });
  assert.equal(restartDenied.status, 401);
  await restartDenied.arrayBuffer();
  const reopenedLfs = await fetch(`${lfs}/${lfsOid}`, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(reopenedLfs.status, 200);
  assert.equal(await reopenedLfs.text(), lfsContent);
  await verifyAdmission(repo, authorization);
  await verifyDeadline(repo, authorization);
  await verifyLfsDeadline(lfs, authorization);
  await verifyMutationDeadlines(repo, authorization);
  await stopWorker();
  uploadIdleMs = "1000";
  await startWorker();
  await verifyMutationDeadlines(repo, authorization, 408);
  await stopWorker();
  uploadIdleMs = "30000";
  clientRate = "1";
  await startWorker();
  for (const expected of [200, 429]) {
    const response = await fetch(`${repo}/refs`, {
      headers: { authorization },
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(
      response.status,
      expected,
      "native client rate binding must enforce its configured budget",
    );
    if (expected === 429) {
      assert.equal(response.headers.get("retry-after"), "60");
      assert.equal(response.headers.get("cache-control"), "no-store");
    }
    await response.arrayBuffer();
  }
  await stopWorker();
  clientRate = "100000";
  retainedLfsBytes = "10";
  retainedGitBytes = "1";
  await startWorker();
  const overQuota = await fetch(`${lfs}/${lfsOid}`, {
    method: "PUT",
    headers: { authorization },
    body: lfsContent,
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(
    overQuota.status,
    507,
    "lowered retained quota must refuse writes against existing data",
  );
  await overQuota.arrayBuffer();
  const quotaPreserved = await fetch(`${lfs}/${lfsOid}`, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(quotaPreserved.status, 200);
  assert.equal(await quotaPreserved.text(), lfsContent);
  const quotaTip = (await git("-C", "private-client", "rev-parse", "HEAD")).stdout.trim();
  await git(
    "-C",
    "private-client",
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.com",
    "commit",
    "--allow-empty",
    "-qm",
    "retained Git quota proof",
  );
  await assert.rejects(
    authedGit("-C", "private-client", "push", "origin", "HEAD:refs/heads/quota-proof"),
    /unpack|500|remote rejected/,
  );
  await logged(/GitStorage/);
  await logged(/quota_exceeded/);
  assert.ok(!output.includes(member.credential), "quota diagnostics must not contain credentials");
  assert.equal((await authedGit("ls-remote", repo, "refs/heads/quota-proof")).stdout.trim(), "");
  assert.equal(
    (await authedGit("ls-remote", repo, "refs/heads/main")).stdout.trim().split("\t")[0],
    quotaTip,
  );
  await stopWorker();
  retainedLfsBytes = "10737418240";
  retainedGitBytes = "10737418240";
  refMetadataRows = "1";
  await startWorker();
  await assert.rejects(
    authedGit("-C", "private-client", "push", "origin", "HEAD:refs/heads/quota-proof"),
    /unpack|500|remote rejected/,
  );
  assert.equal((await authedGit("ls-remote", repo, "refs/heads/quota-proof")).stdout.trim(), "");
  await logged(/metadata_rows/);
  await stopWorker();
  refMetadataRows = "100000";
  doCapacity = "2";
  allowAnonymousWrites = "true";
  await startWorker();
  await authedGit("-C", "private-client", "push", "origin", "HEAD:refs/heads/quota-proof");
  await authedGit("-C", "private-client", "push", "origin", ":refs/heads/quota-proof");
  await verifyAdmission(repo, authorization, 2);
  const openConfigRepo = `${origin}/open-config-proof`;
  await git("-C", "client", "push", openConfigRepo, "HEAD:refs/heads/main");
  const stillPrivate = await fetch(`${repo}/refs`, { signal: AbortSignal.timeout(10_000) });
  assert.equal(
    stillPrivate.status,
    401,
    "open-write opt-in must not weaken repositories with genesis",
  );
  await stillPrivate.arrayBuffer();
  await stopWorker();
  doCapacity = "8";
  allowAnonymousWrites = "false";
  await clearStorageFault();
  await startWorker();
  await assert.rejects(
    git("-C", "client", "push", openConfigRepo, "HEAD:refs/heads/after-close"),
    /403/,
  );
  // A refused mutation still refreshes the derived view after storage recovers.
  const refresh = await fetch(`${repo}/commit`, {
    method: "POST",
    body: "{}",
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(refresh.status, 401);
  await refresh.arrayBuffer();
  await operatorBucket(async (bucket) => {
    const recovered = await bucket.get(Snapshot.keyOf("release-proof"));
    assert.ok(recovered);
    assert.equal(
      Snapshot.decode(new Uint8Array(await recovered.arrayBuffer()))?.anonymousRead,
      false,
    );
  });
  const malformedSecret = "fixture-private-provisioning-payload";
  await operatorBucket(async (bucket) => {
    await bucket.delete(Provisioning.keyOf("release-proof"));
    await bucket.put(Provisioning.keyOf("invalid-proof"), JSON.stringify({ malformedSecret }));
  });
  const noArtifact = await fetch(`${repo}/info/refs?service=git-upload-pack`, {
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(
    noArtifact.status,
    401,
    "removing the completed artifact must not reopen anonymous snapshots",
  );
  await noArtifact.arrayBuffer();
  await stopWorker();
  const useSource = await sourceVersions(scratch, installation, port);
  for (const phase of ["baseline", "candidate", "rollback"] as const) {
    // The baseline wrote R2 without maintaining the candidate's SQLite ledger.
    // Every runtime/proxy is stopped here; candidate initialization rescans R2.
    if (phase === "candidate") await reconcileGitCapacity(localStorage, "release-proof");
    await useSource(phase === "candidate" ? "candidate" : "baseline");
    await startWorker();
    // Distinguish the running versions through real behavior: the archived
    // baseline predates operator provisioning and ignores this malformed artifact.
    const versionBoundary = await fetch(`${origin}/invalid-proof/refs`, {
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(
      versionBoundary.status,
      phase === "candidate" ? 503 : 200,
      `${phase}: actual source version must control provisioning behavior`,
    );
    await versionBoundary.arrayBuffer();
    assert.equal(
      (await probeHost(healthConfig)).healthy,
      true,
      `${phase}: retained private Git/LFS`,
    );
    if (phase !== "rollback") {
      const file = `${phase}-upgrade.txt`;
      await fs.writeFile(path.join(scratch, "client", file), `${phase} source wrote this commit\n`);
      await git("-C", "client", "add", file);
      await git(
        "-C",
        "client",
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        `${phase} data`,
      );
      await authedGit("-C", "client", "push", "origin", "HEAD:refs/heads/main");
      healthConfig.oid = (await git("-C", "client", "rev-parse", "HEAD")).stdout.trim();
      assert.equal((await probeHost(healthConfig)).healthy, true, `${phase}: accepted new history`);
    }
    const checkout = `${phase}-upgrade-client`;
    await authedGit("clone", "-q", repo, checkout);
    await git("-C", checkout, "fsck", "--strict");
    assert.equal((await git("-C", checkout, "rev-parse", "HEAD")).stdout.trim(), healthConfig.oid);
    if (phase === "rollback") {
      for (const version of ["baseline", "candidate"])
        assert.equal(
          await fs.readFile(path.join(scratch, checkout, `${version}-upgrade.txt`), "utf8"),
          `${version} source wrote this commit\n`,
        );
    }
    await stopWorker();
  }
  await useSource("candidate");
  await startWorker();
  assert.equal(
    (await probeHost(healthConfig)).healthy,
    true,
    "candidate resumes after source rollback",
  );
  await authedGit("-C", identity, "fetch", repo, "refs/heads/main:refs/heads/main");
  const secretComment = `redaction-fixture-${crypto.randomUUID()}`;
  const redaction = await Effect.runPromise(
    Effect.gen(function* () {
      const repository = yield* Repository.Repository;
      const head = yield* repository.resolve("refs/heads/main");
      assert.ok(head);
      const { pr } = yield* PullRequest.open({
        repo: member.repoId,
        title: "Worker redaction rehearsal",
        base: "refs/heads/main",
        head,
        key: member.member,
      });
      const commit = yield* PullRequest.comment({
        repo: member.repoId,
        pr,
        body: secretComment,
        key: member.member,
      });
      const { events } = yield* HubEvent.entries(pr);
      const target = events.find((event) => event.commit === commit)?.payload?.id;
      assert.ok(target);
      const info = yield* repository.readCommit(commit);
      const blob = yield* repository.findPath(info.tree, `${HubEvent.RECORD}.json`);
      assert.ok(blob);
      return { pr, target, blob: blob.oid };
    }).pipe(
      Effect.provide(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(identity))),
      ),
    ),
  );
  const hubRef = HubEvent.refOf(redaction.pr);
  const pushHub = () => authedGit("-C", identity, "push", repo, `${hubRef}:${hubRef}`);
  await pushHub();
  const readPull = async () => {
    const response = await fetch(`${repo}/hub/pulls/${redaction.pr}`, {
      headers: { authorization },
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 200);
    return response.text();
  };
  assert.ok(
    (await readPull()).includes(secretComment),
    "fixture comment must be visible before redaction",
  );
  const redactedTip = await Effect.runPromise(
    PullRequest.redact({
      repo: member.repoId,
      pr: redaction.pr,
      target: redaction.target,
      reason: "sensitive-content",
      key: member.member,
    }).pipe(
      Effect.provide(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(identity))),
      ),
    ),
  );
  await stopWorker();
  await snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "delete");
  await startWorker();
  await assert.rejects(
    pushHub,
    "failed invalidation must refuse redaction before changing history",
  );
  assert.ok((await readPull()).includes(secretComment));
  await stopWorker();
  await snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "write");
  await startWorker();
  await pushHub();
  assert.equal((await readPull()).includes(secretComment), false);
  const gcUrl = `${repo}/gc`;
  const collected = await fetch(gcUrl, {
    method: "POST",
    headers: {
      authorization: await nativeHeader(await nativeNonce("POST", gcUrl), "POST", gcUrl),
      "content-type": "application/json",
    },
    body: JSON.stringify({ repack: true }),
    signal: AbortSignal.timeout(20_000),
  });
  assert.equal(
    collected.status,
    200,
    "authenticated collection must finish the remote payload removal",
  );
  await collected.arrayBuffer();
  const verifyRedacted = async () => {
    assert.equal((await readPull()).includes(secretComment), false);
    const checkout = `redacted-${crypto.randomUUID()}`;
    await authedGit("init", "-q", checkout);
    await Effect.runPromise(
      Effect.gen(function* () {
        return yield* fetchRepository({
          url: repo,
          token: member.credential,
          stores: { objects: yield* ObjectStore, refs: yield* RefStore },
          refspecs: [yield* Effect.fromResult(Refspec.parse(`${hubRef}:${hubRef}`))],
        });
      }).pipe(Effect.provide(stores(path.join(scratch, checkout, ".git")))),
    );
    assert.equal((await git("-C", checkout, "rev-parse", hubRef)).stdout.trim(), redactedTip);
    await git("-C", checkout, "cat-file", "-e", redactedTip);
    await assert.rejects(
      () => git("-C", checkout, "cat-file", "-e", redaction.blob),
      "fresh hub-aware fetch must omit the redacted payload blob",
    );
  };
  await verifyRedacted();
  await operatorBucket(async (bucket) => {
    assert.equal(await bucket.get(Snapshot.keyOf("release-proof")), null);
  });
  await stopWorker();
  await startWorker();
  await verifyRedacted();
  await stopWorker();
  await clearStorageFault();
  await startWorker();
  // Refresh the derived snapshot without changing the accepted hub history.
  const redactedRefresh = await fetch(`${repo}/commit`, {
    method: "POST",
    body: "{}",
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(redactedRefresh.status, 401);
  await redactedRefresh.arrayBuffer();
  await operatorBucket(async (bucket) => {
    const recovered = await bucket.get(Snapshot.keyOf("release-proof"));
    assert.ok(recovered);
    const view = Snapshot.decode(new Uint8Array(await recovered.arrayBuffer()));
    assert.equal(view?.refs.find((ref) => ref.name === hubRef)?.oid, redactedTip);
  });
  await verifyRedacted();
  // Quiesce the isolated installation and restore its entire local R2/SQLite
  // and deployment state into newly created directories before revocation.
  const registrySecret = "restore-fixture-private-registry-secret";
  const registration = async (route: string, body: string) => {
    const response = await fetch(`${repo}/${route}`, {
      method: "POST",
      headers: { authorization, "content-type": "application/json" },
      body,
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 200);
    const text = await response.text();
    assert.equal(text.includes(registrySecret), false);
    return text;
  };
  const savedRemote = Schema.decodeSync(Schema.fromJsonString(RemoteWire))(
    await registration(
      "remotes",
      JSON.stringify({
        name: "restore-proof",
        url: "https://restore.invalid/repository",
        credential: registrySecret,
      }),
    ),
  );
  const savedWebhook = Schema.decodeSync(Schema.fromJsonString(WebhookWire))(
    await registration(
      "webhooks",
      JSON.stringify({
        url: "https://restore.invalid/hook",
        secret: registrySecret,
      }),
    ),
  );
  assert.equal(output.includes(registrySecret), false);
  await stopWorker();
  registryRows = "1";
  await startWorker();
  for (const [route, body] of [
    [
      "remotes",
      { name: "quota-proof", url: "https://restore.invalid/other", credential: registrySecret },
    ],
    ["webhooks", { url: "https://restore.invalid/other", secret: registrySecret }],
  ] as const) {
    const refused = await fetch(`${repo}/${route}`, {
      method: "POST",
      headers: { authorization, "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(refused.status, 500, "lowered SQL registry quota must refuse additions");
    assert.ok(!(await refused.text()).includes(registrySecret));
  }
  await logged(/Administrative registry capacity exhausted/);
  assert.ok(!output.includes(registrySecret));
  await stopWorker();
  await restoreLocalState(alchemyRoot, path.join(scratch, "backup"));
  await startWorker();
  for (const [route, expected] of [
    ["remotes", { remotes: [savedRemote] }],
    ["webhooks", { webhooks: [savedWebhook] }],
  ] as const) {
    const response = await fetch(`${repo}/${route}`, {
      headers: { authorization },
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), expected);
  }
  // Remove the restored subscriptions before any push; no external delivery is
  // part of this offline recovery rehearsal. The remote is manual throughout.
  for (const route of [`webhooks/${savedWebhook.id}`, "remotes/restore-proof"]) {
    const response = await fetch(`${repo}/${route}`, {
      method: "DELETE",
      headers: { authorization },
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 200);
    await response.arrayBuffer();
  }
  assert.equal(
    (await probeHost(healthConfig)).healthy,
    true,
    "restored Git/LFS and private authority",
  );
  assert.equal(output.includes(registrySecret), false);
  await authedGit("clone", "-q", repo, "restored-private-client");
  assert.equal(
    await fs.readFile(path.join(scratch, "restored-private-client", "legacy.txt"), "utf8"),
    legacyContent,
  );
  await git("-C", "restored-private-client", "fsck", "--strict");
  await verifyRedacted();
  // Revoke the only reader on the now-populated repository through ordinary
  // authenticated Git transport. The original provisioning artifact is gone.
  const revokedSubject = await Effect.runPromise(fingerprint(member.member.publicKey));
  const revokedLog = await Effect.runPromise(
    Log.issue(
      Certificate.revoke({
        repo: member.repoId,
        subject: revokedSubject,
        reason: "left",
        id: Log.newId(),
      }),
      [member.root],
    ).pipe(
      Effect.provide(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(identity))),
      ),
    ),
  );
  const pushRevocation = () =>
    authedGit("-C", identity, "push", repo, `${Log.LOG_REF}:${Log.LOG_REF}`);
  await stopWorker();
  await snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "delete");
  await startWorker();
  await assert.rejects(pushRevocation, "snapshot deletion failure must refuse the trust mutation");
  const stillGranted = await fetch(nativeReadUrl, {
    headers: { authorization },
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(stillGranted.status, 200, "refused revocation must preserve the prior membership");
  await stillGranted.arrayBuffer();
  await logged(/snapshot invalidation failed/);
  await stopWorker();
  await snapshotFault(localStorage, sentinel, Snapshot.keyOf("release-proof"), "write");
  await startWorker();
  await pushRevocation();
  await logged(/snapshot publication failed/);
  await operatorBucket(async (bucket) => {
    assert.equal(await bucket.get(Snapshot.keyOf("release-proof")), null);
  });
  const verifyRevoked = async () => {
    for (const url of [
      nativeReadUrl,
      `${repo}/info/refs?service=git-upload-pack`,
      `${lfs}/${lfsOid}`,
    ]) {
      const denied = await fetch(url, {
        headers: { authorization },
        signal: AbortSignal.timeout(10_000),
      });
      assert.equal(denied.status, 403, "the revoked reader's existing credential must be refused");
      await denied.arrayBuffer();
      const anonymous = await fetch(url, { signal: AbortSignal.timeout(10_000) });
      assert.equal(
        anonymous.status,
        401,
        "revoking the last reader must not make stored content public",
      );
      await anonymous.arrayBuffer();
    }
    await assert.rejects(() =>
      authedGit("clone", "-q", repo, `revoked-client-${crypto.randomUUID()}`),
    );
  };
  await verifyRevoked();
  await stopWorker();
  await startWorker();
  await verifyRevoked();
  await stopWorker();
  await clearStorageFault();
  await startWorker();
  const revokedRefresh = await fetch(`${repo}/commit`, {
    method: "POST",
    body: "{}",
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(revokedRefresh.status, 401);
  await revokedRefresh.arrayBuffer();
  await operatorBucket(async (bucket) => {
    const snapshot = await bucket.get(Snapshot.keyOf("release-proof"));
    assert.ok(snapshot);
    const view = Snapshot.decode(new Uint8Array(await snapshot.arrayBuffer()));
    assert.ok(view);
    assert.equal(view.anonymousRead, false);
    assert.equal(
      view.refs.find((ref) => ref.name === Log.LOG_REF)?.oid,
      revokedLog,
      "recovered snapshot must contain the accepted trust frontier",
    );
  });
  await verifyRevoked();
  assert.equal(
    (await probeHost(healthConfig)).healthy,
    false,
    "revoked probe credentials must fail health verification",
  );
  const invalidProvisioning = await fetch(`${origin}/invalid-proof/refs`, {
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(invalidProvisioning.status, 503);
  assert.equal(await invalidProvisioning.text(), "repository provisioning unavailable");
  assert.equal(output.includes(malformedSecret), false);
  await operatorBucket(async (bucket) => {
    await bucket.delete(Provisioning.keyOf("invalid-proof"));
  });
  const repaired = await fetch(`${origin}/invalid-proof/refs`, {
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(repaired.status, 200, "invalid input must not leave a partially installed identity");
  assert.deepEqual(await repaired.json(), { refs: [], head: "refs/heads/main" });
  browser = await chromium.launch();
  const page = await browser.newPage();
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(`${origin}/hub/tasks`, { waitUntil: "networkidle" });
  await page.locator(".gp-sidebar").waitFor({ state: "visible", timeout: 10_000 });
  assert.deepEqual(errors, []);
  await logged(/snapshot publication completed/, "actual writer must report publication timing");
  await logged(/response_ready/, "actual runtime must emit response telemetry");
  await logged(/response_body_complete/, "actual runtime must observe consumed bodies");
  await logged(/CloudflareEdge/, "edge observations must be distinguishable");
  await logged(/CloudflareHost/, "DO observations must be distinguishable");
  assert.doesNotMatch(
    output,
    new RegExp(malformedSecret),
    "telemetry must not echo provisioning payloads",
  );
  const bundleReport = await measureWorkerBundle(
    path.join(alchemyRoot, "bundles", `git-${stage}-git`),
  );
  console.info(
    `Local Worker modules: ${bundleReport.bytes} bytes, ${bundleReport.gzipBytes} gzip bytes.`,
  );
  if (bundleReportPath !== undefined)
    await fs.writeFile(bundleReportPath, JSON.stringify(bundleReport, null, 2) + "\n", {
      flag: "wx",
    });
  console.info(
    `Actual Worker/DO local verification passed (${stage}, ${origin}): UI/CSP, default-deny writes, LFS budgets, edge/DO admission and recovery, operator provisioning/restart, local R2/SQLite/deployment-state restore, snapshot delete/write failures and recovery, authenticated Git/LFS, native challenges/replay, no conformance routes.`,
  );
} finally {
  try {
    await browser?.close();
  } finally {
    try {
      await stopWorker();
      await clearStorageFault?.();
    } finally {
      await fs.rm(scratch, { recursive: true, force: true });
    }
  }
}
