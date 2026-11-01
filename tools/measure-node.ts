/** Local capacity evidence, not a release qualification or a production load generator. */
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createCipheriv } from "node:crypto";
import { once } from "node:events";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { Effect, Schema } from "effect";
import { gitEnv, gitIn } from "../src/testing/Git.ts";
import { enableHubUnder } from "../src/testing/Hub.ts";
import { mintDelegation } from "../src/server/Auth.ts";

const [output, mibText = "16", concurrencyText = "2", commitsText = "64", roundsText = "1"] =
  process.argv.slice(2);
assert.ok(
  output,
  "usage: node tools/measure-node.ts <new-report.json> [MiB=16] [concurrency=2] [commits=64] [rounds=1]",
);
assert.ok(process.argv.length <= 7);
const mib = Number(mibText);
const concurrency = Number(concurrencyText);
const commits = Number(commitsText);
const rounds = Number(roundsText);
for (const [value, ceiling] of [
  [mib, 256],
  [concurrency, 16],
  [commits, 4096],
  [rounds, 100],
]) {
  assert.ok(value !== undefined && ceiling !== undefined);
  assert.ok(
    Number.isSafeInteger(value) && value > 0 && value <= ceiling,
    "workload argument out of range",
  );
}
// Reserve the output name before any expensive work. An incomplete run never
// leaves a report that could be mistaken for successful capacity evidence.
const reportFile = await fs.open(output, "wx", 0o600);
let cleanupRoot: string | undefined;
let host: ReturnType<typeof spawn> | undefined;
let closed: ReturnType<typeof once> | undefined;
let complete = false;
try {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-capacity-"));
  cleanupRoot = root;
  const source = path.join(root, "source");
  const data = path.join(root, "data");
  await fs.mkdir(source);
  await fs.mkdir(data);
  const git = gitIn(source);
  git("init", "-q", "-b", "main");
  const cipher = createCipheriv("aes-256-ctr", Buffer.alloc(32, 7), Buffer.alloc(16));
  for (let index = 0; index < mib; index++)
    await fs.writeFile(
      path.join(source, `data-${index}.bin`),
      cipher.update(Buffer.alloc(1024 * 1024)),
    );
  for (let index = 0; index < commits; index++) {
    await fs.writeFile(path.join(source, "sequence"), `${index}\n`);
    git("add", ".");
    git("commit", "-qm", `fixture ${index}`);
  }
  const expected = git("rev-parse", "HEAD").trim();
  const repository = path.join(data, "r");
  git("clone", "-q", "--bare", source, repository);
  const member = await enableHubUnder(data, "r", ["repo.read"]);
  gitIn(repository)("gc", "--prune=now");
  const objects = gitIn(repository)("count-objects", "-v");
  host = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
    import { Effect } from ${JSON.stringify(import.meta.resolve("effect"))};
    import { serve } from ${JSON.stringify(new URL("../src/host/Node.ts", import.meta.url).href)};
    import { configuration } from ${JSON.stringify(new URL("../src/host/Admission.node.ts", import.meta.url).href)};
    import { configuration as requestConfiguration } from ${JSON.stringify(new URL("../src/server/RequestLimits.ts", import.meta.url).href)};
    const limits = await Effect.runPromise(configuration());
    const { fetchGraphSteps } = await Effect.runPromise(requestConfiguration());
    const server = await serve({ root: process.argv[1] });
    const baselineRssBytes = process.memoryUsage().rss;
    const cpu = process.cpuUsage();
    console.log("READY " + server.url);
    process.stdin.once("data", async () => {
      await server.close({ force: true });
      const used = process.cpuUsage(cpu);
      console.log("RESULT " + JSON.stringify({ baselineRssBytes, maxRssKiB: process.resourceUsage().maxRSS,
        userCpuMicroseconds: used.user, systemCpuMicroseconds: used.system, limits, fetchGraphSteps }));
      process.stdin.destroy();
    });
  `,
      data,
    ],
    { env: { ...gitEnv, GIT_HTTP_METRICS_INTERVAL_MS: "0" }, stdio: ["pipe", "pipe", "pipe"] },
  );
  closed = once(host, "close");
  let logs = "";
  const child = host;
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("benchmark host startup timed out")), 30_000);
    const read = (bytes: Buffer) => {
      logs = (logs + bytes.toString()).slice(-128 * 1024);
      const match = /READY (http:\/\/[^\s]+)/.exec(logs);
      if (match?.[1] !== undefined) {
        clearTimeout(timer);
        resolve(match[1]);
      }
    };
    child.stdout?.on("data", read);
    child.stderr?.on("data", read);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`benchmark host exited ${code}`));
    });
  });
  const execute = promisify(execFile);
  const credential = () =>
    Effect.runPromise(
      mintDelegation({
        key: member.member,
        repo: member.repoId,
        capabilities: ["repo.read"],
        ttlSeconds: 300,
      }),
    );
  const clone = (name: string, token: string) =>
    execute(
      "git",
      [
        "-c",
        "credential.helper=",
        "-c",
        'credential.helper=!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
        "clone",
        "-q",
        "--bare",
        `${url}/r`,
        name,
      ],
      {
        cwd: root,
        env: { ...gitEnv, GIT_AUTH_TOKEN: token, GIT_TERMINAL_PROMPT: "0" },
        timeout: 120_000,
      },
    );
  const measurements = [];
  const commandFailure = Schema.Struct({ stderr: Schema.String });
  const rehearsalStart = performance.now();
  for (let round = 1; round <= rounds; round++) {
    // A multi-round measurement can outlive the fixture's five-minute token.
    // Renew outside timing; keep normal expiry and authorization enforcement.
    const token = await credential();
    const start = performance.now();
    const requests = await Promise.all(
      Array.from({ length: concurrency }, async (_, index) => {
        const began = performance.now();
        const directory = `client-${round}-${index}`;
        try {
          await clone(directory, token);
          return {
            round,
            milliseconds: performance.now() - began,
            transferred: true,
            verified: false,
            failure: null,
          };
        } catch (cause) {
          return {
            round,
            milliseconds: performance.now() - began,
            transferred: false,
            verified: false,
            failure:
              Schema.is(commandFailure)(cause) &&
              /requested URL returned error: 503|HTTP 503/.test(cause.stderr)
                ? "http-503"
                : "transfer-failure",
          };
        }
      }),
    );
    const elapsedMs = performance.now() - start;
    // Verification happens after the timed burst: synchronous Git checks must
    // not delay another clone's completion callback and inflate its latency.
    for (const [index, request] of requests.entries()) {
      if (!request.transferred) continue;
      const result = gitIn(path.join(root, `client-${round}-${index}`));
      assert.equal(result("rev-parse", "HEAD").trim(), expected);
      result("fsck", "--strict");
      request.verified = true;
    }
    const recovery = `recovery-${round}`;
    await clone(recovery, await credential());
    const recovered = gitIn(path.join(root, recovery));
    assert.equal(recovered("rev-parse", "HEAD").trim(), expected);
    recovered("fsck", "--strict");
    assert.ok(child.pid);
    const rssKiB = Number(
      (await execute("ps", ["-o", "rss=", "-p", String(child.pid)])).stdout.trim(),
    );
    assert.ok(Number.isFinite(rssKiB) && rssKiB > 0, "host RSS sample unavailable");
    measurements.push({ round, elapsedMs, rssKiB, requests, recoveryCloneVerified: true });
    // Bound disk use across rounds. Cleanup and verification remain outside burst timing.
    await Promise.all(
      [
        ...Array.from({ length: concurrency }, (_, index) => `client-${round}-${index}`),
        recovery,
      ].map((name) => fs.rm(path.join(root, name), { recursive: true, force: true })),
    );
    console.log(`Completed capacity round ${round}/${rounds}`);
  }
  const rehearsalElapsedMs = performance.now() - rehearsalStart;
  child.stdin?.end("report\n");
  const force = setTimeout(() => child.kill("SIGKILL"), 15_000);
  try {
    const [code] = await closed;
    assert.equal(code, 0);
  } finally {
    clearTimeout(force);
  }
  const raw = /^RESULT (.+)$/m.exec(logs)?.[1];
  assert.ok(raw, "host did not return resource measurements");
  const metrics = Schema.decodeUnknownSync(
    Schema.Struct({
      baselineRssBytes: Schema.Finite,
      maxRssKiB: Schema.Finite,
      userCpuMicroseconds: Schema.Finite,
      systemCpuMicroseconds: Schema.Finite,
      fetchGraphSteps: Schema.Finite,
      limits: Schema.Struct({
        active: Schema.Finite,
        perRepository: Schema.Finite,
        connections: Schema.Finite,
        deadlineMs: Schema.Finite,
        idleMs: Schema.Finite,
        headersMs: Schema.Finite,
      }),
    }),
  )(JSON.parse(raw));
  const revision = (await execute("git", ["rev-parse", "HEAD"])).stdout.trim();
  const dirty = (await execute("git", ["status", "--porcelain"])).stdout.length > 0;
  await reportFile.writeFile(
    JSON.stringify(
      {
        schemaVersion: 2,
        measuredAt: new Date().toISOString(),
        revision,
        dirty,
        platform: process.platform,
        arch: process.arch,
        osRelease: os.release(),
        node: process.version,
        git: (await execute("git", ["--version"])).stdout.trim(),
        cpu: os.cpus()[0]?.model,
        logicalCpus: os.availableParallelism(),
        totalMemoryBytes: os.totalmem(),
        workload: {
          dataMiB: mib,
          commits,
          concurrency,
          rounds,
          credentialsRenewed: true,
          packed: true,
          private: true,
          objects,
        },
        rounds: measurements,
        rehearsalElapsedMs,
        recoveryCloneVerified: true,
        host: metrics,
        limitsOfEvidence:
          "Repeated loopback bursts with verification, recovery clone and cleanup between rounds; shared OS file cache. RSS samples are post-recovery, not per-round peaks; aggregate host metrics exclude Git clients and include verification gaps/recovery. Not continuous saturation or a supported-size claim.",
      },
      null,
      2,
    ) + "\n",
  );
  complete = true;
  console.log(`Recorded capacity rehearsal: ${path.resolve(output)}`);
} finally {
  if (host?.exitCode === null && host.signalCode === null) host.kill("SIGKILL");
  await closed;
  await reportFile.close();
  if (!complete) await fs.rm(output, { force: true });
  if (cleanupRoot !== undefined) await fs.rm(cleanupRoot, { recursive: true, force: true });
}
