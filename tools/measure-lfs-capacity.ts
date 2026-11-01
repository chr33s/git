/** Measure actual Node LFS inventory scans; never opens an operator repository. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { ConfigProvider, Effect, Layer, Stream } from "effect";
import { LfsStore } from "../src/server/Lfs.ts";
import { file } from "../src/server/Lfs.node.ts";
import { configuration } from "../src/server/LfsCapacity.ts";

const [output, roundsText = "3", ...extra] = process.argv.slice(2);
assert.ok(
  output !== undefined && extra.length === 0,
  "expected new report path and optional rounds",
);
const rounds = Number(roundsText);
assert.ok(Number.isSafeInteger(rounds) && rounds >= 1 && rounds <= 20, "rounds must be 1–20");
const report = await fs.open(output, "wx", 0o600);
let cleanupRoot: string | undefined;
let complete = false;
try {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-lfs-capacity-"));
  cleanupRoot = root;
  const defaults = ConfigProvider.layer(ConfigProvider.fromUnknown({}));
  const limits = await Effect.runPromise(configuration().pipe(Effect.provide(defaults)));
  const storage = file(root).pipe(Layer.provide(defaults));
  const locate = (oid: string) => path.join(root, oid.slice(0, 2), oid.slice(2, 4), oid);
  const hash = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
  const candidate = Buffer.alloc(1024 * 1024, 42);
  const oid = hash(candidate);
  const upload = Effect.gen(function* () {
    const store = yield* LfsStore;
    const result = yield* store.write(
      oid,
      Stream.fromIterable(
        Array.from({ length: 128 }, (_, index) =>
          candidate.subarray(index * 8192, (index + 1) * 8192),
        ),
      ),
    );
    assert.equal(result.size, candidate.length);
    const content = yield* store.read(oid);
    const digest = createHash("sha256");
    yield* Stream.runForEach(content, (bytes) => Effect.sync(() => digest.update(bytes)));
    assert.equal(digest.digest("hex"), oid);
  }).pipe(Effect.provide(storage));
  const measurements = [];
  let seeded = 0;
  let payloadBytes = 0;
  for (const count of [1000, 10_000, limits.objects - 1]) {
    const seedStarted = performance.now();
    while (seeded < count) {
      const end = Math.min(count, seeded + 32);
      const batch = [];
      for (; seeded < end; seeded++) {
        const content = Buffer.from(`retained LFS fixture ${seeded}\n`);
        payloadBytes += content.length;
        const target = locate(hash(content));
        batch.push(
          (async () => {
            await fs.mkdir(path.dirname(target), { recursive: true });
            await fs.writeFile(target, content, { flag: "wx", mode: 0o600 });
          })(),
        );
      }
      // Let every writer settle before cleanup even if one fails.
      const results = await Promise.allSettled(batch);
      for (const result of results) if (result.status === "rejected") throw result.reason;
    }
    const seedMs = performance.now() - seedStarted;
    const durationsMs = [];
    const cpu = process.cpuUsage();
    const rssBefore = process.memoryUsage().rss;
    let sampledPeakRssBytes = rssBefore;
    const timer = setInterval(() => {
      sampledPeakRssBytes = Math.max(sampledPeakRssBytes, process.memoryUsage().rss);
    }, 20);
    const lag = monitorEventLoopDelay({ resolution: 20 });
    lag.enable();
    try {
      for (let round = 0; round < rounds; round++) {
        const started = performance.now();
        await Effect.runPromise(upload);
        durationsMs.push(performance.now() - started);
        await fs.unlink(locate(oid));
        await assert.rejects(fs.stat(path.join(root, ".quota.lock")), { code: "ENOENT" });
      }
    } finally {
      clearInterval(timer);
      lag.disable();
    }
    const used = process.cpuUsage(cpu);
    measurements.push({
      retainedObjects: count,
      retainedPayloadBytes: payloadBytes,
      seedMs,
      durationsMs,
      rssBeforeBytes: rssBefore,
      sampledPeakRssBytes,
      rssAfterBytes: process.memoryUsage().rss,
      userCpuMicroseconds: used.user,
      systemCpuMicroseconds: used.system,
      eventLoopDelayMaxMs: lag.max / 1e6,
      eventLoopDelayP99Ms: lag.percentile(99) / 1e6,
    });
    console.info(`Verified ${rounds} uploads at ${count} retained objects.`);
  }
  // The next upload would cross the default object ceiling, even though bytes fit.
  await Effect.runPromise(upload);
  const refused = await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* LfsStore;
      return yield* store.write(hash(Buffer.from("excess")), Stream.succeed(Buffer.from("excess")));
    }).pipe(Effect.provide(storage), Effect.result),
  );
  assert.equal(refused._tag, "Failure");
  if (refused._tag === "Failure") assert.equal(refused.failure._tag, "LfsQuotaExceeded");
  await assert.rejects(fs.stat(path.join(root, ".quota.lock")), { code: "ENOENT" });
  const filesystem = await fs.statfs(root);
  await report.writeFile(
    JSON.stringify(
      {
        schemaVersion: 1,
        measuredAt: new Date().toISOString(),
        node: process.version,
        platform: process.platform,
        arch: process.arch,
        kernel: os.release(),
        cpu: os.cpus()[0]?.model,
        revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
        dirty: execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).length > 0,
        filesystemType: filesystem.type,
        limits,
        uploadBytes: candidate.length,
        rounds,
        measurements,
        quotaRefusalVerified: true,
        uploadDigestVerified: true,
        quotaLocksCleaned: true,
        limitsOfEvidence:
          "Local source backend, warm filesystem cache, tiny retained objects. RSS includes fixture seeding; upload timings include scan, durable publication and streamed digest verification. Not HTTP concurrency, R2, cold-cache or production-volume qualification.",
      },
      null,
      2,
    ) + "\n",
  );
  complete = true;
} finally {
  await report.close();
  if (cleanupRoot !== undefined) await fs.rm(cleanupRoot, { recursive: true, force: true });
  if (!complete) await fs.unlink(output);
}
