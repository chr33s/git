import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";

const content = "LFS ancestor sync fixture\n";
const oid = createHash("sha256").update(content).digest("hex");
const child = `
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { Effect, Stream } from "effect";
import { LfsStore } from ${JSON.stringify(new URL("./Lfs.ts", import.meta.url).href)};
import { file } from ${JSON.stringify(new URL("./Lfs.node.ts", import.meta.url).href)};
const [root, ancestor, fault] = process.argv.slice(1);
const open = fs.open;
fs.open = async function (...args) {
  const handle = await Reflect.apply(open, fs, args);
  if (fault === "sync") {
    const stat = await handle.stat();
    const expected = await fs.stat(ancestor);
    if (stat.isDirectory() && stat.dev === expected.dev && stat.ino === expected.ino) {
      handle.sync = async () => { throw Object.assign(new Error("fixture ancestor sync failure"), { code: "EIO" }); };
    }
  }
  return handle;
};
syncBuiltinESMExports();
console.log(await Effect.runPromise(LfsStore.use(store => store.write(${JSON.stringify(oid)}, Stream.make(new TextEncoder().encode(${JSON.stringify(content)})))).pipe(
  Effect.provide(file(root)),
  Effect.match({ onSuccess: () => "written", onFailure: error => error._tag + ":" + error.operation }),
)));
`;

it("LFS retries do not skip a failed ancestor sync after reopening the store", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-ancestor-"));
  const ancestor = path.join(temporary, "new");
  const root = path.join(ancestor, "outer", "lfs");
  const target = path.join(root, oid.slice(0, 2), oid.slice(2, 4), oid);
  const run = async (fault: string) =>
    (
      await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", child, root, ancestor, fault],
        { timeout: 20_000 },
      )
    ).stdout.trim();
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      assert.equal(await run("sync"), "StorageFailure:lfs.write");
      assert.equal(
        await fs.readFile(target, "utf8"),
        content,
        "post-rename failure can leave complete bytes visible",
      );
      assert.deepEqual(await fs.readdir(path.dirname(target)), [oid]);
    }
    assert.equal(await run("healthy"), "written");
    assert.equal(await fs.readFile(target, "utf8"), content);
    assert.equal((await fs.stat(target)).mode & 0o777, 0o600);
  } finally {
    await fs.rm(temporary, { recursive: true, force: true });
  }
});

const crashWriter = `
import fs from "node:fs/promises";
import { writeSync } from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { syncBuiltinESMExports } from "node:module";
import { Effect, Stream } from "effect";
import { LfsStore } from ${JSON.stringify(new URL("./Lfs.ts", import.meta.url).href)};
import { file } from ${JSON.stringify(new URL("./Lfs.node.ts", import.meta.url).href)};
const [root, oid, content, fault] = process.argv.slice(1);
const target = path.join(root, oid.slice(0, 2), oid.slice(2, 4), oid);
const kill = () => process.kill(process.pid, "SIGKILL");
const rename = fs.rename;
fs.rename = async (from, to) => {
  if (to === target && fault === "before-rename") kill();
  await rename(from, to);
  if (to === target && fault === "after-rename") kill();
};
syncBuiltinESMExports();
const bytes = new TextEncoder().encode(content);
const body = Stream.fromAsyncIterable((async function* () {
  yield bytes.subarray(0, 4);
  if (fault === "partial-write") {
    // Wait for the actual native stream to put bytes on disk, without closing
    // the source or allowing the upload to finish and run its finalizers.
    for (;;) {
      const names = await fs.readdir(path.dirname(target)).catch(error => {
        if (error.code === "ENOENT") return [];
        throw error;
      });
      for (const name of names) {
        if (name.startsWith(oid + ".") && name.endsWith(".tmp") &&
            (await fs.stat(path.join(path.dirname(target), name))).size > 0) kill();
      }
      await delay(5);
    }
  }
  yield bytes.subarray(4);
})(), error => error);
await Effect.runPromise(LfsStore.use(store => store.write(oid, body)).pipe(Effect.provide(file(root))));
writeSync(1, "acknowledged\\n");
if (fault === "after-acknowledgement") kill();
`;

const reopenedReader = `
import { Effect, Stream } from "effect";
import { createHash } from "node:crypto";
import { LfsStore } from ${JSON.stringify(new URL("./Lfs.ts", import.meta.url).href)};
import { file } from ${JSON.stringify(new URL("./Lfs.node.ts", import.meta.url).href)};
const [root, oid] = process.argv.slice(1);
console.log(JSON.stringify(await Effect.runPromise(LfsStore.use(store => Effect.gen(function* () {
  const head = yield* store.head(oid);
  if (head === null) return null;
  const digest = createHash("sha256");
  let size = 0;
  yield* (yield* store.read(oid)).pipe(Stream.runForEach(chunk => Effect.sync(() => {
    size += chunk.length;
    digest.update(chunk);
  })));
  return { ...head, readSize: size, digest: digest.digest("hex") };
})).pipe(Effect.provide(file(root))))));
`;

for (const fault of ["partial-write", "before-rename", "after-rename", "after-acknowledgement"]) {
  it(`LFS process death at ${fault} preserves acknowledged objects and permits retry after offline lock recovery`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-crash-"));
    const content = "new upload whose publication is interrupted\n";
    const oid = createHash("sha256").update(content).digest("hex");
    const retained = "previously acknowledged LFS object\n";
    const retainedOid = createHash("sha256").update(retained).digest("hex");
    const run = (script: string, ...args: string[]) =>
      promisify(execFile)(process.execPath, ["--input-type=module", "-e", script, root, ...args], {
        timeout: 20_000,
      });
    const inspect = async (oid: string, content: string) => {
      const result = await run(reopenedReader, oid);
      assert.deepEqual(JSON.parse(result.stdout), {
        oid,
        size: Buffer.byteLength(content),
        readSize: Buffer.byteLength(content),
        digest: oid,
      });
    };
    try {
      assert.equal(
        (await run(crashWriter, retainedOid, retained, "healthy")).stdout,
        "acknowledged\n",
      );
      await assert.rejects(run(crashWriter, oid, content, fault), (error: Error) => {
        assert.ok("signal" in error && error.signal === "SIGKILL");
        assert.ok("stdout" in error);
        assert.equal(error.stdout, fault === "after-acknowledgement" ? "acknowledged\n" : "");
        return true;
      });
      // execFile has observed the writer's exit before any inspection/cleanup.
      await inspect(retainedOid, retained);
      const unpublished = fault === "partial-write" || fault === "before-rename";
      if (unpublished) assert.equal((await run(reopenedReader, oid)).stdout.trim(), "null");
      else await inspect(oid, content);

      const temporary = (await fs.readdir(root, { recursive: true })).filter((name) =>
        name.endsWith(".tmp"),
      );
      assert.equal(temporary.length, unpublished ? 1 : 0);
      for (const entry of temporary) {
        const target = path.join(root, entry);
        assert.equal((await fs.stat(target)).mode & 0o777, 0o600);
        const bytes = await fs.readFile(target);
        assert.equal(bytes.toString(), fault === "partial-write" ? content.slice(0, 4) : content);
      }
      if (fault !== "after-acknowledgement") {
        await assert.rejects(run(crashWriter, oid, content, "healthy"), /LfsQuotaBusy/);
        // Every writer in this isolated fixture has exited. Never infer this
        // from lock age, nor unlink a quota lock while a writer may still run.
        await fs.unlink(path.join(root, ".quota.lock"));
      }
      assert.equal((await run(crashWriter, oid, content, "healthy")).stdout, "acknowledged\n");
      await inspect(oid, content);
      await inspect(retainedOid, retained);
      // Only the killed writer's known temporary files are removed, offline.
      for (const entry of temporary) await fs.rm(path.join(root, entry));
      assert.equal(
        (await fs.readdir(root, { recursive: true })).some((name) => name.endsWith(".tmp")),
        false,
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}
