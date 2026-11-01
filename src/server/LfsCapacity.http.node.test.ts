import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { reserve } from "./LfsCapacity.node.ts";

const script = `
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { serve } from ${JSON.stringify(new URL("../host/Node.ts", import.meta.url).href)};
const [root, mode] = process.argv.slice(1);
const server = await serve({ root, allowAnonymousWrites: true });
const oid = content => createHash("sha256").update(content).digest("hex");
const url = content => server.url + "/r/info/lfs/objects/" + oid(content);
try {
  const first = await fetch(url("123456"), { method: "PUT", body: "123456" });
  assert.equal(first.status, mode === "busy" ? 503 : 200);
  if (mode === "busy") {
    assert.equal(first.headers.get("retry-after"), "1");
    assert.equal(first.headers.get("cache-control"), "no-store");
  }
  await first.arrayBuffer();
  if (mode !== "busy") {
    // A chunked upload also has to fit the aggregate retained-byte allowance.
    const body = new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode("abc"));
      controller.enqueue(new TextEncoder().encode("def"));
      controller.close();
    } });
    const full = await fetch(url("abcdef"), { method: "PUT", body, duplex: "half" });
    assert.equal(full.status, 507);
    assert.match(await full.text(), /LFS retained capacity exceeded/);
    const absent = await fetch(url("abcdef"));
    assert.equal(absent.status, 404);
    await absent.arrayBuffer();
    const retry = await fetch(url("ab"), { method: "PUT", body: "ab" });
    assert.equal(retry.status, 200);
    await retry.arrayBuffer();
    const preserved = await fetch(url("123456"));
    assert.equal(preserved.status, 200);
    assert.equal(await preserved.text(), "123456");
  }
} finally { await server.close(); }
`;

it("enforces quotas over HTTP and excludes a second process while a reservation is held", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "lfs-quota-http-"));
  const run = (mode: string) =>
    promisify(execFile)(process.execPath, ["--input-type=module", "-e", script, root, mode], {
      timeout: 30_000,
      env: { ...process.env, GIT_LFS_RETAINED_BYTES: "10", GIT_LFS_RETAINED_OBJECTS: "100" },
    });
  try {
    const lfs = path.join(root, "r", "lfs");
    await Effect.runPromise(
      reserve(lfs, { bytes: 10, objects: 100 }).pipe(
        Effect.andThen(Effect.promise(() => run("busy"))),
        Effect.scoped,
      ),
    );
    await assert.rejects(fs.stat(path.join(lfs, ".quota.lock")), { code: "ENOENT" });
    await run("capacity");
    const files = await fs.readdir(lfs, { recursive: true });
    assert.equal(
      files.some((name) => name.endsWith(".tmp") || name === ".quota.lock"),
      false,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
