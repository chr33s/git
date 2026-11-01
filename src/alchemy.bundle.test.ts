import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { gzipSync } from "node:zlib";
import { it } from "@effect/vitest";
import { measureWorkerBundle } from "../tools/measure-worker-bundle.ts";

it("measures all emitted module bytes and excludes only source maps", async () => {
  const directory = await fs.mkdtemp(path.join(tmpdir(), "worker-size-"));
  try {
    const files = [
      { name: "module.wasm", data: Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]) },
      {
        name: "worker.js",
        data: Buffer.from("export default {fetch(){return new Response('東京')}}"),
      },
    ];
    for (const file of files) await fs.writeFile(path.join(directory, file.name), file.data);
    await fs.writeFile(path.join(directory, "worker.js.map"), "excluded debug information");
    const report = await measureWorkerBundle(directory);
    assert.deepEqual(
      report.modules,
      files.map((file) => ({
        name: file.name,
        bytes: file.data.length,
        gzipBytes: gzipSync(file.data, { level: 9 }).length,
        sha256: createHash("sha256").update(file.data).digest("hex"),
      })),
    );
    assert.equal(
      report.bytes,
      files.reduce((sum, file) => sum + file.data.length, 0),
    );
    assert.equal(
      report.gzipBytes,
      files.reduce((sum, file) => sum + gzipSync(file.data, { level: 9 }).length, 0),
    );
    assert.deepEqual(report.excludedSourceMaps, ["worker.js.map"]);
    await fs.symlink("worker.js", path.join(directory, "unexpected-link"));
    await assert.rejects(measureWorkerBundle(directory), /regular bundle files/);
    await fs.unlink(path.join(directory, "unexpected-link"));
    await fs.unlink(path.join(directory, "worker.js"));
    await assert.rejects(measureWorkerBundle(directory), /actual Worker entry/);
  } finally {
    await fs.rm(directory, { recursive: true, force: true });
  }
});
