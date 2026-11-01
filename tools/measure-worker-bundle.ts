/** Measure emitted local Worker modules; this is not a Cloudflare upload receipt. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";

export const measureWorkerBundle = async (directory: string) => {
  const names = (await fs.readdir(directory)).sort();
  assert.ok(names.includes("worker.js"), "expected the actual Worker entry bundle");
  const modules = [];
  const excludedSourceMaps: string[] = [];
  for (const name of names) {
    const file = path.join(directory, name);
    assert.ok((await fs.lstat(file)).isFile(), "expected flat regular bundle files");
    if (name.endsWith(".map")) {
      excludedSourceMaps.push(name);
      continue;
    }
    const hash = createHash("sha256");
    let bytes = 0;
    const source = async function* () {
      for await (const chunk of createReadStream(file)) {
        bytes += chunk.length;
        hash.update(chunk);
        yield chunk;
      }
    };
    let gzipBytes = 0;
    await pipeline(source(), createGzip({ level: 9 }), async (chunks) => {
      for await (const chunk of chunks) gzipBytes += chunk.length;
    });
    modules.push({ name, bytes, gzipBytes, sha256: hash.digest("hex") });
  }
  return {
    schemaVersion: 1,
    measuredAt: new Date().toISOString(),
    kind: "local-worker-bundle",
    compression: "sum of independently gzip-compressed modules, zlib level 9",
    bytes: modules.reduce((total, module) => total + module.bytes, 0),
    gzipBytes: modules.reduce((total, module) => total + module.gzipBytes, 0),
    modules,
    excludedSourceMaps,
    limitsOfEvidence:
      "Actual local Alchemy Worker build; excludes static assets, source maps and upload multipart metadata. Not a production upload receipt, account-limit check, startup CPU or runtime memory measurement.",
  };
};
