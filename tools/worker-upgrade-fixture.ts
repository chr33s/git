/** Local source/data compatibility, using the installed dependency set. */
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { promisify } from "node:util";
import assert from "node:assert/strict";

// The same archived prerelease required by release preflight and the Node test.
export const baselineRevision = "54c6199b79d3b121ad7964ac8d0edaeba85b4666";

/** Prepare both source choices; switch only after the verifier stops the runtime. */
export const sourceVersions = async (scratch: string, installation: string, port: number) => {
  const execute = promisify(execFile);
  const baseline = path.join(scratch, "baseline");
  const archive = path.join(scratch, "baseline.tar");
  await fs.mkdir(baseline);
  await execute("git", [
    "archive",
    "--format=tar",
    "--output",
    archive,
    baselineRevision,
    "src",
    "package.json",
    "tsconfig.json",
  ]);
  await execute("tar", ["-xf", archive, "-C", baseline]);
  // The old version predates configurable local ports. Keep the same origin
  // across source switches without changing its request or storage behavior.
  const entry = path.join(baseline, "src/worker.ts");
  const source = await fs.readFile(entry, "utf8");
  const marker = "main: import.meta.url,";
  assert.equal(source.split(marker).length, 2);
  assert.ok(Number.isInteger(port) && port > 0 && port <= 65535);
  await fs.writeFile(
    entry,
    source.replace(marker, `${marker}\n    dev: { port: ${port}, strictPort: true },`),
  );
  await fs.symlink(path.resolve("node_modules"), path.join(baseline, "node_modules"));
  await fs.symlink(path.resolve("dist"), path.join(baseline, "dist"));
  return async (version: "baseline" | "candidate") => {
    await fs.unlink(path.join(installation, "src"));
    await fs.symlink(
      version === "baseline" ? path.join(baseline, "src") : path.resolve("src"),
      path.join(installation, "src"),
    );
  };
};
