/** Required prerequisites must fail CI instead of turning coverage into skips. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { accessSync, constants, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { chromium } from "playwright";
import { verifyReleaseArtifact } from "./verify-release-artifact.ts";

const mode = process.argv[2];
assert.ok(
  mode === "source" || mode === "standalone" || mode === "release",
  "expected source, standalone, or release",
);
if (mode === "release") await verifyReleaseArtifact();
assert.ok(process.platform === "linux" || process.platform === "darwin", "Unix hosts only");
assert.equal(execFileSync("git", ["--version"], { encoding: "utf8" }).trim(), "git version 2.56.0");
const gitPath = execFileSync("git", ["--exec-path"], { encoding: "utf8" }).trim();
accessSync(join(gitPath, "git-http-backend"), constants.X_OK);
execFileSync("tar", ["--version"], { stdio: "ignore" });
execFileSync("unzip", ["-v"], { stdio: "ignore" });

const scratch = mkdtempSync(join(tmpdir(), "git-release-preflight-"));
try {
  execFileSync("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-f", join(scratch, "key")]);
} finally {
  rmSync(scratch, { recursive: true, force: true });
}

// Check the executable the suites use, including its shared-library dependencies.
const browser = await chromium.launch({ headless: true });
await browser.close();

// Both the actual Worker verifier and Node interop rehearse this source baseline.
execFileSync("git", ["cat-file", "-e", "54c6199b79d3b121ad7964ac8d0edaeba85b4666^{commit}"], {
  stdio: "ignore",
});

if (mode === "standalone" || mode === "release") {
  for (const file of ["git+", "git-remote-git+id"]) {
    accessSync(join("dist", "sea", file), constants.X_OK);
  }
  execFileSync(resolve("dist", "sea", "git+"), ["--version"], { stdio: "inherit" });
  for (const file of ["index.html", "bootstrap.js", "main.js", "main.css", "_headers"]) {
    accessSync(join("dist", "ui", file), constants.R_OK);
  }
}

console.info(
  `Release prerequisites passed (${mode}, ${process.platform}/${process.arch}, Node ${process.versions.node}).`,
);
