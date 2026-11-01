/** Local artifact gate; publication still requires authenticated provenance. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, readFileSync, readlinkSync } from "node:fs";
import { resolve } from "node:path";

export const verifyReleaseArtifact = async () => {
  const git = (...args: string[]) => execFileSync("git", args, { encoding: "utf8" }).trim();
  assert.equal(
    git("status", "--porcelain", "--untracked-files=normal"),
    "",
    "release checkout must be clean",
  );
  const manifest = JSON.parse(readFileSync("package.json", "utf8"));
  assert.match(manifest.version, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/, "invalid release version");
  const revision = git("rev-parse", "HEAD");
  assert.equal(
    git("rev-parse", `refs/tags/v${manifest.version}^{commit}`),
    revision,
    "version tag must identify HEAD",
  );
  assert.ok(process.platform === "linux" || process.platform === "darwin", "Unix artifacts only");
  const executable = resolve("dist/sea/git+");
  assert.equal(
    readlinkSync("dist/sea/git-remote-git+id"),
    "git+",
    "helper must target adjacent executable",
  );
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(executable)) hash.update(chunk);
  assert.equal(
    readFileSync("dist/sea/SHA256SUMS", "utf8"),
    `${hash.digest("hex")}  git+\n`,
    "executable checksum mismatch",
  );
  const embedded = execFileSync(executable, ["--build-info"], {
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.equal(
    readFileSync("dist/sea/build-info.json", "utf8"),
    embedded,
    "embedded and external build metadata differ",
  );
  const info = JSON.parse(embedded);
  assert.equal(info.schemaVersion, 1, "unsupported build metadata");
  assert.equal(info.dirty, false, "release artifact was built from a dirty checkout");
  assert.equal(info.revision, revision, "artifact source revision differs from HEAD");
  assert.equal(info.version, manifest.version, "artifact package version differs");
  assert.equal(info.platform, process.platform, "test artifact on its target platform");
  assert.equal(info.arch, process.arch, "test artifact on its target architecture");
  assert.equal(info.node, process.versions.node, "verify with the pinned build runtime");
  assert.equal(
    info.packageLockSha256,
    createHash("sha256").update(readFileSync("package-lock.json")).digest("hex"),
    "artifact dependency lock differs",
  );
  assert.equal(
    execFileSync(executable, ["--version"], { encoding: "utf8", timeout: 30_000 }).trim(),
    `git+ v${manifest.version}`,
    "CLI version differs from build metadata",
  );
  console.info(
    `Release artifact verified: v${manifest.version} ${revision} ${info.platform}/${info.arch}`,
  );
};

if (import.meta.main) await verifyReleaseArtifact();
