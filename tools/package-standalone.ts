/** Package and verify exact standalone bytes. Never publishes an artifact. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { verifyReleaseArtifact } from "./verify-release-artifact.ts";

const digest = async (file: string) => {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file)) hash.update(chunk);
  return hash.digest("hex");
};

export const packageStandalone = async (rehearsal: boolean, destination = "dist/releases") => {
  if (!rehearsal) await verifyReleaseArtifact();
  assert.ok(process.platform === "linux" || process.platform === "darwin", "Unix archives only");
  const source = path.resolve("dist/sea");
  const metadata = await fs.readFile(path.join(source, "build-info.json"), "utf8");
  const info = JSON.parse(metadata);
  assert.match(info.version, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/);
  assert.equal(info.platform, process.platform);
  assert.equal(info.arch, process.arch);
  const name = `git-plus-${info.version}-${process.platform}-${process.arch}${rehearsal ? "-rehearsal" : ""}`;
  const files = [
    "git+",
    "build-info.json",
    "LICENSE",
    "THIRD_PARTY_NOTICES.txt",
    "dependency-inventory.json",
    "SHA256SUMS",
  ];
  const output = path.resolve(destination);
  await fs.mkdir(output, { recursive: true });
  const scratch = await fs.mkdtemp(path.join(output, ".package-"));
  try {
    const staged = path.join(scratch, name);
    await fs.mkdir(staged);
    for (const file of files) {
      assert.ok(
        (await fs.lstat(path.join(source, file))).isFile(),
        `expected regular artifact: ${file}`,
      );
      await fs.copyFile(path.join(source, file), path.join(staged, file));
      await fs.chmod(path.join(staged, file), file === "git+" ? 0o755 : 0o644);
    }
    assert.equal(await fs.readlink(path.join(source, "git-remote-git+id")), "git+");
    await fs.symlink("git+", path.join(staged, "git-remote-git+id"));
    assert.equal(
      await fs.readFile(path.join(staged, "SHA256SUMS"), "utf8"),
      `${await digest(path.join(staged, "git+"))}  git+\n`,
      "artifact checksum mismatch",
    );
    const manifest = `${JSON.stringify({ schemaVersion: 1, kind: rehearsal ? "rehearsal" : "release-candidate", version: info.version, revision: info.revision, dirty: info.dirty }, null, 2)}\n`;
    await fs.writeFile(path.join(staged, "RELEASE.json"), manifest);
    await fs.copyFile("docs/operations.md", path.join(staged, "OPERATIONS.md"));
    const archiveFiles = [...files, "RELEASE.json", "OPERATIONS.md"];
    const checksums: string[] = [];
    for (const file of archiveFiles)
      checksums.push(`${await digest(path.join(staged, file))}  ${file}\n`);
    await fs.writeFile(path.join(staged, "CONTENTS.sha256"), checksums.join(""));
    archiveFiles.push("CONTENTS.sha256");
    const archive = path.join(scratch, `${name}.tar.gz`);
    execFileSync("tar", ["-czf", archive, "-C", scratch, name], { timeout: 120_000 });
    const extracted = path.join(scratch, "extracted");
    await fs.mkdir(extracted);
    execFileSync("tar", ["-xzf", archive, "-C", extracted], { timeout: 120_000 });
    const installed = path.join(extracted, name);
    assert.deepEqual(
      (await fs.readdir(installed)).sort(),
      [...archiveFiles, "git-remote-git+id"].sort(),
    );
    for (const file of archiveFiles) {
      assert.ok((await fs.lstat(path.join(installed, file))).isFile());
      assert.equal(
        await digest(path.join(installed, file)),
        await digest(path.join(staged, file)),
        `archive changed ${file}`,
      );
    }
    assert.equal(await fs.readlink(path.join(installed, "git-remote-git+id")), "git+");
    for (const [flag, file] of [
      ["--build-info", "build-info.json"],
      ["--license", "LICENSE"],
      ["--licenses", "THIRD_PARTY_NOTICES.txt"],
      ["--dependency-inventory", "dependency-inventory.json"],
    ]) {
      assert.ok(flag && file);
      const embedded = execFileSync(path.join(installed, "git+"), [flag], {
        cwd: installed,
        env: { ...process.env, PATH: installed },
        encoding: "utf8",
        maxBuffer: 16 * 1024 * 1024,
        timeout: 30_000,
      });
      assert.equal(
        embedded,
        await fs.readFile(path.join(installed, file), "utf8"),
        `embedded ${file} differs`,
      );
    }
    const target = path.join(output, `${name}.tar.gz`);
    const archiveChecksum = `${await digest(archive)}  ${path.basename(target)}\n`;
    const checksumFile = path.join(scratch, "archive.sha256");
    await fs.writeFile(checksumFile, archiveChecksum);
    // Exclusive links avoid overwriting a previously verified artifact.
    await fs.link(archive, target);
    try {
      await fs.link(checksumFile, `${target}.sha256`);
    } catch (error) {
      await fs.unlink(target);
      throw error;
    }
    console.info(`Verified ${rehearsal ? "rehearsal" : "release candidate"} archive: ${target}`);
    return target;
  } finally {
    await fs.rm(scratch, { recursive: true, force: true });
  }
};

if (import.meta.main) {
  assert.ok(
    process.argv.length === 2 || (process.argv.length === 3 && process.argv[2] === "--rehearsal"),
    "usage: node tools/package-standalone.ts [--rehearsal]",
  );
  await packageStandalone(process.argv[2] === "--rehearsal");
}
