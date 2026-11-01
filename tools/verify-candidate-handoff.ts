/** Check the exact files passed between successful CI jobs; never publishes. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Schema } from "effect";

const Identity = {
  version: Schema.NonEmptyString,
  revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  dirty: Schema.Literal(false),
};
const Report = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  archiveSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  build: Schema.Struct({
    ...Identity,
    platform: Schema.Literal("linux"),
    arch: Schema.Literals(["x64", "arm64"]),
  }),
  release: Schema.Struct({
    ...Identity,
    schemaVersion: Schema.Literal(1),
    kind: Schema.Literal("release-candidate"),
  }),
  nodeAbsent: Schema.Literal(true),
  sourceCheckoutAbsent: Schema.Literal(true),
  installedChecksumsVerified: Schema.Literal(true),
  embeddedUiServed: Schema.Literal(true),
  privateAnonymousReadDenied: Schema.Literal(true),
  identityClonePushFetchVerified: Schema.Literal(true),
  repositoryIdentityPinned: Schema.Literal(true),
  nativeGitFsckPassed: Schema.Literal(true),
});

export const verifyCandidate = async (directory: string, revision: string, version: string) => {
  const reportName = "release-installation.json";
  const reportPath = path.join(directory, reportName);
  assert.ok((await fs.lstat(reportPath)).isFile(), "expected regular installation report");
  assert.ok((await fs.stat(reportPath)).size <= 1024 * 1024, "installation report exceeds budget");
  const report = Schema.decodeSync(Schema.fromJsonString(Report))(
    await fs.readFile(reportPath, "utf8"),
  );
  assert.equal(report.build.revision, revision, "candidate revision differs from checkout");
  assert.equal(report.build.version, version, "candidate version differs from checkout");
  assert.equal(report.release.revision, revision, "release manifest revision differs");
  assert.equal(report.release.version, version, "release manifest version differs");
  const archive = `git-plus-${version}-linux-${report.build.arch}.tar.gz`;
  assert.equal(path.basename(archive), archive, "invalid candidate filename");
  const names = [archive, `${archive}.sha256`, reportName].sort();
  assert.deepEqual(
    (await fs.readdir(directory)).sort(),
    names,
    "candidate must contain exactly the archive, checksum and installation report",
  );
  for (const name of names)
    assert.ok(
      (await fs.lstat(path.join(directory, name))).isFile(),
      "candidate files must be regular",
    );
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path.join(directory, archive))) hash.update(chunk);
  const digest = hash.digest("hex");
  assert.equal(report.archiveSha256, digest, "installation report names different archive bytes");
  assert.ok(
    (await fs.stat(path.join(directory, `${archive}.sha256`))).size <= 1024,
    "checksum file exceeds budget",
  );
  assert.equal(
    await fs.readFile(path.join(directory, `${archive}.sha256`), "utf8"),
    `${digest}  ${archive}\n`,
    "archive checksum differs",
  );
  return archive;
};

if (import.meta.main) {
  const [directory, revision, version] = process.argv.slice(2);
  assert.ok(
    directory && revision && version && process.argv.length === 5,
    "usage: node tools/verify-candidate-handoff.ts directory revision version",
  );
  console.info(
    `Verified candidate handoff: ${await verifyCandidate(directory, revision, version)}`,
  );
}
