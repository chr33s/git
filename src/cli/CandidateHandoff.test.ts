import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { verifyCandidate } from "../../tools/verify-candidate-handoff.ts";

for (const fault of [
  "none",
  "archive",
  "report-hash",
  "checksum",
  "revision",
  "version",
  "dirty",
  "rehearsal",
  "failed-proof",
  "extra",
  "symlink",
] as const)
  it(`candidate handoff validates exact evidence: ${fault}`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "candidate-handoff-"));
    const revision = "a".repeat(40);
    const version = "1.2.3";
    const archive = `git-plus-${version}-linux-x64.tar.gz`;
    const bytes = Buffer.from(
      "fixture archive; semantic installation proof is supplied by the preceding CI job",
    );
    const digest = createHash("sha256").update(bytes).digest("hex");
    const identity = { version, revision, dirty: false };
    const report = {
      schemaVersion: 1,
      archiveSha256: digest,
      build: { ...identity, platform: "linux", arch: "x64" },
      release: { ...identity, schemaVersion: 1, kind: "release-candidate" },
      nodeAbsent: true,
      sourceCheckoutAbsent: true,
      installedChecksumsVerified: true,
      embeddedUiServed: true,
      privateAnonymousReadDenied: true,
      identityClonePushFetchVerified: true,
      repositoryIdentityPinned: true,
      nativeGitFsckPassed: true,
    };
    try {
      await fs.writeFile(path.join(root, archive), bytes);
      let checksum = `${digest}  ${archive}\n`;
      switch (fault) {
        case "archive":
          await fs.appendFile(path.join(root, archive), "changed");
          break;
        case "report-hash":
          report.archiveSha256 = "0".repeat(64);
          break;
        case "checksum":
          checksum = `${digest}  ../another-file\n`;
          break;
        case "revision":
          report.build.revision = "b".repeat(40);
          break;
        case "version":
          report.release.version = "1.2.4";
          break;
        case "dirty":
          report.build.dirty = true;
          break;
        case "rehearsal":
          report.release.kind = "rehearsal";
          break;
        case "failed-proof":
          report.identityClonePushFetchVerified = false;
          break;
        case "extra":
          await fs.writeFile(path.join(root, "unreviewed"), "unexpected");
          break;
        case "symlink":
          await fs.unlink(path.join(root, archive));
          await fs.symlink("release-installation.json", path.join(root, archive));
          break;
        case "none":
          break;
      }
      await fs.writeFile(path.join(root, `${archive}.sha256`), checksum);
      await fs.writeFile(path.join(root, "release-installation.json"), JSON.stringify(report));
      if (fault === "none") assert.equal(await verifyCandidate(root, revision, version), archive);
      else await assert.rejects(verifyCandidate(root, revision, version));
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
