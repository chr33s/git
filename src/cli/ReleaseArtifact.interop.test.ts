/** Exercise the publication gate with real Git tags and executable fixtures. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { gitEnv, gitIn, hasGit } from "../testing/Git.ts";

const execute = promisify(execFile);
const verifier = path.resolve("tools/verify-release-artifact.ts");
const cases = [
  ["valid", ""],
  ["dirty checkout", "release checkout must be clean"],
  ["untagged", "unknown revision"],
  ["dirty artifact", "built from a dirty checkout"],
  ["stale artifact", "artifact source revision differs"],
  ["version", "artifact package version differs"],
  ["lockfile", "artifact dependency lock differs"],
  ["checksum", "executable checksum mismatch"],
  ["sidecar", "embedded and external build metadata differ"],
  ["helper", "helper must target adjacent executable"],
  ["architecture", "target architecture"],
  ["runtime", "pinned build runtime"],
];

describe.skipIf(!hasGit)("release artifact gate", () => {
  it.each(cases)("checks %s", async (scenario, error) => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "release-artifact-"));
    try {
      const git = gitIn(root);
      git("init", "-q", "-b", "main");
      await fs.writeFile(path.join(root, ".gitignore"), "dist/\n");
      await fs.writeFile(path.join(root, "package.json"), '{"version":"1.2.3"}\n');
      await fs.writeFile(path.join(root, "package-lock.json"), "{}\n");
      git("add", ".");
      git("commit", "-qm", "release fixture");
      git("tag", "v1.2.3");
      const info = {
        schemaVersion: 1,
        version: scenario === "version" ? "1.2.2" : "1.2.3",
        revision: scenario === "stale artifact" ? "0".repeat(40) : git("rev-parse", "HEAD").trim(),
        dirty: scenario === "dirty artifact",
        platform: process.platform,
        arch: scenario === "architecture" ? "wrong" : process.arch,
        node: scenario === "runtime" ? "0.0.0" : process.versions.node,
        packageLockSha256:
          scenario === "lockfile"
            ? "0".repeat(64)
            : createHash("sha256").update("{}\n").digest("hex"),
      };
      const output = path.join(root, "dist", "sea");
      await fs.mkdir(output, { recursive: true });
      const metadata = `${JSON.stringify(info)}\n`;
      // This executable is a protocol fixture, not a built SEA or a release.
      const executable = `#!${process.execPath}\nprocess.stdout.write(process.argv[2] === "--build-info" ? ${JSON.stringify(metadata)} : "git+ v1.2.3\\n");\n`;
      await fs.writeFile(path.join(output, "git+"), executable, { mode: 0o755 });
      await fs.symlink(
        scenario === "helper" ? "elsewhere" : "git+",
        path.join(output, "git-remote-git+id"),
      );
      await fs.writeFile(
        path.join(output, "build-info.json"),
        scenario === "sidecar" ? "{}\n" : metadata,
      );
      await fs.writeFile(
        path.join(output, "SHA256SUMS"),
        `${createHash("sha256").update(executable).digest("hex")}  git+\n`,
      );
      if (scenario === "checksum") await fs.appendFile(path.join(output, "git+"), "// modified\n");
      if (scenario === "untagged") git("tag", "-d", "v1.2.3");
      if (scenario === "dirty checkout") await fs.writeFile(path.join(root, "untracked"), "change");
      const result = execute(process.execPath, [verifier], {
        cwd: root,
        env: gitEnv,
        timeout: 30_000,
      });
      if (scenario === "valid") {
        assert.match((await result).stdout, /Release artifact verified: v1.2.3/);
      } else {
        assert.ok(error);
        await assert.rejects(result, new RegExp(error));
      }
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
