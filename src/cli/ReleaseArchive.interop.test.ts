import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import { existsSync } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";

const execute = promisify(execFile);
const source = path.resolve("dist/sea");
const packager = pathToFileURL(path.resolve("tools/package-standalone.ts")).href;

describe.skipIf(!existsSync(path.join(source, "git+")))("standalone archive", () => {
  it("verifies extracted bytes and refuses a notice that differs from the executable", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "release-archive-"));
    try {
      const fixture = path.join(root, "dist/sea");
      await fs.mkdir(fixture, { recursive: true });
      await fs.mkdir(path.join(root, "docs"));
      await fs.copyFile("docs/operations.md", path.join(root, "docs/operations.md"));
      for (const file of [
        "git+",
        "build-info.json",
        "LICENSE",
        "THIRD_PARTY_NOTICES.txt",
        "dependency-inventory.json",
        "SHA256SUMS",
      ]) {
        await fs.copyFile(path.join(source, file), path.join(fixture, file));
      }
      await fs.symlink("git+", path.join(fixture, "git-remote-git+id"));
      const run = (destination: string) =>
        execute(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            `import { packageStandalone } from ${JSON.stringify(packager)}; await packageStandalone(true, ${JSON.stringify(destination)});`,
          ],
          { cwd: root, timeout: 90_000 },
        );
      assert.match((await run("accepted")).stdout, /Verified rehearsal archive/);
      const files = await fs.readdir(path.join(root, "accepted"));
      assert.equal(files.length, 2);
      assert.ok(files.some((file) => file.endsWith("-rehearsal.tar.gz")));
      assert.ok(files.some((file) => file.endsWith("-rehearsal.tar.gz.sha256")));
      const checksumName = files.find((file) => file.endsWith(".sha256"));
      assert.ok(checksumName);
      await fs.mkdir(path.join(root, "occupied"));
      await fs.writeFile(path.join(root, "occupied", checksumName), "existing checksum\n");
      await assert.rejects(run("occupied"), /EEXIST/);
      assert.deepEqual(await fs.readdir(path.join(root, "occupied")), [checksumName]);
      assert.equal(
        await fs.readFile(path.join(root, "occupied", checksumName), "utf8"),
        "existing checksum\n",
      );
      await fs.appendFile(path.join(fixture, "LICENSE"), "tampered license\n");
      await assert.rejects(run("rejected"), /embedded LICENSE differs/);
      assert.deepEqual(
        await fs.readdir(path.join(root, "rejected")),
        [],
        "failed packaging must leave no archive or staging directory",
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
