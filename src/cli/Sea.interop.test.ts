/** Source and SEA must remain identical at the process boundary. */
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { hasGit } from "../testing/Git.ts";
import { sameProcessResult, runProcess } from "../testing/Process.ts";

const source = path.resolve("src", "cli", "main.ts");
const sea = path.resolve("dist", "sea", process.platform === "win32" ? "git+.exe" : "git+");

describe.skipIf(!hasGit || !fs.existsSync(sea))("SEA CLI parity", () => {
  it("matches source help byte-for-byte", async () => {
    const [fromSource, fromSea] = await Promise.all([
      runProcess({ command: process.execPath, args: [source, "--help"] }),
      runProcess({ command: sea, args: ["--help"] }),
    ]);
    assert.equal(sameProcessResult(fromSource, fromSea), true);
  });

  it("matches the source process for a status invocation", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-compat-"));
    try {
      const sourceRepository = path.join(root, "source");
      const seaRepository = path.join(root, "sea");
      await fsPromises.mkdir(sourceRepository);
      await fsPromises.mkdir(seaRepository);
      const initialized = await Promise.all([
        runProcess({
          command: "git",
          args: ["init", "--quiet", "--initial-branch=main"],
          cwd: sourceRepository,
        }),
        runProcess({
          command: "git",
          args: ["init", "--quiet", "--initial-branch=main"],
          cwd: seaRepository,
        }),
      ]);
      assert.equal(initialized[0].code, 0);
      assert.equal(initialized[1].code, 0);
      const [fromSource, fromSea] = await Promise.all([
        runProcess({ command: process.execPath, args: [source, "status"], cwd: sourceRepository }),
        runProcess({ command: sea, args: ["status"], cwd: seaRepository }),
      ]);
      assert.equal(sameProcessResult(fromSource, fromSea), true);
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

  it("matches the source process for a log invocation", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-compat-log-"));
    try {
      const sourceRepository = path.join(root, "source");
      const seaRepository = path.join(root, "sea");
      await fsPromises.mkdir(sourceRepository);
      await fsPromises.mkdir(seaRepository);
      for (const directory of [sourceRepository, seaRepository]) {
        const initialized = await runProcess({
          command: "git",
          args: ["init", "--quiet", "--initial-branch=main"],
          cwd: directory,
        });
        assert.equal(initialized.code, 0);
        await fsPromises.writeFile(path.join(directory, "tracked.txt"), "one\n");
        const added = await runProcess({
          command: "git",
          args: ["add", "--", "tracked.txt"],
          cwd: directory,
        });
        assert.equal(added.code, 0);
        const committed = await runProcess({
          command: "git",
          args: [
            "-c",
            "user.name=Compat",
            "-c",
            "user.email=compat@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "initial",
          ],
          cwd: directory,
        });
        assert.equal(committed.code, 0);
      }
      const [fromSource, fromSea] = await Promise.all([
        runProcess({
          command: process.execPath,
          args: [source, "log", "--format=%s"],
          cwd: sourceRepository,
        }),
        runProcess({ command: sea, args: ["log", "--format=%s"], cwd: seaRepository }),
      ]);
      assert.equal(sameProcessResult(fromSource, fromSea), true);
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

  it("matches the source process for a merge invocation", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-compat-merge-"));
    try {
      const origin = path.join(root, "origin");
      const sourceRepository = path.join(root, "source");
      const seaRepository = path.join(root, "sea");
      await fsPromises.mkdir(origin);
      const initialized = await runProcess({
        command: "git",
        args: ["init", "--quiet", "--initial-branch=main"],
        cwd: origin,
      });
      assert.equal(initialized.code, 0);
      await fsPromises.writeFile(path.join(origin, "tracked.txt"), "base\n");
      const added = await runProcess({
        command: "git",
        args: ["add", "--", "tracked.txt"],
        cwd: origin,
      });
      assert.equal(added.code, 0);
      const committed = await runProcess({
        command: "git",
        args: [
          "-c",
          "user.name=Compat",
          "-c",
          "user.email=compat@example.invalid",
          "commit",
          "--quiet",
          "-m",
          "base",
        ],
        cwd: origin,
      });
      assert.equal(committed.code, 0);
      const topic = await runProcess({
        command: "git",
        args: ["switch", "--quiet", "-c", "topic"],
        cwd: origin,
      });
      assert.equal(topic.code, 0);
      await fsPromises.writeFile(path.join(origin, "topic.txt"), "topic\n");
      const topicAdded = await runProcess({
        command: "git",
        args: ["add", "--", "topic.txt"],
        cwd: origin,
      });
      assert.equal(topicAdded.code, 0);
      const topicCommitted = await runProcess({
        command: "git",
        args: [
          "-c",
          "user.name=Compat",
          "-c",
          "user.email=compat@example.invalid",
          "commit",
          "--quiet",
          "-m",
          "topic",
        ],
        cwd: origin,
      });
      assert.equal(topicCommitted.code, 0);
      const main = await runProcess({
        command: "git",
        args: ["switch", "--quiet", "main"],
        cwd: origin,
      });
      assert.equal(main.code, 0);
      await Promise.all([
        fsPromises.cp(origin, sourceRepository, { recursive: true }),
        fsPromises.cp(origin, seaRepository, { recursive: true }),
      ]);
      const [fromSource, fromSea] = await Promise.all([
        runProcess({
          command: process.execPath,
          args: [source, "merge", "--ff-only", "topic"],
          cwd: sourceRepository,
        }),
        runProcess({ command: sea, args: ["merge", "--ff-only", "topic"], cwd: seaRepository }),
      ]);
      assert.equal(sameProcessResult(fromSource, fromSea), true);
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

  it("matches the source process for a remote invocation", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-compat-remote-"));
    try {
      const sourceRepository = path.join(root, "source");
      const seaRepository = path.join(root, "sea");
      await Promise.all([fsPromises.mkdir(sourceRepository), fsPromises.mkdir(seaRepository)]);
      for (const directory of [sourceRepository, seaRepository]) {
        const initialized = await runProcess({
          command: "git",
          args: ["init", "--quiet", "--initial-branch=main"],
          cwd: directory,
        });
        assert.equal(initialized.code, 0);
      }
      const [fromSource, fromSea] = await Promise.all([
        runProcess({
          command: process.execPath,
          args: [source, "remote", "add", "origin", "../remote"],
          cwd: sourceRepository,
        }),
        runProcess({
          command: sea,
          args: ["remote", "add", "origin", "../remote"],
          cwd: seaRepository,
        }),
      ]);
      assert.equal(sameProcessResult(fromSource, fromSea), true);
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

  it("matches the source process for an fsck invocation", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-compat-fsck-"));
    try {
      const sourceRepository = path.join(root, "source");
      const seaRepository = path.join(root, "sea");
      await Promise.all([fsPromises.mkdir(sourceRepository), fsPromises.mkdir(seaRepository)]);
      for (const directory of [sourceRepository, seaRepository]) {
        const initialized = await runProcess({
          command: "git",
          args: ["init", "--quiet", "--initial-branch=main"],
          cwd: directory,
        });
        assert.equal(initialized.code, 0);
      }
      const [fromSource, fromSea] = await Promise.all([
        runProcess({
          command: process.execPath,
          args: [source, "fsck", "--full"],
          cwd: sourceRepository,
        }),
        runProcess({ command: sea, args: ["fsck", "--full"], cwd: seaRepository }),
      ]);
      assert.equal(sameProcessResult(fromSource, fromSea), true);
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });
});
