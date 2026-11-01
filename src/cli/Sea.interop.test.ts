/** Source and SEA must remain identical at the process boundary. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { hasGit, gitEnv } from "../testing/Git.ts";
import { sameProcessResult, runProcess } from "../testing/Process.ts";
import { enableHubUnder, opensshPrivateKey } from "../testing/Hub.ts";
import { encodeRepository } from "../social/Encode.ts";
import { serve } from "../host/Node.ts";
import { Result } from "effect";

const source = path.resolve("src", "cli", "main.ts");
const sea = path.resolve("dist", "sea", process.platform === "win32" ? "git+.exe" : "git+");

describe.skipIf(!hasGit || !fs.existsSync(sea))("SEA CLI parity", () => {
  it("retains build identity after installation and matches the final executable checksum", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-build-info-"));
    try {
      const installed = path.join(root, "git+");
      await fsPromises.copyFile(sea, installed);
      await fsPromises.chmod(installed, 0o755);
      const result = await runProcess({
        command: installed,
        args: ["--build-info"],
        cwd: root,
        env: { ...gitEnv, PATH: root },
      });
      assert.equal(result.code, 0, result.stderr.toString());
      assert.equal(
        result.stdout.toString(),
        await fsPromises.readFile(path.join(path.dirname(sea), "build-info.json"), "utf8"),
      );
      assert.match(result.stdout.toString(), /"revision": "[0-9a-f]{40}"/);
      assert.match(result.stdout.toString(), /"dirty": (true|false)/);
      for (const [flag, file] of [
        ["--license", "LICENSE"],
        ["--licenses", "THIRD_PARTY_NOTICES.txt"],
        ["--dependency-inventory", "dependency-inventory.json"],
      ] as const) {
        const included = await runProcess({
          command: installed,
          args: [flag],
          cwd: root,
          env: { ...gitEnv, PATH: root },
        });
        assert.equal(included.code, 0, included.stderr.toString());
        assert.equal(
          included.stdout.toString(),
          await fsPromises.readFile(path.join(path.dirname(sea), file), "utf8"),
        );
        assert.ok(included.stdout.length > 1000);
      }
      const hash = createHash("sha256");
      for await (const chunk of fs.createReadStream(installed)) hash.update(chunk);
      assert.equal(
        await fsPromises.readFile(path.join(path.dirname(sea), "SHA256SUMS"), "utf8"),
        `${hash.digest("hex")}  ${path.basename(sea)}\n`,
      );
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

  it("clones, pushes, and fetches through the installed identity helper without Node", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-helper-"));
    const server = await serve({ root });
    try {
      const install = path.join(root, "standalone ' install");
      await fsPromises.mkdir(install);
      await fsPromises.copyFile(sea, path.join(install, "git+"));
      await fsPromises.chmod(path.join(install, "git+"), 0o755);
      // Install the actual generated helper artifact, retaining its target.
      const link = await fsPromises.readlink(path.join(path.dirname(sea), "git-remote-git+id"));
      await fsPromises.symlink(link, path.join(install, "git-remote-git+id"));
      const gitPath = (process.env["PATH"] ?? "")
        .split(path.delimiter)
        .map((directory) => path.join(directory, "git"))
        .find((file) => fs.existsSync(file));
      assert.ok(gitPath);
      await fsPromises.symlink(await fsPromises.realpath(gitPath), path.join(install, "git"));
      const execPath = await runProcess({ command: "git", args: ["--exec-path"] });
      assert.equal(execPath.code, 0);
      const fixture = await enableHubUnder(root, "project", ["source.push"]);
      const encoded = encodeRepository({ id: fixture.repoId, hints: [`${server.url}/project`] });
      assert.ok(Result.isSuccess(encoded));
      const environment = {
        ...gitEnv,
        PATH: install,
        XDG_CONFIG_HOME: path.join(root, "config"),
        GIT_EXEC_PATH: execPath.stdout.toString().trim(),
        GIT_TERMINAL_PROMPT: "0",
        GIT_CONFIG_COUNT: "2",
        GIT_CONFIG_KEY_0: "credential.helper",
        GIT_CONFIG_VALUE_0: "",
        GIT_CONFIG_KEY_1: "credential.helper",
        GIT_CONFIG_VALUE_1: '!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
        GIT_AUTH_TOKEN: fixture.credential,
      };
      assert.equal(fs.existsSync(path.join(install, "node")), false);
      const git = async (cwd: string, ...args: string[]) => {
        const result = await runProcess({
          command: path.join(install, "git"),
          args,
          cwd,
          env: environment,
        });
        assert.equal(result.code, 0, result.stderr.toString());
        return result.stdout.toString().trim();
      };
      const first = path.join(root, "first");
      const second = path.join(root, "second");
      await git(root, "clone", "--quiet", `git+id://${encoded.success}`, first);
      await git(first, "checkout", "-B", "main");
      await fsPromises.writeFile(path.join(first, "content.txt"), "one\n");
      await git(first, "add", ".");
      await git(first, "commit", "--quiet", "-m", "one");
      await git(first, "push", "--quiet", "origin", "main");
      await git(root, "clone", "--quiet", `git+id://${encoded.success}`, second);
      assert.equal(await fsPromises.readFile(path.join(second, "content.txt"), "utf8"), "one\n");
      await fsPromises.writeFile(path.join(second, "content.txt"), "two\n");
      await git(second, "commit", "--quiet", "-am", "two");
      await git(second, "push", "--quiet", "origin", "main");
      await git(first, "fetch", "--quiet", "origin");
      assert.equal(await git(first, "show", "origin/main:content.txt"), "two");
      assert.equal(
        await git(first, "rev-parse", "origin/main"),
        await git(second, "rev-parse", "HEAD"),
      );
      assert.match(
        await fsPromises.readFile(path.join(root, "config", "chr33s-git", "known_repos"), "utf8"),
        /SHA256:/,
      );
    } finally {
      await server.close({ force: true });
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

  it("installs session hooks that run without Node or a source checkout", async () => {
    const root = await fsPromises.mkdtemp(path.join(os.tmpdir(), "sea-hooks-"));
    try {
      const install = path.join(root, "standalone ' install");
      const work = path.join(root, "work ' tree");
      await fsPromises.mkdir(install);
      await fsPromises.mkdir(work);
      const executable = path.join(install, "git+");
      await fsPromises.copyFile(sea, executable);
      await fsPromises.chmod(executable, 0o755);
      const fixture = await enableHubUnder(root, "project", ["hub.session"]);
      const key = path.join(root, "key");
      await fsPromises.writeFile(key, opensshPrivateKey(fixture.member, "test"), { mode: 0o600 });
      const installed = await runProcess({
        command: executable,
        args: ["session", "enable", "--root", root, "--work", work, "--key", key, "project"],
      });
      assert.equal(installed.code, 0, installed.stdout.toString() + installed.stderr.toString());
      const settings: { hooks: Record<string, Array<{ hooks: Array<{ command: string }> }>> } =
        JSON.parse(await fsPromises.readFile(path.join(work, ".claude", "settings.json"), "utf8"));
      const invoke = (event: string, body: string) => {
        const command = settings.hooks[event]?.[0]?.hooks[0]?.command;
        assert.ok(command !== undefined);
        return runProcess({
          command: "/bin/sh",
          args: ["-c", command],
          cwd: work,
          env: {
            ...process.env,
            PATH: path.join(root, "no-node"),
            CHR33S_GIT_BRANCH: "refs/heads/topic",
          },
          stdin: new TextEncoder().encode(body),
        });
      };
      const started = await invoke(
        "UserPromptSubmit",
        JSON.stringify({ prompt: "record from a standalone install" }),
      );
      assert.equal(started.code, 0, started.stdout.toString() + started.stderr.toString());
      const id = (
        await fsPromises.readFile(path.join(work, ".chr33s", "session.id"), "utf8")
      ).trim();
      const stopped = await invoke("Stop", "{}");
      assert.equal(stopped.code, 0, stopped.stdout.toString() + stopped.stderr.toString());
      const shown = await runProcess({
        command: executable,
        args: ["session", "show", "--root", root, "--repo", "project", id],
      });
      assert.equal(shown.code, 0);
      const projection: { prompts: Array<{ prompt: string }>; refs: Array<string> } = JSON.parse(
        shown.stdout.toString(),
      );
      assert.deepEqual(
        projection.prompts.map((entry) => entry.prompt),
        ["record from a standalone install"],
      );
      assert.deepEqual(projection.refs, ["refs/heads/topic"]);
    } finally {
      await fsPromises.rm(root, { recursive: true, force: true });
    }
  });

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
