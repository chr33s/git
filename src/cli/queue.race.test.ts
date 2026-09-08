import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder, opensshPrivateKey } from "../testing/Hub.ts";

const execute = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

describe.skipIf(!hasGit)("queue target snapshot", () => {
  it.live("preserves a Git update between reading the target tip and its write expectation", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "queue-snapshot-"));
      try {
        const directory = path.join(root, "project");
        await fs.mkdir(directory);
        const git = gitIn(directory);
        git("init", "--bare", "-q", "-b", "main");
        fastImport(
          directory,
          [
            importCommit({ branch: "refs/heads/main", mark: 1, message: "base", files: [] }),
            importCommit({
              branch: "refs/heads/topic",
              mark: 2,
              from: 1,
              message: "topic",
              files: [{ path: "topic.txt", content: "proposed\n" }],
            }),
            importCommit({
              branch: "refs/heads/concurrent",
              mark: 3,
              from: 1,
              message: "concurrent",
              files: [{ path: "concurrent.txt", content: "keep me\n" }],
            }),
          ].join(""),
        );
        const before = git("rev-parse", "main").trim();
        const concurrent = git("rev-parse", "concurrent").trim();
        const fixture = await enableHubUnder(root, "project", ["repo.admin"]);
        const key = path.join(root, "runner");
        await fs.writeFile(key, opensshPrivateKey(fixture.member, "runner@example.com"), {
          mode: 0o600,
        });
        const cli = async (args: string[]) =>
          (
            await execute(process.execPath, [entry, ...args, "--root", root, "--key", key])
          ).stdout.trim();
        const queue = await cli(["queue", "open", "--target", "main", "project"]);
        const pr = await cli(["pr", "open", "--title", "Topic", "--head", "topic", "project"]);
        await cli(["queue", "enter", "--queue", queue, "project", pr]);
        const injected = path.join(root, "injected");
        const preload = path.join(root, "race.mjs");
        // Pause the first target read after obtaining its old bytes. Stock Git
        // advances the branch before those bytes reach the queue runner.
        await fs.writeFile(
          preload,
          `
          import fs from 'node:fs/promises';
          import { execFileSync } from 'node:child_process';
          import { syncBuiltinESMExports } from 'node:module';
          const original = fs.readFile;
          let changed = false;
          fs.readFile = async (...args) => {
            const value = await original(...args);
            if (!changed && String(args[0]) === ${JSON.stringify(path.join(directory, "refs/heads/main"))}) {
              changed = true;
              execFileSync('git', ['--git-dir', ${JSON.stringify(directory)}, 'update-ref', 'refs/heads/main', ${JSON.stringify(concurrent)}, ${JSON.stringify(before)}]);
              await fs.writeFile(${JSON.stringify(injected)}, 'yes');
            }
            return value;
          };
          syncBuiltinESMExports();
        `,
        );
        const raced = await execute(process.execPath, [
          "--import",
          preload,
          entry,
          "queue",
          "run",
          "--no-notify",
          "--root",
          root,
          "--key",
          key,
          "--queue",
          queue,
          "project",
        ]);
        const first = JSON.parse(raced.stdout);
        assert.equal(await fs.readFile(injected, "utf8"), "yes");
        assert.deepEqual(first.landed, []);
        assert.equal(first.refused.length, 1);
        assert.equal(git("rev-parse", "main").trim(), concurrent);
        assert.equal(git("merge-base", "--is-ancestor", concurrent, "main"), "");
        assert.equal(git("show", "main:concurrent.txt"), "keep me\n");
        const recovered = JSON.parse(
          await cli(["queue", "run", "--no-notify", "--queue", queue, "project"]),
        );
        assert.deepEqual(recovered.landed, [pr]);
        assert.equal(git("merge-base", "--is-ancestor", concurrent, "main"), "");
        assert.equal(git("show", "main:concurrent.txt"), "keep me\n");
        assert.equal(git("show", "main:topic.txt"), "proposed\n");
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
