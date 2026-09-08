import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHubUnder, opensshPrivateKey } from "../testing/Hub.ts";
import { candidateBranch } from "../hub/Queue.ts";

const execute = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

describe.skipIf(!hasGit)("queue candidate cleanup ownership", () => {
  for (const outcome of ["landed", "dropped"]) {
    it(`preserves a ${outcome} candidate branch moved by Git before cleanup acquires its lock`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "queue-cleanup-"));
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
        const candidate = candidateBranch("refs/heads/main", pr);
        if (outcome === "dropped") {
          git("update-ref", candidate, git("rev-parse", "topic").trim());
          await cli(["pr", "close", "project", pr]);
        }
        const injected = path.join(root, "injected");
        const preload = path.join(root, "race.mjs");
        await fs.writeFile(
          preload,
          `
        import fs from 'node:fs/promises';
        import { execFileSync } from 'node:child_process';
        import { syncBuiltinESMExports } from 'node:module';
        const original = fs.open;
        let locks = 0;
        fs.open = async (...args) => {
          if (String(args[0]) === ${JSON.stringify(path.join(directory, `${candidate}.lock`))} && ++locks === ${outcome === "landed" ? 2 : 1}) {
            execFileSync('git', ['--git-dir', ${JSON.stringify(directory)}, 'update-ref', ${JSON.stringify(candidate)}, ${JSON.stringify(concurrent)}]);
            await fs.writeFile(${JSON.stringify(injected)}, 'yes');
          }
          return original(...args);
        };
        syncBuiltinESMExports();
      `,
        );
        const result = await execute(process.execPath, [
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
        assert.equal(await fs.readFile(injected, "utf8"), "yes");
        const pass = JSON.parse(result.stdout);
        assert.deepEqual(pass.landed, outcome === "landed" ? [pr] : []);
        if (outcome === "landed") assert.equal(git("show", "main:topic.txt"), "proposed\n");
        else
          assert.deepEqual(
            pass.dropped.map((entry: { pr: string }) => entry.pr),
            [pr],
          );
        assert.equal(git("show-ref", "--verify", "--hash", candidate).trim(), concurrent);
        assert.equal(git("show", `${candidate}:concurrent.txt`), "keep me\n");
        const next = JSON.parse(
          await cli(["queue", "run", "--no-notify", "--queue", queue, "project"]),
        );
        assert.deepEqual(next.landed, []);
        assert.equal(git("show-ref", "--verify", "--hash", candidate).trim(), concurrent);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
