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

describe.skipIf(!hasGit)("queue settlement ownership", () => {
  for (const boundary of ["landing", "merge event"]) {
    it(`retains a newer proposal entered during ${boundary}`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "queue-settlement-"));
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
              files: [{ path: "topic.txt", content: "old proposal\n" }],
            }),
            importCommit({
              branch: "refs/heads/newer",
              mark: 3,
              from: 2,
              message: "newer",
              files: [{ path: "newer.txt", content: "new proposal\n" }],
            }),
          ].join(""),
        );
        const newer = git("rev-parse", "newer").trim();
        const fixture = await enableHubUnder(root, "project", ["repo.admin"]);
        const key = path.join(root, "runner");
        await fs.writeFile(key, opensshPrivateKey(fixture.member, "runner@example.com"), {
          mode: 0o600,
        });
        const cli = async (args: string[]) =>
          (
            await execute(process.execPath, [entry, ...args, "--root", root, "--key", key])
          ).stdout.trim();
        const show = async (args: string[]) =>
          JSON.parse((await execute(process.execPath, [entry, ...args, "--root", root])).stdout);
        const queue = await cli(["queue", "open", "--target", "main", "project"]);
        const pr = await cli(["pr", "open", "--title", "Topic", "--head", "topic", "project"]);
        await cli(["queue", "enter", "--queue", queue, "project", pr]);
        const injected = path.join(root, "injected");
        const preload = path.join(root, "race.mjs");
        await fs.writeFile(
          preload,
          `
        import fs from 'node:fs/promises';
        import { execFileSync } from 'node:child_process';
        import { syncBuiltinESMExports } from 'node:module';
        const original = fs.open;
        let injected = false;
        fs.open = async (...args) => {
          if (!injected && String(args[0]) === ${JSON.stringify(path.join(directory, boundary === "landing" ? "refs/heads/main.lock" : `refs/hub/pr/${pr}.lock`))}) {
            injected = true;
            execFileSync(process.execPath, ${JSON.stringify([entry, "pr", "update", "--head", "newer", "project", pr, "--root", root, "--key", key])});
            execFileSync(process.execPath, ${JSON.stringify([entry, "queue", "enter", "--queue", queue, "project", pr, "--root", root, "--key", key])});
            await fs.writeFile(${JSON.stringify(injected)}, 'yes');
          }
          return original(...args);
        };
        syncBuiltinESMExports();
      `,
        );
        await execute(process.execPath, [
          "--import",
          preload,
          entry,
          "queue",
          "run",
          "--no-notify",
          "--queue",
          queue,
          "project",
          "--root",
          root,
          "--key",
          key,
        ]);
        assert.equal(await fs.readFile(injected, "utf8"), "yes");
        assert.equal(git("ls-tree", "--name-only", "main").trim(), "topic.txt");
        const proposal = await show(["pr", "show", "project", pr]);
        assert.equal(proposal.head, newer);
        assert.equal(proposal.state, "open");
        const queued = await show(["queue", "show", "project", queue]);
        assert.equal(queued.entries.length, 1);
        assert.equal(queued.entries[0].pr, pr);
        assert.equal(queued.entries[0].head, newer);
        assert.equal(
          git("show-ref", "--verify", "--hash", candidateBranch("refs/heads/main", pr)).trim(),
          git("rev-parse", "main").trim(),
        );
        await cli(["queue", "run", "--no-notify", "--queue", queue, "project"]);
        assert.equal(git("show", "main:newer.txt"), "new proposal\n");
        assert.equal((await show(["pr", "show", "project", pr])).state, "merged");
        assert.deepEqual((await show(["queue", "show", "project", queue])).entries, []);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
