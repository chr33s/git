import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { hasGit } from "../testing/Git.ts";

// Native filesystem faults stay inside a child; the HTTP host and stock Git
// execute their real write, rejection, cleanup, restart, and retry paths.
describe.skipIf(!hasGit)("HTTP pushes with exhausted storage", () => {
  for (const fault of ["object-write", "object-sync", "ref-write", "journal-write"]) {
    it(`refuses ${fault} ENOSPC without advancing refs and recovers after restart`, async () => {
      const script = `
        import assert from "node:assert/strict";
        import fs from "node:fs/promises";
        import { syncBuiltinESMExports } from "node:module";
        import { join } from "node:path";
        import { tmpdir } from "node:os";
        import { execFile } from "node:child_process";
        import { promisify } from "node:util";
        import { serve } from ${JSON.stringify(new URL("../host/Node.ts", import.meta.url).href)};
        import { gitIn, gitEnv, fastImport, importCommit } from ${JSON.stringify(new URL("../testing/Git.ts", import.meta.url).href)};
        const fault = process.argv[1];
        const root = await fs.realpath(await fs.mkdtemp(join(tmpdir(), "git-disk-full-")));
        const repository = join(root, "r");
        const client = join(root, "client");
        await fs.mkdir(repository);
        gitIn(repository)("init", "--bare", "-q", "-b", "main");
        fastImport(repository, importCommit({ branch: "refs/heads/main", mark: 1, message: "accepted", files: [{path: "file", content: "before"}] }));
        const before = gitIn(repository)("rev-parse", "main").trim();
        gitIn(repository)("update-ref", "refs/heads/side", before);
        gitIn(root)("clone", "-q", repository, client);
        await fs.writeFile(join(client, "file"), "after exhausted storage recovers");
        gitIn(client)("add", "file");
        gitIn(client)("commit", "-qm", "candidate");
        const candidate = gitIn(client)("rev-parse", "HEAD").trim();
        const original = fs.open;
        let broken = true;
        let injected = 0;
        const fail = () => {
          injected++;
          throw Object.assign(new Error("private disk-full fixture detail"), { code: "ENOSPC" });
        };
        fs.open = async (...args) => {
          const file = await original(...args);
          const target = String(args[0]);
          const matches = target.endsWith(".tmp") && (
            (fault.startsWith("object-") && target.startsWith(join(repository, "objects") + "/")) ||
            (fault === "ref-write" && target.startsWith(join(repository, "refs", "heads") + "/")) ||
            (fault === "journal-write" && target.startsWith(join(repository, ".git-plus-ref-transaction.json") + "."))
          );
          if (broken && matches) {
            if (fault.endsWith("sync")) file.sync = async () => fail();
            else {
              const write = file.writeFile.bind(file);
              file.writeFile = async bytes => { await write(bytes.slice(0, 8)); fail(); };
            }
          }
          return file;
        };
        syncBuiltinESMExports();
        let server;
        const execute = promisify(execFile);
        const push = () => execute("git", ["-C", client, "push", "--atomic", server.url + "/r", "HEAD:refs/heads/main", "HEAD:refs/heads/side"], { env: gitEnv, timeout: 15_000 });
        try {
          server = await serve({ root, allowAnonymousWrites: true });
          await assert.rejects(push(), error => {
            assert.doesNotMatch(error.stdout + error.stderr, /private disk-full fixture detail/);
            return true;
          });
          assert.ok(injected > 0, "fault must reach the actual persistence boundary");
          assert.equal(gitIn(repository)("rev-parse", "main").trim(), before);
          assert.equal(gitIn(repository)("rev-parse", "side").trim(), before);
          gitIn(repository)("fsck", "--strict");
          const remaining = await fs.readdir(repository, { recursive: true });
          assert.equal(remaining.some(name => name.endsWith(".tmp") || name.endsWith(".lock") || name === ".git-plus-ref-transaction.json"), false);
          await server.close({ force: true });
          server = undefined;
          broken = false;
          server = await serve({ root, allowAnonymousWrites: true });
          await push();
          assert.equal(gitIn(repository)("rev-parse", "main").trim(), candidate);
          assert.equal(gitIn(repository)("rev-parse", "side").trim(), candidate);
          assert.equal(gitIn(repository)("show", "main:file"), "after exhausted storage recovers");
          gitIn(repository)("fsck", "--strict");
          console.log("disk-full rejection and recovery verified");
        } finally {
          fs.open = original;
          syncBuiltinESMExports();
          await server?.close({ force: true });
          await fs.rm(root, { recursive: true, force: true });
        }
      `;
      const result = await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", script, fault],
        {
          timeout: 45_000,
          env: { ...process.env, GIT_HTTP_METRICS_INTERVAL_MS: "0" },
        },
      );
      assert.match(result.stdout, /disk-full rejection and recovery verified/);
      assert.doesNotMatch(result.stdout + result.stderr, /private disk-full fixture detail/);
      assert.match(result.stdout + result.stderr, /storage_failure/);
    });
  }
});
