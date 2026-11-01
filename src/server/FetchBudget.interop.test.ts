import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { hasGit } from "../testing/Git.ts";

describe.skipIf(!hasGit)("HTTP fetch graph limits", () => {
  it("refuses a full clone but permits a bounded shallow clone and later requests", async () => {
    const script = `
      import assert from "node:assert/strict";
      import * as fs from "node:fs/promises";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { execFile } from "node:child_process";
      import { promisify } from "node:util";
      import { serve } from ${JSON.stringify(new URL("../host/Node.ts", import.meta.url).href)};
      import { gitIn, gitEnv, fastImport, importCommit } from ${JSON.stringify(new URL("../testing/Git.ts", import.meta.url).href)};
      const root = await fs.mkdtemp(join(tmpdir(), "fetch-budget-"));
      const repository = join(root, "r");
      await fs.mkdir(repository);
      gitIn(repository)("init", "--bare", "-q", "-b", "main");
      fastImport(repository, Array.from({ length: 8 }, (_, index) => importCommit({
        branch: "refs/heads/main", mark: index + 1,
        ...(index === 0 ? {} : { from: index }),
        message: "commit " + index, files: [],
      })).join(""));
      const tip = gitIn(repository)("rev-parse", "main").trim();
      // Core callers may reference the virtual empty tree without storing it.
      // The receiving Git must still get its canonical object in the pack.
      await fs.unlink(join(repository, "objects", "4b", "825dc642cb6eb9a060e54bf8d69288fbee4904"));
      const server = await serve({ root });
      const execute = promisify(execFile);
      const git = (...args) => execute("git", args, { env: gitEnv, timeout: 10_000 });
      try {
        await assert.rejects(git("clone", "-q", server.url + "/r", join(root, "full")), /413/);
        await git("clone", "-q", "--depth=1", server.url + "/r", join(root, "shallow"));
        assert.equal(gitIn(join(root, "shallow"))("rev-parse", "HEAD").trim(), tip);
        assert.equal(gitIn(join(root, "shallow"))("rev-list", "--count", "HEAD").trim(), "1");
        gitIn(join(root, "shallow"))("fsck", "--strict");
        // A shallow source clone still computes redaction exclusions from hub
        // histories. That work must share the same allowance, even for refs
        // the client did not request.
        gitIn(repository)("update-ref", "refs/hub/pr/budget", tip);
        await assert.rejects(git("clone", "-q", "--depth=1", server.url + "/r", join(root, "hub-refused")), /413/);
        assert.equal(gitIn(repository)("rev-parse", "refs/hub/pr/budget").trim(), tip);
        gitIn(repository)("update-ref", "-d", "refs/hub/pr/budget");
        await git("clone", "-q", "--depth=1", server.url + "/r", join(root, "recovered"));
        gitIn(join(root, "recovered"))("fsck", "--strict");
        assert.equal(gitIn(repository)("rev-parse", "main").trim(), tip);
        const response = await fetch(server.url + "/r/refs");
        assert.equal(response.status, 200);
        await response.arrayBuffer();
        console.log("fetch budget verified");
      } finally {
        await server.close({ force: true });
        await fs.rm(root, { recursive: true, force: true });
      }
    `;
    const result = await promisify(execFile)(
      process.execPath,
      ["--input-type=module", "-e", script],
      {
        timeout: 30_000,
        env: { ...process.env, GIT_FETCH_GRAPH_STEPS: "12", GIT_HTTP_METRICS_INTERVAL_MS: "0" },
      },
    );
    assert.match(result.stdout, /fetch budget verified/);
  });
});
