import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { serve } from "./Node.ts";

describe.skipIf(!hasGit)("Node repository creation", () => {
  it.live("creates Git-readable repositories on their first JSON commit and push", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "node-created-"));
      const repositories = path.join(root, "repos");
      const server = await serve({ root: repositories, allowAnonymousWrites: true });
      try {
        const read = await fetch(`${server.url}/read-only/refs`);
        assert.equal(read.status, 200);
        await read.arrayBuffer();
        await assert.rejects(fs.stat(path.join(repositories, "read-only")), { code: "ENOENT" });

        const committed = await fetch(`${server.url}/json/commit`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            branch: "main",
            message: "JSON first commit",
            author: {
              name: "T",
              email: "t@e.com",
              at: new Date(1700000000000).toISOString(),
              offset: 0,
            },
            files: [{ path: "hello.txt", content: "hello\n" }],
          }),
        });
        assert.equal(committed.status, 200, await committed.text());

        const source = path.join(root, "source");
        await fs.mkdir(source);
        gitIn(source)("init", "--bare", "-q", "-b", "main");
        fastImport(
          source,
          importCommit({ branch: "refs/heads/main", mark: 1, message: "pushed", files: [] }),
        );
        await promisify(execFile)("git", ["push", `${server.url}/pushed`, "main"], {
          cwd: source,
          env: gitEnv,
        });

        for (const [name, subject] of [
          ["json", "JSON first commit"],
          ["pushed", "pushed"],
        ] as const) {
          const git = gitIn(path.join(repositories, name));
          assert.equal(git("rev-parse", "--is-bare-repository").trim(), "true", name);
          assert.equal(git("symbolic-ref", "HEAD").trim(), "refs/heads/main", name);
          assert.equal(git("log", "-1", "--format=%s", "main").trim(), subject);
        }
      } finally {
        await server.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
