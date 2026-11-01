import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Predicate } from "effect";
import { serve } from "../host/Node.ts";
import { gitEnv, hasGit } from "../testing/Git.ts";

const execute = promisify(execFile);
const git = async (cwd: string, ...args: string[]) =>
  (await execute("git", args, { cwd, env: gitEnv, encoding: "utf8" })).stdout.trim();

describe.skipIf(!hasGit)("best-effort delivery recovery", () => {
  it.live("reconciles missed updates and deletions after outage and host restart", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(join(tmpdir(), "git-delivery-recovery-"));
      const storage = join(root, "storage");
      const work = join(root, "work");
      const replica = join(root, "replica.git");
      const exhausted = Promise.withResolvers<void>();
      let attempts = 0;
      const receiver = createServer((request, response) => {
        request.resume();
        response.writeHead(503);
        response.end();
        if (++attempts === 4) exhausted.resolve();
      });
      await new Promise<void>((resolve) => receiver.listen(0, "127.0.0.1", resolve));
      const address = receiver.address();
      assert.ok(address !== null && !Predicate.isString(address));
      let host = await serve({
        root: storage,
        allowAnonymousWrites: true,
        allowLoopbackEgress: true,
      });
      try {
        await fs.mkdir(work);
        await git(work, "init", "-q", "-b", "main");
        await fs.writeFile(join(work, "file"), "before outage\n");
        await git(work, "add", ".");
        await git(work, "commit", "-qm", "before");
        await git(work, "branch", "obsolete");
        await git(work, "push", `${host.url}/source`, "main", "obsolete");
        await git(root, "clone", "--mirror", `${host.url}/source`, replica);
        const before = await git(replica, "rev-parse", "main");

        const registered = await fetch(`${host.url}/source/webhooks`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            url: `http://127.0.0.1:${address.port}/hook`,
            secret: "fixture-recovery-secret",
          }),
        });
        assert.equal(registered.status, 200, await registered.text());
        await fs.writeFile(join(work, "file"), "accepted during outage\n");
        await git(work, "commit", "-qam", "during outage");
        const accepted = await git(work, "rev-parse", "HEAD");
        await git(work, "push", "--atomic", `${host.url}/source`, "main", ":obsolete");
        await exhausted.promise;
        assert.equal(await git(replica, "rev-parse", "main"), before);
        assert.notEqual(await git(replica, "for-each-ref", "refs/heads/obsolete"), "");

        await host.close();
        host = await serve({
          root: storage,
          allowAnonymousWrites: true,
          allowLoopbackEgress: true,
        });
        // A restarted sender has no durable event queue. Reconcile the receiver
        // from authoritative refs, including removals, instead of replaying stale events.
        await git(
          replica,
          "fetch",
          "--prune",
          `${host.url}/source`,
          "+refs/heads/*:refs/heads/*",
          "+refs/tags/*:refs/tags/*",
        );
        assert.equal(await git(replica, "rev-parse", "main"), accepted);
        assert.equal(await git(replica, "for-each-ref", "refs/heads/obsolete"), "");
        assert.equal(await git(replica, "show", "main:file"), "accepted during outage");
        await git(replica, "fsck", "--strict");
      } finally {
        await host.close();
        receiver.closeAllConnections();
        await new Promise<void>((resolve) => receiver.close(() => resolve()));
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
