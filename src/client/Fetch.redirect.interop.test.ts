import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Predicate } from "effect";
import { stores } from "../git/Node.ts";
import { FLUSH, pkt } from "../git/Pkt.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import * as Repository from "../git/Repository.ts";
import { fetchFrom } from "../server/Sync.ts";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { fetchRepository } from "./Fetch.ts";

describe.skipIf(!hasGit)("fetch discovery redirects", () => {
  for (const driver of ["client", "server"] as const) {
    it(`${driver} uses the final discovery URL for clone and incremental fetch`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "fetch-redirect-"));
      const remote = path.join(root, "remote.git");
      const local = path.join(root, "local.git");
      const requests: string[] = [];
      const server = http.createServer(async (request, response) => {
        try {
          requests.push(`${request.method} ${request.url}`);
          if (request.url === "/alias/info/refs?service=git-upload-pack") {
            response.writeHead(301, {
              location: "/canonical/info/refs?service=git-upload-pack",
            });
            response.end();
            return;
          }
          if (request.url === "/canonical/info/refs?service=git-upload-pack") {
            response.setHeader("content-type", "application/x-git-upload-pack-advertisement");
            response.end(
              Buffer.concat([
                pkt("# service=git-upload-pack\n"),
                FLUSH,
                execFileSync(
                  "git",
                  ["upload-pack", "--stateless-rpc", "--advertise-refs", remote],
                  { env: gitEnv },
                ),
              ]),
            );
            return;
          }
          if (request.url !== "/canonical/git-upload-pack") {
            response.writeHead(404);
            response.end();
            return;
          }
          const chunks: Buffer[] = [];
          for await (const chunk of request) chunks.push(Buffer.from(chunk));
          response.setHeader("content-type", "application/x-git-upload-pack-result");
          response.end(
            execFileSync("git", ["upload-pack", "--stateless-rpc", remote], {
              env: gitEnv,
              input: Buffer.concat(chunks),
            }),
          );
        } catch (error) {
          response.writeHead(500);
          response.end(String(error));
        }
      });
      try {
        gitIn(root)("init", "--bare", "-q", "-b", "main", remote);
        gitIn(root)("init", "--bare", "-q", "-b", "main", local);
        fastImport(
          remote,
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: "first",
            files: [{ path: "a", content: "first" }],
          }),
        );
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const url = `http://127.0.0.1:${address.port}/alias`;
        await promisify(execFile)("git", ["clone", "--bare", url, path.join(root, "stock.git")], {
          env: gitEnv,
        });
        assert.equal(
          gitIn(path.join(root, "stock.git"))("rev-parse", "main"),
          gitIn(remote)("rev-parse", "main"),
        );
        requests.length = 0;
        const target = driver === "client" ? "main" : "refs/remotes/origin/main";
        const fetch = () =>
          driver === "server"
            ? Effect.runPromise(
                fetchFrom({
                  url,
                  remote: "origin",
                  credential: null,
                }).pipe(
                  Effect.provide(
                    Repository.layer.pipe(
                      Layer.provide(Repository.hooksNoop),
                      Layer.provide(stores(local)),
                    ),
                  ),
                ),
              )
            : Effect.runPromise(
                Effect.gen(function* () {
                  const objects = yield* ObjectStore;
                  const refs = yield* RefStore;
                  return yield* fetchRepository({ url, stores: { objects, refs } });
                }).pipe(Effect.provide(stores(local))),
              );
        await fetch();
        assert.equal(gitIn(local)("rev-parse", target), gitIn(remote)("rev-parse", "main"));
        const next = gitIn(remote)(
          "commit-tree",
          "main^{tree}",
          "-p",
          "main",
          "-m",
          "second",
        ).trim();
        gitIn(remote)("update-ref", "refs/heads/main", next);
        await fetch();
        assert.equal(gitIn(local)("rev-parse", target).trim(), next);
        assert.equal(gitIn(local)("show", `${target}:a`), "first");
        const posts = requests.filter((request) => request.startsWith("POST "));
        assert.ok(posts.length >= (driver === "client" ? 3 : 2));
        assert.ok(posts.every((request) => request === "POST /canonical/git-upload-pack"));
      } finally {
        server.closeAllConnections();
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
