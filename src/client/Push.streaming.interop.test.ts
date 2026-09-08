import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Predicate, Stream } from "effect";
import { stores } from "../git/Node.ts";
import { FLUSH, pkt } from "../git/Pkt.ts";
import * as Repository from "../git/Repository.ts";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { push } from "./Push.ts";

describe.skipIf(!hasGit)("streamed pushes to stock Git", () => {
  it.live(
    "starts before pack completion, replays after an early challenge, and keeps the initial redirect",
    () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "push-streaming-"));
        const remote = path.join(root, "remote.git");
        const gate = Promise.withResolvers<void>();
        let posts = 0;
        let aliases = 0;
        const server = http.createServer(async (request, response) => {
          try {
            if (request.url?.startsWith("/alias")) {
              aliases++;
              response.writeHead(308, { location: request.url.replace("/alias", "/canonical") });
              response.end();
              return;
            }
            if (request.method === "GET") {
              response.setHeader("content-type", "application/x-git-receive-pack-advertisement");
              const advertisement = execFileSync(
                "git",
                ["receive-pack", "--stateless-rpc", "--advertise-refs", remote],
                { env: gitEnv },
              );
              response.end(
                Buffer.concat([pkt("# service=git-receive-pack\n"), FLUSH, advertisement]),
              );
              return;
            }
            posts++;
            if (posts === 1) {
              request.resume();
              response.writeHead(401, { "www-authenticate": 'Hub-SSH-v1 nonce="fixture"' });
              response.end();
              return;
            }
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            response.setHeader("content-type", "application/x-git-receive-pack-result");
            response.end(
              execFileSync("git", ["receive-pack", "--stateless-rpc", remote], {
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
          gitIn(root)("init", "--bare", "-q", "-b", "main");
          fastImport(
            root,
            importCommit({
              branch: "refs/heads/main",
              mark: 1,
              message: "streamed",
              files: [{ path: "a", content: "accepted by stock Git" }],
            }),
          );
          gitIn(root)("init", "--bare", "-q", "-b", "main", remote);
          await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
          const address = server.address();
          assert.ok(address !== null && !Predicate.isString(address));
          const url = `http://127.0.0.1:${address.port}/alias`;
          const repository = await Effect.runPromise(
            Repository.Repository.pipe(
              Effect.provide(
                Repository.layer.pipe(
                  Layer.provide(Repository.hooksNoop),
                  Layer.provide(stores(root)),
                ),
              ),
            ),
          );
          const gated = Repository.Repository.of({
            ...repository,
            packOids: (oids) =>
              repository.packOids(oids).pipe(Stream.tap(() => Effect.promise(() => gate.promise))),
          });
          let challenges = 0;
          const result = await Effect.runPromise(
            push({
              url,
              refs: [{ local: "refs/heads/main", remote: "refs/heads/main" }],
              authorize: async () => {
                challenges++;
                gate.resolve();
                return "fixture";
              },
            }).pipe(
              Effect.provideService(Repository.Repository, gated),
              Effect.timeout("10 seconds"),
            ),
          );
          assert.deepEqual(result, [{ ref: "refs/heads/main", ok: true }]);
          assert.equal(challenges, 1);
          assert.equal(posts, 2);
          assert.equal(aliases, 1, "the POST must use the redirected repository URL");
          assert.equal(gitIn(remote)("show", "main:a"), "accepted by stock Git");
          // Stock Git uses the initial redirect as its subsequent request base too.
          await promisify(execFile)("git", ["-C", root, "push", url, "main:refs/heads/stock"], {
            env: gitEnv,
          });
          assert.equal(aliases, 2);
          assert.equal(gitIn(remote)("show", "stock:a"), "accepted by stock Git");
        } finally {
          gate.resolve();
          server.closeAllConnections();
          await new Promise<void>((resolve) => server.close(() => resolve()));
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
  );
});
