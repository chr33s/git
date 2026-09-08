import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { ReadWriteNamespace } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Stream } from "effect";
import { fetchRepository } from "../client/Fetch.ts";
import * as History from "../git/History.ts";
import { stores as nodeStores } from "../git/Node.ts";
import { noPacks } from "../git/Packed.ts";
import * as GitRepository from "../git/Repository.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import { serve } from "../host/Node.ts";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { localMemory, localNode, RepoStores } from "./Namespace.ts";

const execute = promisify(execFile);
const namespace = {
  kind: "Cloudflare.Artifacts.Namespace",
  name: "REPOS",
  namespace: "test",
} as const;

const inspect = Effect.gen(function* () {
  const repository = yield* GitRepository.Repository;
  const tip = yield* repository.resolve("HEAD");
  assert.notEqual(tip, null);
  if (tip === null) throw new Error("fixture must have HEAD");
  const raw = yield* repository.readCommit(tip);
  const history = yield* repository.readHistoryCommit(tip);
  return {
    tip,
    rawParents: raw.parents.length,
    parents: history.parents.length,
    count: (yield* Stream.runCollect(repository.log(tip))).length,
    pathCount: (yield* Stream.runCollect(History.forPath(tip, "file.txt"))).length,
  };
});

describe.skipIf(!hasGit)("Artifacts shallow import", () => {
  for (const backend of ["node", "memory"] as const) {
    it.live(`preserves depth, raw commits and fork boundaries on ${backend}`, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-depth-"));
        const origin = path.join(root, "remote", "origin");
        await fs.mkdir(origin, { recursive: true });
        gitIn(origin)("init", "--bare", "-q", "-b", "main");
        fastImport(
          origin,
          [1, 2, 3]
            .map((mark) =>
              importCommit({
                branch: "refs/heads/main",
                mark,
                message: `commit ${mark}`,
                from: mark === 1 ? undefined : mark - 1,
                files: [{ path: "file.txt", content: `version ${mark}` }],
              }),
            )
            .join(""),
        );
        const server = await serve({ root: path.join(root, "remote") });
        try {
          const source = `${server.url}/origin`;
          const stock = path.join(root, "stock");
          await execute("git", ["clone", "--bare", "--depth", "1", source, stock], { env: gitEnv });
          const stockGit = gitIn(stock);
          assert.equal(stockGit("rev-list", "--count", "HEAD").trim(), "1");
          const local = path.join(root, "local");
          await Effect.runPromise(
            Effect.gen(function* () {
              const client = yield* (yield* ReadWriteNamespace)(namespace);
              const storage = yield* RepoStores;
              yield* client.import({
                source: { url: source, depth: 1 },
                target: { name: "shallow" },
              });
              yield* (yield* client.get("shallow")).fork("fork");
              const parent = yield* storage.open("shallow");
              const check = (name: string) =>
                Effect.gen(function* () {
                  const stores = yield* storage.open(name);
                  return yield* inspect.pipe(
                    Effect.provide(
                      GitRepository.layer.pipe(
                        Layer.provide(GitRepository.hooksNoop),
                        Layer.provide(Layer.succeed(ObjectStore, stores.objects)),
                        Layer.provide(Layer.succeed(RefStore, stores.refs)),
                        Layer.provide(noPacks),
                      ),
                    ),
                  );
                });
              assert.deepEqual(yield* check("shallow"), {
                tip: stockGit("rev-parse", "HEAD").trim(),
                rawParents: 1,
                parents: 0,
                count: 1,
                pathCount: 1,
              });
              for (const depth of [2, 2147483647]) {
                yield* fetchRepository({ url: source, branch: "main", depth, stores: parent });
                const viewed = yield* check("shallow");
                assert.equal(viewed.count, Math.min(depth, 3));
                assert.equal(viewed.pathCount, Math.min(depth, 3));
                // The fork can now read the parent's older objects through
                // alternates, but its own recorded history still stops at HEAD.
                assert.equal((yield* check("fork")).count, 1);
              }
              assert.equal((yield* parent.refs.shallow).size, 0);
            }).pipe(
              Effect.provide(
                Layer.merge(
                  backend === "node" ? localNode({ root: local }) : localMemory(),
                  RuntimeContext.phantom,
                ),
              ),
            ),
          );
          if (backend === "node") {
            const parent = gitIn(path.join(local, "shallow"));
            const child = gitIn(path.join(local, "fork"));
            assert.equal(parent("rev-list", "--count", "HEAD").trim(), "3");
            assert.equal(parent("rev-parse", "--is-shallow-repository").trim(), "false");
            assert.equal(child("rev-list", "--count", "HEAD").trim(), "1");
            assert.equal(child("rev-parse", "--is-shallow-repository").trim(), "true");
            const reopened = await Effect.runPromise(
              inspect.pipe(
                Effect.provide(
                  GitRepository.layer.pipe(
                    Layer.provide(GitRepository.hooksNoop),
                    Layer.provide(nodeStores(path.join(local, "fork"))),
                  ),
                ),
              ),
            );
            assert.equal(reopened.count, 1);
            const serving = await serve({ root: local });
            try {
              for (const version of [0, 2]) {
                const destination = path.join(root, `served-${version}`);
                await execute(
                  "git",
                  [
                    "-c",
                    `protocol.version=${version}`,
                    "clone",
                    "--bare",
                    `${serving.url}/fork`,
                    destination,
                  ],
                  { env: gitEnv },
                );
                assert.equal(gitIn(destination)("rev-list", "--count", "HEAD").trim(), "1");
                assert.equal(
                  gitIn(destination)("rev-parse", "--is-shallow-repository").trim(),
                  "true",
                );
              }
              await Effect.runPromise(
                Effect.gen(function* () {
                  const client = yield* (yield* ReadWriteNamespace)(namespace);
                  yield* client.import({
                    source: { url: `${serving.url}/fork` },
                    target: { name: "again" },
                  });
                }).pipe(
                  Effect.provide(
                    Layer.merge(
                      localNode({ root: path.join(root, "again") }),
                      RuntimeContext.phantom,
                    ),
                  ),
                ),
              );
              assert.equal(
                gitIn(path.join(root, "again", "again"))("rev-list", "--count", "HEAD").trim(),
                "1",
              );
            } finally {
              await serving.close();
            }
          }
        } finally {
          await server.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
