import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { ReadWriteNamespace } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as Repository from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import { fastImport, gitEnv, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { localNode } from "./Namespace.ts";

const execute = promisify(execFile);

describe.skipIf(!hasGit)("Artifacts import branch selection", () => {
  it.live("imports the remote default branch unless another branch is requested", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-import-branch-"));
      const origin = path.join(root, "remote", "origin");
      await fs.mkdir(origin, { recursive: true });
      const git = gitIn(origin);
      git("init", "--bare", "-q", "-b", "trunk");
      fastImport(
        origin,
        importCommit({ branch: "refs/heads/trunk", mark: 1, message: "base", files: [] }),
      );
      git("branch", "side", "trunk");
      git("tag", "v1", "trunk");
      const server = await serve({ root: path.join(root, "remote") });
      try {
        const source = `${server.url}/origin`;
        const local = path.join(root, "local");
        await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* (yield* ReadWriteNamespace)({
              kind: "Cloudflare.Artifacts.Namespace",
              name: "REPOS",
              namespace: "test",
            });
            for (const branch of [undefined, "side"]) {
              const name = branch ?? "default";
              const imported = yield* client.import({
                source: { url: source, branch },
                target: { name },
              });
              const expected = branch ?? "trunk";
              assert.equal(imported.defaultBranch, expected);
              const destination = gitIn(path.join(local, name));
              assert.equal(destination("symbolic-ref", "HEAD").trim(), `refs/heads/${expected}`);
              assert.deepEqual(
                destination("for-each-ref", "--format=%(refname)", "refs/heads/")
                  .trim()
                  .split("\n"),
                [`refs/heads/${expected}`],
              );
            }
          }).pipe(Effect.provide(Layer.merge(localNode({ root: local }), RuntimeContext.phantom))),
        );
        const stock = path.join(root, "stock");
        await execute("git", ["clone", "--bare", "--single-branch", "--no-tags", source, stock], {
          env: gitEnv,
        });
        assert.equal(
          gitIn(stock)("for-each-ref", "--format=%(refname)").trim(),
          "refs/heads/trunk",
        );
        assert.equal(
          gitIn(stock)("rev-parse", "trunk").trim(),
          gitIn(path.join(local, "default"))("rev-parse", "trunk").trim(),
        );

        // Other branches must not replace an unborn default branch. A
        // single-branch stock clone is empty in this case as well.
        git("symbolic-ref", "HEAD", "refs/heads/unborn");
        await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* (yield* ReadWriteNamespace)({
              kind: "Cloudflare.Artifacts.Namespace",
              name: "REPOS",
              namespace: "test",
            });
            const imported = yield* client.import({
              source: { url: source },
              target: { name: "unborn" },
            });
            assert.equal(imported.defaultBranch, "unborn");
          }).pipe(Effect.provide(Layer.merge(localNode({ root: local }), RuntimeContext.phantom))),
        );
        const nativeUnborn = gitIn(path.join(local, "unborn"));
        assert.equal(nativeUnborn("rev-parse", "--is-bare-repository").trim(), "true");
        assert.equal(nativeUnborn("symbolic-ref", "HEAD").trim(), "refs/heads/unborn");
        assert.equal(nativeUnborn("for-each-ref", "--format=%(refname)").trim(), "");
        const stockUnborn = path.join(root, "stock-unborn");
        await execute(
          "git",
          ["clone", "--bare", "--single-branch", "--no-tags", source, stockUnborn],
          { env: gitEnv },
        );
        assert.equal(gitIn(stockUnborn)("for-each-ref", "--format=%(refname)").trim(), "");
      } finally {
        await server.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});

describe.skipIf(!hasGit)("Artifacts remote URLs", () => {
  it.live("creates and forks empty repositories that stock Git can open directly", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-empty-"));
      try {
        await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* (yield* ReadWriteNamespace)({
              kind: "Cloudflare.Artifacts.Namespace",
              name: "REPOS",
              namespace: "test",
            });
            yield* client.create("parent", { setDefaultBranch: "trunk" });
            yield* (yield* client.get("parent")).fork("child");
          }).pipe(Effect.provide(Layer.merge(localNode({ root }), RuntimeContext.phantom))),
        );
        for (const name of ["parent", "child"]) {
          const git = gitIn(path.join(root, name));
          assert.equal(git("rev-parse", "--is-bare-repository").trim(), "true");
          assert.equal(git("symbolic-ref", "HEAD").trim(), "refs/heads/trunk");
          assert.equal(git("for-each-ref", "--format=%(refname)").trim(), "");
        }
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  it.live("Git reaches the named repository when its name ends in .git", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "artifacts-names-"));
      const server = await serve({ root, allowAnonymousWrites: true });
      try {
        const records = await Effect.runPromise(
          Effect.gen(function* () {
            const bind = yield* ReadWriteNamespace;
            const client = yield* bind({
              kind: "Cloudflare.Artifacts.Namespace",
              name: "REPOS",
              namespace: "test",
            });
            return yield* Effect.forEach(["project", "project.git", "project.git.git"], (name) =>
              Effect.gen(function* () {
                const created = yield* client.create(name);
                const head = yield* Effect.gen(function* () {
                  const repository = yield* Repository.Repository;
                  return yield* repository.commit({
                    branch: "main",
                    tree: EMPTY_TREE_OID,
                    message: name,
                    author: { name: "T", email: "t@e.com", at: new Date(1700000000000), offset: 0 },
                  });
                }).pipe(
                  Effect.provide(
                    Repository.layer.pipe(
                      Layer.provide(Repository.hooksNoop),
                      Layer.provide(stores(path.join(root, name))),
                    ),
                  ),
                );
                return { name, head, remote: created.remote };
              }),
            );
          }).pipe(
            Effect.provide(
              Layer.merge(localNode({ root, remoteBase: server.url }), RuntimeContext.phantom),
            ),
          ),
        );
        for (const record of records) {
          const { stdout } = await execute("git", ["ls-remote", record.remote, "refs/heads/main"], {
            env: gitEnv,
          });
          assert.equal(stdout.trim().split(/\s+/)[0], record.head, record.name);
        }
      } finally {
        await server.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
