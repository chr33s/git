import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { fetchFrom } from "./Sync.ts";

const repositoryAt = (directory: string) =>
  Effect.runPromise(
    Repository.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(directory)),
        ),
      ),
    ),
  );

describe.skipIf(!hasGit)("server fetch ref leases", () => {
  it.live("deepens unchanged tracking tips and persists boundaries for Git", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "sync-shallow-"));
      const source = path.join(root, "source");
      const targetPath = path.join(root, "target");
      await fs.mkdir(source);
      await fs.mkdir(targetPath);
      gitIn(source)("init", "--bare", "-q", "-b", "main");
      gitIn(targetPath)("init", "--bare", "-q", "-b", "main");
      fastImport(
        source,
        [1, 2, 3]
          .map((mark) =>
            importCommit({
              branch: "refs/heads/main",
              mark,
              message: String(mark),
              from: mark === 1 ? undefined : mark - 1,
              files: [],
            }),
          )
          .join(""),
      );
      const server = await serve({ root });
      try {
        const target = await repositoryAt(targetPath);
        for (const depth of [1, 2, 2147483647]) {
          await Effect.runPromise(
            fetchFrom({ remote: "up", url: `${server.url}/source`, credential: null, depth }).pipe(
              Effect.provideService(Repository, target),
            ),
          );
          assert.equal(
            gitIn(targetPath)("rev-list", "--count", "refs/remotes/up/main").trim(),
            String(Math.min(depth, 3)),
          );
        }
        assert.equal(gitIn(targetPath)("rev-parse", "--is-shallow-repository").trim(), "false");
      } finally {
        await server.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
  for (const tag of [false, true]) {
    it.live(
      `preserves a ${tag ? "tag created" : "tracking ref moved"} after the fetch snapshot`,
      () =>
        Effect.promise(async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "sync-ref-race-"));
          const server = await serve({ root, allowAnonymousWrites: true });
          try {
            const source = await repositoryAt(path.join(root, "source"));
            const target = await repositoryAt(path.join(root, "target"));
            const author = { name: "T", email: "t@e.com", at: new Date(1700000000000), offset: 0 };
            const remote = await Effect.runPromise(
              source.commit({
                branch: "main",
                tree: EMPTY_TREE_OID,
                message: "upstream",
                author,
              }),
            );
            if (tag) await Effect.runPromise(source.setRef({ name: "refs/tags/v1", to: remote }));
            const before = await Effect.runPromise(
              target.commit({
                branch: "main",
                tree: EMPTY_TREE_OID,
                message: "local",
                author,
              }),
            );
            await Effect.runPromise(target.setHead("refs/heads/main"));
            const arrived = await Effect.runPromise(
              target.commitTree({
                tree: EMPTY_TREE_OID,
                parents: [before],
                message: "concurrent",
                author,
              }),
            );
            const ref = tag ? "refs/tags/v1" : "refs/remotes/up/main";
            if (!tag) await Effect.runPromise(target.setRef({ name: ref, to: before }));
            let pending = true;
            const raced = Repository.of({
              ...target,
              contains: Effect.fn("test.publishAfterFetchSnapshot")(function* (oid) {
                if (pending) {
                  pending = false;
                  yield* target.setRef({ name: ref, to: arrived }).pipe(Effect.orDie);
                }
                return yield* target.contains(oid);
              }),
            });
            const result = await Effect.runPromise(
              fetchFrom({
                remote: "up",
                url: `${server.url}/source`,
                credential: null,
                refs: [tag ? "refs/tags/v1" : "refs/heads/main"],
              }).pipe(Effect.provideService(Repository, raced), Effect.result),
            );
            assert.equal(gitIn(path.join(root, "target"))("rev-parse", ref).trim(), arrived);
            assert.equal(result._tag, "Failure");
            if (result._tag === "Failure") assert.equal(result.failure._tag, "RefConflict");
          } finally {
            await server.close();
            await fs.rm(root, { recursive: true, force: true });
          }
        }),
    );
  }
});
