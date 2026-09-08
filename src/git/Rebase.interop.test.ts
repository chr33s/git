import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { stores } from "./Node.ts";
import * as Repository from "./Repository.ts";
import { rebase } from "./Rebase.ts";

describe.skipIf(!hasGit)("rebase behavior against Git", () => {
  let root: string;
  const git = (...args: string[]) => gitIn(root)(...args).trim();
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "rebase-review-"));
    git("init", "-q", "-b", "main");
  });
  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const replay = () =>
    Effect.runPromise(
      rebase({
        branch: "refs/heads/native",
        onto: "refs/heads/main",
        into: "refs/heads/native",
      }).pipe(
        Effect.provide(
          Repository.layer.pipe(
            Layer.provide(Repository.hooksNoop),
            Layer.provide(stores(path.join(root, ".git"))),
          ),
        ),
      ),
    );

  it.live("flattens merges by replaying the side branch's individual commits", () =>
    Effect.promise(async () => {
      fastImport(
        root,
        [
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: "base",
            files: [{ path: "base", content: "base\n" }],
          }),
          importCommit({
            branch: "refs/heads/topic",
            mark: 2,
            from: 1,
            message: "topic change",
            files: [{ path: "topic", content: "topic\n" }],
          }),
          importCommit({
            branch: "refs/heads/side",
            mark: 3,
            from: 1,
            message: "side one",
            files: [{ path: "side", content: "one\n" }],
          }),
          importCommit({
            branch: "refs/heads/side",
            mark: 4,
            from: 3,
            message: "side two",
            files: [{ path: "side", content: "two\n" }],
          }),
          importCommit({
            branch: "refs/heads/topic",
            mark: 5,
            from: 2,
            merge: 4,
            message: "merge side",
            files: [{ path: "side", content: "two\n" }],
          }),
          importCommit({
            branch: "refs/heads/main",
            mark: 6,
            from: 1,
            message: "main change",
            files: [{ path: "main", content: "main\n" }],
          }),
        ].join(""),
      );
      git("branch", "native", "topic");
      git("checkout", "-q", "topic");
      git("rebase", "main");
      const outcome = await replay();
      assert.equal(outcome.kind, "replayed");
      assert.equal(git("rev-parse", "native^{tree}"), git("rev-parse", "topic^{tree}"));
      const messages = (branch: string) =>
        git("log", "--format=%s", `main..${branch}`).split("\n").sort();
      assert.deepEqual(messages("native"), messages("topic"));
      assert.equal(git("rev-list", "--merges", "main..native"), "");
    }),
  );

  it.live("advances the destination when the branch is already contained in the target", () =>
    Effect.promise(async () => {
      fastImport(
        root,
        [
          importCommit({
            branch: "refs/heads/topic",
            mark: 1,
            message: "base",
            files: [{ path: "base", content: "base\n" }],
          }),
          importCommit({
            branch: "refs/heads/main",
            mark: 2,
            from: 1,
            message: "main change",
            files: [{ path: "main", content: "main\n" }],
          }),
        ].join(""),
      );
      git("branch", "native", "topic");
      git("checkout", "-q", "topic");
      git("rebase", "main");
      const outcome = await replay();
      assert.equal(outcome.head, git("rev-parse", "topic"));
      assert.equal(git("rev-parse", "native"), git("rev-parse", "topic"));
    }),
  );

  it.live("moves the destination when every replayed change is already upstream", () =>
    Effect.promise(async () => {
      fastImport(
        root,
        [
          importCommit({
            branch: "refs/heads/topic",
            mark: 1,
            message: "base",
            files: [{ path: "base", content: "base\n" }],
          }),
          importCommit({
            branch: "refs/heads/topic",
            mark: 2,
            from: 1,
            message: "topic change",
            files: [{ path: "feature", content: "feature\n" }],
          }),
          importCommit({
            branch: "refs/heads/main",
            mark: 3,
            from: 1,
            message: "same change upstream",
            files: [{ path: "feature", content: "feature\n" }],
          }),
          importCommit({
            branch: "refs/heads/main",
            mark: 4,
            from: 3,
            message: "main change",
            files: [{ path: "main", content: "main\n" }],
          }),
        ].join(""),
      );
      git("branch", "native", "topic");
      git("checkout", "-q", "topic");
      git("rebase", "main");
      const outcome = await replay();
      assert.equal(outcome.head, git("rev-parse", "topic"));
      assert.equal(git("rev-parse", "native"), git("rev-parse", "topic"));
      assert.equal(outcome.commits[0]?.replayed, null);
    }),
  );
});
