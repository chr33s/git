/**
 * Replay tests.
 *
 * The one that carries the design is "leaves the file it did not touch alone":
 * a cherry-pick based on `onto` instead of the commit's parent still produces
 * both changes in the easy cases and only reverts the target's work in this
 * one, so it is the assertion that tells the two implementations apart.
 *
 * A rebased history is also read back with the real `git` binary, because
 * "the parent chain is what we meant" is a claim about git's data model, not
 * about ours.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import type { Signature } from "./Format.ts";
import { stores } from "./Memory.ts";
import { stores as nodeStores } from "./Node.ts";
import { hasGit } from "../testing/Git.ts";
import { cherryPick, rebase } from "./Rebase.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import { type Oid, RefStore } from "./Store.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const bob: Signature = {
  name: "Bob",
  email: "bob@example.com",
  at: new Date(1_700_000_100_000),
  offset: 0,
};

/**
 * `provideMerge` so the test and `Repository` share one store instance, rather
 * than relying on layer memoization to make that true.
 */
const memory = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const disk = (root: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provideMerge(nodeStores(root)),
  );

/** Each test gets its own stores, so there is no shared global state to reset. */
const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | RefStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(memory)));

/** The same, backed by a directory `git` itself can be pointed at. */
const onDisk = <A, E>(root: string, effect: Effect.Effect<A, E, Repository | RefStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(disk(root))));

/** A commit on `branch` with `files` applied to whatever is there now. */
const commitOn = (input: {
  readonly branch: string;
  readonly message: string;
  readonly files: Readonly<Record<string, string | null>>;
  readonly author?: Signature;
}) =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const head = yield* repository.resolve(`refs/heads/${input.branch}`);
    const base = head === null ? null : (yield* repository.readCommit(head)).tree;

    const changes = Object.entries(input.files).map(([file, content]) => ({
      path: file,
      content: content === null ? null : encoder.encode(content),
    }));
    const tree = yield* base === null
      ? repository.writeFiles({ changes })
      : repository.writeFiles({ base, changes });

    return yield* repository.commit({
      branch: input.branch,
      tree,
      message: input.message,
      author: input.author ?? alice,
    });
  });

const fileAt = (commit: Oid, file: string) =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const { tree } = yield* repository.readCommit(commit);
    const entry = yield* repository.findPath(tree, file);
    return entry === null ? null : decoder.decode(yield* repository.readBlob(entry.oid));
  });

const messagesFrom = (commit: Oid) =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const commits = yield* Stream.runCollect(repository.log(commit));
    return commits.map((entry) => entry.message);
  });

/**
 * `main` forks at "root", then each side moves: `main` edits `a.txt`, `topic`
 * gains `earlier` and then the commit under test. Two files and two commits on
 * the branch, because that is the smallest shape in which the two wrong bases
 * — `onto`, and the merge base — both show up as a wrong tree.
 */
const forked = (topic: {
  readonly file: string;
  readonly content: string;
  readonly earlier?: Readonly<Record<string, string>>;
}) =>
  Effect.gen(function* () {
    const repository = yield* Repository;

    yield* commitOn({
      branch: "main",
      message: "root",
      files: { "a.txt": "alpha\n", "b.txt": "beta\n" },
    });
    yield* repository.branch({ name: "topic", base: "refs/heads/main" });

    const onMain = yield* commitOn({
      branch: "main",
      message: "main edits a",
      files: { "a.txt": "alpha, edited by main\n" },
    });

    if (topic.earlier !== undefined) {
      yield* commitOn({ branch: "topic", message: "topic, earlier", files: topic.earlier });
    }
    const onTopic = yield* commitOn({
      branch: "topic",
      message: "topic edits it",
      files: { [topic.file]: topic.content },
    });

    return { onMain, onTopic };
  });

describe("cherryPick", () => {
  it("carries the commit's change across and leaves the file it did not touch alone", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const { onMain, onTopic } = yield* forked({
          file: "b.txt",
          content: "beta, edited by topic\n",
          earlier: { "c.txt": "gamma\n" },
        });

        const outcome = yield* cherryPick({
          commit: "refs/heads/topic",
          onto: "refs/heads/main",
          author: bob,
          into: "refs/heads/main",
        });

        const head = outcome.head;
        assert.notEqual(head, null, "a clean pick produces a commit");
        return {
          a: yield* fileAt(head!, "a.txt"),
          b: yield* fileAt(head!, "b.txt"),
          c: yield* fileAt(head!, "c.txt"),
          main: yield* repository.resolve("refs/heads/main"),
          onMain,
          onTopic,
          outcome,
          picked: yield* repository.readCommit(head!),
        };
      }),
    );

    assert.equal(result.outcome.kind, "replayed");
    assert.deepEqual(
      result.outcome.commits.map((entry) => entry.original),
      [result.onTopic],
    );
    assert.equal(result.outcome.commits[0]?.replayed, result.outcome.head);

    assert.equal(result.b, "beta, edited by topic\n", "the picked change is applied");
    // The commit never mentions a.txt. Based on `onto` rather than the
    // commit's parent, the pick would resolve it back to the fork point.
    assert.equal(result.a, "alpha, edited by main\n", "the untouched file must not be reverted");
    // Its parent added c.txt. Based on the merge base rather than the parent,
    // the pick would drag the rest of the branch along with it.
    assert.equal(result.c, null, "only the picked commit's own change is applied");

    assert.deepEqual(result.picked.parents, [result.onMain]);
    assert.equal(result.picked.message, "topic edits it");
    assert.equal(result.picked.author.email, "alice@example.com", "authorship travels along");
    assert.equal(result.picked.committer.email, "bob@example.com", "the picker committed it");
    assert.equal(result.main, result.outcome.head, "`into` moved to the new commit");
  });

  it("reports a conflicting pick, commits nothing and leaves the ref where it was", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const { onMain, onTopic } = yield* forked({
          file: "a.txt",
          content: "alpha, edited by topic\n",
        });

        const outcome = yield* cherryPick({
          commit: "refs/heads/topic",
          onto: "refs/heads/main",
          into: "refs/heads/main",
        });

        return {
          a: yield* fileAt(onMain, "a.txt"),
          main: yield* repository.resolve("refs/heads/main"),
          onMain,
          onTopic,
          outcome,
        };
      }),
    );

    assert.equal(result.outcome.kind, "conflicted");
    assert.equal(result.outcome.head, null);
    assert.equal(result.outcome.commits.length, 1);
    assert.equal(result.outcome.commits[0]?.original, result.onTopic);
    assert.equal(result.outcome.commits[0]?.replayed, null, "no commit was written");
    assert.deepEqual(result.outcome.commits[0]?.conflicts, [{ path: "a.txt", reason: "content" }]);

    assert.equal(result.main, result.onMain, "the ref must not move on a conflict");
    assert.equal(result.a, "alpha, edited by main\n");
  });
});

/**
 * `main` forks at "first", moves on, and `topic` gains three commits — the
 * shape every rebase assertion below is about.
 */
const threeCommitBranch = Effect.gen(function* () {
  const repository = yield* Repository;

  yield* commitOn({ branch: "main", message: "first", files: { "a.txt": "alpha\n" } });
  yield* repository.branch({ name: "topic", base: "refs/heads/main" });

  const onMain = yield* commitOn({
    branch: "main",
    message: "second on main",
    files: { "main.txt": "from main\n" },
  });

  const one = yield* commitOn({ branch: "topic", message: "one", files: { "one.txt": "1\n" } });
  const two = yield* commitOn({
    branch: "topic",
    message: "two",
    files: { "two.txt": "2\n" },
    author: bob,
  });
  const three = yield* commitOn({
    branch: "topic",
    message: "three",
    files: { "three.txt": "3\n" },
  });

  // git's ORIG_HEAD by another name: what the branch pointed at before.
  yield* repository.branch({ name: "backup", base: "refs/heads/topic" });

  return { onMain, one, three, two };
});

describe("rebase", () => {
  it("replays a branch in order, preserving message and author, and leaves the originals", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const built = yield* threeCommitBranch;

        const outcome = yield* rebase({
          branch: "refs/heads/topic",
          onto: "refs/heads/main",
          into: "refs/heads/topic",
        });

        const replayed = outcome.commits.map((entry) => entry.replayed);
        return {
          built,
          history: yield* messagesFrom(outcome.head!),
          original: yield* messagesFrom(built.three),
          outcome,
          parents: yield* Effect.forEach(replayed, (oid) =>
            repository.readCommit(oid!).pipe(Effect.map((commit) => commit.parents)),
          ),
          topic: yield* repository.resolve("refs/heads/topic"),
          two: yield* repository.readCommit(replayed[1]!),
        };
      }),
    );

    assert.equal(result.outcome.kind, "replayed");
    assert.deepEqual(
      result.outcome.commits.map((entry) => entry.original),
      [result.built.one, result.built.two, result.built.three],
    );
    assert.ok(
      result.outcome.commits.every((entry) => entry.replayed !== null),
      "every commit was replayed",
    );

    assert.deepEqual(result.history, ["three", "two", "one", "second on main", "first"]);

    // Each replay's parent is the one before it, and the first sits on `onto`.
    const replayed = result.outcome.commits.map((entry) => entry.replayed);
    assert.deepEqual(result.parents, [[result.built.onMain], [replayed[0]], [replayed[1]]]);

    assert.equal(result.two.author.email, "bob@example.com", "the author is preserved");
    assert.equal(result.two.message, "two");
    assert.notEqual(replayed[1], result.built.two, "a replay is a new commit, not the old one");

    assert.equal(result.topic, result.outcome.head, "`into` moved");
    assert.deepEqual(
      result.original,
      ["three", "two", "one", "first"],
      "the originals are still reachable from the ref that kept them",
    );
  });

  it("stops at the first conflict, keeping what it already replayed", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;

        yield* commitOn({
          branch: "main",
          message: "first",
          files: { "a.txt": "alpha\n", "b.txt": "beta\n" },
        });
        yield* repository.branch({ name: "topic", base: "refs/heads/main" });

        const onMain = yield* commitOn({
          branch: "main",
          message: "main edits b",
          files: { "b.txt": "beta, from main\n" },
        });

        const one = yield* commitOn({
          branch: "topic",
          message: "one",
          files: { "c.txt": "gamma\n" },
        });
        const two = yield* commitOn({
          branch: "topic",
          message: "two",
          files: { "b.txt": "beta, from topic\n" },
        });
        const three = yield* commitOn({
          branch: "topic",
          message: "three",
          files: { "d.txt": "delta\n" },
        });

        const outcome = yield* rebase({
          branch: "refs/heads/topic",
          onto: "refs/heads/main",
          into: "refs/heads/topic",
        });

        return {
          one,
          onMain,
          outcome,
          three,
          topic: yield* repository.resolve("refs/heads/topic"),
          two,
        };
      }),
    );

    assert.equal(result.outcome.kind, "conflicted");
    assert.equal(result.outcome.head, null);

    assert.equal(result.outcome.commits.length, 2, "the third commit was never attempted");
    assert.equal(result.outcome.commits[0]?.original, result.one);
    assert.notEqual(result.outcome.commits[0]?.replayed, null, "the clean one was replayed");
    assert.deepEqual(result.outcome.commits[0]?.conflicts, []);

    assert.equal(result.outcome.commits[1]?.original, result.two);
    assert.equal(result.outcome.commits[1]?.replayed, null);
    assert.deepEqual(result.outcome.commits[1]?.conflicts, [{ path: "b.txt", reason: "content" }]);

    assert.equal(result.topic, result.three, "the branch stays where it was");
  });

  it("skips a commit `onto` already has, and one whose change is already there", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;

        const first = yield* commitOn({
          branch: "main",
          message: "first",
          files: { "a.txt": "alpha\n" },
        });
        yield* repository.branch({ name: "topic", base: "refs/heads/main" });

        const onMain = yield* commitOn({
          branch: "main",
          message: "second on main",
          files: { "main.txt": "from main\n" },
        });
        const one = yield* commitOn({
          branch: "topic",
          message: "one",
          files: { "a.txt": "alpha, edited by topic\n" },
        });

        // A merge of `main` into `topic`: it puts `first` on `topic`'s
        // first-parent walk while `main` already has it, and leaves a merge
        // whose change is entirely present once `one` has been replayed.
        const merged = yield* repository.merge({
          ours: "refs/heads/topic",
          theirs: "refs/heads/main",
          author: alice,
          message: "merge main",
          into: "refs/heads/topic",
          noFastForward: true,
        });

        const outcome = yield* rebase({
          branch: "refs/heads/topic",
          onto: "refs/heads/main",
          into: "refs/heads/topic",
        });

        return {
          a: yield* fileAt(outcome.head!, "a.txt"),
          first,
          history: yield* messagesFrom(outcome.head!),
          merged: merged.commit,
          onMain,
          one,
          outcome,
        };
      }),
    );

    assert.equal(result.outcome.kind, "replayed");
    assert.deepEqual(
      result.outcome.commits.map((entry) => entry.original),
      [result.first, result.one, result.merged],
    );

    assert.equal(result.outcome.commits[0]?.replayed, null, "`onto` already has `first`");
    assert.deepEqual(result.outcome.commits[0]?.conflicts, []);

    assert.notEqual(result.outcome.commits[1]?.replayed, null);
    assert.equal(result.outcome.head, result.outcome.commits[1]?.replayed);

    assert.equal(result.outcome.commits[2]?.replayed, null, "the merge had nothing left to add");
    assert.deepEqual(result.outcome.commits[2]?.conflicts, []);

    assert.deepEqual(result.history, ["one", "second on main", "first"]);
    assert.equal(result.a, "alpha, edited by topic\n");
  });

  it("reports up-to-date and moves nothing when the branch is already contained", async () => {
    const result = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;

        const first = yield* commitOn({
          branch: "main",
          message: "first",
          files: { "a.txt": "alpha\n" },
        });
        yield* repository.branch({ name: "topic", base: "refs/heads/main" });
        const onMain = yield* commitOn({
          branch: "main",
          message: "second on main",
          files: { "main.txt": "from main\n" },
        });

        const outcome = yield* rebase({
          branch: "refs/heads/topic",
          onto: "refs/heads/main",
          into: "refs/heads/topic",
        });

        return { first, onMain, outcome, topic: yield* repository.resolve("refs/heads/topic") };
      }),
    );

    assert.equal(result.outcome.kind, "up-to-date");
    assert.equal(result.outcome.head, result.onMain);
    assert.deepEqual(result.outcome.commits, []);
    assert.equal(result.topic, result.first, "nothing was replayed, so the branch stays put");
  });
});

describe.skipIf(!hasGit)("rebase interop with git", () => {
  const git = (root: string, ...args: string[]) =>
    execFileSync("git", [`--git-dir=${root}`, ...args], { encoding: "utf8" }).trim();

  it("writes a history git reads back and fsck accepts", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-rebase-"));
    try {
      const head = await onDisk(
        root,
        Effect.gen(function* () {
          const refs = yield* RefStore;
          // Without HEAD there is no repository as far as git is concerned.
          yield* refs.setHead("refs/heads/main");
          yield* threeCommitBranch;

          const outcome = yield* rebase({
            branch: "refs/heads/topic",
            onto: "refs/heads/main",
            into: "refs/heads/topic",
          });
          return outcome.head;
        }),
      );

      assert.equal(git(root, "rev-parse", "refs/heads/topic"), head);
      assert.equal(
        git(root, "log", "--format=%s", "refs/heads/topic"),
        ["three", "two", "one", "second on main", "first"].join("\n"),
      );
      // git's own view of the parent chain, not ours.
      assert.equal(
        git(root, "log", "--format=%s", "--first-parent", "refs/heads/main..refs/heads/topic"),
        ["three", "two", "one"].join("\n"),
      );
      assert.match(git(root, "log", "--format=%an", "-1", "refs/heads/topic~1"), /^Bob$/);
      assert.equal(git(root, "fsck", "--strict", "--no-dangling"), "");
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });
});
