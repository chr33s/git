/**
 * Bisect, against `git rev-list --bisect`.
 *
 * "Picks a good commit to test" is not a checkable claim; "picks the same
 * commit git picks" is, and git's choice is the definition of a good one. So
 * the interop tests build a history, ask both, and compare — on a linear
 * history where the answer is obvious, and on a merge-heavy one where it is
 * not and a naive midpoint would diverge.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { next } from "./Bisect.ts";
import { stores as memoryStores } from "./Memory.ts";
import { stores as nodeStores } from "./Node.ts";
import * as GitRepository from "./Repository.ts";
import { Repository } from "./Repository.ts";
import type { Oid } from "./Store.ts";

const hasGit = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const inMemory = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(memoryStores),
);

/** A chain of `count` commits on `branch`, returned oldest-first. */
const chain = (branch: string, count: number) =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const made: Array<Oid> = [];
    for (let index = 0; index < count; index++) {
      made.push(
        yield* repository.commit({
          branch: `refs/heads/${branch}`,
          tree: yield* repository.writeFiles({
            changes: [{ path: "n.txt", content: new TextEncoder().encode(`${index}\n`) }],
          }),
          message: `commit ${index}`,
          author,
        }),
      );
    }
    return made;
  });

describe("bisect", () => {
  it.live("halves a linear history and converges on the culprit", () =>
    Effect.gen(function* () {
      const made = yield* chain("main", 15);
      const first = made[0]!;
      const last = made[14]!;

      const step = yield* next({ bad: last, good: [first] });
      assert.equal(step.kind, "test");
      assert.equal(step.remaining, 14, "everything after the good commit is a suspect");
      // 14 suspects halve to 7, so the midpoint is the 7th commit after the
      // known-good one.
      assert.equal(step.commit, made[7]);

      // Answering "still bad" at the midpoint keeps the lower half.
      const lower = yield* next({ bad: made[7]!, good: [first] });
      assert.equal(lower.remaining, 7);

      // Narrowed to one, the search is over rather than asking again.
      const done = yield* next({ bad: made[1]!, good: [first] });
      assert.equal(done.kind, "found");
      assert.equal(done.commit, made[1]);
      assert.equal(done.steps, 0);
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("rejects a bad commit that a good one can reach", () =>
    Effect.gen(function* () {
      const made = yield* chain("main", 4);
      // `bad` older than `good` is not a narrow search, it is a contradiction:
      // silently returning some commit would send the caller hunting for a
      // fault that the inputs say cannot be there.
      const failed = yield* next({ bad: made[1]!, good: [made[3]!] }).pipe(Effect.flip);
      assert.equal(failed._tag, "Invalid");
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );

  it.live("treats a commit marked both good and bad as a contradiction", () =>
    Effect.gen(function* () {
      const made = yield* chain("main", 3);
      // Not an empty search — the same commit cannot be both, and saying
      // "nothing to test" would hide the mistake instead of naming it.
      const failed = yield* next({ bad: made[2]!, good: [made[2]!] }).pipe(Effect.flip);
      assert.equal(failed._tag, "Invalid");
    }).pipe(Effect.provide(inMemory), Effect.orDie),
  );
});

describe.skipIf(!hasGit)("bisect, against git", () => {
  let root: string;

  const git = (...args: string[]) =>
    execFileSync("git", ["-c", "user.name=T", "-c", "user.email=t@e.com", ...args], {
      cwd: root,
      encoding: "utf8",
    });

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "bisect-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const ours = (bad: string, good: ReadonlyArray<string>) =>
    Effect.runPromise(
      next({ bad: bad as Oid, good: good as ReadonlyArray<Oid> }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(nodeStores(path.join(root, ".git"))),
          ),
        ),
        Effect.orDie,
      ),
    );

  /** git's own answer: the commit it would hand you to test next. */
  const theirs = (bad: string, good: ReadonlyArray<string>) =>
    git("rev-list", "--bisect", bad, ...good.map((oid) => `^${oid}`)).trim();

  /** `file` is a parameter so two branches can move without conflicting. */
  const commit = (message: string, file = "n.txt") => {
    execFileSync("sh", ["-c", `echo ${message} > ${file}`], { cwd: root });
    git("add", file);
    git("commit", "-q", "-m", message);
    return git("rev-parse", "HEAD").trim();
  };

  /** `git merge-base --is-ancestor` answers by exit code, not by output. */
  const isAncestor = (ancestor: string, descendant: string) => {
    try {
      execFileSync("git", ["merge-base", "--is-ancestor", ancestor, descendant], { cwd: root });
      return true;
    } catch {
      return false;
    }
  };

  it("picks the same commit as git on a linear history", async () => {
    git("init", "-q", "-b", "main", ".");

    const made: string[] = [];
    for (let index = 0; index < 13; index++) made.push(commit(`c${index}`));

    const bad = made[12]!;
    const good = [made[0]!];
    assert.equal((await ours(bad, good)).commit, theirs(bad, good));

    // …and again after one answer, so the agreement is not a coincidence of
    // this particular range size.
    const narrowed = theirs(bad, good);
    assert.equal((await ours(narrowed, good)).commit, theirs(narrowed, good));
  });

  it("picks the same commit as git across a merge", async () => {
    git("init", "-q", "-b", "main", ".");

    const root0 = commit("root");
    commit("main-1");
    commit("main-2");

    git("checkout", "-q", "-b", "side", root0);
    commit("side-1", "s.txt");
    commit("side-2", "s.txt");
    commit("side-3", "s.txt");

    git("checkout", "-q", "main");
    git("merge", "-q", "--no-ff", "-m", "merge side", "side");
    const bad = commit("after-merge");

    // A merge means the suspects are a graph, not a line: the midpoint of a
    // list would be the wrong answer here and git's choice is not it either.
    const good = [root0];
    const mine = await ours(bad, good);
    assert.equal(mine.commit, theirs(bad, good));
    assert.equal(
      mine.remaining,
      Number(git("rev-list", "--count", bad, `^${root0}`).trim()),
      "the suspect count is git's `rev-list --count` for the same range",
    );
  });

  it("agrees with git all the way down to the culprit", async () => {
    git("init", "-q", "-b", "main", ".");

    const made: string[] = [];
    for (let index = 0; index < 9; index++) made.push(commit(`c${index}`));

    // Drive a whole session: the fault is at made[6], so every commit from
    // there on is "bad". Both implementations should walk the same path and
    // land on the same commit.
    const culprit = made[6]!;
    const isBad = (oid: string) => isAncestor(culprit, oid);

    let bad = made[8]!;
    const good = [made[0]!];
    for (;;) {
      const step = await ours(bad, good);
      assert.equal(step.commit, theirs(bad, good), "same choice at every step");
      if (step.kind === "found") break;
      if (isBad(step.commit!)) bad = step.commit!;
      else good.push(step.commit!);
    }

    const final = await ours(bad, good);
    assert.equal(final.commit, culprit, "the first bad commit, found");
  });
});
