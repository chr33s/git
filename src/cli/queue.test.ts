/**
 * The merge queue, driven end to end from the command line.
 *
 * The properties worth proving are the ones that separate a queue from a
 * sequence of pushes: that two approved pull requests land in *one* swap
 * against the branch they were queued for, that what lands is a chain the
 * boundary re-derived rather than took on trust, that a check which passed on a
 * pull request's own head does not vouch for the combination it is landing in,
 * and that a failure in the middle of a batch costs the batch its tail and not
 * its head.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Policy from "../server/Policy.ts";
import { enableHubUnder, grantMember, opensshPrivateKey } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

/** The same call, for the paths that are supposed to fail. */
const failing = (args: ReadonlyArray<string>): Promise<string> =>
  cli(args).then(
    () => "",
    (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
  );

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

describe("cli queue", () => {
  let root = "";
  /** The queue runner, who also opens the pull requests. */
  let key = "";
  /** Somebody else, because approving your own work is not review. */
  let reviewer = "";
  let queue = "";
  // SAFETY: `beforeEach` overwrites this with a real oid before any check runs,
  // and every check in this suite goes through it.
  let base: Oid = "" as Oid;
  const heads: Record<string, Oid> = {};

  /** One branch's tip, narrowed rather than asserted. */
  const headOf = (branch: string): Oid => {
    const tip = heads[branch];
    if (tip === undefined) throw new Error(`no head recorded for ${branch}`);
    return tip;
  };

  const repoLayer = () =>
    GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(nodeStores(path.join(root, "project"))),
    );

  const inRepo = <A, E>(effect: Effect.Effect<A, E, GitRepository.Repository>): Promise<A> =>
    Effect.runPromise(effect.pipe(Effect.provide(repoLayer())));

  /** One commit carrying one file, so merging two of them decides something. */
  const write = (files: ReadonlyArray<readonly [string, string]>, parents: ReadonlyArray<Oid>) =>
    Effect.gen(function* () {
      const repository = yield* GitRepository.Repository;
      const tree = yield* repository.writeFiles({
        changes: files.map(([file, content]) => ({
          path: file,
          content: new TextEncoder().encode(content),
          mode: "100644",
        })),
      });
      return yield* repository.commitTree({ tree, parents, message: "c\n", author });
    });

  const publish = (rules: Policy.Rules) =>
    inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const blob = yield* repository.writeBlob(Policy.encodeRules(rules));
        const tree = yield* repository.writeTree([
          { mode: "100644", name: Policy.RULES_PATH, oid: blob },
        ]);
        const commit = yield* repository.commitTree({
          tree,
          parents: [],
          message: "policy\n",
          author,
        });
        yield* repository.setRef({ name: Policy.RULES_REF, to: commit });
      }),
    );

  const protectedRules = (extra: Partial<Policy.Rules> = {}): Policy.Rules => ({
    ...Policy.OPEN,
    protected: ["refs/heads/main"],
    requiredApprovals: 1,
    queueCandidates: true,
    ...extra,
  });

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-queue-"));
    key = path.join(root, "runner");
    reviewer = path.join(root, "reviewer");
    await cli(["init", "--root", root, "project"]);

    const fixture = await enableHubUnder(root, "project", [
      "repo.read",
      "source.push",
      "hub.create-pr",
      "hub.review",
      "hub.approve",
      "hub.merge",
      "hub.queue",
      "hub.check:test",
    ]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "runner@example.com"), {
      mode: 0o600,
    });
    const second = await grantMember(path.join(root, "project"), fixture.root, fixture.repoId, [
      "repo.read",
      "hub.review",
      "hub.approve",
    ]);
    await fs.writeFile(reviewer, opensshPrivateKey(second.member, "reviewer@example.com"), {
      mode: 0o600,
    });

    // One shared base, then a branch each touching a different file.
    await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        base = yield* write([["readme", "base"]], []);
        yield* repository.setRef({ name: "refs/heads/main", to: base });
        for (const [branch, file] of [
          ["one", "a.txt"],
          ["two", "b.txt"],
        ] as const) {
          const tip = yield* write(
            [
              ["readme", "base"],
              [file, branch],
            ],
            [base],
          );
          heads[branch] = tip;
          yield* repository.setRef({ name: `refs/heads/${branch}`, to: tip });
        }
      }),
    );

    queue = (
      await cli([
        "queue",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--target",
        "refs/heads/main",
        "project",
      ])
    ).trim();
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const propose = async (branch: string): Promise<string> => {
    const pr = (
      await cli([
        "pr",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--title",
        branch,
        "--base",
        "main",
        "--head",
        branch,
        "project",
      ])
    ).trim();
    await cli([
      "pr",
      "review",
      "--root",
      root,
      "--key",
      reviewer,
      "--decision",
      "approve",
      "project",
      pr,
    ]);
    return pr;
  };

  const enter = (pr: string) =>
    cli(["queue", "enter", "--root", root, "--key", key, "--queue", queue, "project", pr]);

  const run = async (extra: ReadonlyArray<string> = []) =>
    JSON.parse(
      await cli([
        "queue",
        "run",
        "--root",
        root,
        "--key",
        key,
        "--queue",
        queue,
        ...extra,
        "project",
      ]),
    );

  const mainAt = () =>
    inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.resolve("refs/heads/main"),
      ),
    );

  it("lands two approved pull requests in one swap", async () => {
    await publish(protectedRules());
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const pass = await run();

    assert.deepEqual(pass.landed, [first, second], "both land, and in the order they were queued");
    assert.equal(pass.from, base);
    assert.notEqual(pass.to, base);

    // What the branch holds is the chain's tip, and the whole chain arrived at
    // once — `main` never held the first candidate on its own.
    const landed = await mainAt();
    assert.equal(landed, pass.built.at(-1).commit);

    const merged = JSON.parse(await cli(["pr", "show", "--root", root, "project", first]));
    assert.equal(merged.state, "merged", "a landed entry is recorded as merged on its own ref");

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(state.entries, [], "and both leave the queue");
  });

  it("carries both changes, rather than one wrapped around the other", async () => {
    await publish(protectedRules());
    await enter(await propose("one"));
    await enter(await propose("two"));
    await run();

    const files = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const main = yield* repository.resolve("refs/heads/main");
        if (main === null) return [];
        const commit = yield* repository.readCommit(main);
        return (yield* repository.listFiles(commit.tree)).map((file) => file.path);
      }),
    );
    assert.deepEqual([...files].sort(), ["a.txt", "b.txt", "readme"]);
  });

  it("says what it would do and writes nothing", async () => {
    await publish(protectedRules());
    await enter(await propose("one"));

    const pass = await run(["--dry-run"]);
    assert.equal(pass.dryRun, true);
    assert.equal(pass.built.length, 1, "a dry run still computes the candidate");
    assert.equal(await mainAt(), base, "and leaves the branch exactly where it was");

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(state.entries.length, 1, "the entry is still queued");
    assert.equal(state.entries[0].candidate, null, "and nothing was recorded about it");
  });

  it("builds the same candidate twice from the same inputs", async () => {
    // Load-bearing rather than tidy. A check is bound to an exact object id, so
    // a candidate whose oid moved between passes would invalidate the very
    // evidence it was built to collect, and the queue could never land
    // anything. Nothing environmental may reach the commit — a wall clock in
    // the committer line is enough to break it.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    await enter(await propose("one"));

    const first = await run();
    const second = await run();
    assert.deepEqual(
      second.built.map((step: { commit: string }) => step.commit),
      first.built.map((step: { commit: string }) => step.commit),
    );
  });

  it("does not land on a check that passed on the head alone", async () => {
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);

    // Green on the pull request's own revision, which says nothing about the
    // revision being landed.
    await cli([
      "pr",
      "check",
      "--root",
      root,
      "--key",
      key,
      "--name",
      "test",
      "--status",
      "success",
      "--head",
      headOf("one"),
      "project",
      first,
    ]);

    const built = await run();
    assert.deepEqual(built.landed, []);
    assert.equal(await mainAt(), base);

    // And now against the candidate itself, which is what a queue tests.
    const candidate = built.built[0].commit;
    await cli([
      "pr",
      "check",
      "--root",
      root,
      "--key",
      key,
      "--name",
      "test",
      "--status",
      "success",
      "--head",
      candidate,
      "project",
      first,
    ]);

    const landed = await run();
    assert.deepEqual(landed.landed, [first]);
    assert.equal(await mainAt(), candidate, "the tested candidate is exactly what landed");
  });

  it("lands the green head of a batch and leaves the rest queued", async () => {
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const built = await run();
    assert.equal(built.built.length, 2);

    // Only the first candidate is tested; the second stays unproven.
    await cli([
      "pr",
      "check",
      "--root",
      root,
      "--key",
      key,
      "--name",
      "test",
      "--status",
      "success",
      "--head",
      built.built[0].commit,
      "project",
      first,
    ]);

    const pass = await run();
    assert.deepEqual(pass.landed, [first], "the proven prefix lands");
    assert.deepEqual(pass.waiting, [second], "and the rest waits rather than being thrown away");

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(
      state.entries.map((held: { pr: string }) => held.pr),
      [second],
    );
  });

  it("defers an entry that conflicts with the batch, rather than dropping it", async () => {
    // A conflict with the chain tip is a conflict with entries that have not
    // landed and may never land. Recording a permanent `queue.left` for that
    // takes a pull request out of the queue over a disagreement with something
    // provisional — and the entry it clashed with might be dropped next pass.
    await publish(protectedRules());
    await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        // Rewrites the same file `two` does, but merges onto `main` cleanly.
        const tip = yield* write(
          [
            ["readme", "base"],
            ["b.txt", "conflicting"],
          ],
          [base],
        );
        heads["three"] = tip;
        yield* repository.setRef({ name: "refs/heads/three", to: tip });
      }),
    );

    const second = await propose("two");
    const third = await propose("three");
    await enter(second);
    await enter(third);

    const pass = await run();
    assert.deepEqual(pass.landed, [second]);
    assert.deepEqual(pass.dropped, [], "nothing permanent is recorded about the loser");
    assert.deepEqual(
      pass.unbuilt.map((entry: { pr: string }) => entry.pr),
      [third],
    );

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(
      state.entries.map((held: { pr: string }) => held.pr),
      [third],
      "it is still queued, for a pass this batch does not stand in the way of",
    );
  });

  it("drops an entry that conflicts with the branch itself", async () => {
    await publish(protectedRules());
    const third = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        // The branch moves to something that rewrites `b.txt` …
        const moved = yield* write(
          [
            ["readme", "base"],
            ["b.txt", "landed elsewhere"],
          ],
          [base],
        );
        yield* repository.setRef({ name: "refs/heads/main", to: moved });
        // … and a pull request cut from before it rewrites the same line.
        const tip = yield* write(
          [
            ["readme", "base"],
            ["b.txt", "conflicting"],
          ],
          [base],
        );
        heads["three"] = tip;
        yield* repository.setRef({ name: "refs/heads/three", to: tip });
        return tip;
      }),
    );
    void third;

    await enter(await propose("three"));

    const pass = await run();
    assert.deepEqual(pass.landed, []);
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["conflict"],
      "predicted rather than discovered by a failing test run",
    );
  });

  it("drops an entry whose pull request moved on", async () => {
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);

    // The head moves, which stales the approval — so what was queued is no
    // longer what the pull request proposes.
    const moved = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const tip = yield* write(
          [
            ["readme", "base"],
            ["a.txt", "rewritten"],
          ],
          [headOf("one")],
        );
        yield* repository.setRef({ name: "refs/heads/one", to: tip });
        return tip;
      }),
    );
    await cli(["pr", "update", "--root", root, "--key", key, "--head", moved, "project", first]);

    const pass = await run();
    assert.deepEqual(pass.landed, []);
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["stale"],
    );
  });

  it("refuses to land what the boundary would refuse from anybody", async () => {
    // No approval at all: the runner holds `hub.merge` and every other
    // capability, and still cannot land — because it does not decide.
    await publish(protectedRules());
    const pr = (
      await cli([
        "pr",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--title",
        "unreviewed",
        "--base",
        "main",
        "--head",
        "one",
        "project",
      ])
    ).trim();
    await enter(pr);

    const pass = await run();
    assert.deepEqual(pass.landed, []);
    assert.equal(await mainAt(), base);
  });

  it("rebuilds on a branch that moved under it", async () => {
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);
    const built = await run(["--dry-run"]);
    assert.equal(built.built.length, 1);

    // Somebody lands directly while the queue was thinking. Nothing is lost:
    // the next pass builds on what they left.
    const second = await propose("two");
    await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        yield* repository.setRef({ name: "refs/heads/main", to: headOf("two") });
      }),
    );
    void second;

    const pass = await run();
    assert.equal(pass.from, headOf("two"));
    assert.deepEqual(pass.landed, [first]);

    const files = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const main = yield* repository.resolve("refs/heads/main");
        if (main === null) return [];
        const commit = yield* repository.readCommit(main);
        return (yield* repository.listFiles(commit.tree)).map((file) => file.path);
      }),
    );
    assert.deepEqual(
      [...files].sort(),
      ["a.txt", "b.txt", "readme"],
      "the rebuilt candidate merges onto what the branch actually holds",
    );
  });

  it("continues a batch a previous run left half-built", async () => {
    await publish(protectedRules());
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);

    // One pass builds and lands the first entry; the second is queued only
    // afterwards, exactly as a run that died before seeing it would leave things.
    await run();
    await enter(second);

    const pass = await run();
    assert.deepEqual(pass.landed, [second]);
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(state.entries, []);
  });

  it("says why nothing landed, rather than reporting an empty pass", async () => {
    // A pass that refused every candidate and one with nothing to do both
    // reported `landed: []`, and they want very different responses.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);

    const pass = await run();
    assert.deepEqual(pass.landed, []);
    assert.equal(pass.refused.length, 1);
    assert.match(pass.refused[0].reason, /test/);
  });

  it("refuses a target whose rules require provenance, once and by name", async () => {
    // A candidate is a commit the runner makes and it carries no session
    // trailer, so the boundary would refuse every one — while the runner went
    // on building, publishing and recording them on an append-only ref at every
    // wake. Said once instead.
    await publish(protectedRules({ requireProvenance: true }));
    await enter(await propose("one"));

    const refused = await failing([
      "queue",
      "run",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "project",
    ]);
    assert.match(refused, /requires provenance/);

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(state.entries[0].candidate, null, "and nothing was built or recorded");
  });

  it("refuses a queue id nobody opened, rather than creating one", async () => {
    // `refs/hub/queue/*` cannot be deleted, so a mistyped id must cost an error
    // message and not a permanent entry in every ref listing this repository
    // ever serves — holding records the projection ignores for ever.
    const mistyped = "01920000-0000-7000-8000-00000000dead";
    const refused = await failing([
      "queue",
      "enter",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      mistyped,
      "project",
      await propose("one"),
    ]);
    assert.match(refused, /holds no queue/);

    const listed = JSON.parse(await cli(["queue", "list", "--root", root, "project"]));
    assert.deepEqual(
      listed.map((held: { queue: string }) => held.queue),
      [queue],
      "the typo left no ref behind",
    );
  });

  it("keeps a re-entered pull request where it was in the queue", async () => {
    // Re-entering is how an entry's head moves. Sending it to the back would
    // reorder a batch because somebody pushed a fix, which is not what
    // re-entering means; leaving and entering again is how something moves.
    await publish(protectedRules());
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const moved = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const tip = yield* write(
          [
            ["readme", "base"],
            ["a.txt", "revised"],
          ],
          [base],
        );
        yield* repository.setRef({ name: "refs/heads/one", to: tip });
        return tip;
      }),
    );
    await cli(["pr", "update", "--root", root, "--key", key, "--head", moved, "project", first]);
    await enter(first);

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(
      state.entries.map((held: { pr: string }) => held.pr),
      [first, second],
    );
    assert.equal(state.entries[0].head, moved, "at its new revision, in its old place");
  });

  it("records a candidate once, however many passes rebuild it", async () => {
    // `refs/hub/queue/*` is append-only, undeletable, and capped at the number
    // of events a fold will walk. A pass that re-recorded an identical
    // candidate every time — and the documented wake rules run one constantly —
    // would grow it to that cap, at which point the queue becomes unreadable
    // and unremovable at once.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    await enter(await propose("one"));

    await run();
    const once = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    await run();
    await run();
    const thrice = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));

    assert.deepEqual(thrice.entries[0].candidate, once.entries[0].candidate);
    const events = await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.resolve(`refs/hub/queue/${queue}`),
      ),
    );
    assert.notEqual(events, null);
    const walked = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        let at = events;
        let count = 0;
        while (at !== null) {
          const commit = yield* repository.readCommit(at);
          count += 1;
          at = commit.parents[0] ?? null;
        }
        return count;
      }),
    );
    // opened, entered, one candidate — and nothing added by the two passes
    // that rebuilt exactly the same commit.
    assert.equal(walked, 3);
  });

  it("builds no deeper than the branch will take", async () => {
    // Past the ceiling the boundary reads a push as not a chain at all, so
    // anything built beyond it is published and recorded and can never land.
    await publish(protectedRules({ queueDepth: 1 }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const pass = await run();
    assert.deepEqual(pass.landed, [first]);
    assert.equal(pass.built.length, 1, "one step, because one is all it can land");
    assert.deepEqual(
      pass.unbuilt.map((entry: { pr: string }) => entry.pr),
      [second],
    );
  });

  it("evicts an entry whose required check came back failing", async () => {
    // A check that has not run is the ordinary case and must keep waiting; one
    // that reported failure is the entry's own problem, and leaving it queued
    // blocks everything behind it for ever.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);

    const built = await run();
    assert.deepEqual(built.dropped, [], "not run is not failed");

    await cli([
      "pr",
      "check",
      "--root",
      root,
      "--key",
      key,
      "--name",
      "test",
      "--status",
      "failure",
      "--head",
      built.built[0].commit,
      "project",
      first,
    ]);

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["failed"],
    );
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(state.entries, [], "and the queue is free to move on");
  });

  it("cleans up the candidate branches it no longer needs", async () => {
    // Ordinary branches, cleaned up like ordinary branches. Left behind, every
    // candidate the queue ever built stays a ref pinning its objects out of
    // reach of collection for good.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const built = await run();
    assert.equal(built.built.length, 2);
    const queueBranches = () =>
      inRepo(
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          const names: Array<string> = [];
          for (const [name] of yield* repository.refs) {
            if (name.startsWith("refs/heads/queue/")) names.push(name);
          }
          return names.sort();
        }),
      );
    assert.equal((await queueBranches()).length, 2, "both are published for CI to fetch");

    // The first candidate goes green and lands; its branch is no longer what
    // anything fetches, and the second is rebuilt onto the new tip.
    await cli([
      "pr",
      "check",
      "--root",
      root,
      "--key",
      key,
      "--name",
      "test",
      "--status",
      "success",
      "--head",
      built.built[0].commit,
      "project",
      first,
    ]);
    const landed = await run();
    assert.deepEqual(landed.landed, [first]);
    assert.equal(
      (await queueBranches()).length,
      1,
      "what is still waiting keeps its branch, and nothing else does",
    );
  });

  it("refuses an entry for a pull request aimed somewhere else", async () => {
    await publish(protectedRules());
    const pr = (
      await cli([
        "pr",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--title",
        "elsewhere",
        "--base",
        "two",
        "--head",
        "one",
        "project",
      ])
    ).trim();

    const refused = await cli([
      "queue",
      "enter",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "project",
      pr,
    ]).then(
      () => "",
      (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );
    assert.match(refused, /lands on refs\/heads\/main/);
  });
});
