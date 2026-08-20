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
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Queue from "../hub/Queue.ts";
import * as Policy from "../server/Policy.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { readPrivateKey } from "./shared.ts";
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
    assert.deepEqual(pass.landed, [], "nothing landed, because nothing moved");
    assert.deepEqual(pass.dropped, [], "and nothing was taken out either");
    assert.equal(pass.wouldLand.length, 1, "and what would have is said separately");
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

  it("keeps a queued pull request whose head moved, for it to enter again", async () => {
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
    // Nothing permanent: a pushed fix is not a reason to take somebody out of
    // the queue, and `Queue.project` updates a re-entered head in place — which
    // it can only do if the entry is still there to update.
    assert.deepEqual(pass.dropped, []);
    assert.deepEqual(
      pass.unbuilt.map((entry: { pr: string }) => entry.pr),
      [first],
    );

    await enter(first);
    const again = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(again.entries[0].pr, first, "and it keeps the place it had");
    assert.equal(again.entries[0].head, moved);
  });

  it("drops an entry whose pull request stopped being a candidate", async () => {
    // Closed, merged or aimed somewhere else: not waiting on anything, so the
    // record that says so is settled and permanent.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);
    await cli(["pr", "close", "--root", root, "--key", key, "project", first]);

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["stale"],
    );
  });

  it("refuses to take out a pull request that is not queued", async () => {
    const refused = await failing([
      "queue",
      "leave",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "project",
      await propose("one"),
    ]);
    assert.match(refused, /is not in/);
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

    const pass = await run();
    assert.match(pass.skipped, /requires provenance/);

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
      listed.queues.map((held: { queue: string }) => held.queue),
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

  it("evicts only the entry a failing check is about", async () => {
    // A candidate contains every step beneath it, so one broken pull request
    // fails the checks on every candidate above it too. Evicting them all would
    // take the whole batch out for one entry's fault, permanently.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const built = await run();
    assert.equal(built.built.length, 2);

    // The first breaks, and the second's candidate — which contains it — is
    // red for exactly that reason.
    const fail = (head: string, pr: string) =>
      cli([
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
        head,
        "project",
        pr,
      ]);
    await fail(built.built[0].commit, first);
    await fail(built.built[1].commit, second);

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string }) => entry.pr),
      [first],
      "the cause is evicted and the victim behind it is not",
    );
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(
      state.entries.map((held: { pr: string }) => held.pr),
      [second],
    );
  });

  it("does not blame a failure it cannot attribute", async () => {
    // A candidate contains every step under it, so a red one under a *pending*
    // one says nothing about which of them broke. Evicting the red one there
    // took out a pull request for a change that had not been tested yet.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const built = await run();
    // Nothing reported for the first candidate; the second is red, and it
    // contains the first's change.
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
      built.built[1].commit,
      "project",
      second,
    ]);

    const pass = await run();
    assert.deepEqual(pass.dropped, [], "the evidence does not say whose fault it is");
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(state.entries.length, 2, "both wait for the first candidate to report");
  });

  it("settles an entry a lost pass already landed", async () => {
    // A pass that landed a batch and died before recording it left its entries
    // queued. The next pass then built a no-op merge — a candidate whose tree
    // is the tip's own, which no check ever names — and the queue stalled
    // behind work that was already done.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);

    // The revision reaches the branch without the queue recording anything,
    // which is exactly the state an interrupted pass leaves behind.
    await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.setRef({ name: "refs/heads/main", to: headOf("one") }),
      ),
    );

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["landed"],
    );
    assert.deepEqual(pass.built, [], "and no no-op candidate is built for it");

    const merged = JSON.parse(await cli(["pr", "show", "--root", root, "project", first]));
    assert.equal(merged.state, "merged", "the record the interrupted pass did not get to");
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(state.entries, []);
  });

  it("refuses a protected branch that does not admit candidates", async () => {
    // Every pass would publish a branch and append a record the boundary will
    // always refuse — and a candidate is a pure function of what it merges, so
    // each direct push that moves the branch makes a new one. Unbounded churn
    // on a ref that only grows, in the shape a queue is most likely to be run
    // in before somebody turns the rule on.
    await publish(protectedRules({ queueCandidates: false }));
    await enter(await propose("one"));

    const pass = await run();
    assert.match(pass.skipped, /does not admit queue candidates/);
    assert.equal(pass.queue, queue, "and it names the queue it resolved");

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(state.entries[0].candidate, null, "and nothing was built or recorded");
  });

  it("does not record a reset the branch did not earn", async () => {
    // A chain's foot is the only step built on the branch tip, and finding it
    // by taking the first entry that has a candidate is wrong the moment an
    // entry re-enters: its candidate is cleared in place, so the search returns
    // a later step whose `onto` is another candidate — and the pass recorded a
    // reset that had not happened, on a ref nothing can shorten.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);
    await run();

    const settled = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(settled.resets, 0, "a first build resets nothing");

    // Nothing moves the branch; the second pass rebuilds the same chain.
    const again = await run();
    assert.equal(again.reset, false);
    const after = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(after.resets, 0, "and an unchanged rebuild resets nothing either");
  });

  it("leaves alone a branch under the prefix it did not put there", async () => {
    // The cleanup sweep read the queue as it stood when the pass began, so it
    // deleted whatever a keep-list did not mention — a concurrent runner's
    // freshly published candidate, or a branch somebody happened to keep under
    // the same prefix. Only a pull request this pass settled is finished.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);
    const bystander = "refs/heads/queue/main/someone-elses-work";
    await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.setRef({ name: bystander, to: headOf("two") }),
      ),
    );

    await run();

    const survived = await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) => repository.resolve(bystander)),
    );
    assert.equal(survived, headOf("two"), "a branch this pass knows nothing about stays");
  });

  it("says so when it loses the branch to somebody else mid-pass", async () => {
    // A lost compare-and-swap reported an empty pass, which reads exactly like
    // a pass with nothing to do — the ambiguity `refused` exists to remove.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);
    const built = await run();
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

    // The branch moves after the candidate was judged landable but before the
    // swap this pass will attempt — which is what a racing direct push is.
    await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.setRef({ name: "refs/heads/main", to: headOf("two") }),
      ),
    );

    const pass = await run();
    // The rebuild is onto the moved branch, so it lands there; what matters is
    // that a pass which cannot land says why rather than reporting nothing.
    assert.equal(
      pass.landed.length + pass.refused.length,
      1,
      "either it landed on the new tip or it said what stopped it",
    );
  });

  it("takes the published branch with a pull request that leaves", async () => {
    // A pass only deletes the branches of entries it settled, and no later pass
    // can name this one — so left behind, the candidate commit is pinned out of
    // reach of collection for as long as the repository exists.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);
    const built = await run();
    const branch = `refs/heads/queue/main/${first}`;
    assert.notEqual(
      await inRepo(
        Effect.flatMap(GitRepository.Repository, (repository) => repository.resolve(branch)),
      ),
      null,
    );
    void built;

    await cli([
      "queue",
      "leave",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "--reason",
      "withdrawn",
      "project",
      first,
    ]);
    assert.equal(
      await inRepo(
        Effect.flatMap(GitRepository.Repository, (repository) => repository.resolve(branch)),
      ),
      null,
      "and it goes with it",
    );
  });

  it("builds for a branch protected only against force-push and deletion", async () => {
    // Such a branch asks nothing of the revision arriving on it, so the
    // boundary allows the push and the runner has no reason to refuse — asked
    // as "is it protected?" alone, it refused work the boundary would take.
    await publish({
      ...Policy.OPEN,
      protected: ["refs/heads/main"],
      queueCandidates: false,
    });
    await enter(await propose("one"));

    const pass = await run();
    assert.equal(pass.built.length, 1, "it builds rather than hard-erroring");
    assert.deepEqual(pass.landed.length, 1, "and the boundary takes it");
  });

  it("does not close a pull request that has moved past what landed", async () => {
    // The containment recovery asks whether the *entered* revision is in the
    // branch. Asked before the head check, an entry whose entered revision had
    // landed while its pull request went on to propose more was closed as
    // merged with that new work unlanded.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);

    const later = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        // The entered revision reaches the branch …
        yield* repository.setRef({ name: "refs/heads/main", to: headOf("one") });
        // … and the pull request proposes more on top of it.
        const tip = yield* write(
          [
            ["readme", "base"],
            ["a.txt", "one"],
            ["c.txt", "more"],
          ],
          [headOf("one")],
        );
        yield* repository.setRef({ name: "refs/heads/one", to: tip });
        return tip;
      }),
    );
    await cli(["pr", "update", "--root", root, "--key", key, "--head", later, "project", first]);

    const pass = await run();
    assert.deepEqual(pass.dropped, [], "nothing is settled about work still outstanding");
    const state = JSON.parse(await cli(["pr", "show", "--root", root, "project", first]));
    assert.equal(state.state, "open", "and the pull request stays open for it");
  });

  it("takes a bare branch name for a target, as pr --base does", async () => {
    // The record stores the full name, because the protected-branch rules match
    // on the ref being written; a sibling command that refused the spelling
    // `pr open --base main` accepts would be the only one here that did.
    const second = (
      await cli(["queue", "open", "--root", root, "--key", key, "--target", "release", "project"])
    ).trim();
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", second]));
    assert.equal(state.target, "refs/heads/release");

    // And it finds the queue again by the same spelling.
    const refused = await failing([
      "queue",
      "open",
      "--root",
      root,
      "--key",
      key,
      "--target",
      "release",
      "project",
    ]);
    assert.match(refused, /already has a queue/);
  });

  it("re-records a candidate the reset in the same pass cleared", async () => {
    // The projection was read before this pass appended its own `queue.reset`,
    // so an identically rebuilt candidate looked unchanged against a record the
    // reset had just cleared — leaving the queue showing no candidate for an
    // entry whose branch exists, permanently.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);
    await run();

    // The branch moves, so the next pass resets and rebuilds.
    await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.setRef({ name: "refs/heads/main", to: headOf("two") }),
      ),
    );
    const rebuilt = await run();
    assert.equal(rebuilt.reset, true);
    assert.equal(rebuilt.built.length, 1);

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.notEqual(
      state.entries[0].candidate,
      null,
      "the rebuild is recorded, not skipped as unchanged",
    );
    assert.equal(state.entries[0].candidate.commit, rebuilt.built[0].commit);
  });

  it("ends a queue so a fresh one can take over the branch", async () => {
    // A queue ref grows for as long as its branch does, and every fold of it is
    // bounded by the same ceiling every hub ref is. A pull request, a session
    // and a task are each about one finite piece of work; a queue is about a
    // branch, which is not — so without a way to end one it would eventually
    // pass the ceiling and be unreadable and unremovable at once.
    await publish(protectedRules());
    await enter(await propose("one"));
    await run();

    const closing = await cli([
      "queue",
      "close",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "--reason",
      "rotated",
      "project",
    ]);
    assert.match(closing, /closed: rotated/);

    const ended = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(ended.closed, "rotated");

    // The branch may have a queue again, and the closed one steps aside.
    const fresh = (
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
    assert.notEqual(fresh, queue);

    const byTarget = await failing([
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
    assert.match(byTarget, /was closed/, "and the ended one runs nothing");

    // Named by branch instead — the form a wake uses — it reports rather than
    // fails, so the bookmark advances and the loop does not replay for ever.
    const rotating = JSON.parse(
      await cli([
        "queue",
        "run",
        "--root",
        root,
        "--key",
        key,
        "--target",
        "refs/heads/other",
        "project",
      ]),
    );
    assert.match(rotating.skipped, /no open queue/);

    // Nor does anything else append to it: a record on a queue nothing reads
    // is a permanent entry the projection ignores, reported as success.
    const entering = await failing([
      "queue",
      "enter",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "project",
      await propose("two"),
    ]);
    assert.match(entering, /was closed/);
  });

  it("finishes a landing an interrupted pass half recorded", async () => {
    // A pass can die between `pr.merged` and the `queue.left` beside it. Read
    // in the wrong order, the recovery became unreachable the moment it had
    // written its first record — which is precisely the interruption it exists
    // for — and the entry was settled as `stale` instead.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);

    // Exactly that half-finished state: the revision is in the branch and the
    // pull request says merged, while the queue still holds the entry.
    await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.setRef({ name: "refs/heads/main", to: headOf("one") }),
      ),
    );
    await inRepo(
      Effect.gen(function* () {
        const signer = yield* readPrivateKey(key);
        const stored = yield* readGenesis();
        if (stored === null) throw new Error("no genesis");
        yield* PullRequest.merged({
          repo: stored.genesis.repoId,
          pr: first,
          head: headOf("one"),
          mergeCommit: headOf("one"),
          key: signer,
        });
      }),
    );

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["landed"],
      "the half-recorded landing is finished, not written off as stale",
    );
  });

  it("refuses when --queue and --target name different branches", async () => {
    // A caller that thinks it is talking about one branch while this talks
    // about another is a drifted hook appending records to, or landing on, a
    // branch its invocation never named.
    const refused = await failing([
      "queue",
      "enter",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "--target",
      "release",
      "project",
      await propose("one"),
    ]);
    assert.match(refused, /--target names refs\/heads\/release/);
  });

  it("holds itself to the staleness bound every other door applies", async () => {
    // `Policy.evaluate` judges one ref update; how old a membership view may be
    // is a rule about the request, enforced in `gate`. A runner judging itself
    // with `evaluate` alone was the one writer exempt from it — landing batches
    // on a branch whose every `git push` was being refused for that reason.
    await publish(protectedRules({ maxTrustAgeSeconds: 1 }));
    await enter(await propose("one"));

    const pass = await run();
    assert.match(pass.skipped, /checkpoint/i);
    assert.equal(await mainAt(), base, "and the branch is where it was");
  });

  it("keeps a dry run's would-be evictions out of what it evicted", async () => {
    // The same hazard `wouldLand` was split out of `landed` to avoid: a caller
    // gating on `dropped` would read a rehearsal as an eviction that happened.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);
    await cli(["pr", "close", "--root", root, "--key", key, "project", first]);

    const pass = await run(["--dry-run"]);
    assert.deepEqual(pass.dropped, []);
    assert.deepEqual(
      pass.wouldDrop.map((entry: { reason: string }) => entry.reason),
      ["stale"],
    );
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.equal(state.entries.length, 1, "and it is still queued");
  });

  it("does not put an unapproved entry at the head of a chain", async () => {
    // Built anyway, it blocked every approved entry behind it for as long as it
    // went unapproved — and when its code broke the next candidate's check, the
    // eviction loop blamed the entry that had done nothing wrong.
    await publish(protectedRules());
    const unapproved = (
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
    const approved = await propose("two");
    await enter(unapproved);
    await enter(approved);

    const pass = await run();
    assert.deepEqual(pass.landed, [approved], "the approved one lands past it");
    assert.deepEqual(
      pass.unbuilt.map((entry: { pr: string }) => entry.pr),
      [unapproved],
    );
    assert.deepEqual(pass.dropped, [], "and nothing permanent is said about it");
  });

  it("settles a pull request that closed after its head moved", async () => {
    // The moved-head branch returned first, so such an entry was never settled
    // at all: no record, entry stuck in the projection, candidate branch left
    // pinning its objects — and `queue enter` refuses a closed pull request, so
    // it could never be re-entered either.
    await publish(protectedRules());
    const first = await propose("one");
    await enter(first);

    const moved = await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const tip = yield* write(
          [
            ["readme", "base"],
            ["a.txt", "revised"],
          ],
          [headOf("one")],
        );
        yield* repository.setRef({ name: "refs/heads/one", to: tip });
        return tip;
      }),
    );
    await cli(["pr", "update", "--root", root, "--key", key, "--head", moved, "project", first]);
    await cli(["pr", "close", "--root", root, "--key", key, "project", first]);

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { pr: string; reason: string }) => entry.reason),
      ["stale"],
    );
    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    assert.deepEqual(state.entries, []);
  });

  it("evicts an entry whose required check came back neutral", async () => {
    // `checksPassedAt` requires success, so a neutral check can never land —
    // and recognised as neither failing nor green it was never evicted either,
    // stalling that step and everything behind it for good.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    const first = await propose("one");
    await enter(first);
    const built = await run();

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
      "neutral",
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
  });

  it("does not land what it cannot record as merged", async () => {
    // The ref charge for `refs/hub/pr/*` is the `hub.` prefix alone, so a
    // runner without `hub.merge` had its `pr.merged` accepted onto the ref and
    // then silently dropped by the fold — leaving every pull request it landed
    // open on every replica, for ever.
    await publish(protectedRules());
    await enter(await propose("one"));

    // The reviewer holds hub.review and hub.approve, and no hub.merge.
    const pass = JSON.parse(
      await cli(["queue", "run", "--root", root, "--key", reviewer, "--queue", queue, "project"]),
    );
    assert.match(pass.skipped, /hub\.merge/);
    assert.equal(await mainAt(), base, "and the branch is where it was");
  });

  it("is no stricter than the boundary on a branch that asks nothing", async () => {
    // `protectedBranch` returns before any of its rules on a branch that asks
    // nothing of the revision arriving on it, so applying them in the runner
    // regardless made it stricter than the judge and stalled entries it would
    // have landed.
    await publish({ ...Policy.OPEN, requiredApprovals: 2, queueCandidates: true });
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
    assert.deepEqual(pass.landed, [pr], "main is not protected, so nothing was required");
  });

  it("names the commit that carried a revision into the branch", async () => {
    // The recovery used to name the branch tip for every unsettled entry, which
    // attributed a whole interrupted batch to its topmost step. Walked back
    // along first parents, the step whose second parent is the revision is the
    // candidate a queue built for it.
    await publish(protectedRules());
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    // Land the batch, then put both entries back as an interrupted pass would
    // have left them: the branch has the code, the queue does not know.
    const landed = await run();
    assert.deepEqual(landed.landed, [first, second]);
    await inRepo(
      Effect.gen(function* () {
        const signer = yield* readPrivateKey(key);
        const stored = yield* readGenesis();
        if (stored === null) throw new Error("no genesis");
        for (const [pr, branch] of [
          [first, "one"],
          [second, "two"],
        ] as const) {
          yield* Queue.enter({
            repo: stored.genesis.repoId,
            queue,
            pr,
            head: headOf(branch),
            key: signer,
          });
        }
      }),
    );

    const pass = await run();
    assert.deepEqual(
      pass.dropped.map((entry: { reason: string }) => entry.reason),
      ["landed", "landed"],
    );
    // The first entry's merge names the first candidate, not the branch tip.
    const shown = JSON.parse(await cli(["pr", "show", "--root", root, "project", first]));
    assert.equal(shown.mergeCommit, landed.built[0].commit);
  });

  it("refuses to close a queue id nobody opened", async () => {
    // Every refusal `resolve` makes is an `Invalid`, so catching one to reach
    // the unreadable-ref rescue took that path for a mistyped id too —
    // creating `refs/hub/queue/<typo>` on an undeletable namespace and
    // reporting success, which is the hazard `resolve` exists to refuse.
    const mistyped = "01920000-0000-7000-8000-0000000c105e";
    const refused = await failing([
      "queue",
      "close",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      mistyped,
      "project",
    ]);
    assert.match(refused, /holds no queue/);

    const listed = JSON.parse(await cli(["queue", "list", "--root", root, "project"]));
    assert.deepEqual(
      listed.queues.map((held: { queue: string }) => held.queue),
      [queue],
      "the typo left no ref behind",
    );
  });

  it("closes a queue it can no longer read", async () => {
    // A ref past the ceiling is exactly the state closing exists to rescue, and
    // a close that first insisted on reading the ref could never reach it.
    // Appending needs only the ref's head, so the close lands and the branch
    // sweep — which needs the target — is what is given up.
    await publish(protectedRules({ requiredChecks: ["test"] }));
    await enter(await propose("one"));
    await run();

    // Past the ceiling, which is the state this rescues and the only one that
    // makes folding fail while appending still works. Built rather than grown:
    // `Dag.reachable` counts the commits it turns away as well as the ones it
    // keeps, so a hub commit naming a ceiling's worth of parents nothing holds
    // is a walk that gives up, at the cost of one commit object.
    await inRepo(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const head = yield* repository.resolve(Queue.refOf(queue));
        if (head === null) throw new Error("the queue this suite opened has no ref");
        const carrying = yield* repository.readCommit(head);
        // SAFETY: forty hex characters is what an oid is, and these name nothing
        // on purpose — the walk has to turn them away for the count to blow.
        const fabricated = Array.from(
          { length: Event.MAX_EVENTS },
          (_, index) => index.toString(16).padStart(40, "0") as Oid,
        );
        // The same tree, so the commit still carries a record and the walk keeps
        // it — a commit it turned away would end the walk instead of blowing it.
        const fat = yield* repository.commitTree({
          tree: carrying.tree,
          parents: [head, ...fabricated],
          message: "wide\n",
          author,
        });
        yield* repository.setRef({ name: Queue.refOf(queue), to: fat });
      }),
    );

    const closing = await cli([
      "queue",
      "close",
      "--root",
      root,
      "--key",
      key,
      "--queue",
      queue,
      "--reason",
      "rotated",
      "project",
    ]);
    assert.match(closing, /could not be read here/);
    // Once. The rescue returns into the same caller that prints for every close,
    // and a second line here reads as a second close of the same queue.
    assert.equal(closing.match(/closed: rotated/g)?.length, 1);
  });

  it("refuses a run that names neither a queue nor a branch", async () => {
    // A caller mistake, not a state — and one a hook makes by expanding an
    // argument to nothing, which is exactly when a silent success stops it
    // queueing for good.
    const refused = await failing(["queue", "run", "--root", root, "--key", key, "project"]);
    assert.match(refused, /name a queue with --queue/);
  });

  it("refuses a second queue for a branch that already has one", async () => {
    // `refs/hub/queue/*` cannot be deleted, so a second queue for one branch is
    // a permanent split: entries divide invisibly across the two and two
    // runners delete each other's candidate branches.
    const refused = await failing([
      "queue",
      "open",
      "--root",
      root,
      "--key",
      key,
      "--target",
      "refs/heads/main",
      "project",
    ]);
    assert.match(refused, /already has a queue/);
  });

  it("keeps the published branch of an entry a later pass could not build", async () => {
    // An entry can stop being built while the `queue.candidate` record naming
    // its branch stands. Deleting the branch then points `queue show` — and the
    // CI it tells to fetch — at something nothing can resolve.
    await publish(protectedRules({ requiredChecks: ["test"], queueDepth: 2 }));
    const first = await propose("one");
    const second = await propose("two");
    await enter(first);
    await enter(second);

    const both = await run();
    assert.equal(both.built.length, 2, "both are built and published");

    // The branch stops taking chains that deep, so the second entry is one this
    // pass cannot build — with its record from the pass before still standing.
    await publish(protectedRules({ requiredChecks: ["test"], queueDepth: 1 }));
    const shallower = await run();
    assert.deepEqual(
      shallower.unbuilt.map((entry: { pr: string }) => entry.pr),
      [second],
    );

    const state = JSON.parse(await cli(["queue", "show", "--root", root, "project", queue]));
    const held = state.entries.find((entry: { pr: string }) => entry.pr === second);
    assert.notEqual(held.candidate, null, "the record still names a branch");
    const branch = await inRepo(
      Effect.flatMap(GitRepository.Repository, (repository) =>
        repository.resolve(held.candidate.branch),
      ),
    );
    assert.notEqual(branch, null, "and the branch it names is still there");
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
