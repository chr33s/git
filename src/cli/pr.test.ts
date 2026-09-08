/**
 * Pull requests, driven end to end from the command line.
 *
 * The lifecycle worth proving: a pushed revision becomes a pull request, a
 * conversation and checks accumulate against it, and `pr merge` both moves
 * the base branch and records `pr.merged` — the projection then reporting
 * exactly what the repository's refs say happened.
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
import { enableHubUnder, opensshPrivateKey } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

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

describe("cli pr", () => {
  let root = "";
  let key = "";
  let head = "";

  const repoLayer = () =>
    GitRepository.layer.pipe(
      Layer.provide(GitRepository.hooksNoop),
      Layer.provide(nodeStores(path.join(root, "project"))),
    );

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-pr-"));
    key = path.join(root, "member");
    await cli(["init", "--root", root, "project"]);
    const fixture = await enableHubUnder(root, "project", [
      "repo.read",
      "source.push",
      "hub.create-pr",
      "hub.review",
      "hub.approve",
      "hub.comment",
      "hub.check:test",
      "hub.merge",
    ]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "member@example.com"), {
      mode: 0o600,
    });

    // A base branch and a topic ahead of it, straight through the library —
    // the CLI's own `work` verbs need a checkout this bare fixture is not.
    head = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const readme = yield* repository.writeBlob(new TextEncoder().encode("# project\n"));
        const base = yield* repository.writeTree([
          { mode: "100644", name: "readme.md", oid: readme },
        ]);
        yield* repository.commit({ branch: "main", tree: base, message: "first", author });

        const feature = yield* repository.writeBlob(new TextEncoder().encode("# changed\n"));
        const proposed = yield* repository.writeTree([
          { mode: "100644", name: "readme.md", oid: feature },
        ]);
        const main = yield* repository.resolve("refs/heads/main");
        return yield* repository.commitTree({
          tree: proposed,
          parents: main === null ? [] : [main],
          message: "propose a change\n",
          author,
        });
      }).pipe(Effect.provide(repoLayer())),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        // SAFETY: `head` is the oid `commitTree` returned a moment ago.
        yield* repository.setRef({ name: "refs/heads/topic", to: head });
      }).pipe(Effect.provide(repoLayer())),
    );
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const openPr = async () =>
    (
      await cli([
        "pr",
        "open",
        "--root",
        root,
        "--key",
        key,
        "--title",
        "propose a change",
        "--base",
        "main",
        "--head",
        "topic",
        "project",
      ])
    ).trim();

  it.live("refuses absent pull requests before appending records", () =>
    Effect.promise(async () => {
      for (const [verb, flags] of [
        ["update", ["--head", "topic"]],
        ["close", []],
        ["reopen", []],
        ["review", ["--head", "topic", "--decision", "approve"]],
        ["dismiss", ["--review", "missing"]],
        ["comment", ["--body", "A comment"]],
        ["reply", ["--thread", "missing", "--body", "A reply"]],
        ["resolve", ["--thread", "missing"]],
        ["reopen-thread", ["--thread", "missing"]],
        ["check", ["--head", "topic", "--name", "test", "--status", "success"]],
        ["merge", []],
      ] as const) {
        const pr = `missing-${verb}`;
        assert.match(
          await failing(["pr", verb, "--root", root, "--key", key, ...flags, "project", pr]),
          /no pull request/,
        );
        await assert.rejects(fs.readFile(path.join(root, "project", "refs", "hub", "pr", pr)), {
          code: "ENOENT",
        });
      }
    }),
  );

  it.live("refuses replies and dismissals naming absent discussions", () =>
    Effect.promise(async () => {
      const pr = await openPr();
      const ref = path.join(root, "project", "refs", "hub", "pr", pr);
      const before = await fs.readFile(ref, "utf8");
      for (const [verb, flags] of [
        ["reply", ["--thread", "missing", "--body", "A reply"]],
        ["resolve", ["--thread", "missing"]],
        ["reopen-thread", ["--thread", "missing"]],
        ["dismiss", ["--review", "missing"]],
      ] as const) {
        assert.match(
          await failing(["pr", verb, "--root", root, "--key", key, ...flags, "project", pr]),
          /no (thread|review)/,
        );
        assert.equal(await fs.readFile(ref, "utf8"), before);
      }
    }),
  );

  it.effect("opens, discusses, checks and shows a pull request", () =>
    Effect.promise(async () => {
      const pr = await openPr();

      const listed = JSON.parse(await cli(["pr", "list", "--root", root, "project"]));
      assert.equal(listed.length, 1);
      assert.equal(listed[0].id, pr);
      assert.equal(listed[0].state, "open");
      assert.equal(listed[0].head, head);

      await cli([
        "pr",
        "comment",
        "--root",
        root,
        "--key",
        key,
        "--body",
        "looks plausible",
        "project",
        pr,
      ]);
      await cli([
        "pr",
        "check",
        "--root",
        root,
        "--key",
        key,
        "--name",
        "test",
        "--head",
        "topic",
        "--status",
        "started",
        "project",
        pr,
      ]);
      await cli([
        "pr",
        "check",
        "--root",
        root,
        "--key",
        key,
        "--name",
        "test",
        "--head",
        "topic",
        "--status",
        "success",
        "project",
        pr,
      ]);

      const shown = JSON.parse(await cli(["pr", "show", "--root", root, "project", pr]));
      assert.equal(shown.threads.length, 1);
      assert.equal(shown.threads[0].comments[0].body, "looks plausible");
      assert.equal(shown.checks.at(-1)?.status, "success");

      const thread = shown.threads[0].id;
      await cli(["pr", "resolve", "--root", root, "--key", key, "--thread", thread, "project", pr]);
      const resolved = JSON.parse(await cli(["pr", "show", "--root", root, "project", pr]));
      assert.equal(resolved.threads[0].resolved, true);
      await cli([
        "pr",
        "reply",
        "--root",
        root,
        "--key",
        key,
        "--thread",
        thread,
        "--body",
        "Follow-up",
        "project",
        pr,
      ]);
      await cli([
        "pr",
        "reopen-thread",
        "--root",
        root,
        "--key",
        key,
        "--thread",
        thread,
        "project",
        pr,
      ]);
      await cli([
        "pr",
        "review",
        "--root",
        root,
        "--key",
        key,
        "--decision",
        "approve",
        "project",
        pr,
      ]);
      const reviewed = JSON.parse(await cli(["pr", "show", "--root", root, "project", pr]));
      assert.equal(reviewed.threads[0].comments.at(-1).body, "Follow-up");
      assert.equal(reviewed.threads[0].resolved, false);
      const review = reviewed.reviews[0].id;
      await cli(["pr", "dismiss", "--root", root, "--key", key, "--review", review, "project", pr]);
      const dismissed = JSON.parse(await cli(["pr", "show", "--root", root, "project", pr]));
      assert.equal(dismissed.reviews[0].dismissed, true);
    }),
  );

  it.effect("merges an open pull request and records the merge as an event", () =>
    Effect.promise(async () => {
      const pr = await openPr();

      const merged = (
        await cli(["pr", "merge", "--root", root, "--key", key, "project", pr])
      ).trim();
      assert.match(merged, /^[0-9a-f]{40}$/);

      // The branch really moved, and the projection says merged — from the
      // event, not from anybody's local bookkeeping.
      const main = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          return yield* repository.resolve("refs/heads/main");
        }).pipe(Effect.provide(repoLayer())),
      );
      assert.equal(main, merged);

      const shown = JSON.parse(await cli(["pr", "show", "--root", root, "project", pr]));
      assert.equal(shown.state, "merged");
      assert.equal(shown.mergeCommit, merged);

      const open = JSON.parse(await cli(["pr", "list", "--root", root, "project"]));
      assert.equal(open.length, 0);
      const all = JSON.parse(await cli(["pr", "list", "--root", root, "--all", "project"]));
      assert.equal(all.length, 1);
    }),
  );
});
