/**
 * Tasks, driven as a fleet drives them: opened, raced for, worked, closed.
 *
 * The property worth checking is the one a lease is for — that a claim tells
 * other agents to look elsewhere while it is live, and stops telling them that
 * the moment it expires or its holder lets go.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

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

describe("cli task", () => {
  let root = "";
  let key = "";
  let otherKey = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-task-"));
    key = path.join(root, "agent");
    await cli(["init", "--root", root, "project"]);
    const fixture = await enableHubUnder(root, "project", ["repo.read", "hub.task"]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "agent@example.com"), {
      mode: 0o600,
    });
    otherKey = path.join(root, "other-agent");
    await fs.writeFile(otherKey, opensshPrivateKey(fixture.root, "other@example.com"), {
      mode: 0o600,
    });
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const openTask = async (title: string) =>
    (await cli(["task", "open", "--root", root, "--key", key, "--title", title, "project"])).trim();

  for (const verb of ["release", "close", "reopen"]) {
    it.live(`reports failure when ${verb} is ignored by the task projection`, () =>
      Effect.promise(async () => {
        const task = await openTask("owned work");
        await cli(["task", "claim", "--root", root, "--key", key, "project", task]);
        if (verb === "reopen") {
          await cli(["task", "close", "--root", root, "--key", key, "project", task]);
        }
        const before = JSON.parse(await cli(["task", "show", "--root", root, "project", task]));
        const output = await failing([
          "task",
          verb,
          "--root",
          root,
          "--key",
          otherKey,
          "project",
          task,
        ]);
        assert.match(output, /event was ignored/, verb);
        const after = JSON.parse(await cli(["task", "show", "--root", root, "project", task]));
        assert.deepEqual(after.claim, before.claim, verb);
        assert.deepEqual(after.closed, before.closed, verb);
        assert.equal(after.ignored.length, before.ignored.length + 1, verb);
      }),
    );
  }

  it.live("refuses claims on missing or closed tasks without appending events", () =>
    Effect.promise(async () => {
      const task = await openTask("already finished");
      await cli(["task", "close", "--root", root, "--key", key, "project", task]);
      const ref = path.join(root, "project", "refs", "hub", "task", task);
      const before = await fs.readFile(ref, "utf8");
      for (const [id, reason] of [
        ["missing-task", /does not exist/],
        [task, /closed/],
      ] as const) {
        const result = await failing([
          "task",
          "claim",
          "--root",
          root,
          "--key",
          key,
          "project",
          id,
        ]);
        assert.match(result, reason);
      }
      assert.equal(await fs.readFile(ref, "utf8"), before);
      await assert.rejects(
        fs.access(path.join(root, "project", "refs", "hub", "task", "missing-task")),
      );
    }),
  );

  it.effect("offers a task until somebody takes it, and again once they let go", () =>
    Effect.promise(async () => {
      const task = await openTask("fix the flaky test");

      const available = JSON.parse(await cli(["task", "list", "--root", root, "project"]));
      assert.equal(available.length, 1);
      assert.equal(available[0].task, task);
      assert.equal(available[0].title, "fix the flaky test");

      await cli(["task", "claim", "--root", root, "--key", key, "--ttl", "3600", "project", task]);

      // What the next agent woken by this ref reads: nothing to pick up.
      assert.deepEqual(JSON.parse(await cli(["task", "list", "--root", root, "project"])), []);
      const claimed = JSON.parse(await cli(["task", "show", "--root", root, "project", task]));
      assert.notEqual(claimed.claim, null);
      assert.equal(claimed.available, false);

      // A second claimant is told so rather than appending a claim nobody
      // honours — advisory, but there is no reason to make it useless.
      const raced = await failing(["task", "claim", "--root", root, "--key", key, "project", task]);
      assert.match(raced, /already claimed/);

      await cli(["task", "release", "--root", root, "--key", key, "project", task]);
      const again = JSON.parse(await cli(["task", "list", "--root", root, "project"]));
      assert.equal(again.length, 1, "letting go puts it back");
    }),
  );

  it.effect("frees a task whose lease ran out, without anybody saying so", () =>
    Effect.promise(async () => {
      const task = await openTask("something abandoned");
      // A sandbox that dies holding a claim is the case this is for: nothing
      // releases it, and the work has to become available again anyway.
      //
      // Five seconds, not one: reading the list back spawns a second CLI, and a
      // lease short enough for that to outrun it made this assert that an
      // expired claim still holds — which is the opposite of what it checks.
      const ttl = 5;
      await cli([
        "task",
        "claim",
        "--root",
        root,
        "--key",
        key,
        "--ttl",
        String(ttl),
        "project",
        task,
      ]);
      // Measured after the claim landed, so it is never earlier than the expiry
      // the CLI wrote; waiting past it therefore always waits long enough.
      const claimed = Date.now();
      assert.deepEqual(JSON.parse(await cli(["task", "list", "--root", root, "project"])), []);

      const expiry = claimed + ttl * 1000 + 200;
      await new Promise((resolve) => setTimeout(resolve, Math.max(0, expiry - Date.now())));
      const after = JSON.parse(await cli(["task", "list", "--root", root, "project"]));
      assert.equal(after.length, 1, "an expired lease frees the work by doing nothing");
    }),
  );

  it.live("reports only the winning claim when local workers race", () =>
    Effect.promise(async () => {
      const task = await openTask("one worker should start");
      const outcomes = await Promise.allSettled(
        Array.from({ length: 8 }, () =>
          cli(["task", "claim", "--root", root, "--key", key, "project", task]),
        ),
      );
      const claimed = outcomes.filter((outcome) => outcome.status === "fulfilled");
      assert.equal(claimed.length, 1, "only one worker may report a successful claim");
    }),
  );

  it.live("refuses lifecycle changes to absent tasks without creating refs", () =>
    Effect.promise(async () => {
      for (const verb of ["release", "close", "reopen", "reparent"]) {
        const task = `missing-${verb}`;
        const output = await failing(["task", verb, "--root", root, "--key", key, "project", task]);
        assert.match(output, /does not exist/, verb);
        await assert.rejects(fs.readFile(path.join(root, "project", "refs", "hub", "task", task)), {
          code: "ENOENT",
        });
      }
    }),
  );

  it.effect("files work under a release, and moves it when the release slips", () =>
    Effect.promise(async () => {
      const v4 = await openTask("v0.4 — Identity");
      const v5 = await openTask("v0.5 — Scale");
      const work = (
        await cli([
          "task",
          "open",
          "--root",
          root,
          "--key",
          key,
          "--title",
          "sign events with the browser key",
          "--parent",
          v4,
          "project",
        ])
      ).trim();

      const filed = JSON.parse(await cli(["task", "show", "--root", root, "project", work]));
      assert.equal(filed.parent, v4);
      // A release is a task like any other, so it has no parent of its own and
      // shows up in the same listing.
      const milestone = JSON.parse(await cli(["task", "show", "--root", root, "project", v4]));
      assert.equal(milestone.parent, null);

      // The whole reason this is an event and not a field on `task.opened`:
      // the ref cannot be rewound, and work slips between releases anyway.
      await cli([
        "task",
        "reparent",
        "--root",
        root,
        "--key",
        key,
        "--parent",
        v5,
        "project",
        work,
      ]);
      const slipped = JSON.parse(await cli(["task", "show", "--root", root, "project", work]));
      assert.equal(slipped.parent, v5);

      // And out from under anything at all.
      await cli(["task", "reparent", "--root", root, "--key", key, "project", work]);
      const loose = JSON.parse(await cli(["task", "show", "--root", root, "project", work]));
      assert.equal(loose.parent, null);

      // An edge no reader could follow is refused where it is written.
      const reparent = (task: string, parent: string) =>
        failing([
          "task",
          "reparent",
          "--root",
          root,
          "--key",
          key,
          "--parent",
          parent,
          "project",
          task,
        ]);
      assert.match(await reparent(work, work), /cannot belong to itself/);

      // And a longer way round, which one record cannot see on its own: v0.4 is
      // above `work` only because `work` was filed under it a moment ago.
      await cli([
        "task",
        "reparent",
        "--root",
        root,
        "--key",
        key,
        "--parent",
        v4,
        "project",
        work,
      ]);
      assert.match(await reparent(v4, work), /would close a loop/);
    }),
  );

  it.effect("closes by saying so, and names what resolved it", () =>
    Effect.promise(async () => {
      const task = await openTask("ship the thing");
      await cli([
        "task",
        "close",
        "--root",
        root,
        "--key",
        key,
        "--outcome",
        "completed",
        "--session",
        "0198f2aa-71c4-7d2e-9a3b-4c5d6e7f8a9b",
        "project",
        task,
      ]);

      const state = JSON.parse(await cli(["task", "show", "--root", root, "project", task]));
      assert.equal(state.closed.outcome, "completed");
      assert.equal(state.available, false);
      assert.deepEqual(state.sessions, ["0198f2aa-71c4-7d2e-9a3b-4c5d6e7f8a9b"]);

      // Closed is a thing said, not a ref removed: the history is still there.
      assert.deepEqual(JSON.parse(await cli(["task", "list", "--root", root, "project"])), []);
      const all = JSON.parse(await cli(["task", "list", "--root", root, "--all", "project"]));
      assert.equal(all.length, 1);
      await cli(["task", "reopen", "--root", root, "--key", key, "project", task]);
      const reopened = JSON.parse(await cli(["task", "show", "--root", root, "project", task]));
      assert.equal(reopened.closed, null);
      assert.equal(reopened.available, true);
    }),
  );
});
