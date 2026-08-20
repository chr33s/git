/**
 * `wake`, driven as an operator drives it: real repositories under a root, a
 * real rules file, and real child processes.
 *
 * End to end rather than unit, because what is worth checking is the property
 * the whole thing exists for — that an event wakes something exactly once
 * while it keeps working, and again when it did not.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import { enableHubUnder } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

/** The same call, for the runs that are supposed to fail. */
const failing = (args: ReadonlyArray<string>): Promise<string> =>
  cli(args).then(
    () => "",
    (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
  );

const inRepository = <A, E>(
  directory: string,
  effect: Effect.Effect<A, E, GitRepository.Repository>,
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(nodeStores(directory)),
        ),
      ),
    ),
  );

describe("cli wake", () => {
  let root = "";
  let project = "";
  let log = "";

  /** A rule that records what it was woken for, so the test can read it back. */
  const recorder = (
    on: ReadonlyArray<string>,
    body = `require('fs').appendFileSync(${JSON.stringify(
      "REPLACED",
    )}, process.env.CHR33S_GIT_EVENT + ' ' + process.env.CHR33S_GIT_REF + '\\n')`,
  ) => ({
    ref: "refs/hub/pr/*",
    on,
    run: [process.execPath, "-e", body.replace(JSON.stringify("REPLACED"), JSON.stringify(log))],
  });

  const writeRules = (rules: ReadonlyArray<unknown>) =>
    fs.writeFile(path.join(project, "wake.json"), JSON.stringify({ rules }, null, 2));

  const lines = async (): Promise<ReadonlyArray<string>> =>
    fs
      .readFile(log, "utf8")
      .then((contents) => contents.split("\n").filter((line) => line.length > 0))
      .catch(() => []);

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-wake-"));
    project = path.join(root, "project");
    log = path.join(root, "woken.log");
    await cli(["init", "--root", root, "project"]);
  });

  afterEach(async () => {
    // Retried, because this suite starts processes that outlive the assertion
    // they were started for: a file appearing mid-removal makes `rm` fail with
    // ENOTEMPTY, and a teardown that fails the run for that reports a defect
    // nobody has.
    for (let attempt = 0; ; attempt++) {
      const removed = await fs
        .rm(root, { recursive: true, force: true })
        .then(() => true)
        .catch(() => false);
      if (removed || attempt === 4) break;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  });

  /** A repository with an identity, a member, and one opened pull request. */
  const withPullRequest = async () => {
    const fixture = await enableHubUnder(root, "project", [
      "repo.read",
      "source.push",
      "hub.create-pr",
      "hub.comment",
    ]);
    const head = await inRepository(
      project,
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        return yield* repository.commit({
          branch: "refs/heads/main",
          tree: EMPTY_TREE_OID,
          message: "first",
          author: {
            name: "Dev",
            email: "dev@example.com",
            at: new Date(1_700_000_000_000),
            offset: 0,
          },
        });
      }),
    );
    const opened = await inRepository(
      project,
      PullRequest.open({
        repo: fixture.repoId,
        title: "Add a thing",
        base: "refs/heads/main",
        head,
        key: fixture.member,
      }),
    );
    return { ...fixture, head, pr: opened.pr };
  };

  it("wakes once for an event, and not again for the same one", async () => {
    await withPullRequest();
    await writeRules([recorder(["pr.opened"])]);

    const first = await cli(["wake", "--root", root, "project"]);
    assert.match(first, /1 rule\(s\) run/);
    const woken = await lines();
    assert.equal(woken.length, 1);
    assert.match(woken[0] ?? "", /^pr\.opened refs\/hub\/pr\//);

    // The cursor advanced, so the same state wakes nothing: a hook that fires
    // twice, or a timer racing a hook, must not start the work twice.
    const again = await cli(["wake", "--root", root, "project"]);
    assert.match(again, /0 rule\(s\) run/);
    assert.equal((await lines()).length, 1);
  });

  it("wakes for what arrived since the last run, and passes it through the environment", async () => {
    const where = await withPullRequest();
    await writeRules([recorder(["*"])]);
    await cli(["wake", "--root", root, "project"]);

    await inRepository(
      project,
      PullRequest.comment({
        repo: where.repoId,
        pr: where.pr,
        body: "looks right to me",
        key: where.member,
      }),
    );

    const second = await cli(["wake", "--root", root, "project"]);
    assert.match(second, /1 rule\(s\) run/);

    const woken = await lines();
    assert.equal(woken.length, 2, `one line per event: ${JSON.stringify(woken)}`);
    assert.match(woken[0] ?? "", /^pr\.opened refs\/hub\/pr\//);
    assert.match(woken[1] ?? "", /^comment\.created refs\/hub\/pr\//);
  });

  it("leaves the cursor where it was when a woken command fails, and replays it", async () => {
    await withPullRequest();
    await writeRules([
      { ref: "refs/hub/pr/*", on: ["*"], run: [process.execPath, "-e", "process.exit(3)"] },
    ]);

    const failed = await failing(["wake", "--root", root, "project"]);
    assert.match(failed, /exited 3/);

    // Replayed rather than lost. A woken command re-reads the refs anyway, so
    // arriving twice costs a wasted start while never arriving costs the work.
    await writeRules([recorder(["*"])]);
    const retried = await cli(["wake", "--root", root, "project"]);
    assert.match(retried, /1 rule\(s\) run/);
    assert.equal((await lines()).length, 1);
  });

  it("says what it would do without doing it", async () => {
    await withPullRequest();
    await writeRules([recorder(["*"])]);

    const dry = await cli(["wake", "--root", root, "--dry-run", "project"]);
    assert.match(dry, /would run/);
    assert.deepEqual(await lines(), [], "a dry run runs nothing");

    // And leaves the cursor alone, so the real run still has the event.
    const real = await cli(["wake", "--root", root, "project"]);
    assert.match(real, /1 rule\(s\) run/);
    assert.equal((await lines()).length, 1);
  });

  it("wakes from a push, when the host was told to", async () => {
    const where = await withPullRequest();
    await writeRules([recorder(["*"])]);

    // A client with something to push. What it pushes is a source branch —
    // the wake is a walk, not a delivery, so the push only has to happen.
    const client = path.join(root, "client");
    await fs.mkdir(client, { recursive: true });
    await cli(["init", "--root", client, "copy"]);
    await inRepository(
      path.join(client, "copy"),
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        yield* repository.commit({
          branch: "refs/heads/topic",
          tree: EMPTY_TREE_OID,
          message: "from the client",
          author: {
            name: "Dev",
            email: "dev@example.com",
            at: new Date(1_700_000_100_000),
            offset: 0,
          },
        });
      }),
    );

    const server = await serve({ root, wake: true });
    try {
      await cli([
        "push",
        "--root",
        client,
        "--token",
        where.credential,
        "copy",
        `${server.url}/project`,
        "topic",
      ]);

      // Forked off the response, so the push does not wait for it — which is
      // the point, and why this waits here instead.
      const deadline = Date.now() + 10_000;
      while ((await lines()).length === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      const woken = await lines();
      assert.equal(
        woken.length,
        1,
        `the push should have woken the rule: ${JSON.stringify(woken)}`,
      );
      assert.match(woken[0] ?? "", /^pr\.opened refs\/hub\/pr\//);

      // The rule having run is not the pass having finished: the fork advances
      // its bookmark afterwards, and a teardown that removed the directory in
      // between failed with ENOTEMPTY — intermittently, which is the worst way
      // for a suite to fail. The bookmark is the pass's last write, so waiting
      // for it is waiting for the fork to be done.
      const bookmark = path.join(project, "wake.cursor.json");
      while (
        !(await fs
          .access(bookmark)
          .then(() => true)
          .catch(() => false)) &&
        Date.now() < deadline
      ) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    } finally {
      await server.close();
    }
  });

  it("refuses a rule for a ref it never walks, and says when one matches nothing", async () => {
    await withPullRequest();

    // A rule outside the one namespace a wake walks can never fire, so it is
    // refused where it is written rather than accepted and ignored.
    await writeRules([{ ref: "refs/heads/main", on: ["*"], run: [process.execPath, "-e", ""] }]);
    const refused = await failing(["wake", "--root", root, "project"]);
    assert.match(refused, /watches a ref this never walks/);

    // And one that is in the namespace but matches nothing here — a typo the
    // file cannot catch — is reported instead of passing as "nothing to do".
    await writeRules([{ ref: "refs/hub/prs/*", on: ["*"], run: [process.execPath, "-e", ""] }]);
    const quiet = await cli(["wake", "--root", root, "project"]);
    assert.match(quiet, /watches nothing here/);
  });

  it("refuses rules it cannot read rather than treating them as none", async () => {
    await withPullRequest();
    await fs.writeFile(path.join(project, "wake.json"), "{ this is not json");

    const refused = await failing(["wake", "--root", root, "project"]);
    assert.match(refused, /not valid JSON/);
  });
});
