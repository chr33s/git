import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";
import { TestClock } from "effect/testing";
import { Deferred, Effect, Fiber, Layer, Schema } from "effect";
import * as Node from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Host from "./Maintenance.node.ts";
import * as Maintenance from "./Maintenance.ts";
import * as Inputs from "./Inputs.ts";
import * as Journal from "./Journal.node.ts";
import * as Timer from "./MaintenanceTimer.node.ts";

const AT = "2026-10-28T09:00:00+07:00";
const author = {
  name: "Maintainer",
  email: "maintainer@example.com",
  at: new Date("2026-10-01T00:00:00Z"),
  offset: 0,
};
const concept = (name: string, deadline?: string) =>
  `---\ntype: Convention\ntitle: ${name}\ndescription: Keep the ${name} invariant.\n${deadline === undefined ? "" : `stale_after: '${deadline}'\n`}---\nScoped knowledge.\n`;
const Evidence = Schema.Struct({
  semantic: Schema.Struct({ state: Schema.Literal("not-assessed") }),
  evidence: Schema.Struct({
    concepts: Schema.Array(Schema.Struct({ temporal: Schema.Struct({ state: Schema.String }) })),
  }),
});

describe("Node knowledge maintenance", () => {
  let root = "";
  let directory = "";
  let repository: Repository["Service"];
  const execute = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
    Effect.runPromise(effect.pipe(Effect.provideService(Repository, repository)));
  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "knowledge-maintenance-"));
    directory = path.join(root, "project");
    repository = await Effect.runPromise(
      Repository.pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provide(Node.stores(directory)),
          ),
        ),
      ),
    );
    const tree = await execute(
      repository.writeFiles({
        changes: ["a", "b", "c"].map((name) => ({
          path: `.gitplus/knowledge/conventions/${name}.md`,
          mode: "100644",
          content: new TextEncoder().encode(concept(name, "2026-11-01T00:00:00Z")),
        })),
      }),
    );
    await execute(
      repository.commit({ branch: "refs/heads/main", tree, message: "knowledge\n", author }),
    );
  });
  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it.live("durably coalesces requests and resumes bounded deterministic work without a model", () =>
    Effect.promise(async () => {
      const first = await execute(
        Host.request({ directory, repository: "project", trigger: "manual" }),
      );
      const duplicate = await execute(
        Host.request({ directory, repository: "project", trigger: "manual" }),
      );
      assert.ok(first.state === "requested" && duplicate.state === "requested");
      assert.equal(first.request.id, duplicate.request.id);
      assert.equal(duplicate.reused, true);
      const initial = await execute(
        Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: AT, maximumConcepts: 1 }),
      );
      assert.equal(initial.exitCode, 2);
      assert.equal(initial.job.cursor, 1);
      assert.equal(initial.job.results.length, 1);
      assert.equal((await execute(Host.show(directory, initial.job.id))).state, "partial");
      assert.deepEqual(await fs.readdir(path.join(directory, "maintenance", "requests")), []);
      const second = await execute(Host.run({ directory, job: initial.job.id }));
      assert.equal(second.job.cursor, 2, "resume retains the original one-Concept pass limit");
      const third = await execute(Host.run({ directory, job: initial.job.id, propose: true }));
      assert.equal(third.exitCode, 0);
      assert.equal(third.job.state, "reported");
      assert.equal(third.job.publication, "no-proposal");
      assert.equal(third.job.results.length, 3);
      for (const result of third.job.results)
        assert.equal(
          (await Effect.runPromise(Schema.decodeUnknownEffect(Evidence)(result))).semantic.state,
          "not-assessed",
        );
      assert.equal(
        (await execute(Host.run({ directory, job: initial.job.id }))).job.results.length,
        3,
      );
      assert.deepEqual(
        (await execute(repository.refs)).map(([ref]) => ref),
        ["refs/heads/main"],
        "deterministic proposal mode creates no maintenance session or proposal branch",
      );
    }),
  );

  it.live("a killed worker resumes its durable report cursor with the same job identity", () =>
    Effect.promise(async () => {
      const module = (name: string) => new URL(name, import.meta.url).href;
      const script = `import * as fs from 'node:fs';
        import * as path from 'node:path';
        import {Effect,Layer} from 'effect';
        import * as Node from ${JSON.stringify(module("../git/Node.ts"))};
        import * as GitRepository from ${JSON.stringify(module("../git/Repository.ts"))};
        import {Repository} from ${JSON.stringify(module("../git/Repository.ts"))};
        import * as Host from ${JSON.stringify(module("./Maintenance.node.ts"))};
        await Effect.runPromise(Effect.gen(function* () {
          const repository=yield* Repository;
          let stopped=false;
          const blocked=Repository.of({...repository,readBlob:oid=>Effect.gen(function* () {
            const jobs=${JSON.stringify(path.join(directory, "maintenance", "jobs"))};
            if (!stopped && fs.existsSync(jobs)) {
              const name=fs.readdirSync(jobs).find(name=>/^[a-f0-9]{64}\\.json$/.test(name));
              if(name!==undefined) {
                const job=JSON.parse(fs.readFileSync(path.join(jobs,name),'utf8'));
                if(job.cursor===1) { stopped=true; process.stdout.write(job.id); yield* Effect.never; }
              }
            }
            return yield* repository.readBlob(oid);
          })});
          yield* Host.run({directory:${JSON.stringify(directory)},sourceRef:'refs/heads/main',evaluatedAt:${JSON.stringify(AT)}}).pipe(Effect.provideService(Repository,blocked));
        }).pipe(Effect.provide(GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop),Layer.provide(Node.stores(${JSON.stringify(directory)}))))));`;
      const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      try {
        const ready = await Promise.race([
          once(child.stdout, "data").then(([data]) => String(data)),
          once(child, "exit").then(() => "exited-before-checkpoint"),
        ]);
        assert.match(ready, /^[a-f0-9]{64}$/u);
        const exit = once(child, "exit");
        child.kill("SIGKILL");
        await exit;
        const retained = await execute(Host.show(directory, ready));
        assert.equal(retained.cursor, 1);
        assert.equal(retained.results.length, 1);
        assert.equal(retained.state, "running");
        const resumed = await execute(Host.run({ directory, job: ready }));
        assert.equal(resumed.exitCode, 0);
        assert.equal(resumed.job.id, ready);
        assert.equal(resumed.job.results.length, 3);
        assert.deepEqual(resumed.job.results[0], retained.results[0]);
      } finally {
        child.kill("SIGKILL");
      }
    }),
  );

  it.live("dry-run creates no journal and does not acknowledge requests", () =>
    Effect.promise(async () => {
      const preview = await execute(
        Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: AT, dryRun: true }),
      );
      assert.equal(preview.exitCode, 0);
      assert.equal(preview.job.results.length, 3);
      await assert.rejects(fs.stat(Journal.directory(directory)), { code: "ENOENT" });
      await execute(Host.request({ directory, repository: "project", trigger: "manual" }));
      const before = await fs.readdir(path.join(Journal.directory(directory), "requests"));
      await execute(
        Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: AT, dryRun: true }),
      );
      assert.deepEqual(
        await fs.readdir(path.join(Journal.directory(directory), "requests")),
        before,
      );
      assert.deepEqual(await fs.readdir(path.join(Journal.directory(directory), "jobs")), []);
    }),
  );

  it.effect(
    "the direct timer checks quiet refs after a deadline and exposes each completed pass",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(new Date("2026-10-31T00:00:00Z").getTime());
        const first = yield* Deferred.make<Timer.Result>();
        const second = yield* Deferred.make<Timer.Result>();
        let passes = 0;
        const timer = yield* Timer.run({
          directory,
          sourceRef: "refs/heads/main",
          intervalMs: 2 * 24 * 60 * 60 * 1000,
          onResult: (result) => Deferred.succeed(++passes === 1 ? first : second, result),
        }).pipe(Effect.provideService(Repository, repository), Effect.forkChild);
        const before = yield* Deferred.await(first);
        assert.equal(before.exitCode, 0);
        yield* TestClock.adjust("2 days");
        const after = yield* Deferred.await(second);
        assert.equal(after.exitCode, 0);
        assert.notEqual(before.job.id, after.job.id);
        assert.equal(before.job.plan.manifest.sourceHead, after.job.plan.manifest.sourceHead);
        assert.ok(after.job.plan.chosen.every((entry) => entry.reason === "expired"));
        for (const result of after.job.results)
          assert.equal(
            (yield* Schema.decodeUnknownEffect(Evidence)(result)).evidence.concepts[0]?.temporal
              .state,
            "stale",
          );
        yield* Fiber.interrupt(timer);
      }),
  );

  it.live("checks deadlines on a quiet day without wake activity", () =>
    Effect.promise(async () => {
      await execute(Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: AT }));
      const source = await execute(repository.resolve("refs/heads/main"));
      const later = await execute(
        Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: "2026-11-02T00:00:00Z" }),
      );
      assert.equal(later.exitCode, 0);
      assert.equal(await execute(repository.resolve("refs/heads/main")), source);
      assert.ok(later.job.plan.chosen.every((entry) => entry.reason === "expired"));
      for (const result of later.job.results)
        assert.equal(
          (await Effect.runPromise(Schema.decodeUnknownEffect(Evidence)(result))).evidence
            .concepts[0]?.temporal.state,
          "stale",
        );
    }),
  );

  it.live("reports supersession while retaining evidence from its captured source view", () =>
    Effect.promise(async () => {
      const source = await execute(repository.resolve("refs/heads/main"));
      assert.ok(source);
      const original = await execute(repository.readCommit(source));
      const replacement = await execute(
        repository.commitTree({
          tree: original.tree,
          parents: [source],
          message: "another writer\n",
          author,
        }),
      );
      let moved = false;
      const racing = Repository.of({
        ...repository,
        readBlob: Effect.fn("test.moveSource")(function* (oid: Oid) {
          if (!moved) {
            moved = true;
            yield* repository
              .setRef({
                name: "refs/heads/main",
                to: replacement,
                expected: source,
              })
              .pipe(Effect.orDie);
          }
          return yield* repository.readBlob(oid);
        }),
      });
      const result = await Effect.runPromise(
        Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: AT }).pipe(
          Effect.provideService(Repository, racing),
        ),
      );
      assert.equal(result.exitCode, 3);
      assert.equal(result.job.publication, "superseded-input");
      assert.equal(result.job.plan.manifest.sourceHead, `sha1:${source}`);
      assert.equal(await execute(repository.resolve("refs/heads/main")), replacement);
    }),
  );

  it.live("rejects incompatible resume inputs without replacing the saved manifest", () =>
    Effect.promise(async () => {
      const initial = await execute(
        Host.run({ directory, sourceRef: "refs/heads/main", evaluatedAt: AT, maximumConcepts: 1 }),
      );
      const error = await execute(
        Host.run({ directory, job: initial.job.id, evaluatedAt: "2027-01-01T00:00:00Z" }).pipe(
          Effect.flip,
        ),
      );
      assert.equal(error._tag, "Invalid");
      assert.equal(
        (await execute(Host.show(directory, initial.job.id))).plan.manifest.evaluatedAt,
        initial.job.plan.manifest.evaluatedAt,
      );
    }),
  );

  it.live("a timer pass continues to select aging work under continuous changes", () =>
    Effect.promise(async () => {
      const selections: Array<Maintenance.Selection> = [
        { path: "old.md", blob: "old", reason: "aging" },
        ...Array.from({ length: 50 }, (_, index): Maintenance.Selection => ({
          path: `changed-${index}.md`,
          blob: "changed",
          reason: "changed",
        })),
        { path: "unchecked.md", blob: "unchecked", reason: "unchecked" },
      ];
      const normal = Maintenance.select(selections, [], 25);
      assert.ok(normal.chosen.some((entry) => entry.path === "old.md"));
      assert.ok(normal.chosen.some((entry) => entry.path === "unchecked.md"));
      const singles = [0, 1, 2].flatMap((round) =>
        Maintenance.select(selections, [], 1, round).chosen.map((entry) => entry.reason),
      );
      assert.deepEqual(singles, ["aging", "unchecked", "changed"]);
      const captured = await execute(
        Inputs.capture({
          sourceRef: "refs/heads/main",
          evaluatedAt: AT,
          limits: { concepts: 1, sessions: 512, bytes: 16384 },
          configuration: { version: 1 },
        }),
      );
      assert.equal(captured.view?.base, captured.sourceHead);
    }),
  );
  it.live("times out a bounded read and resumes from the retained checkpoint", () =>
    Effect.promise(async () => {
      const stalled = Repository.of({ ...repository, readBlob: () => Effect.never });
      const initial = await Effect.runPromise(
        Host.run({
          directory,
          sourceRef: "refs/heads/main",
          evaluatedAt: AT,
          timeoutMs: 1000,
        }).pipe(Effect.provideService(Repository, stalled)),
      );
      assert.equal(initial.exitCode, 2);
      assert.equal(initial.job.cursor, 0);
      const resumed = await execute(Host.run({ directory, job: initial.job.id }));
      assert.equal(resumed.exitCode, 0);
      assert.equal(resumed.job.results.length, 3);
    }),
  );

  it.live("cancellation checkpoints completed work and releases the local worker lock", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let reads = 0;
      const stalled = Repository.of({
        ...repository,
        readBlob: Effect.fn("test.interruptedRead")(function* (oid: Oid) {
          reads += 1;
          if (reads === 2) {
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.never;
          }
          return yield* repository.readBlob(oid);
        }),
      });
      const fiber = yield* Host.run({
        directory,
        sourceRef: "refs/heads/main",
        evaluatedAt: AT,
      }).pipe(Effect.provideService(Repository, stalled), Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const names = yield* Journal.names(path.join(Journal.directory(directory), "jobs"));
      assert.equal(names.length, 1);
      const id = names[0]!.slice(0, -5);
      const interrupted = yield* Host.show(directory, id);
      assert.equal(interrupted.state, "partial");
      assert.equal(interrupted.cursor, 1);
      assert.equal(interrupted.results.length, 1);
      const resumed = yield* Host.run({ directory, job: id }).pipe(
        Effect.provideService(Repository, repository),
      );
      assert.equal(resumed.exitCode, 0);
      assert.equal(resumed.job.results.length, 3);
    }),
  );
});
