import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import { afterEach, beforeEach, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Node from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { knowledgeFixture, draftPath } from "../testing/Knowledge.ts";
import { opensshPrivateKey } from "../testing/Hub.ts";
import * as Host from "./Maintenance.node.ts";
import * as Session from "../hub/Session.ts";
import * as Memory from "../hub/Memory.ts";
import * as Trust from "../trust/Projection.ts";

let root = "";
let directory = "";
let repository: Repository["Service"];
const execute = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(effect.pipe(Effect.provideService(Repository, repository)));
beforeEach(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "knowledge-assisted-"));
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
});
afterEach(async () => {
  await fs.rm(root, { recursive: true, force: true });
});

const configure = async (
  script: string,
  key: string,
  actors: ReadonlyArray<{ fingerprint: string; kind: "human" | "bot" | "unknown" }>,
  timeoutMs = 3000,
) => {
  const location = path.join(directory, "maintenance");
  await fs.mkdir(location, { recursive: true, mode: 0o700 });
  await fs.writeFile(
    path.join(location, "config.json"),
    JSON.stringify({
      version: 1,
      sourceRef: "refs/heads/main",
      actors,
      runner: {
        id: "fixture",
        version: "1",
        executable: process.execPath,
        args: ["--input-type=module", "-e", script],
        timeoutMs,
      },
      publisher: { key },
    }),
    { mode: 0o600 },
  );
};
const readInput =
  "let input=''; for await (const chunk of process.stdin) input+=chunk; const request=JSON.parse(input);";

it.live(
  "one evidenced outcome creates an inspectable local draft; repeated evaluations reuse interpretation and source commits",
  () =>
    Effect.promise(async () => {
      const f = await execute(knowledgeFixture(true));
      const key = path.join(root, "publisher-key");
      await fs.writeFile(key, opensshPrivateKey(f.root, "publisher@example.com"), { mode: 0o600 });
      const counter = path.join(root, "runner-calls");
      const script = `import * as fs from 'node:fs'; ${readInput}
    if ('SSH_AUTH_SOCK' in process.env || 'CHR33S_TEST_GIT_KEY' in process.env) throw new Error('credential environment inherited');
    fs.appendFileSync(${JSON.stringify(counter)}, 'called\\n');
    const outcome=request.evidence.outcomes.find(outcome=>outcome.eligible);
    const draft={path:${JSON.stringify(draftPath)},status:'draft',type:'Convention',title:'Auth comparison',description:'Use constant time comparison in the auth module.',body:'Use a constant time comparison primitive in src/auth.ts.',claim:'scoped-observation',scope:['src/auth.ts'],outcomes:[outcome.key],evidence:outcome.sources.map(source=>source.record)};
    process.stdout.write(JSON.stringify({version:1,assessments:[],drafts:[draft]}));`;
      await configure(script, key, f.evidence.actors);
      const previous = process.env.CHR33S_TEST_GIT_KEY;
      process.env.CHR33S_TEST_GIT_KEY = "a private host credential marker";
      try {
        const report = await execute(
          Host.run({
            directory,
            sourceRef: "refs/heads/main",
            evaluatedAt: "2026-10-29T00:00:00Z",
          }),
        );
        assert.equal(report.exitCode, 0);
        assert.equal(report.job.assistance.state, "complete");
        assert.equal(report.job.assistance.output?.drafts[0]?.status, "draft");
        assert.equal(
          (await execute(repository.refs)).filter(([ref]) =>
            ref.startsWith("refs/heads/agent-memory/"),
          ).length,
          0,
        );
        const proposed = await execute(Host.run({ directory, job: report.job.id, propose: true }));
        assert.equal(proposed.exitCode, 0);
        assert.equal(proposed.job.publication, "written");
        assert.ok(proposed.job.candidate);
        const ref = `refs/heads/agent-memory/${proposed.job.candidate}`;
        const original = await execute(repository.resolve(ref));
        assert.ok(original);
        const candidate = await execute(repository.readCommit(original));
        const file = await execute(repository.findPath(candidate.tree, draftPath));
        assert.ok(file);
        const markdown = new TextDecoder().decode(await execute(repository.readBlob(file.oid)));
        assert.match(markdown, /status: draft/u);
        assert.ok(markdown.includes(f.pr));
        const repeated = await execute(
          Host.run({
            directory,
            sourceRef: "refs/heads/main",
            evaluatedAt: "2026-10-29T00:30:00Z",
            propose: true,
          }),
        );
        assert.equal(repeated.exitCode, 0);
        assert.equal(repeated.job.publication, "unchanged");
        assert.equal(await execute(repository.resolve(ref)), original);
        assert.equal((await fs.readFile(counter, "utf8")).trim().split("\n").length, 1);
        const session = await execute(Session.project(`memory-${proposed.job.candidate}`));
        assert.equal(session.notes.length, 0);
        assert.equal(session.commits.length, 2);
        const recalled = await execute(
          Memory.derive({
            sourceRef: ref,
            repo: f.genesis.repoId,
            trust: await execute(Trust.project(f.genesis)),
          }),
        );
        assert.ok(!recalled.entries.some((entry) => entry.source?.path === draftPath));
      } finally {
        if (previous === undefined) delete process.env.CHR33S_TEST_GIT_KEY;
        else process.env.CHR33S_TEST_GIT_KEY = previous;
      }
    }),
);

it.live(
  "invalid model output preserves deterministic results and creates no source session or branch",
  () =>
    Effect.promise(async () => {
      const f = await execute(knowledgeFixture());
      await configure(
        `${readInput} process.stdout.write(JSON.stringify({version:1,assessments:[],drafts:[{status:'stable',path:'AGENTS.md'}]}));`,
        path.join(root, "unused-key"),
        [],
      );
      const before = await execute(repository.refs);
      const result = await execute(
        Host.run({
          directory,
          sourceRef: "refs/heads/main",
          evaluatedAt: "2026-10-29T00:00:00Z",
          propose: true,
        }),
      );
      assert.equal(result.exitCode, 2);
      assert.equal(result.job.assistance.state, "partial");
      assert.equal(result.job.results.length, 1);
      assert.equal(result.job.outcomes[0]?.eligible, true);
      assert.deepEqual(await execute(repository.refs), before);
      assert.equal(await execute(repository.resolve(`refs/hub/session/memory-${f.pr}`)), null);
    }),
);

it.live("dry-run never executes the configured runner", () =>
  Effect.promise(async () => {
    const f = await execute(knowledgeFixture());
    const marker = path.join(root, "model-ran");
    await configure(
      `import * as fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(marker)},'ran'); process.stdout.write('{}');`,
      path.join(root, "unused-key"),
      f.evidence.actors,
    );
    const result = await execute(
      Host.run({
        directory,
        sourceRef: "refs/heads/main",
        evaluatedAt: "2026-10-29T00:00:00Z",
        dryRun: true,
      }),
    );
    assert.equal(result.exitCode, 0);
    await assert.rejects(fs.stat(marker), { code: "ENOENT" });
    await assert.rejects(fs.stat(path.join(directory, "maintenance", "history.json")), {
      code: "ENOENT",
    });
  }),
);

for (const failure of ["timeout", "oversized"] as const) {
  it.live(`a ${failure} runner is killed and leaves deterministic evidence available`, () =>
    Effect.promise(async () => {
      const f = await execute(knowledgeFixture());
      const marker = path.join(root, "runner-pid");
      await configure(
        `import * as fs from 'node:fs'; ${readInput}
        fs.writeFileSync(${JSON.stringify(marker)}, String(process.pid));
        process.on('SIGTERM', () => {});
        ${failure === "oversized" ? "process.stdout.write('x'.repeat(2 * 1024 * 1024));" : ""}
        setInterval(() => {}, 1000);`,
        path.join(root, "unused-key"),
        f.evidence.actors,
        1000,
      );
      const refs = await execute(repository.refs);
      const result = await execute(
        Host.run({
          directory,
          sourceRef: "refs/heads/main",
          evaluatedAt: "2026-10-29T00:00:00Z",
          propose: true,
        }),
      );
      assert.equal(result.exitCode, 2);
      assert.equal(result.job.assistance.state, "partial");
      assert.equal(result.job.publication, "pending");
      assert.equal(result.job.results.length, 1);
      assert.equal(result.job.outcomes[0]?.eligible, true);
      assert.deepEqual(await execute(repository.refs), refs);
      const pid = Number(await fs.readFile(marker, "utf8"));
      assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    }),
  );
}

it.live(
  "the pass deadline checkpoints unfinished publication and a resumed pass reuses interpretation",
  () =>
    Effect.promise(async () => {
      const f = await execute(knowledgeFixture(true));
      const key = path.join(root, "publisher-key");
      await fs.writeFile(key, opensshPrivateKey(f.root, "publisher@example.com"), { mode: 0o600 });
      await configure(
        `${readInput} process.stdout.write(${JSON.stringify(JSON.stringify(f.output))});`,
        key,
        f.evidence.actors,
      );
      const blocked = Repository.of({
        ...repository,
        setRef: (input) =>
          input.name.startsWith("refs/heads/agent-memory/")
            ? Effect.never
            : repository.setRef(input),
      });
      const partial = await Effect.runPromise(
        Host.run({
          directory,
          sourceRef: "refs/heads/main",
          evaluatedAt: "2026-10-29T00:00:00Z",
          timeoutMs: 2000,
          propose: true,
        }).pipe(Effect.provideService(Repository, blocked)),
      );
      assert.equal(partial.exitCode, 2);
      assert.equal(partial.job.publication, "pending");
      assert.equal(partial.job.assistance.state, "complete");
      assert.ok(partial.job.candidate);
      const ref = `refs/heads/agent-memory/${partial.job.candidate}`;
      assert.equal(await execute(repository.readRef(ref)), null);
      const completed = await execute(Host.run({ directory, job: partial.job.id, propose: true }));
      assert.equal(completed.exitCode, 0);
      assert.equal(completed.job.publication, "written");
      assert.equal(completed.job.candidate, partial.job.candidate);
      assert.equal(
        (await execute(Session.project(`memory-${partial.job.candidate}`))).commits.length,
        2,
      );
    }),
);

it.live("an empty validated model result creates neither a publisher session nor a branch", () =>
  Effect.promise(async () => {
    const f = await execute(knowledgeFixture());
    await configure(
      `${readInput} process.stdout.write(JSON.stringify({version:1,assessments:[],drafts:[]}));`,
      path.join(root, "nonexistent-key"),
      f.evidence.actors,
    );
    const before = await execute(repository.refs);
    const result = await execute(
      Host.run({
        directory,
        sourceRef: "refs/heads/main",
        evaluatedAt: "2026-10-29T00:00:00Z",
        propose: true,
      }),
    );
    assert.equal(result.exitCode, 0);
    assert.equal(result.job.assistance.state, "complete");
    assert.equal(result.job.publication, "no-proposal");
    assert.equal(result.job.candidate, null);
    assert.deepEqual(await execute(repository.refs), before);
  }),
);
