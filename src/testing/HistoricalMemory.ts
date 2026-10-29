/** Synthetic replay evidence. This never attributes historical commits to a real agent. */
import { Effect } from "effect";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { Repository } from "../git/Repository.ts";
import { qualify } from "../git/Oid.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Log from "../trust/Log.ts";
import * as Session from "../hub/Session.ts";
import * as PR from "../hub/PullRequest.ts";
import * as Event from "../hub/Event.ts";
import * as Inputs from "../knowledge/Inputs.ts";
import * as Outcomes from "../knowledge/Outcomes.ts";
import * as Proposal from "../knowledge/Proposal.ts";
import * as Publisher from "../knowledge/Publisher.ts";
import { Invalid } from "../git/Error.ts";

export const fixture = Effect.fn("testing.HistoricalMemory.fixture")(function* (input: {
  readonly caseId: string;
  readonly historicalBase: string;
  readonly historicalFix: string;
  readonly before: ReadonlyArray<{ readonly path: string; readonly content: Uint8Array }>;
  readonly after: ReadonlyArray<{ readonly path: string; readonly content: Uint8Array }>;
  readonly lesson: string;
}) {
  const repository = yield* Repository;
  const root = yield* generate("evaluation-fixture-operator@example.invalid");
  const agent = yield* generate("evaluation-fixture-replay@example.invalid");
  const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
  yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
  for (const [key, capabilities] of [
    [
      root,
      ["source.push", "hub.session", "hub.create-pr", "hub.review", "hub.merge", "hub.redact"],
    ],
    [agent, ["hub.session", "hub.create-pr"]],
  ] as const)
    yield* Log.issue(
      yield* Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(key.publicKey),
        capabilities,
        id: Log.newId(),
      }),
      [root],
    );
  const author = {
    name: "Synthetic historical replay",
    email: "replay@example.invalid",
    at: new Date(),
    offset: 0,
  };
  const tree = yield* repository.writeFiles({
    changes: input.before.map((file) => ({ ...file, mode: "100644" })),
  });
  const base = yield* repository.commitTree({
    tree,
    parents: [],
    author,
    message: `Synthetic snapshot of ${input.historicalBase}\n`,
  });
  yield* repository.setHead("refs/heads/main");
  yield* repository.setRef({ name: "refs/heads/main", to: base, expected: null });
  const opened = yield* Session.open({
    repo: genesis.repoId,
    key: agent,
    agent: {
      kind: "historical-evaluation-fixture",
      model: "deterministic-replay",
      harness: "fixture",
    },
    prompt: `Replay selected paths of ${input.historicalFix}; this is synthetic evidence, not original historical authorship.`,
  });
  const acceptedTree = yield* repository.writeFiles({
    base: tree,
    changes: input.after.map((file) => ({ ...file, mode: "100644" })),
  });
  const accepted = yield* repository.commitTree({
    tree: acceptedTree,
    parents: [base],
    author,
    message: `Synthetic replay of ${input.historicalFix}\n\nSession: ${opened.session}\n`,
  });
  const { pr } = yield* PR.open({
    repo: genesis.repoId,
    key: agent,
    title: `Synthetic ${input.caseId}`,
    base: "refs/heads/main",
    head: accepted,
  });
  yield* PR.review({
    repo: genesis.repoId,
    key: root,
    pr,
    head: accepted,
    decision: "comment",
    body: `Benchmark fixture interpretation of the historical fix: ${input.lesson}`,
  });
  const output = yield* Session.produced({
    repo: genesis.repoId,
    key: agent,
    session: opened.session,
    commits: [accepted],
    pulls: [pr],
    refs: ["refs/heads/main"],
    note: `gotcha: ${input.lesson}`,
  });
  yield* repository.setRef({ name: "refs/heads/main", to: accepted, expected: base });
  yield* PR.merged({ repo: genesis.repoId, key: root, pr, head: accepted, mergeCommit: accepted });
  return { ...input, root, agent, genesis, session: opened.session, output, accepted, pr };
});

/** The fixture operator reviews a predetermined lesson; the maintenance worker only writes drafts. */
export const curate = Effect.fn("testing.HistoricalMemory.curate")(function* (
  held: Effect.Success<ReturnType<typeof fixture>>,
  evaluatedAt: string,
) {
  const repository = yield* Repository;
  const manifest = yield* Inputs.capture({
    sourceRef: "refs/heads/main",
    evaluatedAt,
    limits: { concepts: 25, sessions: 512, bytes: 1048576 },
    configuration: { fixture: "historical-replay/1" },
  });
  const outcome = yield* Outcomes.evaluate({ manifest, ref: Event.refOf(held.pr), actors: [] });
  if (!outcome.eligible)
    return yield* new Invalid({
      field: "evaluation",
      reason: "synthetic accepted outcome is ineligible",
    });
  const path = `${Proposal.ROOT}${held.caseId}.md`;
  const evidence: Proposal.Evidence = {
    version: 1,
    manifest,
    actors: [],
    concepts: [],
    outcomes: [outcome],
  };
  const output: Proposal.Output = {
    version: 1,
    assessments: [],
    drafts: [
      {
        path,
        status: "draft",
        type: "Convention",
        title: held.caseId,
        description: held.lesson,
        body: held.lesson,
        claim: "scoped-observation",
        scope: outcome.paths,
        outcomes: [outcome.key],
        evidence: [qualify(held.output)],
      },
    ],
  };
  const prepared = yield* Publisher.prepare({
    evidence,
    output,
    runner: { id: "preregistered-fixture-curation", version: "1" },
    roots: [Proposal.ROOT],
    maximumBytes: 1048576,
  });
  if (prepared === null)
    return yield* new Invalid({
      field: "evaluation",
      reason: "fixture curation produced no draft",
    });
  const published = yield* Publisher.publish({
    state: prepared,
    key: held.root,
    actors: [],
    roots: [Proposal.ROOT],
    maximumBytes: 1048576,
    checkpoint: () => Effect.void,
  });
  if (!("commit" in published))
    return yield* new Invalid({
      field: "evaluation",
      reason: `fixture proposal ${published.state}`,
    });
  const commit = yield* repository.readCommit(published.commit);
  const file = yield* repository.findPath(commit.tree, path);
  if (file === null)
    return yield* new Invalid({ field: "evaluation", reason: "fixture draft is missing" });
  const draft = new TextDecoder().decode(yield* repository.readBlob(file.oid));
  // This is benchmark setup under the task's authorization, outside Maintenance.run.
  // The exact lesson is fixed from the historical reference criteria, without a human-preference claim.
  const reportCommit = commit.parents[0];
  if (reportCommit === undefined)
    return yield* new Invalid({
      field: "evaluation",
      reason: "fixture proposal has no report parent",
    });
  const events = (yield* Session.entries(prepared.session)).events;
  const reportOutput = events.find(
    (entry) =>
      entry.payload.type === "session.produced" &&
      entry.payload.note === null &&
      entry.payload.commits.includes(reportCommit),
  );
  if (reportOutput === undefined)
    return yield* new Invalid({
      field: "evaluation",
      reason: "fixture report citation is unavailable",
    });
  const dependencies = yield* Effect.forEach(held.after, (file) =>
    Effect.gen(function* () {
      const found = yield* repository.findPath(commit.tree, file.path);
      if (found === null)
        return yield* new Invalid({ field: "evaluation", reason: "fixture dependency is missing" });
      return { kind: "blob", path: file.path, blob: qualify(found.oid) };
    }),
  );
  const reviewed = draft
    .replace("status: draft", "status: stable")
    .replace(
      /  cites: .*\n/u,
      `  cites: ${JSON.stringify([qualify(reportOutput.commit), qualify(held.output)])}\n  evidence:\n${dependencies.map((dependency) => `    - kind: blob\n      path: ${JSON.stringify(dependency.path)}\n      blob: ${dependency.blob}\n`).join("")}`,
    );
  const reviewedTree = yield* repository.writeFiles({
    base: commit.tree,
    changes: [{ path, mode: "100644", content: new TextEncoder().encode(reviewed) }],
  });
  const promoted = yield* repository.commitTree({
    tree: reviewedTree,
    parents: [published.commit],
    author: {
      name: "Benchmark fixture preparer",
      email: "fixture@example.invalid",
      at: new Date(),
      offset: 0,
    },
    message: "Fixture-only review against fixed historical criteria\n",
  });
  yield* repository.setRef({ name: "refs/heads/main", to: promoted, expected: held.accepted });
  return { path, proposal: published, reviewedCommit: promoted, outcome, draft, reviewed };
});
