/** A native, signed agent outcome and authored Concept, shared by memory-publication tests. */
import { Effect, Schema } from "effect";
import { fingerprint, formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { Repository } from "../git/Repository.ts";
import { qualify } from "../git/Oid.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Session from "../hub/Session.ts";
import * as PR from "../hub/PullRequest.ts";
import * as Event from "../hub/Event.ts";
import * as Policy from "../server/Policy.ts";
import * as Inputs from "../knowledge/Inputs.ts";
import * as Maintenance from "../knowledge/Maintenance.ts";
import * as Outcomes from "../knowledge/Outcomes.ts";
import * as Proposal from "../knowledge/Proposal.ts";

export const authored = ".gitplus/knowledge/conventions/auth.md";
export const draftPath = ".gitplus/knowledge/maintained/auth.md";
const author = {
  name: "Agent",
  email: "agent@example.com",
  at: new Date("2026-10-28T00:00:00Z"),
  offset: 0,
};
export const knowledgeFixture = Effect.fn("testing.knowledgeFixture")(function* (
  provenance = false,
) {
  const repository = yield* Repository;
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");
  const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
  yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(root.publicKey),
      capabilities: [
        "source.push",
        "hub.session",
        "hub.create-pr",
        "hub.review",
        "hub.merge",
        "hub.redact",
      ],
      id: Log.newId(),
    }),
    [root],
  );
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(agent.publicKey),
      capabilities: ["hub.session", "hub.create-pr"],
      id: Log.newId(),
    }),
    [root],
  );
  const tree = yield* repository.writeFiles({
    changes: [
      {
        path: "src/auth.ts",
        mode: "100644",
        content: new TextEncoder().encode("export const equal = (a, b) => a === b\n"),
      },
      {
        path: authored,
        mode: "100644",
        content: new TextEncoder().encode(
          "---\ntype: Convention\ndescription: Compare credentials using constant time primitives.\n---\nAuth is a scoped module convention.\n",
        ),
      },
    ],
  });
  const base = yield* repository.commitTree({ tree, parents: [], message: "source\n", author });
  yield* repository.setRef({ name: "refs/heads/main", to: base, expected: null });
  const opened = yield* Session.open({
    repo: genesis.repoId,
    agent: { kind: "test-agent", model: "fixture", harness: "test" },
    prompt: "fix the auth comparison",
    key: agent,
  });
  const changed = yield* repository.writeFiles({
    base: tree,
    changes: [
      {
        path: "src/auth.ts",
        mode: "100644",
        content: new TextEncoder().encode(
          "export const equal = (a, b) => constantTimeEqual(a, b)\n",
        ),
      },
    ],
  });
  const head = yield* repository.commitTree({
    tree: changed,
    parents: [base],
    message: `fix auth comparison\n\nSession: ${opened.session}\n`,
    author,
  });
  const { pr } = yield* PR.open({
    repo: genesis.repoId,
    title: "Fix auth comparison",
    base: "refs/heads/main",
    head,
    key: agent,
  });
  yield* PR.review({
    repo: genesis.repoId,
    pr,
    head,
    decision: "comment",
    body: "Use a constant time primitive in the auth module.",
    key: root,
  });
  yield* Session.produced({
    repo: genesis.repoId,
    session: opened.session,
    commits: [head],
    pulls: [pr],
    refs: ["refs/heads/work"],
    key: agent,
    note: null,
  });
  yield* repository.setRef({ name: "refs/heads/main", to: head, expected: base });
  yield* PR.merged({ repo: genesis.repoId, pr, head, mergeCommit: head, key: root });
  if (provenance) {
    const blob = yield* repository.writeBlob(
      Policy.encodeRules({ ...Policy.OPEN, requireProvenance: true }),
    );
    const policy = yield* repository.commitTree({
      tree: yield* repository.writeTree([{ mode: "100644", name: Policy.RULES_PATH, oid: blob }]),
      parents: [],
      message: "policy\n",
      author,
    });
    yield* repository.setRef({ name: Policy.RULES_REF, to: policy, expected: null });
  }
  const manifest = yield* Inputs.capture({
    sourceRef: "refs/heads/main",
    evaluatedAt: "2026-10-29T00:00:00Z",
    limits: { concepts: 25, sessions: 512, bytes: 1048576 },
    configuration: { version: 1 },
  });
  const actors: Array<Outcomes.Actor> = [
    { fingerprint: yield* fingerprint(root.publicKey), kind: "human" },
  ];
  const outcome = yield* Outcomes.evaluate({ manifest, ref: Event.refOf(pr), actors });
  const checked = yield* Maintenance.check(manifest, {
    path: authored,
    blob: qualify((yield* repository.findPath(changed, authored))!.oid),
    reason: "unchecked",
  });
  const concept = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
    JSON.stringify(checked),
  );
  const evidence: Proposal.Evidence = {
    version: 1,
    manifest,
    actors,
    concepts: [concept],
    outcomes: [outcome],
  };
  const output: Proposal.Output = {
    version: 1,
    assessments: [],
    drafts: [
      {
        path: draftPath,
        status: "draft",
        type: "Convention",
        title: "Auth comparison",
        description: "Use constant time comparison in the auth module.",
        body: "Use a constant time comparison primitive in src/auth.ts.",
        claim: "scoped-observation",
        scope: ["src/auth.ts"],
        outcomes: [outcome.key],
        evidence: outcome.sources.map((source) => source.record),
      },
    ],
  };
  return { root, agent, genesis, pr, source: head, session: opened.session, evidence, output };
});
