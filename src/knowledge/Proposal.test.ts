import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { qualify } from "../git/Oid.ts";
import * as Pack from "../context/Pack.ts";
import * as Trust from "../trust/Projection.ts";
import * as Event from "../hub/Event.ts";
import * as Check from "./Check.ts";
import * as Inputs from "./Inputs.ts";
import * as Outcomes from "./Outcomes.ts";
import * as Proposal from "./Proposal.ts";
import * as Publisher from "./Publisher.ts";
import { knowledgeFixture, draftPath } from "../testing/Knowledge.ts";

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);
for (const path of [
  "AGENTS.md",
  ".gitplus/knowledge/maintained/../../CLAUDE.md",
  ".gitplus/knowledge/AGENTS.md",
  ".gitplus/knowledge/maintained/policy.md",
  ".gitplus/knowledge/reports/report.md",
  ".gitplus/wake.json",
]) {
  it.effect(`rejects ${path} before any ref or session changes`, () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture();
      const repository = yield* Repository;
      const before = yield* repository.refs;
      const output = { ...f.output, drafts: f.output.drafts.map((draft) => ({ ...draft, path })) };
      const error = yield* Publisher.prepare({
        evidence: f.evidence,
        output,
        runner: { id: "fixture", version: "1" },
        roots: [".gitplus/knowledge/"],
        maximumBytes: 1048576,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "Invalid");
      assert.deepEqual(yield* repository.refs, before);
    }).pipe(Effect.provide(world)),
  );
}

it.effect("rejects omitted draft status at the model-output decoding boundary", () =>
  Effect.gen(function* () {
    const f = yield* knowledgeFixture();
    const repository = yield* Repository;
    const before = yield* repository.refs;
    const missing = {
      ...f.output,
      drafts: f.output.drafts.map(({ status: _status, ...rest }) => rest),
    };
    const error = yield* Schema.decodeUnknownEffect(Proposal.Output)(missing).pipe(Effect.flip);
    assert.equal(error._tag, "SchemaError");
    assert.deepEqual(yield* repository.refs, before);
  }).pipe(Effect.provide(world)),
);

for (const [path, mode] of [
  [".gitplus/knowledge/maintained", "120000"],
  [draftPath, "100755"],
  [draftPath, "120000"],
] as const) {
  it.effect(`rejects a ${mode} entry at ${path}`, () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture();
      const repository = yield* Repository;
      const source = yield* repository.readCommit(f.source);
      const tree = yield* repository.writeFiles({
        base: source.tree,
        changes: [{ path, mode, content: new TextEncoder().encode("elsewhere") }],
      });
      const commit = yield* repository.commitTree({
        tree,
        parents: [f.source],
        message: "unsafe path\n",
        author: source.author,
      });
      yield* repository.setRef({ name: "refs/heads/main", to: commit, expected: f.source });
      const manifest = yield* Inputs.capture({
        sourceRef: "refs/heads/main",
        evaluatedAt: "2026-10-29T00:00:00Z",
        limits: { concepts: 25, sessions: 512, bytes: 1048576 },
        configuration: { version: 1 },
      });
      const error = yield* Proposal.validate({
        evidence: { ...f.evidence, manifest },
        output: f.output,
        roots: [Proposal.ROOT],
        maximumBytes: 1048576,
      }).pipe(Effect.flip);
      assert.equal(error._tag, "Invalid");
    }).pipe(Effect.provide(world)),
  );
}

it.effect("rejects unsupported human preference and repeated summaries of one outcome", () =>
  Effect.gen(function* () {
    const f = yield* knowledgeFixture();
    const original = f.evidence.outcomes[0];
    assert.ok(original);
    const actor = f.evidence.actors[0];
    assert.ok(actor);
    const bot = yield* Outcomes.evaluate({
      manifest: f.evidence.manifest,
      ref: original.ref,
      actors: [{ ...actor, kind: "bot" }],
    });
    const evidence = {
      ...f.evidence,
      actors: [{ ...actor, kind: "bot" as const }],
      outcomes: [bot],
    };
    const human = {
      ...f.output,
      drafts: f.output.drafts.map((draft) => ({ ...draft, claim: "human-decision" as const })),
    };
    assert.equal(
      (yield* Proposal.validate({
        evidence,
        output: human,
        roots: [Proposal.ROOT],
        maximumBytes: 1048576,
      }).pipe(Effect.flip))._tag,
      "Invalid",
    );
    const repeated = {
      ...f.output,
      drafts: f.output.drafts.map((draft) => ({
        ...draft,
        claim: "repeated-pattern" as const,
        outcomes: [original.key, original.key],
      })),
    };
    assert.equal(
      (yield* Proposal.validate({
        evidence: f.evidence,
        output: repeated,
        roots: [Proposal.ROOT],
        maximumBytes: 1048576,
      }).pipe(Effect.flip))._tag,
      "Invalid",
    );
  }).pipe(Effect.provide(world)),
);

it.effect("raw native PR event OIDs cannot satisfy canonical session citations", () =>
  Effect.gen(function* () {
    const f = yield* knowledgeFixture();
    const repository = yield* Repository;
    const head = yield* repository.resolve(Event.refOf(f.pr));
    assert.ok(head);
    const source = yield* repository.readCommit(f.source);
    const path = ".gitplus/knowledge/maintained/raw-pr.md";
    const tree = yield* repository.writeFiles({
      base: source.tree,
      changes: [
        {
          path,
          mode: "100644",
          content: new TextEncoder().encode(
            `---\ntype: Convention\nstatus: draft\ngitplus:\n  cites: [${qualify(head)}]\n---\nA claim.\n`,
          ),
        },
      ],
    });
    const commit = yield* repository.commitTree({
      tree,
      parents: [f.source],
      message: "invalid citation\n",
      author: source.author,
    });
    const checked = yield* Check.check({
      view: yield* Pack.committed(commit),
      concept: path,
      repo: f.genesis.repoId,
      trust: yield* Trust.project(f.genesis),
      evaluationTime: new Date("2026-10-29T00:00:00Z"),
    });
    assert.equal(checked.concepts[0]?.citations[0]?.state, "invalid");
    assert.equal(checked.concepts[0]?.eligibility.recall, false);
  }).pipe(Effect.provide(world)),
);

it.effect(
  "semantic assessments cannot borrow scope or support from an unrelated selected input",
  () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture();
      const outcome = f.evidence.outcomes[0];
      assert.ok(outcome);
      const source = outcome.sources[0];
      assert.ok(source);
      const assessment = {
        target: ".gitplus/knowledge/conventions/auth.md",
        state: "supported-within-scope" as const,
        scope: ["src/auth.ts"],
        evidence: [source.record],
        explanation: "A scoped finding.",
      };
      assert.equal(
        (yield* Proposal.validate({
          evidence: f.evidence,
          output: { version: 1, drafts: [], assessments: [assessment] },
          roots: [Proposal.ROOT],
          maximumBytes: 1048576,
        }).pipe(Effect.flip))._tag,
        "Invalid",
      );
      yield* Proposal.validate({
        evidence: f.evidence,
        output: { version: 1, drafts: [], assessments: [{ ...assessment, target: outcome.key }] },
        roots: [Proposal.ROOT],
        maximumBytes: 1048576,
      });
      const unattributed = {
        ...f.output,
        drafts: f.output.drafts.map((draft) => ({ ...draft, outcomes: [] })),
      };
      assert.equal(
        (yield* Proposal.validate({
          evidence: f.evidence,
          output: unattributed,
          roots: [Proposal.ROOT],
          maximumBytes: 1048576,
        }).pipe(Effect.flip))._tag,
        "Invalid",
      );
    }).pipe(Effect.provide(world)),
);
