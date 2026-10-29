import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { StorageFailure } from "../git/Error.ts";
import { qualify } from "../git/Oid.ts";
import * as Session from "../hub/Session.ts";
import * as Memory from "../hub/Memory.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Trust from "../trust/Projection.ts";
import * as Pack from "../context/Pack.ts";
import * as Check from "./Check.ts";
import * as Proposal from "./Proposal.ts";
import * as Publication from "./Publication.ts";
import * as Publisher from "./Publisher.ts";
import { draftPath, knowledgeFixture } from "../testing/Knowledge.ts";

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);
const prepared = (evidence: Proposal.Evidence, output: Proposal.Output) =>
  Publisher.prepare({
    evidence,
    output,
    runner: { id: "fixture", version: "1" },
    roots: [Proposal.ROOT],
    maximumBytes: 1048576,
  });

it.effect(
  "publishes a draft with acyclic report citations and output attestations under requireProvenance",
  () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture(true);
      assert.equal(f.evidence.outcomes[0]?.eligible, true);
      const state = yield* prepared(f.evidence, f.output);
      assert.ok(state);
      const result = yield* Publisher.publish({
        state,
        key: f.root,
        actors: f.evidence.actors,
        roots: [Proposal.ROOT],
        maximumBytes: 1048576,
        checkpoint: () => Effect.void,
      });
      assert.equal(result.state, "written");
      assert.ok("commit" in result);
      const repository = yield* Repository;
      assert.equal(yield* repository.resolve("refs/heads/main"), f.source);
      const final = yield* repository.readCommit(result.commit);
      const report = final.parents[0];
      assert.ok(report);
      const events = (yield* Session.entries(state.session)).events.filter(
        (entry) => entry.payload.type === "session.produced",
      );
      assert.equal(events.length, 2);
      assert.ok(
        events.every(
          (entry) => entry.payload.type === "session.produced" && entry.payload.note === null,
        ),
      );
      const reportOutput = events.find(
        (entry) =>
          entry.payload.type === "session.produced" && entry.payload.commits.includes(report),
      );
      const finalOutput = events.find(
        (entry) =>
          entry.payload.type === "session.produced" &&
          entry.payload.commits.includes(result.commit),
      );
      assert.ok(reportOutput && finalOutput);
      const trust = yield* Trust.project(f.genesis);
      const view = yield* Pack.committed(result.commit);
      const checks = yield* Check.check({
        view,
        repo: f.genesis.repoId,
        trust,
        evaluationTime: new Date("2026-10-29T00:00:00Z"),
        concept: draftPath,
      });
      assert.equal(checks.concepts[0]?.lifecycle, "draft");
      assert.deepEqual(
        checks.concepts[0]?.citations.map((entry) => [entry.record, entry.state]),
        [[qualify(reportOutput.commit), "accepted"]],
      );
      assert.ok(!checks.concepts[0]?.concept?.cites.includes(qualify(finalOutput.commit)));
      const recalled = yield* Memory.derive({ view, repo: f.genesis.repoId, trust });
      assert.ok(!recalled.entries.some((entry) => entry.source?.path === draftPath));
      assert.ok(!recalled.entries.some((entry) => entry.origin === "session"));
    }).pipe(Effect.provide(world)),
);

for (const failedPhase of [
  "report-attested",
  "report-published",
  "concept-attested",
  "complete",
] as const) {
  it.effect(
    `recovers failure at ${failedPhase} without repeating interpretation or source outputs`,
    () =>
      Effect.gen(function* () {
        const f = yield* knowledgeFixture(true);
        const initial = yield* prepared(f.evidence, f.output);
        assert.ok(initial);
        let retained: Publication.State = initial;
        let failed = false;
        const failure = yield* Publisher.publish({
          state: retained,
          key: f.root,
          actors: f.evidence.actors,
          roots: [Proposal.ROOT],
          maximumBytes: 1048576,
          checkpoint: (next) => {
            if (next.phase === failedPhase && !failed) {
              failed = true;
              return Effect.fail(
                new StorageFailure({ operation: "test.checkpoint", path: "publication" }),
              );
            }
            retained = next;
            return Effect.void;
          },
        }).pipe(Effect.flip);
        assert.equal(failure._tag, "StorageFailure");
        const recovered = yield* Publisher.publish({
          state: retained,
          key: f.root,
          actors: f.evidence.actors,
          roots: [Proposal.ROOT],
          maximumBytes: 1048576,
          checkpoint: (next) => {
            retained = next;
            return Effect.void;
          },
        });
        assert.ok(recovered.state === "written" || recovered.state === "unchanged");
        assert.equal(retained.phase, "complete");
        const produced = (yield* Session.entries(initial.session)).events.filter(
          (entry) => entry.payload.type === "session.produced",
        );
        assert.equal(produced.length, 2);
        assert.equal(
          produced.filter(
            (entry) => entry.payload.type === "session.produced" && entry.payload.note !== null,
          ).length,
          0,
        );
      }).pipe(Effect.provide(world)),
  );
}

it.effect(
  "a deterministic pass without validated changes creates no proposal or maintenance session",
  () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture();
      const repository = yield* Repository;
      const before = yield* repository.refs;
      assert.equal(yield* prepared(f.evidence, { version: 1, assessments: [], drafts: [] }), null);
      assert.deepEqual(yield* repository.refs, before);
    }).pipe(Effect.provide(world)),
);

it.effect("hub.session without source.push does not grant knowledge-publisher authority", () =>
  Effect.gen(function* () {
    const f = yield* knowledgeFixture();
    const state = yield* prepared(f.evidence, f.output);
    assert.ok(state);
    const repository = yield* Repository;
    const before = yield* repository.refs;
    const error = yield* Publisher.publish({
      state,
      key: f.agent,
      actors: f.evidence.actors,
      roots: [Proposal.ROOT],
      maximumBytes: 1048576,
      checkpoint: () => Effect.void,
    }).pipe(Effect.flip);
    assert.equal(error._tag, "Invalid");
    assert.deepEqual(yield* repository.refs, before);
  }).pipe(Effect.provide(world)),
);

for (const fact of ["acceptedHead", "evaluationKey"] as const) {
  it.effect(`rejects a retained outcome whose ${fact} differs from native evidence`, () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture();
      const repository = yield* Repository;
      const source = yield* repository.readCommit(f.source);
      const base = source.parents[0];
      assert.ok(base);
      const evidence = {
        ...f.evidence,
        outcomes: f.evidence.outcomes.map((outcome) =>
          fact === "acceptedHead"
            ? { ...outcome, acceptedHead: qualify(base) }
            : { ...outcome, evaluationKey: `sha256:${"0".repeat(64)}` },
        ),
      };
      const state = yield* prepared(evidence, f.output);
      assert.ok(state);
      const before = yield* repository.refs;
      const result = yield* Publisher.publish({
        state,
        key: f.root,
        actors: f.evidence.actors,
        roots: [Proposal.ROOT],
        maximumBytes: 1048576,
        checkpoint: () => Effect.void,
      });
      assert.equal(result.state, "superseded-input");
      assert.deepEqual(yield* repository.refs, before);
    }).pipe(Effect.provide(world)),
  );
}

it.effect(
  "does not accept an existing branch's altered report body merely because its header and attestations match",
  () =>
    Effect.gen(function* () {
      const f = yield* knowledgeFixture();
      const state = yield* prepared(f.evidence, f.output);
      assert.ok(state);
      const publish = () =>
        Publisher.publish({
          state,
          key: f.root,
          actors: f.evidence.actors,
          roots: [Proposal.ROOT],
          maximumBytes: 1048576,
          checkpoint: () => Effect.void,
        });
      const first = yield* publish();
      assert.ok("commit" in first);
      const repository = yield* Repository;
      const final = yield* repository.readCommit(first.commit);
      const report = final.parents[0];
      assert.ok(report);
      const original = yield* repository.readCommit(report);
      const payload = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json)),
      )(state.reportText);
      const changed =
        Input.canonical({
          ...payload,
          interpretation: "Unrelated interpretation substituted under the same header.",
        }) + "\n";
      const reportTree = yield* repository.writeFiles({
        base: original.tree,
        changes: [
          { path: state.reportPath, mode: "100644", content: new TextEncoder().encode(changed) },
        ],
      });
      const forgedReport = yield* repository.commitTree({
        tree: reportTree,
        parents: original.parents,
        author: original.author,
        message: original.message,
      });
      const output = yield* Session.produced({
        repo: f.genesis.repoId,
        session: state.session,
        key: f.root,
        commits: [forgedReport],
        refs: [state.ref],
        note: null,
      });
      const finalTree = yield* repository.writeFiles({
        base: reportTree,
        changes: f.output.drafts.map((draft) => ({
          path: draft.path,
          mode: "100644",
          content: new TextEncoder().encode(
            Proposal.render(draft, [qualify(output)], state.reportPath, f.evidence.outcomes),
          ),
        })),
      });
      const forgedFinal = yield* repository.commitTree({
        tree: finalTree,
        parents: [forgedReport],
        author: final.author,
        message: final.message,
      });
      yield* Session.produced({
        repo: f.genesis.repoId,
        session: state.session,
        key: f.root,
        commits: [forgedFinal],
        refs: [state.ref],
        note: null,
      });
      yield* repository.setRef({ name: state.ref, to: forgedFinal, expected: first.commit });
      const events = (yield* Session.entries(state.session)).events.length;
      const rejected = yield* publish();
      assert.equal(rejected.state, "conflict");
      assert.equal(yield* repository.readRef(state.ref), forgedFinal);
      assert.equal((yield* Session.entries(state.session)).events.length, events);
    }).pipe(Effect.provide(world)),
);
