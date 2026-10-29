import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import * as Evaluation from "./Evaluation.ts";

const revision = "a".repeat(40);
const digest = `sha256:${"b".repeat(64)}`;
export const corpus: Evaluation.Corpus = {
  version: 1,
  id: "fixture",
  baselineRuntime: revision,
  replayKind: "retrospective-known-fixes",
  metrics: ["acceptedTaskResult", "inputTokens", "timeToSupportedDiagnosisMs"],
  cases: ["recurring", "rare"].map((id) => ({
    id,
    category: id,
    base: revision,
    referenceFix: revision,
    paths: ["src/example.ts"],
    prompt: "Diagnose this historical task.",
    acceptance: ["Executable result"],
    variants: ["normal"],
  })),
};
export const configuration: Evaluation.Configuration = {
  version: 1,
  runner: { id: "fixture", version: "1", model: "fixture-only", settings: {} },
  budget: { wallTimeMs: 1000, inputTokens: 100, outputTokens: 100, toolCalls: 2, corrections: 0 },
  repetitions: 1,
  standingInstructionsDigest: digest,
  runtimeDigests: {
    "current-main": revision,
    "integrated-recall": digest,
    "recall-with-reviewed-curation": digest,
  },
};
const unknown: Evaluation.Measurement = {
  state: "unavailable",
  reason: "Runner did not report usage.",
};
export const samplesOf = (plan: Evaluation.Plan): Array<Evaluation.Sample> =>
  plan.slots.map((slot, index) => ({
    version: 1,
    plan: plan.id,
    slot: slot.id,
    attempt: 1,
    inputDigest: digest,
    outputDigest: digest,
    state: "completed",
    diagnostic: null,
    metrics: {
      acceptedTaskResult: {
        state: "measured",
        value: 1,
        source: "reviewer",
        evidence: ["retained-check.log"],
      },
      inputTokens: unknown,
      timeToSupportedDiagnosisMs: {
        state: "measured",
        value: 100 + index,
        source: "host",
        evidence: ["retained-trace.json"],
      },
    },
  }));

it.effect(
  "freezes the same comparison and rotates condition order without changing per-task budgets",
  () =>
    Effect.gen(function* () {
      const plan = yield* Evaluation.plan({ corpus, configuration });
      assert.deepEqual(yield* Evaluation.plan({ corpus, configuration }), plan);
      assert.equal(plan.slots.length, 6);
      assert.deepEqual(
        plan.slots.slice(0, 3).map((slot) => slot.condition),
        Evaluation.conditions,
      );
      assert.deepEqual(
        plan.slots.slice(3, 6).map((slot) => slot.condition),
        ["integrated-recall", "recall-with-reviewed-curation", "current-main"],
      );
      assert.equal(new Set(plan.slots.map((slot) => slot.id)).size, 6);
      const changed = yield* Evaluation.plan({
        corpus,
        configuration: {
          ...configuration,
          runner: { ...configuration.runner, model: "different-model" },
        },
      });
      assert.notEqual(changed.id, plan.id);
    }),
);

it.effect("partial comparisons expose every missing slot and do not impute zero usage", () =>
  Effect.gen(function* () {
    const plan = yield* Evaluation.plan({ corpus, configuration });
    const summary = yield* Evaluation.summarize({
      corpus,
      plan,
      samples: samplesOf(plan).slice(0, 1),
    });
    assert.equal(summary.state, "partial");
    assert.equal(summary.missing.length, 5);
    assert.equal(summary.statistics[0]?.metrics.inputTokens?.mean, null);
    assert.equal(summary.statistics[0]?.metrics.inputTokens?.unavailable, 1);
    assert.equal(summary.paired[0]?.metrics.timeToSupportedDiagnosisMs?.pairs, 0);
    assert.equal(summary.paired[0]?.metrics.timeToSupportedDiagnosisMs?.meanDifference, null);
  }),
);

it.effect("complete measured pairs retain coverage separately from unknown telemetry", () =>
  Effect.gen(function* () {
    const plan = yield* Evaluation.plan({ corpus, configuration });
    const summary = yield* Evaluation.summarize({ corpus, plan, samples: samplesOf(plan) });
    assert.equal(summary.state, "recorded");
    assert.ok(summary.statistics.every((row) => row.completed === 2));
    assert.ok(summary.statistics.every((row) => row.metrics.inputTokens?.sum === null));
    assert.equal(summary.paired[0]?.metrics.timeToSupportedDiagnosisMs?.pairs, 2);
    assert.equal(summary.paired[0]?.metrics.inputTokens?.pairs, 0);
  }),
);

for (const fault of [
  "duplicate",
  "foreign-plan",
  "self-accepted",
  "missing-metric",
  "no-evidence",
  "changed-configuration",
] as const) {
  it.effect(`rejects ${fault} comparison artifacts`, () =>
    Effect.gen(function* () {
      const plan = yield* Evaluation.plan({ corpus, configuration });
      const samples = samplesOf(plan);
      const first = samples[0];
      assert.ok(first);
      let retained = plan;
      switch (fault) {
        case "duplicate":
          samples.push(first);
          break;
        case "foreign-plan":
          samples[0] = { ...first, plan: digest };
          break;
        case "self-accepted":
          samples[0] = {
            ...first,
            metrics: {
              ...first.metrics,
              acceptedTaskResult: {
                state: "measured",
                value: 1,
                source: "runner",
                evidence: ["self-report"],
              },
            },
          };
          break;
        case "missing-metric":
          samples[0] = { ...first, metrics: { inputTokens: unknown } };
          break;
        case "no-evidence":
          samples[0] = {
            ...first,
            metrics: {
              ...first.metrics,
              inputTokens: { state: "measured", value: 0, source: "host", evidence: [] },
            },
          };
          break;
        case "changed-configuration":
          retained = {
            ...plan,
            configuration: {
              ...configuration,
              budget: { ...configuration.budget, inputTokens: 200 },
            },
          };
          break;
      }
      assert.equal(
        (yield* Evaluation.summarize({ corpus, plan: retained, samples }).pipe(Effect.flip))._tag,
        "Invalid",
      );
    }),
  );
}
