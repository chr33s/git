/** Fixed comparison planning and aggregation. Missing telemetry never becomes a zero. */
import { Effect, Schema } from "effect";
import { Invalid } from "../git/Error.ts";
import * as Input from "../hub/MemoryInput.ts";

export const Condition = Schema.Literals([
  "current-main",
  "integrated-recall",
  "recall-with-reviewed-curation",
]);
export type Condition = typeof Condition.Type;
export const conditions: ReadonlyArray<Condition> = Condition.literals;
const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const Nonnegative = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
export const Case = Schema.Struct({
  id: Schema.NonEmptyString,
  category: Schema.NonEmptyString,
  base: Schema.String,
  referenceFix: Schema.String,
  paths: Schema.Array(Schema.String),
  prompt: Schema.NonEmptyString,
  acceptance: Schema.Array(Schema.String),
  variants: Schema.Array(Schema.NonEmptyString),
});
export const Corpus = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.NonEmptyString,
  baselineRuntime: Schema.String,
  replayKind: Schema.Literal("retrospective-known-fixes"),
  metrics: Schema.Array(Schema.NonEmptyString),
  cases: Schema.Array(Case),
});
export type Corpus = typeof Corpus.Type;
export const Configuration = Schema.Struct({
  version: Schema.Literal(1),
  runner: Schema.Struct({
    id: Schema.NonEmptyString,
    version: Schema.NonEmptyString,
    model: Schema.NonEmptyString,
    settings: Schema.Json,
  }),
  budget: Schema.Struct({
    wallTimeMs: Positive,
    inputTokens: Positive,
    outputTokens: Positive,
    toolCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    corrections: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  }),
  repetitions: Positive,
  standingInstructionsDigest: Schema.String,
  runtimeDigests: Schema.Struct({
    "current-main": Schema.String,
    "integrated-recall": Schema.String,
    "recall-with-reviewed-curation": Schema.String,
  }),
});
export type Configuration = typeof Configuration.Type;
const Slot = Schema.Struct({
  id: Schema.String,
  caseId: Schema.String,
  variant: Schema.String,
  condition: Condition,
  repetition: Positive,
});
export const Plan = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  corpusDigest: Schema.String,
  configurationDigest: Schema.String,
  configuration: Configuration,
  slots: Schema.Array(Slot),
});
export type Plan = typeof Plan.Type;

const valid = (condition: boolean, reason: string) =>
  condition ? Effect.void : Effect.fail(new Invalid({ field: "evaluation", reason }));
const unique = (values: ReadonlyArray<string>) => new Set(values).size === values.length;
const digest = (value: string) => /^sha256:[0-9a-f]{64}$/u.test(value);
const commit = (value: string) => /^[0-9a-f]{40}$/u.test(value);
const revision = (value: string) => commit(value) || digest(value);

export const plan = Effect.fn("knowledge.Evaluation.plan")(function* (input: {
  readonly corpus: Corpus;
  readonly configuration: Configuration;
}) {
  const { corpus, configuration } = input;
  yield* valid(
    corpus.cases.length > 0 && unique(corpus.cases.map((entry) => entry.id)),
    "case IDs must be unique and nonempty",
  );
  yield* valid(
    corpus.metrics.length > 0 && unique(corpus.metrics),
    "metrics must be unique and nonempty",
  );
  yield* valid(commit(corpus.baselineRuntime), "baseline must name an immutable runtime");
  yield* valid(
    configuration.runtimeDigests["current-main"] === corpus.baselineRuntime,
    "baseline runtime differs from the fixed corpus",
  );
  yield* valid(
    conditions.every((condition) => revision(configuration.runtimeDigests[condition])) &&
      digest(configuration.standingInstructionsDigest),
    "runtime and standing instruction identities must be immutable",
  );
  yield* valid(
    corpus.cases.length <= 100 &&
      configuration.repetitions <= 100 &&
      corpus.cases.every((entry) => entry.variants.length <= 10),
    "comparison exceeds 100 cases, 100 repetitions, or 10 variants per case",
  );
  yield* valid(
    corpus.cases.reduce((count, entry) => count + entry.variants.length, 0) *
      configuration.repetitions *
      conditions.length <=
      10000,
    "comparison exceeds 10,000 planned slots",
  );
  const corpusDigest = yield* Input.digest(corpus);
  const configurationDigest = yield* Input.digest(configuration);
  const id = yield* Input.digest({ corpusDigest, configurationDigest });
  const slots: Array<typeof Slot.Type> = [];
  for (const [index, entry] of corpus.cases.entries()) {
    yield* valid(
      entry.variants.length > 0 && unique(entry.variants),
      "case variants must be unique and nonempty",
    );
    yield* valid(
      commit(entry.base) && commit(entry.referenceFix),
      "task history must name immutable commits",
    );
    for (let repetition = 1; repetition <= configuration.repetitions; repetition += 1) {
      for (const variant of entry.variants) {
        // Rotate order without changing any condition's task or resource budget.
        for (let offset = 0; offset < conditions.length; offset += 1) {
          const condition = conditions[(index + repetition - 1 + offset) % conditions.length];
          if (condition === undefined) continue;
          const slot = { caseId: entry.id, variant, condition, repetition };
          slots.push({ id: yield* Input.digest({ plan: id, ...slot }), ...slot });
        }
      }
    }
  }
  return { version: 1, id, corpusDigest, configurationDigest, configuration, slots } satisfies Plan;
});

export const Measurement = Schema.Union([
  Schema.Struct({
    state: Schema.Literal("measured"),
    value: Nonnegative,
    source: Schema.Literals(["host", "runner", "reviewer"]),
    evidence: Schema.Array(Schema.NonEmptyString),
  }),
  Schema.Struct({ state: Schema.Literal("unavailable"), reason: Schema.NonEmptyString }),
]);
export type Measurement = typeof Measurement.Type;
export const Sample = Schema.Struct({
  version: Schema.Literal(1),
  plan: Schema.String,
  slot: Schema.String,
  attempt: Positive,
  inputDigest: Schema.String,
  outputDigest: Schema.NullOr(Schema.String),
  state: Schema.Literals(["completed", "failed", "incomplete"]),
  metrics: Schema.Record(Schema.String, Measurement),
  diagnostic: Schema.NullOr(Schema.String),
});
export type Sample = typeof Sample.Type;

/** A slot is counted once; retries cannot inflate the comparison's sample size. */
export const summarize = Effect.fn("knowledge.Evaluation.summarize")(function* (input: {
  readonly corpus: Corpus;
  readonly plan: Plan;
  readonly samples: ReadonlyArray<Sample>;
}) {
  const rebuilt = yield* plan({ corpus: input.corpus, configuration: input.plan.configuration });
  yield* valid(
    Input.canonical(rebuilt) === Input.canonical(input.plan),
    "plan does not match its retained corpus and configuration",
  );
  const byId = new Map(input.plan.slots.map((slot) => [slot.id, slot]));
  yield* valid(
    unique(input.samples.map((sample) => sample.slot)),
    "duplicate slot: retain retry attempts separately, never as independent comparisons",
  );
  for (const sample of input.samples) {
    yield* valid(
      sample.plan === input.plan.id && byId.has(sample.slot),
      "sample does not belong to this plan",
    );
    yield* valid(
      digest(sample.inputDigest) && (sample.outputDigest === null || digest(sample.outputDigest)),
      "sample payload commitments are invalid",
    );
    yield* valid(
      sample.state !== "completed" || sample.outputDigest !== null,
      "a completed sample must retain its output commitment",
    );
    yield* valid(
      unique(Object.keys(sample.metrics)) &&
        Input.canonical(Object.keys(sample.metrics).sort()) ===
          Input.canonical([...input.corpus.metrics].sort()),
      "each sample must report every declared metric, including unavailable values",
    );
    for (const [name, measurement] of Object.entries(sample.metrics)) {
      if (measurement.state === "unavailable") continue;
      yield* valid(measurement.evidence.length > 0, "measured values require retained evidence");
      if (
        ["unsupportedClaimsSupplied", "staleClaimsSupplied", "reviewerCorrections"].includes(name)
      )
        yield* valid(
          measurement.source === "reviewer",
          "semantic and correction counts require reviewer evidence",
        );
      if (name === "acceptedTaskResult")
        yield* valid(
          measurement.source === "reviewer" && (measurement.value === 0 || measurement.value === 1),
          "task acceptance requires reviewer evidence and a binary result",
        );
    }
  }
  const samples = new Map(input.samples.map((sample) => [sample.slot, sample]));
  const missing = input.plan.slots.filter((slot) => !samples.has(slot.id));
  const statistics = conditions.map((condition) => {
    const slots = input.plan.slots.filter((slot) => slot.condition === condition);
    const held = slots.flatMap((slot) => {
      const sample = samples.get(slot.id);
      return sample === undefined ? [] : [sample];
    });
    return {
      condition,
      expected: slots.length,
      recorded: held.length,
      completed: held.filter((sample) => sample.state === "completed").length,
      failed: held.filter((sample) => sample.state === "failed").length,
      incomplete: held.filter((sample) => sample.state === "incomplete").length,
      metrics: Object.fromEntries(
        input.corpus.metrics.map((name) => {
          const measured = held.flatMap((sample) => {
            const metric = sample.metrics[name];
            return metric?.state === "measured" ? [metric.value] : [];
          });
          return [
            name,
            {
              measured: measured.length,
              unavailable: held.length - measured.length,
              sum: measured.length === 0 ? null : measured.reduce((sum, value) => sum + value, 0),
              mean:
                measured.length === 0
                  ? null
                  : measured.reduce((sum, value) => sum + value, 0) / measured.length,
            },
          ];
        }),
      ),
    };
  });
  const paired = conditions
    .filter((condition) => condition !== "current-main")
    .map((condition) => ({
      condition,
      baseline: "current-main",
      metrics: Object.fromEntries(
        input.corpus.metrics.map((name) => {
          const differences: Array<number> = [];
          for (const slot of input.plan.slots.filter((slot) => slot.condition === condition)) {
            const baseline = input.plan.slots.find(
              (other) =>
                other.condition === "current-main" &&
                other.caseId === slot.caseId &&
                other.variant === slot.variant &&
                other.repetition === slot.repetition,
            );
            const measured = samples.get(slot.id)?.metrics[name];
            const control =
              baseline === undefined ? undefined : samples.get(baseline.id)?.metrics[name];
            if (measured?.state === "measured" && control?.state === "measured")
              differences.push(measured.value - control.value);
          }
          return [
            name,
            {
              pairs: differences.length,
              meanDifference:
                differences.length === 0
                  ? null
                  : differences.reduce((sum, value) => sum + value, 0) / differences.length,
            },
          ];
        }),
      ),
    }));
  return {
    version: 1,
    plan: input.plan.id,
    state:
      missing.length === 0 && input.samples.every((sample) => sample.state !== "incomplete")
        ? "recorded"
        : "partial",
    expected: input.plan.slots.length,
    recorded: input.samples.length,
    missing,
    statistics,
    paired,
    limitations: [
      "Aggregates describe retained measurements; they do not establish semantic truth or productivity improvement.",
      "Per-metric coverage can differ. Missing telemetry is excluded and must be inspected before comparing means.",
      "Retrospective known-fix tasks do not demonstrate generalization to unseen tasks.",
    ],
  };
});
