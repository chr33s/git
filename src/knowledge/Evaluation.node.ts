/** Local evaluation artifacts. Preparing a packet never invokes a model or a task command. */
import * as fs from "node:fs";
import * as path from "node:path";
import { Effect, Schema } from "effect";
import { Invalid, StorageFailure } from "../git/Error.ts";
import { isTree } from "../git/Format.ts";
import { unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Journal from "./Journal.node.ts";
import * as Evaluation from "./Evaluation.ts";
import { sha256 } from "../context/Render.ts";

export const Contexts = Schema.Struct({
  version: Schema.Literal(1),
  plan: Schema.String,
  entries: Schema.Array(
    Schema.Struct({
      slot: Schema.String,
      text: Schema.String,
      provenance: Schema.Json,
      maintenance: Schema.NullOr(Schema.Json),
    }),
  ),
});

const Packet = Schema.Struct({
  version: Schema.Literal(1),
  plan: Schema.String,
  slot: Schema.String,
  caseId: Schema.String,
  variant: Schema.String,
  condition: Evaluation.Condition,
  repetition: Schema.Int,
  prompt: Schema.String,
  snapshot: Schema.Json,
  snapshotDigest: Schema.String,
  configuration: Evaluation.Configuration,
  context: Schema.Null,
});

const required = <S extends Schema.Top>(directory: string, name: string, schema: S) =>
  Effect.gen(function* () {
    const value = yield* Journal.read(path.join(directory, name), schema);
    if (value === null)
      return yield* new Invalid({
        field: "evaluation",
        reason: `missing retained artifact: ${name}`,
      });
    return value;
  });

/** Bind context produced by the actual condition runtime before any model run. */
export const contexts = Effect.fn("knowledge.EvaluationHost.contexts")(function* (
  directory: string,
  input: typeof Contexts.Type,
) {
  const plan = yield* required(directory, "plan.json", Evaluation.Plan);
  const corpus = yield* required(directory, "corpus.json", Evaluation.Corpus);
  const rebuilt = yield* Evaluation.plan({ corpus, configuration: plan.configuration });
  if (Input.canonical(rebuilt) !== Input.canonical(plan))
    return yield* new Invalid({
      field: "evaluation",
      reason: "retained plan differs from its corpus/configuration",
    });
  const prepared = yield* required(
    directory,
    "prepared.json",
    Schema.Struct({
      version: Schema.Literal(1),
      plan: Schema.Literal(plan.id),
      snapshots: Schema.Array(Schema.Struct({ caseId: Schema.String, digest: Schema.String })),
    }),
  );
  if (
    input.plan !== plan.id ||
    new Set(input.entries.map((entry) => entry.slot)).size !== plan.slots.length ||
    input.entries.length !== plan.slots.length ||
    input.entries.some((entry) => !plan.slots.some((slot) => slot.id === entry.slot))
  )
    return yield* new Invalid({
      field: "evaluation.contexts",
      reason: "contexts must cover every planned slot exactly once",
    });
  const ready = path.join(directory, "contexts.json");
  if (yield* file(ready, () => fs.existsSync(ready)))
    return yield* new Invalid({
      field: "evaluation.contexts",
      reason: "contexts are already frozen; create a new comparison to change inputs",
    });
  const inputs: Array<{ slot: string; digest: string }> = [];
  for (const entry of input.entries) {
    if (new TextEncoder().encode(entry.text).byteLength > 16384)
      return yield* new Invalid({
        field: "evaluation.contexts",
        reason: "startup context exceeds the fixed 16,384-byte comparison ceiling",
      });
    const packet = yield* required(directory, `${entry.slot.slice(7)}.packet.json`, Packet);
    if (
      packet.plan !== plan.id ||
      packet.slot !== entry.slot ||
      Input.canonical(packet.configuration) !== Input.canonical(plan.configuration)
    )
      return yield* new Invalid({
        field: "evaluation.contexts",
        reason: "retained packet does not match the frozen plan",
      });
    const slot = plan.slots.find((slot) => slot.id === entry.slot);
    const task = corpus.cases.find((task) => task.id === slot?.caseId);
    if (
      task === undefined ||
      slot === undefined ||
      packet.caseId !== slot.caseId ||
      packet.condition !== slot.condition ||
      packet.variant !== slot.variant ||
      packet.repetition !== slot.repetition ||
      packet.prompt !== task.prompt ||
      (yield* Input.digest(packet.snapshot)) !== packet.snapshotDigest ||
      prepared.snapshots.find((snapshot) => snapshot.caseId === task.id)?.digest !==
        packet.snapshotDigest
    )
      return yield* new Invalid({
        field: "evaluation",
        reason: "task packet differs from the prepared snapshot or task",
      });
    const value = { ...packet, context: entry.text };
    const digest = yield* sha256(new TextEncoder().encode(JSON.stringify(value)));
    yield* Journal.write(path.join(directory, `${entry.slot.slice(7)}.input.json`), value);
    inputs.push({ slot: entry.slot, digest });
  }
  yield* Journal.write(ready, { version: 1, plan: plan.id, inputs, contexts: input.entries });
  return { version: 1, plan: plan.id, inputs };
});

const file = <A>(location: string, thunk: () => A) =>
  Effect.try({
    try: thunk,
    catch: (cause) =>
      new StorageFailure({ operation: "evaluation.artifact", path: location, cause }),
  });
const safePath = (name: string) =>
  name
    .split("/")
    .every((part) => part !== "." && part !== ".." && /^[A-Za-z0-9._-]+$/u.test(part)) &&
  !name.startsWith(".git/");
const decode = <S extends Schema.Top>(schema: S, value: string) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError((cause) => new Invalid({ field: "evaluation", reason: cause.message })),
  );

/** Freeze only selected task files; gold acceptance criteria and fixes remain in a separate artifact. */
export const prepare = Effect.fn("knowledge.EvaluationHost.prepare")(function* (input: {
  readonly directory: string;
  readonly corpus: Evaluation.Corpus;
  readonly configuration: Evaluation.Configuration;
  readonly maximumSourceBytes?: number;
}) {
  const plan = yield* Evaluation.plan(input);
  const repository = yield* Repository;
  const maximum = input.maximumSourceBytes ?? 4 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1)
    return yield* new Invalid({
      field: "evaluation",
      reason: "source byte ceiling must be a positive safe integer",
    });
  // A fresh output directory prevents overwriting results or silently refreshing a frozen plan.
  yield* file(input.directory, () => fs.mkdirSync(input.directory, { mode: 0o700 }));
  yield* Journal.write(path.join(input.directory, "plan.json"), plan);
  yield* Journal.write(path.join(input.directory, "corpus.json"), input.corpus);
  let bytes = 0;
  const snapshots: Array<{ caseId: string; digest: string }> = [];
  for (const task of input.corpus.cases) {
    const oid = unqualify(`sha1:${task.base}`);
    if (oid === null)
      return yield* new Invalid({
        field: "evaluation",
        reason: "invalid historical source commit",
      });
    const tree = (yield* repository.readCommit(oid)).tree;
    const files: Array<{ readonly path: string; readonly blob: string; readonly text: string }> =
      [];
    for (const name of task.paths) {
      if (!safePath(name))
        return yield* new Invalid({
          field: "evaluation.path",
          reason: "unsafe task snapshot path",
        });
      const found = yield* repository.findPath(tree, name);
      if (found === null || isTree(found.mode) || found.mode !== "100644")
        return yield* new Invalid({
          field: "evaluation.path",
          reason: `task input is not a regular file: ${name}`,
        });
      const blob = yield* repository.readBlob(found.oid);
      bytes += blob.byteLength;
      if (bytes > maximum)
        return yield* new Invalid({
          field: "evaluation",
          reason: "selected source snapshots exceed their byte ceiling",
        });
      const text = yield* Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(blob),
        catch: () => new Invalid({ field: "evaluation", reason: "task source is not UTF-8" }),
      });
      files.push({ path: name, blob: `sha1:${found.oid}`, text });
    }
    const snapshot = { base: task.base, files };
    const snapshotDigest = yield* Input.digest(snapshot);
    snapshots.push({ caseId: task.id, digest: snapshotDigest });
    for (const slot of plan.slots.filter((slot) => slot.caseId === task.id)) {
      const packet = {
        version: 1,
        plan: plan.id,
        slot: slot.id,
        caseId: task.id,
        variant: slot.variant,
        condition: slot.condition,
        repetition: slot.repetition,
        prompt: task.prompt,
        snapshot,
        snapshotDigest,
        configuration: input.configuration,
        context: null,
      };
      yield* Journal.write(path.join(input.directory, `${slot.id.slice(7)}.packet.json`), packet);
    }
  }
  // Absence of this marker means preparation did not finish; such packets cannot count as runs.
  const prepared = {
    version: 1,
    plan: plan.id,
    slots: plan.slots.length,
    sourceBytes: bytes,
    snapshots,
    contextsPrepared: false,
    modelInvoked: false,
  };
  yield* Journal.write(path.join(input.directory, "prepared.json"), prepared);
  return prepared;
});

export const report = Effect.fn("knowledge.EvaluationHost.report")(function* (directory: string) {
  const read = <S extends Schema.Top>(name: string, schema: S) => required(directory, name, schema);
  const corpus = yield* read("corpus.json", Evaluation.Corpus);
  const plan = yield* read("plan.json", Evaluation.Plan);
  yield* read(
    "prepared.json",
    Schema.Struct({ version: Schema.Literal(1), plan: Schema.Literal(plan.id) }),
  );
  const names = yield* file(directory, () =>
    fs
      .readdirSync(directory)
      .filter((name) => name.endsWith(".sample.json"))
      .sort(),
  );
  const samples = yield* Effect.forEach(names, (name) => read(name, Evaluation.Sample));
  const summary = yield* Evaluation.summarize({ corpus, plan, samples });
  const bound = yield* Journal.read(
    path.join(directory, "contexts.json"),
    Schema.Struct({
      version: Schema.Literal(1),
      plan: Schema.Literal(plan.id),
      inputs: Schema.Array(Schema.Struct({ slot: Schema.String, digest: Schema.String })),
    }),
  );
  for (const sample of samples) {
    if (
      bound === null ||
      bound.inputs.find((input) => input.slot === sample.slot)?.digest !== sample.inputDigest
    )
      return yield* new Invalid({
        field: "evaluation",
        reason: "sample input was not frozen before execution",
      });
    for (const [suffix, expected] of [
      ["input.json", sample.inputDigest],
      ["output.txt", sample.outputDigest],
    ] as const) {
      if (expected === null) continue;
      const location = path.join(directory, `${sample.slot.slice(7)}.${suffix}`);
      const bytes = yield* file(location, () => {
        if (fs.statSync(location).size > 16 * 1024 * 1024)
          throw new Error("evaluation payload exceeds 16 MiB");
        return fs.readFileSync(location);
      });
      if ((yield* sha256(bytes)) !== expected)
        return yield* new Invalid({
          field: "evaluation",
          reason: `retained ${suffix} does not match the sample commitment`,
        });
    }
  }

  yield* Journal.write(path.join(directory, "summary.json"), summary);
  return summary;
});

export const readConfiguration = Effect.fn("knowledge.EvaluationHost.readConfiguration")(function* (
  location: string,
) {
  return yield* decode(
    Evaluation.Configuration,
    yield* file(location, () => fs.readFileSync(location, "utf8")),
  );
});
