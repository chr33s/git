#!/usr/bin/env node
/** Operator benchmark adapter for an installed CLI; never shipped as an application provider. */
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { spawnSync } from "node:child_process";
import { Config, Effect, Option, Redacted, Schema } from "effect";
import { Invalid } from "../../src/git/Error.ts";
import * as Journal from "../../src/knowledge/Journal.node.ts";
import * as Evaluation from "../../src/knowledge/Evaluation.ts";
import { sha256 } from "../../src/context/Render.ts";

const Settings = Schema.Struct({
  systemPrompt: Schema.String,
  effort: Schema.String,
  maxBudgetUSD: Schema.Finite,
  maxOutputTokens: Schema.Int,
});
const Envelope = Schema.Struct({
  type: Schema.String,
  subtype: Schema.String,
  is_error: Schema.Boolean,
  result: Schema.String,
  usage: Schema.Json,
  modelUsage: Schema.Record(Schema.String, Schema.Json),
});
const read = <S extends Schema.Top>(location: string, schema: S) =>
  Effect.gen(function* () {
    const value = yield* Journal.read(location, schema);
    if (value === null)
      return yield* new Invalid({ field: "evaluation", reason: `missing ${location}` });
    return value;
  });
const program = Effect.gen(function* () {
  const [destination, executable] = process.argv.slice(2);
  if (destination === undefined || executable === undefined)
    return yield* new Invalid({
      field: "arguments",
      reason:
        "usage: node evaluation/proactive-memory/run-claude.node.ts <frozen-comparison-directory> <installed-claude-executable>",
    });
  const directory = path.resolve(destination);
  const plan = yield* read(path.join(directory, "plan.json"), Evaluation.Plan);
  const bound = yield* read(
    path.join(directory, "contexts.json"),
    Schema.Struct({
      version: Schema.Literal(1),
      plan: Schema.Literal(plan.id),
      inputs: Schema.Array(Schema.Struct({ slot: Schema.String, digest: Schema.String })),
    }),
  );
  const settings = yield* Schema.decodeUnknownEffect(Settings)(plan.configuration.runner.settings);
  if (
    (yield* sha256(new TextEncoder().encode(settings.systemPrompt))) !==
      plan.configuration.standingInstructionsDigest ||
    plan.configuration.budget.toolCalls !== 0
  )
    return yield* new Invalid({
      field: "evaluation",
      reason: "this adapter requires fixed system instructions and a no-tools comparison",
    });
  const key = yield* Config.Redacted("ANTHROPIC_API_KEY");
  const apiURL = yield* Config.option(Config.String("ANTHROPIC_BASE_URL"));
  const environmentPath = yield* Config.String("PATH");
  for (const slot of plan.slots) {
    const stem = path.join(directory, slot.id.slice(7));
    const completed = `${stem}.run.json`;
    if (fs.existsSync(completed)) {
      console.log(`retained ${slot.caseId}/${slot.variant}/${slot.condition}`);
      continue;
    }
    if (fs.existsSync(`${stem}.started.json`))
      return yield* new Invalid({
        field: "evaluation",
        reason: "unfinished attempt exists; reconcile it before another provider call",
      });
    const payload = fs.readFileSync(`${stem}.input.json`);
    const inputDigest = yield* sha256(payload);
    if (bound.inputs.find((input) => input.slot === slot.id)?.digest !== inputDigest)
      return yield* new Invalid({
        field: "evaluation",
        reason: "frozen input changed before model execution",
      });
    yield* Journal.write(`${stem}.started.json`, {
      version: 1,
      plan: plan.id,
      slot: slot.id,
      attempt: 1,
      startedAt: new Date().toISOString(),
      pid: process.pid,
    });
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "memory-task-model-"));
    const args = [
      "--bare",
      "--print",
      "--tools",
      "",
      "--disable-slash-commands",
      "--strict-mcp-config",
      "--no-session-persistence",
      "--setting-sources",
      "",
      "--model",
      plan.configuration.runner.model,
      "--effort",
      settings.effort,
      "--output-format",
      "json",
      "--system-prompt",
      settings.systemPrompt,
      "--max-budget-usd",
      String(settings.maxBudgetUSD),
    ];
    const started = performance.now();
    const environment = {
      PATH: environmentPath,
      LANG: "C.UTF-8",
      ANTHROPIC_API_KEY: Redacted.value(key),
      CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(settings.maxOutputTokens),
      ANTHROPIC_BASE_URL: Option.getOrUndefined(apiURL),
    };
    const result = (() => {
      try {
        return spawnSync(executable, args, {
          cwd: workspace,
          env: environment,
          input: payload,
          timeout: plan.configuration.budget.wallTimeMs,
          killSignal: "SIGKILL",
          maxBuffer: 16 * 1024 * 1024,
        });
      } finally {
        fs.rmSync(workspace, { recursive: true, force: true });
      }
    })();
    const elapsedMs = performance.now() - started;
    const bytes = result.stdout ?? new Uint8Array();
    fs.writeFileSync(`${stem}.output.txt`, bytes, { mode: 0o600 });
    const outputDigest = yield* sha256(bytes);
    const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(Envelope))(
      new TextDecoder().decode(bytes),
    ).pipe(Effect.result);
    const parsed = decoded._tag === "Success" ? decoded.success : null;
    const state =
      result.status === 0 &&
      parsed?.is_error === false &&
      Object.keys(parsed.modelUsage).length === 1 &&
      Object.hasOwn(parsed.modelUsage, plan.configuration.runner.model)
        ? "completed"
        : "failed";
    const run = {
      version: 1,
      plan: plan.id,
      slot: slot.id,
      attempt: 1,
      inputDigest,
      outputDigest,
      state,
      elapsedMs,
      exitStatus: result.status,
      signal: result.signal,
      errorCode: result.error?.name ?? null,
      modelUsage: parsed?.modelUsage ?? null,
      usage: parsed?.usage ?? null,
      runner: {
        id: plan.configuration.runner.id,
        version: plan.configuration.runner.version,
        model: plan.configuration.runner.model,
        args,
      },
      toolsEnabled: false,
      workspaceIsolated: true,
      sessionPersistence: false,
    };
    yield* Journal.write(completed, run);
    if (parsed !== null) fs.writeFileSync(`${stem}.answer.txt`, parsed.result, { mode: 0o600 });
    console.log(
      `${state} ${slot.caseId}/${slot.variant}/${slot.condition} ${Math.round(elapsedMs)}ms`,
    );
  }
});

await Effect.runPromise(
  program.pipe(
    Effect.catch((error) =>
      Effect.sync(() => {
        console.error(`evaluation-model: ${Schema.is(Invalid)(error) ? error.reason : error._tag}`);
        process.exitCode = 1;
      }),
    ),
  ),
);
