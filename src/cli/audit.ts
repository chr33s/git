/**
 * The Invocation, as a person reads it.
 *
 * docs/telemetry.md §14 sketches this layout, and every rule in it is a rule
 * about not overstating:
 *
 * - the Attempts section is **absent** when attempts were not observed, rather
 *   than showing one, because "it succeeded first time" and "nobody was
 *   counting" are different things;
 * - one model label when request and response agree, two when they differ,
 *   because a single label would quietly assert they were the same;
 * - a normal finish reason such as `length` is never styled as an error unless
 *   `outcome.status` is also an error;
 * - every number says how it was known — reported, observed or derived (§5) —
 *   so a provider's token count never reads as a measurement.
 *
 * The projection itself lives in `telemetry/Invocation.ts`. This file only
 * decides what a terminal shows, which is why `--json` bypasses it entirely:
 * a machine consumer wants the structure, and formatting is exactly the layer
 * that would otherwise become an accidental API.
 */
import type * as Invocation from "../telemetry/Invocation.ts";

/** A qualified oid, short enough to scan and long enough to grep for. */
const short = (oid: string | null | undefined): string => {
  if (oid === undefined || oid === null) return "—";
  const separator = oid.indexOf(":");
  const hex = separator === -1 ? oid : oid.slice(separator + 1);
  return hex.length <= 12 ? hex : `${hex.slice(0, 8)}…`;
};

/** `118420` → `118k`. Rounded for reading, never for arithmetic. */
const compact = (value: number): string => {
  if (value < 1000) return `${value}`;
  const thousands = value / 1000;
  return thousands < 10 ? `${thousands.toFixed(1)}k` : `${Math.round(thousands)}k`;
};

const usageLine = (usage: Invocation.Runtime["usage"]): string | null => {
  if (usage === null) return null;
  const parts: Array<string> = [];
  if (usage.inputTokens !== undefined) parts.push(`${compact(usage.inputTokens)} input`);
  if (usage.cacheReadInputTokens !== undefined) {
    parts.push(`${compact(usage.cacheReadInputTokens)} cache-read`);
  }
  if (usage.cacheWriteInputTokens !== undefined) {
    parts.push(`${compact(usage.cacheWriteInputTokens)} cache-write`);
  }
  if (usage.outputTokens !== undefined) parts.push(`${compact(usage.outputTokens)} output`);
  if (usage.reasoningOutputTokens !== undefined) {
    parts.push(`${compact(usage.reasoningOutputTokens)} reasoning`);
  }
  if (parts.length === 0) return null;
  // The evidence class travels with the number, every time it is shown.
  const attribution =
    usage.source === "estimated" ? `estimated ${usage.estimator ?? ""}`.trim() : usage.source;
  return `${parts.join(" · ")} (${attribution})`;
};

/**
 * One model label when the two agree, two when they differ (§14).
 *
 * A single label for a request/response pair that differed would assert that
 * the weights that answered were the ones asked for, which is the claim the
 * two fields exist to keep separable.
 */
const modelLine = (runtime: Invocation.Runtime): string => {
  const model = runtime.model;
  const provider = model?.provider;
  const requested = model?.requested;
  const response = model?.response;

  const label =
    requested !== undefined && response !== undefined && requested !== response
      ? `${requested} → ${response}`
      : (response ?? requested ?? "unknown model");
  const head = [runtime.operation ?? "invocation", provider].filter(
    (part) => part !== undefined && part !== null,
  );
  return `${head.join(" · ")} / ${label}`;
};

const contextLines = (context: Invocation.ContextView): ReadonlyArray<string> => [
  `  tree      ${short(context.view?.tree)}`,
  `  evidence  ${context.blobs} blob${context.blobs === 1 ? "" : "s"} · ${context.gitlinks} gitlink${context.gitlinks === 1 ? "" : "s"}`,
  `  render    ${context.render}`,
  `  audit     ${context.verified ? "verified" : "not verified"}`,
];

const runtimeLines = (row: Invocation.Invocation): ReadonlyArray<string> => {
  const runtime = row.runtime;
  if (runtime === null) return [];
  const lines = [`  ${modelLine(runtime)}`];

  const usage = usageLine(runtime.usage);
  if (usage !== null) lines.push(`  usage     ${usage}`);

  // Finish and outcome are printed as two facts, because they are two (§7.3).
  if (runtime.finishReasons.length > 0) {
    lines.push(`  finish    ${runtime.finishReasons.join(", ")}`);
  }
  if (runtime.outcome !== undefined) {
    const error = runtime.outcome.errorType;
    lines.push(`  outcome   ${runtime.outcome.status}${error === undefined ? "" : ` · ${error}`}`);
  }
  if (row.inputPressure !== null) {
    lines.push(
      `  pressure  ${Math.round(row.inputPressure * 100)}% of the effective input limit (derived)`,
    );
  }
  if (runtime.conversation !== null) {
    lines.push(`  conversation ${runtime.conversation} (correlation only)`);
  }
  return lines;
};

/** One Invocation, as §14 lays it out. */
export const render = (row: Invocation.Invocation): ReadonlyArray<string> => {
  const lines: Array<string> = [`Invocation ${row.id}`];

  if (row.parents.length > 1) {
    // A join: this row follows two lanes that never saw each other, and saying
    // so is the difference between a history and a story about one (§15).
    lines.push(`  follows   ${row.parents.map((parent) => short(parent)).join(" + ")}`);
  }

  if (row.context !== null) {
    lines.push("", "Context", ...contextLines(row.context));
  }

  const runtime = runtimeLines(row);
  if (runtime.length > 0) lines.push("", "Runtime", ...runtime);
  else if (row.context !== null) {
    // The honest gap: an exposure whose call never came back. Named rather
    // than left as an absent section a reader might not notice.
    lines.push("", "Runtime", "  no runtime record was written for this exposure");
  }

  // Omitted entirely when attempts were not explicitly observed (§14).
  const attempts = row.runtime?.attempts;
  if (attempts !== null && attempts !== undefined && attempts.length > 0) {
    lines.push("", "Attempts");
    for (const attempt of attempts) {
      lines.push(`  ${attempt.index} ${attempt.errorType ?? attempt.status}`);
    }
  }

  if (row.workspace !== null) {
    lines.push("", "Workspace", `  ${short(row.workspace.before)} → ${short(row.workspace.after)}`);
  }

  const capture = row.capture;
  const stage = [capture?.transport, capture?.stage].filter((part) => part !== undefined);
  lines.push(
    "",
    "Capture",
    `  ${stage.length === 0 ? "not recorded" : stage.join(" · ")} · coverage ${row.coverage}`,
  );
  return lines;
};

/** A session's whole audit, Invocations first and the rest beneath them. */
export const renderAll = (projection: Invocation.Projection): ReadonlyArray<string> => {
  const lines: Array<string> = [];

  if (projection.concurrent) {
    // Reported, not resolved: §15 forbids manufacturing a single causal order
    // when concurrent parents exist, so the reader is told the shape rather
    // than shown a line that pretends there was one.
    lines.push(
      "This trace has concurrent lanes; the order below is causal, not chronological.",
      "",
    );
  }

  for (const [index, row] of projection.invocations.entries()) {
    if (index > 0) lines.push("");
    lines.push(...render(row));
  }
  if (projection.invocations.length === 0) lines.push("No invocations recorded for this session.");

  if (projection.tools.length > 0) {
    lines.push("", "Tools");
    for (const tool of projection.tools) {
      const result = tool.payload.result;
      const detail = [
        tool.payload.outcome?.status,
        result?.bytes === undefined ? undefined : `${result.bytes} bytes`,
        result?.truncated === true ? "truncated" : undefined,
      ].filter((part) => part !== undefined);
      lines.push(
        `  ${tool.payload.tool.name}${detail.length === 0 ? "" : ` · ${detail.join(" · ")}`}`,
      );
    }
  }

  if (projection.lifecycle.length > 0) {
    lines.push("", "Context lifecycle");
    for (const entry of projection.lifecycle) {
      lines.push(
        `  compaction · ${entry.payload.evidence} · ${entry.payload.strategy ?? "strategy unrecorded"}`,
      );
    }
  }

  if (projection.unreadable.length > 0) {
    lines.push("", `${projection.unreadable.length} record(s) could not be read here.`);
  }
  return lines;
};
