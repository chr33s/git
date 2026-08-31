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
import { qualify } from "../git/Oid.ts";
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
  // A negative count is not a measurement of anything. `Records.check` refuses
  // one at the door, but that runs from `record` alone — a record written by a
  // peer or by an older implementation replicates in and decodes fine, so the
  // reader has to hold the same line the writer does.
  const said = (value: number | undefined) =>
    value === undefined || value < 0 ? undefined : value;
  const inputTokens = said(usage.inputTokens);
  const outputTokens = said(usage.outputTokens);
  const cacheRead = said(usage.cacheReadInputTokens);
  const cacheWrite = said(usage.cacheWriteInputTokens);
  const reasoning = said(usage.reasoningOutputTokens);
  if (inputTokens !== undefined) parts.push(`${compact(inputTokens)} input`);
  if (cacheRead !== undefined) parts.push(`${compact(cacheRead)} cache-read`);
  if (cacheWrite !== undefined) parts.push(`${compact(cacheWrite)} cache-write`);
  if (outputTokens !== undefined) parts.push(`${compact(outputTokens)} output`);
  if (reasoning !== undefined) parts.push(`${compact(reasoning)} reasoning`);
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
  // Printed only when it failed: a verdict shown on every row teaches a reader
  // to stop seeing it, and "verified" is the state the Context half already
  // reports for the same record set.
  if (runtime.trust?.ok === false) lines.push(`  trust     no — ${runtime.trust.reason}`);
  return lines;
};

/** One Invocation, as §14 lays it out. */
export const render = (
  row: Invocation.Invocation,
  /** Record commits a counted tombstone names; see the Context branch below. */
  removed: ReadonlySet<string> = new Set(),
): ReadonlyArray<string> => {
  const lines: Array<string> = [`Invocation ${row.id}`];

  if (row.parents.length > 1) {
    // A join: this row follows two lanes that never saw each other, and saying
    // so is the difference between a history and a story about one (§15).
    lines.push(`  follows   ${row.parents.map((parent) => short(parent)).join(" + ")}`);
  }

  if (row.context !== null) {
    lines.push("", "Context", ...contextLines(row.context));
  }

  if (row.context === null && row.runtime?.exposure != null) {
    // Named an exposure and it is not here. Said out loud, because an absent
    // Context section otherwise reads as "this invocation exposed nothing" —
    // and said *why*, because the projection knows. An exposure a counted
    // tombstone names was dropped on purpose, and reporting a deliberate,
    // signed removal as a replication gap is the confusion this module's own
    // rule is about: an absence with a tombstone beside it is a removal, and
    // one without is a replica that has not caught up.
    const why = removed.has(row.runtime.exposure)
      ? "which a signed redaction removed"
      : "which this replica does not hold";
    lines.push("", "Context", `  names ${row.runtime.exposure}, ${why}`);
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
    // Marked where it is shown, like every other attached record: a transition
    // nobody accountable signed must not read as a fact about the repository.
    const untrusted = row.workspace.trust?.ok === false ? " · untrusted" : "";
    lines.push(
      "",
      "Workspace",
      `  ${short(row.workspace.before)} → ${short(row.workspace.after)}${untrusted}`,
    );
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

const describeTool = (payload: Invocation.Projection["tools"][number]["payload"]): string => {
  const result = payload.result;
  const detail = [
    payload.outcome?.status,
    // The error type beside the status, because it was rendered nowhere. A
    // tool call that failed with `not_found` printed only its status, so the
    // one field saying *what* went wrong was in the signed record and in no
    // human-facing surface.
    payload.outcome?.errorType,
    result?.bytes === undefined ? undefined : `${result.bytes} bytes`,
    result?.truncated === true ? "truncated" : undefined,
  ].filter((part) => part !== undefined);
  return `${payload.tool.name}${detail.length === 0 ? "" : ` · ${detail.join(" · ")}`}`;
};

/**
 * What the policy-visible half of a session says, above its trace.
 *
 * `--audit` promises to *join* the two, and rendering only the Invocation
 * history dropped everything plain `session show` gives — the prompt, the
 * agent, what was produced, what was decided. A reader following the documented
 * workflow lost the account of the work to see the account of the runtime.
 */
const sessionLines = (session: Session): ReadonlyArray<string> => {
  // Said, because `Session.project` computes it and the prose path was the
  // only reader that dropped it. `context for --session X` writes the trace
  // ref and nothing else, so a full audit rendered under a plain
  // `Session <id>` heading for a session nobody ever opened — the account of
  // the runtime presented as the account of the work, which is the one
  // distinction this module exists to keep.
  // Three states, not two. `exists` is `walked.events.length > 0`, so a session
  // whose records were all removed by `session redact`, or whose payloads this
  // replica cannot decode, reads the same as one that was never opened — and
  // asserts that no session record was ever written on precisely the run where
  // an operator is checking what a removal did.
  const emptied = session.redacted.length > 0 || session.unreadable.length > 0;
  const lines = [
    session.exists
      ? `Session ${session.session}`
      : emptied
        ? `Session ${session.session} (every record here was removed or cannot be read)`
        : `Session ${session.session} (no session record; the trace ref is all there is)`,
  ];
  if (session.agent !== null) {
    const agent = [session.agent.kind, session.agent.model, session.agent.harness].filter(
      (part) => part !== "",
    );
    lines.push(`  agent     ${agent.join(" · ")}`);
  }
  for (const prompt of session.prompts) {
    lines.push(`  ${prompt.role.padEnd(9)} ${prompt.prompt.split("\n")[0] ?? ""}`);
  }
  for (const decision of session.decisions) {
    lines.push(`  decided   ${decision.question} → ${decision.chose ?? "unanswered"}`);
  }
  if (session.commits.length > 0) lines.push(`  commits   ${session.commits.join(", ")}`);
  if (session.refs.length > 0) lines.push(`  refs      ${session.refs.join(", ")}`);
  for (const note of session.notes) lines.push(`  note      ${note}`);
  return lines;
};

/** The distilled half of a session, as `hub/Session.project` reports it. */
export interface Session {
  readonly session: string;
  /**
   * Whether `refs/hub/session/<id>` holds anything at all.
   *
   * `context for --session X` and `trace record --session X` write only the
   * trace ref, so a session can have a full runtime account and no record of
   * the work it was doing.
   */
  readonly exists: boolean;
  /** Records this session says were removed, and ones it could not read. */
  readonly redacted: ReadonlyArray<string>;
  readonly unreadable: ReadonlyArray<unknown>;
  readonly agent: {
    readonly kind: string;
    readonly model: string;
    readonly harness: string;
  } | null;
  readonly prompts: ReadonlyArray<{ readonly role: string; readonly prompt: string }>;
  readonly decisions: ReadonlyArray<{
    readonly question: string;
    readonly chose: string | null;
  }>;
  readonly commits: ReadonlyArray<string>;
  readonly refs: ReadonlyArray<string>;
  readonly notes: ReadonlyArray<string>;
}

/** A session's whole audit, Invocations first and the rest beneath them. */
export const renderAll = (
  projection: Invocation.Projection,
  session?: Session,
): ReadonlyArray<string> => {
  const lines: Array<string> = [];
  if (session !== undefined) lines.push(...sessionLines(session), "");

  if (projection.concurrent) {
    // Reported, not resolved: §15 forbids manufacturing a single causal order
    // when concurrent parents exist, so the reader is told the shape rather
    // than shown a line that pretends there was one.
    lines.push(
      "This trace has concurrent lanes; the order below is causal, not chronological.",
      "",
    );
  }

  const removed = new Set(projection.redacted);
  for (const [index, row] of projection.invocations.entries()) {
    if (index > 0) lines.push("");
    lines.push(...render(row, removed));
  }
  if (projection.invocations.length === 0) lines.push("No invocations recorded for this session.");

  // Beneath the invocation each names, which is what the join is for: a flat
  // list leaves an operator with several invocations unable to tell which one
  // issued which call, even though the record says.
  if (projection.tools.length > 0) {
    lines.push("", "Tools");
    const grouped = new Map<string, Array<string>>();
    for (const tool of projection.tools) {
      const under = tool.payload.invocation ?? "";
      const held = grouped.get(under) ?? [];
      // An untrusted record is marked where it is shown. Printed identically,
      // a peer's tool call sat directly under a `trust no` invocation with
      // nothing to tell the two apart.
      held.push(
        tool.trust?.ok === false
          ? `${describeTool(tool.payload)} · untrusted`
          : describeTool(tool.payload),
      );
      grouped.set(under, held);
    }
    for (const [under, listed] of grouped) {
      if (under !== "") lines.push(`  under ${short(under)}`);
      else if (grouped.size > 1) lines.push("  naming no invocation");
      for (const entry of listed) lines.push(`  ${under === "" ? "" : "  "}${entry}`);
    }
  }

  // Said out loud, because a transition the audit cannot attach is still a
  // signed record that the workspace changed — and silence read as "nothing
  // changed", which is the opposite.
  if (projection.transitions.length > 0) {
    lines.push("", "Workspace changes no invocation claims");
    for (const entry of projection.transitions) {
      lines.push(
        `  ${short(entry.before)} → ${short(entry.after)}${entry.trust?.ok === false ? " · untrusted" : ""}`,
      );
    }
  }

  if (projection.lifecycle.length > 0) {
    lines.push("", "Context lifecycle");
    for (const entry of projection.lifecycle) {
      const detail = `${entry.payload.evidence} · ${entry.payload.strategy ?? "strategy unrecorded"}`;
      lines.push(`  compaction · ${detail}${entry.trust?.ok === false ? " · untrusted" : ""}`);
    }
  }

  // Removals and gaps, said apart. `Invocation.project` computes `redacted`
  // for exactly this — "an absence with a tombstone beside it is a removal,
  // and one without is a replica that has not caught up" — and the prose path
  // was the only one that never read it.
  // Every record a counted tombstone names, not only the ones whose bytes
  // this replica has actually lost. The two used to be treated as one because
  // an uncollected removal is still readable — but so is a *collected* one
  // whose blobs a later identical `context for` recreated, since a Pack and a
  // ContextRender are deterministic. Split by what resolves, that record was
  // rendered in full again and counted as neither removed nor missing. The
  // tombstone is what somebody signed; whether the bytes are here right now is
  // not the same question, and is not the one this line is answering.
  const gone = projection.redacted;
  const missing = projection.unreadable.filter((commit) => !removed.has(qualify(commit)));
  if (gone.length > 0) {
    lines.push("", `${gone.length} record(s) removed by a signed redaction.`);
  }
  if (missing.length > 0) {
    lines.push("", `${missing.length} record(s) could not be read here.`);
  }
  // The capture claims nobody accountable made. `Invocation.project` keeps
  // these out of the coverage fold and off `complete`, but no human-facing
  // path printed them — so a `trace-health` record signed by a key that never
  // held `hub.trace` was invisible, and a session with one read exactly like a
  // session whose capture path reported nothing. That is the distinction the
  // field exists to make, re-collapsed by the only renderer.
  // Records a peer landed on this ref that name another session. Said, for the
  // reason `context audit` says its own: an audit surface that silently
  // discards records is the other half of the problem it is trying to solve.
  if (projection.foreign.length > 0) {
    lines.push("", `${projection.foreign.length} record(s) here name another session.`);
  }
  for (const entry of projection.unjudged) {
    const said = [
      entry.payload.source,
      entry.payload.stage ?? "stage unrecorded",
      `${entry.payload.dropped} dropped`,
    ].join(" · ");
    lines.push("", `capture health claimed by an unverified signer: ${said}`);
  }
  return lines;
};
