/**
 * One Invocation, joined from the records that were written separately.
 *
 * ```text
 * Invocation sha1:c3…
 *   Context     the exact tree, its evidence, the render commitment
 *   Runtime     model / usage / outcome / finish
 *   Attempts    only when instrumentation observed them
 *   Workspace   tree A → tree B
 *   Capture     transport, stage, coverage
 * ```
 *
 * A user should never have to know that the pre-call and post-call halves are
 * two signed records (docs/telemetry.md §19.14). They are two records because
 * they happen at different times and fail independently; they are *one* row
 * because that is the thing a person is asking about.
 *
 * The join is by Git record OID and only by that (§3). Timestamp proximity is
 * not a join here and never becomes one: two invocations a millisecond apart
 * on two fibers would pair with each other's exposures, and the resulting row
 * would be a confident account of something that did not happen. A runtime
 * record that names no exposure stays a runtime-only row, and an exposure with
 * no runtime record stays a context-only row — a harness that crashed
 * mid-call produces exactly that, and inventing the missing half would be the
 * one failure this whole plane exists to make visible.
 *
 * Everything the server owes the browser and the CLI is computed here (§14):
 * trust verification, the DAG fold, the joins, coverage and derived
 * diagnostics. Clients receive a projected Invocation; they do not rebuild
 * protocol state.
 */
import { Effect } from "effect";

import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import type { ObjectNotFound, StorageFailure, Invalid } from "../git/Error.ts";
import { qualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Trace from "../hub/Trace.ts";
import * as Records from "./Records.ts";

/**
 * What is known about how completely this session was captured (§12).
 *
 * `complete` requires a `trace-health` record that says nothing was sampled,
 * transformed or dropped. Its absence is `unknown`, never `complete`: §12.1
 * says an absence in the trace is meaningful only alongside known capture
 * capability and health, so a pipeline that never reported on itself has told
 * us nothing about what it swallowed.
 */
export type Coverage = "complete" | "degraded" | "unknown";

export interface ContextView {
  /** The exposure record's own identity. */
  readonly exposure: string;
  readonly view: Pack.View | null;
  readonly blobs: number;
  readonly gitlinks: number;
  readonly renderFormat: string | null;
  readonly renderDigest: string | null;
  /** `verified` / `absent` / `unreadable`, as `Exposure.audit` reports it. */
  readonly render: string;
  /** Whether every dimension of the exposure audit held. */
  readonly verified: boolean;
}

export interface Runtime {
  /** The telemetry record's own identity. */
  readonly record: string;
  readonly operation: string | null;
  readonly model: Records.InvocationTelemetry["model"];
  readonly usage: Records.Usage | null;
  readonly outcome: Records.InvocationTelemetry["outcome"];
  readonly finishReasons: ReadonlyArray<string>;
  readonly context: Records.ContextFacts | null;
  readonly agent: Records.InvocationTelemetry["agent"];
  readonly conversation: string | null;
  /** Absent when attempts were not instrumented; never a manufactured `[1]`. */
  readonly attempts: ReadonlyArray<Records.Attempt> | null;
}

export interface Invocation {
  /**
   * The row's canonical identity: the runtime record's OID when there is one,
   * otherwise the exposure's. Whichever it is, it is a Git record OID (§19.12).
   */
  readonly id: string;
  readonly commit: Oid;
  /** The trace records this row follows, so lanes survive the projection. */
  readonly parents: ReadonlyArray<Oid>;
  readonly context: ContextView | null;
  readonly runtime: Runtime | null;
  readonly workspace: { readonly before: string; readonly after: string } | null;
  readonly capture: Records.Capture | null;
  readonly coverage: Coverage;
  /**
   * `inputTokens / effectiveInputLimitTokens`, when both are known (§9).
   *
   * `null` whenever the denominator is missing, because a pressure ratio over
   * an incompatible or guessed limit is a number that reads as a measurement
   * and is not one. Derived, and labelled as such wherever it is shown.
   */
  readonly inputPressure: number | null;
}

export interface Projection {
  readonly session: string;
  readonly invocations: ReadonlyArray<Invocation>;
  /** Compaction and other lifecycle records, in the order they were written. */
  readonly lifecycle: ReadonlyArray<{
    readonly record: string;
    readonly payload: Records.ContextCompaction;
  }>;
  /** Tool operations, kept beneath the invocation each names when it names one. */
  readonly tools: ReadonlyArray<{
    readonly record: string;
    readonly payload: Records.ToolOperation;
  }>;
  readonly health: ReadonlyArray<Records.TraceHealth>;
  readonly coverage: Coverage;
  /**
   * Whether the trace DAG branches.
   *
   * Reported rather than resolved: §15 forbids manufacturing a single causal
   * order when concurrent parents exist, so a renderer that sees this true
   * must show lanes rather than a list.
   */
  readonly concurrent: boolean;
  /** Records this replica holds but could not read — a redaction, usually. */
  readonly unreadable: ReadonlyArray<Oid>;
}

/**
 * What the capture path admits to having lost.
 *
 * The strictest health record wins, because coverage is a claim about the
 * weakest link: one collector that sampled makes the session's audit
 * incomplete however clean every other stage was (§12).
 */
export const coverageOf = (health: ReadonlyArray<Records.TraceHealth>): Coverage => {
  if (health.length === 0) return "unknown";
  const clean = health.every(
    (entry) => entry.sampling === "none" && !entry.transformed && entry.dropped === 0,
  );
  return clean ? "complete" : "degraded";
};

/**
 * Input pressure, when the two numbers mean compatible things (§9).
 *
 * The numerator is whole-invocation input, which includes far more than the
 * repository render — §7.2 forbids describing it as ContextRender tokens — and
 * the denominator is the input budget believed usable for this call. Both have
 * to be present, and the denominator has to be positive, or there is no ratio
 * to report.
 */
export const pressureOf = (telemetry: Records.InvocationTelemetry): number | null => {
  const input = telemetry.usage?.inputTokens;
  const limit = telemetry.context?.effectiveInputLimitTokens;
  if (input === undefined || limit === undefined || limit <= 0) return null;
  return input / limit;
};

/**
 * The context half of a row: what the exposure says, and whether it holds.
 *
 * Audited rather than merely read, because §19.8 keeps Context Exposure the
 * authoritative repository-context boundary — a projection that printed the
 * pack's own claims without checking them would present a drifted view as a
 * verified one, which is precisely the confusion the audit exists to prevent.
 */
const contextOf = Effect.fn("telemetry.Invocation.contextOf")(function* (input: {
  readonly commit: Oid;
  readonly repo: string;
  readonly session: string;
  readonly trust: Parameters<typeof Exposure.audit>[0]["trust"];
}) {
  const audited = yield* Exposure.audit(input);
  const repository = yield* Repository;

  const packed = yield* Exposure.packOf(input.commit).pipe(
    Effect.catchTags({
      Invalid: () => Effect.succeed(null),
      ObjectNotFound: () => Effect.succeed(null),
    }),
  );
  const pack =
    packed === null
      ? null
      : yield* Pack.decode(packed.bytes).pipe(Effect.orElseSucceed(() => null));

  // Read for its side effect of proving the repository still holds it: an
  // exposure whose retained view was collected is one whose counts describe a
  // tree nobody can look at any more.
  if (pack !== null) {
    const tree = Pack.unqualify(pack.view.tree);
    if (tree !== null) {
      yield* repository
        .readTree(tree)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    }
  }

  return {
    exposure: audited.exposure,
    view: pack?.view ?? null,
    blobs: pack?.items.filter((item) => item.kind === "blob").length ?? 0,
    gitlinks: pack?.items.filter((item) => item.kind === "gitlink").length ?? 0,
    renderFormat: audited.payload?.renderFormat ?? null,
    renderDigest: audited.payload?.renderDigest ?? null,
    render: audited.render.state,
    verified: audited.ok,
  } satisfies ContextView;
});

/**
 * Fold one session's trace ref into the Invocations a person reads.
 *
 * `trust` is threaded through to the exposure audit rather than resolved here,
 * for the reason `Exposure.audit` takes it: a caller with no membership to
 * judge against gets an honest `null` dimension instead of a check that
 * silently passed.
 */
export const project = Effect.fn("telemetry.Invocation.project")(function* (input: {
  readonly session: string;
  readonly repo: string;
  readonly trust?: Parameters<typeof Exposure.audit>[0]["trust"];
}) {
  const walked = yield* Trace.walk(input.session);
  const telemetry = yield* Records.entries(input.session);
  const exposures = yield* Exposure.entries(input.session);

  const health: Array<Records.TraceHealth> = [];
  const lifecycle: Array<{ record: string; payload: Records.ContextCompaction }> = [];
  const tools: Array<{ record: string; payload: Records.ToolOperation }> = [];
  const workspaces: Array<{ commit: Oid; payload: Records.WorkspaceTransition }> = [];
  const runtimes: Array<{ commit: Oid; payload: Records.InvocationTelemetry }> = [];

  for (const entry of telemetry.records) {
    const payload = entry.payload;
    if (payload.type === Records.HEALTH) health.push(payload);
    else if (payload.type === Records.COMPACTION) {
      lifecycle.push({ record: qualify(entry.commit), payload });
    } else if (payload.type === Records.TOOL)
      tools.push({ record: qualify(entry.commit), payload });
    else if (payload.type === Records.WORKSPACE) {
      workspaces.push({ commit: entry.commit, payload });
    } else runtimes.push({ commit: entry.commit, payload });
  }

  const coverage = coverageOf(health);

  // A workspace transition belongs to the invocation whose exposure names the
  // tree it started from: §11's capture pattern remembers tree A before the
  // work and materializes tree B before the next invocation, so A is what
  // links them. Nothing here reads a clock.
  const transitionByBefore = new Map(
    workspaces.map((entry) => [entry.payload.beforeTree, entry.payload]),
  );

  const paired = new Set<string>();
  const invocations: Array<Invocation> = [];

  for (const runtime of runtimes) {
    const payload = runtime.payload;
    const exposure =
      payload.exposure === null
        ? null
        : (exposures.exposures.find(
            (candidate) => qualify(candidate.commit) === payload.exposure,
          ) ?? null);
    if (exposure !== null) paired.add(qualify(exposure.commit));

    const context =
      exposure === null
        ? null
        : yield* contextOf({
            commit: exposure.commit,
            repo: input.repo,
            session: input.session,
            trust: input.trust,
          });

    const before = context?.view?.tree;
    const transition = before === undefined ? undefined : transitionByBefore.get(before);

    invocations.push({
      id: qualify(runtime.commit),
      commit: runtime.commit,
      parents: walked.parents.get(runtime.commit) ?? [],
      context,
      runtime: {
        record: qualify(runtime.commit),
        operation: payload.operation?.name ?? null,
        model: payload.model,
        usage: payload.usage ?? null,
        outcome: payload.outcome,
        finishReasons: payload.response?.finishReasons ?? [],
        context: payload.context ?? null,
        agent: payload.agent,
        conversation: payload.conversation?.externalId ?? null,
        attempts: payload.attempts ?? null,
      },
      workspace:
        transition === undefined
          ? null
          : { before: transition.beforeTree, after: transition.afterTree },
      capture: payload.capture,
      coverage,
      inputPressure: pressureOf(payload),
    });
  }

  // Exposures nothing claimed. A context-only row rather than a dropped one:
  // "the harness showed the model this and then never came back" is a fact an
  // audit exists to be able to state.
  for (const exposure of exposures.exposures) {
    if (paired.has(qualify(exposure.commit))) continue;
    invocations.push({
      id: qualify(exposure.commit),
      commit: exposure.commit,
      parents: walked.parents.get(exposure.commit) ?? [],
      context: yield* contextOf({
        commit: exposure.commit,
        repo: input.repo,
        session: input.session,
        trust: input.trust,
      }),
      runtime: null,
      workspace: null,
      capture: exposure.payload.capture,
      coverage,
      inputPressure: null,
    });
  }

  // Ordered by the DAG, never by `issuedAt`: a clock is not a causal join, and
  // this is the order the walk already established from the edges themselves.
  const order = new Map(walked.records.map((entry, index) => [entry.commit, index]));
  invocations.sort(
    (left, right) =>
      (order.get(left.commit) ?? 0) - (order.get(right.commit) ?? 0) ||
      left.id.localeCompare(right.id),
  );

  return {
    session: input.session,
    invocations,
    lifecycle,
    tools,
    health,
    coverage,
    concurrent: Trace.concurrent(walked.parents),
    unreadable: [...new Set([...walked.unreadable, ...telemetry.unreadable])],
  } satisfies Projection;
});

export type InvocationError = Invalid | ObjectNotFound | StorageFailure;
