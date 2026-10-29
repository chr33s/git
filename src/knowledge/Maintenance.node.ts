/** Node maintenance host: durable requests, resumable reports, and bounded direct invocation. */
import * as path from "node:path";
import { Clock, Config, DateTime, Effect, Option, Schema } from "effect";
import { Invalid } from "../git/Error.ts";
import { isOid } from "../git/Oid.ts";
import { checkRefName } from "../git/Store.ts";
import { Repository } from "../git/Repository.ts";
import * as Dag from "../git/Dag.ts";
import * as Event from "../hub/Event.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Record from "../trust/Record.ts";
import * as Journal from "./Journal.node.ts";
import * as Inputs from "./Inputs.ts";
import * as Maintenance from "./Maintenance.ts";
import * as Outcomes from "./Outcomes.ts";
import * as Session from "../hub/Session.ts";
import * as Runner from "./Runner.node.ts";
import * as Assistance from "./Assistance.node.ts";
import * as Proposal from "./Proposal.ts";

const Positive = Schema.Int.check(Schema.isGreaterThan(0));
export const Configuration = Schema.Struct({
  version: Schema.Literal(1),
  sourceRef: Schema.optionalKey(Schema.String),
  actors: Schema.optionalKey(Schema.Array(Outcomes.Actor)),
  runner: Schema.optionalKey(Runner.Configuration),
  publisher: Schema.optionalKey(Schema.Struct({ key: Schema.String })),
  conceptRoots: Schema.optionalKey(Schema.Array(Schema.String)),
  limits: Schema.optionalKey(
    Schema.Struct({
      concepts: Schema.optionalKey(Positive),
      outcomes: Schema.optionalKey(Positive),
      timeoutMs: Schema.optionalKey(Positive),
      evidenceBytes: Schema.optionalKey(Positive),
    }),
  ),
});
export const configuration = Effect.fn("knowledge.MaintenanceHost.configuration")(function* (
  directory: string,
) {
  const stored = yield* Journal.read(
    path.join(Journal.directory(directory), "config.json"),
    Configuration,
  );
  const actors = stored?.actors ?? [];
  if (new Set(actors.map((actor) => actor.fingerprint)).size !== actors.length)
    return yield* new Invalid({
      field: "actors",
      reason: "actor mappings must identify each fingerprint exactly once",
    });
  return {
    sourceRef: stored?.sourceRef ?? null,
    actors: stored?.actors ?? [],
    runner: stored?.runner ?? null,
    publisherKey: stored?.publisher?.key ?? null,
    conceptRoots: stored?.conceptRoots ?? [Proposal.ROOT],
    limits: { ...Maintenance.DEFAULTS, ...stored?.limits },
  };
});

const identity = Effect.fn("knowledge.MaintenanceHost.identity")(function* (value: Schema.Json) {
  return (yield* Input.digest(value)).slice("sha256:".length);
});
const supported = new Set([
  "session.produced",
  "session.opened",
  "decision.resolved",
  "task.closed",
  "pr.merged",
  "pr.closed",
  "event.redacted",
]);

export const request = Effect.fn("knowledge.MaintenanceHost.request")(function* (input: {
  readonly directory: string;
  readonly repository: string;
  readonly trigger: "manual" | "wake";
}) {
  const repository = yield* Repository;
  const configured = yield* configuration(input.directory);
  const target = configured.sourceRef ?? (yield* repository.head);
  let event: string | null = null;
  let ref: string | null = null;
  let commit: string | null = null;
  if (input.trigger === "wake") {
    event = yield* Config.string("CHR33S_GIT_EVENT");
    ref = yield* Config.string("CHR33S_GIT_REF");
    commit = yield* Config.string("CHR33S_GIT_COMMIT");
    const hintedRepo = yield* Config.string("CHR33S_GIT_REPO");
    if (
      hintedRepo !== input.repository ||
      !supported.has(event) ||
      !ref.startsWith("refs/hub/") ||
      checkRefName(ref) !== null ||
      !isOid(commit)
    )
      return yield* new Invalid({
        field: "wake",
        reason: "invalid maintenance notification hints",
      });
    const head = yield* repository.resolve(ref);
    if (head === null)
      return yield* new Invalid({ field: "wake", reason: "the notified ref is unavailable" });
    const reachable = yield* Dag.reachable(head, null, Event.isHubCommit, yield* Event.ceilingOf());
    if (!reachable.has(commit))
      return yield* new Invalid({
        field: "wake",
        reason: "the notified record is not reachable from its ref",
      });
    const record = yield* Record.read(commit, Event.RECORD);
    const envelope = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ type: Schema.String })),
    )(new TextDecoder().decode(record.payload)).pipe(
      Effect.mapError((cause) => new Invalid({ field: "wake", reason: cause.message })),
    );
    if (envelope.type !== event)
      return yield* new Invalid({
        field: "wake",
        reason: "the notified event type does not match the retained record",
      });
    if (event === "session.opened")
      return { version: 1, state: "ignored", reason: "session openings are bookkeeping" } as const;
    if (event === "session.produced") {
      const output = yield* Session.decode(record.payload);
      if (
        output.type === "session.produced" &&
        output.refs.some((name) => name.startsWith("refs/heads/agent-memory/"))
      )
        return { version: 1, state: "ignored", reason: "maintenance bookkeeping" } as const;
    }
  }
  const id = yield* identity({
    repository: input.repository,
    target,
    trigger: input.trigger,
    event,
    ref,
    commit,
  });
  const location = Journal.directory(input.directory);
  yield* Journal.initialize(location);
  const destination = path.join(location, "requests", `${id}.json`);
  const existing = yield* Journal.read(destination, Journal.Request);
  if (existing !== null)
    return { version: 1, state: "requested", request: existing, reused: true } as const;
  const requested: Journal.Request = {
    version: 1,
    id,
    repository: input.repository,
    target,
    trigger: input.trigger,
    event,
    ref,
    commit,
    requestedAt: DateTime.formatIso(yield* DateTime.now),
  };
  yield* Journal.write(destination, requested);
  return { version: 1, state: "requested", request: requested, reused: false } as const;
});

export interface Run {
  readonly directory: string;
  readonly sourceRef?: string | undefined;
  readonly evaluatedAt?: string | undefined;
  readonly job?: string | undefined;
  readonly maximumConcepts?: number | undefined;
  readonly maximumOutcomes?: number | undefined;
  readonly timeoutMs?: number | undefined;
  readonly dryRun?: boolean | undefined;
  readonly propose?: boolean | undefined;
}

export const show = Effect.fn("knowledge.MaintenanceHost.show")(function* (
  directory: string,
  id: string,
) {
  if (!/^[0-9a-f]{64}$/u.test(id))
    return yield* new Invalid({ field: "job", reason: "invalid job identity" });
  const job = yield* Journal.read(
    path.join(Journal.directory(directory), "jobs", `${id}.json`),
    Journal.Job,
  );
  if (job === null) return yield* new Invalid({ field: "job", reason: "no such maintenance job" });
  return job;
});

export const run = Effect.fn("knowledge.MaintenanceHost.run")(function* (input: Run) {
  if (input.dryRun === true && input.propose === true)
    return yield* new Invalid({
      field: "dry-run",
      reason: "dry-run and propose are mutually exclusive",
    });
  const location = Journal.directory(input.directory);
  const configured = yield* configuration(input.directory);
  let limits = {
    concepts: input.maximumConcepts ?? configured.limits.concepts,
    outcomes: input.maximumOutcomes ?? configured.limits.outcomes,
    timeoutMs: input.timeoutMs ?? configured.limits.timeoutMs,
    evidenceBytes: configured.limits.evidenceBytes,
  };
  for (const key of ["concepts", "outcomes", "timeoutMs", "evidenceBytes"] as const) {
    if (
      !Number.isSafeInteger(limits[key]) ||
      limits[key] < 1 ||
      limits[key] > configured.limits[key]
    )
      return yield* new Invalid({
        field: key,
        reason: "run limits must be positive safe integers within operator ceilings",
      });
  }
  const operatorDigest = yield* Input.digest(configured);
  const execute = Effect.gen(function* () {
    const started = yield* Clock.currentTimeMillis;
    const boundedPreparation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const remaining = limits.timeoutMs - ((yield* Clock.currentTimeMillis) - started);
        return yield* effect.pipe(
          Effect.timeoutOrElse({
            duration: Math.max(0, remaining),
            orElse: () =>
              Effect.fail(
                new Invalid({
                  field: "timeout",
                  reason:
                    "pass budget exhausted before a work plan could be checkpointed; retry the evaluation",
                }),
              ),
          }),
        );
      });
    const historyPath = path.join(location, "history.json");
    const previousHistory = yield* Journal.read(historyPath, Journal.History);
    let history = previousHistory?.entries ?? [];
    let round = previousHistory?.round ?? 0;
    const outcomeHistoryPath = path.join(location, "outcomes.json");
    let outcomeHistory =
      (yield* Journal.read(outcomeHistoryPath, Journal.OutcomeHistory))?.entries ?? [];
    let job: Journal.Job;
    if (input.job !== undefined) {
      job = yield* show(input.directory, input.job);
      if (
        job.operatorDigest !== operatorDigest ||
        Input.canonical(job.actors) !== Input.canonical(configured.actors)
      )
        return yield* new Invalid({
          field: "job",
          reason: "operator configuration changed; start a new evaluation",
        });
      if (
        (input.maximumOutcomes !== undefined && input.maximumOutcomes !== job.limits.outcomes) ||
        (input.timeoutMs !== undefined && input.timeoutMs !== job.limits.timeoutMs)
      )
        return yield* new Invalid({
          field: "job",
          reason: "resume cannot change the stored limits",
        });
      for (const key of ["concepts", "outcomes", "timeoutMs", "evidenceBytes"] as const) {
        if (job.limits[key] > configured.limits[key])
          return yield* new Invalid({
            field: "job",
            reason: "stored limits exceed current operator ceilings; start a new evaluation",
          });
      }
      limits = job.limits;
      if (
        (input.sourceRef !== undefined && input.sourceRef !== job.plan.manifest.sourceRef) ||
        (input.evaluatedAt !== undefined &&
          Date.parse(input.evaluatedAt) !== Date.parse(job.plan.manifest.evaluatedAt)) ||
        (input.maximumConcepts !== undefined &&
          input.maximumConcepts !== job.plan.manifest.limits.concepts)
      )
        return yield* new Invalid({
          field: "job",
          reason:
            "resume uses the stored input manifest; incompatible overrides require a new evaluation",
        });
      if ((job.state === "reported" && input.propose !== true) || job.state === "proposed")
        return { version: 1, state: job.state, exitCode: 0, job } as const;
    } else {
      const sourceRef = input.sourceRef ?? configured.sourceRef;
      if (sourceRef === null || input.evaluatedAt === undefined)
        return yield* new Invalid({
          field: "inputs",
          reason: "a new run requires --at and --ref (or an operator-configured source ref)",
        });
      const manifest = yield* boundedPreparation(
        Inputs.capture({
          sourceRef,
          evaluatedAt: input.evaluatedAt,
          limits: { concepts: limits.concepts, sessions: 512, bytes: limits.evidenceBytes },
          configuration: {
            limits,
            sourceRef,
            version: Maintenance.VERSION,
            historyDigest: yield* Input.digest(history),
            selectionRound: round,
            actors: configured.actors,
            operatorDigest,
            outcomeHistory,
          },
        }),
      );
      const plan = yield* boundedPreparation(
        Maintenance.plan({ manifest, history, maximum: limits.concepts, round }),
      );
      const requests = (yield* Journal.names(path.join(location, "requests"))).map((name) =>
        name.slice(0, -5),
      );
      const id = yield* identity({ manifest, requests });
      const existing =
        input.dryRun === true
          ? null
          : yield* Journal.read(path.join(location, "jobs", `${id}.json`), Journal.Job);
      job = existing ?? {
        version: 1,
        id,
        state: "requested",
        plan,
        limits,
        requests,
        cursor: 0,
        results: [],
        pendingOutcomes: manifest.outcomeHeads
          .map(({ ref }) => ref)
          .sort((left, right) => {
            const before = outcomeHistory.find((entry) => entry.ref === left)?.checkedAt ?? "";
            const after = outcomeHistory.find((entry) => entry.ref === right)?.checkedAt ?? "";
            return before < after
              ? -1
              : before > after
                ? 1
                : left < right
                  ? -1
                  : left > right
                    ? 1
                    : 0;
          }),
        outcomes: [],
        actors: configured.actors,
        operatorDigest,
        candidate: null,
        assistance: {
          version: 1,
          state: configured.runner === null ? "not-configured" : "pending",
          runner:
            configured.runner === null
              ? null
              : { id: configured.runner.id, version: configured.runner.version },
          fingerprint: null,
          output: null,
          diagnostic: null,
        },
        createdAt: manifest.evaluatedAt,
        updatedAt: manifest.evaluatedAt,
        elapsedMs: 0,
        retry: null,
        publication: "not-requested",
      };
    }
    const destination = path.join(location, "jobs", `${job.id}.json`);
    const save = (next: Journal.Job) =>
      input.dryRun === true ? Effect.void : Journal.write(destination, next);
    job = { ...job, state: "running", retry: null };
    yield* save(job);
    round = Math.max(round, job.plan.round + 1);
    if (input.dryRun !== true)
      yield* Journal.write(historyPath, { version: 1, round, entries: history });
    const work = [...job.plan.chosen, ...job.plan.pending];
    if (!Number.isSafeInteger(job.cursor) || job.cursor < 0 || job.cursor > work.length)
      return yield* new Invalid({
        field: "job.cursor",
        reason: "stored continuation is outside its retained work plan",
      });
    const passStart = job.cursor;
    const process = Effect.gen(function* () {
      while (job.cursor < work.length && job.cursor - passStart < limits.concepts) {
        const remaining = limits.timeoutMs - ((yield* Clock.currentTimeMillis) - started);
        if (remaining <= 0) break;
        const selected = work[job.cursor];
        if (selected === undefined)
          return yield* new Invalid({
            field: "job.cursor",
            reason: "stored continuation has no selected item",
          });
        const checked = yield* Maintenance.check(job.plan.manifest, selected).pipe(
          Effect.timeoutOption(remaining),
        );
        if (Option.isNone(checked)) break;
        const result = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
          JSON.stringify(checked.value),
        ).pipe(Effect.mapError((cause) => new Invalid({ field: "report", reason: cause.message })));
        const results = [...job.results, result];
        if (new TextEncoder().encode(JSON.stringify(results)).length > limits.evidenceBytes) {
          job = { ...job, retry: "report byte budget exhausted; completed results retained" };
          break;
        }
        job = { ...job, cursor: job.cursor + 1, results };
        history = [
          ...history.filter(
            (prior) => !checked.value.history.some((next) => next.path === prior.path),
          ),
          ...checked.value.history,
        ];
        yield* save(job);
        if (input.dryRun !== true)
          yield* Journal.write(historyPath, { version: 1, round, entries: history });
      }
      for (const ref of job.pendingOutcomes.slice(0, limits.outcomes)) {
        const remaining = limits.timeoutMs - ((yield* Clock.currentTimeMillis) - started);
        if (remaining <= 0) break;
        const checked = yield* Outcomes.evaluate({
          manifest: job.plan.manifest,
          ref,
          actors: job.actors,
        }).pipe(Effect.timeoutOption(remaining));
        if (Option.isNone(checked)) break;
        const result = checked.value;
        const outcomes = [...job.outcomes.filter((held) => held.key !== result.key), result];
        if (
          new TextEncoder().encode(JSON.stringify({ results: job.results, outcomes })).length >
          limits.evidenceBytes
        ) {
          job = {
            ...job,
            retry: "outcome report byte budget exhausted; completed results retained",
          };
          break;
        }
        const pending = job.pendingOutcomes.filter((name) => name !== ref);
        job = {
          ...job,
          outcomes,
          pendingOutcomes: result.coverage.complete ? pending : [...pending, ref],
        };
        outcomeHistory = [
          ...outcomeHistory.filter((entry) => entry.ref !== ref),
          {
            ref,
            key: result.key,
            checkedAt: job.plan.manifest.evaluatedAt,
            evidenceHead: result.evidenceHead,
          },
        ];
        yield* save(job);
        if (input.dryRun !== true)
          yield* Journal.write(outcomeHistoryPath, { version: 1, entries: outcomeHistory });
      }
      let current = yield* Inputs.current(
        job.plan.manifest,
        job.candidate === null ? undefined : Session.refOf(`memory-${job.candidate}`),
      );
      const deterministicComplete = job.cursor === work.length && job.pendingOutcomes.length === 0;
      const evidence: Proposal.Evidence = {
        version: 1,
        manifest: job.plan.manifest,
        actors: job.actors,
        concepts: job.results,
        outcomes: job.outcomes,
      };
      if (current && deterministicComplete && input.dryRun !== true) {
        const remaining = limits.timeoutMs - ((yield* Clock.currentTimeMillis) - started);
        if (remaining > 0) {
          const assistance = yield* Assistance.assess({
            directory: input.directory,
            evidence,
            configuration: configured.runner,
            timeoutMs: remaining,
            roots: configured.conceptRoots,
            maximumBytes: limits.evidenceBytes,
            previous: job.assistance,
            checkpoint: (assistance) => {
              job = { ...job, assistance };
              return save(job);
            },
          });
          job = { ...job, assistance };
          yield* save(job);
        }
      }
      let publication: Journal.Job["publication"] = current
        ? input.propose === true
          ? deterministicComplete &&
            (job.assistance.state === "not-configured" ||
              (job.assistance.state === "complete" && job.assistance.output?.drafts.length === 0))
            ? "no-proposal"
            : "pending"
          : "not-requested"
        : "superseded-input";
      job = { ...job, publication };
      yield* save(job);
      if (
        current &&
        deterministicComplete &&
        input.propose === true &&
        input.dryRun !== true &&
        job.assistance.state === "complete" &&
        job.assistance.output !== null &&
        job.assistance.runner !== null
      ) {
        const published = yield* Assistance.propose({
          directory: input.directory,
          evidence,
          output: job.assistance.output,
          runner: job.assistance.runner,
          roots: configured.conceptRoots,
          maximumBytes: limits.evidenceBytes,
          keyPath: configured.publisherKey,
          actors: configured.actors,
          checkpoint: (candidate) => {
            job = { ...job, candidate };
            return save(job);
          },
        });
        publication = published.state;
        if (publication === "superseded-input") current = false;
      }
      const complete =
        deterministicComplete &&
        publication !== "pending" &&
        job.assistance.state !== "partial" &&
        (configured.runner === null ||
          input.dryRun === true ||
          job.assistance.state === "complete");
      job = {
        ...job,
        state: current
          ? complete
            ? publication === "written" || publication === "unchanged"
              ? "proposed"
              : "reported"
            : "partial"
          : "retryable-failure",
        updatedAt: DateTime.formatIso(yield* DateTime.now),
        elapsedMs: job.elapsedMs + (yield* Clock.currentTimeMillis) - started,
        publication,
        retry: current ? job.retry : "captured source/trust inputs moved; start a new evaluation",
      };
      yield* save(job);
      if (input.dryRun !== true) yield* Journal.acknowledge(location, job.requests);
      return {
        version: 1,
        state: job.state,
        exitCode: current ? (publication === "conflict" ? 3 : complete ? 0 : 2) : 3,
        job,
      } as const;
    });
    const remaining = Math.max(0, limits.timeoutMs - ((yield* Clock.currentTimeMillis) - started));
    const result = yield* process.pipe(
      Effect.tapError((error) =>
        save({
          ...job,
          state: "retryable-failure",
          retry: `${error._tag}; completed checkpoints retained`,
        }),
      ),
      Effect.onInterrupt(() =>
        save({ ...job, state: "partial", retry: "interrupted; resume the saved job" }).pipe(
          Effect.orDie,
        ),
      ),
      Effect.timeoutOption(remaining),
    );
    if (Option.isSome(result)) return result.value;
    job = {
      ...job,
      state: "partial",
      publication: input.propose === true ? "pending" : job.publication,
      updatedAt: DateTime.formatIso(yield* DateTime.now),
      elapsedMs: job.elapsedMs + (yield* Clock.currentTimeMillis) - started,
      retry: "pass time budget exhausted; resume the saved job and publication checkpoints",
    };
    yield* save(job);
    return { version: 1, state: job.state, exitCode: 2, job } as const;
  });
  if (input.dryRun === true) return yield* execute;
  yield* Journal.initialize(location);
  return yield* Journal.withLock(location, execute);
});
