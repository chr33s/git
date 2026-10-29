/** Bounded deterministic curation planning. Models and publication belong to separate adapters. */
import { Effect, Schema } from "effect";
import { compareUtf8 as compare } from "../text.ts";
import { Invalid } from "../git/Error.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Trust from "../trust/Projection.ts";
import * as Check from "./Check.ts";
import * as Concept from "./Concept.ts";
import * as Inputs from "./Inputs.ts";

export const VERSION = "knowledge-maintenance/1";
export const DEFAULTS = {
  concepts: 25,
  outcomes: 5,
  timeoutMs: 120_000,
  evidenceBytes: 1024 * 1024,
} as const;
export const History = Schema.Struct({
  path: Schema.String,
  blob: Schema.String,
  checkedAt: Schema.String,
  nextBoundary: Schema.NullOr(Schema.String),
  dependencies: Schema.Array(
    Schema.Struct({ path: Schema.String, found: Schema.NullOr(Schema.String) }),
  ),
});
export interface History extends Schema.Schema.Type<typeof History> {}
export const Selection = Schema.Struct({
  path: Schema.String,
  blob: Schema.String,
  reason: Schema.Literals(["changed", "unchecked", "expired", "aging"]),
});
export interface Selection extends Schema.Schema.Type<typeof Selection> {}
export const Plan = Schema.Struct({
  version: Schema.Literal(1),
  round: Schema.Int,
  history: Schema.Array(History),
  manifest: Input.InputManifest,
  chosen: Schema.Array(Selection),
  pending: Schema.Array(Selection),
  complete: Schema.Boolean,
});
export interface Plan extends Schema.Schema.Type<typeof Plan> {}

/** Half of a pass is reserved for aging/unchecked work, with alternating stable choices. */
export interface SelectionResult {
  readonly chosen: ReadonlyArray<Selection>;
  readonly pending: ReadonlyArray<Selection>;
}

export const select = (
  candidates: ReadonlyArray<Selection>,
  history: ReadonlyArray<History>,
  maximum: number,
  round = 0,
): SelectionResult => {
  const previous = new Map(history.map((entry) => [entry.path, entry]));
  const ordered = [...candidates].sort(
    (left, right) =>
      compare(
        previous.get(left.path)?.checkedAt ?? "",
        previous.get(right.path)?.checkedAt ?? "",
      ) || compare(left.path, right.path),
  );
  const aging = ordered.filter((entry) => entry.reason === "aging" || entry.reason === "expired");
  const unchecked = ordered.filter((entry) => entry.reason === "unchecked");
  const changed = ordered.filter((entry) => entry.reason === "changed");
  const chosen: Array<Selection> = [];
  if (maximum === 1) {
    const groups = [aging, unchecked, changed];
    for (let offset = 0; offset < groups.length; offset += 1) {
      const first = groups[(round + offset) % groups.length]?.[0];
      if (first !== undefined)
        return { chosen: [first], pending: ordered.filter((entry) => entry.path !== first.path) };
    }
    return { chosen, pending: [] };
  }
  let old = 0;
  let fresh = 0;
  const reserve = Math.ceil(maximum / 2);
  while (chosen.length < reserve && (old < aging.length || fresh < unchecked.length)) {
    if (old < aging.length) chosen.push(aging[old++]!);
    if (chosen.length < reserve && fresh < unchecked.length) chosen.push(unchecked[fresh++]!);
  }
  for (const entry of [...changed, ...aging.slice(old), ...unchecked.slice(fresh)]) {
    if (chosen.length >= maximum) break;
    chosen.push(entry);
  }
  const selected = new Set(chosen.map((entry) => entry.path));
  return { chosen, pending: ordered.filter((entry) => !selected.has(entry.path)) };
};

export const plan = Effect.fn("knowledge.Maintenance.plan")(function* (input: {
  readonly manifest: Input.InputManifest;
  readonly history: ReadonlyArray<History>;
  readonly maximum: number;
  readonly round?: number;
}) {
  if (!Number.isSafeInteger(input.maximum) || input.maximum < 1)
    return yield* new Invalid({
      field: "max-concepts",
      reason: "max-concepts must be a positive safe integer",
    });
  const tree = input.manifest.view === null ? null : unqualify(input.manifest.view.tree);
  if (tree === null)
    return yield* new Invalid({
      field: "view",
      reason: "maintenance requires a retained committed source tree",
    });
  const reader = yield* Inputs.reader(input.manifest);
  const prior = new Map(input.history.map((entry) => [entry.path, entry]));
  const candidates: Array<Selection> = [];
  for (const file of yield* Check.list(tree, Concept.BUNDLE).pipe(
    Effect.provideService(Repository, reader),
  )) {
    const previous = prior.get(file.path);
    let reason: Selection["reason"] =
      previous === undefined
        ? "unchecked"
        : previous.blob !== qualify(file.oid)
          ? "changed"
          : "aging";
    if (previous !== undefined && reason === "aging") {
      for (const dependency of previous.dependencies) {
        const entry = yield* reader.findPath(tree, dependency.path);
        if ((entry === null ? null : qualify(entry.oid)) !== dependency.found) {
          reason = "changed";
          break;
        }
      }
      if (
        reason === "aging" &&
        previous.nextBoundary !== null &&
        Date.parse(previous.nextBoundary) <= Date.parse(input.manifest.evaluatedAt)
      )
        reason = "expired";
    }
    candidates.push({ path: file.path, blob: qualify(file.oid), reason });
  }
  const selected = select(candidates, input.history, input.maximum, input.round ?? 0);
  return {
    version: 1,
    round: input.round ?? 0,
    history: input.history,
    manifest: input.manifest,
    ...selected,
    complete: selected.pending.length === 0,
  } satisfies Plan;
});

export interface Checked {
  readonly selection: Selection;
  readonly evidence: Check.Report;
  readonly semantic: { readonly state: "not-assessed"; readonly scope: string };
  readonly history: ReadonlyArray<History>;
}

/** One checkpoint-sized unit of deterministic work, against captured heads only. */
export const check = Effect.fn("knowledge.Maintenance.check")(function* (
  manifest: Input.InputManifest,
  selection: Selection,
) {
  const view = manifest.view;
  if (view === null) return yield* new Invalid({ field: "view", reason: "missing source view" });
  const reader = yield* Inputs.reader(manifest);
  return yield* Effect.gen(function* () {
    const stored = yield* Genesis.readGenesis();
    const trust = stored === null ? null : yield* Trust.project(stored.genesis);
    const evidence = yield* Check.check({
      view,
      paths: [selection.path],
      evaluationTime: new Date(manifest.evaluatedAt),
      repo: manifest.repository,
      trust,
      sessionHeads: new Map(
        manifest.sessionHeads.flatMap(({ ref, head }) => {
          const oid = unqualify(head);
          return oid === null ? [] : [[ref, oid] as const];
        }),
      ),
      trustHead: manifest.trustHead === null ? null : unqualify(manifest.trustHead),
    });
    const history = evidence.concepts.map((entry): History => ({
      path: entry.path,
      blob: entry.blob,
      checkedAt: manifest.evaluatedAt,
      nextBoundary: entry.temporal.state === "within-deadline" ? entry.temporal.deadline : null,
      dependencies: entry.repositoryEvidence.map((dependency) => ({
        path: dependency.path,
        found: dependency.state === "unchanged" ? dependency.recorded : (dependency.found ?? null),
      })),
    }));
    return {
      selection,
      evidence,
      semantic: {
        state: "not-assessed",
        scope: "offline provenance, dependencies, deadlines and retention only",
      },
      history,
    } satisfies Checked;
  }).pipe(Effect.provideService(Repository, reader));
});
