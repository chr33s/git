/** A model suggests typed drafts; deterministic code owns paths, evidence and publication. */
import { Effect, Schema } from "effect";
import { Invalid } from "../git/Error.ts";
import { isTree } from "../git/Format.ts";
import { unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Secrets from "../hub/Secrets.ts";
import * as Concept from "./Concept.ts";
import * as Outcomes from "./Outcomes.ts";

export const Assessment = Schema.Struct({
  target: Schema.String,
  state: Schema.Literals([
    "not-assessed",
    "supported-within-scope",
    "contradicted-within-scope",
    "inconclusive",
  ]),
  scope: Schema.Array(Schema.String),
  evidence: Schema.Array(Schema.String),
  explanation: Schema.String,
});
export const Draft = Schema.Struct({
  path: Schema.String,
  status: Schema.Literal("draft"),
  type: Schema.String,
  title: Schema.String,
  description: Schema.String,
  body: Schema.String,
  claim: Schema.Literals(["scoped-observation", "human-decision", "repeated-pattern"]),
  outcomes: Schema.Array(Schema.String),
  scope: Schema.Array(Schema.String),
  evidence: Schema.Array(Schema.String),
});
export type Draft = typeof Draft.Type;
export const Output = Schema.Struct({
  version: Schema.Literal(1),
  assessments: Schema.Array(Assessment),
  drafts: Schema.Array(Draft),
});
export type Output = typeof Output.Type;
export const Evidence = Schema.Struct({
  version: Schema.Literal(1),
  manifest: Input.InputManifest,
  actors: Schema.Array(Outcomes.Actor),
  concepts: Schema.Array(Schema.Json),
  outcomes: Schema.Array(Outcomes.Report),
});
export type Evidence = typeof Evidence.Type;
export const Label = Schema.Struct({ id: Schema.String, version: Schema.String });
export type Label = typeof Label.Type;
export const ROOT = ".gitplus/knowledge/maintained/";
const RESERVED = new Set([
  "agents.md",
  "claude.md",
  "gemini.md",
  "instructions.md",
  "policy.md",
  "skill.md",
]);

export const pathAllowed = (path: string, roots: ReadonlyArray<string>): boolean => {
  if (
    !path.startsWith(`${Concept.BUNDLE}/`) ||
    path.startsWith(`${Concept.BUNDLE}/reports/`) ||
    !Concept.isConceptPath(Concept.BUNDLE, path)
  )
    return false;
  if (
    !roots.some(
      (root) =>
        root.startsWith(`${Concept.BUNDLE}/`) && root.endsWith("/") && path.startsWith(root),
    )
  )
    return false;
  const segments = path.slice(Concept.BUNDLE.length + 1).split("/");
  return (
    segments.every(
      (segment) => /^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(segment) && !segment.includes(".."),
    ) && !RESERVED.has(segments.at(-1)?.toLowerCase() ?? "")
  );
};

/** Existing symlinks, executable files and non-directory ancestors cannot be revised. */
export const safePath = Effect.fn("knowledge.Proposal.safePath")(function* (
  tree: string,
  path: string,
  roots: ReadonlyArray<string>,
) {
  const repository = yield* Repository;
  const oid = unqualify(tree);
  if (oid === null || !pathAllowed(path, roots))
    return yield* new Invalid({
      field: "proposal.path",
      reason: "proposal path is outside the configured Concept allowlist",
    });
  const parts = path.split("/");
  for (let index = 1; index <= parts.length; index += 1) {
    const found = yield* repository.findPath(oid, parts.slice(0, index).join("/"));
    if (found !== null && (index < parts.length ? !isTree(found.mode) : found.mode !== "100644"))
      return yield* new Invalid({
        field: "proposal.path",
        reason: "proposal path traverses a symlink, executable, or non-directory entry",
      });
  }
});

const Checked = Schema.Struct({
  evidence: Schema.Struct({
    concepts: Schema.Array(
      Schema.Struct({
        path: Schema.String,
        blob: Schema.String,
        citations: Schema.Array(Schema.Struct({ record: Schema.String, state: Schema.String })),
        repositoryEvidence: Schema.Array(
          Schema.Struct({ path: Schema.String, found: Schema.optionalKey(Schema.String) }),
        ),
      }),
    ),
  }),
});
export const validate = Effect.fn("knowledge.Proposal.validate")(function* (input: {
  readonly evidence: Evidence;
  readonly output: Output;
  readonly roots: ReadonlyArray<string>;
  readonly maximumBytes: number;
}) {
  const { evidence, output } = input;
  if (new TextEncoder().encode(JSON.stringify(output)).length > input.maximumBytes)
    return yield* new Invalid({
      field: "proposal",
      reason: "proposal exceeds its configured byte ceiling",
    });
  const known = new Set<string>();
  const supports = new Map<
    string,
    { readonly scope: ReadonlySet<string>; readonly evidence: ReadonlySet<string> }
  >();
  const scopes = new Set<string>();
  for (const raw of evidence.concepts) {
    const checked = yield* Schema.decodeUnknownEffect(Checked)(raw).pipe(
      Effect.mapError(
        () =>
          new Invalid({ field: "proposal.evidence", reason: "invalid deterministic check report" }),
      ),
    );
    for (const entry of checked.evidence.concepts) {
      known.add(entry.blob);
      const scope = new Set([
        entry.path,
        ...entry.repositoryEvidence.map((dependency) => dependency.path),
      ]);
      const records = new Set([
        entry.blob,
        ...entry.citations
          .filter((citation) => citation.state === "accepted")
          .map((citation) => citation.record),
        ...entry.repositoryEvidence.flatMap((dependency) =>
          dependency.found === undefined ? [] : [dependency.found],
        ),
      ]);
      supports.set(entry.path, { scope, evidence: records });
      for (const path of scope) scopes.add(path);
      for (const record of records) known.add(record);
    }
  }
  const outcomes = new Map(
    evidence.outcomes
      .filter((outcome) => outcome.eligible)
      .map((outcome) => [outcome.key, outcome]),
  );
  for (const outcome of outcomes.values()) {
    supports.set(outcome.key, {
      scope: new Set(outcome.paths),
      evidence: new Set([
        ...outcome.records.map((record) => record.commit),
        ...outcome.sources.flatMap((source) => [source.record, source.originalHead]),
        ...(outcome.acceptedHead === null ? [] : [outcome.acceptedHead]),
        ...(outcome.mergeCommit === null ? [] : [outcome.mergeCommit]),
      ]),
    });
    for (const path of outcome.paths) scopes.add(path);
    for (const record of outcome.records) known.add(record.commit);
    for (const source of outcome.sources) {
      known.add(source.record);
      known.add(source.originalHead);
    }
    if (outcome.acceptedHead !== null) known.add(outcome.acceptedHead);
    if (outcome.mergeCommit !== null) known.add(outcome.mergeCommit);
  }
  const paths = new Set<string>();
  for (const draft of output.drafts) {
    const view = evidence.manifest.view;
    if (view === null)
      return yield* new Invalid({
        field: "proposal.view",
        reason: "proposal requires a captured source view",
      });
    yield* safePath(view.tree, draft.path, input.roots);
    if (paths.has(draft.path.toLowerCase()))
      return yield* new Invalid({
        field: "proposal.path",
        reason: "duplicate or case-colliding Concept path",
      });
    paths.add(draft.path.toLowerCase());
    if (
      draft.scope.length === 0 ||
      draft.scope.some((path) => !scopes.has(path)) ||
      draft.evidence.length === 0 ||
      draft.evidence.some((record) => !known.has(record))
    )
      return yield* new Invalid({
        field: "proposal.evidence",
        reason: "draft scope and evidence must come from examined inputs",
      });
    const keys = new Set(draft.outcomes);
    if (draft.outcomes.some((key) => !outcomes.has(key)))
      return yield* new Invalid({
        field: "proposal.outcome",
        reason: "draft names an ineligible or unknown outcome",
      });
    const selectedSupports = [...supports].filter(
      ([target, support]) =>
        keys.has(target) ||
        (draft.outcomes.length === 0 &&
          !outcomes.has(target) &&
          draft.evidence.some((record) => support.evidence.has(record))),
    );
    const selectedScope = new Set(selectedSupports.flatMap(([, support]) => [...support.scope]));
    const selectedEvidence = new Set(
      selectedSupports.flatMap(([, support]) => [...support.evidence]),
    );
    if (
      draft.scope.some((path) => !selectedScope.has(path)) ||
      draft.evidence.some((record) => !selectedEvidence.has(record))
    )
      return yield* new Invalid({
        field: "proposal.evidence",
        reason: "draft scope and evidence must belong to its selected supporting inputs",
      });
    if (draft.claim === "repeated-pattern" && keys.size < 2)
      return yield* new Invalid({
        field: "proposal.outcome",
        reason: "a repeated pattern requires distinct underlying PR outcomes",
      });
    if (
      draft.claim === "human-decision" &&
      ![...keys].some((key) =>
        outcomes
          .get(key)
          ?.records.some(
            (record) =>
              record.actorKind === "human" &&
              record.type === "review.submitted" &&
              record.reviewState !== "dismissed" &&
              (record.text?.trim().length ?? 0) > 0,
          ),
      )
    )
      return yield* new Invalid({
        field: "proposal.actor",
        reason: "a human decision requires an operator-classified human's accepted explicit review",
      });
    if (
      Secrets.scan([draft.type, draft.title, draft.description, draft.body].join("\n")).length > 0
    )
      return yield* new Invalid({ field: "proposal", reason: "draft contains a possible secret" });
    const parsed = Concept.parse({
      bundle: Concept.BUNDLE,
      path: draft.path,
      bytes: new TextEncoder().encode(
        render(draft, [], ".gitplus/knowledge/reports/pending.json", evidence.outcomes),
      ),
    });
    if (!parsed.ok || parsed.concept.status !== "draft")
      return yield* new Invalid({
        field: "proposal",
        reason: "generated Concept must explicitly parse as draft",
      });
  }
  for (const assessment of output.assessments) {
    const support = supports.get(assessment.target);
    if (
      support === undefined ||
      (assessment.state !== "not-assessed" && assessment.evidence.length === 0) ||
      assessment.scope.length === 0 ||
      assessment.scope.some((path) => !support.scope.has(path)) ||
      assessment.evidence.some((record) => !support.evidence.has(record))
    )
      return yield* new Invalid({
        field: "assessment",
        reason: "semantic assessments must retain examined evidence and scope",
      });
    if (Secrets.scan(assessment.explanation).length > 0)
      return yield* new Invalid({
        field: "assessment",
        reason: "semantic assessment contains a possible secret",
      });
  }
});

export const render = (
  draft: Draft,
  citations: ReadonlyArray<string>,
  reportPath: string,
  outcomes: ReadonlyArray<Outcomes.Report>,
): string => {
  const lines = [
    "---",
    `type: ${JSON.stringify(draft.type)}`,
    `title: ${JSON.stringify(draft.title)}`,
    `description: ${JSON.stringify(draft.description)}`,
    "status: draft",
    "gitplus:",
    `  cites: ${JSON.stringify(citations)}`,
    "---",
    "",
    draft.body,
    "",
    "## Examined evidence",
    "",
    `Report: ${reportPath}`,
    `Scope: ${draft.scope.join(", ")}`,
    `Claim: ${draft.claim}`,
  ];
  for (const key of [...new Set(draft.outcomes)].sort()) {
    const outcome = outcomes.find((entry) => entry.key === key);
    if (outcome === undefined) continue;
    lines.push(
      `PR: ${outcome.pr} (${outcome.key})`,
      `Accepted head: ${outcome.acceptedHead ?? "unknown"}`,
      `Merge commit: ${outcome.mergeCommit ?? "unknown"}`,
    );
    for (const source of outcome.sources)
      lines.push(`Original head: ${source.originalHead}; session output: ${source.record}`);
    for (const record of outcome.records)
      lines.push(
        `Native record: ${record.commit} (${record.type}; actor ${record.actorKind}; review ${record.reviewState}; decision ${record.reviewDecision ?? "not-applicable"})`,
      );
  }
  for (const outcome of outcomes) {
    for (const contradiction of outcome.contradictions)
      lines.push(`Contradictory examined outcome ${outcome.pr}: ${contradiction}`);
  }
  lines.push(
    ...draft.evidence.map((record) => `Evidence: ${record}`),
    "",
    "This draft is a scoped interpretation. Its session citation authenticates the recorder, without transitively verifying the underlying PR or semantic claims.",
    "",
  );
  return lines.join("\n");
};

/** Stable candidate identity excludes clocks, selection history, job IDs and destination expectations. */
export const identity = Effect.fn("knowledge.Proposal.identity")(function* (
  evidence: Evidence,
  output: Output,
) {
  return (yield* Input.digest({
    source: evidence.manifest.view,
    sourceHead: evidence.manifest.sourceHead,
    genesis: evidence.manifest.genesis,
    trust: evidence.manifest.trustHead,
    policy: evidence.manifest.policyHead,
    output,
    outcomes: evidence.outcomes
      .filter((entry) => entry.eligible)
      .map((entry) => ({
        key: entry.key,
        head: entry.evidenceHead,
        accepted: entry.acceptedHead,
        merge: entry.mergeCommit,
        sources: entry.sources,
        records: entry.records,
      })),
  })).slice("sha256:".length);
});
