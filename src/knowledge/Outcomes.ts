/** Native outcomes are accepted hub evidence joined to actual source history. */
import { compareUtf8 } from "../text.ts";
import { Effect, Schema } from "effect";
import { Invalid } from "../git/Error.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Projection from "../hub/Projection.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Trust from "../trust/Projection.ts";
import * as Verify from "../trust/Verify.ts";
import * as Inputs from "./Inputs.ts";
import * as Sources from "./OutcomeSources.ts";
import * as Paths from "./OutcomePaths.ts";

export const Actor = Schema.Struct({
  fingerprint: Schema.String,
  kind: Schema.Literals(["human", "bot", "unknown"]),
});
export type Actor = typeof Actor.Type;
export const Record = Schema.Struct({
  commit: Schema.String,
  event: Schema.String,
  type: Schema.String,
  actor: Schema.NullOr(Schema.String),
  actorKind: Schema.Literals(["human", "bot", "unknown"]),
  text: Schema.NullOr(Schema.String),
  head: Schema.NullOr(Schema.String),
  relatedEvent: Schema.NullOr(Schema.String),
  reviewState: Schema.Literals(["not-applicable", "current", "stale", "dismissed"]),
  reviewDecision: Schema.NullOr(Schema.Literals(["approve", "reject", "comment"])),
});
export const Report = Schema.Struct({
  version: Schema.Literal(1),
  key: Schema.String,
  evaluationKey: Schema.String,
  repository: Schema.NullOr(Schema.String),
  pr: Schema.String,
  ref: Schema.String,
  evidenceHead: Schema.String,
  disposition: Schema.Literals(["open", "closed", "merged", "unavailable"]),
  linkage: Schema.Literals(["accepted", "insufficient", "not-applicable"]),
  eligible: Schema.Boolean,
  baseRef: Schema.NullOr(Schema.String),
  baseHead: Schema.NullOr(Schema.String),
  acceptedHead: Schema.NullOr(Schema.String),
  mergeCommit: Schema.NullOr(Schema.String),
  sources: Schema.Array(Sources.Link),
  records: Schema.Array(Record),
  paths: Schema.Array(Schema.String),
  contradictions: Schema.Array(Schema.String),
  diagnostics: Schema.Array(Schema.String),
  exclusions: Schema.Array(Schema.Struct({ record: Schema.String, reason: Schema.String })),
  coverage: Schema.Struct({
    complete: Schema.Boolean,
    omittedSessions: Schema.Int,
    omittedRecords: Schema.Int,
  }),
  independentExamples: Schema.Int,
  semantic: Schema.Struct({ state: Schema.Literal("not-assessed"), scope: Schema.String }),
});
export type Report = typeof Report.Type;

const oidOf = (named: string | null): Oid | null => (named === null ? null : unqualify(named));
export const evaluate = Effect.fn("knowledge.Outcomes.evaluate")(function* (input: {
  readonly manifest: Input.InputManifest;
  readonly ref: string;
  readonly actors: ReadonlyArray<Actor>;
}) {
  const pr = Event.prOf(input.ref);
  const evidenceHead = input.manifest.outcomeHeads.find(({ ref }) => ref === input.ref)?.head;
  if (pr === null || evidenceHead === undefined)
    return yield* new Invalid({
      field: "outcome",
      reason: "outcome is absent from the captured input manifest",
    });
  const key = yield* Input.digest({ repository: input.manifest.repository, pr });
  const evaluationKey = yield* Input.digest({
    key,
    evidenceHead,
    sessions: input.manifest.sessionHeads,
    actors: input.actors,
    source: input.manifest.sourceHead,
    trust: input.manifest.trustHead,
    policy: input.manifest.policyHead,
    configuration: input.manifest.configurationDigest,
    evaluatedAt: input.manifest.evaluatedAt,
    version: "knowledge-outcomes/1",
  });
  const initial: Report = {
    version: 1,
    key,
    evaluationKey,
    repository: input.manifest.repository,
    pr,
    ref: input.ref,
    evidenceHead,
    disposition: "unavailable",
    linkage: "insufficient",
    eligible: false,
    baseRef: null,
    baseHead: null,
    acceptedHead: null,
    mergeCommit: null,
    sources: [],
    records: [],
    paths: [],
    contradictions: [],
    diagnostics: [],
    exclusions: [],
    coverage: { complete: true, omittedSessions: 0, omittedRecords: 0 },
    independentExamples: 0,
    semantic: {
      state: "not-assessed",
      scope: "native PR disposition and explicitly linked source history only",
    },
  };
  const reader = yield* Inputs.reader(input.manifest);
  const result = yield* Effect.gen(function* () {
    const repository = yield* Repository;
    const stored = yield* Genesis.readGenesis();
    if (stored === null || stored.genesis.repoId !== input.manifest.repository)
      return {
        ...initial,
        diagnostics: ["repository identity unavailable"],
        coverage: { ...initial.coverage, complete: false },
      };
    const trust = yield* Trust.project(stored.genesis);
    const projection = yield* Projection.project(
      stored.genesis,
      trust,
      pr,
      new Date(input.manifest.evaluatedAt),
    );
    const accepted = new Set([...projection.claims.values()].flat());
    const walked = yield* Event.entries(pr);
    const entries = walked.events.filter(
      (entry) =>
        accepted.has(entry.commit) &&
        !projection.redacted.has(entry.commit) &&
        entry.payload !== null,
    );
    const records: Array<typeof Record.Type> = [];
    const actors = new Map(input.actors.map((actor) => [actor.fingerprint, actor.kind]));
    for (const entry of entries.slice(0, 128)) {
      const payload = entry.payload;
      if (payload === null) continue;
      const signers = yield* Verify.signers(entry.bytes, entry.signatures);
      const review = projection.reviews.find((review) => review.commit === entry.commit);
      const actor = review?.author ?? (signers.length === 1 ? signers[0]! : null);
      records.push({
        commit: qualify(entry.commit),
        event: payload.id,
        type: payload.type,
        actor,
        actorKind: actor === null ? "unknown" : (actors.get(actor) ?? "unknown"),
        text: "body" in payload ? payload.body : "reason" in payload ? payload.reason : null,
        head: "head" in payload ? payload.head : null,
        relatedEvent:
          "review" in payload ? payload.review : "thread" in payload ? payload.thread : null,
        reviewState:
          review === undefined
            ? "not-applicable"
            : review.dismissed
              ? "dismissed"
              : review.stale
                ? "stale"
                : "current",
        reviewDecision: payload.type === "review.submitted" ? payload.decision : null,
      });
    }
    const revisions = new Set<Oid>();
    for (const entry of entries) {
      if (entry.payload?.type !== "pr.opened" && entry.payload?.type !== "pr.updated") continue;
      const head = unqualify(entry.payload.head);
      if (head !== null) revisions.add(head);
    }
    const sources = yield* Sources.collect({ manifest: input.manifest, pr, revisions, trust });
    const baseHead = yield* repository.resolve(projection.base);
    const common = {
      ...initial,
      disposition: projection.state,
      baseRef: projection.base || null,
      baseHead: baseHead === null ? null : qualify(baseHead),
      records,
      sources: sources.links,
      exclusions: sources.exclusions,
      coverage: {
        complete: sources.complete && entries.length <= 128,
        omittedSessions: sources.omittedSessions,
        omittedRecords: Math.max(0, entries.length - 128),
      },
    };
    if (projection.state !== "merged")
      return {
        ...common,
        linkage: "not-applicable",
        diagnostics: [
          projection.state === "closed"
            ? "closure does not establish a negative preference"
            : "no settled PR disposition",
        ],
      } satisfies Report;
    const merge = entries
      .filter(
        (entry) =>
          entry.payload?.type === "pr.merged" &&
          oidOf(entry.payload.mergeCommit) === projection.mergeCommit,
      )
      .at(-1);
    const finalHead = merge?.payload?.type === "pr.merged" ? unqualify(merge.payload.head) : null;
    const landed = projection.mergeCommit;
    const identities = {
      acceptedHead: finalHead === null ? null : qualify(finalHead),
      mergeCommit: landed === null ? null : qualify(landed),
    };
    if (
      finalHead === null ||
      landed === null ||
      baseHead === null ||
      !(yield* repository.isAncestor(finalHead, landed)) ||
      !(yield* repository.isAncestor(landed, baseHead))
    )
      return {
        ...common,
        ...identities,
        diagnostics: ["the accepted merge assertion is not linked to the captured base history"],
      } satisfies Report;
    if (sources.links.length === 0)
      return {
        ...common,
        ...identities,
        diagnostics: ["no accepted agent output links this PR to an original proposed head"],
      } satisfies Report;
    const paths = new Set<string>();
    const contradictions: Array<string> = [];
    let complete = common.coverage.complete;
    for (const link of sources.links) {
      const original = unqualify(link.originalHead);
      if (original === null || !(yield* repository.isAncestor(original, finalHead))) {
        contradictions.push(
          `original ${link.originalHead} cannot be correlated to accepted ${qualify(finalHead)}`,
        );
        continue;
      }
      const commit = yield* repository.readCommit(original);
      const parent = commit.parents[0];
      const prior = parent === undefined ? null : (yield* repository.readCommit(parent)).tree;
      const originalPaths = yield* Paths.changed(prior, commit.tree);
      const corrections = yield* Paths.changed(
        commit.tree,
        (yield* repository.readCommit(finalHead)).tree,
      );
      for (const name of [...originalPaths.paths, ...corrections.paths]) paths.add(name);
      complete &&= originalPaths.complete && corrections.complete;
    }
    const eligible = contradictions.length === 0 && complete && paths.size > 0;
    return {
      ...common,
      ...identities,
      linkage: eligible ? "accepted" : "insufficient",
      eligible,
      independentExamples: eligible ? 1 : 0,
      paths: [...paths].sort(compareUtf8),
      contradictions,
      coverage: { ...common.coverage, complete },
      diagnostics: eligible ? [] : ["source linkage or bounded path coverage is insufficient"],
    } satisfies Report;
  }).pipe(
    Effect.provideService(Repository, reader),
    Effect.catchTag("ObjectNotFound", () =>
      Effect.succeed({
        ...initial,
        diagnostics: ["required outcome evidence is unavailable on this replica"],
        coverage: { ...initial.coverage, complete: false },
      }),
    ),
  );
  return result;
});
