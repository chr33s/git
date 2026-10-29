/** Trusted local publication. Models cannot choose credentials, ref updates or executable commands. */
import { DateTime, Effect, Schema } from "effect";
import { fingerprint, type PrivateKey } from "../crypto/SshSignature.ts";
import { Invalid, type StorageFailure } from "../git/Error.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Capture from "../hub/SessionCapture.ts";
import * as Session from "../hub/Session.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Trust from "../trust/Projection.ts";
import * as Verify from "../trust/Verify.ts";
import * as Policy from "../server/Policy.ts";
import * as Inputs from "./Inputs.ts";
import * as Outcomes from "./Outcomes.ts";
import * as Proposal from "./Proposal.ts";
import * as Provenance from "./Provenance.ts";
import * as Concept from "./Concept.ts";
import * as Maintenance from "./Maintenance.ts";
import { isTree } from "../git/Format.ts";
import * as Publication from "./Publication.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const checked = <S extends Schema.Top>(schema: S, text: string, field: string) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError(
      () => new Invalid({ field, reason: "invalid or unsupported retained evidence" }),
    ),
  );

const reportTextOf = Effect.fn("knowledge.Publisher.reportTextOf")(function* (input: {
  readonly id: string;
  readonly session: string;
  readonly evidence: Proposal.Evidence;
  readonly output: Proposal.Output;
  readonly runner: Proposal.Label;
}) {
  const report = {
    version: 1,
    proposalId: input.id,
    draftDigest: yield* Input.digest(input.output.drafts),
    sourceHead: input.evidence.manifest.sourceHead,
    sourceTree: input.evidence.manifest.view?.tree ?? "",
    maintenanceSession: input.session,
    manifest: input.evidence.manifest,
    runner: input.runner,
    checks: input.evidence.concepts,
    outcomes: input.evidence.outcomes,
    assessments: input.output.assessments,
    independentOutcomes: [
      ...new Set(input.output.drafts.flatMap((draft) => draft.outcomes)),
    ].sort(),
    interpretation:
      "Scoped draft interpretation; session citations authenticate the recorder without transitive verification of PR or semantic claims.",
  };
  return Input.canonical(report) + "\n";
});

export const prepare = Effect.fn("knowledge.Publisher.prepare")(function* (input: {
  readonly evidence: Proposal.Evidence;
  readonly output: Proposal.Output;
  readonly runner: Proposal.Label;
  readonly roots: ReadonlyArray<string>;
  readonly maximumBytes: number;
}) {
  yield* Proposal.validate({ ...input });
  if (input.output.drafts.length === 0) return null;
  const source = input.evidence.manifest.sourceHead;
  const view = input.evidence.manifest.view;
  if (source === null || view === null || view.base !== source)
    return yield* new Invalid({
      field: "proposal.source",
      reason: "proposal publication requires a committed source view",
    });
  const id = yield* Proposal.identity(input.evidence, input.output);
  const ref = `refs/heads/agent-memory/${id}`;
  const session = `memory-${id}`;
  const reportPath = `.gitplus/knowledge/reports/${id}.json`;
  const createdAt = DateTime.formatIso(yield* DateTime.now);
  const reportText = yield* reportTextOf({ id, session, ...input });
  if (encoder.encode(reportText).length > input.maximumBytes)
    return yield* new Invalid({
      field: "proposal.report",
      reason: "retained evidence report exceeds its configured byte ceiling",
    });
  return {
    version: 1,
    id,
    ref,
    session,
    createdAt,
    reportPath,
    reportText,
    expectedHead:
      input.evidence.manifest.proposalHeads.find((head) => head.ref === ref)?.head ?? null,
    evidence: input.evidence,
    output: input.output,
    runner: input.runner,
    roots: input.roots,
    phase: "prepared",
    opening: null,
    reportCommit: null,
    reportOutput: null,
    conceptCommit: null,
    conceptOutput: null,
  } satisfies Publication.State;
});

const authority = Effect.fn("knowledge.Publisher.authority")(function* (key: PrivateKey) {
  const stored = yield* Genesis.readGenesis();
  if (stored === null)
    return yield* new Invalid({
      field: "proposal",
      reason: "proposal publication requires a repository genesis",
    });
  const trust = yield* Trust.project(stored.genesis);
  const signer = yield* fingerprint(key.publicKey);
  const authorized = yield* Verify.authorizeKey({
    projection: trust,
    signer,
    capability: "source.push",
  });
  if (!authorized.ok)
    return yield* new Invalid({ field: "proposal.key", reason: authorized.reason });
  const session = yield* Verify.authorizeKey({
    projection: trust,
    signer,
    capability: "hub.session",
  });
  if (!session.ok) return yield* new Invalid({ field: "proposal.key", reason: session.reason });
  return {
    genesis: stored.genesis,
    trust,
    principal: { member: authorized.principal, capabilities: authorized.principal.capabilities },
  };
});

const citation = Effect.fn("knowledge.Publisher.citation")(function* (
  record: string,
  repo: string,
  trust: Trust.Projection,
  target?: string,
) {
  const repository = yield* Repository;
  const accepted = yield* Provenance.evaluate(
    record,
    Provenance.judge(
      repo,
      trust,
      new Map(yield* repository.refs),
      DateTime.toDateUtc(yield* DateTime.now),
    ),
  );
  if (accepted.state !== "accepted") return false;
  const oid = unqualify(record);
  if (oid === null) return false;
  const entry = (yield* Session.entries(accepted.session ?? "")).events.find(
    (event) => event.commit === oid,
  );
  return (
    entry?.payload.type === "session.produced" &&
    entry.payload.note === null &&
    (target === undefined || entry.payload.commits.includes(target))
  );
});

const current = Effect.fn("knowledge.Publisher.current")(function* (state: Publication.State) {
  const manifest = state.evidence.manifest;
  if (!(yield* Inputs.current(manifest, Session.refOf(state.session)))) return false;
  const now = DateTime.formatIso(yield* DateTime.now);
  // Re-evaluate eligibility at consumption, even when refs and unsigned fingerprints match.
  for (const original of state.evidence.outcomes) {
    const captured = yield* Outcomes.evaluate({
      manifest,
      ref: original.ref,
      actors: state.evidence.actors,
    });
    if (Input.canonical(captured) !== Input.canonical(original)) return false;
    const latest = yield* Outcomes.evaluate({
      manifest: { ...manifest, evaluatedAt: now },
      ref: original.ref,
      actors: state.evidence.actors,
    });
    if (
      Input.canonical({ ...original, evaluationKey: null }) !==
      Input.canonical({ ...latest, evaluationKey: null })
    )
      return false;
  }
  const Timed = Schema.Struct({
    evidence: Schema.Struct({
      concepts: Schema.Array(
        Schema.Struct({
          temporal: Schema.Struct({
            state: Schema.String,
            deadline: Schema.optionalKey(Schema.String),
          }),
          citations: Schema.Array(Schema.Struct({ record: Schema.String, state: Schema.String })),
        }),
      ),
    }),
  });
  const stored = yield* Genesis.readGenesis();
  if (stored === null) return false;
  const trust = yield* Trust.project(stored.genesis);
  const repository = yield* Repository;
  const judge = Provenance.judge(
    manifest.repository,
    trust,
    new Map(yield* repository.refs),
    new Date(now),
  );
  for (const raw of state.evidence.concepts) {
    const recorded = yield* Schema.decodeUnknownEffect(
      Schema.Struct({ selection: Maintenance.Selection, evidence: Schema.Json }),
    )(raw).pipe(
      Effect.mapError(
        () => new Invalid({ field: "proposal", reason: "invalid captured deterministic report" }),
      ),
    );
    const rechecked = yield* Maintenance.check(manifest, recorded.selection);
    const normalized = yield* checked(Schema.Json, JSON.stringify(rechecked), "proposal.evidence");
    if (Input.canonical(normalized) !== Input.canonical(raw)) return false;
    const result = yield* Schema.decodeUnknownEffect(Timed)(raw).pipe(
      Effect.mapError(
        () => new Invalid({ field: "proposal", reason: "invalid deterministic check evidence" }),
      ),
    );
    for (const concept of result.evidence.concepts) {
      if (
        concept.temporal.state === "within-deadline" &&
        concept.temporal.deadline !== undefined &&
        Date.parse(concept.temporal.deadline) <= Date.parse(now)
      )
        return false;
      for (const source of concept.citations.filter((entry) => entry.state === "accepted"))
        if ((yield* Provenance.evaluate(source.record, judge)).state !== "accepted") return false;
    }
  }
  return true;
});

const documents = Effect.fn("knowledge.Publisher.documents")(function* (
  state: Publication.State,
  record: string,
  trust: Trust.Projection,
) {
  const repo = state.evidence.manifest.repository;
  if (repo === null || !(yield* citation(record, repo, trust)))
    return yield* new Invalid({
      field: "proposal.citation",
      reason: "generated Concepts require an accepted null-note session report output",
    });
  return yield* Effect.forEach(state.output.drafts, (draft) =>
    Effect.gen(function* () {
      const text = Proposal.render(draft, [record], state.reportPath, state.evidence.outcomes);
      const parsed = Concept.parse({
        bundle: Concept.BUNDLE,
        path: draft.path,
        bytes: encoder.encode(text),
      });
      if (!parsed.ok || parsed.concept.status !== "draft" || !parsed.concept.cites.includes(record))
        return yield* new Invalid({
          field: "proposal.concept",
          reason: "generated Concept lacks explicit draft status or its accepted session citation",
        });
      return { path: draft.path, mode: "100644", content: encoder.encode(text) };
    }),
  );
});

const sourceCommit = Effect.fn("knowledge.Publisher.sourceCommit")(function* (
  state: Publication.State,
  phase: "report" | "concept",
  report: Oid | null,
  citationRecord: string | null,
  trust: Trust.Projection,
) {
  const repository = yield* Repository;
  const source = unqualify(state.evidence.manifest.sourceHead ?? "");
  if (source === null)
    return yield* new Invalid({
      field: "proposal.source",
      reason: "invalid captured source commit",
    });
  const base = phase === "report" ? source : report;
  if (base === null)
    return yield* new Invalid({ field: "proposal", reason: "report source is not prepared" });
  const baseTree = (yield* repository.readCommit(base)).tree;
  const changes =
    phase === "report"
      ? [{ path: state.reportPath, mode: "100644", content: encoder.encode(state.reportText) }]
      : citationRecord === null
        ? []
        : yield* documents(state, citationRecord, trust);
  if (changes.length === 0)
    return yield* new Invalid({ field: "proposal", reason: "no validated draft documents" });
  const tree = yield* repository.writeFiles({ base: baseTree, changes });
  return yield* repository.commitTree({
    tree,
    parents: [base],
    message: `memory ${phase === "report" ? "evidence" : "concepts"} ${state.id}\n\nSession: ${state.session}\n`,
    author: {
      name: "Agent memory curator",
      email: "agent-memory@localhost",
      at: new Date(state.createdAt),
      offset: 0,
    },
  });
});

const allowedReport = Effect.fn("knowledge.Publisher.allowedReport")(function* (
  state: Publication.State,
) {
  const repository = yield* Repository;
  if (
    !/^[0-9a-f]{64}$/u.test(state.id) ||
    state.ref !== `refs/heads/agent-memory/${state.id}` ||
    state.session !== `memory-${state.id}` ||
    state.reportPath !== `.gitplus/knowledge/reports/${state.id}.json` ||
    state.id !== (yield* Proposal.identity(state.evidence, state.output))
  )
    return yield* new Invalid({
      field: "proposal.identity",
      reason: "proposal identity, report path or destination is not host-derived",
    });
  if (Concept.instantOf(state.createdAt).state !== "instant")
    return yield* new Invalid({
      field: "proposal.createdAt",
      reason: "publication journal has an invalid prepared instant",
    });
  const expected =
    state.evidence.manifest.proposalHeads.find((entry) => entry.ref === state.ref)?.head ?? null;
  if (expected !== state.expectedHead || (expected !== null && unqualify(expected) === null))
    return yield* new Invalid({
      field: "proposal.expected",
      reason: "proposal destination expectation differs from its captured manifest",
    });
  const tree = unqualify(state.evidence.manifest.view?.tree ?? "");
  if (tree === null)
    return yield* new Invalid({ field: "proposal.view", reason: "invalid source view" });
  const parts = state.reportPath.split("/");
  for (let index = 1; index <= parts.length; index += 1) {
    const entry = yield* repository.findPath(tree, parts.slice(0, index).join("/"));
    if (entry !== null && (index < parts.length ? !isTree(entry.mode) : entry.mode !== "100644"))
      return yield* new Invalid({
        field: "proposal.report",
        reason: "report path traverses a symlink, executable or non-directory",
      });
  }
  if (state.reportText !== (yield* reportTextOf(state)))
    return yield* new Invalid({
      field: "proposal.report",
      reason: "retained report bytes are not the host-derived report",
    });
  const header = yield* checked(Publication.Header, state.reportText, "proposal.report");
  if (
    header.proposalId !== state.id ||
    header.maintenanceSession !== state.session ||
    header.sourceHead !== state.evidence.manifest.sourceHead ||
    header.sourceTree !== state.evidence.manifest.view?.tree ||
    header.draftDigest !== (yield* Input.digest(state.output.drafts))
  )
    return yield* new Invalid({
      field: "proposal.report",
      reason: "retained report does not bind this proposal and source view",
    });
});

const equivalent = Effect.fn("knowledge.Publisher.equivalent")(function* (
  state: Publication.State,
  head: Oid,
  trust: Trust.Projection,
  maximumBytes: number,
) {
  const repository = yield* Repository;
  const final = yield* repository.readCommit(head);
  if (
    final.message !== `memory concepts ${state.id}\n\nSession: ${state.session}\n` ||
    final.parents.length !== 1
  )
    return false;
  const report = final.parents[0]!;
  const reportCommit = yield* repository.readCommit(report);
  const entry = yield* repository.findPath(final.tree, state.reportPath);
  if (entry === null || entry.mode !== "100644") return false;
  const reportText = decoder.decode(yield* repository.readBlob(entry.oid));
  if (encoder.encode(reportText).length > maximumBytes) return false;
  const raw = yield* checked(Schema.Json, reportText, "proposal.report");
  const header = yield* checked(Publication.Report, reportText, "proposal.report");
  const retained: Publication.State = {
    ...state,
    evidence: {
      version: 1,
      manifest: header.manifest,
      actors: state.evidence.actors,
      concepts: header.checks,
      outcomes: header.outcomes,
    },
    runner: header.runner,
  };
  const expected = yield* checked(Schema.Json, yield* reportTextOf(retained), "proposal.report");
  if (Input.canonical(raw) !== Input.canonical(expected) || !(yield* current(retained)))
    return false;
  if (
    header.proposalId !== state.id ||
    header.sourceHead !== state.evidence.manifest.sourceHead ||
    header.sourceTree !== state.evidence.manifest.view?.tree ||
    header.draftDigest !== (yield* Input.digest(state.output.drafts)) ||
    header.maintenanceSession !== state.session ||
    reportCommit.parents[0] !== unqualify(header.sourceHead)
  )
    return false;
  const events = (yield* Session.entries(state.session)).events;
  const reportOutput = events.find(
    ({ payload }) =>
      payload.type === "session.produced" &&
      payload.note === null &&
      payload.commits.includes(report),
  );
  const finalOutput = events.find(
    ({ payload }) =>
      payload.type === "session.produced" &&
      payload.note === null &&
      payload.commits.includes(head),
  );
  const repo = state.evidence.manifest.repository;
  if (
    reportOutput === undefined ||
    finalOutput === undefined ||
    repo === null ||
    !(yield* citation(qualify(reportOutput.commit), repo, trust, report)) ||
    !(yield* citation(qualify(finalOutput.commit), repo, trust, head))
  )
    return false;
  for (const document of yield* documents(state, qualify(reportOutput.commit), trust)) {
    const held = yield* repository.findPath(final.tree, document.path);
    if (
      held === null ||
      held.mode !== "100644" ||
      decoder.decode(yield* repository.readBlob(held.oid)) !== decoder.decode(document.content)
    )
      return false;
  }
  // The branch may contain only the retained report and the host-rendered drafts.
  const originalSource = unqualify(header.sourceHead);
  if (originalSource === null) return false;
  const sourceTree = (yield* repository.readCommit(originalSource)).tree;
  const expectedTree = yield* repository.writeFiles({
    base: sourceTree,
    changes: [
      { path: state.reportPath, mode: "100644", content: yield* repository.readBlob(entry.oid) },
      ...(yield* documents(state, qualify(reportOutput.commit), trust)),
    ],
  });
  return expectedTree === final.tree;
});

export type Result =
  | {
      readonly state: "written" | "unchanged";
      readonly id: string;
      readonly ref: string;
      readonly commit: Oid;
    }
  | { readonly state: "conflict" | "superseded-input"; readonly id: string; readonly ref: string };

export const publish = Effect.fn("knowledge.Publisher.publish")(function* (input: {
  readonly state: Publication.State;
  readonly key: PrivateKey;
  readonly roots: ReadonlyArray<string>;
  readonly actors: ReadonlyArray<Outcomes.Actor>;
  readonly maximumBytes: number;
  readonly checkpoint: (state: Publication.State) => Effect.Effect<void, StorageFailure>;
}) {
  let state = input.state;
  if (encoder.encode(state.reportText).length > input.maximumBytes)
    return yield* new Invalid({
      field: "proposal.report",
      reason: "retained report exceeds the current configured byte ceiling",
    });
  const repository = yield* Repository;
  yield* allowedReport(state);
  if (Input.canonical(input.actors) !== Input.canonical(state.evidence.actors))
    return yield* new Invalid({
      field: "proposal.actors",
      reason: "actor classification differs from operator configuration",
    });
  yield* Proposal.validate({
    evidence: state.evidence,
    output: state.output,
    roots: input.roots,
    maximumBytes: input.maximumBytes,
  });
  const outcome = (kind: "conflict" | "superseded-input"): Result => ({
    state: kind,
    id: state.id,
    ref: state.ref,
  });
  if (!(yield* current(state))) return outcome("superseded-input");
  const auth = yield* authority(input.key);
  if (auth.genesis.repoId !== state.evidence.manifest.repository)
    return outcome("superseded-input");
  const existing = yield* repository.readRef(state.ref);
  if (state.phase === "prepared" && existing !== null && state.reportCommit === null) {
    if (yield* equivalent(state, existing, auth.trust, input.maximumBytes))
      return {
        state: "unchanged",
        id: state.id,
        ref: state.ref,
        commit: existing,
      } satisfies Result;
    return outcome("conflict");
  }
  const save = Effect.fn("knowledge.Publisher.checkpoint")(function* (next: Publication.State) {
    yield* input.checkpoint(next);
    state = next;
  });
  if (state.opening === null)
    yield* save({
      ...state,
      opening: yield* Capture.prepareOpen({
        repo: auth.genesis.repoId,
        session: state.session,
        key: input.key,
      }),
    });
  if (state.opening === null)
    return yield* new Invalid({ field: "proposal", reason: "missing prepared session opening" });
  yield* Capture.append(state.opening, { repo: auth.genesis.repoId, session: state.session });
  yield* save({ ...state, phase: "session-opened" });
  // Reconcile accepted output support before recreating any retained source bytes.
  const preparedRecord = state.reportOutput === null ? null : unqualify(state.reportOutput.commit);
  if (state.reportOutput !== null && preparedRecord === null)
    return yield* new Invalid({
      field: "proposal",
      reason: "invalid prepared report output identity",
    });
  if (
    preparedRecord !== null &&
    (yield* Session.entries(state.session)).parents.has(preparedRecord)
  ) {
    if (!(yield* citation(qualify(preparedRecord), auth.genesis.repoId, auth.trust)))
      return outcome("superseded-input");
  }
  const report = yield* sourceCommit(state, "report", null, null, auth.trust);
  if (state.reportCommit !== null && state.reportCommit !== qualify(report))
    return yield* new Invalid({
      field: "proposal",
      reason: "retained report commit identity changed",
    });
  yield* save({ ...state, reportCommit: qualify(report), phase: "report-written" });
  if (state.reportOutput === null)
    yield* save({
      ...state,
      reportOutput: yield* Capture.prepare({
        repo: auth.genesis.repoId,
        session: state.session,
        key: input.key,
        commits: [report],
        refs: [state.ref],
        pulls: [],
        note: null,
        usage: null,
      }),
    });
  if (state.reportOutput === null)
    return yield* new Invalid({ field: "proposal", reason: "missing prepared report output" });
  yield* Capture.append(state.reportOutput, { repo: auth.genesis.repoId, session: state.session });
  if (!(yield* citation(state.reportOutput.commit, auth.genesis.repoId, auth.trust, report)))
    return outcome("superseded-input");
  yield* save({ ...state, phase: "report-attested" });
  const advance = Effect.fn("knowledge.Publisher.advance")(function* (
    to: Oid,
    expected: Oid | null,
  ) {
    if (!(yield* current(state))) return "superseded-input" as const;
    const actual = yield* repository.readRef(state.ref);
    if (actual === to) return "unchanged" as const;
    if (actual !== expected) return "conflict" as const;
    const fresh = yield* authority(input.key);
    const rules = yield* Policy.rulesOf();
    const sessions = new Map(
      (yield* repository.refs).flatMap(([ref, head]) => {
        const id = Session.sessionOf(ref);
        return id === null ? [] : [[id, head] as const];
      }),
    );
    const allowed = yield* Policy.evaluate({
      update: { name: state.ref, value: to, expected },
      genesis: fresh.genesis,
      trust: fresh.trust,
      rules,
      principal: fresh.principal,
      sessions,
    });
    if (!allowed.ok)
      return yield* new Invalid({ field: "proposal.policy", reason: allowed.reason });
    return yield* repository.setRef({ name: state.ref, to, expected }).pipe(
      Effect.as("written" as const),
      Effect.catchTag("RefConflict", () => Effect.succeed("conflict" as const)),
    );
  });
  const held = yield* repository.readRef(state.ref);
  const finalExpected = state.conceptCommit === null ? null : unqualify(state.conceptCommit);
  if (held !== finalExpected || finalExpected === null) {
    const published = yield* advance(
      report,
      state.expectedHead === null ? null : unqualify(state.expectedHead),
    );
    if (published === "conflict" || published === "superseded-input") return outcome(published);
  }
  yield* save({ ...state, phase: "report-published" });
  const concepts = yield* sourceCommit(
    state,
    "concept",
    report,
    state.reportOutput.commit,
    auth.trust,
  );
  if (state.conceptCommit !== null && state.conceptCommit !== qualify(concepts))
    return yield* new Invalid({
      field: "proposal",
      reason: "retained Concept commit identity changed",
    });
  yield* save({ ...state, conceptCommit: qualify(concepts), phase: "concept-written" });
  if (state.conceptOutput === null)
    yield* save({
      ...state,
      conceptOutput: yield* Capture.prepare({
        repo: auth.genesis.repoId,
        session: state.session,
        key: input.key,
        commits: [concepts],
        refs: [state.ref],
        pulls: [],
        note: null,
        usage: null,
      }),
    });
  if (state.conceptOutput === null)
    return yield* new Invalid({ field: "proposal", reason: "missing prepared final output" });
  yield* Capture.append(state.conceptOutput, { repo: auth.genesis.repoId, session: state.session });
  if (!(yield* citation(state.conceptOutput.commit, auth.genesis.repoId, auth.trust, concepts)))
    return outcome("superseded-input");
  yield* save({ ...state, phase: "concept-attested" });
  const result = yield* advance(concepts, report);
  if (result === "conflict" || result === "superseded-input") return outcome(result);
  yield* save({ ...state, phase: "complete" });
  return { state: result, id: state.id, ref: state.ref, commit: concepts } satisfies Result;
});
