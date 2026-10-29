/** Durable publication state. Every identity needed to reconcile an append is retained here. */
import { Schema } from "effect";
import * as Capture from "../hub/SessionCapture.ts";
import * as Proposal from "./Proposal.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Outcomes from "./Outcomes.ts";

export const State = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  ref: Schema.String,
  expectedHead: Schema.NullOr(Schema.String),
  session: Schema.String,
  createdAt: Schema.String,
  evidence: Proposal.Evidence,
  output: Proposal.Output,
  runner: Proposal.Label,
  roots: Schema.Array(Schema.String),
  reportPath: Schema.String,
  reportText: Schema.String,
  phase: Schema.Literals([
    "prepared",
    "session-opened",
    "report-written",
    "report-attested",
    "report-published",
    "concept-written",
    "concept-attested",
    "complete",
  ]),
  opening: Schema.NullOr(Capture.Prepared),
  reportCommit: Schema.NullOr(Schema.String),
  reportOutput: Schema.NullOr(Capture.Prepared),
  conceptCommit: Schema.NullOr(Schema.String),
  conceptOutput: Schema.NullOr(Capture.Prepared),
});
export type State = typeof State.Type;
export const Header = Schema.Struct({
  version: Schema.Literal(1),
  proposalId: Schema.String,
  draftDigest: Schema.String,
  sourceHead: Schema.String,
  sourceTree: Schema.String,
  maintenanceSession: Schema.String,
});

export const Report = Schema.Struct({
  ...Header.fields,
  manifest: Input.InputManifest,
  runner: Proposal.Label,
  checks: Schema.Array(Schema.Json),
  outcomes: Schema.Array(Outcomes.Report),
  assessments: Schema.Array(Proposal.Assessment),
  independentOutcomes: Schema.Array(Schema.String),
  interpretation: Schema.String,
});
