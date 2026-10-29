/** Assisted results are proposed drafts, never knowledge authority. */
import { Schema } from "effect";
import * as Proposal from "./Proposal.ts";

export const State = Schema.Struct({
  version: Schema.Literal(1),
  state: Schema.Literals(["not-configured", "pending", "complete", "partial", "dry-run"]),
  runner: Schema.NullOr(Proposal.Label),
  fingerprint: Schema.NullOr(Schema.String),
  output: Schema.NullOr(Proposal.Output),
  diagnostic: Schema.NullOr(Schema.String),
});
export type State = typeof State.Type;
