/**
 * The Task / Change Request domain, as the product spec defines it.
 *
 * > A **Task** is the canonical unit of trackable work. Tasks may be nested,
 * > assigned, labeled, discussed, prioritized, and related to other Tasks.
 * >
 * > A **Change Request** is a specialized Task representing a proposed change
 * > to repository content. […] Change Requests are not a parallel entity type;
 * > they are a specialization of Task.
 *
 * That inheritance is why a Change Request is a Task with more fields here
 * rather than the two sitting side by side in a union: everything that reads a
 * Task reads a Change Request unchanged, and the hierarchy can mix them freely.
 *
 * Every type below is a Schema, and the TypeScript type is read back off it.
 * The Foldkit Model holds these values, and a Foldkit Model is declared as a
 * Schema — so writing the interface and a matching Schema separately would be
 * two statements of one shape, free to drift. One statement, read twice.
 *
 * Nothing in this module talks to the server. The repository's git-native hub
 * (`src/hub/PullRequest.ts`, `src/hub/Projection.ts`) models pull requests as
 * signed events in `refs/hub/*`, but that layer has no HTTP surface yet — the
 * JSON API in `src/server/Api.ts` exposes only git itself. So Tasks come from
 * `fixtures.ts` for now, shaped so that swapping in a projection later is a
 * change to one module.
 */

import { Schema } from "effect";

export const Kind = Schema.Literals(["Task", "CR"]);
export type Kind = typeof Kind.Type;

export const Status = Schema.Literals([
  "Todo",
  "In progress",
  "In review",
  "Checks failing",
  "Done",
  "Open",
  "Merged",
]);
export type Status = typeof Status.Type;

export const Person = Schema.Struct({
  name: Schema.String,
  avatar: Schema.String,
});
export type Person = typeof Person.Type;

/** A CSS custom-property name from `tokens.css`, not a literal colour. */
export const LabelHue = Schema.Literals(["accent", "blue", "purple", "red", "amber", "orange"]);
export type LabelHue = typeof LabelHue.Type;

export const Label = Schema.Struct({
  name: Schema.String,
  hue: LabelHue,
});
export type Label = typeof Label.Type;

export const Comment = Schema.Struct({
  avatar: Schema.String,
  author: Schema.String,
  when: Schema.String,
  text: Schema.String,
});
export type Comment = typeof Comment.Type;

export const Commit = Schema.Struct({
  sha: Schema.String,
  msg: Schema.String,
  when: Schema.String,
});
export type Commit = typeof Commit.Type;

export const Check = Schema.Struct({
  name: Schema.String,
  detail: Schema.String,
  ok: Schema.Boolean,
});
export type Check = typeof Check.Type;

export const Review = Schema.Struct({
  headline: Schema.String,
  detail: Schema.String,
  ok: Schema.Boolean,
  action: Schema.String,
  merged: Schema.optional(Schema.Boolean),
});
export type Review = typeof Review.Type;

/** A single line of the fixture diff: line number, text, and which side. */
export const DiffLine = Schema.Struct({
  n: Schema.Finite,
  text: Schema.String,
  kind: Schema.Literals(["add", "del", "context"]),
});
export type DiffLine = typeof DiffLine.Type;

/** One review thread on a hub Change Request, with its conversation. */
export const Thread = Schema.Struct({
  id: Schema.String,
  path: Schema.NullOr(Schema.String),
  resolved: Schema.Boolean,
  comments: Schema.Array(Comment),
});
export type Thread = typeof Thread.Type;

/** One agent session, as the hub projects it — provenance, not planning. */
export const SessionRow = Schema.Struct({
  id: Schema.String,
  agent: Schema.String,
  refs: Schema.Array(Schema.String),
  pulls: Schema.Array(Schema.String),
  commits: Schema.Finite,
  openDecisions: Schema.Finite,
  tokens: Schema.Finite,
});
export type SessionRow = typeof SessionRow.Type;

/**
 * The members every Task has, Change Requests included.
 *
 * The Change Request members are optional here rather than absent, because a
 * Task and a Change Request share one list and one row renderer: a reader that
 * had to narrow before touching `title` would be narrowing on every line.
 * `isChangeRequest` is what narrows, once, where the extra members are used.
 */
export const Task = Schema.Struct({
  id: Schema.String,
  kind: Kind,
  title: Schema.String,
  status: Status,
  avatar: Schema.String,
  desc: Schema.String,
  assignees: Schema.Array(Person),
  labels: Schema.Array(Label),
  comments: Schema.Array(Comment),
  updated: Schema.String,
  parent: Schema.optional(Schema.String),
  children: Schema.optional(Schema.Array(Schema.String)),
  /** Set when this row is the hub's projection rather than a fixture. */
  hub: Schema.optional(Schema.Boolean),
  /** Hub Change Requests carry their review threads once hydrated. */
  threads: Schema.optional(Schema.Array(Thread)),
  /** The proposed revision a hub review approves — the head oid. */
  reviewHead: Schema.optional(Schema.String),

  // What a Change Request adds: source ref, target ref, diff, commits,
  // reviews and approvals, automated checks, and mergeability state — exactly
  // the list the spec gives. `isChangeRequest` is what makes them non-optional.
  sourceRef: Schema.optional(Schema.String),
  targetRef: Schema.optional(Schema.String),
  diffStat: Schema.optional(Schema.String),
  commitCount: Schema.optional(Schema.String),
  diffFile: Schema.optional(Schema.String),
  commits: Schema.optional(Schema.Array(Commit)),
  checks: Schema.optional(Schema.Array(Check)),
  review: Schema.optional(Review),
  diff: Schema.optional(Schema.Array(DiffLine)),
});
export type Task = typeof Task.Type;

/** A Task with a proposed repository change attached. */
export type ChangeRequest = Task &
  Readonly<{
    kind: "CR";
    sourceRef: string;
    targetRef: string;
    diffStat: string;
    commitCount: string;
    diffFile: string;
    commits: readonly Commit[];
    checks: readonly Check[];
    review: Review;
    diff: readonly DiffLine[];
  }>;

/**
 * Whether a Task carries a proposed change.
 *
 * A predicate rather than a `kind === "CR"` test at each call site, so the
 * narrowing and the rule live in one place.
 */
export const isChangeRequest = (task: Task): task is ChangeRequest =>
  task.kind === "CR" && task.review !== undefined;

/**
 * Which token a status paints itself with.
 *
 * The design gives Merged and In review the same purple, and Open and Done the
 * same green — the pairs differ in meaning, not in hue.
 */
export const statusToken = (status: Status): string => {
  switch (status) {
    case "Done":
    case "Open":
      return "accent";
    case "In progress":
      return "blue";
    case "In review":
    case "Merged":
      return "purple";
    case "Checks failing":
      return "red";
    case "Todo":
      return "todo";
  }
};

/**
 * A status is "filled" when the work behind it is finished.
 *
 * Borrowed from Linear during the design conversation: every row carries a
 * status ring, outlined while open and solid once it has landed.
 */
export const isTerminal = (status: Status): boolean => status === "Done" || status === "Merged";

/**
 * The ring colour follows the pill colour except for Todo, which the design
 * paints a shade lighter so a whole list of untouched work stays quiet.
 */
export const ringToken = (status: Status): string =>
  status === "Todo" ? "todo-ring" : statusToken(status);
