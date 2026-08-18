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
 * That inheritance is why `ChangeRequest extends Task` here rather than the two
 * sitting side by side in a union: everything that reads a Task reads a Change
 * Request unchanged, and the hierarchy can mix them freely.
 *
 * Nothing in this module talks to the server. The repository's git-native hub
 * (`src/hub/PullRequest.ts`, `src/hub/Projection.ts`) models pull requests as
 * signed events in `refs/hub/*`, but that layer has no HTTP surface yet — the
 * JSON API in `src/server/Api.ts` exposes only git itself. So Tasks come from
 * `fixtures.ts` for now, shaped so that swapping in a projection later is a
 * change to one module.
 */

export type Kind = "Task" | "CR";

export type Status =
  | "Todo"
  | "In progress"
  | "In review"
  | "Checks failing"
  | "Done"
  | "Open"
  | "Merged";

export interface Person {
  readonly name: string;
  readonly avatar: string;
}

export interface Label {
  readonly name: string;
  /** A CSS custom-property name from `tokens.css`, not a literal colour. */
  readonly hue: LabelHue;
}

export type LabelHue = "accent" | "blue" | "purple" | "red" | "amber" | "orange";

export interface Comment {
  readonly avatar: string;
  readonly author: string;
  readonly when: string;
  readonly text: string;
}

export interface Commit {
  readonly sha: string;
  readonly msg: string;
  readonly when: string;
}

export interface Check {
  readonly name: string;
  readonly detail: string;
  readonly ok: boolean;
}

export interface Review {
  readonly headline: string;
  readonly detail: string;
  readonly ok: boolean;
  readonly action: string;
  readonly merged?: boolean;
}

/** A single line of the fixture diff: line number, text, and which side. */
export interface DiffLine {
  readonly n: number;
  readonly text: string;
  readonly kind: "add" | "del" | "context";
}

export interface Task {
  readonly id: string;
  readonly kind: Kind;
  readonly title: string;
  readonly status: Status;
  readonly avatar: string;
  readonly desc: string;
  readonly assignees: readonly Person[];
  readonly labels: readonly Label[];
  readonly milestone: string;
  readonly comments: readonly Comment[];
  readonly updated: string;
  readonly parent?: string;
  readonly children?: readonly string[];
}

/**
 * A Task with a proposed repository change attached.
 *
 * The extra members are exactly the ones the spec lists as what a Change
 * Request adds: source ref, target ref, diff, commits, reviews and approvals,
 * automated checks, and mergeability state.
 */
export interface ChangeRequest extends Task {
  readonly kind: "CR";
  readonly sourceRef: string;
  readonly targetRef: string;
  readonly diffStat: string;
  readonly commitCount: string;
  readonly diffFile: string;
  readonly commits: readonly Commit[];
  readonly checks: readonly Check[];
  readonly review: Review;
  readonly diff: readonly DiffLine[];
}

/**
 * Whether a Task carries a proposed change.
 *
 * A predicate rather than a `kind === "CR"` test at each call site, so the
 * narrowing and the rule live in one place.
 */
export const isChangeRequest = (task: Task): task is ChangeRequest => task.kind === "CR";

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
