/**
 * The Activity timeline's geometry, as pure functions.
 *
 * Which days a window covers, which column a date lands in, where the "now"
 * marker sits: all of it is arithmetic over a window and a date, so none of it
 * needs to live inside a component. Splitting it out is what lets the view be
 * a function of the Model and lets the awkward cases — a fixture card clipped
 * by the window's edge, two commits on one day — be tested without a browser.
 */
import { daysBetween, startOfDay } from "./time.ts";

/** How many days each zoom level shows. "Week" is the design's fortnight. */
export type Zoom = "day" | "week" | "month";

export const SPANS = { day: 7, week: 14, month: 31 } satisfies Record<Zoom, number>;

/**
 * The day the design's timeline is drawn around, and the fortnight it sits in.
 *
 * `fixtures.ts` places its cards in 1-based columns of that fortnight, which
 * began five days before this date and ran five days after it. Anchoring both
 * to a real date is what lets the fixture window zoom and page: a column is
 * just a day, so any window can say which of its days a card covers.
 */
export const FIXTURE_TODAY = new Date(2026, 7, 18);
export const FIXTURE_LEAD = 5;

/** The day before fixture column 1 — the origin its columns are counted from. */
export const FIXTURE_ORIGIN = new Date(2026, 7, 9);

export interface Day {
  readonly letter: string;
  readonly date: number;
  readonly today: boolean;
}

const LETTERS = ["S", "M", "T", "W", "T", "F", "S"] as const;

/** `at`, moved by whole days, without mutating the argument. */
export const shift = (at: Date, days: number): Date => {
  const moved = new Date(at);
  moved.setDate(moved.getDate() + days);
  return moved;
};

/** Today, or the day the fixture timeline stands in for. */
export const todayOf = (live: boolean, now = new Date()): Date =>
  live ? startOfDay(now) : FIXTURE_TODAY;

/**
 * The date of the window's last column.
 *
 * Live history ends today; the fixture fortnight ran five days past the day it
 * is drawn around, so its window ends there and "now" sits mid-grid — the
 * ninth of fourteen columns the design drew. `offset` pages both back.
 */
export const endOf = (live: boolean, offset: number, now = new Date()): Date =>
  shift(todayOf(live, now), (live ? 0 : FIXTURE_LEAD) - offset);

/** The `span` days the window covers, oldest first. */
export const daysOf = (
  zoom: Zoom,
  live: boolean,
  offset: number,
  now = new Date(),
): readonly Day[] => {
  const span = SPANS[zoom];
  const end = endOf(live, offset, now);
  const today = todayOf(live, now).getTime();
  return Array.from({ length: span }, (_, index) => {
    const at = shift(end, index - (span - 1));
    return {
      letter: LETTERS[at.getDay()] ?? "M",
      date: at.getDate(),
      today: at.getTime() === today,
    };
  });
};

export const monthOf = (zoom: Zoom, live: boolean, offset: number, now = new Date()): string => {
  const end = endOf(live, offset, now);
  const start = shift(end, -(SPANS[zoom] - 1));
  const month = (at: Date): string =>
    at.toLocaleString(undefined, { month: "long", year: "numeric" });
  return start.getMonth() === end.getMonth() && start.getFullYear() === end.getFullYear()
    ? month(end)
    : `${start.toLocaleString(undefined, { month: "long" })} – ${month(end)}`;
};

export const rangeOf = (zoom: Zoom, live: boolean, offset: number, now = new Date()): string => {
  const days = daysOf(zoom, live, offset, now);
  const first = days[0]?.date ?? 1;
  const last = days[days.length - 1]?.date ?? 1;
  return `${String(first)} – ${String(last)}`;
};

/**
 * Where the "now" marker sits, or `null` when that day is not in the window.
 *
 * Mid-column of the day `todayOf` names, which is the last column for a live
 * window ending today and the design's column 9 for the fixture fortnight. A
 * window paged into the past has no "now" to mark.
 */
export const nowOffset = (
  zoom: Zoom,
  live: boolean,
  offset: number,
  now = new Date(),
): string | null => {
  const span = SPANS[zoom];
  const index = span - 1 - daysBetween(todayOf(live, now), endOf(live, offset, now));
  if (index < 0 || index >= span) return null;
  return `calc(100% / ${String(span)} * ${String(index + 0.5)})`;
};

/** One card's placement in the grid: a start column, a width, and a row. */
export interface Placement {
  readonly column: number;
  readonly span: number;
  readonly row: number;
}

/**
 * Where a fixture card sits in this window, or `null` when it misses it.
 *
 * A fixture column is a day of the design's fortnight, so it maps onto the
 * window exactly as a commit's date does. Cards reaching past an edge are
 * clipped to it rather than dropped, which is what keeps the week zoom showing
 * the bars that started before it.
 */
export const placeFixture = (
  zoom: Zoom,
  offset: number,
  event: { readonly column: number; readonly span: number; readonly row: number },
  now = new Date(),
): Placement | null => {
  const span = SPANS[zoom];
  const start = shift(endOf(false, offset, now), -(span - 1));
  const from = Math.max(daysBetween(start, shift(FIXTURE_ORIGIN, event.column)), 0);
  const to = Math.min(
    daysBetween(start, shift(FIXTURE_ORIGIN, event.column + event.span - 1)),
    span - 1,
  );
  if (from > to) return null;
  return { column: from + 1, span: to - from + 1, row: event.row };
};

/**
 * Where each commit sits, bucketed by day and stacked within it.
 *
 * Two commits on one day would otherwise land on top of each other, so each
 * day's commits take successive grid rows. Commits outside the window are
 * dropped: a point in time either falls in the window or it does not.
 */
export const placeCommits = <A extends { readonly at: Date }>(
  commits: readonly A[],
  zoom: Zoom,
  offset: number,
  now = new Date(),
): readonly (readonly [A, Placement])[] => {
  const span = SPANS[zoom];
  const today = startOfDay(now);
  const perDay = new Map<number, number>();
  const placed: (readonly [A, Placement])[] = [];
  for (const commit of commits) {
    const column = span - 1 + daysBetween(today, commit.at) + offset;
    if (column < 0 || column >= span) continue;
    const row = (perDay.get(column) ?? 0) + 1;
    perDay.set(column, row);
    placed.push([commit, { column: column + 1, span: 1, row }]);
  }
  return placed;
};
