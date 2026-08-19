/**
 * Activity — a window of repository history as a calendar timeline.
 *
 * The design went through two shapes here: a plain event feed first, then the
 * calendar-style timeline the user supplied as a reference. This is the second:
 * a grid of day columns with a "now" marker on today.
 *
 * What sits on the grid depends on what the server can answer. Commits are real
 * — `/commits/:oid` for the list, then each commit's raw header for its author
 * and date, because no JSON endpoint carries a timestamp. A commit is a point in
 * time rather than a span, so each occupies its own day column and stacks with
 * the others from that day; the design's spanning bars belonged to Tasks, which
 * have no HTTP surface yet (see `model.ts`). When the API cannot be reached the
 * screen falls back to the design's own Task timeline and says so.
 *
 * The header controls work the same in both modes: the zoom segments set how
 * many days the window spans (a week, the design's fortnight, or a month) and
 * ‹ › page it back and forth. Both modes therefore need a window expressed as
 * dates, so the fixture timeline is anchored to the day the design drew it
 * around and its cards are placed — and clipped — by date like real commits.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, state } from "lit/decorators.js";

import { UIToggleGroup } from "@chr33s/base-wc/src/toggle";

import { ApiError, type CodeApi, type CommitDetail, describe } from "./api.ts";
import { store } from "./store.ts";
import { GitPlusElement, navigate } from "./base.ts";
import { byId, timeline } from "./fixtures.ts";
import { statusToken, type Task } from "./model.ts";
import { ago, daysBetween, initials, startOfDay } from "./time.ts";

/** How many days each zoom level shows. "Week" is the design's fortnight. */
type Zoom = "day" | "week" | "month";

const SPANS = { day: 7, week: 14, month: 31 } satisfies Record<Zoom, number>;

/**
 * The day the design's timeline is drawn around, and the fortnight it sits in.
 *
 * `fixtures.ts` places its cards in 1-based columns of that fortnight, which
 * began five days before this date and ran five days after it. Anchoring both
 * to a real date is what lets the fixture window zoom and page: a column is
 * just a day, so any window can say which of its days a card covers.
 */
const FIXTURE_TODAY = new Date(2026, 7, 18);
const FIXTURE_LEAD = 5;
/** The day before fixture column 1 — the origin its columns are counted from. */
const FIXTURE_ORIGIN = new Date(2026, 7, 9);

interface Day {
  readonly letter: string;
  readonly date: number;
  readonly today: boolean;
}

const LETTERS = ["S", "M", "T", "W", "T", "F", "S"] as const;

/** `at`, moved by whole days, without mutating the argument. */
const shift = (at: Date, days: number): Date => {
  const moved = new Date(at);
  moved.setDate(moved.getDate() + days);
  return moved;
};

@customElement("gp-activity")
export class GpActivity extends GitPlusElement {
  /**
   * Injected by the shell so every screen shares one client — the local
   * OPFS repository once its clone lands, which turns this screen's
   * hundred-commit read into local object reads instead of an N+1 of
   * requests.
   */
  api: CodeApi | null = null;

  @state() private accessor commits: readonly CommitDetail[] | null = null;
  @state() private accessor offline = false;
  /** Why the fallback is showing, in the reader\'s terms. */
  @state() private accessor reason = "";
  @state() private accessor loading = true;
  @state() private accessor zoom: Zoom = "week";
  /** How many days back the window's last column sits; 0 means it ends today. */
  @state() private accessor offset = 0;

  #unsubscribe: (() => void) | null = null;

  override connectedCallback(): void {
    super.connectedCallback();
    this.#unsubscribe = store.subscribe(() => this.requestUpdate());
    void this.#load();
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.#unsubscribe?.();
    this.#unsubscribe = null;
  }

  async #load(): Promise<void> {
    const api = this.api;
    if (api === null) {
      this.offline = true;
      this.reason = "no API client was provided";
      this.loading = false;
      return;
    }
    try {
      const refs = await api.refs();
      const heads = refs.filter((ref) => ref.name.startsWith("refs/heads/"));
      const main = heads.find((ref) => ref.name === "refs/heads/main") ?? heads[0];
      if (main === undefined) {
        this.commits = [];
        this.offline = false;
        return;
      }
      // One fetch covers paging too: ‹ walks back through what is already
      // loaded rather than repeating the N+1 header reads per window. 100
      // commits of history is the bound, and a window past it reads as empty.
      this.commits = await api.recentCommits(main.oid, 100);
      this.offline = false;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.offline = true;
      this.reason = describe(error);
    } finally {
      this.loading = false;
    }
  }

  get #span(): number {
    return SPANS[this.zoom];
  }

  /** Today, or the day the fixture timeline stands in for. */
  get #today(): Date {
    return this.commits === null ? FIXTURE_TODAY : startOfDay(new Date());
  }

  /**
   * The date of the window's last column.
   *
   * Live history ends today; the fixture fortnight ran five days past the day
   * it is drawn around, so its window ends there and "now" sits mid-grid — the
   * ninth of fourteen columns the design drew. `offset` pages both back.
   */
  get #end(): Date {
    const lead = this.commits === null ? FIXTURE_LEAD : 0;
    return shift(this.#today, lead - this.offset);
  }

  /** The group types its own `value`, so nothing here is asserted. */
  #onZoom = (event: Event): void => {
    const group = event.currentTarget;
    if (!(group instanceof UIToggleGroup)) return;
    const value = group.value;
    if (value !== "day" && value !== "week" && value !== "month") return;
    this.zoom = value;
    this.offset = 0;
  };

  /** The `span` days the window covers, oldest first. */
  #days(): readonly Day[] {
    const span = this.#span;
    const end = this.#end;
    const today = this.#today.getTime();
    return Array.from({ length: span }, (_, index) => {
      const at = shift(end, index - (span - 1));
      return {
        letter: LETTERS[at.getDay()] ?? "M",
        date: at.getDate(),
        today: at.getTime() === today,
      };
    });
  }

  #month(): string {
    const end = this.#end;
    const start = shift(end, -(this.#span - 1));
    const month = (at: Date): string =>
      at.toLocaleString(undefined, { month: "long", year: "numeric" });
    return start.getMonth() === end.getMonth() && start.getFullYear() === end.getFullYear()
      ? month(end)
      : `${start.toLocaleString(undefined, { month: "long" })} – ${month(end)}`;
  }

  #range(): string {
    const days = this.#days();
    const first = days[0]?.date ?? 1;
    const last = days[days.length - 1]?.date ?? 1;
    return `${String(first)} – ${String(last)}`;
  }

  /**
   * The hub's sessions: what each agent was told and what came of it, from
   * `GET /hub/sessions`. Absent entirely until the hub answers — provenance
   * is not something to fake with fixtures.
   */
  #sessions(): TemplateResult | typeof nothing {
    const sessions = store.sessions;
    if (sessions.length === 0) return nothing;
    return html`
      <h2 class="gp-section-label">Sessions</h2>
      <div class="gp-panel-card gp-sessions">
        ${sessions.map(
          (session) => html`
            <div class="gp-list-row">
              <span class="gp-sha">${session.id.slice(0, 8)}</span>
              <span>${session.agent}</span>
              <span class="gp-when">
                ${session.commits} commit${session.commits === 1 ? "" : "s"}
                ${session.pulls.length > 0 ? ` · ${session.pulls.length} PR(s)` : ""}
                ${session.openDecisions > 0 ? ` · ${session.openDecisions} open question(s)` : ""}
                ${session.tokens > 0 ? ` · ${String(session.tokens)} tokens` : ""}
              </span>
            </div>
          `,
        )}
      </div>
    `;
  }

  protected override render(): TemplateResult {
    const days = this.#days();
    const now = this.#nowOffset();
    return html`
      <div class="gp-screen" style="--gp-cal-cols: ${String(this.#span)}">
        <div class="gp-activity-head">
          <h1 class="gp-heading">Activity</h1>
          <ui-toggle-group class="gp-segmented" aria-label="Timeline zoom" @change=${this.#onZoom}>
            ${(["day", "week", "month"] as const).map(
              (level) => html`
                <ui-toggle class="gp-segment" value=${level} ?data-active=${this.zoom === level}
                  >${level.charAt(0).toUpperCase() + level.slice(1)}</ui-toggle
                >
              `,
            )}
          </ui-toggle-group>
          <div class="gp-range">
            <button
              type="button"
              aria-label="Earlier"
              @click=${() => {
                this.offset += this.#span;
              }}
            >
              ‹
            </button>
            ${this.#range()}
            <button
              type="button"
              aria-label="Later"
              ?disabled=${this.offset === 0}
              @click=${() => {
                this.offset = Math.max(0, this.offset - this.#span);
              }}
            >
              ›
            </button>
          </div>
        </div>

        ${
          this.offline
            ? html`<p class="gp-notice">Showing the design's sample timeline — ${this.reason}.</p>`
            : nothing
        }
        ${this.#sessions()}

        <div class="gp-cal-head">
          <div class="gp-cal-month">${this.#month()}</div>
          ${days.map(
            (day) => html`
              <div class="gp-cal-day" ?data-today=${day.today}>
                ${day.letter} <strong>${day.date}</strong>
              </div>
            `,
          )}
        </div>

        <div class="gp-cal-body">
          ${
            now === null
              ? nothing
              : html`
                  <div class="gp-cal-now" style=${now} aria-hidden="true"></div>
                  <div class="gp-cal-now-dot" style=${now} aria-hidden="true"></div>
                `
          }
          <div class="gp-cal-grid">${this.#cards()}</div>
        </div>
      </div>
    `;
  }

  /**
   * Where the "now" marker sits, or `null` when that day is not in the window.
   *
   * Mid-column of the day `#today` names, which is the last column for a live
   * window ending today and the design's column 9 for the fixture fortnight.
   * A window paged into the past has no "now" to mark.
   */
  #nowOffset(): string | null {
    const span = this.#span;
    const index = span - 1 - daysBetween(this.#today, this.#end);
    if (index < 0 || index >= span) return null;
    return `left: calc(100% / ${String(span)} * ${String(index + 0.5)})`;
  }

  #cards(): TemplateResult | readonly TemplateResult[] {
    if (this.loading) return html`<div class="gp-empty">Loading history…</div>`;

    const commits = this.commits;
    if (commits === null) {
      const span = this.#span;
      const start = shift(this.#end, -(span - 1));
      // A fixture column is a day of the design's fortnight, so it maps onto
      // the window the same way a commit's date does. Cards reaching past an
      // edge are clipped to it rather than dropped, which is what keeps the
      // week zoom showing the bars that started before it.
      const cards: TemplateResult[] = [];
      for (const event of timeline) {
        const task = byId.get(event.id);
        if (task === undefined) continue;
        const from = Math.max(daysBetween(start, shift(FIXTURE_ORIGIN, event.column)), 0);
        const to = Math.min(
          daysBetween(start, shift(FIXTURE_ORIGIN, event.column + event.span - 1)),
          span - 1,
        );
        if (from > to) continue;
        cards.push(this.#task(task, from + 1, to - from + 1, event.row, event.epic === true));
      }
      if (cards.length === 0) {
        return html`<div class="gp-empty">No events in this window.</div>`;
      }
      return cards;
    }

    // Bucket by day, then stack each day's commits into successive grid rows so
    // two commits on one day cannot land on top of each other.
    const span = this.#span;
    const today = startOfDay(new Date());
    const perDay = new Map<number, number>();
    const cards: TemplateResult[] = [];
    for (const commit of commits) {
      const column = span - 1 + daysBetween(today, commit.at) + this.offset;
      if (column < 0 || column >= span) continue;
      const row = (perDay.get(column) ?? 0) + 1;
      perDay.set(column, row);
      cards.push(this.#commit(commit, column + 1, row));
    }
    if (cards.length === 0) {
      return html`<div class="gp-empty">No commits in this window.</div>`;
    }
    return cards;
  }

  /** One real commit, in its own day column. */
  #commit(commit: CommitDetail, column: number, row: number): TemplateResult {
    return html`
      <div
        class="gp-cal-event"
        data-point
        style="grid-column: ${String(column)} / span 1; grid-row: ${String(
          row,
        )}; --gp-status-color: var(--gp-accent);"
        title="${commit.subject} — ${commit.author}"
      >
        <span class="gp-cal-event-body">
          <span class="gp-cal-event-title">${commit.subject}</span>
          <span class="gp-cal-event-meta">
            ${commit.oid.slice(0, 7)} · ${initials(commit.author)} · ${ago(commit.at)}
          </span>
        </span>
      </div>
    `;
  }

  /** A fixture Task, laid out as the design draws it. */
  #task(task: Task, column: number, span: number, row: number, epic: boolean): TemplateResult {
    const meta = epic ? `${task.id} · Epic · ${task.status}` : `${task.id} · ${task.status}`;
    return html`
      <button
        class="gp-cal-event"
        type="button"
        ?data-epic=${epic}
        style="grid-column: ${String(column)} / span ${String(span)}; grid-row: ${String(
          row,
        )}; --gp-status-color: var(--gp-${statusToken(task.status)});"
        @click=${() => navigate(this, { screen: "detail", id: task.id })}
      >
        <span class="gp-cal-event-body">
          <span class="gp-cal-event-title">${task.title}</span>
          <span class="gp-cal-event-meta">${meta}</span>
        </span>
        ${
          epic
            ? html`<span class="gp-cal-avatars">
                ${task.assignees.map(
                  (person) => html`<span class="gp-avatar">${person.avatar}</span>`,
                )}
              </span>`
            : nothing
        }
      </button>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-activity": GpActivity;
  }
}
