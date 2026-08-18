/**
 * Activity — a fortnight of repository history as a calendar timeline.
 *
 * The design went through two shapes here: a plain event feed first, then the
 * calendar-style timeline the user supplied as a reference. This is the second:
 * a 14-column grid, one column per day, with a "now" marker on today.
 *
 * What sits on the grid depends on what the server can answer. Commits are real
 * — `/commits/:oid` for the list, then each commit's raw header for its author
 * and date, because no JSON endpoint carries a timestamp. A commit is a point in
 * time rather than a span, so each occupies its own day column and stacks with
 * the others from that day; the design's spanning bars belonged to Tasks, which
 * have no HTTP surface yet (see `model.ts`). When the API cannot be reached the
 * screen falls back to the design's own Task timeline and says so.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, state } from "lit/decorators.js";

import { ApiError, type CommitDetail, describe, type GitApi } from "./api.ts";
import { GitPlusElement, navigate } from "./base.ts";
import { byId, timeline } from "./fixtures.ts";
import { statusToken, type Task } from "./model.ts";
import { ago, daysBetween, initials, startOfDay } from "./time.ts";

/** How many days the grid shows, and how far back it starts. */
const SPAN = 14;

interface Day {
  readonly letter: string;
  readonly date: number;
  readonly today: boolean;
}

const LETTERS = ["S", "M", "T", "W", "T", "F", "S"] as const;

/** Mon 10th – Sun 23rd August 2026, the window the design draws. */
const FIXTURE_DAYS: readonly Day[] = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23].map(
  (date, index) => ({
    letter: LETTERS[(index + 1) % 7] ?? "M",
    date,
    today: date === 18,
  }),
);

@customElement("gp-activity")
export class GpActivity extends GitPlusElement {
  /** Injected by the shell so every screen shares one client. */
  api: GitApi | null = null;

  @state() private accessor commits: readonly CommitDetail[] | null = null;
  @state() private accessor offline = false;
  /** Why the fallback is showing, in the reader\'s terms. */
  @state() private accessor reason = "";
  @state() private accessor loading = true;

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#load();
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
      this.commits = await api.recentCommits(main.oid, 40);
      this.offline = false;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.offline = true;
      this.reason = describe(error);
    } finally {
      this.loading = false;
    }
  }

  /** The 14 days ending today, which is the window real commits land in. */
  #days(): readonly Day[] {
    if (this.commits === null) return FIXTURE_DAYS;
    const today = startOfDay(new Date());
    return Array.from({ length: SPAN }, (_, index) => {
      const at = new Date(today);
      at.setDate(at.getDate() - (SPAN - 1 - index));
      return {
        letter: LETTERS[at.getDay()] ?? "M",
        date: at.getDate(),
        today: index === SPAN - 1,
      };
    });
  }

  #month(): string {
    if (this.commits === null) return "August 2026";
    const today = new Date();
    const start = new Date(today);
    start.setDate(start.getDate() - (SPAN - 1));
    const month = (at: Date): string =>
      at.toLocaleString(undefined, { month: "long", year: "numeric" });
    return start.getMonth() === today.getMonth()
      ? month(today)
      : `${start.toLocaleString(undefined, { month: "long" })} – ${month(today)}`;
  }

  #range(): string {
    const days = this.#days();
    const first = days[0]?.date ?? 1;
    const last = days[days.length - 1]?.date ?? 1;
    const suffix = this.commits === null ? " Aug" : "";
    return `${String(first)} – ${String(last)}${suffix}`;
  }

  protected override render(): TemplateResult {
    const days = this.#days();
    return html`
      <div class="gp-screen">
        <div class="gp-activity-head">
          <h1 class="gp-heading">Activity</h1>
          <ui-toggle-group class="gp-segmented" aria-label="Timeline zoom">
            <ui-toggle class="gp-segment" value="day">Day</ui-toggle>
            <ui-toggle class="gp-segment" value="week" data-active>Week</ui-toggle>
            <ui-toggle class="gp-segment" value="month">Month</ui-toggle>
          </ui-toggle-group>
          <div class="gp-range">
            <button type="button" aria-label="Previous fortnight">‹</button>
            ${this.#range()}
            <button type="button" aria-label="Next fortnight">›</button>
          </div>
        </div>

        ${
          this.offline
            ? html`<p class="gp-notice">Showing the design's sample timeline — ${this.reason}.</p>`
            : nothing
        }

        <div class="gp-cal-head">
          <div class="gp-cal-month">${this.#month()}</div>
          <div class="gp-cal-spacer"></div>
          ${days.map(
            (day) => html`
              <div class="gp-cal-day" ?data-today=${day.today}>
                ${day.letter} <strong>${day.date}</strong>
              </div>
            `,
          )}
        </div>

        <div class="gp-cal-body">
          <div class="gp-cal-now" style=${this.#nowOffset()} aria-hidden="true"></div>
          <div class="gp-cal-now-dot" style=${this.#nowOffset()} aria-hidden="true"></div>
          <div class="gp-cal-grid">${this.#cards()}</div>
        </div>
      </div>
    `;
  }

  /**
   * Where the "now" marker sits.
   *
   * The design pinned it mid-column 9 for its fixed fortnight; with a window
   * that ends today it belongs in the last column instead.
   */
  #nowOffset(): string {
    const column = this.commits === null ? 8.5 : SPAN - 0.5;
    return `left: calc(100% / ${String(SPAN)} * ${String(column)})`;
  }

  #cards(): TemplateResult | readonly TemplateResult[] {
    if (this.loading) return html`<div class="gp-empty">Loading history…</div>`;

    const commits = this.commits;
    if (commits === null) {
      return timeline.map((event) => {
        const task = byId.get(event.id);
        return task === undefined
          ? html``
          : this.#task(task, event.column, event.span, event.row, event.epic === true);
      });
    }

    // Bucket by day, then stack each day's commits into successive grid rows so
    // two commits on one day cannot land on top of each other.
    const today = startOfDay(new Date());
    const perDay = new Map<number, number>();
    const cards: TemplateResult[] = [];
    for (const commit of commits) {
      const offset = SPAN - 1 + daysBetween(today, commit.at);
      if (offset < 0 || offset >= SPAN) continue;
      const row = (perDay.get(offset) ?? 0) + 1;
      perDay.set(offset, row);
      cards.push(this.#commit(commit, offset + 1, row));
    }
    if (cards.length === 0) {
      return html`<div class="gp-empty">No commits in the last ${String(SPAN)} days.</div>`;
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
