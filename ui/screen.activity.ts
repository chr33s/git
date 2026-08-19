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
 * The header controls work on the live window: the zoom segments set how many
 * days it spans (a week, the design's fortnight, or a month) and ‹ › page it
 * back and forth through history. The fixture timeline is drawn for exactly
 * the design's fortnight, so in fallback mode those controls disable rather
 * than pretending to page data that does not move.
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

/** The design's own fortnight, which the fixture timeline is drawn for. */
const FIXTURE_SPAN = 14;

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

  /** The design's fortnight when showing fixtures; the chosen zoom when live. */
  get #span(): number {
    return this.commits === null ? FIXTURE_SPAN : SPANS[this.zoom];
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

  /** The `span` days ending `offset` days ago, which real commits land in. */
  #days(): readonly Day[] {
    if (this.commits === null) return FIXTURE_DAYS;
    const span = this.#span;
    const today = startOfDay(new Date());
    return Array.from({ length: span }, (_, index) => {
      const at = new Date(today);
      at.setDate(at.getDate() - this.offset - (span - 1 - index));
      return {
        letter: LETTERS[at.getDay()] ?? "M",
        date: at.getDate(),
        today: this.offset === 0 && index === span - 1,
      };
    });
  }

  #month(): string {
    if (this.commits === null) return "August 2026";
    const end = new Date();
    end.setDate(end.getDate() - this.offset);
    const start = new Date(end);
    start.setDate(start.getDate() - (this.#span - 1));
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
    const suffix = this.commits === null ? " Aug" : "";
    return `${String(first)} – ${String(last)}${suffix}`;
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
    const live = this.commits !== null;
    return html`
      <div class="gp-screen" style="--gp-cal-cols: ${String(this.#span)}">
        <div class="gp-activity-head">
          <h1 class="gp-heading">Activity</h1>
          <ui-toggle-group class="gp-segmented" aria-label="Timeline zoom" @change=${this.#onZoom}>
            ${(["day", "week", "month"] as const).map(
              (level) => html`
                <ui-toggle
                  class="gp-segment"
                  value=${level}
                  ?disabled=${!live}
                  ?data-active=${this.zoom === level}
                  >${level.charAt(0).toUpperCase() + level.slice(1)}</ui-toggle
                >
              `,
            )}
          </ui-toggle-group>
          <div class="gp-range">
            <button
              type="button"
              aria-label="Earlier"
              ?disabled=${!live}
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
              ?disabled=${!live || this.offset === 0}
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
            live && this.offset > 0
              ? nothing
              : html`
                  <div class="gp-cal-now" style=${this.#nowOffset()} aria-hidden="true"></div>
                  <div class="gp-cal-now-dot" style=${this.#nowOffset()} aria-hidden="true"></div>
                `
          }
          <div class="gp-cal-grid">${this.#cards()}</div>
        </div>
      </div>
    `;
  }

  /**
   * Where the "now" marker sits.
   *
   * The design pinned it mid-column 9 for its fixed fortnight; with a window
   * that ends today it belongs in the last column instead, and a window paged
   * into the past has no "now" to mark at all — the marker is omitted there.
   */
  #nowOffset(): string {
    const span = this.#span;
    const column = this.commits === null ? 8.5 : span - 0.5;
    return `left: calc(100% / ${String(span)} * ${String(column)})`;
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
