/**
 * Activity — a fortnight of work as a calendar timeline.
 *
 * The design went through two shapes here: a plain event feed first, then the
 * calendar-style timeline the user supplied as a reference. This is the second:
 * a 14-column grid, one column per day, with each Task or Change Request laid
 * across the days it spans and a "now" line at the current day.
 *
 * Cards are buttons — clicking one opens that Task or Change Request, which is
 * what makes the timeline a navigation surface rather than a picture.
 */
import { html, type TemplateResult } from "lit";
import { customElement } from "lit/decorators.js";

import { GitPlusElement, navigate } from "./base.ts";
import { byId, timeline } from "./fixtures.ts";
import { statusToken, type Task } from "./model.ts";

/** Mon 10th through Sun 23rd, with the 18th as today — the design's window. */
interface Day {
  readonly letter: string;
  readonly date: number;
  readonly today?: boolean;
}

const DAYS: readonly Day[] = [
  { letter: "M", date: 10 },
  { letter: "T", date: 11 },
  { letter: "W", date: 12 },
  { letter: "T", date: 13 },
  { letter: "F", date: 14 },
  { letter: "S", date: 15 },
  { letter: "S", date: 16 },
  { letter: "M", date: 17 },
  { letter: "T", date: 18, today: true },
  { letter: "W", date: 19 },
  { letter: "T", date: 20 },
  { letter: "F", date: 21 },
  { letter: "S", date: 22 },
  { letter: "S", date: 23 },
];

@customElement("gp-activity")
export class GpActivity extends GitPlusElement {
  protected override render(): TemplateResult {
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
            10 – 23 Aug
            <button type="button" aria-label="Next fortnight">›</button>
          </div>
        </div>

        <div class="gp-cal-head">
          <div class="gp-cal-month">August 2026</div>
          <div class="gp-cal-spacer"></div>
          ${DAYS.map(
            (day) => html`
              <div class="gp-cal-day" ?data-today=${day.today === true}>
                ${day.letter} <strong>${day.date}</strong>
              </div>
            `,
          )}
        </div>

        <div class="gp-cal-body">
          <div class="gp-cal-now" aria-hidden="true"></div>
          <div class="gp-cal-now-dot" aria-hidden="true"></div>
          <div class="gp-cal-grid">
            ${timeline.map((event) => {
              const task = byId.get(event.id);
              return task === undefined
                ? ""
                : this.#card(task, event.column, event.span, event.row, event.epic === true);
            })}
          </div>
        </div>
      </div>
    `;
  }

  #card(task: Task, column: number, span: number, row: number, epic: boolean): TemplateResult {
    // The epic reads as a summary of everything nested under it, so the design
    // gives it the accent-tinted treatment and stacks its assignees on the end.
    const meta = epic ? `${task.id} · Epic · ${task.status}` : `${task.id} · ${task.status}`;
    return html`
      <button
        class="gp-cal-event"
        type="button"
        ?data-epic=${epic}
        style="grid-column: ${column} / span ${span}; grid-row: ${row}; --gp-status-color: var(--gp-${statusToken(task.status)});"
        @click=${() => navigate(this, { screen: "detail", id: task.id })}
      >
        <span class="gp-cal-event-body">
          <span class="gp-cal-event-title">${task.title}</span>
          <span class="gp-cal-event-meta">${meta}</span>
        </span>
        ${
          epic
            ? html`<span class="gp-cal-avatars">
                ${task.assignees.map((person) => html`<span class="gp-avatar">${person.avatar}</span>`)}
              </span>`
            : ""
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
