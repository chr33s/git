/**
 * The Tasks list.
 *
 * One hierarchy holding both Tasks and Change Requests, because a Change
 * Request is a Task with a diff attached rather than a parallel entity — so
 * splitting them into two lists would misrepresent the model.
 *
 * The row treatment carries the Linear borrowings from the design conversation:
 * a status ring at the head of each row (solid once Done or Merged, outlined
 * otherwise), quiet coloured status text in place of a loud pill, and an
 * "updated …" stamp before it. Top-level rows drop the tree-glyph slot so the
 * kind chip sits 18px from the left edge, matching the avatar's 18px on the
 * right; children indent by 14px and keep the glyph.
 */
import { html, type TemplateResult } from "lit";
import { customElement } from "lit/decorators.js";

import { GitPlusElement, navigate } from "./base.ts";
import { rows } from "./fixtures.ts";
import { isTerminal, ringToken, statusToken, type Task } from "./model.ts";

/** The kind chip: "Task" or "CR", tinted by kind. */
export const kindChip = (task: Task): TemplateResult =>
  html`<span class="gp-kind" data-kind=${task.kind}>${task.kind}</span>`;

/** The full status pill, used on detail screens. */
export const statusPill = (task: Task): TemplateResult =>
  html`<span
    class="gp-status"
    style="--gp-status-color: var(--gp-${statusToken(task.status)}); --gp-status-rgb: var(--gp-${statusToken(task.status)}-rgb);"
    >${task.status}</span
  >`;

@customElement("gp-tasks")
export class GpTasks extends GitPlusElement {
  protected override render(): TemplateResult {
    return html`
      <div class="gp-screen">
        <div class="gp-tasks-head">
          <h1 class="gp-heading">Tasks</h1>
          <ui-toggle-group class="gp-segmented" aria-label="Filter by kind">
            <ui-toggle class="gp-segment" value="all" data-active>All</ui-toggle>
            <ui-toggle class="gp-segment" value="tasks">Tasks</ui-toggle>
            <ui-toggle class="gp-segment" value="crs">Change Requests</ui-toggle>
          </ui-toggle-group>
          <button class="gp-btn-primary" type="button">New task</button>
        </div>

        <div class="gp-task-list">${rows().map(({ task, depth }) => this.#row(task, depth))}</div>
      </div>
    `;
  }

  #row(task: Task, depth: number): TemplateResult {
    const ring = ringToken(task.status);
    const quiet = statusToken(task.status);
    return html`
      <button
        class="gp-task-row"
        type="button"
        data-depth=${depth}
        @click=${() => navigate(this, { screen: "detail", id: task.id })}
      >
        ${depth > 0 ? html`<span class="gp-task-glyph" aria-hidden="true">└</span>` : ""}
        <span
          class="gp-status-ring"
          style="--gp-status-color: var(--gp-${ring});"
          ?data-filled=${isTerminal(task.status)}
          aria-hidden="true"
        ></span>
        ${kindChip(task)}
        <span class="gp-id">${task.id}</span>
        <span class="gp-task-title">${task.title}</span>
        <span class="gp-task-meta">
          <span class="gp-task-updated">updated ${task.updated}</span>
          <span class="gp-status-quiet" style="--gp-status-color: var(--gp-${quiet});"
            >${task.status}</span
          >
          <span class="gp-avatar">${task.avatar}</span>
        </span>
      </button>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-tasks": GpTasks;
  }
}
