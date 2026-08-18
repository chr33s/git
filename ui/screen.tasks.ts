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
 *
 * The list reads the store, not the fixtures, so it reflects what the segments
 * select, what the rail's search matched, and what "New task" created. That
 * button opens a `ui-dialog` holding a real form: title and description, and a
 * submit that asks the store for the Task — creation is a store concern, this
 * screen only navigates to the result.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import { UIToggleGroup } from "@chr33s/base-wc/src/toggle";

import { GitPlusElement, navigate } from "./base.ts";
import { isTerminal, ringToken, statusToken, type Task } from "./model.ts";
import { type Filter, store } from "./store.ts";
import { initials } from "./time.ts";

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

const SEGMENTS: readonly { readonly value: Filter; readonly label: string }[] = [
  { value: "all", label: "All" },
  { value: "tasks", label: "Tasks" },
  { value: "crs", label: "Change Requests" },
];

@customElement("gp-tasks")
export class GpTasks extends GitPlusElement {
  /** Who the server said is asking; new Tasks are authored by them. */
  @property({ type: String }) accessor viewer: string | null = null;

  @state() private accessor filter: Filter = "all";

  #unsubscribe: (() => void) | null = null;

  override connectedCallback(): void {
    super.connectedCallback();
    this.#unsubscribe = store.subscribe(() => this.requestUpdate());
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.#unsubscribe?.();
    this.#unsubscribe = null;
  }

  /** The group types its own `value`, so nothing here is asserted. */
  #onFilter = (event: Event): void => {
    const group = event.currentTarget;
    if (!(group instanceof UIToggleGroup)) return;
    const value = group.value;
    // Single-select toggle groups allow deselection; no segment means "all".
    this.filter = value === "tasks" || value === "crs" ? value : "all";
  };

  #create = (event: SubmitEvent): void => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const titleField = form.elements.namedItem("title");
    const descField = form.elements.namedItem("desc");
    if (!(titleField instanceof HTMLInputElement)) return;
    if (!(descField instanceof HTMLTextAreaElement)) return;
    const title = titleField.value.trim();
    if (title === "") return;
    const name = this.viewer ?? "anonymous";
    const task = store.create({
      title,
      desc: descField.value.trim(),
      author: { name, avatar: initials(name) },
    });
    form.reset();
    this.querySelector("ui-dialog")?.hide();
    navigate(this, { screen: "detail", id: task.id });
  };

  protected override render(): TemplateResult {
    const rows = store.rows(this.filter);
    return html`
      <div class="gp-screen">
        <div class="gp-tasks-head">
          <h1 class="gp-heading">Tasks</h1>
          <ui-toggle-group
            class="gp-segmented"
            aria-label="Filter by kind"
            @change=${this.#onFilter}
          >
            ${SEGMENTS.map(
              (segment) => html`
                <ui-toggle
                  class="gp-segment"
                  value=${segment.value}
                  ?data-active=${this.filter === segment.value}
                  >${segment.label}</ui-toggle
                >
              `,
            )}
          </ui-toggle-group>
          ${this.#newTask()}
        </div>

        ${
          rows.length === 0
            ? html`<div class="gp-empty">Nothing here.</div>`
            : html`<div class="gp-task-list">
                ${rows.map(({ task, depth }) => this.#row(task, depth))}
              </div>`
        }
      </div>
    `;
  }

  /**
   * The "New task" dialog.
   *
   * `ui-dialog` owns the modality — trigger wiring, focus trap, Escape and
   * outside-press dismissal — and this template owns only the form. Creation
   * stays in this browser until the hub gets an HTTP surface, and the dialog
   * says so rather than passing a local change off as a durable one.
   */
  #newTask(): TemplateResult {
    return html`
      <ui-dialog class="gp-new-task">
        <button class="gp-btn-primary" data-dialog-trigger type="button">New task</button>
        <ui-dialog-popup class="gp-dialog">
          <h2 class="gp-dialog-title" data-dialog-title>New task</h2>
          <p class="gp-dialog-hint" data-dialog-description>
            Stored in this browser tab — the hub has no HTTP surface yet, so nothing is written to
            the repository.
          </p>
          <form @submit=${this.#create}>
            <label class="gp-field-label" for="gp-new-title">Title</label>
            <input
              id="gp-new-title"
              class="gp-input"
              name="title"
              required
              autocomplete="off"
              placeholder="What needs doing?"
            />
            <label class="gp-field-label" for="gp-new-desc">Description</label>
            <textarea
              id="gp-new-desc"
              class="gp-textarea"
              name="desc"
              rows="4"
              placeholder="Context, constraints, links…"
            ></textarea>
            <div class="gp-dialog-actions">
              <button
                class="gp-btn-quiet"
                type="button"
                @click=${() => this.querySelector("ui-dialog")?.hide()}
              >
                Cancel
              </button>
              <button class="gp-btn-primary" type="submit">Create task</button>
            </div>
          </form>
        </ui-dialog-popup>
      </ui-dialog>
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
        ${depth > 0 ? html`<span class="gp-task-glyph" aria-hidden="true">└</span>` : nothing}
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
