/**
 * Search — what the rail's ⌘K field opens.
 *
 * One query, answered from both sides of the repository's split: Tasks and
 * Change Requests come from the client-side store (the hub has no HTTP
 * surface yet — see `store.ts`), and file contents come from the server's
 * `POST /grep`, literal and case-insensitive because a reader types text, not
 * a regular expression.
 *
 * A task hit opens its detail; a code hit opens the Code screen on that file.
 * When the API cannot be reached the code section says so and the task
 * section still answers — half an answer, honestly labelled, beats none.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import { ApiError, describe, type GrepMatch, type SearchApi } from "./api.ts";
import { GitPlusElement, navigate } from "./base.ts";
import { statusToken, type Task } from "./model.ts";
import { kindChip } from "./screen.tasks.ts";
import { store } from "./store.ts";

type CodeResults =
  | { readonly tag: "idle" }
  | { readonly tag: "loading" }
  | {
      readonly tag: "loaded";
      readonly matches: readonly GrepMatch[];
      readonly truncated: boolean;
    }
  | { readonly tag: "unavailable"; readonly reason: string };

@customElement("gp-search")
export class GpSearch extends GitPlusElement {
  /** Injected by the shell so every screen shares one client. */
  api: SearchApi | null = null;

  /** The rail's query, already debounced by `ui-search-field`. */
  @property({ type: String }) accessor query = "";

  @state() private accessor code: CodeResults = { tag: "idle" };

  #generation = 0;
  #abort: AbortController | null = null;

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#grep();
  }

  override willUpdate(changed: Map<string, unknown>): void {
    if (changed.has("query")) void this.#grep();
  }

  override disconnectedCallback(): void {
    this.#abort?.abort();
    this.#abort = null;
    super.disconnectedCallback();
  }

  async #grep(): Promise<void> {
    this.#abort?.abort();
    const abort = new AbortController();
    this.#abort = abort;
    const generation = ++this.#generation;
    const pattern = this.query.trim();
    if (pattern === "") {
      this.code = { tag: "idle" };
      return;
    }
    const api = this.api;
    if (api === null) {
      this.code = { tag: "unavailable", reason: "no API client was provided" };
      return;
    }
    this.code = { tag: "loading" };
    try {
      const refs = await api.refs();
      const heads = refs.filter((ref) => ref.name.startsWith("refs/heads/"));
      const main = heads.find((ref) => ref.name === "refs/heads/main") ?? heads[0];
      if (main === undefined) {
        if (generation === this.#generation) {
          this.code = { tag: "loaded", matches: [], truncated: false };
        }
        return;
      }
      const found = await api.grep(pattern, main.name, undefined, abort.signal);
      if (generation === this.#generation) {
        this.code = { tag: "loaded", matches: found.matches, truncated: found.truncated };
      }
    } catch (error) {
      if (abort.signal.aborted) return;
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#generation) {
        this.code = { tag: "unavailable", reason: describe(error) };
      }
    }
  }

  protected override render(): TemplateResult {
    const query = this.query.trim();
    return html`
      <div class="gp-screen">
        <h1 class="gp-heading">Search</h1>
        ${
          query === ""
            ? html`<div class="gp-empty">
                Type in the rail's search — ⌘K — to look across Tasks and file contents.
              </div>`
            : html`${this.#tasks(query)} ${this.#code(query)}`
        }
      </div>
    `;
  }

  #tasks(query: string): TemplateResult {
    const rows = store.rows("all", query);
    return html`
      <h2 class="gp-section-label">Tasks</h2>
      ${
        rows.length === 0
          ? html`<div class="gp-empty">No task titles or ids match “${query}”.</div>`
          : html`<div class="gp-task-list">${rows.map(({ task }) => this.#task(task))}</div>`
      }
    `;
  }

  #task(task: Task): TemplateResult {
    return html`
      <button
        class="gp-task-row gp-search-task-row"
        type="button"
        @click=${() => navigate(this, { screen: "detail", id: task.id })}
      >
        ${kindChip(task)}
        <span class="gp-id">${task.id}</span>
        <span class="gp-task-title">${task.title}</span>
        <span class="gp-task-meta">
          <span
            class="gp-status-quiet"
            style="--gp-status-color: var(--gp-${statusToken(task.status)});"
            >${task.status}</span
          >
        </span>
      </button>
    `;
  }

  #code(query: string): TemplateResult {
    const code = this.code;
    return html`
      <h2 class="gp-section-label">Code</h2>
      ${
        code.tag === "unavailable"
          ? html`<p class="gp-notice">Code search needs the server — ${code.reason}.</p>`
          : code.tag === "loaded"
            ? this.#matches(code, query)
            : html`<div class="gp-empty">Searching file contents…</div>`
      }
    `;
  }

  #matches(
    code: { readonly matches: readonly GrepMatch[]; readonly truncated: boolean },
    query: string,
  ): TemplateResult {
    if (code.matches.length === 0) {
      return html`<div class="gp-empty">No file contents match “${query}”.</div>`;
    }
    return html`
      <div class="gp-task-list">
        ${code.matches.map(
          (match) => html`
            <button
              class="gp-search-hit"
              type="button"
              @click=${() => navigate(this, { screen: "code", id: match.path })}
            >
              <span class="gp-search-hit-path"
                >${match.path}<span class="gp-search-hit-line">:${match.line}</span></span
              >
              <span class="gp-search-hit-text">${match.text.trim()}</span>
            </button>
          `,
        )}
      </div>
      ${
        code.truncated
          ? html`<p class="gp-notice">More matches exist — the answer was capped.</p>`
          : nothing
      }
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-search": GpSearch;
  }
}
