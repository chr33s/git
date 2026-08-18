/**
 * The left rail.
 *
 * Two behaviours the design conversation settled on: clicking the git+ logo
 * collapses the rail to a 64px strip of 36px icon squares and back, and the
 * search row carries a ⌘K hint that collapses to a bare icon button with it.
 */
import { html, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import { ApiError, type GitApi } from "./api.ts";
import { GitPlusElement, navigate, type Screen } from "./base.ts";
import * as icons from "./icons.ts";
import * as palette from "./theme.ts";
import { ThemeChangeEvent, type Theme } from "./theme.ts";
import { initials } from "./time.ts";

@customElement("gp-sidebar")
export class GpSidebar extends GitPlusElement {
  /** Which screen is lit. `detail` keeps Tasks lit — it is a child of it. */
  @property({ type: String }) accessor screen: Screen = "code";

  @property({ type: Boolean, reflect: true }) accessor collapsed = false;

  @property({ type: String }) accessor theme: Theme = palette.current();

  /** Open Task and Change Request count, shown as the Tasks badge. */
  @property({ type: Number }) accessor openCount = 8;

  /** Injected by the shell so every screen shares one client. */
  api: GitApi | null = null;

  /**
   * Who the server says is asking, from `/whoami`.
   *
   * `null` for an unauthenticated caller — which is the common case for a
   * repository with no genesis — so the rail says "anonymous" rather than
   * inventing the design's placeholder name.
   */
  @state() private accessor subject: string | null = null;

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#identify();
  }

  async #identify(): Promise<void> {
    const api = this.api;
    if (api === null) return;
    try {
      const who = await api.whoami();
      this.subject = who.subject;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
    }
  }

  #go(screen: Screen): void {
    navigate(this, { screen });
  }

  #item(screen: Screen, label: string, glyph: TemplateResult, badge?: number): TemplateResult {
    // `detail` is reached from the tasks list, so it keeps Tasks current.
    const active = this.screen === screen || (screen === "tasks" && this.screen === "detail");
    return html`
      <button
        class="gp-nav-item"
        type="button"
        aria-current=${active ? "page" : "false"}
        title=${this.collapsed ? label : ""}
        @click=${() => this.#go(screen)}
      >
        ${glyph}
        <span class="gp-nav-label">${label}</span>
        ${badge === undefined ? "" : html`<span class="gp-nav-badge">${badge}</span>`}
      </button>
    `;
  }

  protected override render(): TemplateResult {
    const mode = this.theme;
    return html`
      <nav class="gp-sidebar" ?data-collapsed=${this.collapsed} aria-label="Primary">
        <button
          class="gp-logo-row"
          type="button"
          title="Toggle navigation"
          aria-expanded=${this.collapsed ? "false" : "true"}
          @click=${() => {
            this.collapsed = !this.collapsed;
          }}
        >
          <span class="gp-logo-mark">${icons.logo()}</span>
          <span class="gp-logo-text">git<span>+</span></span>
        </button>

        <ui-search-field class="gp-search" debounce="200" title="Search">
          ${icons.search()}
          <span class="gp-nav-label">Search</span>
          <span class="gp-search-kbd">⌘K</span>
          <input class="gp-visually-hidden" type="search" name="q" aria-label="Search" />
        </ui-search-field>

        <div class="gp-nav-list">
          ${this.#item("activity", "Activity", icons.activity())}
          ${this.#item("code", "Code", icons.code())}
          ${this.#item("tasks", "Tasks", icons.tasks(), this.openCount)}
          ${this.#item("settings", "Settings", icons.settings())}
        </div>

        <div class="gp-sidebar-foot">
          <span class="gp-user-avatar">${initials(this.subject ?? "anon")}</span>
          <span class="gp-user-name">${this.subject ?? "anonymous"}</span>
          <button
            class="gp-theme-toggle"
            type="button"
            title=${mode === "dark" ? "Switch to light" : "Switch to dark"}
            aria-label=${mode === "dark" ? "Switch to light theme" : "Switch to dark theme"}
            @click=${() => {
              this.dispatchEvent(new ThemeChangeEvent(palette.toggle()));
            }}
          >
            ${mode === "dark" ? icons.sun() : icons.moon()}
          </button>
        </div>
      </nav>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-sidebar": GpSidebar;
  }
}
