/**
 * The left rail.
 *
 * Two behaviours the design conversation settled on: clicking the git+ logo
 * collapses the rail to a 64px strip of 36px icon squares and back, and the
 * search row carries a ⌘K hint that collapses to a bare icon button with it.
 */
import { html, type TemplateResult } from "lit";
import { customElement, property } from "lit/decorators.js";

import { GitPlusElement, navigate, type Screen } from "./base.ts";
import * as icons from "./icons.ts";
import * as theme from "./theme.ts";

@customElement("gp-sidebar")
export class GpSidebar extends GitPlusElement {
  /** Which screen is lit. `detail` keeps Tasks lit — it is a child of it. */
  @property({ type: String }) accessor screen: Screen = "code";

  @property({ type: Boolean, reflect: true }) accessor collapsed = false;

  /** Open Task and Change Request count, shown as the Tasks badge. */
  @property({ type: Number }) accessor openCount = 8;

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
    const mode = theme.current();
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
          <span class="gp-user-avatar">MK</span>
          <span class="gp-user-name">mkessler</span>
          <button
            class="gp-theme-toggle"
            type="button"
            title=${mode === "dark" ? "Switch to light" : "Switch to dark"}
            aria-label=${mode === "dark" ? "Switch to light theme" : "Switch to dark theme"}
            @click=${() => {
              theme.toggle();
              this.requestUpdate();
              this.dispatchEvent(new CustomEvent("gp-theme-change", { bubbles: true }));
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
