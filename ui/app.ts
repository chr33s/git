/**
 * The shell: the left rail, and whichever screen is current.
 *
 * Navigation is one listener. Rows, nav items and timeline cards all fire a
 * bubbling `gp-navigate`, so no child needs a reference to the shell or to a
 * router — the shell is simply the first thing up the tree that handles it.
 *
 * The repo header (breadcrumb / Public / branch / Clone) belongs to Code alone.
 * The design review was explicit about that: Activity, Tasks, detail and
 * Settings start directly with their own heading, so `gp-code` renders that
 * header itself rather than the shell rendering it for everyone.
 */
import { html, type TemplateResult } from "lit";
import { customElement, state } from "lit/decorators.js";

import { clientFromDocument, type GitApi } from "./api.ts";
import { GitPlusElement, NAVIGATE, NavigateEvent, type Screen } from "./base.ts";
import { tasks } from "./fixtures.ts";
import { current as currentTheme, THEME_CHANGE, ThemeChangeEvent, type Theme } from "./theme.ts";

import "./nav.sidebar.ts";
import "./screen.activity.ts";
import "./screen.code.ts";
import "./screen.detail.ts";
import "./screen.settings.ts";
import "./screen.tasks.ts";

/** Statuses that mean the work is still open, for the nav badge. */
const OPEN = tasks.filter((task) => task.status !== "Done" && task.status !== "Merged").length;

const SCREENS: readonly string[] = ["activity", "code", "tasks", "detail", "settings"];

/**
 * Declared above `GpApp`, not below it.
 *
 * `@customElement` registers the tag as the class initialises, and `<gp-app>`
 * is already in the document — so `connectedCallback` runs during that
 * initialisation, before any `const` further down the module has a value. A
 * reference from below would land in the temporal dead zone and throw on the
 * very first navigation.
 */
const isScreen = (value: string): value is Screen => SCREENS.includes(value);

@customElement("gp-app")
export class GpApp extends GitPlusElement {
  @state() private accessor screen: Screen = "code";
  @state() private accessor selected = "T-12";
  @state() private accessor theme: Theme = currentTheme();

  readonly #api: GitApi = clientFromDocument();

  override connectedCallback(): void {
    super.connectedCallback();
    this.addEventListener(NAVIGATE, this.#onNavigate);
    this.addEventListener(THEME_CHANGE, this.#onThemeChange);
    this.#fromHash();
    globalThis.addEventListener("hashchange", this.#onHashChange);
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.removeEventListener(NAVIGATE, this.#onNavigate);
    this.removeEventListener(THEME_CHANGE, this.#onThemeChange);
    globalThis.removeEventListener("hashchange", this.#onHashChange);
  }

  #onHashChange = (): void => this.#fromHash();

  #onThemeChange = (event: Event): void => {
    if (event instanceof ThemeChangeEvent) this.theme = event.detail;
  };

  #onNavigate = (event: Event): void => {
    if (!(event instanceof NavigateEvent)) return;
    event.stopPropagation();
    this.screen = event.detail.screen;
    if (event.detail.id !== undefined) this.selected = event.detail.id;
    const hash =
      event.detail.id === undefined
        ? `#/${event.detail.screen}`
        : `#/${event.detail.screen}/${event.detail.id}`;
    if (globalThis.location.hash !== hash) globalThis.location.hash = hash;
  };

  /** `#/tasks`, `#/detail/CR-14` — deep-linkable, and survives a refresh. */
  #fromHash(): void {
    const [screen, id] = globalThis.location.hash.replace(/^#\/?/, "").split("/");
    if (screen === undefined || !isScreen(screen)) return;
    this.screen = screen;
    if (id !== undefined && id !== "") this.selected = id;
  }

  protected override render(): TemplateResult {
    return html`
      <div class="gp-shell">
        <gp-sidebar
          .screen=${this.screen}
          .openCount=${OPEN}
          .api=${this.#api}
          .theme=${this.theme}
        ></gp-sidebar>
        ${this.#screen()}
      </div>
    `;
  }

  #screen(): TemplateResult {
    switch (this.screen) {
      case "code":
        // `gp-code` owns both the explorer column and the main column, because
        // the explorer is a sibling of the content in the design's layout, not
        // a child of it.
        return html`<gp-code .api=${this.#api} .theme=${this.theme}></gp-code>`;
      case "activity":
        return html`<div class="gp-main"><gp-activity .api=${this.#api}></gp-activity></div>`;
      case "tasks":
        return html`<div class="gp-main"><gp-tasks></gp-tasks></div>`;
      case "detail":
        return html`<div class="gp-main">
          <gp-detail .api=${this.#api} .taskId=${this.selected} .theme=${this.theme}></gp-detail>
        </div>`;
      case "settings":
        return html`<div class="gp-main"><gp-settings .api=${this.#api}></gp-settings></div>`;
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-app": GpApp;
  }
}
