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

import { UISearchField } from "@chr33s/base-wc/src/search-field";

import { ApiError, clientFromDocument, type GitApi } from "./api.ts";
import { GitPlusElement, NAVIGATE, NavigateEvent, type Screen } from "./base.ts";
import { store } from "./store.ts";
import { current as currentTheme, THEME_CHANGE, ThemeChangeEvent, type Theme } from "./theme.ts";

import "./nav.sidebar.ts";
import "./screen.activity.ts";
import "./screen.code.ts";
import "./screen.detail.ts";
import "./screen.settings.ts";
import "./screen.tasks.ts";

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
  @state() private accessor openCount = store.openCount();
  @state() private accessor query = "";

  /**
   * Who the server says is asking, from `/whoami` — fetched once here and
   * handed down, so the rail, the Tasks screen and the detail screen agree on
   * one identity instead of each asking for it. `null` for an unauthenticated
   * caller, which the screens render as "anonymous" rather than inventing the
   * design's placeholder name.
   */
  @state() private accessor viewer: string | null = null;

  readonly #api: GitApi = clientFromDocument();

  #unsubscribe: (() => void) | null = null;

  override connectedCallback(): void {
    super.connectedCallback();
    this.addEventListener(NAVIGATE, this.#onNavigate);
    this.addEventListener(THEME_CHANGE, this.#onThemeChange);
    this.addEventListener("search", this.#onSearch);
    this.#unsubscribe = store.subscribe(() => {
      this.openCount = store.openCount();
    });
    this.#fromHash();
    globalThis.addEventListener("hashchange", this.#onHashChange);
    void this.#identify();
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.removeEventListener(NAVIGATE, this.#onNavigate);
    this.removeEventListener(THEME_CHANGE, this.#onThemeChange);
    this.removeEventListener("search", this.#onSearch);
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    globalThis.removeEventListener("hashchange", this.#onHashChange);
  }

  async #identify(): Promise<void> {
    try {
      this.viewer = (await this.#api.whoami()).subject;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
    }
  }

  #onHashChange = (): void => this.#fromHash();

  /**
   * The rail's search, debounced by `ui-search-field`.
   *
   * Tasks are what the store can search, so a query shows the Tasks screen
   * filtered by it; clearing the field clears the filter but stays put.
   */
  #onSearch = (event: Event): void => {
    const field = event.target;
    if (!(field instanceof UISearchField)) return;
    this.query = field.value;
    if (field.value.trim() !== "" && this.screen !== "tasks") {
      this.screen = "tasks";
      globalThis.location.hash = "#/tasks";
    }
  };

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
          .openCount=${this.openCount}
          .subject=${this.viewer}
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
        return html`<div class="gp-main">
          <gp-tasks .query=${this.query} .viewer=${this.viewer}></gp-tasks>
        </div>`;
      case "detail":
        return html`<div class="gp-main">
          <gp-detail
            .api=${this.#api}
            .taskId=${this.selected}
            .theme=${this.theme}
            .viewer=${this.viewer}
          ></gp-detail>
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
