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

import { ApiError, clientFromDocument, type GitApi, type Whoami } from "./api.ts";
import { GitPlusElement, NAVIGATE, NavigateEvent, type Screen } from "./base.ts";
import { store } from "./store.ts";
import { current as currentTheme, THEME_CHANGE, ThemeChangeEvent, type Theme } from "./theme.ts";

import "./nav.sidebar.ts";
import "./screen.activity.ts";
import "./screen.code.ts";
import "./screen.detail.ts";
import "./screen.search.ts";
import "./screen.settings.ts";
import "./screen.tasks.ts";

const SCREENS: readonly string[] = ["activity", "code", "tasks", "detail", "settings", "search"];

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
   * The whole `/whoami` answer — fetched once here and handed down, so the
   * rail, the screens and the Settings identity card agree on one identity
   * instead of each asking for it. `null` while unanswered or offline.
   */
  @state() private accessor who: Whoami | null = null;

  /** Which file the Code screen should open, when navigation named one. */
  @state() private accessor codePath: string | null = null;

  get #viewer(): string | null {
    return this.who?.subject ?? null;
  }

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
      this.who = await this.#api.whoami();
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
    }
  }

  #onHashChange = (): void => this.#fromHash();

  /**
   * The rail's search, debounced by `ui-search-field`.
   *
   * A query opens the Search screen, which answers from both sides of the
   * split: Tasks from the client-side store, file contents from the server's
   * `/grep`. Clearing the field leaves the screen in place with its hint.
   */
  #onSearch = (event: Event): void => {
    const field = event.target;
    if (!(field instanceof UISearchField)) return;
    this.query = field.value;
    if (field.value.trim() !== "" && this.screen !== "search") {
      this.screen = "search";
      globalThis.location.hash = "#/search";
    }
  };

  #onThemeChange = (event: Event): void => {
    if (event instanceof ThemeChangeEvent) this.theme = event.detail;
  };

  #onNavigate = (event: Event): void => {
    if (!(event instanceof NavigateEvent)) return;
    event.stopPropagation();
    this.screen = event.detail.screen;
    if (event.detail.screen === "code") this.codePath = event.detail.id ?? null;
    else if (event.detail.id !== undefined) this.selected = event.detail.id;
    const hash =
      event.detail.id === undefined
        ? `#/${event.detail.screen}`
        : `#/${event.detail.screen}/${event.detail.id}`;
    if (globalThis.location.hash !== hash) globalThis.location.hash = hash;
  };

  /**
   * `#/tasks`, `#/detail/CR-14`, `#/code/src/server/Api.ts` — deep-linkable,
   * and survives a refresh. Only the first segment is the screen; the rest is
   * the id, joined back together because a file path carries slashes.
   */
  #fromHash(): void {
    const [screen, ...rest] = globalThis.location.hash.replace(/^#\/?/, "").split("/");
    if (screen === undefined || !isScreen(screen)) return;
    this.screen = screen;
    const id = rest.join("/");
    if (id === "") return;
    if (screen === "code") this.codePath = id;
    else this.selected = id;
  }

  protected override render(): TemplateResult {
    return html`
      <div class="gp-shell">
        <gp-sidebar
          .screen=${this.screen}
          .openCount=${this.openCount}
          .subject=${this.#viewer}
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
        return html`<gp-code
          .api=${this.#api}
          .theme=${this.theme}
          .who=${this.who}
          .wanted=${this.codePath}
        ></gp-code>`;
      case "search":
        return html`<div class="gp-main">
          <gp-search .api=${this.#api} .query=${this.query}></gp-search>
        </div>`;
      case "activity":
        return html`<div class="gp-main"><gp-activity .api=${this.#api}></gp-activity></div>`;
      case "tasks":
        return html`<div class="gp-main"><gp-tasks .viewer=${this.#viewer}></gp-tasks></div>`;
      case "detail":
        return html`<div class="gp-main">
          <gp-detail
            .api=${this.#api}
            .taskId=${this.selected}
            .theme=${this.theme}
            .viewer=${this.#viewer}
          ></gp-detail>
        </div>`;
      case "settings":
        return html`<div class="gp-main">
          <gp-settings .api=${this.#api} .who=${this.who}></gp-settings>
        </div>`;
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-app": GpApp;
  }
}
