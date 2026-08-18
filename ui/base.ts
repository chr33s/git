/**
 * The base every git+ element extends.
 *
 * Lit renders into a shadow root by default. Every element here overrides that
 * and renders into the **light DOM** instead, for one reason: `@chr33s/base-wc`
 * — the component library this UI is built on — requires it. Its contract is
 * "light DOM, no shadow root, so consumer CSS applies directly and cross-root
 * ARIA (`aria-controls`, `aria-activedescendant`, `aria-labelledby`) resolves
 * against elements the page owns". A `ui-tabs` whose panels live behind a
 * shadow boundary cannot wire that ARIA, and `app.css` could not reach it.
 *
 * So: Lit supplies the reactive rendering, `base-wc` supplies the behaviour,
 * and a single global stylesheet supplies the look. The two libraries agree
 * about the DOM they share because neither one hides it.
 */
import { LitElement } from "lit";

export class GitPlusElement extends LitElement {
  /** Render into the element itself — see the note above. */
  protected override createRenderRoot(): HTMLElement {
    return this;
  }
}

/**
 * The screens this UI can show.
 *
 * `detail` covers both a Task and a Change Request, because a Change Request
 * *is* a Task with a diff attached — the spec is explicit that they are not
 * parallel entity types — so one screen renders both and shows the extra
 * sections only when a diff exists.
 */
export type Screen = "activity" | "code" | "tasks" | "detail" | "settings";

/** The event a child fires to ask the shell to navigate. */
export interface NavigateDetail {
  readonly screen: Screen;
  /** Set when `screen` is `detail`: which Task or Change Request to open. */
  readonly id?: string;
}

export const NAVIGATE = "gp-navigate";

/**
 * The navigation request, as its own class.
 *
 * A named subclass rather than a bare `CustomEvent`, so the shell can narrow
 * with `instanceof` and read a typed `detail`; listening for a plain
 * `CustomEvent` would force an assertion back to `NavigateDetail`.
 */
export class NavigateEvent extends CustomEvent<NavigateDetail> {
  constructor(detail: NavigateDetail) {
    super(NAVIGATE, { bubbles: true, composed: true, detail });
  }
}

/** Fired by rows, nav items and timeline cards; handled once, by the shell. */
export const navigate = (target: EventTarget, detail: NavigateDetail): void => {
  target.dispatchEvent(new NavigateEvent(detail));
};
