/**
 * Light and dark, and how the page remembers which.
 *
 * Both palettes come from the design — `git-plus.dc.html` is the dark original
 * and `git-plus light.dc.html` the light variant derived from it — so neither
 * is a computed inversion of the other; `tokens.css` carries both verbatim and
 * this module only decides which one is in force.
 *
 * Three states, in the order they are consulted: an explicit choice the user
 * made here, then the OS preference, and `tokens.css` handles that last case on
 * its own through `prefers-color-scheme`, so an untouched page needs no script
 * to look right.
 */

export type Theme = "light" | "dark";

export const THEME_CHANGE = "gp-theme-change";

/** A typed palette change, owned by the shell and passed back down as state. */
export class ThemeChangeEvent extends CustomEvent<Theme> {
  constructor(theme: Theme) {
    super(THEME_CHANGE, { bubbles: true, composed: true, detail: theme });
  }
}

const KEY = "gp-theme";

/**
 * The stored choice, or `null` when the user has not made one.
 *
 * Guarded the way `index.html`'s inline script guards the same key: a private
 * window refuses storage outright, and a browser with no remembered choice and
 * a browser that will not say are the same thing to every caller here.
 */
export const stored = (): Theme | null => {
  try {
    const value = localStorage.getItem(KEY);
    return value === "light" || value === "dark" ? value : null;
  } catch {
    return null;
  }
};

/** What the page is actually showing right now. */
export const current = (): Theme => {
  const explicit = stored();
  if (explicit !== null) return explicit;
  return globalThis.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
};

/**
 * Pin a palette: stamps the root so `tokens.css` switches, and remembers it.
 *
 * The stamp is the part that must happen; remembering it is the part that may
 * fail. A refused write — a full origin quota on a page that clones whole
 * repositories into OPFS — is a page that forgets the palette between
 * sessions, not a reason to end the one it is in: this runs inside a Command,
 * and a Command that throws crashes the application terminally.
 */
export const apply = (theme: Theme): void => {
  document.documentElement.dataset["theme"] = theme;
  try {
    localStorage.setItem(KEY, theme);
  } catch {
    // See above: the palette is applied, only the memory of it is lost.
  }
};

/**
 * Restore the stored choice before first paint.
 *
 * Called from `main.ts` at module scope rather than from a component, so the
 * attribute lands before anything renders and there is no flash of the wrong
 * palette.
 */
export const restore = (): void => {
  const explicit = stored();
  if (explicit !== null) document.documentElement.dataset["theme"] = explicit;
};

export const toggle = (): Theme => {
  const next: Theme = current() === "dark" ? "light" : "dark";
  apply(next);
  return next;
};
