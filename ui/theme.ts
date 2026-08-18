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

const KEY = "gp-theme";

/** The stored choice, or `null` when the user has not made one. */
export const stored = (): Theme | null => {
  const value = localStorage.getItem(KEY);
  return value === "light" || value === "dark" ? value : null;
};

/** What the page is actually showing right now. */
export const current = (): Theme => {
  const explicit = stored();
  if (explicit !== null) return explicit;
  return globalThis.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
};

/** Pin a palette: stamps the root so `tokens.css` switches, and remembers it. */
export const apply = (theme: Theme): void => {
  document.documentElement.dataset["theme"] = theme;
  localStorage.setItem(KEY, theme);
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
