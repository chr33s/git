/**
 * Entry point.
 *
 * The theme is already settled by the time this runs — `index.html` carries a
 * tiny inline script that stamps `data-theme` before the first paint, which is
 * the only way to avoid a flash of the wrong palette without blocking on the
 * bundle. `restore()` is called again here so the module is also correct when
 * the UI is mounted into a host page that has no such script.
 *
 * `elements.ts` is imported before `app.ts` so every `ui-*` definition exists
 * by the time the first template using one is stamped.
 */
import "./tokens.css";
import "./app.css";

import { REGISTERED } from "./elements.ts";
import { restore } from "./theme.ts";
import "./app.ts";

restore();

// Reading the array is what keeps `elements.ts` — and so every custom element
// definition in it — from being tree-shaken out of the bundle.
if (REGISTERED.length === 0) throw new Error("git+ UI: no base-wc elements registered");
