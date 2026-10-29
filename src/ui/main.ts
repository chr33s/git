/**
 * Entry point.
 *
 * The theme is already settled by the time this runs — `index.html` carries a
 * tiny inline script that stamps `data-theme` before the first paint, which is
 * the only way to avoid a flash of the wrong palette without blocking on the
 * bundle. `restore()` is called again here so the module is also correct when
 * the UI is mounted into a host page that has no such script.
 */
import "@fontsource-variable/instrument-sans";
import "@fontsource-variable/jetbrains-mono";
import "./tokens.css";
import "./styles/base.css";
import "./styles/sidebar.css";
import "./styles/primitives.css";
import "./styles/screen.code.css";
import "./styles/screen.activity.css";
import "./styles/screen.tasks.css";
import "./styles/screen.detail.css";
import "./styles/screen.search.css";
import "./styles/screen.settings.css";
import "./styles/a11y.css";

import { Runtime } from "foldkit";

import { restore } from "./theme.ts";
import { application } from "./app.ts";

restore();

Runtime.run(application);
