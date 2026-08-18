/**
 * The icon set, transcribed from the design.
 *
 * Every glyph is inline SVG with `stroke="currentColor"`, which is what lets a
 * single `color` on the parent theme an icon in both palettes without a second
 * asset. Sizes are the design's own — 16px in the nav, 13–15px inline.
 */
import { html, type TemplateResult } from "lit";
import { svg } from "lit";

const icon = (size: number, body: TemplateResult, viewBox = "0 0 16 16"): TemplateResult =>
  html`<svg
    width="${size}"
    height="${size}"
    viewBox="${viewBox}"
    fill="none"
    stroke="currentColor"
    stroke-width="1.5"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
  >
    ${body}
  </svg>`;

/** The git+ mark: four dots, the first in accent, the last in the text colour. */
export const logo = (size = 15): TemplateResult =>
  html`<svg width="${size}" height="${size}" viewBox="0 0 16 16" fill="none" aria-hidden="true">
    ${svg`<circle cx="4" cy="4" r="1.7" fill="var(--gp-accent)"></circle>
    <circle cx="12" cy="4" r="1.7" fill="var(--gp-fg-faint)"></circle>
    <circle cx="4" cy="12" r="1.7" fill="var(--gp-fg-faint)"></circle>
    <circle cx="12" cy="12" r="1.7" fill="var(--gp-fg)"></circle>`}
  </svg>`;

export const search = (size = 14): TemplateResult =>
  icon(size, svg`<circle cx="7" cy="7" r="4.5"></circle><path d="m10.5 10.5 3 3"></path>`);

/** Activity: a heartbeat line. */
export const activity = (size = 16): TemplateResult =>
  icon(size, svg`<path d="M1.5 8h3l2-5 3 10 2-5h3"></path>`);

/** Code: angle brackets. Doubles as the README "view source" affordance. */
export const code = (size = 16): TemplateResult =>
  icon(size, svg`<path d="M5.5 4.5 2 8l3.5 3.5M10.5 4.5 14 8l-3.5 3.5"></path>`);

/** Tasks: two nodes joined by an elbow — a hierarchy, not a checklist. */
export const tasks = (size = 16): TemplateResult =>
  icon(
    size,
    svg`<circle cx="4" cy="4" r="2"></circle><circle cx="12" cy="12" r="2"></circle>
    <path d="M4 6v3a3 3 0 0 0 3 3h3"></path>`,
  );

export const settings = (size = 16): TemplateResult =>
  icon(
    size,
    svg`<path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"></path>
    <circle cx="12" cy="12" r="3"></circle>`,
    "0 0 24 24",
  );

export const branch = (size = 13): TemplateResult =>
  icon(
    size,
    svg`<path d="M5 3v7M5 10a2 2 0 1 0 0 4 2 2 0 0 0 0-4ZM11 6a2 2 0 1 0 0-4 2 2 0 0 0 0 4Zm0 0v1a3 3 0 0 1-3 3H5"></path>`,
  );

export const chevronDown = (size = 10): TemplateResult =>
  icon(size, svg`<path d="m4 6 4 4 4-4"></path>`);

export const chevronLeft = (size = 12): TemplateResult =>
  icon(size, svg`<path d="M10 3 5 8l5 5"></path>`);

export const arrowRight = (size = 13): TemplateResult =>
  icon(size, svg`<path d="M2 8h11M10 4.5 13.5 8 10 11.5"></path>`);

export const plus = (size = 15): TemplateResult =>
  icon(size, svg`<path d="M8 3v10M3 8h10"></path>`);

export const ellipsis = (size = 15): TemplateResult =>
  html`<svg
    width="${size}"
    height="${size}"
    viewBox="0 0 16 16"
    fill="currentColor"
    aria-hidden="true"
  >
    ${svg`<circle cx="3" cy="8" r="1.3"></circle><circle cx="8" cy="8" r="1.3"></circle><circle cx="13" cy="8" r="1.3"></circle>`}
  </svg>`;

export const document_ = (size = 13): TemplateResult =>
  icon(size, svg`<path d="M3 2.5h10v11H3z"></path><path d="M5.5 5.5h5M5.5 8h5M5.5 10.5h3"></path>`);

/** Sun and moon, for the theme toggle the design did not need but a two-palette
 * build does. */
export const sun = (size = 14): TemplateResult =>
  icon(
    size,
    svg`<circle cx="8" cy="8" r="3"></circle>
    <path d="M8 1v1.5M8 13.5V15M15 8h-1.5M2.5 8H1M12.9 3.1l-1 1M4.1 11.9l-1 1M12.9 12.9l-1-1M4.1 4.1l-1-1"></path>`,
  );

export const moon = (size = 14): TemplateResult =>
  icon(size, svg`<path d="M13.5 9.6A5.8 5.8 0 0 1 6.4 2.5a5.8 5.8 0 1 0 7.1 7.1Z"></path>`);
