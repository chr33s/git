/**
 * Phosphor Icons (regular), sized for this UI.
 *
 * Glyphs come from `@phosphor-icons/core` as SVG source and are inlined so a
 * single `color` on the parent themes them in both palettes. The git+ mark is
 * this project's own, not Phosphor's.
 *
 * @see https://github.com/phosphor-icons/homepage
 */
import { html, svg, type TemplateResult } from "lit";
import { unsafeSVG } from "lit/directives/unsafe-svg.js";

import arrowRightSvg from "@phosphor-icons/core/regular/arrow-right.svg";
import caretDownSvg from "@phosphor-icons/core/regular/caret-down.svg";
import caretLeftSvg from "@phosphor-icons/core/regular/caret-left.svg";
import clockSvg from "@phosphor-icons/core/regular/clock.svg";
import codeSvg from "@phosphor-icons/core/regular/code.svg";
import copySvg from "@phosphor-icons/core/regular/copy.svg";
import dotsThreeSvg from "@phosphor-icons/core/regular/dots-three.svg";
import fileTextSvg from "@phosphor-icons/core/regular/file-text.svg";
import gearSvg from "@phosphor-icons/core/regular/gear.svg";
import gitBranchSvg from "@phosphor-icons/core/regular/git-branch.svg";
import gitDiffSvg from "@phosphor-icons/core/regular/git-diff.svg";
import magnifyingGlassSvg from "@phosphor-icons/core/regular/magnifying-glass.svg";
import moonSvg from "@phosphor-icons/core/regular/moon.svg";
import pencilSimpleSvg from "@phosphor-icons/core/regular/pencil-simple.svg";
import plusSvg from "@phosphor-icons/core/regular/plus.svg";
import pulseSvg from "@phosphor-icons/core/regular/pulse.svg";
import sunSvg from "@phosphor-icons/core/regular/sun.svg";
import trashSvg from "@phosphor-icons/core/regular/trash.svg";
import treeStructureSvg from "@phosphor-icons/core/regular/tree-structure.svg";
import xSvg from "@phosphor-icons/core/regular/x.svg";

const icon = (markup: string, size: number): TemplateResult =>
  html`${unsafeSVG(
    markup.replace("<svg ", `<svg width="${size}" height="${size}" aria-hidden="true" `),
  )}`;

/** The git+ mark: four dots, the first in accent, the last in the text colour. */
export const logo = (size = 15): TemplateResult =>
  html`<svg width="${size}" height="${size}" viewBox="0 0 16 16" fill="none" aria-hidden="true">
    ${svg`<circle cx="4" cy="4" r="1.7" fill="var(--gp-accent)"></circle>
    <circle cx="12" cy="4" r="1.7" fill="var(--gp-fg-faint)"></circle>
    <circle cx="4" cy="12" r="1.7" fill="var(--gp-fg-faint)"></circle>
    <circle cx="12" cy="12" r="1.7" fill="var(--gp-fg)"></circle>`}
  </svg>`;

export const search = (size = 14): TemplateResult => icon(magnifyingGlassSvg, size);

/** Activity: a heartbeat line. */
export const activity = (size = 16): TemplateResult => icon(pulseSvg, size);

/** Code: angle brackets. Doubles as the README "view source" affordance. */
export const code = (size = 16): TemplateResult => icon(codeSvg, size);

/** Tasks: a hierarchy, not a checklist. */
export const tasks = (size = 16): TemplateResult => icon(treeStructureSvg, size);

export const settings = (size = 16): TemplateResult => icon(gearSvg, size);

export const branch = (size = 13): TemplateResult => icon(gitBranchSvg, size);

export const chevronDown = (size = 10): TemplateResult => icon(caretDownSvg, size);

export const chevronLeft = (size = 12): TemplateResult => icon(caretLeftSvg, size);

export const arrowRight = (size = 13): TemplateResult => icon(arrowRightSvg, size);

export const plus = (size = 15): TemplateResult => icon(plusSvg, size);

export const ellipsis = (size = 15): TemplateResult => icon(dotsThreeSvg, size);

export const clock = (size = 13): TemplateResult => icon(clockSvg, size);

export const copy = (size = 13): TemplateResult => icon(copySvg, size);

export const pencil = (size = 13): TemplateResult => icon(pencilSimpleSvg, size);

export const diff = (size = 13): TemplateResult => icon(gitDiffSvg, size);

export const close = (size = 13): TemplateResult => icon(xSvg, size);

export const trash = (size = 13): TemplateResult => icon(trashSvg, size);

export const document_ = (size = 13): TemplateResult => icon(fileTextSvg, size);

/** Sun and moon, for the theme toggle. */
export const sun = (size = 14): TemplateResult => icon(sunSvg, size);

export const moon = (size = 14): TemplateResult => icon(moonSvg, size);
