/**
 * Phosphor Icons (regular), as Foldkit views.
 *
 * Inlined from the same SVG source the design uses, so a single `color` on
 * the parent themes every glyph in both palettes.
 *
 * `InnerHTML` is how the markup gets in. It is the right tool here and a bad
 * one nearly everywhere else: the strings are build-time imports of files in
 * this repository, so there is no input to trust or fail to escape.
 */
import { Html } from "foldkit";

import arrowRightSvg from "@phosphor-icons/core/regular/arrow-right.svg?raw";
import caretDownSvg from "@phosphor-icons/core/regular/caret-down.svg?raw";
import caretLeftSvg from "@phosphor-icons/core/regular/caret-left.svg?raw";
import clockSvg from "@phosphor-icons/core/regular/clock.svg?raw";
import codeSvg from "@phosphor-icons/core/regular/code.svg?raw";
import copySvg from "@phosphor-icons/core/regular/copy.svg?raw";
import dotsThreeSvg from "@phosphor-icons/core/regular/dots-three.svg?raw";
import fileTextSvg from "@phosphor-icons/core/regular/file-text.svg?raw";
import gearSvg from "@phosphor-icons/core/regular/gear.svg?raw";
import gitBranchSvg from "@phosphor-icons/core/regular/git-branch.svg?raw";
import gitDiffSvg from "@phosphor-icons/core/regular/git-diff.svg?raw";
import magnifyingGlassSvg from "@phosphor-icons/core/regular/magnifying-glass.svg?raw";
import moonSvg from "@phosphor-icons/core/regular/moon.svg?raw";
import pencilSimpleSvg from "@phosphor-icons/core/regular/pencil-simple.svg?raw";
import plusSvg from "@phosphor-icons/core/regular/plus.svg?raw";
import pulseSvg from "@phosphor-icons/core/regular/pulse.svg?raw";
import sunSvg from "@phosphor-icons/core/regular/sun.svg?raw";
import trashSvg from "@phosphor-icons/core/regular/trash.svg?raw";
import treeStructureSvg from "@phosphor-icons/core/regular/tree-structure.svg?raw";
import xSvg from "@phosphor-icons/core/regular/x.svg?raw";

const icon = <M>(h: Html.HtmlBuilder<M>, markup: string, size: number): Html.Html =>
  h.span([
    h.Class("gp-icon"),
    h.InnerHTML(
      markup.replace(
        "<svg ",
        `<svg width="${String(size)}" height="${String(size)}" aria-hidden="true" `,
      ),
    ),
  ]);

/** The git+ mark: four dots, the first in accent, the last in the text colour. */
export const logo = <M>(h: Html.HtmlBuilder<M>, size = 15): Html.Html =>
  h.span([
    h.Class("gp-icon"),
    h.InnerHTML(
      `<svg width="${String(size)}" height="${String(size)}" viewBox="0 0 16 16" fill="none" aria-hidden="true">` +
        `<circle cx="4" cy="4" r="1.7" fill="var(--gp-accent)"></circle>` +
        `<circle cx="12" cy="4" r="1.7" fill="var(--gp-fg-faint)"></circle>` +
        `<circle cx="4" cy="12" r="1.7" fill="var(--gp-fg-faint)"></circle>` +
        `<circle cx="12" cy="12" r="1.7" fill="var(--gp-fg)"></circle>` +
        `</svg>`,
    ),
  ]);

export const search = <M>(h: Html.HtmlBuilder<M>, size = 14): Html.Html =>
  icon(h, magnifyingGlassSvg, size);

export const activity = <M>(h: Html.HtmlBuilder<M>, size = 16): Html.Html =>
  icon(h, pulseSvg, size);

export const code = <M>(h: Html.HtmlBuilder<M>, size = 16): Html.Html => icon(h, codeSvg, size);

export const tasks = <M>(h: Html.HtmlBuilder<M>, size = 16): Html.Html =>
  icon(h, treeStructureSvg, size);

export const settings = <M>(h: Html.HtmlBuilder<M>, size = 16): Html.Html => icon(h, gearSvg, size);

export const branch = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html =>
  icon(h, gitBranchSvg, size);

export const chevronDown = <M>(h: Html.HtmlBuilder<M>, size = 10): Html.Html =>
  icon(h, caretDownSvg, size);

export const chevronLeft = <M>(h: Html.HtmlBuilder<M>, size = 12): Html.Html =>
  icon(h, caretLeftSvg, size);

export const arrowRight = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html =>
  icon(h, arrowRightSvg, size);

export const plus = <M>(h: Html.HtmlBuilder<M>, size = 15): Html.Html => icon(h, plusSvg, size);

export const ellipsis = <M>(h: Html.HtmlBuilder<M>, size = 15): Html.Html =>
  icon(h, dotsThreeSvg, size);

export const clock = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html => icon(h, clockSvg, size);

export const copy = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html => icon(h, copySvg, size);

export const pencil = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html =>
  icon(h, pencilSimpleSvg, size);

export const diff = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html => icon(h, gitDiffSvg, size);

export const close = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html => icon(h, xSvg, size);

export const trash = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html => icon(h, trashSvg, size);

export const document_ = <M>(h: Html.HtmlBuilder<M>, size = 13): Html.Html =>
  icon(h, fileTextSvg, size);

export const sun = <M>(h: Html.HtmlBuilder<M>, size = 14): Html.Html => icon(h, sunSvg, size);

export const moon = <M>(h: Html.HtmlBuilder<M>, size = 14): Html.Html => icon(h, moonSvg, size);
