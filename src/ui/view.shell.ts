/**
 * The left rail, and the frame the screens render inside.
 *
 * Two behaviours the design conversation settled on: clicking the git+ logo
 * collapses the rail to a 64px strip of 36px icon squares and back, and the
 * search row carries a ⌘K hint that collapses to a bare icon button with it.
 * The hint is honoured — ⌘K (or Ctrl+K) focuses the search input, expanding
 * the rail first if it was collapsed — but the keystroke is a Subscription and
 * the focus is a Command, because neither belongs to a view.
 *
 * The repo header (breadcrumb, Public, branch, Clone) belongs to Code alone.
 * The design review was explicit: Activity, Tasks, detail and Settings start
 * with their own heading, so Code renders that header itself rather than the
 * shell rendering it for everyone.
 */
import { Html } from "foldkit";

import type { Elements } from "./element.base-wc.ts";
import * as icon from "./icon.ts";
import { initials } from "./time.ts";
import { AppMessage } from "./app.message.ts";
import { AppRoute, railScreen, urlOf } from "./app.route.ts";
import { type Model, subjectOf } from "./app.model.ts";
import * as Tasks from "./task.ts";

/** The rail's search input, for the Command that ⌘K sends focus through. */
export const SEARCH_INPUT = ".gp-search input";

const item = (
  h: Html.HtmlBuilder<AppMessage>,
  model: Model,
  route: AppRoute,
  label: string,
  glyph: Html.Html,
  badge?: number,
): Html.Html =>
  h.button(
    [
      h.Class("gp-nav-item"),
      h.Type("button"),
      h.AriaCurrent(railScreen(model.route) === railScreen(route) ? "page" : "false"),
      h.Title(model.railCollapsed ? label : ""),
      h.OnClick(AppMessage.ClickedNavigate({ route })),
    ],
    [
      glyph,
      h.span([h.Class("gp-nav-label")], [label]),
      ...(badge === undefined ? [] : [h.span([h.Class("gp-nav-badge")], [String(badge)])]),
    ],
  );

export const sidebar = (
  model: Model,
  h: Html.HtmlBuilder<AppMessage>,
  ui: Elements<AppMessage>,
): Html.Html =>
  h.nav(
    [
      h.Class("gp-sidebar"),
      ...(model.railCollapsed ? [h.DataAttribute("collapsed", "")] : []),
      h.AriaLabel("Primary"),
    ],
    [
      h.button(
        [
          h.Class("gp-logo-row"),
          h.Type("button"),
          h.Title("Toggle navigation"),
          h.AriaExpanded(!model.railCollapsed),
          h.OnClick(AppMessage.ClickedRailToggle()),
        ],
        [
          h.span([h.Class("gp-logo-mark")], [icon.logo(h)]),
          h.span([h.Class("gp-logo-text")], ["git", h.span([], ["+"])]),
        ],
      ),

      ui.searchField(
        [
          h.Class("gp-search"),
          h.Attribute("debounce", "200"),
          h.Title("Search"),
          h.OnClick(AppMessage.ClickedSearchField()),
        ],
        [
          icon.search(h),
          h.span([h.Class("gp-nav-label")], ["Search"]),
          h.span([h.Class("gp-search-kbd")], ["⌘K"]),
          h.input([
            h.Class("gp-visually-hidden"),
            h.Type("search"),
            h.Name("q"),
            h.AriaLabel("Search"),
            h.Value(model.query),
            h.OnInput((query) => AppMessage.ChangedSearchQuery({ query })),
          ]),
        ],
      ),

      h.div(
        [h.Class("gp-nav-list")],
        [
          item(h, model, AppRoute.Activity(), "Activity", icon.activity(h)),
          item(h, model, AppRoute.Code({ path: "" }), "Code", icon.code(h)),
          item(
            h,
            model,
            AppRoute.Tasks(),
            "Tasks",
            icon.tasks(h),
            Tasks.openCount(model.tasks.tasks),
          ),
          item(h, model, AppRoute.Settings(), "Settings", icon.settings(h)),
        ],
      ),

      h.div(
        [h.Class("gp-sidebar-foot")],
        [
          h.span([h.Class("gp-user-avatar")], [initials(subjectOf(model) ?? "anon")]),
          h.span([h.Class("gp-user-name")], [subjectOf(model) ?? "anonymous"]),
          h.button(
            [
              h.Class("gp-theme-toggle"),
              h.Type("button"),
              h.Title(model.theme === "dark" ? "Switch to light" : "Switch to dark"),
              h.AriaLabel(
                model.theme === "dark" ? "Switch to light theme" : "Switch to dark theme",
              ),
              h.OnClick(AppMessage.ClickedThemeToggle()),
            ],
            [model.theme === "dark" ? icon.sun(h) : icon.moon(h)],
          ),
        ],
      ),
    ],
  );

/** The address every rail item links to, for a real `href` on a real link. */
export const linkTo = urlOf;
