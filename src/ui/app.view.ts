/**
 * The page: the rail, and whichever screen the route names.
 *
 * One function, matching on one route. What used to be a shell element
 * choosing among six custom elements is now a `match` a reader can see the
 * whole of — and because it is exhaustive, a route added without a screen is a
 * compile error rather than a blank panel.
 */
import { Html } from "foldkit";

import { elements } from "./element.base-wc.ts";
import { AppMessage } from "./app.message.ts";
import { AppRoute } from "./app.route.ts";
import type { Model } from "./app.model.ts";
import { sidebar } from "./view.shell.ts";
import { view as activity } from "./view.activity.ts";
import { view as code } from "./view.code.ts";
import { view as detail } from "./view.detail.ts";
import { view as search } from "./view.search.ts";
import { view as settings } from "./view.settings.ts";
import { view as tasks } from "./view.tasks.ts";

export const view = (model: Model, h: Html.HtmlBuilder<AppMessage>): Html.Document => {
  const ui = elements(h);

  /**
   * The explanation above whichever screen fell back to.
   *
   * Rendered by the shell rather than by each screen, because the thing it
   * explains is the address, and the address belongs to the shell.
   */
  const notice =
    model.navError === null
      ? h.empty
      : h.p([h.Class("gp-notice gp-nav-error"), h.DataAttribute("error", "")], [model.navError]);

  /**
   * One column, keyed by the screen in it.
   *
   * Code is the exception and does not use this: its explorer sits beside the
   * content rather than inside it, so it supplies its own `.gp-main` and goes
   * straight into the flex row. Every other screen is a single column, and the
   * shell supplies that column for it.
   *
   * The key is what stops the diff reusing one screen's elements for the
   * next: two screens both open with a `ui-toggle-group`, and a reused one
   * carries its selection across — the Tasks filter arriving as the Activity
   * zoom. A different screen is a different element.
   */
  const column = (screen: string, body: Html.Html): Html.Html =>
    h.keyed("main")(screen, [h.Class("gp-main")], [notice, body]);

  const body = AppRoute.match(model.route, {
    Code: () => [code(model, h, ui, notice)],
    NotFound: () => [code(model, h, ui, notice)],
    Tasks: () => [column("tasks", tasks(model, h, ui))],
    Activity: () => [column("activity", activity(model, h, ui))],
    Detail: () => [column("detail", detail(model, h, ui))],
    Settings: () => [column("settings", settings(model, h, ui))],
    Search: () => [column("search", search(model, h))],
  });

  return {
    title: "git+",
    body: h.div([h.Class("gp-shell")], [sidebar(model, h, ui), ...body]),
  };
};
