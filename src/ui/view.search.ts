/**
 * Search — what the rail's ⌘K field opens.
 *
 * One query, answered from both sides of the repository's split: Tasks and
 * Change Requests are a pure query over the Model, and file contents come
 * from the server's `POST /grep`, literal and case-insensitive because a
 * reader types text, not a regular expression.
 *
 * A task hit opens its detail; a code hit opens the Code screen on that file.
 * When the API cannot be reached the code section says so and the task
 * section still answers — half an answer, honestly labelled, beats none.
 */
import { AsyncData, Html } from "foldkit";

import { statusToken, type Task } from "./model.ts";
import { AppMessage } from "./app.message.ts";
import type { CodeHits, Model } from "./app.model.ts";
import { kindChip } from "./view.tasks.ts";
import * as Tasks from "./task.ts";

const taskRow = (h: Html.HtmlBuilder<AppMessage>, task: Task): Html.Html =>
  h.button(
    [
      h.Class("gp-task-row gp-search-task-row"),
      h.Type("button"),
      h.OnClick(AppMessage.ClickedTask({ id: task.id })),
    ],
    [
      kindChip(h, task),
      h.span([h.Class("gp-id")], [task.id]),
      h.span([h.Class("gp-task-title")], [task.title]),
      h.span(
        [h.Class("gp-task-meta")],
        [
          h.span(
            [
              h.Class("gp-status-quiet"),
              h.Style({ "--gp-status-color": `var(--gp-${statusToken(task.status)})` }),
            ],
            [task.status],
          ),
        ],
      ),
    ],
  );

const taskHalf = (h: Html.HtmlBuilder<AppMessage>, model: Model, query: string): Html.Html => {
  const rows = Tasks.rows(model.tasks.tasks, "all", query);
  return h.div(
    [],
    [
      h.h2([h.Class("gp-section-label")], ["Tasks"]),
      rows.length === 0
        ? h.div([h.Class("gp-empty")], [`No task titles or ids match “${query}”.`])
        : h.div(
            [h.Class("gp-task-list")],
            rows.map(({ task }) => taskRow(h, task)),
          ),
    ],
  );
};

const hits = (h: Html.HtmlBuilder<AppMessage>, found: CodeHits, query: string): Html.Html =>
  found.matches.length === 0
    ? h.div([h.Class("gp-empty")], [`No file contents match “${query}”.`])
    : h.div(
        [],
        [
          h.div(
            [h.Class("gp-task-list")],
            found.matches.map((match) =>
              h.button(
                [
                  h.Class("gp-search-hit"),
                  h.Type("button"),
                  h.OnClick(AppMessage.ClickedCodeHit({ path: match.path })),
                ],
                [
                  h.span(
                    [h.Class("gp-search-hit-path")],
                    [
                      match.path,
                      h.span([h.Class("gp-search-hit-line")], [`:${String(match.line)}`]),
                    ],
                  ),
                  h.span([h.Class("gp-search-hit-text")], [match.text.trim()]),
                ],
              ),
            ),
          ),
          found.truncated
            ? h.p([h.Class("gp-notice")], ["More matches exist — the answer was capped."])
            : h.empty,
        ],
      );

const codeHalf = (h: Html.HtmlBuilder<AppMessage>, model: Model, query: string): Html.Html =>
  h.div(
    [],
    [
      h.h2([h.Class("gp-section-label")], ["Code"]),
      AsyncData.match(model.searchScreen.code, {
        onIdle: () => h.div([h.Class("gp-empty")], ["Searching file contents…"]),
        onLoading: () => h.div([h.Class("gp-empty")], ["Searching file contents…"]),
        onRefreshing: (found) => hits(h, found, query),
        onSuccess: (found) => hits(h, found, query),
        onStale: ({ data }) => hits(h, data, query),
        onFailure: (reason) =>
          h.p([h.Class("gp-notice")], [`Code search needs the server — ${reason}.`]),
      }),
    ],
  );

export const view = (model: Model, h: Html.HtmlBuilder<AppMessage>): Html.Html => {
  const query = model.query.trim();
  return h.div(
    [h.Class("gp-screen")],
    [
      h.h1([h.Class("gp-heading")], ["Search"]),
      ...(query === ""
        ? [
            h.div(
              [h.Class("gp-empty")],
              ["Type in the rail's search — ⌘K — to look across Tasks and file contents."],
            ),
          ]
        : [taskHalf(h, model, query), codeHalf(h, model, query)]),
    ],
  );
};
