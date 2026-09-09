/**
 * The Tasks list.
 *
 * One hierarchy holding both Tasks and Change Requests, because a Change
 * Request is a Task with a diff attached rather than a parallel entity — so
 * splitting them into two lists would misrepresent the model.
 *
 * The row treatment carries the Linear borrowings from the design conversation:
 * a status ring at the head of each row (solid once Done or Merged, outlined
 * otherwise), quiet coloured status text in place of a loud pill, and an
 * "updated …" stamp before it. Top-level rows drop the tree-glyph slot so the
 * kind chip sits 18px from the left edge, matching the avatar's 18px on the
 * right; children indent by 14px and keep the glyph.
 *
 * A view, not a component: it reads the Model and returns Html, and the only
 * thing it can do about a click is name a Message. Creation, filtering and
 * navigation all happen in `app.update.ts`, which is why nothing here is
 * async and nothing here touches the DOM.
 */
import { Html } from "foldkit";

import { type Elements } from "./element.base-wc.ts";
import { isTerminal, ringToken, statusToken, type Task } from "./model.ts";
import { AppMessage } from "./app.message.ts";
import { AppRoute } from "./app.route.ts";
import type { Model } from "./app.model.ts";
import * as Tasks from "./task.ts";

/** The kind chip: "Task" or "CR", tinted by kind. */
export const kindChip = <M>(h: Html.HtmlBuilder<M>, task: Task): Html.Html =>
  h.span([h.Class("gp-kind"), h.DataAttribute("kind", task.kind)], [task.kind]);

/** The full status pill, used on detail screens. */
export const statusPill = <M>(h: Html.HtmlBuilder<M>, task: Task): Html.Html => {
  const token = statusToken(task.status);
  return h.span(
    [
      h.Class("gp-status"),
      h.Style({
        "--gp-status-color": `var(--gp-${token})`,
        "--gp-status-rgb": `var(--gp-${token}-rgb)`,
      }),
    ],
    [task.status],
  );
};

const SEGMENTS: readonly { readonly value: Tasks.Filter; readonly label: string }[] = [
  { value: "all", label: "All" },
  { value: "tasks", label: "Tasks" },
  { value: "crs", label: "Change Requests" },
];

/** The selector `CloseDialog` is given; the dialog is found by it, not held. */
export const NEW_TASK_DIALOG = "ui-dialog.gp-new-task";

const row = (h: Html.HtmlBuilder<AppMessage>, task: Task, depth: number): Html.Html =>
  h.button(
    [
      h.Class("gp-task-row"),
      h.Type("button"),
      h.DataAttribute("depth", String(depth)),
      h.OnClick(AppMessage.ClickedTask({ id: task.id })),
    ],
    [
      depth > 0 ? h.span([h.Class("gp-task-glyph"), h.AriaHidden(true)], ["└"]) : h.empty,
      h.span(
        [
          h.Class("gp-status-ring"),
          h.Style({ "--gp-status-color": `var(--gp-${ringToken(task.status)})` }),
          ...(isTerminal(task.status) ? [h.DataAttribute("filled", "")] : []),
          h.AriaHidden(true),
        ],
        [],
      ),
      kindChip(h, task),
      h.span([h.Class("gp-id")], [task.id]),
      h.span([h.Class("gp-task-title")], [task.title]),
      h.span(
        [h.Class("gp-task-meta")],
        [
          h.span([h.Class("gp-task-updated")], [`updated ${task.updated}`]),
          h.span(
            [
              h.Class("gp-status-quiet"),
              h.Style({ "--gp-status-color": `var(--gp-${statusToken(task.status)})` }),
            ],
            [task.status],
          ),
          h.span([h.Class("gp-avatar")], [task.avatar]),
        ],
      ),
    ],
  );

/**
 * The "New task" dialog.
 *
 * `ui-dialog` owns the modality — trigger wiring, focus trap, Escape and
 * outside-press dismissal — and this owns only the form. The fields are Model
 * values rather than DOM state, so the composer survives a re-render and the
 * submit reads what the reader typed from the same place the view drew it.
 */
const newTask = (
  h: Html.HtmlBuilder<AppMessage>,
  ui: Elements<AppMessage>,
  model: Model,
): Html.Html =>
  ui.dialog(
    [h.Class("gp-new-task")],
    [
      h.button(
        [h.Class("gp-btn-primary"), h.DataAttribute("dialog-trigger", ""), h.Type("button")],
        ["New task"],
      ),
      ui.dialogPopup(
        [h.Class("gp-dialog")],
        [
          h.h2([h.Class("gp-dialog-title"), h.DataAttribute("dialog-title", "")], ["New task"]),
          h.p(
            [h.Class("gp-dialog-hint"), h.DataAttribute("dialog-description", "")],
            [
              "Signed with this browser's key and appended to the repository's hub. If the repository refuses the key — it may not be a member yet — the task stays in this tab instead.",
            ],
          ),
          h.form(
            [h.OnSubmit(AppMessage.SubmittedNewTask())],
            [
              h.label([h.Class("gp-field-label"), h.For("gp-new-title")], ["Title"]),
              h.input([
                h.Id("gp-new-title"),
                h.Class("gp-input"),
                h.Name("title"),
                h.Required(true),
                h.Autocomplete("off"),
                h.Placeholder("What needs doing?"),
                h.Value(model.tasksScreen.title),
                h.OnInput((value) => AppMessage.ChangedNewTaskTitle({ title: value })),
              ]),
              h.label([h.Class("gp-field-label"), h.For("gp-new-parent")], ["Belongs to"]),
              h.select(
                [
                  h.Id("gp-new-parent"),
                  h.Class("gp-input"),
                  h.Name("parent"),
                  h.Value(model.tasksScreen.parent),
                  h.OnChange((value) => AppMessage.ChangedNewTaskParent({ parent: value })),
                ],
                [
                  h.option([h.Value("")], ["Nothing"]),
                  ...model.tasks.tasks.map((candidate) =>
                    h.option([h.Value(candidate.id)], [`${candidate.id} — ${candidate.title}`]),
                  ),
                ],
              ),
              h.label([h.Class("gp-field-label"), h.For("gp-new-desc")], ["Description"]),
              h.textarea([
                h.Id("gp-new-desc"),
                h.Class("gp-textarea"),
                h.Name("desc"),
                h.Rows(4),
                h.Placeholder("Context, constraints, links…"),
                h.Value(model.tasksScreen.desc),
                h.OnInput((value) => AppMessage.ChangedNewTaskDescription({ desc: value })),
              ]),
              h.div(
                [h.Class("gp-dialog-actions")],
                [
                  h.button(
                    [
                      h.Class("gp-btn-quiet"),
                      h.Type("button"),
                      h.OnClick(AppMessage.ClickedCancelNewTask()),
                    ],
                    ["Cancel"],
                  ),
                  h.button([h.Class("gp-btn-primary"), h.Type("submit")], ["Create task"]),
                ],
              ),
            ],
          ),
        ],
      ),
    ],
  );

export const view = (
  model: Model,
  h: Html.HtmlBuilder<AppMessage>,
  ui: Elements<AppMessage>,
): Html.Html => {
  const groups = Tasks.groups(model.tasks.tasks, model.tasksScreen.filter);
  const empty = groups.every((group) => group.rows.length === 0);
  return h.div(
    [h.Class("gp-screen")],
    [
      model.tasks.liveNotice === null
        ? h.empty
        : h.p([h.Class("gp-notice"), h.DataAttribute("error", "")], [model.tasks.liveNotice]),
      h.div(
        [h.Class("gp-tasks-head")],
        [
          h.h1([h.Class("gp-heading")], ["Tasks"]),
          ui.toggleGroup(
            [
              h.Class("gp-segmented"),
              h.AriaLabel("Filter by kind"),
              ui.toggleGroup.OnChange(({ value }) =>
                // A single-select group allows deselection; no segment is "all".
                AppMessage.ChangedTaskFilter({
                  filter: value === "tasks" || value === "crs" ? value : "all",
                }),
              ),
            ],
            SEGMENTS.map((segment) =>
              ui.toggle(
                [
                  h.Class("gp-segment"),
                  // An attribute, not a property: `ui-toggle` exposes `value`
                  // as a getter over its own attribute, and writing the
                  // property throws.
                  h.Attribute("value", segment.value),
                  ...(model.tasksScreen.filter === segment.value
                    ? [h.DataAttribute("active", "")]
                    : []),
                ],
                [segment.label],
              ),
            ),
          ),
          newTask(h, ui, model),
        ],
      ),
      ...(empty
        ? [h.div([h.Class("gp-empty")], ["Nothing here."])]
        : groups.flatMap((group) => [
            group.milestone === null
              ? h.empty
              : h.button(
                  [
                    h.Class("gp-milestone-head"),
                    h.Type("button"),
                    h.OnClick(
                      AppMessage.ClickedNavigate({
                        route: AppRoute.Detail({ id: group.milestone?.id ?? "" }),
                      }),
                    ),
                  ],
                  [
                    h.span([h.Class("gp-milestone-title")], [group.milestone.title]),
                    h.span(
                      [h.Class("gp-milestone-count")],
                      [
                        `${String(group.rows.filter(({ task }) => !isTerminal(task.status)).length)} open`,
                      ],
                    ),
                  ],
                ),
            h.div(
              [h.Class("gp-task-list")],
              group.rows.map(({ task, depth }) => row(h, task, depth)),
            ),
          ])),
    ],
  );
};
