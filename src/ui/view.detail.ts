/**
 * Task / Change Request detail.
 *
 * One screen for both, because a Change Request is a Task with a diff
 * attached: the title, description, subtasks, discussion and meta sidebar are
 * the Task half and always render; the refs, tabs, commits, checks and merge
 * state are the Change Request half and appear only when there is a proposed
 * change.
 *
 * The Diff tab is wired to the live server, and `@pierre/diffs` draws it
 * through a Mount — the library owns that subtree, so the view declares an
 * empty host and names the file. When the refs are not in the repository (the
 * fixture Change Requests name branches that only exist in the design) the tab
 * falls back to the design's own diff and says so.
 */
import { AsyncData, Html } from "foldkit";

import type { Elements } from "./element.base-wc.ts";
import * as icon from "./icon.ts";
import { type ChangeRequest, isChangeRequest, type Task, type Thread } from "./model.ts";
import { AppMessage } from "./app.message.ts";
import { AppRoute } from "./app.route.ts";
import type { Model } from "./app.model.ts";
import { PierreDiff } from "./mount.pierre-diff.ts";
import { kindChip, statusPill } from "./view.tasks.ts";
import * as Tasks from "./task.ts";

type H = Html.HtmlBuilder<AppMessage>;

const TABS = ["conversation", "diff", "commits", "checks"] as const;

/** Only reachable if the fixtures are emptied; keeps the screen total. */
const EMPTY: Task = {
  id: "—",
  kind: "Task",
  title: "Not found",
  status: "Todo",
  avatar: "··",
  desc: "",
  assignees: [],
  labels: [],
  comments: [],
  updated: "",
};

/**
 * Which task this route is about.
 *
 * An id the list does not hold shows the placeholder, not some other task: a
 * freshly opened Change Request is one the projection has not delivered yet,
 * and drawing a different task under its address would be a lie the reader
 * cannot see through.
 */
const taskOf = (model: Model): Task => {
  const id = model.route._tag === "Detail" ? model.route.id : "";
  return Tasks.byId(model.tasks.tasks, id) ?? EMPTY;
};

/**
 * Every task below this one, so the move control cannot offer a descendant.
 *
 * Cycle-guarded like `ancestorsOf`, and for the reason given there: this list
 * also holds fixtures and tab-local moves that never went near the hub, and a
 * walk that trusted the data would hang the tab rather than misdraw one row.
 */
const descendants = (
  tasks: readonly Task[],
  task: Task,
  seen: Set<string> = new Set(),
): readonly string[] => {
  const out: string[] = [];
  for (const childId of task.children ?? []) {
    if (seen.has(childId)) continue;
    seen.add(childId);
    out.push(childId);
    const child = Tasks.byId(tasks, childId);
    if (child !== undefined) out.push(...descendants(tasks, child, seen));
  }
  return out;
};

const comment = (
  h: H,
  entry: {
    readonly avatar: string;
    readonly author: string;
    readonly when: string;
    readonly text: string;
  },
): Html.Html =>
  h.div(
    [h.Class("gp-comment")],
    [
      h.span([h.Class("gp-avatar"), h.DataAttribute("size", "lg")], [entry.avatar]),
      h.div(
        [],
        [
          h.div(
            [h.Class("gp-comment-head")],
            [
              h.span([h.Class("gp-comment-author")], [entry.author]),
              h.span([h.Class("gp-comment-when")], [entry.when]),
            ],
          ),
          h.div([h.Class("gp-comment-body")], [entry.text]),
        ],
      ),
    ],
  );

const diffPanel = (h: H, model: Model, cr: ChangeRequest): Html.Html => {
  const state = model.detailScreen.diff;
  const mine = model.detailScreen.diffFor === cr.id;
  if (!mine || AsyncData.isLoading(state) || AsyncData.isIdle(state)) {
    return h.div([h.Class("gp-panel-card")], [h.div([h.Class("gp-empty")], ["Loading diff…"])]);
  }

  const held = AsyncData.getData(state);
  if (held._tag === "Some") {
    const files = held.value;
    if (files.length === 0) {
      return h.div(
        [h.Class("gp-panel-card")],
        [h.div([h.Class("gp-empty")], ["No textual changes."])],
      );
    }
    return h.div(
      [],
      files.map((file) =>
        h.div(
          [h.Class("gp-panel-card")],
          [
            h.div(
              [h.Class("gp-diff-file-head")],
              [
                h.span([], [file.path]),
                h.span(
                  [
                    h.Class("gp-diff-state"),
                    h.Style({
                      "--gp-status-color": `var(--gp-${file.status === "removed" ? "red" : file.status === "added" ? "accent" : "amber"})`,
                    }),
                  ],
                  [file.status],
                ),
              ],
            ),
            // The library owns this subtree; Foldkit owns its lifetime. The
            // key is everything the renderer was built from, because Mount
            // arguments are captured once at insert: a re-render with a new
            // palette or a re-fetched side would otherwise leave the old
            // instance in place, painting the previous theme and the previous
            // diff under the current one's heading.
            h.keyed("div")(
              `${file.path}:${model.theme}:${String(file.oldContents?.length ?? -1)}:${String(file.newContents?.length ?? -1)}`,
              [
                h.Class("gp-diff-host"),
                h.DataAttribute("diff-host", file.path),
                h.OnMount(
                  PierreDiff({
                    path: file.path,
                    oldContents: file.oldContents,
                    newContents: file.newContents,
                    theme: model.theme,
                  }),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  // The fixture rendering: the design's own diff, line for line.
  const reason = AsyncData.getError(state);
  return h.div(
    [],
    [
      reason._tag === "Some"
        ? h.p([h.Class("gp-notice")], [`Showing the design's sample diff — ${reason.value}.`])
        : h.empty,
      h.div(
        [h.Class("gp-panel-card")],
        [
          h.div([h.Class("gp-diff-file-head")], [h.span([], [cr.diffFile])]),
          h.div(
            [h.Class("gp-diff-static")],
            // Only the text span preserves whitespace, and the row is a flex
            // container: a template's own newlines would be whitespace-only
            // nodes there, dropped rather than rendered as leading spaces.
            cr.diff.map((line) =>
              h.div(
                [h.Class("gp-diff-line"), h.DataAttribute("kind", line.kind)],
                [
                  h.span([h.Class("gp-diff-num")], [String(line.n)]),
                  h.span([h.Class("gp-diff-text")], [line.text]),
                ],
              ),
            ),
          ),
        ],
      ),
    ],
  );
};

const review = (h: H, model: Model, cr: ChangeRequest): Html.Html => {
  const state = cr.review.merged === true ? "merged" : cr.review.ok ? "ready" : "blocked";
  const acting = model.detailScreen.acting;
  return h.div(
    [],
    [
      h.div(
        [h.Class("gp-review")],
        [
          h.span(
            [h.Class("gp-review-dot"), ...(cr.review.ok ? [h.DataAttribute("ok", "")] : [])],
            [],
          ),
          h.div(
            [],
            [
              h.div([h.Class("gp-review-headline")], [cr.review.headline]),
              h.div([h.Class("gp-review-detail")], [cr.review.detail]),
            ],
          ),
          ...(cr.hub === true && cr.review.merged !== true
            ? [
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.Title("Approve the proposed revision, signed with this browser's key"),
                    h.Disabled(acting),
                    h.OnClick(AppMessage.ClickedReview({ decision: "approve" })),
                  ],
                  ["Approve"],
                ),
                h.button(
                  [
                    h.Class("gp-btn-quiet"),
                    h.Type("button"),
                    h.Title("Request changes to the proposed revision"),
                    h.Disabled(acting),
                    h.OnClick(AppMessage.ClickedReview({ decision: "reject" })),
                  ],
                  ["Request changes"],
                ),
              ]
            : []),
          h.button(
            [
              h.Class("gp-merge-btn"),
              h.Type("button"),
              h.DataAttribute("state", state),
              h.Disabled(state !== "ready" || acting),
              h.OnClick(AppMessage.ClickedMerge()),
            ],
            [cr.review.action],
          ),
        ],
      ),
      model.detailScreen.notice === null
        ? h.empty
        : h.p(
            [h.Class("gp-notice"), h.Role("alert"), h.DataAttribute("error", "")],
            [model.detailScreen.notice],
          ),
    ],
  );
};

const tab = (
  h: H,
  model: Model,
  id: (typeof TABS)[number],
  label: readonly (Html.Html | string)[],
): Html.Html =>
  h.button(
    [
      h.Class("gp-tab"),
      h.Type("button"),
      h.DataAttribute("tab", ""),
      h.Attribute("value", id),
      ...(model.detailScreen.tab === id ? [h.DataAttribute("selected", "")] : []),
      h.OnClick(AppMessage.ChangedDetailTab({ tab: id })),
    ],
    label,
  );

const changeRequest = (
  h: H,
  ui: Elements<AppMessage>,
  model: Model,
  cr: ChangeRequest,
): Html.Html => {
  const panel = (id: (typeof TABS)[number], body: Html.Html): Html.Html =>
    h.div(
      [
        h.Class("gp-tabpanel"),
        h.DataAttribute("tab-panel", ""),
        h.Attribute("value", id),
        h.Hidden(model.detailScreen.tab !== id),
      ],
      [body],
    );
  return h.div(
    [],
    [
      h.div(
        [h.Class("gp-refs")],
        [
          h.span([h.Class("gp-ref"), h.DataAttribute("source", "")], [cr.sourceRef]),
          icon.arrowRight(h),
          h.span([h.Class("gp-ref")], [cr.targetRef]),
        ],
      ),
      ui.tabs(
        [
          h.Class("gp-tabs"),
          // An attribute: `ui-tabs` reflects its selection there, and the
          // element reads it back on connect. A property alone would leave
          // the DOM disagreeing with the Model about which panel is open.
          h.Attribute("value", model.detailScreen.tab),
          // And the element's own `change` comes back, because arrow-key
          // activation moves the selection without any button being clicked.
          ui.tabs.OnChange(({ value }) =>
            value === "conversation" ||
            value === "diff" ||
            value === "commits" ||
            value === "checks"
              ? AppMessage.ChangedDetailTab({ tab: value })
              : AppMessage.ChangedDetailTab({ tab: model.detailScreen.tab }),
          ),
        ],
        [
          ui.tabList(
            [h.Class("gp-tablist")],
            [
              tab(h, model, "conversation", ["Conversation"]),
              tab(h, model, "diff", ["Diff ", h.span([h.Class("gp-tab-count")], [cr.diffStat])]),
              tab(h, model, "commits", [
                "Commits ",
                h.span([h.Class("gp-tab-count")], [cr.commitCount]),
              ]),
              tab(h, model, "checks", ["Checks"]),
            ],
          ),
          panel("conversation", review(h, model, cr)),
          panel("diff", diffPanel(h, model, cr)),
          panel(
            "commits",
            h.div(
              [h.Class("gp-panel-card")],
              cr.commits.map((held) =>
                h.div(
                  [h.Class("gp-list-row")],
                  [
                    h.span([h.Class("gp-sha")], [held.sha]),
                    h.span([], [held.msg]),
                    h.span([h.Class("gp-when")], [held.when]),
                  ],
                ),
              ),
            ),
          ),
          panel(
            "checks",
            h.div(
              [h.Class("gp-panel-card")],
              cr.checks.map((check) =>
                h.div(
                  [h.Class("gp-list-row")],
                  [
                    h.span(
                      [h.Class("gp-check-dot"), ...(check.ok ? [h.DataAttribute("ok", "")] : [])],
                      [],
                    ),
                    h.span([], [check.name]),
                    h.span([h.Class("gp-when")], [check.detail]),
                  ],
                ),
              ),
            ),
          ),
        ],
      ),
    ],
  );
};

const subtasks = (h: H, model: Model, task: Task): Html.Html => {
  const children = (task.children ?? [])
    .map((id) => Tasks.byId(model.tasks.tasks, id))
    .filter((child): child is Task => child !== undefined);
  if (children.length === 0) return h.empty;
  return h.div(
    [],
    [
      h.h2([h.Class("gp-section-label")], ["Subtasks"]),
      h.div(
        [h.Class("gp-panel-card")],
        children.map((child) =>
          h.button(
            [
              h.Class("gp-subtask-row"),
              h.Type("button"),
              h.OnClick(AppMessage.ClickedTask({ id: child.id })),
            ],
            [
              kindChip(h, child),
              h.span([h.Class("gp-id")], [child.id]),
              h.span([h.Class("gp-subtask-title")], [child.title]),
              statusPill(h, child),
            ],
          ),
        ),
      ),
    ],
  );
};

/**
 * A hub task's lease, and its end: claim it, let it go, close it. The fixture
 * tasks have no lease to speak of, so they show nothing here.
 */
const lifecycle = (h: H, model: Model, task: Task): Html.Html => {
  if (task.hub !== true || task.kind !== "Task" || task.status === "Done") return h.empty;
  const claimed = task.status === "In progress";
  const acting = model.detailScreen.acting;
  const verb = (label: string, action: "claim" | "release" | "complete" | "abandon"): Html.Html =>
    h.button(
      [
        h.Class("gp-btn-quiet"),
        h.Type("button"),
        h.Disabled(acting),
        h.OnClick(AppMessage.ClickedTaskAction({ action })),
      ],
      [label],
    );
  return h.div(
    [],
    [
      h.div(
        [h.Class("gp-panel-card gp-task-actions")],
        [
          h.span([h.Class("gp-field-label")], [claimed ? "Claimed" : "Available"]),
          verb(claimed ? "Release" : "Claim", claimed ? "release" : "claim"),
          verb("Complete", "complete"),
          verb("Abandon", "abandon"),
        ],
      ),
      model.detailScreen.taskNotice === null
        ? h.empty
        : h.p([h.Class("gp-notice"), h.Role("alert")], [model.detailScreen.taskNotice]),
    ],
  );
};

/**
 * One review thread: its conversation, its state, and the two verbs a reader
 * has — answer it, or settle it. Hub Change Requests only; the fixtures keep
 * the flat discussion they were designed with.
 */
const threadCard = (h: H, model: Model, thread: Thread): Html.Html =>
  h.div(
    [
      h.Class("gp-panel-card gp-thread"),
      ...(thread.resolved ? [h.DataAttribute("resolved", "")] : []),
    ],
    [
      h.div(
        [h.Class("gp-thread-head")],
        [
          h.span([h.Class("gp-thread-path")], [thread.path ?? "conversation"]),
          h.span([h.Class("gp-thread-state")], [thread.resolved ? "resolved" : "open"]),
          h.button(
            [
              h.Class("gp-btn-quiet"),
              h.Type("button"),
              h.Disabled(model.detailScreen.acting),
              h.OnClick(
                AppMessage.ClickedThreadResolve({
                  thread: thread.id,
                  resolved: !thread.resolved,
                }),
              ),
            ],
            [thread.resolved ? "Reopen" : "Resolve"],
          ),
        ],
      ),
      ...thread.comments.map((entry) => comment(h, entry)),
      h.form(
        [
          h.Class("gp-thread-reply"),
          h.OnSubmit(AppMessage.SubmittedThreadReply({ thread: thread.id })),
        ],
        [
          h.input([
            h.Class("gp-input"),
            h.Name("reply"),
            h.Placeholder("Reply in this thread…"),
            h.Autocomplete("off"),
            h.Value(model.detailScreen.replies[thread.id] ?? ""),
            h.OnInput((text) => AppMessage.ChangedThreadDraft({ thread: thread.id, text })),
          ]),
        ],
      ),
    ],
  );

/**
 * The comment form — or, for a live hub Task, the honest absence of one.
 *
 * Task and pull-request ids share one shape, so a task comment written into
 * the pull-request namespace would *create* a ghost Change Request ref; and a
 * tab-local comment on live repository state would vanish on reload while
 * looking canonical. Until a task-comment event exists in the protocol, saying
 * so beats either lie.
 */
const commentForm = (h: H, model: Model, task: Task): Html.Html => {
  if (task.hub === true && !isChangeRequest(task)) {
    return h.p(
      [h.Class("gp-notice")],
      [
        "Live task discussion is not part of the hub protocol yet — there is no signed task-comment event for this browser to append, so commenting is off rather than written somewhere it does not belong.",
      ],
    );
  }
  return h.form(
    [h.Class("gp-comment-form"), h.OnSubmit(AppMessage.SubmittedComment())],
    [
      h.textarea([
        h.Class("gp-textarea"),
        h.Name("text"),
        h.Rows(3),
        h.Required(true),
        h.Placeholder("Leave a comment…"),
        h.AriaLabel("Leave a comment"),
        h.Value(model.detailScreen.comment),
        h.OnInput((text) => AppMessage.ChangedCommentDraft({ text })),
      ]),
      h.div(
        [h.Class("gp-comment-actions")],
        [h.button([h.Class("gp-btn-primary"), h.Type("submit")], ["Comment"])],
      ),
    ],
  );
};

const meta = (h: H, model: Model, task: Task): Html.Html => {
  // One chain, outermost first. Rendering the release and the parent as two
  // rows said the same thing twice for anything filed straight under a
  // release, and claimed two relationships where the hub records one edge.
  const chain = Tasks.ancestorsOf(model.tasks.tasks, task);
  // Nothing may be filed under its own descendants, and nothing under itself —
  // offering either would only earn a refusal from the hub.
  const under = new Set<string>([task.id, ...descendants(model.tasks.tasks, task)]);
  return h.aside(
    [h.Class("gp-meta")],
    [
      h.div(
        [],
        [
          h.div([h.Class("gp-meta-label")], ["Assignees"]),
          ...task.assignees.map((person) =>
            h.div(
              [h.Class("gp-assignee")],
              [
                h.span([h.Class("gp-avatar"), h.DataAttribute("size", "sm")], [person.avatar]),
                h.span([], [person.name]),
              ],
            ),
          ),
        ],
      ),
      h.div(
        [],
        [
          h.div([h.Class("gp-meta-label")], ["Labels"]),
          h.div(
            [h.Class("gp-labels")],
            task.labels.map((label) =>
              h.span(
                [h.Class("gp-label"), h.Style({ "--gp-label-color": `var(--gp-${label.hue})` })],
                [label.name],
              ),
            ),
          ),
        ],
      ),
      h.div(
        [],
        [
          h.div([h.Class("gp-meta-label")], ["Belongs to"]),
          chain.length === 0
            ? h.div([h.Class("gp-meta-value")], ["—"])
            : h.div(
                [h.Class("gp-crumbs")],
                chain.flatMap((ancestor, index) => [
                  ...(index === 0
                    ? []
                    : [h.span([h.Class("gp-crumb-sep"), h.AriaHidden(true)], ["›"])]),
                  h.button(
                    [
                      h.Class("gp-parent-link"),
                      h.Type("button"),
                      h.Title(ancestor.id),
                      h.OnClick(AppMessage.ClickedTask({ id: ancestor.id })),
                    ],
                    [ancestor.title],
                  ),
                ]),
              ),
          h.select(
            [
              h.Class("gp-meta-select"),
              h.AriaLabel("Move this task"),
              h.Disabled(model.detailScreen.acting),
              h.Value(task.parent ?? ""),
              h.OnChange((parent) => AppMessage.ChangedTaskParent({ parent })),
            ],
            [
              h.option([h.Value("")], ["Belongs to nothing"]),
              ...model.tasks.tasks
                .filter((candidate) => !under.has(candidate.id))
                .map((candidate) =>
                  h.option([h.Value(candidate.id)], [`${candidate.id} — ${candidate.title}`]),
                ),
            ],
          ),
          model.detailScreen.moveNotice === null
            ? h.empty
            : h.p([h.Class("gp-notice"), h.Role("alert")], [model.detailScreen.moveNotice]),
        ],
      ),
    ],
  );
};

export const view = (model: Model, h: H, ui: Elements<AppMessage>): Html.Html => {
  const task = taskOf(model);
  const cr = isChangeRequest(task) ? task : null;
  return h.div(
    [h.Class("gp-screen gp-screen--flush")],
    [
      h.div(
        [h.Class("gp-detail")],
        [
          h.div(
            [h.Class("gp-detail-main")],
            [
              h.button(
                [
                  h.Class("gp-back"),
                  h.Type("button"),
                  h.OnClick(AppMessage.ClickedNavigate({ route: AppRoute.Tasks() })),
                ],
                [icon.chevronLeft(h), " Tasks"],
              ),
              h.div(
                [h.Class("gp-detail-eyebrow")],
                [kindChip(h, task), h.span([h.Class("gp-id")], [task.id]), statusPill(h, task)],
              ),
              h.h1([h.Class("gp-detail-title")], [task.title]),
              h.p([h.Class("gp-detail-desc")], [task.desc]),
              cr === null ? h.empty : changeRequest(h, ui, model, cr),
              subtasks(h, model, task),
              lifecycle(h, model, task),
              h.h2([h.Class("gp-section-label")], ["Discussion"]),
              ...(task.threads !== undefined && task.threads.length > 0
                ? task.threads.map((thread) => threadCard(h, model, thread))
                : task.comments.map((entry) => comment(h, entry))),
              commentForm(h, model, task),
            ],
          ),
          meta(h, model, task),
        ],
      ),
    ],
  );
};
