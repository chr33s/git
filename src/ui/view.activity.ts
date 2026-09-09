/**
 * Activity — a window of repository history as a calendar timeline.
 *
 * The design went through two shapes here: a plain event feed first, then the
 * calendar-style timeline the user supplied as a reference. This is the
 * second: a grid of day columns with a "now" marker on today.
 *
 * What sits on the grid depends on what the server can answer. Commits are
 * real, and a commit is a point in time rather than a span, so each occupies
 * its own day column and stacks with the others from that day; the design's
 * spanning bars belonged to Tasks, which have no HTTP surface yet. When the
 * API cannot be reached the screen falls back to the design's own Task
 * timeline and says so.
 *
 * The header controls work the same in both modes: the zoom segments set how
 * many days the window spans and ‹ › page it back and forth. All of that
 * arithmetic is in `activity.ts`; this only draws what it answers.
 */
import { AsyncData, Html } from "foldkit";

import type { Elements } from "./element.base-wc.ts";
import { byId, timeline } from "./fixtures.ts";
import { statusToken, type Task } from "./model.ts";
import { ago, initials } from "./time.ts";
import { AppMessage } from "./app.message.ts";
import type { Model, TimelineCommit } from "./app.model.ts";
import * as Activity from "./activity.ts";

const ZOOMS: readonly Activity.Zoom[] = ["day", "week", "month"];

/** One real commit, in its own day column. */
const commitCard = (
  h: Html.HtmlBuilder<AppMessage>,
  commit: TimelineCommit,
  at: Activity.Placement,
): Html.Html =>
  h.div(
    [
      h.Class("gp-cal-event"),
      h.DataAttribute("point", ""),
      h.Style({
        "grid-column": `${String(at.column)} / span 1`,
        "grid-row": String(at.row),
        "--gp-status-color": "var(--gp-accent)",
      }),
      h.Title(`${commit.subject} — ${commit.author}`),
    ],
    [
      h.span(
        [h.Class("gp-cal-event-body")],
        [
          h.span([h.Class("gp-cal-event-title")], [commit.subject]),
          h.span(
            [h.Class("gp-cal-event-meta")],
            [`${commit.oid.slice(0, 7)} · ${initials(commit.author)} · ${ago(commit.at)}`],
          ),
        ],
      ),
    ],
  );

/** A fixture Task, laid out as the design draws it. */
const taskCard = (
  h: Html.HtmlBuilder<AppMessage>,
  task: Task,
  at: Activity.Placement,
  epic: boolean,
): Html.Html =>
  h.button(
    [
      h.Class("gp-cal-event"),
      h.Type("button"),
      ...(epic ? [h.DataAttribute("epic", "")] : []),
      h.Style({
        "grid-column": `${String(at.column)} / span ${String(at.span)}`,
        "grid-row": String(at.row),
        "--gp-status-color": `var(--gp-${statusToken(task.status)})`,
      }),
      h.OnClick(AppMessage.ClickedTask({ id: task.id })),
    ],
    [
      h.span(
        [h.Class("gp-cal-event-body")],
        [
          h.span([h.Class("gp-cal-event-title")], [task.title]),
          h.span(
            [h.Class("gp-cal-event-meta")],
            [epic ? `${task.id} · Epic · ${task.status}` : `${task.id} · ${task.status}`],
          ),
        ],
      ),
      epic
        ? h.span(
            [h.Class("gp-cal-avatars")],
            task.assignees.map((person) => h.span([h.Class("gp-avatar")], [person.avatar])),
          )
        : h.empty,
    ],
  );

/** The design's timeline, which is what an unreachable server leaves showing. */
const fixtureCards = (
  h: Html.HtmlBuilder<AppMessage>,
  zoom: Activity.Zoom,
  offset: number,
): readonly Html.Html[] => {
  const cards: Html.Html[] = [];
  for (const event of timeline) {
    const task = byId.get(event.id);
    if (task === undefined) continue;
    const at = Activity.placeFixture(zoom, offset, event);
    if (at === null) continue;
    cards.push(taskCard(h, task, at, event.epic === true));
  }
  return cards;
};

/**
 * The hub's sessions: what each agent was told and what came of it.
 *
 * Absent entirely until the hub answers — provenance is not something to fake
 * with fixtures.
 */
const sessions = (h: Html.HtmlBuilder<AppMessage>, model: Model): Html.Html => {
  if (model.tasks.sessions.length === 0) return h.empty;
  return h.div(
    [],
    [
      h.h2([h.Class("gp-section-label")], ["Sessions"]),
      h.div(
        [h.Class("gp-panel-card gp-sessions")],
        model.tasks.sessions.map((session) =>
          h.keyed("div")(
            session.id,
            [h.Class("gp-list-row")],
            [
              h.span([h.Class("gp-sha")], [session.id.slice(0, 8)]),
              h.span([], [session.agent]),
              h.span(
                [h.Class("gp-when")],
                [
                  `${String(session.commits)} commit${session.commits === 1 ? "" : "s"}`,
                  session.pulls.length > 0 ? ` · ${String(session.pulls.length)} PR(s)` : "",
                  session.openDecisions > 0
                    ? ` · ${String(session.openDecisions)} open question(s)`
                    : "",
                  session.tokens > 0 ? ` · ${String(session.tokens)} tokens` : "",
                ],
              ),
            ],
          ),
        ),
      ),
    ],
  );
};

export const view = (
  model: Model,
  h: Html.HtmlBuilder<AppMessage>,
  ui: Elements<AppMessage>,
): Html.Html => {
  const { zoom, offset } = model.activityScreen;
  const live = AsyncData.isSuccess(model.activityScreen.commits);
  const days = Activity.daysOf(zoom, live, offset);
  const now = Activity.nowOffset(zoom, live, offset);

  const cards = AsyncData.match(model.activityScreen.commits, {
    onIdle: () => [h.div([h.Class("gp-empty")], ["Loading history…"])],
    onLoading: () => [h.div([h.Class("gp-empty")], ["Loading history…"])],
    onRefreshing: () => [h.div([h.Class("gp-empty")], ["Loading history…"])],
    onSuccess: (commits) => {
      const placed = Activity.placeCommits(commits, zoom, offset);
      return placed.length === 0
        ? [h.div([h.Class("gp-empty")], ["No commits in this window."])]
        : placed.map(([commit, at]) => commitCard(h, commit, at));
    },
    onStale: ({ data }) => {
      const placed = Activity.placeCommits(data, zoom, offset);
      return placed.map(([commit, at]) => commitCard(h, commit, at));
    },
    onFailure: () => {
      const drawn = fixtureCards(h, zoom, offset);
      return drawn.length === 0
        ? [h.div([h.Class("gp-empty")], ["No events in this window."])]
        : drawn;
    },
  });

  return h.div(
    [h.Class("gp-screen"), h.Style({ "--gp-cal-cols": String(Activity.SPANS[zoom]) })],
    [
      h.div(
        [h.Class("gp-activity-head")],
        [
          h.h1([h.Class("gp-heading")], ["Activity"]),
          ui.toggleGroup(
            [
              h.Class("gp-segmented"),
              h.AriaLabel("Timeline zoom"),
              ui.toggleGroup.OnChange(({ value }) =>
                value === "day" || value === "week" || value === "month"
                  ? AppMessage.ChangedTimelineZoom({ zoom: value })
                  : AppMessage.ChangedTimelineZoom({ zoom }),
              ),
            ],
            ZOOMS.map((level) =>
              ui.toggle(
                [
                  h.Class("gp-segment"),
                  h.Attribute("value", level),
                  ...(zoom === level ? [h.DataAttribute("active", "")] : []),
                ],
                [level.charAt(0).toUpperCase() + level.slice(1)],
              ),
            ),
          ),
          h.div(
            [h.Class("gp-range")],
            [
              h.button(
                [
                  h.Type("button"),
                  h.AriaLabel("Earlier"),
                  h.OnClick(AppMessage.ClickedTimelineEarlier()),
                ],
                ["‹"],
              ),
              Activity.rangeOf(zoom, live, offset),
              h.button(
                [
                  h.Type("button"),
                  h.AriaLabel("Later"),
                  h.Disabled(offset === 0),
                  h.OnClick(AppMessage.ClickedTimelineLater()),
                ],
                ["›"],
              ),
            ],
          ),
        ],
      ),

      AsyncData.match(model.activityScreen.commits, {
        onIdle: () => h.empty,
        onLoading: () => h.empty,
        onRefreshing: () => h.empty,
        onSuccess: () => h.empty,
        onStale: () => h.empty,
        onFailure: (reason) =>
          h.p([h.Class("gp-notice")], [`Showing the design's sample timeline — ${reason}.`]),
      }),

      sessions(h, model),

      h.div(
        [h.Class("gp-cal-head")],
        [
          h.div([h.Class("gp-cal-month")], [Activity.monthOf(zoom, live, offset)]),
          ...days.map((day) =>
            h.div(
              [h.Class("gp-cal-day"), ...(day.today ? [h.DataAttribute("today", "")] : [])],
              [day.letter, " ", h.strong([], [String(day.date)])],
            ),
          ),
        ],
      ),

      h.div(
        [h.Class("gp-cal-body")],
        [
          ...(now === null
            ? []
            : [
                h.div([h.Class("gp-cal-now"), h.Style({ left: now }), h.AriaHidden(true)]),
                h.div([h.Class("gp-cal-now-dot"), h.Style({ left: now }), h.AriaHidden(true)]),
              ]),
          h.div([h.Class("gp-cal-grid")], cards),
        ],
      ),
    ],
  );
};
