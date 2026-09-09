/**
 * Every state transition the application makes.
 *
 * Pure, and exhaustive: `Message.match` will not compile with a case missing,
 * so a Message that can be dispatched is a Message something here answers for.
 * No network, no storage, no DOM — those are the Commands this returns, and
 * keeping them out is what makes a transition replayable in DevTools and
 * testable without a browser.
 *
 * `evo` is not used: these Models are small enough that a spread says the same
 * thing with one less concept in the file.
 */
import { AsyncData, Navigation, Update } from "foldkit";

import { AppMessage } from "./app.message.ts";
import { authorName, type Model } from "./app.model.ts";
import { isChangeRequest, type Task } from "./model.ts";
import { AppRoute, urlOf } from "./app.route.ts";
import { CreateTask, Grep, LoadTasks, MoveTask } from "./app.command.ts";
import {
  CommentRemote,
  HydrateDetail,
  LoadDiff,
  MergeFixture,
  MergeRemote,
  ReviewRemote,
  TaskAction,
  ThreadAction,
} from "./app.command.detail.ts";
import { LoadSettings, RunAdmin } from "./app.command.settings.ts";
import {
  CommitFile,
  CreateBranch,
  FetchOrigin,
  LoadCode,
  LoadFile,
  LoadFileAt,
  Bisect,
  CherryPick,
  LoadHistory,
  Propose,
  SignLocalAs,
  PushBranch,
  Rebase,
  RefreshSync,
} from "./app.command.code.ts";
import { NEW_TASK_DIALOG } from "./view.tasks.ts";
import { SEARCH_INPUT } from "./view.shell.ts";
import {
  ApplyTheme,
  CloseNewTaskDialog,
  CopyText,
  FocusSearch,
  LoadUrl,
  OpenDialog,
  Navigate,
  RememberRail,
} from "./app.command.shell.ts";
import { initials } from "./time.ts";
import { viewOf, writableBranch as writableRef } from "./code.ts";
import * as Activity from "./activity.ts";
import * as Settings from "./settings.ts";
import * as Tasks from "./task.ts";

type Return = Update.Return<Model, AppMessage>;

/** The composer, emptied — after a create, and after a cancel. */
const clearComposer = (model: Model): Model["tasksScreen"] => ({
  ...model.tasksScreen,
  title: "",
  desc: "",
  parent: "",
  submitting: false,
});

export const update = (model: Model, message: AppMessage): Return =>
  AppMessage.match<Return>(message, {
    // -- the shell ------------------------------------------------------
    /**
     * A new address is a new question.
     *
     * A stale explanation of an old one would sit above a screen it has
     * nothing to do with, and a Change Request's diff would flash under
     * another's title — so the Detail screen resets whenever the task it is
     * about changes, and asks the hub to fill in what a listing row omits.
     */
    ChangedUrl: ({ route }) => {
      const before = model.route._tag === "Detail" ? model.route.id : null;
      const after = route._tag === "Detail" ? route.id : null;
      const opened = after !== null && after !== before;
      // A Code address naming a file the screen is not showing opens it —
      // which is how a search hit, a Back and a pasted link all land on the
      // same file rather than on whatever the pane happened to hold.
      const view = viewOf(model);
      const wanted =
        route._tag === "Code" && route.path !== "" && route.path !== view.selected
          ? route.path
          : null;
      return {
        model: {
          ...model,
          route,
          navError: route._tag === "NotFound" ? "this link's address is malformed" : null,
          codeScreen:
            wanted === null || view.tip === null
              ? model.codeScreen
              : {
                  ...freshCode(model),
                  view: AsyncData.succeed({ ...view, selected: wanted, content: null }),
                },
          detailScreen: opened
            ? {
                tab: "conversation",
                diffFor: after,
                diff: AsyncData.Idle(),
                comment: "",
                replies: {},
                acting: false,
                notice: null,
                taskNotice: null,
                moveNotice: null,
              }
            : model.detailScreen,
        },
        // A hub-sourced task carries only its listing row until someone looks
        // at it; a fixture id makes this a no-op on the other side. A Code
        // address naming a file it is not showing opens that file, which is
        // how a search hit and a Back both land on the right one.
        commands: [
          ...(opened
            ? [
                HydrateDetail({
                  id: after,
                  head: Tasks.byId(model.tasks.tasks, after)?.reviewHead ?? null,
                }),
              ]
            : []),
          ...(wanted === null || view.tip === null
            ? []
            : [LoadFile({ tip: view.tip, path: wanted })]),
        ],
      };
    },

    /**
     * A link, before the browser follows it.
     *
     * Internal addresses are pushed so this application routes them without
     * a reload; anything else leaves, which is what a link off-site means.
     */
    RequestedUrl: ({ request }) =>
      Navigation.UrlRequest.match<Return>(request, {
        Internal: ({ url }) => ({
          model,
          commands: [Navigate({ url: url.pathname })],
        }),
        External: ({ href }) => ({ model, commands: [LoadUrl({ href })] }),
      }),

    ClickedNavigate: ({ route }) => ({
      model,
      commands: [Navigate({ url: urlOf(route) })],
    }),

    ClickedTask: ({ id }) => ({
      model,
      commands: [Navigate({ url: urlOf(AppRoute.Detail({ id })) })],
    }),

    /**
     * A query opens Search, which answers from both sides of the split:
     * Tasks from the Model, file contents from the server's `/grep`. Clearing
     * the field leaves the screen in place with its hint.
     */
    ChangedSearchQuery: ({ query }) => {
      const pattern = query.trim();
      return {
        model: {
          ...model,
          query,
          // An empty field is not a search in progress; showing the previous
          // answer under no query would say the repository still matches it.
          searchScreen: {
            code: pattern === "" ? AsyncData.Idle() : AsyncData.Loading(),
          },
        },
        commands: [
          ...(pattern === "" ? [] : [Grep({ pattern })]),
          ...(pattern !== "" && model.route._tag !== "Search"
            ? [Navigate({ url: urlOf(AppRoute.Search()) })]
            : []),
        ],
      };
    },

    // Toggling pins: from here the reader has said which palette they want,
    // and the system changing its mind later is not an instruction.
    ClickedThemeToggle: () => {
      const theme = model.theme === "dark" ? "light" : "dark";
      return {
        model: { ...model, theme, themePinned: true },
        commands: [ApplyTheme({ theme })],
      };
    },

    ChangedTheme: ({ theme }) => ({ model: { ...model, theme } }),

    ClickedRailToggle: () => {
      const railCollapsed = !model.railCollapsed;
      return {
        model: { ...model, railCollapsed },
        commands: [RememberRail({ collapsed: railCollapsed })],
      };
    },

    // A collapsed rail hides the input, so expanding comes first — and the
    // focus is a Command because the element is the DOM's, not the Model's.
    ClickedSearchField: () => ({
      model: { ...model, railCollapsed: false },
      commands: [RememberRail({ collapsed: false }), FocusSearch({ selector: SEARCH_INPUT })],
    }),

    PressedSearchShortcut: () => ({
      model: { ...model, railCollapsed: false },
      commands: [RememberRail({ collapsed: false }), FocusSearch({ selector: SEARCH_INPUT })],
    }),

    CompletedFocus: () => ({ model }),

    CompletedNavigate: () => ({ model }),

    // -- identity -------------------------------------------------------
    /**
     * The clone and this answer race, and either can land first — so both
     * sides tell the repository who is asking. Without it every commit
     * written through the local clone is authored by the browser rather than
     * by the reader, and pushed that way.
     */
    SucceededFetchIdentity: ({ viewer }) => ({
      model: { ...model, viewer: AsyncData.succeed(viewer) },
      commands: [SignLocalAs({ subject: viewer.subject })],
    }),

    // Anonymous is a state, not a fault: the screens show a repository this
    // browser cannot write to rather than nothing at all.
    FailedFetchIdentity: ({ reason }) => ({
      model: { ...model, viewer: AsyncData.fail(reason) },
    }),

    // -- the hub's projection -------------------------------------------
    /**
     * The projection, adopted whole — and the open Change Request re-filled.
     *
     * A listing row carries none of the detail: no description, no
     * discussion, no threads, no checks. Adopting it over a task that had
     * been hydrated would blank the screen the reader is looking at, which is
     * what every write below causes by re-reading. So the listing lands and
     * the detail is asked for again, in that order.
     */
    SucceededLoadTasks: ({ tasks, sessions }) => {
      const open = model.route._tag === "Detail" ? model.route.id : null;
      const held = open === null ? undefined : tasks.find((task) => task.id === open);
      return {
        model: {
          ...model,
          tasks: { tasks, sessions, liveNotice: null, load: AsyncData.succeed(undefined) },
        },
        commands:
          held === undefined ? [] : [HydrateDetail({ id: held.id, head: held.reviewHead ?? null })],
      };
    },

    /**
     * Refused, which is not absent.
     *
     * The fixtures are the documented offline sample; showing them over a
     * repository that turned this browser away would dress a denial up as
     * data. The list empties and the notice says why.
     */
    DeniedLoadTasks: ({ reason }) => ({
      model: {
        ...model,
        tasks: { tasks: [], sessions: [], liveNotice: reason, load: AsyncData.succeed(undefined) },
      },
    }),

    /** Unreachable or empty: the fixtures stand, and nothing is said. */
    FailedLoadTasks: ({ reason }) => ({
      model: { ...model, tasks: { ...model.tasks, load: AsyncData.fail(reason) } },
    }),

    // -- the Tasks screen -----------------------------------------------
    ChangedTaskFilter: ({ filter }) => ({
      model: { ...model, tasksScreen: { ...model.tasksScreen, filter } },
    }),

    ChangedNewTaskTitle: ({ title }) => ({
      model: { ...model, tasksScreen: { ...model.tasksScreen, title } },
    }),

    ChangedNewTaskDescription: ({ desc }) => ({
      model: { ...model, tasksScreen: { ...model.tasksScreen, desc } },
    }),

    ChangedNewTaskParent: ({ parent }) => ({
      model: { ...model, tasksScreen: { ...model.tasksScreen, parent } },
    }),

    /**
     * The id is decided here, not in the Command.
     *
     * The hub answers with its own id when it takes the task; when it will
     * not, the tab-local task needs one, and picking it from the list the
     * Model already holds keeps the Command free of the domain.
     */
    SubmittedNewTask: () => {
      const title = model.tasksScreen.title.trim();
      if (title === "" || model.tasksScreen.submitting) return { model };
      return {
        model: { ...model, tasksScreen: { ...model.tasksScreen, submitting: true } },
        commands: [
          CreateTask({
            title,
            desc: model.tasksScreen.desc.trim(),
            parent: model.tasksScreen.parent,
            fallbackId: Tasks.nextId(model.tasks.tasks),
          }),
        ],
      };
    },

    ClickedCancelNewTask: () => ({
      model: { ...model, tasksScreen: clearComposer(model) },
      commands: [CloseNewTaskDialog({ selector: NEW_TASK_DIALOG })],
    }),

    /**
     * The hub took it, so the projection is what carries it.
     *
     * Opened *and* re-read: the detail screen shows the repository's own
     * answer, and until that answer lands there is nothing to show — which is
     * why this reloads rather than drawing a task from this side.
     */
    SucceededCreateTask: ({ id }) => ({
      model: { ...model, tasksScreen: clearComposer(model) },
      commands: [
        CloseNewTaskDialog({ selector: NEW_TASK_DIALOG }),
        LoadTasks(),
        Navigate({ url: urlOf(AppRoute.Detail({ id })) }),
      ],
    }),

    /**
     * The hub would not take it, so this tab holds it.
     *
     * Filed under the same parent the dialog was told, so the two paths agree
     * about where a task the reader filed ends up.
     */
    FellBackCreateTask: ({ id }) => {
      const name = authorName(model);
      const task = Tasks.opened({
        id,
        title: model.tasksScreen.title.trim(),
        desc: model.tasksScreen.desc.trim(),
        author: { name, avatar: initials(name) },
      });
      const parent = model.tasksScreen.parent;
      const withTask = [...model.tasks.tasks, task];
      return {
        model: {
          ...model,
          tasks: {
            ...model.tasks,
            tasks: parent === "" ? withTask : Tasks.moved(withTask, id, parent),
          },
          tasksScreen: clearComposer(model),
        },
        commands: [
          CloseNewTaskDialog({ selector: NEW_TASK_DIALOG }),
          Navigate({ url: urlOf(AppRoute.Detail({ id })) }),
        ],
      };
    },

    SucceededMoveTask: ({ id, parent }) => ({
      model: {
        ...model,
        tasks: { ...model.tasks, tasks: Tasks.moved(model.tasks.tasks, id, parent) },
        detailScreen: { ...model.detailScreen, acting: false, moveNotice: null },
      },
    }),

    /** The hub kept the task where it was; say so rather than disagreeing. */
    FailedMoveTask: ({ reason }) => ({
      model: {
        ...model,
        detailScreen: { ...model.detailScreen, acting: false, moveNotice: reason },
      },
    }),

    CompletedCloseNewTaskDialog: () => ({ model }),

    // -- the Search screen ----------------------------------------------
    ClickedCodeHit: ({ path }) => ({
      model,
      commands: [Navigate({ url: urlOf(AppRoute.Code({ path })) })],
    }),

    SucceededGrep: ({ matches, truncated }) => ({
      model: {
        ...model,
        searchScreen: { code: AsyncData.succeed({ matches, truncated }) },
      },
    }),

    FailedGrep: ({ reason }) => ({
      model: { ...model, searchScreen: { code: AsyncData.fail(reason) } },
    }),

    // -- the Activity screen --------------------------------------------
    // Zooming resets the offset: a window paged twenty days back means one
    // thing at a week's width and another at a month's, and keeping the
    // number would move the reader somewhere they did not ask to be.
    ChangedTimelineZoom: ({ zoom }) => ({
      model: { ...model, activityScreen: { ...model.activityScreen, zoom, offset: 0 } },
    }),

    ClickedTimelineEarlier: () => ({
      model: {
        ...model,
        activityScreen: {
          ...model.activityScreen,
          offset: model.activityScreen.offset + Activity.SPANS[model.activityScreen.zoom],
        },
      },
    }),

    // Never past today: the window's last column is the present, and paging
    // "later" from there would draw days that have not happened.
    ClickedTimelineLater: () => ({
      model: {
        ...model,
        activityScreen: {
          ...model.activityScreen,
          offset: Math.max(
            0,
            model.activityScreen.offset - Activity.SPANS[model.activityScreen.zoom],
          ),
        },
      },
    }),

    SucceededLoadCommits: ({ commits }) => ({
      model: {
        ...model,
        activityScreen: { ...model.activityScreen, commits: AsyncData.succeed(commits) },
      },
    }),

    FailedLoadCommits: ({ reason }) => ({
      model: {
        ...model,
        activityScreen: { ...model.activityScreen, commits: AsyncData.fail(reason) },
      },
    }),

    // -- the Settings screen --------------------------------------------
    /**
     * The forms are seeded from the answer.
     *
     * The policy inputs show what the repository enforces, so they have to be
     * filled from it — and the branch selector defaults to the branch the
     * "Move" button would otherwise refuse to name. A reload after an action
     * re-seeds them, which is what makes the published policy the one on
     * screen rather than the one that was typed.
     */
    SucceededLoadSettings: ({ data }) => ({
      model: {
        ...model,
        settingsScreen: {
          ...model.settingsScreen,
          data: AsyncData.succeed(data),
          forms: {
            ...model.settingsScreen.forms,
            resetRef:
              model.settingsScreen.forms.resetRef === ""
                ? Settings.short(data.branches[0]?.name ?? "")
                : model.settingsScreen.forms.resetRef,
            policyProtected: data.policy?.rules.protected.join(", ") ?? "",
            policyApprovals: String(data.policy?.rules.requiredApprovals ?? 0),
            policyChecks: data.policy?.rules.requiredChecks.join(", ") ?? "",
            policyRequirePullRequest: data.policy?.rules.requirePullRequest ?? false,
            policyRequireResolvedThreads: data.policy?.rules.requireResolvedThreads ?? false,
          },
        },
      },
    }),

    FailedLoadSettings: ({ failure }) => ({
      model: {
        ...model,
        settingsScreen: { ...model.settingsScreen, data: AsyncData.fail(failure) },
      },
    }),

    SucceededLoadBrowserKey: ({ key }) => ({
      model: { ...model, settingsScreen: { ...model.settingsScreen, browserKey: key } },
    }),

    ChangedSettingsField: ({ field, value }) => ({
      model: {
        ...model,
        settingsScreen: {
          ...model.settingsScreen,
          forms: { ...model.settingsScreen.forms, [field]: value },
        },
      },
    }),

    ToggledSettingsField: ({ field, value }) => ({
      model: {
        ...model,
        settingsScreen: {
          ...model.settingsScreen,
          forms: { ...model.settingsScreen.forms, [field]: value },
        },
      },
    }),

    // One at a time: each action reloads the lists it changed, and two in
    // flight would race over which answer the cards end up showing.
    SubmittedAdmin: ({ action }) =>
      model.settingsScreen.busy
        ? { model }
        : {
            model: { ...model, settingsScreen: { ...model.settingsScreen, busy: true } },
            commands: [RunAdmin({ action })],
          },

    /**
     * Including a refusal — the server answered, and the card says what.
     *
     * The forms the action consumed are cleared, and only those: a reader who
     * added a remote is done with that form, and one whose tag was refused
     * still has the name they typed.
     */
    SucceededAdmin: ({ card, note, reflog }) => ({
      model: {
        ...model,
        settingsScreen: {
          ...model.settingsScreen,
          busy: false,
          notes: { ...model.settingsScreen.notes, [card]: note },
          reflog: reflog ?? model.settingsScreen.reflog,
          forms: clearedForms(model.settingsScreen.forms, card),
        },
      },
      commands: [LoadSettings()],
    }),

    FailedAdmin: ({ card, note }) => ({
      model: {
        ...model,
        settingsScreen: {
          ...model.settingsScreen,
          busy: false,
          notes: { ...model.settingsScreen.notes, [card]: note },
        },
      },
    }),

    ClickedCopyBrowserKey: ({ text }) => ({ model, commands: [CopyText({ text })] }),

    // The button says "Copied" until the clipboard write settles, and then
    // goes back to offering the copy — a label stuck on "Copied" would stop
    // reading as a button at all.
    CompletedCopy: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, copied: false } },
    }),

    // -- the Detail screen ----------------------------------------------
    /**
     * A settled Diff selection is reloaded.
     *
     * A route can replace the task while its previous diff is still
     * resolving, so what is on screen may belong to that previous route while
     * the Command that would have corrected it was superseded.
     */
    ChangedDetailTab: ({ tab }) => {
      const task = detailTask(model);
      const cr = task !== undefined && isChangeRequest(task) ? task : null;
      const stale = !AsyncData.isLoading(model.detailScreen.diff);
      return {
        model: { ...model, detailScreen: { ...model.detailScreen, tab } },
        commands:
          tab === "diff" && cr !== null && stale
            ? [
                LoadDiff({
                  id: cr.id,
                  sourceRef: cr.sourceRef,
                  targetRef: cr.targetRef,
                }),
              ]
            : [],
      };
    },

    SucceededLoadDiff: ({ id, files }) =>
      // The answer belongs to the Change Request it was asked about. A reader
      // who opened another one while this resolved must not be shown it.
      model.detailScreen.diffFor === id
        ? {
            model: {
              ...model,
              detailScreen: { ...model.detailScreen, diff: AsyncData.succeed(files) },
            },
          }
        : { model },

    FellBackLoadDiff: ({ id, reason }) =>
      model.detailScreen.diffFor === id
        ? {
            model: {
              ...model,
              detailScreen: { ...model.detailScreen, diff: AsyncData.fail(reason) },
            },
          }
        : { model },

    CompletedMountDiff: () => ({ model }),

    ChangedCommentDraft: ({ text }) => ({
      model: { ...model, detailScreen: { ...model.detailScreen, comment: text } },
    }),

    /**
     * Append a comment, split on provenance.
     *
     * A hub comment is a signed event read back from the projection; a
     * fixture's stays tab-local, authored as whoever `/whoami` said is asking,
     * which is the design's documented sample behaviour.
     */
    SubmittedComment: () => {
      const task = detailTask(model);
      const text = model.detailScreen.comment.trim();
      if (task === undefined || text === "") return { model };
      if (task.hub === true) {
        return {
          model: { ...model, detailScreen: { ...model.detailScreen, acting: true, notice: null } },
          commands: [CommentRemote({ id: task.id, body: text })],
        };
      }
      const author = authorName(model);
      return {
        model: {
          ...model,
          tasks: {
            ...model.tasks,
            tasks: Tasks.replace(model.tasks.tasks, task.id, (held) => ({
              ...held,
              comments: [
                ...held.comments,
                { avatar: initials(author), author, when: "just now", text },
              ],
              updated: "just now",
            })),
          },
          detailScreen: { ...model.detailScreen, comment: "", notice: null },
        },
      };
    },

    // The projection is what shows next, so the draft clears and the listing
    // is re-read rather than a comment being drawn from this side.
    SucceededComment: () => ({
      model: {
        ...model,
        detailScreen: { ...model.detailScreen, acting: false, comment: "", notice: null },
      },
      commands: [LoadTasks()],
    }),

    FailedComment: ({ reason }) => ({
      model: { ...model, detailScreen: { ...model.detailScreen, acting: false, notice: reason } },
    }),

    ClickedMerge: () => {
      const task = detailTask(model);
      if (task === undefined || !isChangeRequest(task)) return { model };
      return {
        model: { ...model, detailScreen: { ...model.detailScreen, acting: true, notice: null } },
        commands: [
          task.hub === true
            ? MergeRemote({
                id: task.id,
                head: task.reviewHead ?? "",
                base: task.targetRef,
              })
            : MergeFixture({
                id: task.id,
                title: task.title,
                sourceRef: task.sourceRef,
                targetRef: task.targetRef,
              }),
        ],
      };
    },

    /**
     * Settled.
     *
     * A hub Change Request shows the projection, re-read. A fixture's records
     * the merge in the sample list — clearly sample behaviour, and never
     * reachable by a hub entity, which took the other branch above.
     */
    SucceededMerge: () => {
      const task = detailTask(model);
      const local = task !== undefined && task.hub !== true;
      return {
        model: {
          ...model,
          tasks:
            task === undefined || !local
              ? model.tasks
              : {
                  ...model.tasks,
                  tasks: Tasks.replace(model.tasks.tasks, task.id, Tasks.merged),
                },
          detailScreen: { ...model.detailScreen, acting: false, notice: null },
        },
        commands: local ? [] : [LoadTasks()],
      };
    },

    FailedMerge: ({ reason }) => ({
      model: { ...model, detailScreen: { ...model.detailScreen, acting: false, notice: reason } },
    }),

    ClickedReview: ({ decision }) => {
      const task = detailTask(model);
      if (task === undefined) return { model };
      return {
        model: { ...model, detailScreen: { ...model.detailScreen, acting: true, notice: null } },
        commands: [ReviewRemote({ id: task.id, decision, head: task.reviewHead ?? "" })],
      };
    },

    ClickedThreadResolve: ({ thread, resolved }) => {
      const task = detailTask(model);
      if (task === undefined) return { model };
      return {
        model: { ...model, detailScreen: { ...model.detailScreen, acting: true, notice: null } },
        commands: [
          ThreadAction({
            id: task.id,
            thread,
            action: resolved ? "resolve" : "reopen",
            body: "",
          }),
        ],
      };
    },

    ChangedThreadDraft: ({ thread, text }) => ({
      model: {
        ...model,
        detailScreen: {
          ...model.detailScreen,
          replies: { ...model.detailScreen.replies, [thread]: text },
        },
      },
    }),

    SubmittedThreadReply: ({ thread }) => {
      const task = detailTask(model);
      const body = (model.detailScreen.replies[thread] ?? "").trim();
      if (task === undefined || body === "") return { model };
      return {
        model: { ...model, detailScreen: { ...model.detailScreen, acting: true, notice: null } },
        commands: [ThreadAction({ id: task.id, thread, action: "reply", body })],
      };
    },

    SucceededThread: () => ({
      model: {
        ...model,
        detailScreen: { ...model.detailScreen, acting: false, notice: null, replies: {} },
      },
      commands: [LoadTasks()],
    }),

    FailedThread: ({ reason }) => ({
      model: { ...model, detailScreen: { ...model.detailScreen, acting: false, notice: reason } },
    }),

    ClickedTaskAction: ({ action }) => {
      const task = detailTask(model);
      if (task === undefined) return { model };
      return {
        model: {
          ...model,
          detailScreen: { ...model.detailScreen, acting: true, taskNotice: null },
        },
        commands: [TaskAction({ id: task.id, action })],
      };
    },

    SucceededTaskAction: () => ({
      model: {
        ...model,
        detailScreen: { ...model.detailScreen, acting: false, taskNotice: null },
      },
      commands: [LoadTasks()],
    }),

    FailedTaskAction: ({ reason }) => ({
      model: {
        ...model,
        detailScreen: { ...model.detailScreen, acting: false, taskNotice: reason },
      },
    }),

    /**
     * Re-file this task under another, or out from under one.
     *
     * Any member may re-file work in the hub, so this is offered on every task
     * rather than only on the ones this browser opened.
     */
    // -- the Code screen ------------------------------------------------
    // Only the newest request's answer paints. See `wantedRef`.
    SucceededLoadCode: ({ ref, view }) =>
      ref === model.codeScreen.wantedRef
        ? {
            model: {
              ...model,
              codeScreen: { ...freshCode(model), view: AsyncData.succeed(view) },
            },
            commands: [RefreshSync({ ref: view.ref })],
          }
        : { model },

    // Guarded like its success twin: a superseded request's failure must not
    // drop the newer request's screen to the sample.
    FellBackLoadCode: ({ ref, reason }) =>
      ref === model.codeScreen.wantedRef
        ? {
            model: {
              ...model,
              codeScreen: { ...freshCode(model), view: AsyncData.fail(reason) },
            },
          }
        : { model },

    /**
     * Walking the tree abandons an open editor.
     *
     * Carrying a draft of one file over to another would write it to the
     * wrong path; a history view belongs to the file it was opened from.
     */
    ClickedFile: ({ path }) => {
      const view = viewOf(model);
      // Directories reach here too; they have no blob to read.
      if (!view.paths.includes(path)) return { model };
      // The sample repository has no tip to fetch from, so its files are
      // opened with what the sample holds. Doing nothing would make every
      // click in the offline explorer inert, which is worse than a stub.
      if (view.tip === null) {
        return {
          model: {
            ...model,
            codeScreen: {
              ...freshCode(model),
              view: AsyncData.succeed({
                ...view,
                selected: path,
                content: path === "README.md" ? view.content : `// ${path}`,
              }),
            },
          },
          commands: [Navigate({ url: urlOf(AppRoute.Code({ path })) })],
        };
      }
      return {
        model: {
          ...model,
          codeScreen: {
            ...freshCode(model),
            view: AsyncData.succeed({ ...view, selected: path, content: null }),
          },
        },
        commands: [
          LoadFile({ tip: view.tip, path }),
          Navigate({ url: urlOf(AppRoute.Code({ path })) }),
        ],
      };
    },

    SucceededLoadFileAt: ({ oid, path, content }) => {
      const view = viewOf(model);
      if (view.selected !== path) return { model };
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            at: oid === "" ? null : oid,
            view: AsyncData.succeed({ ...view, content }),
          },
        },
      };
    },

    FailedLoadFileAt: ({ path, reason }) => {
      const view = viewOf(model);
      if (view.selected !== path) return { model };
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            view: AsyncData.succeed({ ...view, content: `// ${path} — ${reason}` }),
          },
        },
      };
    },

    /**
     * The whole screen reads one ref, so a switch refetches the lot rather
     * than trying to patch the explorer, the pane and the commit bar in place.
     */
    /**
     * The menu's last item is not a branch: it opens the dialog that creates
     * one. `ui-dialog` opens from a trigger it owns, and a menu item is not
     * that trigger, so this is the one place the show is a Command.
     */
    SelectedBranch: ({ ref }) =>
      ref === "__new-branch"
        ? { model, commands: [OpenDialog({ selector: "ui-dialog.gp-new-branch" })] }
        : ref === "__rebase"
          ? update(model, AppMessage.ClickedRebase())
          : {
              model: {
                ...model,
                codeScreen: { ...model.codeScreen, wantedRef: ref, view: AsyncData.Loading() },
              },
              commands: [LoadCode({ ref, keep: "" })],
            },

    ChangedNewBranch: ({ name }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, newBranch: name } },
    }),

    SubmittedNewBranch: () => {
      const name = model.codeScreen.newBranch.trim();
      if (name === "") return { model };
      return {
        model: { ...model, codeScreen: { ...model.codeScreen, syncNotice: null } },
        commands: [CreateBranch({ name, from: viewOf(model).ref })],
      };
    },

    SucceededCreateBranch: ({ name }) => ({
      model: {
        ...model,
        codeScreen: {
          ...model.codeScreen,
          newBranch: "",
          wantedRef: name,
          view: AsyncData.Loading(),
        },
      },
      commands: [
        CloseNewTaskDialog({ selector: "ui-dialog.gp-new-branch" }),
        LoadCode({ ref: name, keep: "" }),
      ],
    }),

    ClickedRefresh: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, wantedRef: viewOf(model).ref } },
      commands: [LoadCode({ ref: viewOf(model).ref, keep: viewOf(model).selected ?? "" })],
    }),

    /** The commit bar toggles the recent-history panel it summarises. */
    ClickedCommitPanel: () => {
      const view = viewOf(model);
      if (model.codeScreen.panel === "commits") {
        return { model: { ...model, codeScreen: { ...model.codeScreen, panel: "none" } } };
      }
      return {
        model: {
          ...model,
          codeScreen: { ...model.codeScreen, panel: "commits", history: AsyncData.Loading() },
        },
        commands: view.tip === null ? [] : [LoadHistory({ tip: view.tip, path: "" })],
      };
    },

    ClickedFileLogPanel: () => {
      const view = viewOf(model);
      if (model.codeScreen.panel === "filelog") {
        return { model: { ...model, codeScreen: { ...model.codeScreen, panel: "none" } } };
      }
      if (view.tip === null || view.selected === null) return { model };
      return {
        model: {
          ...model,
          codeScreen: { ...model.codeScreen, panel: "filelog", history: AsyncData.Loading() },
        },
        commands: [LoadHistory({ tip: view.tip, path: view.selected })],
      };
    },

    SucceededLoadHistory: ({ rows }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, history: AsyncData.succeed(rows) } },
    }),

    FailedLoadHistory: ({ reason }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, history: AsyncData.fail(reason) } },
    }),

    /** Show the open file as it was at this commit — a read-only look back. */
    ClickedHistoryRow: ({ oid }) => {
      const view = viewOf(model);
      if (view.selected === null) return { model };
      return {
        model: {
          ...model,
          codeScreen: { ...model.codeScreen, mode: "view", at: oid, diffing: false },
        },
        commands: [LoadFileAt({ oid, path: view.selected })],
      };
    },

    ClickedBackToTip: () => {
      const view = viewOf(model);
      if (view.selected === null || view.tip === null) return { model };
      return {
        model: { ...model, codeScreen: { ...model.codeScreen, at: null } },
        commands: [LoadFile({ tip: view.tip, path: view.selected })],
      };
    },

    /**
     * The draft opens as the file that is on screen.
     *
     * Seeding it here rather than inside the editor is what keeps the two in
     * step: the Model is the text, and the Mount is only the surface it is
     * typed on.
     */
    ClickedEdit: () => {
      const view = viewOf(model);
      // Not until the blob is here. A file whose request is still out has
      // `content: null`, and seeding the draft from that would commit an
      // empty file over the one the reader meant to edit.
      if (!writableCode(view) || view.selected === null || view.content === null) {
        return { model };
      }
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            mode: "edit",
            newPath: null,
            draft: view.content ?? "",
            message: "",
            editError: null,
            diffing: false,
          },
        },
      };
    },

    ClickedNewFile: () => {
      const view = viewOf(model);
      if (!writableCode(view)) return { model };
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            mode: "edit",
            newPath: "",
            draft: "",
            message: "",
            editError: null,
            diffing: false,
          },
        },
      };
    },

    ClickedCancelEdit: () => ({
      model: {
        ...model,
        codeScreen: {
          ...model.codeScreen,
          mode: "view",
          newPath: null,
          draft: "",
          editError: null,
          diffing: false,
        },
      },
    }),

    ChangedNewFilePath: ({ path }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, newPath: path } },
    }),

    ChangedFileDraft: ({ text }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, draft: text } },
    }),

    ChangedCommitMessage: ({ message }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, message } },
    }),

    /** Commit what the Model holds, at the chosen path. */
    ClickedSave: () => {
      const view = viewOf(model);
      const creating = model.codeScreen.newPath !== null;
      const path = creating ? (model.codeScreen.newPath ?? "").trim() : view.selected;
      if (model.codeScreen.saving) return { model };
      if (path === null || path === "") {
        return {
          model: {
            ...model,
            codeScreen: { ...model.codeScreen, editError: "name the file to create" },
          },
        };
      }
      if (creating && view.paths.includes(path)) {
        return {
          model: {
            ...model,
            codeScreen: {
              ...model.codeScreen,
              editError: `${path} already exists — select it in the explorer and edit it instead`,
            },
          },
        };
      }
      const typed = model.codeScreen.message.trim();
      return {
        model: {
          ...model,
          codeScreen: { ...model.codeScreen, saving: true, editError: null },
        },
        commands: [
          CommitFile({
            branch: view.ref,
            path,
            content: model.codeScreen.draft,
            message: typed === "" ? `${creating ? "add" : "update"} ${path}` : typed,
            expected: view.tip,
            keep: path,
          }),
        ],
      };
    },

    /** Remove the open file — the same commit request, with no content. */
    ClickedDeleteFile: () => {
      const view = viewOf(model);
      if (view.selected === null || model.codeScreen.newPath !== null || model.codeScreen.saving) {
        return { model };
      }
      return {
        model: {
          ...model,
          codeScreen: { ...model.codeScreen, saving: true, editError: null },
        },
        commands: [
          CommitFile({
            branch: view.ref,
            path: view.selected,
            content: null,
            message: `delete ${view.selected}`,
            expected: view.tip,
            keep: "",
          }),
        ],
      };
    },

    SucceededCommitFile: ({ keep }) => ({
      model: {
        ...model,
        codeScreen: { ...model.codeScreen, saving: false, wantedRef: viewOf(model).ref },
      },
      commands: [LoadCode({ ref: viewOf(model).ref, keep })],
    }),

    FailedCommitFile: ({ reason }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, saving: false, editError: reason } },
    }),

    ToggledDiffReview: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, diffing: !model.codeScreen.diffing } },
    }),

    ClickedClone: ({ text }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, copied: true } },
      commands: [CopyText({ text })],
    }),

    ClickedPush: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, syncing: true, syncNotice: null } },
      commands: [PushBranch({ ref: viewOf(model).ref })],
    }),

    ClickedFetch: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, syncing: true, syncNotice: null } },
      commands: [FetchOrigin()],
    }),

    SucceededRefreshSync: ({ sync }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, sync } },
    }),

    CompletedSync: ({ notice, reload }) => ({
      model: {
        ...model,
        codeScreen: {
          ...model.codeScreen,
          syncing: false,
          syncNotice: notice,
          wantedRef: reload ? viewOf(model).ref : model.codeScreen.wantedRef,
        },
      },
      commands: [
        RefreshSync({ ref: viewOf(model).ref }),
        ...(reload
          ? [LoadCode({ ref: viewOf(model).ref, keep: viewOf(model).selected ?? "" })]
          : []),
      ],
    }),

    /**
     * The clone landed, or it did not.
     *
     * Either way the screen reloads through whichever client now answers: the
     * local repository turns a hundred-commit history read into local object
     * reads rather than an N+1 of requests, and a browser that cannot hold one
     * keeps the HTTP client with nothing about the page changing.
     */
    /**
     * The clone landed, or it did not.
     *
     * Ready is worth re-reading through: the local repository answers a
     * hundred-commit history from local objects rather than an N+1 of
     * requests. Anything else changes nothing at all — the HTTP client keeps
     * answering, which is the documented behaviour rather than a failure — so
     * `wantedRef` moves only when a request is actually issued to replace
     * whatever is in flight. Resetting it either way would strand a branch
     * switch the reader made while the clone was still opening.
     */
    SettledLocalRepository: ({ state }) =>
      state === "Ready"
        ? {
            model: {
              ...model,
              localRepository: state,
              codeScreen: { ...model.codeScreen, wantedRef: viewOf(model).ref },
            },
            commands: [LoadCode({ ref: viewOf(model).ref, keep: viewOf(model).selected ?? "" })],
          }
        : { model: { ...model, localRepository: state } },

    ChangedProposeField: ({ field, value }) => ({
      model: {
        ...model,
        codeScreen: {
          ...model.codeScreen,
          ...(field === "title" ? { proposeTitle: value } : { proposeDescription: value }),
        },
      },
    }),

    /**
     * The revision the proposal names is the one on screen now.
     *
     * Read here rather than inside the Command: navigation and typing may
     * continue while the push is out, and the proposal must keep the branch,
     * the tip and the text the reader chose.
     */
    SubmittedPropose: () => {
      const view = viewOf(model);
      const title = model.codeScreen.proposeTitle.trim();
      if (title === "" || view.tip === null || view.defaultBranch === null) return { model };
      return {
        model: { ...model, codeScreen: { ...model.codeScreen, syncing: true, syncNotice: null } },
        commands: [
          Propose({
            branch: view.ref,
            head: view.tip,
            base: view.defaultBranch,
            title,
            description: model.codeScreen.proposeDescription.trim(),
          }),
        ],
      };
    },

    SucceededPropose: ({ id }) => ({
      model: {
        ...model,
        codeScreen: {
          ...model.codeScreen,
          syncing: false,
          syncNotice: null,
          proposeTitle: "",
          proposeDescription: "",
        },
      },
      commands: [
        CloseNewTaskDialog({ selector: "ui-dialog.gp-propose" }),
        LoadTasks(),
        Navigate({ url: urlOf(AppRoute.Detail({ id })) }),
      ],
    }),

    FailedPropose: ({ reason }) => ({
      model: {
        ...model,
        codeScreen: { ...model.codeScreen, syncing: false, syncNotice: reason },
      },
    }),

    ClickedRebase: () => {
      const view = viewOf(model);
      if (view.defaultBranch === null || view.ref === view.defaultBranch) return { model };
      return {
        model: { ...model, codeScreen: { ...model.codeScreen, syncing: true, syncNotice: null } },
        commands: [Rebase({ branch: view.ref, onto: view.defaultBranch })],
      };
    },

    ClickedCherryPick: ({ commit }) => {
      const view = viewOf(model);
      if (!writableCode(view)) return { model };
      return {
        model: { ...model, codeScreen: { ...model.codeScreen, syncing: true, syncNotice: null } },
        commands: [CherryPick({ commit, onto: view.ref })],
      };
    },

    /**
     * A bisect needs one commit known good and one known bad.
     *
     * Marking either records it; the step only runs once both exist, because
     * a bisect over one mark has nothing to halve.
     */
    MarkedBisect: ({ commit, as }) => {
      const current = model.codeScreen.bisect ?? { good: [], bad: null, answer: null };
      const next =
        as === "bad"
          ? { ...current, bad: commit }
          : {
              ...current,
              good: [...current.good.filter((oid) => oid !== commit), commit],
            };
      return {
        model: { ...model, codeScreen: { ...model.codeScreen, bisect: next } },
        commands:
          next.bad !== null && next.good.length > 0
            ? [Bisect({ good: next.good, bad: next.bad })]
            : [],
      };
    },

    SucceededBisect: ({ answer }) => ({
      model: {
        ...model,
        codeScreen: {
          ...model.codeScreen,
          bisect: model.codeScreen.bisect === null ? null : { ...model.codeScreen.bisect, answer },
        },
      },
    }),

    ClickedResetBisect: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, bisect: null } },
    }),

    CompletedMountTree: () => ({ model }),

    /**
     * The hub filled in what a listing row omits.
     *
     * Folded into the list rather than held beside it, so the detail screen
     * and the Tasks list read one task — and a detail naming a head the
     * listing has since moved past was already dropped on the way here.
     */
    SucceededHydrate: ({ task }) => ({
      model: {
        ...model,
        tasks: {
          ...model.tasks,
          tasks: Tasks.replace(model.tasks.tasks, task.id, () => task),
        },
      },
    }),

    ChangedTaskParent: ({ parent }) => {
      const task = detailTask(model);
      if (task === undefined || model.detailScreen.acting) return { model };
      return {
        model: {
          ...model,
          detailScreen: { ...model.detailScreen, acting: true, moveNotice: null },
        },
        commands: [MoveTask({ id: task.id, parent, live: task.hub === true })],
      };
    },
  });

/**
 * The Code screen, reset around whatever survives a reload.
 *
 * A new view is a new file, a closed panel, no draft and no history: carrying
 * any of it across would show one repository's answer under another's title.
 */
const freshCode = (model: Model): Model["codeScreen"] => ({
  ...model.codeScreen,
  mode: "view",
  newPath: null,
  draft: "",
  message: "",
  saving: false,
  editError: null,
  panel: "none",
  history: AsyncData.Idle(),
  at: null,
  diffing: false,
});

/** The task this route is about, when the route names one. */
const detailTask = (model: Model): Task | undefined =>
  model.route._tag === "Detail" ? Tasks.byId(model.tasks.tasks, model.route.id) : undefined;

/**
 * The inputs a landed action has consumed.
 *
 * Only the ones that action filled: a card whose form is a standing choice
 * (which branch to move, what the policy is) keeps it, and one that files
 * something new empties so the next entry starts blank.
 */
const clearedForms = (
  forms: Model["settingsScreen"]["forms"],
  card: string,
): Model["settingsScreen"]["forms"] => {
  switch (card) {
    case "branches":
      return { ...forms, resetTo: "" };
    case "tags":
      return { ...forms, tagName: "", tagMessage: "" };
    case "remotes":
      return { ...forms, remoteName: "", remoteUrl: "", remoteCredential: "" };
    case "webhooks":
      return { ...forms, webhookUrl: "", webhookSecret: "" };
    default:
      return forms;
  }
};

/** Exported for the Story tests, which drive `update` without a runtime. */
export { MoveTask };

/** A detached HEAD can be read or branched from, but commits need a branch. */
const writableCode = (view: {
  readonly ref: string;
  readonly branches: readonly string[];
  readonly defaultBranch: string | null;
  readonly offline: boolean;
}): boolean => !view.offline && writableRef(view.ref, view.branches, view.defaultBranch);
