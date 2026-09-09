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
import { Option } from "effect";

import { AppMessage } from "./app.message.ts";
import {
  authorName,
  subjectOf,
  type Model,
  type PolicyForm,
  type SettingsData,
} from "./app.model.ts";
import { isChangeRequest, type Task } from "./model.ts";
import { AppRoute, urlOf } from "./app.route.ts";
import { fromLegacyHash } from "./route.ts";
import { CreateTask, Grep, LoadCommits, LoadTasks, MoveTask } from "./app.command.ts";
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
import { LoadBrowserKey, LoadSettings, RunAdmin } from "./app.command.settings.ts";
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
  ForgetCopied,
  FocusSearch,
  LoadUrl,
  OpenDialog,
  Navigate,
  RememberRail,
} from "./app.command.shell.ts";
import { initials } from "./time.ts";
import { NEW_BRANCH_DIALOG, viewOf, writableBranch as writableRef } from "./code.ts";
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
      // Settings and Activity are read once and then only when something makes
      // them stale, so opening them reads again. Without this a refused or
      // offline first read could not be recovered — every control on Settings
      // is disabled while the read has failed, so nothing the reader can click
      // would ask again — and the timeline stayed frozen at boot, missing the
      // commits the reader made during the session. The Lit screens they
      // replaced re-read in `connectedCallback` on every visit.
      //
      // Arriving, not re-arriving: `pushUrl` dispatches a url change whether
      // or not the address moved, and the rail's items stay clickable on the
      // screen they name. Re-reading on that click would drop a typed but
      // unsubmitted policy back to whatever the server still says.
      const arriving = model.route._tag !== route._tag;
      // `revalidateOrLoad` rather than a bare `Loading`, because the cards
      // read "no data" as "this repository has none": a Settings screen that
      // discarded its answer to re-read it said the repository had no
      // branches and that its policy could not be read, for the whole round
      // trip. `Refreshing` keeps the last answer on screen, and an already
      // pending read yields `None` rather than a second request.
      const reading =
        arriving && route._tag === "Settings"
          ? AsyncData.revalidateOrLoad(model.settingsScreen.data)
          : Option.none();
      const timeline =
        arriving && route._tag === "Activity" ? model.activityScreen.wanted + 1 : null;
      return {
        model: {
          ...model,
          route,
          navError: route._tag === "NotFound" ? "this link's address is malformed" : null,
          settingsScreen: Option.isSome(reading)
            ? { ...model.settingsScreen, data: reading.value }
            : model.settingsScreen,
          activityScreen:
            timeline === null
              ? model.activityScreen
              : { ...model.activityScreen, wanted: timeline },
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
          ...(Option.isSome(reading) ? [LoadSettings()] : []),
          // The key is read once and then never again, so a boot read that
          // failed left the one card that makes a refused screen recoverable
          // — copy this browser's public key, grant it, come back — showing a
          // bare "—" for the rest of the session.
          ...(arriving && route._tag === "Settings" && model.settingsScreen.browserKey === null
            ? [LoadBrowserKey()]
            : []),
          ...(timeline === null ? [] : [LoadCommits({ wanted: timeline })]),
        ],
      };
    },

    /**
     * A link, before the browser follows it.
     *
     * Internal addresses are pushed so this application routes them without
     * a reload; anything else leaves, which is what a link off-site means.
     *
     * A `#/screen` fragment is one of this page's own addresses from before it
     * moved to `/hub`. `index.html`'s inline script handles that on a cold
     * load; this is the other half — a link clicked into a page already open,
     * whose pathname alone is `/` and would otherwise land on Not Found.
     */
    RequestedUrl: ({ request }) =>
      Navigation.UrlRequest.match<Return>(request, {
        Internal: ({ url }) => ({
          model,
          commands: [
            Navigate({
              url: fromLegacyHash(Option.getOrElse(url.hash, () => "")) ?? url.pathname,
            }),
          ],
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
    // Typing, not searching: the Model keeps up with the box so the controlled
    // value is never re-asserted over it. What shows under a query that has
    // moved on is not that query's answer, so the previous one goes.
    ChangedSearchDraft: ({ query }) => ({
      model: {
        ...model,
        query,
        searchScreen: {
          code: query.trim() === "" ? AsyncData.Idle() : AsyncData.Loading(),
        },
      },
    }),

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
        detailScreen: answersOpen(model, id)
          ? { ...model.detailScreen, acting: false, moveNotice: null }
          : model.detailScreen,
      },
    }),

    /** The hub kept the task where it was; say so rather than disagreeing. */
    FailedMoveTask: ({ id, reason }) => ({
      model: answersOpen(model, id)
        ? { ...model, detailScreen: { ...model.detailScreen, acting: false, moveNotice: reason } }
        : model,
    }),

    CompletedCloseNewTaskDialog: () => ({ model }),

    // -- the Search screen ----------------------------------------------
    ClickedCodeHit: ({ path }) => ({
      model,
      commands: [Navigate({ url: urlOf(AppRoute.Code({ path })) })],
    }),

    // Only the query that is in the box may paint. See `SucceededGrep`.
    SucceededGrep: ({ pattern, matches, truncated }) =>
      pattern === model.query.trim()
        ? { model: { ...model, searchScreen: { code: AsyncData.succeed({ matches, truncated }) } } }
        : { model },

    FailedGrep: ({ pattern, reason }) =>
      pattern === model.query.trim()
        ? { model: { ...model, searchScreen: { code: AsyncData.fail(reason) } } }
        : { model },

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

    // Only the newest read paints. See `ActivityScreen.wanted`.
    SucceededLoadCommits: ({ wanted, commits }) =>
      wanted === model.activityScreen.wanted
        ? {
            model: {
              ...model,
              activityScreen: { ...model.activityScreen, commits: AsyncData.succeed(commits) },
            },
          }
        : { model },

    FailedLoadCommits: ({ wanted, reason }) =>
      wanted === model.activityScreen.wanted
        ? {
            model: {
              ...model,
              activityScreen: { ...model.activityScreen, commits: AsyncData.fail(reason) },
            },
          }
        : { model },

    // -- the Settings screen --------------------------------------------
    /**
     * The forms are seeded from the answer.
     *
     * The policy inputs show what the repository enforces, so they have to be
     * filled from it — and the branch selector defaults to the branch the
     * "Move" button would otherwise refuse to name. A reload re-seeds them
     * while nothing has been typed into them, which is what keeps the card
     * showing the policy in force rather than one somebody left behind.
     *
     * The selector's own choice is re-seeded too when the branch it named is
     * gone: the control is bound to the Model, so a name the answer no longer
     * carries leaves it blank while the form still submits that name — and
     * `reset` on a ref the repository does not have creates it, putting the
     * branch the operator just deleted back.
     */
    SucceededLoadSettings: ({ data }) => {
      const forms = model.settingsScreen.forms;
      // Every one of the sixteen admin actions reloads Settings, and eleven of
      // them fill no form at all. A reload that re-seeded the policy card
      // regardless would take an operator who was halfway through typing a
      // protected-ref list, had clicked "Show reflog" in another card, and
      // snap both fields and both switches back to the server's answer — and
      // then publish that answer when they pressed the button.
      //
      // So the card is re-seeded only while it still holds exactly what the
      // last answer put there. Anything else is the operator's, including the
      // policy they just published: it is what the new answer says too.
      const held = AsyncData.getData(model.settingsScreen.data);
      const seed = policySeed(data);
      const before = Option.isSome(held) ? policySeed(held.value) : null;
      const follow =
        // Nothing has filled the card yet.
        before === null ||
        // Or it still holds what it published AND the repository now says
        // that same thing, so the card is level with it whatever either is
        // spelled — the case the values cannot tell on their own, because a
        // publish that only respelled the policy answers with a policy that
        // did not move. Both halves are needed: a publish the server refused
        // leaves the card holding what it sent while the repository still
        // enforces something else, and taking the answer there would throw
        // away the rules the operator is about to send again.
        (model.settingsScreen.policyPublished !== null &&
          sameSpelling(policyOf(forms), model.settingsScreen.policyPublished) &&
          samePolicy(model.settingsScreen.policyPublished, seed)) ||
        // Or it still holds exactly what the last answer put there.
        sameSpelling(policyOf(forms), before) ||
        // Or the policy moved, and it moved to what this card already says —
        // which is what the operator's own publish looks like coming back. The
        // answer's spelling is the canonical one, so take it, and the card is
        // level with the repository again rather than latched a space away
        // from it. A reload that changed nothing cannot reach this, so a box
        // halfway through a word is left alone.
        (!sameSpelling(seed, before) && samePolicy(policyOf(forms), seed));
      const policy = follow ? seed : policyOf(forms);
      return {
        model: {
          ...model,
          settingsScreen: {
            ...model.settingsScreen,
            data: AsyncData.succeed(data),
            // Answered: whatever the card shows now is the repository's own
            // spelling of it, so there is nothing left to reconcile.
            policyPublished: null,
            forms: {
              ...forms,
              resetRef: data.branches.some(
                (branch) => Settings.short(branch.name) === forms.resetRef,
              )
                ? forms.resetRef
                : Settings.short(data.branches[0]?.name ?? ""),
              ...policy,
            },
          },
        },
      };
    },

    /**
     * A failed read, over the answer it was refreshing rather than instead of
     * it.
     *
     * Every action on this screen reloads it, so a blip on that follow-up read
     * used to blank a screen the application still held the answer for: the
     * policy card lost the rules in force, every list said the repository was
     * unreachable, and the guard above lost the seed it compares against, so
     * the next good read wrote over what the operator had typed. `Stale` says
     * both things at once — this is the last answer, and it could not be
     * confirmed — which is what the cards need to keep showing it while
     * refusing to act on it.
     */
    FailedLoadSettings: ({ failure }) => {
      const held = AsyncData.getData(model.settingsScreen.data);
      return {
        model: {
          ...model,
          settingsScreen: {
            ...model.settingsScreen,
            data: Option.isSome(held)
              ? AsyncData.Stale({ error: failure, data: held.value })
              : AsyncData.fail(failure),
          },
        },
      };
    },

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
            model: {
              ...model,
              settingsScreen: {
                ...model.settingsScreen,
                busy: true,
                // Recorded here rather than on the answer, because this is the
                // text that went to the repository — by the time it answers,
                // the boxes may have moved on.
                policyPublished:
                  Settings.cardOf(action) === "policy"
                    ? policyOf(model.settingsScreen.forms)
                    : model.settingsScreen.policyPublished,
              },
            },
            commands: [RunAdmin({ action })],
          },

    /**
     * Including a refusal — the server answered, and the card says what.
     *
     * The forms the action consumed are cleared, and only those: a reader who
     * added a remote is done with that form, and one whose tag was refused
     * still has the name they typed.
     */
    SucceededAdmin: ({ card, filled, note, reflog }) => ({
      model: {
        ...model,
        settingsScreen: {
          ...model.settingsScreen,
          busy: false,
          notes: { ...model.settingsScreen.notes, [card]: note },
          reflog: reflog ?? model.settingsScreen.reflog,
          forms: clearedForms(model.settingsScreen.forms, filled),
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

    /**
     * The clipboard has it.
     *
     * The confirmation starts here rather than on the click: a write settles
     * in a millisecond or two, so a flag set on the click and cleared on the
     * answer was never on screen long enough to be read. `ForgetCopied` is
     * what ends it, a second and a half later — the wait is a Command because
     * `update` is pure, and a label stuck on "Copied" would stop reading as a
     * button at all.
     */
    CompletedCopy: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, copied: true } },
      commands: [ForgetCopied()],
    }),

    ForgotCopied: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, copied: false } },
    }),

    // -- the Detail screen ----------------------------------------------
    /**
     * A settled Diff selection is reloaded.
     *
     * A route can replace the task while its previous diff is still
     * resolving, so what is on screen may belong to that previous route while
     * the Command that would have corrected it was superseded.
     *
     * The read is marked Loading, and a selection that is already loading is
     * left alone. Both halves matter: the tab is clickable while it is the
     * open one, a diff reads both sides of every changed file, and nothing
     * cancels a superseded read — so without this, clicking Diff three times
     * runs three full reads of the same revision at once.
     */
    ChangedDetailTab: ({ tab }) => {
      const task = detailTask(model);
      const cr = task !== undefined && isChangeRequest(task) ? task : null;
      const reading =
        tab === "diff" && cr !== null && !AsyncData.isLoading(model.detailScreen.diff);
      return {
        model: {
          ...model,
          detailScreen: {
            ...model.detailScreen,
            tab,
            diff: reading ? AsyncData.Loading() : model.detailScreen.diff,
          },
        },
        commands: reading
          ? [LoadDiff({ id: cr.id, sourceRef: cr.sourceRef, targetRef: cr.targetRef })]
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
    // is re-read rather than a comment being drawn from this side. The draft
    // clears only if it still holds what was sent: anything typed since is
    // the reader's, not this answer's to discard.
    SucceededComment: ({ id, body }) => ({
      model: answersOpen(model, id)
        ? {
            ...model,
            detailScreen: {
              ...model.detailScreen,
              acting: false,
              comment: model.detailScreen.comment.trim() === body ? "" : model.detailScreen.comment,
              notice: null,
            },
          }
        : model,
      commands: [LoadTasks()],
    }),

    FailedComment: ({ id, reason }) => ({
      model: answersOpen(model, id)
        ? { ...model, detailScreen: { ...model.detailScreen, acting: false, notice: reason } }
        : model,
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
    SucceededMerge: ({ id }) => {
      // The merged Change Request is the one that was asked about, not the one
      // the route happens to name now — stamping "Merged" onto whatever the
      // reader opened next would mark a Change Request nobody merged.
      const task = Tasks.byId(model.tasks.tasks, id);
      const local = task !== undefined && task.hub !== true;
      const screen = answersOpen(model, id)
        ? { ...model.detailScreen, acting: false, notice: null }
        : model.detailScreen;
      return {
        model: {
          ...model,
          tasks: local
            ? { ...model.tasks, tasks: Tasks.replace(model.tasks.tasks, id, Tasks.merged) }
            : model.tasks,
          detailScreen: screen,
        },
        commands: local ? [] : [LoadTasks()],
      };
    },

    FailedMerge: ({ id, reason }) => ({
      model: answersOpen(model, id)
        ? { ...model, detailScreen: { ...model.detailScreen, acting: false, notice: reason } }
        : model,
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

    /**
     * The thread settled.
     *
     * Only the reply that landed clears its own box, and only while it still
     * holds what was sent. A resolve or a reopen carries no body and so
     * touches no draft at all — the reader's unsent replies to other threads
     * are theirs, not this answer's to discard.
     */
    SucceededThread: ({ id, thread, body }) => ({
      model: answersOpen(model, id)
        ? {
            ...model,
            detailScreen: {
              ...model.detailScreen,
              acting: false,
              notice: null,
              replies:
                body !== "" && (model.detailScreen.replies[thread] ?? "").trim() === body
                  ? { ...model.detailScreen.replies, [thread]: "" }
                  : model.detailScreen.replies,
            },
          }
        : model,
      commands: [LoadTasks()],
    }),

    FailedThread: ({ id, reason }) => ({
      model: answersOpen(model, id)
        ? { ...model, detailScreen: { ...model.detailScreen, acting: false, notice: reason } }
        : model,
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

    SucceededTaskAction: ({ id }) => ({
      model: answersOpen(model, id)
        ? { ...model, detailScreen: { ...model.detailScreen, acting: false, taskNotice: null } }
        : model,
      commands: [LoadTasks()],
    }),

    FailedTaskAction: ({ id, reason }) => ({
      model: answersOpen(model, id)
        ? { ...model, detailScreen: { ...model.detailScreen, acting: false, taskNotice: reason } }
        : model,
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
      if (view.selected !== path || !answersRevision(model, oid)) return { model };
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

    FailedLoadFileAt: ({ oid, path, reason }) => {
      const view = viewOf(model);
      if (view.selected !== path || !answersRevision(model, oid)) return { model };
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
     *
     * The menu's last item is not a branch: it opens the dialog that creates
     * one. `ui-dialog` opens from a trigger it owns, and a menu item is not
     * that trigger, so this is the one place the show is a Command.
     */
    SelectedBranch: ({ ref }) =>
      ref === "__new-branch"
        ? { model, commands: [OpenDialog({ selector: NEW_BRANCH_DIALOG })] }
        : ref === "__rebase"
          ? update(model, AppMessage.ClickedRebase())
          : {
              model: {
                ...model,
                codeScreen: { ...model.codeScreen, wantedRef: ref, view: AsyncData.Loading() },
              },
              commands: [LoadCode({ ref, keep: "" })],
            },

    /**
     * Cancel.
     *
     * The dialog closes as well as clearing: it is opened imperatively from a
     * menu item rather than from a `[data-dialog-trigger]`, so `ui-dialog` has
     * no trigger to press again and only Escape or the backdrop would have
     * dismissed it — with the focus trap over the screen until then.
     */
    ClickedCancelNewBranch: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, newBranch: "", syncNotice: null } },
      commands: [CloseNewTaskDialog({ selector: NEW_BRANCH_DIALOG })],
    }),

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
        CloseNewTaskDialog({ selector: NEW_BRANCH_DIALOG }),
        LoadCode({ ref: name, keep: "" }),
      ],
    }),

    ClickedRefresh: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, wantedRef: askedFor(model).ref } },
      commands: [LoadCode({ ref: askedFor(model).ref, keep: askedFor(model).keep })],
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

    SucceededLoadHistory: ({ path, rows }) =>
      answersPanel(model, path)
        ? {
            model: {
              ...model,
              codeScreen: { ...model.codeScreen, history: AsyncData.succeed(rows) },
            },
          }
        : { model },

    FailedLoadHistory: ({ path, reason }) =>
      answersPanel(model, path)
        ? {
            model: {
              ...model,
              codeScreen: { ...model.codeScreen, history: AsyncData.fail(reason) },
            },
          }
        : { model },

    /**
     * Show the open file as it was at this commit — a read-only look back.
     *
     * The blob goes with the revision. The pane is keyed on what it is showing
     * and a revision's text is the same length as another's often enough —
     * a typo fix, a version bump — that the key would not change and the
     * mounted viewer, whose text is captured once, would keep painting the
     * revision the banner says the reader has left. Blanking it also removes
     * the frame where the tip's text sits under a historic commit's name.
     */
    ClickedHistoryRow: ({ oid }) => {
      const view = viewOf(model);
      if (view.selected === null) return { model };
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            mode: "view",
            at: oid,
            diffing: false,
            view: AsyncData.succeed({ ...view, content: null }),
          },
        },
        commands: [LoadFileAt({ oid, path: view.selected })],
      };
    },

    ClickedBackToTip: () => {
      const view = viewOf(model);
      if (view.selected === null || view.tip === null) return { model };
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            at: null,
            view: AsyncData.succeed({ ...view, content: null }),
          },
        },
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
            session: model.codeScreen.session + 1,
          },
        },
      };
    },

    ClickedNewFile: () => {
      const view = viewOf(model);
      if (!writableCode(view)) return { model };
      // Leaving a historical revision means leaving its text with it. Clearing
      // `at` alone took the read-only banner away and left the old blob in the
      // pane — Cancel then the pencil would then have opened that revision as a
      // draft of the tip, and committing it would have written the old file
      // over the new one.
      const lookingBack = model.codeScreen.at !== null && view.selected !== null;
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
            view: lookingBack
              ? AsyncData.succeed({ ...view, content: null })
              : model.codeScreen.view,
            // The panel closes with it: its rows belong to the file that was
            // open, and clicking one from here would leave a card that is
            // creating a file and viewing another at once — with the button
            // that would close the panel disabled precisely because a file is
            // being created. The revision goes too: a file that does not exist
            // yet is not being looked back at, and leaving `at` set put the
            // history banner's "read-only" over a working Commit button.
            panel: "none",
            history: AsyncData.Idle(),
            at: null,
            // A second "+" is a second session over the same key, and the
            // editor's text was captured at mount — without this the pane
            // would keep the abandoned draft while the Model holds none.
            session: model.codeScreen.session + 1,
          },
        },
        commands:
          lookingBack && view.selected !== null && view.tip !== null
            ? [LoadFile({ tip: view.tip, path: view.selected })]
            : [],
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

    /**
     * The write landed on `branch`, and that is the ref re-read.
     *
     * Unless the reader has moved on: a branch chosen while the commit was out
     * has its own read in flight, and `wantedRef` is what lets that read paint.
     * Overwriting it would supersede the switch with the branch the reader has
     * left, so the commit's reload is simply dropped instead — the branch they
     * are now on is the one being loaded, and it is loading already.
     */
    SucceededCommitFile: ({ branch, keep }) => {
      const stale = askedFor(model).ref !== branch;
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            saving: false,
            wantedRef: stale ? model.codeScreen.wantedRef : branch,
          },
        },
        commands: stale ? [] : [LoadCode({ ref: branch, keep })],
      };
    },

    FailedCommitFile: ({ reason }) => ({
      model: { ...model, codeScreen: { ...model.codeScreen, saving: false, editError: reason } },
    }),

    ToggledDiffReview: () => ({
      model: { ...model, codeScreen: { ...model.codeScreen, diffing: !model.codeScreen.diffing } },
    }),

    ClickedClone: ({ text }) => ({
      model,
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

    CompletedSync: ({ notice, reload }) => {
      // A push or a fetch can settle while a branch switch is still out, so
      // the reload asks for what is actually wanted rather than for the
      // sample's ref. See `askedFor`.
      const asked = askedFor(model);
      return {
        model: {
          ...model,
          codeScreen: {
            ...model.codeScreen,
            syncing: false,
            syncNotice: notice,
            wantedRef: reload ? asked.ref : model.codeScreen.wantedRef,
          },
        },
        commands: [
          RefreshSync({ ref: asked.ref }),
          ...(reload ? [LoadCode({ ref: asked.ref, keep: asked.keep })] : []),
        ],
      };
    },

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
     *
     * Everything that reads the repository is re-read, not just Code. The
     * timeline was drawn from origin at boot and a live query answered from
     * it, so leaving them alone would have three screens disagreeing about
     * one repository — the browser's own commits visible in Code and missing
     * from both the search and the history that should hold them.
     */
    SettledLocalRepository: ({ state }) => {
      if (state !== "Ready") return { model: { ...model, localRepository: state } };
      const pattern = model.query.trim();
      const asked = askedFor(model);
      const reading = model.activityScreen.wanted + 1;
      return {
        model: {
          ...model,
          localRepository: state,
          codeScreen: { ...model.codeScreen, wantedRef: asked.ref },
          activityScreen: { ...model.activityScreen, wanted: reading },
        },
        commands: [
          LoadCode({ ref: asked.ref, keep: asked.keep }),
          LoadCommits({ wanted: reading }),
          ...(pattern === "" ? [] : [Grep({ pattern })]),
          // The clone and `/whoami` race, and either can land first. Signing
          // is re-applied here so a clone that opened second still commits as
          // the reader rather than as the anonymous browser default.
          SignLocalAs({ subject: subjectOf(model) }),
        ],
      };
    },

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
 * Whether an answer asked about `id` still belongs on the screen.
 *
 * A hub round-trip outlives the click that started it, and `ChangedUrl` clears
 * the Detail screen without cancelling what is still out. Without this, a
 * refusal about one Change Request is announced against the one the reader
 * opened next, and that one's half-written drafts are cleared by it.
 */
const answersOpen = (model: Model, id: string): boolean =>
  model.route._tag === "Detail" && model.route.id === id;

/**
 * Whether a file read's answer is still the revision the reader is asking for.
 *
 * `LoadFile` and `LoadFileAt` are both interruptible, but Foldkit keys an
 * interrupt by Command *name*, so neither supersedes the other: clicking
 * "Back to tip" while a history read is out leaves both in flight. The path
 * alone cannot tell them apart — they are the same file — so the answer
 * carries its revision, empty for the tip, and only the one that matches what
 * the click already recorded may paint.
 */
const answersRevision = (model: Model, oid: string): boolean =>
  (oid === "" ? null : oid) === model.codeScreen.at;

/**
 * Whether a history answer belongs to the panel that is open.
 *
 * The branch panel asks with an empty path and the file panel with the open
 * file's, and the two are the same Command — so the answer says which it is
 * and the other is dropped rather than painted under the wrong heading.
 */
const answersPanel = (model: Model, path: string): boolean =>
  path === ""
    ? model.codeScreen.panel === "commits"
    : model.codeScreen.panel === "filelog" && path === viewOf(model).selected;

/**
 * What a re-read of the Code screen should ask for.
 *
 * The screen's own ref and open file once it has them, and what the request
 * already in flight asked for while it does not. `viewOf` answers an
 * un-settled screen with the design's sample shape, whose ref is the design's
 * own `main` — and re-reading *that* would supersede the boot request with a
 * branch this repository may not have. `LoadCode` is interruptible and its
 * answer is dropped unless it names `wantedRef`, so the real read would never
 * land: the screen would settle empty, with no explorer, no commit bar and
 * nothing said about why.
 */
const askedFor = (model: Model) => {
  const held = AsyncData.getData(model.codeScreen.view);
  // A settled *sample* is not the repository. Opening a file in the offline
  // explorer stores the sample as a success so the click is not inert, and its
  // ref is the design's own `main` — asking for that is the same mistake as
  // asking for PENDING's.
  if (held._tag === "Some" && !held.value.offline) {
    return { ref: held.value.ref, keep: held.value.selected ?? "" };
  }
  return {
    ref: model.codeScreen.wantedRef,
    keep: model.route._tag === "Code" ? model.route.path : "",
  };
};

/**
 * The inputs a landed action has consumed.
 *
 * Keyed on what the action actually filled in — `settings.ts`'s `filledBy` —
 * rather than on the card it reports into, because a card holds more than one
 * action: a card whose form is a standing choice (which branch to move, what
 * the policy is) keeps it, one that files something new empties so the next
 * entry starts blank, and a read-only action beside either of them empties
 * nothing at all.
 */
/** The card, as an answer would fill it. */
const policySeed = (data: SettingsData): PolicyForm => ({
  policyProtected: data.policy?.rules.protected.join(", ") ?? "",
  policyApprovals: String(data.policy?.rules.requiredApprovals ?? 0),
  policyChecks: data.policy?.rules.requiredChecks.join(", ") ?? "",
  policyRequirePullRequest: data.policy?.rules.requirePullRequest ?? false,
  policyRequireResolvedThreads: data.policy?.rules.requireResolvedThreads ?? false,
});

/** The card, as it stands. */
const policyOf = (forms: Model["settingsScreen"]["forms"]): PolicyForm => ({
  policyProtected: forms.policyProtected,
  policyApprovals: forms.policyApprovals,
  policyChecks: forms.policyChecks,
  policyRequirePullRequest: forms.policyRequirePullRequest,
  policyRequireResolvedThreads: forms.policyRequireResolvedThreads,
});

/**
 * Whether the card still holds, character for character, what an answer put
 * there.
 *
 * The plain reading of "nobody has touched this", and the one a reload uses to
 * decide whether the card may follow the repository. Character for character
 * because a box being typed into passes through states that *mean* the same
 * thing — `main, ` on the way to `main, release/*` is still the list `main` —
 * and re-seeding one of those pulls the text out from under the caret.
 */
const sameSpelling = (forms: PolicyForm, seed: PolicyForm): boolean =>
  forms.policyProtected === seed.policyProtected &&
  forms.policyApprovals === seed.policyApprovals &&
  forms.policyChecks === seed.policyChecks &&
  forms.policyRequirePullRequest === seed.policyRequirePullRequest &&
  forms.policyRequireResolvedThreads === seed.policyRequireResolvedThreads;

/**
 * Whether the card and an answer say the same thing, however either is spelled.
 *
 * The two sides are written by different code: `policySeed` renders a list as
 * `join(", ")` and the count through `String`, while the publish path reads the
 * same boxes through `Settings.list` and `Settings.count`. So an operator who
 * typed `main,release/*` without the space and published it got the answer back
 * spelled `main, release/*` — one character different, which the spelling test
 * above reads as "still typing". Left at that the card stopped following the
 * repository for the rest of the session, and the next publish wrote its stale
 * list back over whatever anyone else had added.
 */
const samePolicy = (forms: PolicyForm, seed: PolicyForm): boolean => {
  const same = (left: string, right: string): boolean => {
    const one = Settings.list(left);
    const other = Settings.list(right);
    return one.length === other.length && one.every((entry, at) => entry === other[at]);
  };
  return (
    same(forms.policyProtected, seed.policyProtected) &&
    Settings.count(forms.policyApprovals) === Settings.count(seed.policyApprovals) &&
    same(forms.policyChecks, seed.policyChecks) &&
    forms.policyRequirePullRequest === seed.policyRequirePullRequest &&
    forms.policyRequireResolvedThreads === seed.policyRequireResolvedThreads
  );
};

const clearedForms = (
  forms: Model["settingsScreen"]["forms"],
  filled: string,
): Model["settingsScreen"]["forms"] => {
  switch (filled) {
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
