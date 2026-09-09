/**
 * Every application event, named for what happened.
 *
 * The naming is the discipline: `ClickedTask`, not `openTask`; `SucceededCreateTask`,
 * not `taskCreated`. A Message says what the world did, and `update` decides
 * what that means — which is why the same `ClickedTask` serves the Tasks list,
 * the Search results and the Activity timeline without any of them knowing
 * about the others.
 *
 * Every asynchronous Command answers with a pair — `Succeeded…` and `Failed…` —
 * because a failure the Model cannot see is a failure the reader cannot see
 * either. There is no silent catch anywhere in this union.
 */
import { Message, Navigation } from "foldkit";
import { Schema } from "effect";

import { SessionRow, Task } from "./model.ts";
import { AppRoute } from "./app.route.ts";
import {
  CodeHit,
  CodeView,
  HistoryRow,
  LoadedDiff,
  LocalRepository,
  SettingsData,
  SettingsFailure,
  Theme,
  TimelineCommit,
  Viewer,
} from "./app.model.ts";
import { AdminAction } from "./settings.ts";
import { Outcome } from "./element.base-wc.ts";

export const AppMessage = Message.defineMessageUnion({
  // -- the shell --------------------------------------------------------
  /** The address changed: a link, Back, Forward, or a `pushUrl` of our own. */
  ChangedUrl: { route: AppRoute },
  /**
   * A link was clicked, before the address moves.
   *
   * Internal links are pushed so the page routes them; external ones are
   * loaded, which leaves the application. Intercepting rather than letting
   * the browser navigate is what keeps an in-page link from reloading the
   * bundle, and it is the runtime that hands this over.
   */
  RequestedUrl: { request: Navigation.UrlRequest },
  /** A rail item, a row, a card — anything that opens a screen. */
  ClickedNavigate: { route: AppRoute },
  ClickedTask: { id: Schema.String },
  ChangedSearchQuery: { query: Schema.String },
  ClickedThemeToggle: {},
  /** The logo, which collapses the rail to its icon strip and back. */
  ClickedRailToggle: {},
  /** The search row, and ⌘K, which both expand the rail and focus the input. */
  ClickedSearchField: {},
  PressedSearchShortcut: {},
  ChangedTheme: { theme: Theme },
  CompletedFocus: {},
  CompletedNavigate: {},

  // -- identity ---------------------------------------------------------
  SucceededFetchIdentity: { viewer: Viewer },
  FailedFetchIdentity: { reason: Schema.String },

  // -- the hub's projection ---------------------------------------------
  /** The hub answered: this is what the repository holds. */
  SucceededLoadTasks: { tasks: Schema.Array(Task), sessions: Schema.Array(SessionRow) },
  /** The hub refused to be read, and said why. Not the same as empty. */
  DeniedLoadTasks: { reason: Schema.String },
  /** The hub could not be reached. The fixtures stand, as documented. */
  FailedLoadTasks: { reason: Schema.String },

  // -- the Tasks screen -------------------------------------------------
  ChangedTaskFilter: { filter: Schema.Literals(["all", "tasks", "crs"]) },
  ChangedNewTaskTitle: { title: Schema.String },
  ChangedNewTaskDescription: { desc: Schema.String },
  ChangedNewTaskParent: { parent: Schema.String },
  SubmittedNewTask: {},
  ClickedCancelNewTask: {},
  /** The hub took it: `id` is the one the repository gave it. */
  SucceededCreateTask: { id: Schema.String },
  /** The hub would not take it, so it stays in this tab under `id`. */
  FellBackCreateTask: { id: Schema.String },
  SucceededMoveTask: { id: Schema.String, parent: Schema.String },
  FailedMoveTask: { id: Schema.String, reason: Schema.String },
  CompletedCloseNewTaskDialog: { outcome: Outcome },

  // -- the Search screen ------------------------------------------------
  ClickedCodeHit: { path: Schema.String },
  SucceededGrep: { matches: Schema.Array(CodeHit), truncated: Schema.Boolean },
  /** The server could not answer. The task half still can, and says so. */
  FailedGrep: { reason: Schema.String },

  // -- the Activity screen ----------------------------------------------
  ChangedTimelineZoom: { zoom: Schema.Literals(["day", "week", "month"]) },
  /** ‹ and ›, which page the window by its own width. */
  ClickedTimelineEarlier: {},
  ClickedTimelineLater: {},
  SucceededLoadCommits: { commits: Schema.Array(TimelineCommit) },
  /** No history to draw, so the design's sample timeline stands in. */
  FailedLoadCommits: { reason: Schema.String },

  // -- the Settings screen ----------------------------------------------
  SucceededLoadSettings: { data: SettingsData },
  FailedLoadSettings: { failure: SettingsFailure },
  SucceededLoadBrowserKey: {
    key: Schema.NullOr(
      Schema.Struct({
        fingerprint: Schema.String,
        publicKey: Schema.String,
        note: Schema.NullOr(Schema.String),
      }),
    ),
  },
  ChangedSettingsField: { field: Schema.String, value: Schema.String },
  ToggledSettingsField: { field: Schema.String, value: Schema.Boolean },
  SubmittedAdmin: { action: AdminAction },
  /** Including a refusal: the server answered, and the card must say what. */
  SucceededAdmin: {
    card: Schema.String,
    note: Schema.String,
    reflog: Schema.NullOr(
      Schema.Array(
        Schema.Struct({
          from: Schema.NullOr(Schema.String),
          to: Schema.NullOr(Schema.String),
          at: Schema.String,
          message: Schema.String,
        }),
      ),
    ),
  },
  /** The request never landed. */
  FailedAdmin: { card: Schema.String, note: Schema.String },
  ClickedCopyBrowserKey: { text: Schema.String },
  CompletedCopy: {},

  // -- the Detail screen ------------------------------------------------
  ChangedDetailTab: { tab: Schema.Literals(["conversation", "diff", "commits", "checks"]) },
  SucceededLoadDiff: { id: Schema.String, files: Schema.Array(LoadedDiff) },
  /** The refs are not in this repository; the design's own diff stands in. */
  FellBackLoadDiff: { id: Schema.String, reason: Schema.String },
  CompletedMountDiff: { path: Schema.String },
  ChangedCommentDraft: { text: Schema.String },
  SubmittedComment: {},
  SucceededComment: {},
  FailedComment: { reason: Schema.String },
  ClickedMerge: {},
  SucceededMerge: {},
  /** Refused or unreachable: the Change Request stays open, and says why. */
  FailedMerge: { reason: Schema.String },
  ClickedReview: { decision: Schema.Literals(["approve", "reject"]) },
  ClickedThreadResolve: { thread: Schema.String, resolved: Schema.Boolean },
  ChangedThreadDraft: { thread: Schema.String, text: Schema.String },
  SubmittedThreadReply: { thread: Schema.String },
  SucceededThread: {},
  FailedThread: { reason: Schema.String },
  ClickedTaskAction: { action: Schema.Literals(["claim", "release", "complete", "abandon"]) },
  SucceededTaskAction: {},
  FailedTaskAction: { reason: Schema.String },
  ChangedTaskParent: { parent: Schema.String },
  /** One Change Request's detail, filled in from the hub. */
  SucceededHydrate: { task: Task },

  // -- the Code screen --------------------------------------------------
  SucceededLoadCode: { ref: Schema.String, view: CodeView },
  /** The repository could not be read; the design's sample stands in. */
  FellBackLoadCode: { ref: Schema.String, reason: Schema.String },
  ClickedFile: { path: Schema.String },
  SucceededLoadFileAt: { oid: Schema.String, path: Schema.String, content: Schema.String },
  FailedLoadFileAt: { path: Schema.String, reason: Schema.String },
  SelectedBranch: { ref: Schema.String },
  ChangedNewBranch: { name: Schema.String },
  SubmittedNewBranch: {},
  SucceededCreateBranch: { name: Schema.String },
  ClickedRefresh: {},
  ClickedCommitPanel: {},
  ClickedFileLogPanel: {},
  SucceededLoadHistory: { rows: Schema.Array(HistoryRow) },
  FailedLoadHistory: { reason: Schema.String },
  ClickedHistoryRow: { oid: Schema.String },
  ClickedBackToTip: {},
  ClickedEdit: {},
  ClickedNewFile: {},
  ClickedCancelEdit: {},
  ChangedNewFilePath: { path: Schema.String },
  /** The editor reported what the reader typed; the Model holds the draft. */
  ChangedFileDraft: { text: Schema.String },
  ChangedCommitMessage: { message: Schema.String },
  ClickedSave: {},
  ClickedDeleteFile: {},
  SucceededCommitFile: { keep: Schema.String },
  FailedCommitFile: { reason: Schema.String },
  ToggledDiffReview: {},
  ClickedClone: { text: Schema.String },
  ClickedPush: {},
  ClickedFetch: {},
  SucceededRefreshSync: {
    sync: Schema.NullOr(
      Schema.Struct({
        ahead: Schema.Finite,
        behind: Schema.Finite,
        canPush: Schema.Boolean,
      }),
    ),
  },
  CompletedSync: { notice: Schema.NullOr(Schema.String), reload: Schema.Boolean },
  SettledLocalRepository: { state: LocalRepository },
  ChangedProposeField: { field: Schema.String, value: Schema.String },
  SubmittedPropose: {},
  SucceededPropose: { id: Schema.String },
  FailedPropose: { reason: Schema.String },
  ClickedRebase: {},
  ClickedCherryPick: { commit: Schema.String },
  MarkedBisect: { commit: Schema.String, as: Schema.Literals(["good", "bad"]) },
  SucceededBisect: {
    answer: Schema.Struct({
      kind: Schema.Literals(["test", "found"]),
      commit: Schema.String,
      steps: Schema.Finite,
    }),
  },
  ClickedResetBisect: {},
  CompletedMountTree: {},
});
export type AppMessage = typeof AppMessage.Type;
