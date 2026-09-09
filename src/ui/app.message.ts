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
  /**
   * The box, as it reads now.
   *
   * Every keystroke, so the Model never lags the DOM. Foldkit re-asserts a
   * controlled `value` onto the element on every patch, and a Model that only
   * heard the debounced query would rewrite the box back to it — silently
   * deleting whatever had been typed since the last pause.
   */
  ChangedSearchDraft: { query: Schema.String },
  /** The query the reader has stopped typing. This is the one that searches. */
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
  /**
   * File contents matching `pattern`.
   *
   * The query travels with the answer because `interrupt` does not supersede:
   * Foldkit registers an interrupt key, and nothing is cancelled unless
   * `update` returns an Interrupt Command. Two searches are therefore in
   * flight whenever a reader keeps typing, and the broader — slower — one
   * would otherwise land last and list hits for a query that is no longer in
   * the box.
   */
  SucceededGrep: {
    pattern: Schema.String,
    matches: Schema.Array(CodeHit),
    truncated: Schema.Boolean,
  },
  /** The server could not answer. The task half still can, and says so. */
  FailedGrep: { pattern: Schema.String, reason: Schema.String },

  // -- the Activity screen ----------------------------------------------
  ChangedTimelineZoom: { zoom: Schema.Literals(["day", "week", "month"]) },
  /** ‹ and ›, which page the window by its own width. */
  ClickedTimelineEarlier: {},
  ClickedTimelineLater: {},
  SucceededLoadCommits: { wanted: Schema.Finite, commits: Schema.Array(TimelineCommit) },
  /** No history to draw, so the design's sample timeline stands in. */
  FailedLoadCommits: { wanted: Schema.Finite, reason: Schema.String },

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
    /**
     * Which form this action consumed, or `""`.
     *
     * The card is not the answer: Fetch, Push, Pull and Delete all report into
     * the Remotes card without reading its Add form, and a reader who checked
     * a row must not lose the remote — credential included — they were part
     * way through typing beside it.
     */
    filled: Schema.String,
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
  /** The confirmation has been on screen long enough; offer the copy again. */
  ForgotCopied: {},

  // -- the Detail screen ------------------------------------------------
  ChangedDetailTab: { tab: Schema.Literals(["conversation", "diff", "commits", "checks"]) },
  SucceededLoadDiff: { id: Schema.String, files: Schema.Array(LoadedDiff) },
  /**
   * The diff could not be read; `reason` is why, in the server's own words
   * where it answered, and the design's own diff stands in.
   */
  FellBackLoadDiff: { id: Schema.String, reason: Schema.String },
  CompletedMountDiff: { path: Schema.String },
  ChangedCommentDraft: { text: Schema.String },
  SubmittedComment: {},
  /**
   * The comment landed.
   *
   * `id` and `body` are what it was asked about and what it said. A hub
   * round-trip outlives the click that started it, so an answer has to name
   * its subject: one that arrives after the reader opened another Change
   * Request must not clear that one's half-written draft.
   */
  SucceededComment: { id: Schema.String, body: Schema.String },
  FailedComment: { id: Schema.String, reason: Schema.String },
  ClickedMerge: {},
  SucceededMerge: { id: Schema.String },
  /** Refused or unreachable: the Change Request stays open, and says why. */
  FailedMerge: { id: Schema.String, reason: Schema.String },
  ClickedReview: { decision: Schema.Literals(["approve", "reject"]) },
  ClickedThreadResolve: { thread: Schema.String, resolved: Schema.Boolean },
  ChangedThreadDraft: { thread: Schema.String, text: Schema.String },
  SubmittedThreadReply: { thread: Schema.String },
  /**
   * The thread update landed.
   *
   * `body` is empty for `resolve` and `reopen`, which is what keeps them from
   * touching a draft at all: only the reply that was actually sent clears the
   * text it was sent with.
   */
  SucceededThread: { id: Schema.String, thread: Schema.String, body: Schema.String },
  FailedThread: { id: Schema.String, reason: Schema.String },
  ClickedTaskAction: { action: Schema.Literals(["claim", "release", "complete", "abandon"]) },
  SucceededTaskAction: { id: Schema.String },
  FailedTaskAction: { id: Schema.String, reason: Schema.String },
  ChangedTaskParent: { parent: Schema.String },
  /** One Change Request's detail, filled in from the hub. */
  SucceededHydrate: { task: Task },

  // -- the Code screen --------------------------------------------------
  SucceededLoadCode: { ref: Schema.String, view: CodeView },
  /** The repository could not be read; the design's sample stands in. */
  FellBackLoadCode: { ref: Schema.String, reason: Schema.String },
  ClickedFile: { path: Schema.String },
  /**
   * One file's text, as of `oid` — empty for the tip.
   *
   * The revision travels with the answer because `LoadFile` and `LoadFileAt`
   * do not supersede each other: Foldkit keys an interrupt by Command name,
   * so a "back to tip" read and the history read it replaces are in flight
   * together, and only the one the reader is still asking for may paint.
   */
  SucceededLoadFileAt: { oid: Schema.String, path: Schema.String, content: Schema.String },
  FailedLoadFileAt: { oid: Schema.String, path: Schema.String, reason: Schema.String },
  SelectedBranch: { ref: Schema.String },
  ChangedNewBranch: { name: Schema.String },
  /** Cancel: the dialog has no trigger to press again, so it is closed here. */
  ClickedCancelNewBranch: {},
  SubmittedNewBranch: {},
  SucceededCreateBranch: { name: Schema.String },
  ClickedRefresh: {},
  ClickedCommitPanel: {},
  ClickedFileLogPanel: {},
  /**
   * One history panel's rows — the branch's when `path` is empty, one file's
   * otherwise.
   *
   * The path travels with the answer because both panels read through the same
   * Command and `interrupt` cancels nothing: opening one and then the other
   * leaves two reads in flight, and the branch read is twenty round trips
   * against the file read's one, so it routinely lands last. Without this the
   * whole branch's commits paint under "File history".
   */
  SucceededLoadHistory: { path: Schema.String, rows: Schema.Array(HistoryRow) },
  FailedLoadHistory: { path: Schema.String, reason: Schema.String },
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
  /**
   * The commit landed on `branch`, and the screen re-reads that ref.
   *
   * The branch travels with the answer because the reader may have switched
   * away while the write was out: re-reading whatever `viewOf` says would ask
   * the design sample's own `main` for a screen that is still loading, and
   * `LoadCode` drops every answer but the newest, so the switch would never
   * paint.
   */
  SucceededCommitFile: { branch: Schema.String, keep: Schema.String },
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
