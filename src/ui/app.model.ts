/**
 * The application's state, as one Schema.
 *
 * Every fact the UI shows is in here. That is the rule the migration is for:
 * a reader can answer "what is on screen and why" by looking at one value, and
 * DevTools can show that value at any point in the session's history.
 *
 * What is deliberately *not* in here: live handles. No DOM elements, no API
 * clients, no OPFS repository, no Pierre viewers, no abort controllers. Those
 * have lifetimes, and a lifetime is not a fact — they live in Commands,
 * Resources, ManagedResources and Mounts, and the Model holds only what is
 * true about them (`Ready`, `Failed`, a path, an id).
 *
 * Screen state is a nested record rather than a Foldkit Submodel. A Submodel
 * earns its wrapping when a screen owns Messages the shell must not see; here
 * every screen's work is the application's work — opening a Task from Search
 * is the same navigation as opening it from Tasks — so one Message union and
 * one `update` keep the transitions in one place to read.
 */
import { AsyncData } from "foldkit";
import { Schema } from "effect";

import * as Contract from "../server/ApiContract.ts";
import { SessionRow, Task } from "./model.ts";
import { AppRoute } from "./app.route.ts";

/** The palette in force. Mirrors `theme.ts`, which owns reading and storing it. */
export const Theme = Schema.Literals(["light", "dark"]);
export type Theme = typeof Theme.Type;

/**
 * Who the server says is asking — the whole `/whoami` answer.
 *
 * The rail shows only the subject, but the Settings identity card shows what
 * that identity may actually do: its capabilities, its grant's expiry, how
 * fresh its trust is, what budget it has left, and the verdict on each branch.
 * That matters now that the UI writes commits and moves refs, so the Model
 * holds the answer rather than a summary of it.
 */
export const Viewer = Contract.WhoamiAnswer;
export type Viewer = typeof Viewer.Type;

/**
 * The browser-held repository, as a lifecycle rather than a handle.
 *
 * `Ready` says a clone exists and the Code screen may use it; the handle
 * itself belongs to a ManagedResource, because it has to be released and
 * reacquired when the repository changes, which a value in the Model cannot do.
 */
export const LocalRepository = Schema.Literals(["Unavailable", "Opening", "Ready", "Failed"]);
export type LocalRepository = typeof LocalRepository.Type;

/** Everything the hub projects, and whether it has answered yet. */
export const TasksModel = Schema.Struct({
  tasks: Schema.Array(Task),
  sessions: Schema.Array(SessionRow),
  /**
   * Why live state is withheld — a refusal, not absence.
   *
   * Refusal is not emptiness: the fixtures are the documented offline sample,
   * and showing them for a private repository that turned this browser away
   * would dress a denial up as data. The list empties and this says why.
   */
  liveNotice: Schema.NullOr(Schema.String),
  load: AsyncData.Schema(Schema.Void, Schema.String).schema,
});
export type TasksModel = typeof TasksModel.Type;

/** The Tasks screen's own state: which segment, and the composer. */
export const TasksScreen = Schema.Struct({
  filter: Schema.Literals(["all", "tasks", "crs"]),
  title: Schema.String,
  desc: Schema.String,
  parent: Schema.String,
  submitting: Schema.Boolean,
});
export type TasksScreen = typeof TasksScreen.Type;

/**
 * One file-content hit, as the Search screen shows it.
 *
 * A copy of the API's `GrepMatch` rather than a reference to it: the Model is
 * the application's own vocabulary, and a transport type in it would make a
 * server-side rename a Model migration.
 */
export const CodeHit = Schema.Struct({
  path: Schema.String,
  line: Schema.Finite,
  text: Schema.String,
});
export type CodeHit = typeof CodeHit.Type;

/** What `POST /grep` answered, and whether it had to stop early. */
export const CodeHits = Schema.Struct({
  matches: Schema.Array(CodeHit),
  truncated: Schema.Boolean,
});
export type CodeHits = typeof CodeHits.Type;

/**
 * The Search screen.
 *
 * Only the code half has state: task matches are a pure query over the tasks
 * the Model already holds, so storing them would be storing a derivation.
 */
export const SearchScreen = Schema.Struct({
  code: AsyncData.Schema(CodeHits, Schema.String).schema,
});
export type SearchScreen = typeof SearchScreen.Type;

/**
 * One commit on the Activity timeline.
 *
 * The date is a `Date`, and the Model is a Schema, so the two have to agree:
 * `Schema.Date` is what makes a Model snapshot round-trip through DevTools
 * with the timestamp intact rather than as a string that only looks like one.
 */
export const TimelineCommit = Schema.Struct({
  oid: Schema.String,
  subject: Schema.String,
  author: Schema.String,
  at: Schema.Date,
});
export type TimelineCommit = typeof TimelineCommit.Type;

/**
 * The Activity screen.
 *
 * `commits` absent — a `Failure` — is what puts the design's sample timeline
 * on the grid, and the reason is what the notice above it says. That is a
 * different state from an empty success, which is a repository with no history
 * in this window and says so instead.
 */
export const ActivityScreen = Schema.Struct({
  zoom: Schema.Literals(["day", "week", "month"]),
  /** How many days back the window's last column sits; 0 means it ends today. */
  offset: Schema.Finite,
  commits: AsyncData.Schema(Schema.Array(TimelineCommit), Schema.String).schema,
});
export type ActivityScreen = typeof ActivityScreen.Type;

/**
 * What the administrative endpoints answered.
 *
 * One value rather than six fields, because they arrive together and a
 * half-loaded Settings screen is not a state the reader should ever see. The
 * registries that a reader may be refused individually — tags, remotes,
 * webhooks, policy — come back empty rather than absent: one refused card
 * must not blank the other five.
 */
export const SettingsData = Schema.Struct({
  branches: Schema.Array(Contract.Ref),
  defaultBranch: Schema.NullOr(Schema.String),
  tags: Schema.Array(Contract.Ref),
  remotes: Schema.Array(Contract.RemoteWire),
  webhooks: Schema.Array(Contract.WebhookWire),
  policy: Schema.NullOr(Contract.PolicyAnswer),
});
export type SettingsData = typeof SettingsData.Type;

/**
 * Why a card cannot answer.
 *
 * A refusal names the cure, an outage names the fault. Conflating the two sent
 * operators of private repositories debugging a network that was fine, so the
 * two are different values here rather than one boolean and a guess.
 */
export const SettingsFailure = Schema.Literals(["Denied", "Offline"]);
export type SettingsFailure = typeof SettingsFailure.Type;

/** What the reader has typed into the Settings forms. */
export const SettingsForms = Schema.Struct({
  resetRef: Schema.String,
  resetTo: Schema.String,
  tagName: Schema.String,
  tagMessage: Schema.String,
  remoteName: Schema.String,
  remoteUrl: Schema.String,
  remoteCredential: Schema.String,
  webhookUrl: Schema.String,
  webhookSecret: Schema.String,
  policyProtected: Schema.String,
  policyApprovals: Schema.String,
  policyChecks: Schema.String,
  policyRequirePullRequest: Schema.Boolean,
  policyRequireResolvedThreads: Schema.Boolean,
});
export type SettingsForms = typeof SettingsForms.Type;

export const SettingsScreen = Schema.Struct({
  data: AsyncData.Schema(SettingsData, SettingsFailure).schema,
  /**
   * The browser's own signing key, for the identity card.
   *
   * Absent until `identity.ts` is loaded and has answered — the key material
   * is derived lazily, and the boot path should not wait on it.
   */
  browserKey: Schema.NullOr(
    Schema.Struct({
      fingerprint: Schema.String,
      publicKey: Schema.String,
      note: Schema.NullOr(Schema.String),
    }),
  ),
  /** The last action's outcome, per card. An admin screen that swallows a
   *  policy refusal teaches its reader the wrong lesson. */
  notes: Schema.Record(Schema.String, Schema.String),
  /** One action at a time: the lists are reloaded after each. */
  busy: Schema.Boolean,
  /**
   * The default branch's reflog, once the reader asks for it.
   *
   * Declared here rather than reached for from the contract: `ReflogResponse`
   * names its entries inline, and the Model needs the element type alone.
   */
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
  forms: SettingsForms,
});
export type SettingsScreen = typeof SettingsScreen.Type;

/** One file's two sides, ready for `@pierre/diffs`. */
export const LoadedDiff = Schema.Struct({
  path: Schema.String,
  status: Schema.Literals(["added", "removed", "modified", "renamed"]),
  oldContents: Schema.NullOr(Schema.String),
  newContents: Schema.NullOr(Schema.String),
});
export type LoadedDiff = typeof LoadedDiff.Type;

/**
 * The Detail screen.
 *
 * `diff` keyed by the task it belongs to, not by nothing: a reader can open
 * another Change Request while the first is still resolving, and a diff that
 * did not say whose it was would flash under the wrong title.
 */
export const DetailScreen = Schema.Struct({
  tab: Schema.Literals(["conversation", "diff", "commits", "checks"]),
  diffFor: Schema.NullOr(Schema.String),
  diff: AsyncData.Schema(Schema.Array(LoadedDiff), Schema.String).schema,
  comment: Schema.String,
  /** One reply draft per thread, keyed by thread id. */
  replies: Schema.Record(Schema.String, Schema.String),
  /** One action at a time, and the reason the last one did not land. */
  acting: Schema.Boolean,
  notice: Schema.NullOr(Schema.String),
  taskNotice: Schema.NullOr(Schema.String),
  moveNotice: Schema.NullOr(Schema.String),
});
export type DetailScreen = typeof DetailScreen.Type;

/** The tip commit, as the commit bar shows it. */
export const HeadCommit = Schema.Struct({
  sha: Schema.String,
  message: Schema.String,
  author: Schema.String,
  avatar: Schema.String,
  when: Schema.String,
});
export type HeadCommit = typeof HeadCommit.Type;

/**
 * One coherent repository view.
 *
 * Committed only after every request in it succeeded: a failure at any step
 * drops the whole screen to the sample rather than showing a half-live tree
 * beside a sample README, which would be the more confusing outcome.
 */
export const CodeView = Schema.Struct({
  ref: Schema.String,
  defaultBranch: Schema.NullOr(Schema.String),
  branches: Schema.Array(Schema.String),
  paths: Schema.Array(Schema.String),
  selected: Schema.NullOr(Schema.String),
  content: Schema.NullOr(Schema.String),
  head: Schema.NullOr(HeadCommit),
  /** The tip's full oid — what `expected` pins when an edit commits. */
  tip: Schema.NullOr(Schema.String),
  /** Set when this is the design's sample rather than the repository. */
  offline: Schema.Boolean,
  reason: Schema.String,
});
export type CodeView = typeof CodeView.Type;

/** One row of a history panel. */
export const HistoryRow = Schema.Struct({
  oid: Schema.String,
  subject: Schema.String,
  author: Schema.String,
  when: Schema.String,
});
export type HistoryRow = typeof HistoryRow.Type;

/**
 * The Code screen.
 *
 * The editor's draft is here, not inside `@pierre/diffs`. The library owns the
 * surface a reader types on and reports every change back as a Message, so the
 * text that gets committed is the text the Model holds — which is the rule the
 * migration is for, and the difference between one source of truth and two.
 */
export const CodeScreen = Schema.Struct({
  view: AsyncData.Schema(CodeView, Schema.String).schema,
  /**
   * The ref the newest request asked for.
   *
   * An interrupted Command stops running, but a request already past its
   * `await` can still land — so the answer says which ref it is about and
   * this says which ref is wanted, and an older one is dropped rather than
   * painted under the newer one's heading. Empty means "whatever HEAD names",
   * which is what a first load asks for and no later one does.
   */
  wantedRef: Schema.String,
  mode: Schema.Literals(["view", "edit"]),
  /** The path being written, when the edit is of a file that does not exist. */
  newPath: Schema.NullOr(Schema.String),
  draft: Schema.String,
  message: Schema.String,
  saving: Schema.Boolean,
  editError: Schema.NullOr(Schema.String),
  /** Which history panel is open under the commit bar. */
  panel: Schema.Literals(["none", "commits", "filelog"]),
  history: AsyncData.Schema(Schema.Array(HistoryRow), Schema.String).schema,
  /** The commit being viewed, when the reader followed history back. */
  at: Schema.NullOr(Schema.String),
  /** Reviewing the draft against the blob rather than reading the file. */
  diffing: Schema.Boolean,
  copied: Schema.Boolean,
  /** Where the branch stands against origin — local client only. */
  sync: Schema.NullOr(
    Schema.Struct({
      ahead: Schema.Finite,
      behind: Schema.Finite,
      canPush: Schema.Boolean,
    }),
  ),
  syncing: Schema.Boolean,
  syncNotice: Schema.NullOr(Schema.String),
  /** The new-branch field in the branch menu. */
  newBranch: Schema.String,
  /** The Propose dialog's fields, and what the last attempt said. */
  proposeTitle: Schema.String,
  proposeDescription: Schema.String,
  /**
   * A bisect in progress: what has been marked, and what to test next.
   *
   * `null` until the reader marks something. Two marks — one good, one bad —
   * are what a step needs, so the answer only appears once both exist.
   */
  bisect: Schema.NullOr(
    Schema.Struct({
      good: Schema.Array(Schema.String),
      bad: Schema.NullOr(Schema.String),
      answer: Schema.NullOr(
        Schema.Struct({
          kind: Schema.Literals(["test", "found"]),
          commit: Schema.String,
          steps: Schema.Finite,
        }),
      ),
    }),
  ),
});
export type CodeScreen = typeof CodeScreen.Type;

export const Model = Schema.Struct({
  route: AppRoute,
  /** Which repository this build talks to — from the page's own meta tag. */
  repo: Schema.String,
  /** The URL `git clone` would be handed, which the Clone dialog shows. */
  cloneUrl: Schema.String,
  theme: Theme,
  /**
   * Whether the reader pinned that palette, or it is following the system.
   *
   * Two facts, not one: "dark" and "dark because the OS is" behave
   * differently when the OS changes its mind, and a single field would have
   * to guess. `app.subscription.ts` gates the media-query stream on this.
   */
  themePinned: Schema.Boolean,
  viewer: AsyncData.Schema(Viewer, Schema.String).schema,
  localRepository: LocalRepository,
  /** The rail's query, already debounced by `ui-search-field`. */
  query: Schema.String,
  /**
   * Whether the rail is the 64px strip.
   *
   * The rail used to own this and remember it itself. It is an application
   * fact — the frame every screen renders inside is a different width — so it
   * lives here, and `localStorage` is written by a Command rather than by a
   * component's `updated` hook.
   */
  railCollapsed: Schema.Boolean,
  /** A malformed address's explanation, shown above the screen it fell back to. */
  navError: Schema.NullOr(Schema.String),
  tasks: TasksModel,
  tasksScreen: TasksScreen,
  searchScreen: SearchScreen,
  activityScreen: ActivityScreen,
  settingsScreen: SettingsScreen,
  detailScreen: DetailScreen,
  codeScreen: CodeScreen,
});
export type Model = typeof Model.Type;

/**
 * Who the server said is asking, or `null` while unanswered or anonymous.
 *
 * Read through a function rather than stored twice: the answer is in `viewer`,
 * and a second field holding the same subject is a second field to keep true.
 */
export const subjectOf = (model: Model): string | null =>
  AsyncData.getOrElse(model.viewer, () => null)?.subject ?? null;

/** Who a new Task is authored by, when the hub will not take it. */
export const authorName = (model: Model): string => subjectOf(model) ?? "anonymous";
