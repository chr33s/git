/**
 * The transitions the product depends on, driven through `update` itself.
 *
 * These replace eight browser tests that bundled Lit elements, injected a
 * client into them and drove the DOM to reach the same rules. The rules were
 * always about state: a refusal belongs to the request it answered, a draft
 * survives a refused send, a stale answer cannot overwrite a newer one, a
 * clone that lands is worth re-reading through. Foldkit's Story runner drives
 * those transitions with no browser at all, which is faster to run and — more
 * to the point — readable as the statements about behaviour they always were.
 *
 * What those tests also proved, and this cannot, is that a real browser and a
 * real server agree: `verify.ts` does that, against the built UI, and its
 * checks were kept and extended rather than replaced.
 *
 * Commands are resolved by name here rather than executed, so nothing reaches
 * the network and each story says exactly which answer it is testing.
 */
import { describe, it } from "@effect/vitest";
import { AsyncData, Story } from "foldkit";
import assert from "node:assert/strict";

import { AppMessage } from "./app.message.ts";
import type { Model } from "./app.model.ts";
import { AppRoute } from "./app.route.ts";
import { update } from "./app.update.ts";
import {
  CommentRemote,
  HydrateDetail,
  MergeRemote,
  TaskAction,
  ThreadAction,
} from "./app.command.detail.ts";
import { CommitFile, LoadCode, LoadFile, LoadFileAt, RefreshSync } from "./app.command.code.ts";
import { CreateTask, LoadTasks } from "./app.command.ts";
import { CloseNewTaskDialog, Navigate } from "./app.command.shell.ts";
import type { Task } from "./model.ts";

const task = (over: Partial<Task> = {}): Task => ({
  id: "CR-14",
  kind: "CR",
  title: "Add auth middleware",
  status: "In review",
  avatar: "RB",
  desc: "",
  assignees: [],
  labels: [],
  comments: [],
  updated: "1h ago",
  hub: true,
  reviewHead: "a".repeat(40),
  sourceRef: "topic/auth",
  targetRef: "main",
  diffStat: "+1 −1",
  commitCount: "2",
  diffFile: "src/server/Api.ts",
  commits: [],
  checks: [],
  review: { headline: "Ready", detail: "", ok: true, action: "Merge" },
  diff: [],
  threads: [{ id: "thread", path: null, resolved: false, comments: [] }],
  ...over,
});

/** A Model with one hub Change Request open, which is what most of these need. */
const openOn = (route: AppRoute, tasks: readonly Task[] = [task()]): Model => ({
  route,
  repo: "core",
  cloneUrl: "http://localhost/core",
  theme: "dark",
  themePinned: false,
  viewer: AsyncData.Idle(),
  localRepository: "Unavailable",
  query: "",
  navError: null,
  railCollapsed: false,
  tasks: { tasks, sessions: [], liveNotice: null, load: AsyncData.Idle() },
  tasksScreen: { filter: "all", title: "", desc: "", parent: "", submitting: false },
  searchScreen: { code: AsyncData.Idle() },
  activityScreen: { zoom: "week", offset: 0, commits: AsyncData.Idle() },
  settingsScreen: {
    data: AsyncData.Idle(),
    browserKey: null,
    notes: {},
    busy: false,
    reflog: null,
    forms: {
      resetRef: "",
      resetTo: "",
      tagName: "",
      tagMessage: "",
      remoteName: "",
      remoteUrl: "",
      remoteCredential: "",
      webhookUrl: "",
      webhookSecret: "",
      policyProtected: "",
      policyApprovals: "0",
      policyChecks: "",
      policyRequirePullRequest: false,
      policyRequireResolvedThreads: false,
    },
  },
  detailScreen: {
    tab: "conversation",
    diffFor: "CR-14",
    diff: AsyncData.Idle(),
    comment: "",
    replies: {},
    acting: false,
    notice: null,
    taskNotice: null,
    moveNotice: null,
  },
  codeScreen: {
    view: AsyncData.succeed({
      ref: "main",
      defaultBranch: "main",
      branches: ["main"],
      paths: ["README.md", "src/a.ts", "src/b.ts"],
      selected: "README.md",
      content: "# hi",
      head: null,
      tip: "b".repeat(40),
      offline: false,
      reason: "",
    }),
    wantedRef: "main",
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
    copied: false,
    sync: null,
    syncing: false,
    syncNotice: null,
    newBranch: "",
    proposeTitle: "",
    proposeDescription: "",
    bisect: null,
  },
});

describe("the discussion", () => {
  it("keeps a refused comment where the reader can still send it", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Detail({ id: "CR-14" }))),
      Story.message(AppMessage.ChangedCommentDraft({ text: "worth another look" })),
      Story.message(AppMessage.SubmittedComment()),
      Story.Command.expectHas(CommentRemote),
      // The draft is the reader's until the hub takes it. Clearing on submit
      // would lose the text on exactly the refusal that needs it kept.
      Story.model((model) => {
        assert.equal(model.detailScreen.comment, "worth another look");
        assert.equal(model.detailScreen.acting, true);
      }),
      Story.Command.resolve(
        CommentRemote,
        AppMessage.FailedComment({ reason: "the hub refused the comment" }),
      ),
      Story.model((model) => {
        assert.equal(model.detailScreen.comment, "worth another look");
        assert.equal(model.detailScreen.notice, "the hub refused the comment");
        assert.equal(model.detailScreen.acting, false);
      }),
    );
  });

  it("clears the draft only once the hub has it, and re-reads the projection", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Detail({ id: "CR-14" }))),
      Story.message(AppMessage.ChangedCommentDraft({ text: "shipping this" })),
      Story.message(AppMessage.SubmittedComment()),
      Story.Command.resolve(CommentRemote, AppMessage.SucceededComment()),
      Story.model((model) => {
        assert.equal(model.detailScreen.comment, "");
        assert.equal(model.detailScreen.notice, null);
      }),
      // What shows next is the repository's answer, not one drawn from here.
      Story.Command.expectHas(LoadTasks),
      Story.Command.resolve(LoadTasks, AppMessage.FailedLoadTasks({ reason: "not in this test" })),
    );
  });

  it("keeps a refused reply in the thread it was written for", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Detail({ id: "CR-14" }))),
      Story.message(AppMessage.ChangedThreadDraft({ thread: "thread", text: "and this line?" })),
      Story.message(AppMessage.SubmittedThreadReply({ thread: "thread" })),
      Story.Command.resolve(
        ThreadAction,
        AppMessage.FailedThread({ reason: "the hub refused the thread update" }),
      ),
      Story.model((model) => {
        assert.equal(model.detailScreen.replies["thread"], "and this line?");
        assert.equal(model.detailScreen.notice, "the hub refused the thread update");
      }),
    );
  });
});

describe("a refusal belongs to what it answered", () => {
  it("does not tell a reader that the Change Request they just opened was refused", () => {
    Story.story(
      update,
      Story.given(
        openOn(AppRoute.Detail({ id: "CR-14" }), [task(), task({ id: "CR-15", title: "Other" })]),
      ),
      Story.message(AppMessage.ClickedMerge()),
      Story.Command.expectHas(MergeRemote),
      // The hub refuses, and the reader is still on the Change Request that
      // asked — so the refusal is theirs to see.
      Story.Command.resolve(
        MergeRemote,
        AppMessage.FailedMerge({ reason: "the hub refused the merge" }),
      ),
      Story.model((model) => {
        assert.equal(model.detailScreen.notice, "the hub refused the merge");
      }),
      // Moving on takes the refusal with it: it was about the other one.
      Story.message(AppMessage.ChangedUrl({ route: AppRoute.Detail({ id: "CR-15" }) })),
      Story.Command.resolve(HydrateDetail, AppMessage.CompletedNavigate()),
      Story.model((model) => {
        assert.equal(model.detailScreen.notice, null);
        assert.equal(model.detailScreen.acting, false);
        assert.equal(model.detailScreen.diffFor, "CR-15");
      }),
    );
  });

  it("clears a task-action notice when the next one is attempted", () => {
    Story.story(
      update,
      Story.given(
        openOn(AppRoute.Detail({ id: "T-1" }), [task({ kind: "Task", id: "T-1", status: "Todo" })]),
      ),
      Story.message(AppMessage.ClickedTaskAction({ action: "claim" })),
      Story.Command.resolve(
        TaskAction,
        AppMessage.FailedTaskAction({ reason: "the hub refused the task update" }),
      ),
      Story.model((model) => {
        assert.equal(model.detailScreen.taskNotice, "the hub refused the task update");
      }),
      Story.message(AppMessage.ClickedTaskAction({ action: "claim" })),
      // A retry that still showed the last refusal would read as this attempt
      // having failed before it was answered.
      Story.model((model) => {
        assert.equal(model.detailScreen.taskNotice, null);
        assert.equal(model.detailScreen.acting, true);
      }),
      Story.Command.resolve(TaskAction, AppMessage.SucceededTaskAction()),
      Story.Command.resolve(LoadTasks, AppMessage.FailedLoadTasks({ reason: "not in this test" })),
    );
  });
});

describe("the Tasks composer", () => {
  it("files a refused task in this tab, under the parent the dialog was told", () => {
    Story.story(
      update,
      Story.given(
        openOn(AppRoute.Tasks(), [task({ id: "T-1", kind: "Task", hub: undefined, children: [] })]),
      ),
      Story.message(AppMessage.ChangedNewTaskTitle({ title: "Write it down" })),
      Story.message(AppMessage.ChangedNewTaskParent({ parent: "T-1" })),
      Story.message(AppMessage.SubmittedNewTask()),
      Story.Command.resolve(CreateTask, AppMessage.FellBackCreateTask({ id: "T-2" })),
      Story.Command.resolve(
        CloseNewTaskDialog,
        AppMessage.CompletedCloseNewTaskDialog({ outcome: "Closed" }),
      ),
      Story.Command.resolve(Navigate, AppMessage.CompletedNavigate()),
      Story.model((model) => {
        const filed = model.tasks.tasks.find((held) => held.id === "T-2");
        assert.equal(filed?.title, "Write it down");
        // The two paths agree about where a task the dialog was told to file
        // ends up: under T-1, whether the hub took it or this tab kept it.
        assert.equal(filed?.parent, "T-1");
        assert.deepEqual(model.tasks.tasks.find((held) => held.id === "T-1")?.children, ["T-2"]);
        assert.equal(model.tasksScreen.title, "");
      }),
    );
  });
});

describe("the Code screen", () => {
  it("refuses a commit that lost the race rather than overwriting it", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Code({ path: "README.md" }))),
      Story.message(AppMessage.ClickedEdit()),
      Story.message(AppMessage.ChangedFileDraft({ text: "# edited" })),
      Story.message(AppMessage.ClickedSave()),
      Story.Command.expectHas(CommitFile),
      Story.Command.resolve(
        CommitFile,
        AppMessage.FailedCommitFile({
          reason: "someone else committed to main while you were editing",
        }),
      ),
      Story.model((model) => {
        assert.equal(model.codeScreen.saving, false);
        assert.match(model.codeScreen.editError ?? "", /someone else committed/);
        // The draft is still here, which is the whole point of saying so.
        assert.equal(model.codeScreen.draft, "# edited");
        assert.equal(model.codeScreen.mode, "edit");
      }),
    );
  });

  it("keeps the tree and the file at the commit the reader followed back to", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Code({ path: "README.md" }))),
      Story.message(AppMessage.ClickedHistoryRow({ oid: "c".repeat(40) })),
      Story.Command.expectHas(LoadFileAt),
      Story.Command.resolve(
        LoadFileAt,
        AppMessage.SucceededLoadFileAt({
          oid: "c".repeat(40),
          path: "README.md",
          content: "# older",
        }),
      ),
      Story.model((model) => {
        assert.equal(model.codeScreen.at, "c".repeat(40));
        // Read-only: an old revision is not a place to write from.
        assert.equal(model.codeScreen.mode, "view");
      }),
      Story.message(AppMessage.ClickedBackToTip()),
      Story.Command.expectHas(LoadFile),
      Story.Command.resolve(
        LoadFile,
        AppMessage.SucceededLoadFileAt({ oid: "", path: "README.md", content: "# hi" }),
      ),
      Story.model((model) => {
        assert.equal(model.codeScreen.at, null);
      }),
    );
  });

  it("drops an older branch answer rather than painting it over the newer one", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Code({ path: "" }))),
      Story.message(AppMessage.SelectedBranch({ ref: "topic/one" })),
      Story.message(AppMessage.SelectedBranch({ ref: "topic/two" })),
      Story.Command.expectHas(LoadCode),
      // The slow first request lands last. It names the ref it asked about,
      // and that is not the ref the reader is now on.
      Story.Command.resolveAll(
        [
          LoadCode,
          AppMessage.SucceededLoadCode({
            ref: "topic/one",
            view: {
              ref: "topic/one",
              defaultBranch: "main",
              branches: ["main"],
              paths: [],
              selected: null,
              content: null,
              head: null,
              tip: null,
              offline: false,
              reason: "",
            },
          }),
        ],
        [
          LoadCode,
          AppMessage.SucceededLoadCode({
            ref: "topic/two",
            view: {
              ref: "topic/two",
              defaultBranch: "main",
              branches: ["main", "topic/two"],
              paths: ["a.ts"],
              selected: "a.ts",
              content: "",
              head: null,
              tip: "d".repeat(40),
              offline: false,
              reason: "",
            },
          }),
        ],
        [RefreshSync, AppMessage.SucceededRefreshSync({ sync: null })],
      ),
      Story.model((model) => {
        assert.equal(model.codeScreen.wantedRef, "topic/two");
        // The newer answer is the one on screen; the older one was dropped.
        assert.equal(AsyncData.getOrElse(model.codeScreen.view, () => null)?.ref, "topic/two");
      }),
    );
  });
});

describe("the browser's own repository", () => {
  it("re-reads the screen through the local clone once it is open", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Code({ path: "" }))),
      Story.message(AppMessage.SettledLocalRepository({ state: "Ready" })),
      Story.model((model) => {
        assert.equal(model.localRepository, "Ready");
      }),
      // The clone answers a hundred-commit history from local objects rather
      // than an N+1 of requests, so the screen is worth reading again — and
      // `wantedRef` is cleared so that read's answer is the one that paints.
      Story.Command.expectHas(LoadCode),
      Story.Command.resolveAll([
        LoadCode,
        AppMessage.FellBackLoadCode({ ref: "", reason: "not in this test" }),
      ]),
    );
  });

  it("changes nothing when the browser cannot hold one", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Code({ path: "" }))),
      Story.message(AppMessage.SettledLocalRepository({ state: "Unavailable" })),
      Story.model((model) => {
        assert.equal(model.localRepository, "Unavailable");
      }),
      // The HTTP client keeps answering and the page does not flicker: no
      // reload, which is the documented behaviour rather than a failure.
      Story.Command.expectNone(),
    );
  });
});

describe("a hub Change Request's detail", () => {
  it("replaces the listing row it was filled in from", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Detail({ id: "CR-14" }))),
      Story.message(
        AppMessage.SucceededHydrate({
          task: task({ desc: "the whole description", comments: [] }),
        }),
      ),
      Story.model((model) => {
        const held = model.tasks.tasks.find((entry) => entry.id === "CR-14");
        assert.equal(held?.desc, "the whole description");
        // One task, not two: the detail *is* the listing row now, so the
        // Tasks list and the detail screen cannot disagree about it.
        assert.equal(model.tasks.tasks.length, 1);
      }),
    );
  });
});

describe("what a listing row omits", () => {
  it("asks for the open Change Request's detail again after every re-read", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Detail({ id: "CR-14" }))),
      // The projection is re-read after every write, and it carries rows, not
      // detail — so adopting it over a hydrated task would blank the screen
      // the reader is looking at.
      Story.message(AppMessage.SucceededLoadTasks({ tasks: [task({ desc: "" })], sessions: [] })),
      Story.Command.expectHas(HydrateDetail),
      Story.Command.resolve(
        HydrateDetail,
        AppMessage.SucceededHydrate({ task: task({ desc: "the whole description" }) }),
      ),
      Story.model((model) => {
        assert.equal(
          model.tasks.tasks.find((held) => held.id === "CR-14")?.desc,
          "the whole description",
        );
      }),
    );
  });
});

describe("the editor", () => {
  it("will not open over a file whose blob has not arrived", () => {
    const model = openOn(AppRoute.Code({ path: "README.md" }));
    Story.story(
      update,
      Story.given({
        ...model,
        codeScreen: {
          ...model.codeScreen,
          // What `ClickedFile` leaves behind while the request is out.
          view: AsyncData.succeed({
            ...(AsyncData.getOrElse(model.codeScreen.view, () => null) ?? {
              ref: "main",
              defaultBranch: "main",
              branches: ["main"],
              paths: ["README.md"],
              selected: "README.md",
              content: null,
              head: null,
              tip: "b".repeat(40),
              offline: false,
              reason: "",
            }),
            content: null,
          }),
        },
      }),
      Story.message(AppMessage.ClickedEdit()),
      // Opening here would seed the draft empty, and committing it would
      // overwrite the file with nothing.
      Story.model((held) => {
        assert.equal(held.codeScreen.mode, "view");
      }),
      Story.Command.expectNone(),
    );
  });
});

describe("the sample repository", () => {
  it("opens the files it holds rather than doing nothing", () => {
    const base = openOn(AppRoute.Code({ path: "" }));
    Story.story(
      update,
      Story.given({
        ...base,
        // No tip: this is the design's sample, which is what an unreachable
        // server leaves showing.
        codeScreen: { ...base.codeScreen, view: AsyncData.fail("not reachable") },
      }),
      Story.message(AppMessage.ClickedFile({ path: "src/components/Button.tsx" })),
      Story.model((model) => {
        const held = AsyncData.getOrElse(model.codeScreen.view, () => null);
        assert.equal(held?.selected, "src/components/Button.tsx");
        assert.notEqual(held?.content, null);
      }),
      Story.Command.resolveAll([Navigate, AppMessage.CompletedNavigate()]),
    );
  });
});

describe("the address", () => {
  it("resets the detail screen when it names a different Change Request", () => {
    Story.story(
      update,
      Story.given({
        ...openOn(AppRoute.Detail({ id: "CR-14" })),
        detailScreen: {
          tab: "diff",
          diffFor: "CR-14",
          diff: AsyncData.succeed([]),
          comment: "half written",
          replies: { thread: "half written" },
          acting: false,
          notice: "an old refusal",
          taskNotice: null,
          moveNotice: null,
        },
      }),
      Story.message(AppMessage.ChangedUrl({ route: AppRoute.Detail({ id: "CR-15" }) })),
      Story.model((model) => {
        // None of the previous Change Request's state may appear under the
        // next one's title: not the tab, not the diff, not the drafts, and
        // above all not the refusal, which was about something else.
        assert.equal(model.detailScreen.tab, "conversation");
        assert.equal(model.detailScreen.diffFor, "CR-15");
        assert.equal(model.detailScreen.comment, "");
        assert.deepEqual(model.detailScreen.replies, {});
        assert.equal(model.detailScreen.notice, null);
      }),
      Story.Command.expectHas(HydrateDetail),
      Story.Command.resolve(HydrateDetail, AppMessage.CompletedNavigate()),
    );
  });

  it("explains a malformed address instead of failing to start", () => {
    Story.story(
      update,
      Story.given(openOn(AppRoute.Tasks())),
      Story.message(AppMessage.ChangedUrl({ route: AppRoute.NotFound({ path: "/hub/code/%E0" }) })),
      Story.model((model) => {
        assert.match(model.navError ?? "", /malformed/);
      }),
      Story.message(AppMessage.ChangedUrl({ route: AppRoute.Tasks() })),
      // A new address is a new question; the old explanation goes with it.
      Story.model((model) => {
        assert.equal(model.navError, null);
      }),
    );
  });
});
