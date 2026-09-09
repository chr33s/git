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
import { Option } from "effect";
import { AsyncData, Story } from "foldkit";
import assert from "node:assert/strict";

import { AppMessage } from "./app.message.ts";
import type { Model, SettingsData, SettingsFailure } from "./app.model.ts";
import { AppRoute } from "./app.route.ts";
import { update } from "./app.update.ts";
import { fingerprint, viewOf } from "./code.ts";
import { fromLegacyHash } from "./route.ts";
import { AdminAction } from "./settings.ts";
import {
  CommentRemote,
  HydrateDetail,
  MergeRemote,
  TaskAction,
  ThreadAction,
} from "./app.command.detail.ts";
import {
  CommitFile,
  LoadCode,
  LoadFile,
  LoadFileAt,
  RefreshSync,
  SignLocalAs,
} from "./app.command.code.ts";
import { CreateTask, LoadCommits, LoadTasks } from "./app.command.ts";
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
  activityScreen: { zoom: "week", offset: 0, commits: AsyncData.Idle(), wanted: 0 },
  settingsScreen: {
    data: AsyncData.Idle(),
    browserKey: null,
    notes: {},
    busy: false,
    policyPublished: null,
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
      pending: false,
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
    session: 0,
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
        AppMessage.FailedComment({ id: "CR-14", reason: "the hub refused the comment" }),
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
      Story.Command.resolve(
        CommentRemote,
        AppMessage.SucceededComment({ id: "CR-14", body: "shipping this" }),
      ),
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
        AppMessage.FailedThread({ id: "CR-14", reason: "the hub refused the thread update" }),
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
        AppMessage.FailedMerge({ id: "CR-14", reason: "the hub refused the merge" }),
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
        AppMessage.FailedTaskAction({ id: "T-1", reason: "the hub refused the task update" }),
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
      Story.Command.resolve(TaskAction, AppMessage.SucceededTaskAction({ id: "T-1" })),
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
              pending: false,
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
              pending: false,
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
      // The timeline goes with it: it was drawn from origin at boot, and
      // leaving it there would have two screens disagreeing about one
      // repository.
      // Signing goes with it: the clone and `/whoami` race, and a clone that
      // opened second would otherwise commit as the anonymous browser default.
      Story.Command.expectHas(LoadCode, LoadCommits, SignLocalAs),
      Story.Command.resolveAll(
        [LoadCode, AppMessage.FellBackLoadCode({ ref: "", reason: "not in this test" })],
        [LoadCommits, AppMessage.FailedLoadCommits({ wanted: 1, reason: "not in this test" })],
        [SignLocalAs, AppMessage.CompletedNavigate()],
      ),
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

describe("an answer belongs to the Change Request that asked", () => {
  /**
   * Driven through `update` directly rather than through a Story.
   *
   * The rule under test is what happens while a Command is still out — the
   * reader navigates, or keeps typing, before the hub answers — and the Story
   * runner insists every dispatched Command is resolved before the next
   * Message. Folding the Messages by hand is the only way to put them in the
   * order that actually breaks this.
   */
  const fold = (start: Model, ...messages: readonly AppMessage[]): Model =>
    messages.reduce((held, message) => update(held, message).model, start);

  const two = [
    task({ hub: false, id: "CR-14" }),
    task({ hub: false, id: "CR-19", title: "Other", threads: [] }),
  ];

  it("marks the Change Request that was merged, not the one now open", () => {
    const after = fold(
      openOn(AppRoute.Detail({ id: "CR-14" }), two),
      AppMessage.ClickedMerge(),
      AppMessage.ChangedUrl({ route: AppRoute.Detail({ id: "CR-19" }) }),
      // The fixture merge answers after the reader has moved on. Stamping the
      // route's task would close a Change Request nobody merged and leave the
      // merged one open.
      AppMessage.SucceededMerge({ id: "CR-14" }),
    );
    const merged = after.tasks.tasks.find((held) => held.id === "CR-14");
    const other = after.tasks.tasks.find((held) => held.id === "CR-19");
    assert.equal(merged?.review?.merged, true);
    assert.equal(other?.review?.merged, undefined);
    assert.equal(other?.status, "In review");
  });

  it("does not put one Change Request's refusal on another, or take its draft", () => {
    const after = fold(
      openOn(AppRoute.Detail({ id: "CR-14" }), two),
      AppMessage.ClickedMerge(),
      AppMessage.ChangedUrl({ route: AppRoute.Detail({ id: "CR-19" }) }),
      AppMessage.ChangedCommentDraft({ text: "half a thought" }),
      AppMessage.FailedMerge({ id: "CR-14", reason: "the hub refused the merge" }),
    );
    assert.equal(after.detailScreen.notice, null);
    assert.equal(after.detailScreen.comment, "half a thought");
  });

  it("keeps a comment typed since the one that landed was sent", () => {
    const after = fold(
      openOn(AppRoute.Detail({ id: "CR-14" })),
      AppMessage.ChangedCommentDraft({ text: "sent" }),
      AppMessage.SubmittedComment(),
      // The reader keeps typing while the hub decides. What comes back is an
      // answer about the text that was sent, not a licence to empty the box.
      AppMessage.ChangedCommentDraft({ text: "second thoughts" }),
      AppMessage.SucceededComment({ id: "CR-14", body: "sent" }),
    );
    assert.equal(after.detailScreen.comment, "second thoughts");
  });

  it("does not discard a reply draft when another thread is resolved", () => {
    const after = fold(
      openOn(AppRoute.Detail({ id: "CR-14" })),
      AppMessage.ChangedThreadDraft({ thread: "other", text: "still writing" }),
      AppMessage.ClickedThreadResolve({ thread: "thread", resolved: true }),
      // A resolve carries no body, so it has nothing to clear.
      AppMessage.SucceededThread({ id: "CR-14", thread: "thread", body: "" }),
    );
    assert.equal(after.detailScreen.replies["other"], "still writing");
  });
});

describe("looking back at a file", () => {
  const opened = (): Model => {
    const base = openOn(AppRoute.Code({ path: "README.md" }));
    return {
      ...base,
      codeScreen: {
        ...base.codeScreen,
        view: AsyncData.succeed({
          ref: "main",
          defaultBranch: "main",
          branches: ["main"],
          paths: ["README.md"],
          selected: "README.md",
          content: "# tip",
          head: null,
          tip: "b".repeat(40),
          offline: false,
          pending: false,
          reason: "",
        }),
      },
    };
  };

  it("does not return the reader to a revision they left", () => {
    // Both reads are interruptible, but Foldkit keys an interrupt by Command
    // name, so "back to tip" does not supersede the history read: both answer,
    // and the slower one must not paint over the newer one.
    const after = [
      AppMessage.ClickedHistoryRow({ oid: "c".repeat(40) }),
      AppMessage.ClickedBackToTip(),
      AppMessage.SucceededLoadFileAt({ oid: "", path: "README.md", content: "# tip" }),
      AppMessage.SucceededLoadFileAt({
        oid: "c".repeat(40),
        path: "README.md",
        content: "# as it was",
      }),
    ].reduce((held, message) => update(held, message).model, opened());
    assert.equal(after.codeScreen.at, null);
    assert.equal(AsyncData.getOrElse(after.codeScreen.view, () => null)?.content, "# tip");
  });
});

describe("a clone that opens before the first read answers", () => {
  it("does not supersede the boot request with the sample's ref", () => {
    // The OPFS clone needs no network on a repeat visit, so it can settle
    // while the boot `LoadCode` is still out. Re-reading through `viewOf` at
    // that moment would ask for the design's own "main" — a branch this
    // repository may not have — and `LoadCode` is interruptible, so the real
    // answer would never land.
    const base = openOn(AppRoute.Code({ path: "src/server/Api.ts" }));
    const start: Model = {
      ...base,
      codeScreen: { ...base.codeScreen, view: AsyncData.Loading(), wantedRef: "" },
    };
    const { model: after, commands } = update(
      start,
      AppMessage.SettledLocalRepository({ state: "Ready" }),
    );
    assert.equal(after.codeScreen.wantedRef, "");
    const read = (commands ?? []).find((command) => command.name === "LoadCode");
    assert.deepEqual(read?.args, { ref: "", keep: "src/server/Api.ts" });
  });
});

describe("an answer that is no longer the question", () => {
  /**
   * `interrupt` registers a key; it does not cancel.
   *
   * Foldkit stops a Command only when `update` returns an Interrupt, so every
   * read dispatched is still running. These pin the guards that stand in for
   * the cancellation the comments used to claim.
   */
  it("keeps the hits for the query that is in the box", () => {
    const after = [
      AppMessage.ChangedSearchQuery({ query: "co" }),
      AppMessage.ChangedSearchQuery({ query: "commit" }),
      // The broader query is the slower one, and lands last.
      AppMessage.SucceededGrep({
        pattern: "co",
        matches: [{ path: "a.ts", line: 1, text: "const" }],
        truncated: false,
      }),
    ].reduce((held, message) => update(held, message).model, openOn(AppRoute.Search()));
    assert.equal(after.query, "commit");
    assert.equal(
      AsyncData.getOrElse(after.searchScreen.code, () => null),
      null,
    );
  });

  it("keeps the timeline the clone answered, not the one origin did", () => {
    const base = openOn(AppRoute.Activity());
    const opened = update(base, AppMessage.SettledLocalRepository({ state: "Ready" })).model;
    const local = [{ oid: "a".repeat(40), subject: "mine", author: "me", at: new Date(0) }];
    const withLocal = update(
      opened,
      AppMessage.SucceededLoadCommits({ wanted: opened.activityScreen.wanted, commits: local }),
    ).model;
    // The boot read against origin lands afterwards. It knows nothing of the
    // commit the browser wrote, and must not put the timeline back.
    const after = update(
      withLocal,
      AppMessage.SucceededLoadCommits({ wanted: 0, commits: [] }),
    ).model;
    assert.deepEqual(
      AsyncData.getOrElse(after.activityScreen.commits, () => null),
      local,
    );
  });
});

describe("a commit that lands after the reader moved on", () => {
  const saving = (wantedRef: string): Model => {
    const base = openOn(AppRoute.Code({ path: "src/a.ts" }));
    return {
      ...base,
      codeScreen: { ...base.codeScreen, view: AsyncData.Loading(), wantedRef, saving: true },
    };
  };

  it("reloads the branch it was written to, not the sample's", () => {
    const { model: after, commands } = update(
      saving("topic/b"),
      AppMessage.SucceededCommitFile({ branch: "topic/b", keep: "src/a.ts" }),
    );
    assert.equal(after.codeScreen.wantedRef, "topic/b");
    const read = (commands ?? []).find((command) => command.name === "LoadCode");
    assert.deepEqual(read?.args, { ref: "topic/b", keep: "src/a.ts" });
  });

  it("leaves a branch switch made while the write was out alone", () => {
    // The reader picked another branch before the commit answered. That read
    // is what `wantedRef` names and what will paint; reloading the branch they
    // have left would supersede it and pull them back.
    const { model: after, commands } = update(
      saving("topic/c"),
      AppMessage.SucceededCommitFile({ branch: "topic/b", keep: "src/a.ts" }),
    );
    assert.equal(after.codeScreen.wantedRef, "topic/c");
    assert.equal(after.codeScreen.saving, false);
    assert.equal((commands ?? []).length, 0);
  });
});

describe("the two history panels", () => {
  const opened = (): Model => {
    const base = openOn(AppRoute.Code({ path: "README.md" }));
    return {
      ...base,
      codeScreen: {
        ...base.codeScreen,
        panel: "filelog",
        history: AsyncData.Loading(),
        view: AsyncData.succeed({
          ref: "main",
          defaultBranch: "main",
          branches: ["main"],
          paths: ["README.md"],
          selected: "README.md",
          content: "# tip",
          head: null,
          tip: "b".repeat(40),
          offline: false,
          pending: false,
          reason: "",
        }),
      },
    };
  };

  it("does not paint the branch's commits under File history", () => {
    // Both panels read through one Command and neither cancels the other, so
    // the branch read — twenty round trips against the file read's one —
    // routinely lands last.
    const after = update(
      opened(),
      AppMessage.SucceededLoadHistory({
        path: "",
        rows: [{ oid: "c".repeat(40), subject: "unrelated", author: "rb", when: "1h ago" }],
      }),
    ).model;
    assert.equal(AsyncData.isLoading(after.codeScreen.history), true);
  });

  it("paints the file's own answer", () => {
    const after = update(
      opened(),
      AppMessage.SucceededLoadHistory({
        path: "README.md",
        rows: [{ oid: "c".repeat(40), subject: "touched it", author: "rb", when: "1h ago" }],
      }),
    ).model;
    assert.equal(AsyncData.getOrElse(after.codeScreen.history, () => [])?.length, 1);
  });
});

describe("an address this page used before it moved", () => {
  it("routes an in-page legacy fragment to the screen it names", () => {
    // Foldkit's parsed `Url.hash` carries the fragment without its "#", while
    // `index.html`'s cold-load script reads `location.hash`, which keeps it.
    assert.equal(fromLegacyHash("/code/src/foo.ts"), "/hub/code/src/foo.ts");
    assert.equal(fromLegacyHash("#/code/src/foo.ts"), "/hub/code/src/foo.ts");
    assert.equal(fromLegacyHash("/not-a-screen"), null);
    assert.equal(fromLegacyHash(""), null);
  });
});

describe("the new-branch dialog", () => {
  it("closes on Cancel, which is the only affordance it has", () => {
    const { model: after, commands } = update(
      openOn(AppRoute.Code({ path: "" })),
      AppMessage.ClickedCancelNewBranch(),
    );
    assert.equal(after.codeScreen.newBranch, "");
    // Opened imperatively from a menu item, so there is no trigger to press
    // again and nothing else would dismiss it but Escape or the backdrop.
    assert.equal(
      (commands ?? []).some((command) => command.name === "CloseNewTaskDialog"),
      true,
    );
  });
});

describe("starting a new file over", () => {
  it("gives the second attempt its own editor", () => {
    // The editor's text is captured when it mounts, and the host is keyed so
    // that typing a path does not remount it — so the session is what has to
    // change when a session starts over, or the pane keeps the abandoned
    // draft while the Model holds none.
    const base = openOn(AppRoute.Code({ path: "README.md" }));
    const open: Model = {
      ...base,
      codeScreen: {
        ...base.codeScreen,
        view: AsyncData.succeed({
          ref: "main",
          defaultBranch: "main",
          branches: ["main"],
          paths: ["README.md"],
          selected: "README.md",
          content: "# tip",
          head: null,
          tip: "b".repeat(40),
          offline: false,
          pending: false,
          reason: "",
        }),
      },
    };
    // With the file-history panel open, so the assertion below has something
    // to close. Seeded rather than clicked: `ClickedFileLogPanel` dispatches a
    // read this story does not need to resolve.
    const browsing: Model = {
      ...open,
      codeScreen: {
        ...open.codeScreen,
        panel: "filelog",
        history: AsyncData.succeed([
          { oid: "c".repeat(40), subject: "earlier", author: "rb", when: "1h ago" },
        ]),
      },
    };
    const first = update(browsing, AppMessage.ClickedNewFile()).model;
    const typed = [
      AppMessage.ChangedNewFilePath({ path: "docs/notes.md" }),
      AppMessage.ChangedFileDraft({ text: "# Notes" }),
    ].reduce((held, message) => update(held, message).model, first);
    const second = update(typed, AppMessage.ClickedNewFile()).model;
    assert.equal(second.codeScreen.draft, "");
    assert.notEqual(second.codeScreen.session, first.codeScreen.session);
    // And the file-history panel does not stay open across it: its rows
    // belong to the file that was open, and the button that closes it is
    // disabled while a file is being created.
    assert.equal(second.codeScreen.panel, "none");
  });

  it("does not remount while the path is being typed", () => {
    const base = openOn(AppRoute.Code({ path: "README.md" }));
    const first = update(base, AppMessage.ClickedNewFile()).model;
    const typed = update(first, AppMessage.ChangedNewFilePath({ path: "d" })).model;
    assert.equal(typed.codeScreen.session, first.codeScreen.session);
  });
});

describe("looking back and coming forward", () => {
  const opened = (content: string): Model => {
    const base = openOn(AppRoute.Code({ path: "README.md" }));
    return {
      ...base,
      codeScreen: {
        ...base.codeScreen,
        view: AsyncData.succeed({
          ref: "main",
          defaultBranch: "main",
          branches: ["main"],
          paths: ["README.md"],
          selected: "README.md",
          content,
          head: null,
          tip: "b".repeat(40),
          offline: false,
          pending: false,
          reason: "",
        }),
      },
    };
  };

  it("does not leave one revision's text under another's name", () => {
    // The pane is keyed on what it is showing, and two revisions of a file are
    // the same length often enough — a typo fix, a version bump — that the key
    // would not change and the mounted viewer, whose text is captured once,
    // would keep painting the revision the reader has left.
    const back = update(
      opened("# tip"),
      AppMessage.ClickedHistoryRow({ oid: "c".repeat(40) }),
    ).model;
    assert.equal(AsyncData.getOrElse(back.codeScreen.view, () => null)?.content, null);
    // Coming forward is the same hazard in reverse, so the older revision's
    // text has to arrive first for the assertion to mean anything.
    const showing = update(
      back,
      AppMessage.SucceededLoadFileAt({
        oid: "c".repeat(40),
        path: "README.md",
        content: "# was",
      }),
    ).model;
    assert.equal(AsyncData.getOrElse(showing.codeScreen.view, () => null)?.content, "# was");
    const forward = update(showing, AppMessage.ClickedBackToTip()).model;
    assert.equal(AsyncData.getOrElse(forward.codeScreen.view, () => null)?.content, null);
  });

  it("leaves a revision's text behind when a new file is started from it", () => {
    // Clearing `at` alone took the read-only banner away and left the old blob
    // in the pane: Cancel, then the pencil, would have opened that revision as
    // a draft of the tip, and committing it would have written the old file
    // over the new one.
    const back = update(
      opened("# tip"),
      AppMessage.ClickedHistoryRow({ oid: "c".repeat(40) }),
    ).model;
    const showing = update(
      back,
      AppMessage.SucceededLoadFileAt({
        oid: "c".repeat(40),
        path: "README.md",
        content: "# was",
      }),
    ).model;
    const creating = update(showing, AppMessage.ClickedNewFile());
    assert.equal(creating.model.codeScreen.at, null);
    assert.equal(AsyncData.getOrElse(creating.model.codeScreen.view, () => null)?.content, null);
    // And the tip is read back, so cancelling returns to the file as it is.
    assert.equal(
      (creating.commands ?? []).some((command) => command.name === "LoadFile"),
      true,
    );
  });

  it("a new file is not a look back", () => {
    // The history banner's "read-only" over a working Commit button told the
    // reader their work could not be saved when it could.
    const back = update(
      opened("# tip"),
      AppMessage.ClickedHistoryRow({ oid: "c".repeat(40) }),
    ).model;
    assert.equal(back.codeScreen.at, "c".repeat(40));
    const creating = update(back, AppMessage.ClickedNewFile()).model;
    assert.equal(creating.codeScreen.at, null);
  });
});

describe("opening a screen that reads", () => {
  it("reads Settings again, so a refusal is recoverable", () => {
    // Every control on a failed Settings screen is disabled, so nothing the
    // reader can click would ask again — granting the key and coming back has
    // to be enough.
    const refused: Model = (() => {
      const base = openOn(AppRoute.Code({ path: "" }));
      return {
        ...base,
        settingsScreen: { ...base.settingsScreen, data: AsyncData.fail("Denied") },
      };
    })();
    const opened = update(refused, AppMessage.ChangedUrl({ route: AppRoute.Settings() }));
    assert.equal(AsyncData.isLoading(opened.model.settingsScreen.data), true);
    assert.equal(
      (opened.commands ?? []).some((command) => command.name === "LoadSettings"),
      true,
    );
  });

  it("reads the timeline again, so a commit made this session is on it", () => {
    const base = openOn(AppRoute.Code({ path: "" }));
    const opened = update(base, AppMessage.ChangedUrl({ route: AppRoute.Activity() }));
    // The counter moves with it, so the earlier read's answer cannot paint
    // over the newer one.
    assert.notEqual(opened.model.activityScreen.wanted, base.activityScreen.wanted);
    const read = (opened.commands ?? []).find((command) => command.name === "LoadCommits");
    assert.deepEqual(read?.args, { wanted: opened.model.activityScreen.wanted });
  });

  it("leaves the other screens alone", () => {
    const base = openOn(AppRoute.Code({ path: "" }));
    const opened = update(base, AppMessage.ChangedUrl({ route: AppRoute.Tasks() }));
    assert.equal(opened.model.activityScreen.wanted, base.activityScreen.wanted);
    assert.equal((opened.commands ?? []).length, 0);
  });

  it("keeps the answer it already has while it asks again", () => {
    // Dropping to `Loading` discarded it, and the cards read no data as "this
    // repository has none": for the whole round trip Settings said a repo with
    // branches had none, and that its policy could not be read.
    const loaded: Model = (() => {
      const base = openOn(AppRoute.Code({ path: "" }));
      return {
        ...base,
        settingsScreen: {
          ...base.settingsScreen,
          data: AsyncData.succeed<SettingsData, SettingsFailure>({
            branches: [],
            defaultBranch: "main",
            tags: [],
            remotes: [],
            webhooks: [],
            policy: null,
          }),
        },
      };
    })();
    const opened = update(loaded, AppMessage.ChangedUrl({ route: AppRoute.Settings() }));
    assert.equal(AsyncData.isRefreshing(opened.model.settingsScreen.data), true);
    const held = AsyncData.getData(opened.model.settingsScreen.data);
    assert.equal(Option.isSome(held) && held.value.defaultBranch === "main", true);
    assert.equal(
      (opened.commands ?? []).some((command) => command.name === "LoadSettings"),
      true,
    );
  });

  it("asks for the browser's key again when the first read did not answer", () => {
    // The one card that makes a refused screen recoverable — copy this key,
    // grant it, come back — was read once at boot and never again.
    const base = openOn(AppRoute.Code({ path: "" }));
    const opened = update(base, AppMessage.ChangedUrl({ route: AppRoute.Settings() }));
    assert.equal(base.settingsScreen.browserKey, null);
    assert.equal(
      (opened.commands ?? []).some((command) => command.name === "LoadBrowserKey"),
      true,
    );
  });

  it("does not read again when the address did not move", () => {
    // The rail's items stay clickable on the screen they name, and `pushUrl`
    // dispatches whether or not the address changed. Re-reading on that click
    // put the server's policy back over one the reader had typed.
    const onIt = update(
      openOn(AppRoute.Code({ path: "" })),
      AppMessage.ChangedUrl({ route: AppRoute.Settings() }),
    ).model;
    const again = update(onIt, AppMessage.ChangedUrl({ route: AppRoute.Settings() }));
    assert.deepEqual(again.model.settingsScreen, onIt.settingsScreen);
    assert.equal((again.commands ?? []).length, 0);
  });

  it("does not stack a second read on one already in flight", () => {
    const base = openOn(AppRoute.Code({ path: "" }));
    const asking = update(base, AppMessage.ChangedUrl({ route: AppRoute.Settings() })).model;
    // Leaving and coming back before the first answer lands.
    const away = update(asking, AppMessage.ChangedUrl({ route: AppRoute.Tasks() })).model;
    const back = update(away, AppMessage.ChangedUrl({ route: AppRoute.Settings() }));
    assert.equal(
      (back.commands ?? []).some((command) => command.name === "LoadSettings"),
      false,
    );
  });
});

describe("what tells one blob from another", () => {
  it("distinguishes two revisions of the same size", () => {
    // The pane rebuilds only when its key changes, and a length alone cannot
    // tell a typo fix from the text it replaced.
    assert.notEqual(fingerprint("# version 1.2.3"), fingerprint("# version 1.2.4"));
    assert.equal(fingerprint("# same"), fingerprint("# same"));
    assert.equal(fingerprint(null), "pending");
    assert.notEqual(fingerprint(""), fingerprint(null));
  });

  it("distinguishes two trees holding the same number of files", () => {
    // The explorer is keyed on this too. A rename leaves the count alone, and
    // a host that is not re-keyed is never re-mounted — so the tree went on
    // listing a file that no longer exists, whose row nothing would open.
    const before = ["src/Badge.tsx", "src/index.ts"];
    const after = ["src/Chip.tsx", "src/index.ts"];
    assert.notEqual(fingerprint(before.join("\n")), fingerprint(after.join("\n")));
    assert.equal(fingerprint(before.join("\n")), fingerprint([...before].join("\n")));
  });
});

describe("the clone URL's copy button", () => {
  it("says it copied, and only once the clipboard has it", () => {
    const clicked = update(
      openOn(AppRoute.Code({ path: "" })),
      AppMessage.ClickedClone({ text: "http://localhost/core" }),
    );
    // Not yet: a confirmation shown before the write settles is a claim the
    // page cannot make.
    assert.equal(clicked.model.codeScreen.copied, false);
    const copied = update(clicked.model, AppMessage.CompletedCopy());
    assert.equal(copied.model.codeScreen.copied, true);
    // And it ends on its own rather than sticking as a label.
    assert.equal(
      (copied.commands ?? []).some((command) => command.name === "ForgetCopied"),
      true,
    );
    assert.equal(update(copied.model, AppMessage.ForgotCopied()).model.codeScreen.copied, false);
  });
});

describe("the rail search box", () => {
  it("keeps up with the box between debounced searches", () => {
    // Foldkit re-asserts a controlled value on every patch, so a Model that
    // only heard the debounced query would rewrite the box back to it and
    // delete whatever had been typed since.
    const after = [
      AppMessage.ChangedSearchDraft({ query: "auth" }),
      AppMessage.ChangedSearchQuery({ query: "auth" }),
      AppMessage.ChangedSearchDraft({ query: "authentication" }),
    ].reduce((held, message) => update(held, message).model, openOn(AppRoute.Search()));
    assert.equal(after.query, "authentication");
    // And the answer to the query that has been typed past does not paint.
    const settled = update(
      after,
      AppMessage.SucceededGrep({ pattern: "auth", matches: [], truncated: false }),
    ).model;
    assert.equal(AsyncData.isLoading(settled.searchScreen.code), true);
  });

  it("searches only when the reader has stopped typing", () => {
    const drafted = update(
      openOn(AppRoute.Search()),
      AppMessage.ChangedSearchDraft({ query: "auth" }),
    );
    assert.equal((drafted.commands ?? []).length, 0);
    const searched = update(drafted.model, AppMessage.ChangedSearchQuery({ query: "auth" }));
    assert.equal(
      (searched.commands ?? []).some((command) => command.name === "Grep"),
      true,
    );
  });
});

describe("the Diff tab", () => {
  it("does not start a second read of a diff that is already loading", () => {
    const open = openOn(AppRoute.Detail({ id: "CR-14" }));
    const first = update(open, AppMessage.ChangedDetailTab({ tab: "diff" }));
    assert.equal(AsyncData.isLoading(first.model.detailScreen.diff), true);
    assert.equal(
      (first.commands ?? []).some((command) => command.name === "LoadDiff"),
      true,
    );
    // The tab stays clickable while it is the open one, and a diff reads both
    // sides of every changed file — so a second click must not run it again.
    const second = update(first.model, AppMessage.ChangedDetailTab({ tab: "diff" }));
    assert.equal((second.commands ?? []).length, 0);
  });
});

/** A Settings answer carrying just the policy the tests care about. */
const settings = (rules: {
  readonly protected: readonly string[];
  readonly approvals: number;
}): SettingsData => ({
  branches: [],
  defaultBranch: null,
  tags: [],
  remotes: [],
  webhooks: [],
  policy: {
    ref: null,
    rules: {
      protected: rules.protected,
      requiredApprovals: rules.approvals,
      requiredChecks: [],
      requireResolvedThreads: false,
      requirePullRequest: false,
      maxTrustAgeSeconds: 0,
      requireProvenance: false,
      maxUsageTokens: 0,
      usageWindowSeconds: 0,
      queueCandidates: false,
      queueDepth: 0,
      inbox: false,
    },
  },
});

describe("the Settings cards", () => {
  it("keeps a half-typed remote when a row's own action answers", () => {
    const base = openOn(AppRoute.Settings());
    const typed: Model = {
      ...base,
      settingsScreen: {
        ...base.settingsScreen,
        forms: {
          ...base.settingsScreen.forms,
          remoteName: "backup",
          remoteUrl: "https://git.example.com/backup",
          remoteCredential: "a-token",
        },
      },
    };
    // Fetch reports into the Remotes card without ever reading its Add form.
    const { model: after } = update(
      typed,
      AppMessage.SucceededAdmin({ card: "remotes", filled: "", note: "fetched", reflog: null }),
    );
    assert.equal(after.settingsScreen.forms.remoteName, "backup");
    assert.equal(after.settingsScreen.forms.remoteCredential, "a-token");

    // Adding one does consume it, and empties it for the next entry.
    const { model: added } = update(
      typed,
      AppMessage.SucceededAdmin({
        card: "remotes",
        filled: "remotes",
        note: "added",
        reflog: null,
      }),
    );
    assert.equal(added.settingsScreen.forms.remoteName, "");
    assert.equal(added.settingsScreen.forms.remoteCredential, "");
  });

  it("keeps a typed policy when an unrelated action reloads the screen", () => {
    // Eleven of the sixteen actions fill no form, and every one of them
    // reloads. A reload that re-seeded the card regardless snapped a
    // half-written protected-ref list back to the server's answer — and then
    // published that answer when the operator pressed the button.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(answered.settingsScreen.forms.policyProtected, "main");

    const typed: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: {
          ...answered.settingsScreen.forms,
          policyProtected: "main, release/*",
          policyApprovals: "2",
        },
      },
    };
    // "Show reflog" reports into Maintenance and reads no form at all.
    const reloaded = update(
      typed,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(reloaded.settingsScreen.forms.policyProtected, "main, release/*");
    assert.equal(reloaded.settingsScreen.forms.policyApprovals, "2");
  });

  it("still shows what the repository enforces when nobody has typed", () => {
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    // Someone else published a stricter policy; an untouched card follows it.
    const again = update(
      answered,
      AppMessage.SucceededLoadSettings({
        data: settings({ protected: ["main", "release/*"], approvals: 2 }),
      }),
    ).model;
    assert.equal(again.settingsScreen.forms.policyProtected, "main, release/*");
    assert.equal(again.settingsScreen.forms.policyApprovals, "2");
  });

  it("follows the repository again after publishing a list it spelled its own way", () => {
    // The publish path reads the box through `list`, so `main,release/*` is
    // published and answered as `main, release/*`. Comparing the spelling
    // rather than the meaning read that one space as "still typing" — forever,
    // so the card stopped following the repository for the whole session and
    // the next publish wrote its stale list back over anyone else's rule.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const published: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: { ...answered.settingsScreen.forms, policyProtected: "main,release/*" },
      },
    };
    const landed = update(
      published,
      AppMessage.SucceededLoadSettings({
        data: settings({ protected: ["main", "release/*"], approvals: 1 }),
      }),
    ).model;
    assert.equal(landed.settingsScreen.forms.policyProtected, "main, release/*");

    // And it is following again: someone else's later rule lands too.
    const others = update(
      landed,
      AppMessage.SucceededLoadSettings({
        data: settings({ protected: ["main", "release/*", "hotfix/*"], approvals: 1 }),
      }),
    ).model;
    assert.equal(others.settingsScreen.forms.policyProtected, "main, release/*, hotfix/*");
  });

  it("keeps showing the policy when a reload fails, and keeps the typed one too", () => {
    // Every action reloads, so a blip on that follow-up read blanked a screen
    // the application still held the answer for — and lost the seed the guard
    // above compares against, so the next good read wrote over what was typed.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const typed: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: { ...answered.settingsScreen.forms, policyProtected: "main, release/*" },
      },
    };
    const blipped = update(typed, AppMessage.FailedLoadSettings({ failure: "Offline" })).model;
    // The answer is still there to draw, and still marked as unconfirmed.
    assert.equal(Option.isSome(AsyncData.getData(blipped.settingsScreen.data)), true);
    assert.equal(Option.isSome(AsyncData.getError(blipped.settingsScreen.data)), true);

    const recovered = update(
      blipped,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(recovered.settingsScreen.forms.policyProtected, "main, release/*");
  });

  it("follows the repository again after publishing a spelling of what it already said", () => {
    // `main,` and `main` are the same list, so this publish stores nothing new
    // and the answer comes back unmoved. Read from the values alone that is
    // indistinguishable from an unrelated reload, and the card latched: it
    // stopped following for the session and its next publish would have
    // written `main` back over anyone else's rule.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const published: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: { ...answered.settingsScreen.forms, policyProtected: "main," },
      },
    };
    const acted = update(
      published,
      AppMessage.SubmittedAdmin({
        action: AdminAction.WritePolicy({
          protectedRefs: "main,",
          approvals: "1",
          checks: "",
          requirePullRequest: false,
          requireResolvedThreads: false,
        }),
      }),
    ).model;
    const landed = update(
      acted,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(landed.settingsScreen.forms.policyProtected, "main");
    assert.equal(landed.settingsScreen.policyPublished, null);

    // Following again: someone else's later rule lands.
    const others = update(
      landed,
      AppMessage.SucceededLoadSettings({
        data: settings({ protected: ["main", "hotfix/*"], approvals: 1 }),
      }),
    ).model;
    assert.equal(others.settingsScreen.forms.policyProtected, "main, hotfix/*");
  });

  it("does not let another card's action respell what is being typed", () => {
    // Only a policy publish spends the flag; the other fifteen actions reload
    // the same way and must leave the card alone.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const typing: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: { ...answered.settingsScreen.forms, policyProtected: "main, " },
      },
    };
    const acted = update(
      typing,
      AppMessage.SubmittedAdmin({ action: AdminAction.ShowReflog({ branch: "main" }) }),
    ).model;
    assert.equal(acted.settingsScreen.policyPublished, null);
    const landed = update(
      acted,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(landed.settingsScreen.forms.policyProtected, "main, ");
  });

  it("resyncs a publish whose own reload failed, once a read finally lands", () => {
    // The publish landed; only the read after it did not. Forgetting it there
    // stranded the card: the policy it published had not moved, so nothing in
    // the values could tell a later answer that the card was already level
    // with the repository, and it stopped following for the session.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const published: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: { ...answered.settingsScreen.forms, policyProtected: "main," },
      },
    };
    const acted = update(
      published,
      AppMessage.SubmittedAdmin({
        action: AdminAction.WritePolicy({
          protectedRefs: "main,",
          approvals: "1",
          checks: "",
          requirePullRequest: false,
          requireResolvedThreads: false,
        }),
      }),
    ).model;
    const blipped = update(acted, AppMessage.FailedLoadSettings({ failure: "Offline" })).model;
    const landed = update(
      blipped,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(landed.settingsScreen.forms.policyProtected, "main");
    assert.equal(landed.settingsScreen.policyPublished, null);
  });

  it("keeps a refused policy so it can be sent again", () => {
    // The card holds what it sent; the repository still enforces something
    // else. Reading "it holds what it published" as "it is level with the
    // repository" there threw away the rules the operator was about to retry.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const typed: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: {
          ...answered.settingsScreen.forms,
          policyProtected: "main, release/*",
          policyApprovals: "2",
        },
      },
    };
    const sent = update(
      typed,
      AppMessage.SubmittedAdmin({
        action: AdminAction.WritePolicy({
          protectedRefs: "main, release/*",
          approvals: "2",
          checks: "",
          requirePullRequest: false,
          requireResolvedThreads: false,
        }),
      }),
    ).model;
    const refused = update(sent, AppMessage.FailedAdmin({ card: "policy", note: "refused" })).model;
    // Any later read of the policy that is still in force leaves it alone.
    const read = update(
      refused,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(read.settingsScreen.forms.policyProtected, "main, release/*");
    assert.equal(read.settingsScreen.forms.policyApprovals, "2");
  });

  it("leaves a box typed into after a publish alone, however that publish ends", () => {
    // What was published is remembered as text, not as a flag, so it can only
    // ever respell itself: a box that has moved on since no longer matches it.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const acted = update(
      answered,
      AppMessage.SubmittedAdmin({
        action: AdminAction.WritePolicy({
          protectedRefs: "main",
          approvals: "1",
          checks: "",
          requirePullRequest: false,
          requireResolvedThreads: false,
        }),
      }),
    ).model;
    const blipped = update(acted, AppMessage.FailedLoadSettings({ failure: "Offline" })).model;
    const typing: Model = {
      ...blipped,
      settingsScreen: {
        ...blipped.settingsScreen,
        forms: { ...blipped.settingsScreen.forms, policyProtected: "main, " },
      },
    };
    const later = update(
      typing,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(later.settingsScreen.forms.policyProtected, "main, ");
  });

  it("leaves a box alone that is halfway through a word", () => {
    // `main, ` on the way to `main, release/*` still *means* the list `main`,
    // so a comparison by meaning alone called the card untouched and pulled
    // the text out from under the caret on the next unrelated reload.
    const answered = update(
      openOn(AppRoute.Settings()),
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    const typing: Model = {
      ...answered,
      settingsScreen: {
        ...answered.settingsScreen,
        forms: { ...answered.settingsScreen.forms, policyProtected: "main, " },
      },
    };
    const reloaded = update(
      typing,
      AppMessage.SucceededLoadSettings({ data: settings({ protected: ["main"], approvals: 1 }) }),
    ).model;
    assert.equal(reloaded.settingsScreen.forms.policyProtected, "main, ");
  });

  it("still says the screen is unreadable when there was never an answer", () => {
    const cold = update(
      openOn(AppRoute.Settings()),
      AppMessage.FailedLoadSettings({ failure: "Denied" }),
    ).model;
    assert.equal(Option.isNone(AsyncData.getData(cold.settingsScreen.data)), true);
    assert.equal(AsyncData.isFailure(cold.settingsScreen.data), true);
  });
});

describe("the Code screen before the repository answers", () => {
  it("says nothing about the server until it has answered", () => {
    const base = openOn(AppRoute.Code({ path: "" }));
    const view = viewOf({
      ...base,
      codeScreen: { ...base.codeScreen, view: AsyncData.Loading() },
    });
    // Pending is not fallen back: the sample's tree and commit are a claim
    // that the server could not be reached, and nothing has been claimed yet.
    assert.equal(view.pending, true);
    assert.deepEqual([...view.paths], []);
    assert.equal(view.head, null);
    assert.equal(view.reason, "");
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
              pending: false,
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
