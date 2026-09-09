/**
 * Code — the repository explorer and file view.
 *
 * `GET /files` answers every blob at a ref as a flat list of tree paths, and
 * `@pierre/trees` is path-first, so the response feeds it unchanged. Selecting
 * a row fetches that blob and renders it with `@pierre/diffs`, which brings
 * Shiki highlighting and a header matching the design's file card. Both
 * libraries own their subtrees, so both arrive through Mounts.
 *
 * The pane edits as well as views. The pencil attaches the package's edit mode
 * to the rendered file — the same highlighted surface, made writable — and the
 * explorer's "+" opens the same editor over a new path. Committing either pins
 * `expected` to the tip the editor opened at, so a commit that lands mid-edit
 * is a visible conflict rather than a silent overwrite.
 *
 * When the repository cannot be read the screen falls back to the design's own
 * sample tree and README and says so, which keeps the branch reviewable
 * without a running worker — and read-only, since there is nothing to write to.
 */
import { AsyncData, Html } from "foldkit";

import type { Elements } from "./element.base-wc.ts";
import * as icon from "./icon.ts";
import { AppMessage } from "./app.message.ts";
import type { CodeView, HeadCommit, Model } from "./app.model.ts";
import * as Code from "./code.ts";
import { viewOf } from "./code.ts";
import { PierreDiff } from "./mount.pierre-diff.ts";
import { PierreSource } from "./mount.pierre-source.ts";
import { PierreTree } from "./mount.pierre-tree.ts";

type H = Html.HtmlBuilder<AppMessage>;

/** A detached HEAD can be read or branched from, but commits need a branch. */
const writable = (view: CodeView): boolean =>
  !view.offline && Code.writableBranch(view.ref, view.branches, view.defaultBranch);

const explorer = (h: H, ui: Elements<AppMessage>, model: Model, view: CodeView): Html.Html =>
  h.div(
    [h.Class("gp-explorer")],
    [
      h.div(
        [h.Class("gp-explorer-head")],
        [
          h.div([h.Class("gp-explorer-title")], ["Explorer"]),
          h.div(
            [h.Class("gp-explorer-actions")],
            [
              h.button(
                [
                  h.Class("gp-icon-btn"),
                  h.Type("button"),
                  h.Title(
                    Code.unreachable(view)
                      ? "Read-only — the git+ API is not reachable"
                      : view.pending
                        ? "Reading the repository…"
                        : !writable(view)
                          ? "Select or create a branch to edit"
                          : "New file",
                  ),
                  h.AriaLabel("New file"),
                  h.Disabled(!writable(view)),
                  h.OnClick(AppMessage.ClickedNewFile()),
                ],
                [icon.plus(h)],
              ),
              ui.menu(
                [
                  h.Class("gp-explorer-menu"),
                  ui.menu.OnMenuSelect(() => AppMessage.ClickedRefresh()),
                ],
                [
                  h.button(
                    [
                      h.Class("gp-icon-btn"),
                      h.Type("button"),
                      h.DataAttribute("menu-trigger", ""),
                      h.Title("Explorer actions"),
                      h.AriaLabel("Explorer actions"),
                    ],
                    [icon.ellipsis(h)],
                  ),
                  ui.menuPopup(
                    [h.Class("gp-menu-popup")],
                    [
                      ui.menuItem(
                        [h.Class("gp-menu-item"), h.Attribute("value", "refresh")],
                        ["Refresh"],
                      ),
                    ],
                  ),
                ],
              ),
            ],
          ),
        ],
      ),
      // Keyed on the ref, the paths themselves and which repository this is: a
      // different tree is a different element, so the library builds a fresh
      // one rather than being asked to reconcile two repositories inside one
      // instance. `offline` is part of that — the sample and the real
      // repository can share a ref and a file list, and they do not share a
      // git status. `selected` deliberately is not: the tree owns its scroll
      // and its open folders, and rebuilding it on every click would throw
      // both away.
      //
      // The paths and not how many there are, because a rename leaves the
      // count alone and a host that is not re-keyed is never re-mounted — so
      // the explorer went on listing a file that no longer exists, whose row
      // `ClickedFile` then refuses because the path is not in the tree it is
      // checked against, while the file that replaced it had no row at all.
      h.keyed("div")(
        `${view.ref}:${Code.fingerprint(view.paths.join("\n"))}:${String(view.offline)}`,
        [
          h.Class("gp-explorer-tree"),
          h.OnMount(
            PierreTree({
              paths: view.paths,
              repo: view.offline ? "" : model.repo,
              selected: view.selected,
              offline: view.offline,
            }),
          ),
        ],
      ),
    ],
  );

const branchMenu = (h: H, ui: Elements<AppMessage>, model: Model, view: CodeView): Html.Html => {
  const trigger = [icon.branch(h), ` ${view.ref} `, icon.chevronDown(h)];
  if (view.offline) {
    return h.button([h.Class("gp-branch-trigger"), h.Type("button"), h.Disabled(true)], trigger);
  }
  return h.div(
    [],
    [
      ui.menu(
        [
          h.Class("gp-branch-menu"),
          ui.menu.OnMenuSelect(({ value }) => AppMessage.SelectedBranch({ ref: value })),
        ],
        [
          h.button(
            [h.Class("gp-branch-trigger"), h.Type("button"), h.DataAttribute("menu-trigger", "")],
            trigger,
          ),
          ui.menuPopup(
            [h.Class("gp-menu-popup")],
            [
              ...view.branches.map((branch) =>
                ui.menuItem(
                  [
                    h.Class("gp-menu-item"),
                    h.Attribute("value", branch),
                    ...(branch === view.ref ? [h.DataAttribute("current", "")] : []),
                  ],
                  [icon.branch(h, 12), ` ${branch}`],
                ),
              ),
              ui.menuItem(
                [
                  h.Class("gp-menu-item"),
                  h.DataAttribute("action", ""),
                  h.Attribute("value", "__new-branch"),
                ],
                [icon.plus(h, 12), " New branch…"],
              ),
              // Only off the default branch: rebasing a branch onto itself is
              // not an operation, and offering it would earn a refusal.
              view.defaultBranch === null || view.ref === view.defaultBranch
                ? h.empty
                : ui.menuItem(
                    [
                      h.Class("gp-menu-item"),
                      h.DataAttribute("action", ""),
                      h.Attribute("value", "__rebase"),
                    ],
                    [icon.branch(h, 12), ` Rebase onto ${view.defaultBranch}`],
                  ),
            ],
          ),
        ],
      ),
      ui.dialog(
        [h.Class("gp-new-branch")],
        [
          ui.dialogPopup(
            [h.Class("gp-dialog")],
            [
              h.h2(
                [h.Class("gp-dialog-title"), h.DataAttribute("dialog-title", "")],
                ["New branch"],
              ),
              h.p(
                [h.Class("gp-dialog-hint"), h.DataAttribute("dialog-description", "")],
                [`Created at the tip of ${view.ref} and switched to.`],
              ),
              h.form(
                [h.OnSubmit(AppMessage.SubmittedNewBranch())],
                [
                  h.label([h.Class("gp-field-label"), h.For("gp-new-branch-name")], ["Name"]),
                  h.input([
                    h.Id("gp-new-branch-name"),
                    h.Class("gp-input"),
                    h.Name("branch"),
                    h.Required(true),
                    h.Autocomplete("off"),
                    h.Spellcheck(false),
                    h.Placeholder("topic/branch-name"),
                    h.Value(model.codeScreen.newBranch),
                    h.OnInput((name) => AppMessage.ChangedNewBranch({ name })),
                  ]),
                  model.codeScreen.syncNotice === null
                    ? h.empty
                    : h.p([h.Class("gp-notice"), h.Role("alert")], [model.codeScreen.syncNotice]),
                  h.div(
                    [h.Class("gp-dialog-actions")],
                    [
                      h.button(
                        [
                          h.Class("gp-btn-quiet"),
                          h.Type("button"),
                          h.OnClick(AppMessage.ClickedCancelNewBranch()),
                        ],
                        ["Cancel"],
                      ),
                      h.button([h.Class("gp-btn-primary"), h.Type("submit")], ["Create branch"]),
                    ],
                  ),
                ],
              ),
            ],
          ),
        ],
      ),
    ],
  );
};

/** The Clone dialog: the smart-HTTP URL, and a copy that says it copied. */
const clone = (h: H, ui: Elements<AppMessage>, model: Model, url: string): Html.Html =>
  ui.dialog(
    [h.Class("gp-clone")],
    [
      h.button(
        [h.Class("gp-btn-primary"), h.DataAttribute("dialog-trigger", ""), h.Type("button")],
        ["Clone"],
      ),
      ui.dialogPopup(
        [h.Class("gp-dialog")],
        [
          h.h2([h.Class("gp-dialog-title"), h.DataAttribute("dialog-title", "")], ["Clone"]),
          h.p(
            [h.Class("gp-dialog-hint"), h.DataAttribute("dialog-description", "")],
            ["Smart HTTP, served from the same place as this page."],
          ),
          h.div(
            [h.Class("gp-clone-row")],
            [
              h.input([
                h.Class("gp-input gp-clone-url"),
                h.Readonly(true),
                h.Value(url),
                h.AriaLabel("Clone URL"),
              ]),
              h.button(
                [
                  h.Class("gp-btn-quiet"),
                  h.Type("button"),
                  h.OnClick(AppMessage.ClickedClone({ text: url })),
                ],
                [model.codeScreen.copied ? "Copied" : "Copy"],
              ),
            ],
          ),
          h.p([h.Class("gp-dialog-hint")], [`git clone ${url}`]),
        ],
      ),
    ],
  );

/**
 * Where this branch stands against origin.
 *
 * Only the local client answers it — against the HTTP client there is no
 * "against" — so the controls are absent rather than showing zeroes.
 */
const syncControls = (h: H, ui: Elements<AppMessage>, model: Model, view: CodeView): Html.Html => {
  const sync = model.codeScreen.sync;
  if (sync === null || !writable(view)) return h.empty;
  const busy = model.codeScreen.syncing;
  return h.span(
    [
      h.Class("gp-sync"),
      h.Title("This branch lives in this browser (OPFS); origin is the server."),
    ],
    [
      h.button(
        [
          h.Class("gp-btn-quiet"),
          h.Type("button"),
          h.Title(
            sync.ahead === 0
              ? "Nothing to push"
              : `Push ${String(sync.ahead)} commit${sync.ahead === 1 ? "" : "s"} to origin`,
          ),
          h.Disabled(busy || sync.ahead === 0),
          h.OnClick(AppMessage.ClickedPush()),
        ],
        [`Push ↑${String(sync.ahead)}`],
      ),
      h.button(
        [
          h.Class("gp-btn-quiet"),
          h.Type("button"),
          h.Title("Fetch origin"),
          h.Disabled(busy),
          h.OnClick(AppMessage.ClickedFetch()),
        ],
        [`Fetch${sync.behind > 0 ? ` ↓${String(sync.behind)}` : ""}`],
      ),
      // Only off the default branch: a Change Request proposing a branch onto
      // itself is not a proposal.
      view.defaultBranch === null || view.ref === view.defaultBranch
        ? h.empty
        : propose(h, ui, model, view),
    ],
  );
};

/**
 * Open a Change Request for this branch.
 *
 * Pushed first, signed with the browser's key, and read back from the
 * projection — the order `Propose` runs them in, which is the honest one.
 */
const propose = (h: H, ui: Elements<AppMessage>, model: Model, view: CodeView): Html.Html =>
  ui.dialog(
    [h.Class("gp-propose")],
    [
      h.button(
        [
          h.Class("gp-btn-primary"),
          h.DataAttribute("dialog-trigger", ""),
          h.Type("button"),
          h.Disabled(model.codeScreen.syncing),
          h.Title(`Push ${view.ref} and open a Change Request against ${view.defaultBranch ?? ""}`),
        ],
        ["Propose"],
      ),
      ui.dialogPopup(
        [h.Class("gp-dialog")],
        [
          h.h2(
            [h.Class("gp-dialog-title"), h.DataAttribute("dialog-title", "")],
            [`Propose ${view.ref}`],
          ),
          h.p(
            [h.Class("gp-dialog-hint"), h.DataAttribute("dialog-description", "")],
            [
              `Pushes the branch, then opens a Change Request against ${view.defaultBranch ?? ""} — signed with this browser's key.`,
            ],
          ),
          h.form(
            [h.OnSubmit(AppMessage.SubmittedPropose())],
            [
              h.label([h.Class("gp-field-label"), h.For("gp-propose-title")], ["Title"]),
              h.input([
                h.Id("gp-propose-title"),
                h.Class("gp-input"),
                h.Name("title"),
                h.Required(true),
                h.Autocomplete("off"),
                h.Placeholder("What this changes"),
                h.Value(model.codeScreen.proposeTitle),
                h.OnInput((value) => AppMessage.ChangedProposeField({ field: "title", value })),
              ]),
              h.label([h.Class("gp-field-label"), h.For("gp-propose-desc")], ["Description"]),
              h.textarea([
                h.Id("gp-propose-desc"),
                h.Class("gp-textarea"),
                h.Name("desc"),
                h.Rows(3),
                h.Placeholder("Why, and anything a reviewer should know…"),
                h.Value(model.codeScreen.proposeDescription),
                h.OnInput((value) => AppMessage.ChangedProposeField({ field: "desc", value })),
              ]),
              model.codeScreen.syncNotice === null
                ? h.empty
                : h.p([h.Class("gp-notice"), h.Role("alert")], [model.codeScreen.syncNotice]),
              h.div(
                [h.Class("gp-dialog-actions")],
                [
                  h.button(
                    [
                      h.Class("gp-btn-primary"),
                      h.Type("submit"),
                      h.Disabled(model.codeScreen.syncing),
                    ],
                    ["Open Change Request"],
                  ),
                ],
              ),
            ],
          ),
        ],
      ),
    ],
  );

const commitBar = (h: H, model: Model, view: CodeView, head: HeadCommit): Html.Html =>
  h.button(
    [
      h.Class("gp-commit-bar"),
      h.Type("button"),
      h.Title("Recent commits"),
      h.AriaExpanded(model.codeScreen.panel === "commits"),
      h.Disabled(view.offline),
      h.OnClick(AppMessage.ClickedCommitPanel()),
    ],
    [
      h.span([h.Class("gp-avatar")], [head.avatar]),
      h.span([h.Class("gp-commit-author")], [head.author]),
      h.span([h.Class("gp-commit-subject")], [head.message]),
      h.span([h.Class("gp-commit-sha")], [head.sha]),
      // The space is load-bearing: the bar reads as one sentence, and a sha
      // running straight into an age is a different string to anything
      // reading it — a person or a test.
      " ",
      h.span([], [head.when]),
    ],
  );

/** Whichever history panel is open: the branch's commits, or one file's. */
const panel = (h: H, model: Model): Html.Html => {
  if (model.codeScreen.panel === "none") return h.empty;
  const files = model.codeScreen.panel === "filelog";
  return h.div(
    [h.Class("gp-panel-card gp-history-panel")],
    [
      AsyncData.match(model.codeScreen.history, {
        onIdle: () =>
          h.div([h.Class("gp-empty")], [files ? "Loading file history…" : "Loading history…"]),
        onLoading: () =>
          h.div([h.Class("gp-empty")], [files ? "Loading file history…" : "Loading history…"]),
        onRefreshing: () => h.div([h.Class("gp-empty")], ["Loading history…"]),
        onFailure: (reason) => h.div([h.Class("gp-empty")], [reason]),
        onStale: ({ data }) => rows(h, model, data, files),
        onSuccess: (data) => rows(h, model, data, files),
      }),
      files ? h.empty : bisectNotice(h, model),
    ],
  );
};

const rows = (
  h: H,
  model: Model,
  history: readonly {
    readonly oid: string;
    readonly subject: string;
    readonly author: string;
    readonly when: string;
  }[],
  files: boolean,
): Html.Html => {
  if (history.length === 0) {
    return h.div(
      [h.Class("gp-empty")],
      [files ? "No commits touch this file." : "No history to show."],
    );
  }
  return h.div(
    [],
    history.map((entry) =>
      files
        ? h.button(
            [
              h.Class("gp-list-row gp-filelog-row"),
              h.Type("button"),
              ...(model.codeScreen.at === entry.oid ? [h.DataAttribute("current", "")] : []),
              h.OnClick(AppMessage.ClickedHistoryRow({ oid: entry.oid })),
            ],
            [h.span([h.Class("gp-sha")], [entry.oid.slice(0, 7)]), h.span([], [entry.subject])],
          )
        : h.div(
            [h.Class("gp-list-row")],
            [
              h.span([h.Class("gp-sha")], [entry.oid.slice(0, 7)]),
              h.span([], [entry.subject]),
              h.span([h.Class("gp-when")], [`${entry.author} · ${entry.when}`]),
              h.span(
                [h.Class("gp-row-actions")],
                [
                  h.button(
                    [
                      h.Class("gp-link-btn"),
                      h.Type("button"),
                      h.Title(`Replay this commit onto ${viewOf(model).ref}`),
                      h.Disabled(model.codeScreen.syncing || !writable(viewOf(model))),
                      h.OnClick(AppMessage.ClickedCherryPick({ commit: entry.oid })),
                    ],
                    ["pick"],
                  ),
                  h.button(
                    [
                      h.Class("gp-link-btn"),
                      h.Type("button"),
                      h.Title("Mark good for bisect"),
                      h.Disabled(viewOf(model).offline),
                      h.OnClick(AppMessage.MarkedBisect({ commit: entry.oid, as: "good" })),
                    ],
                    ["good"],
                  ),
                  h.button(
                    [
                      h.Class("gp-link-btn"),
                      h.Type("button"),
                      h.Title("Mark bad for bisect"),
                      h.Disabled(viewOf(model).offline),
                      h.OnClick(AppMessage.MarkedBisect({ commit: entry.oid, as: "bad" })),
                    ],
                    ["bad"],
                  ),
                ],
              ),
            ],
          ),
    ),
  );
};

/** What the marks so far imply: a commit to test, or the first bad one. */
const bisectNotice = (h: H, model: Model): Html.Html => {
  const answer = model.codeScreen.bisect?.answer ?? null;
  if (answer === null) return h.empty;
  return h.p(
    [h.Class("gp-notice")],
    [
      answer.kind === "found"
        ? `bisect: first bad commit is ${answer.commit.slice(0, 7)}`
        : `bisect: test ${answer.commit.slice(0, 7)} — about ${String(answer.steps)} step(s) left`,
      " ",
      h.button(
        [h.Class("gp-link-btn"), h.Type("button"), h.OnClick(AppMessage.ClickedResetBisect())],
        ["reset"],
      ),
    ],
  );
};

/** The read-only banner while the pane shows an old revision. */
const atBanner = (h: H, model: Model, view: CodeView): Html.Html => {
  const at = model.codeScreen.at;
  if (view.selected === null || at === null) return h.empty;
  return h.p(
    [h.Class("gp-notice"), h.DataAttribute("history", "")],
    [
      `Viewing ${view.selected} at ${at.slice(0, 7)} — read-only. `,
      h.button(
        [h.Class("gp-link-btn"), h.Type("button"), h.OnClick(AppMessage.ClickedBackToTip())],
        ["Back to tip"],
      ),
    ],
  );
};

/**
 * The commit bar under the editable pane: message and Commit.
 *
 * The content itself lives in the source host above, and the draft in the
 * Model. Cancel and delete sit in the card head beside the filename, where the
 * pencil that opened the session was.
 */
const editorBar = (h: H, model: Model, view: CodeView): Html.Html =>
  h.div(
    [],
    [
      model.codeScreen.editError === null
        ? h.empty
        : h.p([h.Class("gp-notice"), h.DataAttribute("error", "")], [model.codeScreen.editError]),
      h.div(
        [h.Class("gp-editor-bar")],
        [
          h.input([
            h.Class("gp-input gp-editor-message"),
            h.AriaLabel("Commit message"),
            h.Placeholder(
              model.codeScreen.newPath !== null
                ? "commit message"
                : `update ${view.selected ?? "this file"}`,
            ),
            h.Autocomplete("off"),
            h.Value(model.codeScreen.message),
            h.OnInput((message) => AppMessage.ChangedCommitMessage({ message })),
          ]),
          h.button(
            [
              h.Class("gp-btn-primary"),
              h.Type("button"),
              h.Disabled(model.codeScreen.saving),
              h.OnClick(AppMessage.ClickedSave()),
            ],
            [model.codeScreen.saving ? "Committing…" : "Commit"],
          ),
        ],
      ),
    ],
  );

/**
 * The diff behind the ± toggle.
 *
 * Editing, it is the draft against the blob it opened from — a new file
 * against no old side at all, which renders as pure addition. Viewing, there
 * is nothing to compare the file to yet, and saying so beats hiding the pane
 * and showing an empty card, which is what a bare toggle would do.
 */
const diffReview = (h: H, model: Model, view: CodeView, name: string): Html.Html => {
  if (model.codeScreen.mode !== "edit") {
    return h.div(
      [h.Class("gp-empty")],
      ["Nothing to compare — open the editor to review a change."],
    );
  }
  if (model.codeScreen.draft === (view.content ?? "")) {
    return h.div([h.Class("gp-empty")], ["No changes yet."]);
  }
  // Keyed like the source host beside it: a name that is being typed is not
  // an identity, and rebuilding this one per keystroke means a dynamic import
  // and a Shiki pass per character. The mount takes `path` for its grammar and
  // captures it once, which is the same trade made there.
  const identity =
    model.codeScreen.newPath === null
      ? `review:${name}:${model.theme}:${String(model.codeScreen.draft.length)}`
      : `review:__new:${String(model.codeScreen.session)}:${model.theme}:${String(model.codeScreen.draft.length)}`;
  return h.keyed("div")(identity, [
    h.Class("gp-diff-review-host gp-diff-host"),
    h.OnMount(
      PierreDiff({
        path: name,
        // A file being created has no old side; one being edited has the
        // blob it opened from, which is what the Model still holds.
        oldContents: model.codeScreen.newPath === null ? (view.content ?? "") : null,
        newContents: model.codeScreen.draft,
        theme: model.theme,
      }),
    ),
  ]);
};

const fileCard = (h: H, model: Model, view: CodeView): Html.Html => {
  const editing = model.codeScreen.mode === "edit";
  const creating = model.codeScreen.newPath !== null;
  const loading = AsyncData.isLoading(model.codeScreen.view);
  const name = creating
    ? model.codeScreen.newPath === ""
      ? "untitled"
      : (model.codeScreen.newPath ?? "untitled")
    : (view.selected ?? "untitled");
  return h.div(
    [h.Class("gp-card gp-file-card")],
    [
      h.div(
        [h.Class("gp-card-head")],
        [
          icon.document_(h),
          creating
            ? h.input([
                h.Class("gp-input gp-editor-path"),
                h.Placeholder("path/to/file.md"),
                h.AriaLabel("New file path"),
                h.Autocomplete("off"),
                h.Spellcheck(false),
                h.Value(model.codeScreen.newPath ?? ""),
                h.OnInput((path) => AppMessage.ChangedNewFilePath({ path })),
              ])
            : h.span([], [view.selected ?? "—"]),
          h.button(
            [
              h.Class("gp-icon-btn"),
              h.Type("button"),
              h.Title("File history"),
              h.AriaLabel("File history"),
              ...(model.codeScreen.panel === "filelog" ? [h.DataAttribute("active", "")] : []),
              h.Disabled(view.offline || loading || view.selected === null || creating),
              h.OnClick(AppMessage.ClickedFileLogPanel()),
            ],
            [icon.clock(h, 14)],
          ),
          h.button(
            [
              h.Class("gp-icon-btn"),
              h.Type("button"),
              h.DataAttribute("tight", ""),
              h.Title(
                model.codeScreen.diffing
                  ? "Back to the file"
                  : editing
                    ? "Review changes as a diff"
                    : "Diff against the previous version",
              ),
              h.AriaLabel("Review changes"),
              ...(model.codeScreen.diffing ? [h.DataAttribute("active", "")] : []),
              h.Disabled(
                view.offline ||
                  loading ||
                  model.codeScreen.saving ||
                  (!editing && view.selected === null),
              ),
              h.OnClick(AppMessage.ToggledDiffReview()),
            ],
            [icon.diff(h, 14)],
          ),
          ...(editing
            ? [
                ...(creating
                  ? []
                  : [
                      h.button(
                        [
                          h.Class("gp-icon-btn"),
                          h.Type("button"),
                          h.DataAttribute("tight", ""),
                          h.Title("Remove this file in a new commit"),
                          h.AriaLabel("Delete file"),
                          h.Disabled(model.codeScreen.saving),
                          h.OnClick(AppMessage.ClickedDeleteFile()),
                        ],
                        [icon.trash(h, 14)],
                      ),
                    ]),
                h.button(
                  [
                    h.Class("gp-icon-btn"),
                    h.Type("button"),
                    h.DataAttribute("tight", ""),
                    h.Title("Cancel editing"),
                    h.AriaLabel("Cancel editing"),
                    h.Disabled(model.codeScreen.saving),
                    h.OnClick(AppMessage.ClickedCancelEdit()),
                  ],
                  [icon.close(h, 14)],
                ),
              ]
            : [
                h.button(
                  [
                    h.Class("gp-icon-btn"),
                    h.Type("button"),
                    h.DataAttribute("tight", ""),
                    h.Title(
                      Code.unreachable(view)
                        ? "Read-only — the git+ API is not reachable"
                        : view.pending
                          ? "Reading the repository…"
                          : !writable(view)
                            ? "Select or create a branch to edit"
                            : model.codeScreen.at !== null
                              ? "Read-only — viewing an old commit"
                              : "Edit file",
                    ),
                    h.AriaLabel("Edit file"),
                    h.Disabled(
                      !writable(view) ||
                        loading ||
                        view.selected === null ||
                        model.codeScreen.at !== null,
                    ),
                    h.OnClick(AppMessage.ClickedEdit()),
                  ],
                  [icon.pencil(h, 14)],
                ),
              ]),
        ],
      ),
      atBanner(h, model, view),
      ...(loading
        ? [h.div([h.Class("gp-empty")], ["Loading…"])]
        : [
            // Keyed on what the surface *is*: a different file, a different
            // revision of it, a different mode or a different palette is a
            // different instance, and the library rebuilds rather than being
            // patched underneath. Mount arguments are captured once, at
            // mount, so the blob's arrival has to change this key — otherwise
            // the pane keeps the empty text it was mounted with while the
            // request was still out.
            //
            // The draft is deliberately *not* in it: in edit mode the Model
            // tracks every keystroke, and remounting on each one would take
            // the caret with it. A new file's *path* is the same hazard —
            // typed a character at a time into a field beside this one, while
            // the editor pulls the caret into itself on mount — so a file
            // being created is keyed on its editing *session* instead, and on
            // nothing else. Not the name, which is being typed; not another
            // file's blob, whose late arrival would remount this one. The
            // session is what makes a second "+" a second editor rather than
            // the first one still showing an abandoned draft. The cost is
            // that a file being created has no grammar until it is saved and
            // reopened.
            h.keyed("div")(
              creating
                ? `__new:${String(model.codeScreen.session)}:${model.theme}`
                : `${name}:${String(editing)}:${model.theme}:${model.codeScreen.at ?? ""}:${Code.fingerprint(view.content)}`,
              [
                h.Class("gp-source-host gp-diff-host"),
                h.Hidden(model.codeScreen.diffing),
                h.OnMount(
                  PierreSource({
                    name,
                    contents: editing ? model.codeScreen.draft : (view.content ?? ""),
                    editable: editing,
                    theme: model.theme,
                  }),
                ),
              ],
            ),
            ...(model.codeScreen.diffing ? [diffReview(h, model, view, name)] : []),
            ...(editing ? [editorBar(h, model, view)] : []),
          ]),
    ],
  );
};

/**
 * `notice` is the shell's explanation of a bad address.
 *
 * Handed in rather than read here: every other screen gets it from the column
 * the shell wraps it in, and Code supplies its own column — so the one thing
 * it has to carry across is this.
 */
export const view = (
  model: Model,
  h: H,
  ui: Elements<AppMessage>,
  notice: Html.Html,
): Html.Html => {
  const current = viewOf(model);
  const viewer = AsyncData.getOrElse(model.viewer, () => null);
  // Two columns of the shell, not one. A view returns one element, so this
  // wrapper carries `display: contents` (see `base.css`) and the explorer and
  // the content become the flex items the design's layout assumes — exactly
  // what the custom-element boundary used to do here.
  return h.div(
    [h.Class("gp-code")],
    [
      explorer(h, ui, model, current),
      h.div(
        [h.Class("gp-main")],
        [
          h.header(
            [h.Class("gp-repo-header")],
            [
              h.div(
                [h.Class("gp-breadcrumb")],
                [
                  h.span([h.Class("gp-breadcrumb-owner")], ["git-plus"]),
                  h.span([h.Class("gp-breadcrumb-sep")], ["/"]),
                  h.span([h.Class("gp-breadcrumb-name")], [model.repo]),
                  h.span(
                    [h.Class("gp-pill-outline"), h.Title(viewer?.why ?? "")],
                    [viewer?.member === true ? "Member" : "Public"],
                  ),
                ],
              ),
              h.div(
                [h.Class("gp-repo-actions")],
                [
                  syncControls(h, ui, model, current),
                  branchMenu(h, ui, model, current),
                  clone(h, ui, model, model.cloneUrl),
                ],
              ),
            ],
          ),
          h.div(
            [h.Class("gp-screen gp-screen--code")],
            [
              notice,
              Code.unreachable(current)
                ? h.p(
                    [h.Class("gp-notice")],
                    [`Showing the design's sample repository — ${current.reason}.`],
                  )
                : h.empty,
              model.codeScreen.syncNotice === null
                ? h.empty
                : h.p([h.Class("gp-notice")], [model.codeScreen.syncNotice]),
              current.head === null ? h.empty : commitBar(h, model, current, current.head),
              panel(h, model),
              fileCard(h, model, current),
            ],
          ),
        ],
      ),
    ],
  );
};
