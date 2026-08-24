/**
 * Task / Change Request detail.
 *
 * One screen for both, because a Change Request is a Task with a diff attached:
 * the title, description, subtasks, discussion and meta sidebar are the Task
 * half and always render; the refs, tabs, commits, checks and merge state are
 * the Change Request half and appear only when there is a proposed change.
 *
 * The Diff tab is wired to the live server. `POST /:repo/diff` reports which
 * files a Change Request touches, and each side is then read through
 * `GET /:repo/file` at the source and target refs and handed to `@pierre/diffs`
 * as `{ oldFile, newFile }` — added and removed files pass `null` for the side
 * that does not exist. When those refs are not in the repository (the fixture
 * Change Requests name branches that only exist in the design) the tab falls
 * back to the design's own diff and says so.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import type { FileDiff } from "@pierre/diffs";

import { ApiError, describe, type DiffFile, type GitApi } from "./api.ts";
import { GitPlusElement, navigate } from "./base.ts";
import { diffs } from "./highlight.ts";
import * as icons from "./icons.ts";
import { type ChangeRequest, isChangeRequest, type Task } from "./model.ts";
import { kindChip, statusPill } from "./screen.tasks.ts";
import { store } from "./store.ts";
import { current as currentTheme, type Theme } from "./theme.ts";
import { initials } from "./time.ts";

type Tab = "conversation" | "diff" | "commits" | "checks";

/** One file's two sides, ready for `@pierre/diffs`. */
interface LoadedDiff {
  readonly path: string;
  readonly status: DiffFile["status"];
  readonly oldContents: string | null;
  readonly newContents: string | null;
}

type DiffState =
  | { readonly tag: "idle" }
  | { readonly tag: "loading"; readonly taskId: string }
  | { readonly tag: "loaded"; readonly taskId: string; readonly files: readonly LoadedDiff[] }
  | { readonly tag: "fallback"; readonly taskId: string; readonly reason: string };

@customElement("gp-detail")
export class GpDetail extends GitPlusElement {
  api: GitApi | null = null;

  @property({ type: String }) accessor taskId = "T-12";
  @property({ type: String }) accessor theme: Theme = currentTheme();

  /** Who the server said is asking; their comments are authored as them. */
  @property({ type: String }) accessor viewer: string | null = null;

  @state() private accessor tab: Tab = "conversation";
  @state() private accessor diffState: DiffState = { tag: "idle" };

  /** Why the last merge attempt refused, shown beside the review card. */
  @state() private accessor mergeNotice: string | null = null;

  /** A signed hub write is in flight; the action buttons disable meanwhile. */
  @state() private accessor acting = false;

  #renderers = new Map<string, FileDiff>();
  #diffGeneration = 0;
  #unsubscribe: (() => void) | null = null;

  override connectedCallback(): void {
    super.connectedCallback();
    this.#unsubscribe = store.subscribe(() => this.requestUpdate());
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    this.#diffGeneration += 1;
    this.#renderers.clear();
  }

  override willUpdate(changed: Map<string, unknown>): void {
    // A different Change Request means a different diff; drop what was loaded
    // so the old one cannot flash under the new title.
    if (changed.has("taskId")) {
      this.#diffGeneration += 1;
      this.tab = "conversation";
      this.diffState = { tag: "idle" };
      this.mergeNotice = null;
      this.#renderers.clear();
      // A hub-sourced task carries only its listing row until someone looks
      // at it; the store fills the discussion and checks from the detail
      // endpoint and notifies. For a fixture id this is a no-op.
      store.hydrate(this.taskId);
    }
  }

  get #task(): Task {
    return store.get(this.taskId) ?? store.get("T-12") ?? { ...EMPTY };
  }

  /**
   * Merge a mergeable Change Request.
   *
   * Two entirely separate stories, split on provenance. A *hub* Change
   * Request settles through the hub's own merge endpoint: one server-side
   * transition that advances the base to the approved head and appends the
   * signed `pr.merged` beside it, judged together — and what shows next is
   * the projection, re-read. A refused or unreachable merge leaves the
   * Change Request open with the reason beside the button; there is no
   * tab-local "merged" for something the repository still holds open.
   *
   * A *fixture* Change Request keeps the design's tab-local story: the
   * server's generic `POST /merge` is offered the refs (which usually do
   * not exist outside the design), and the store's sample projection
   * records the merge either way — clearly sample behaviour, never
   * reachable by a hub entity.
   */
  async #merge(cr: ChangeRequest): Promise<void> {
    const api = this.api;
    this.mergeNotice = null;

    if (cr.hub === true) {
      this.acting = true;
      try {
        const refused = await store.mergeRemote(cr.id);
        if (refused !== null) this.mergeNotice = refused;
      } finally {
        this.acting = false;
      }
      return;
    }

    if (api !== null) {
      try {
        const result = await api.merge({
          ours: cr.targetRef,
          theirs: cr.sourceRef,
          into: cr.targetRef,
          message: `merge ${cr.id}: ${cr.title}`,
        });
        if (result.kind === "conflicted") {
          const paths = result.conflicts.map((conflict) => conflict.path).join(", ");
          this.mergeNotice = `merge conflicted on ${paths} — resolve on the branch first`;
          return;
        }
      } catch (error) {
        if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
        // Refs this repository does not have — the fixture case — fall through
        // to the projection, as does a server with no merge endpoint at all.
        // A refusal it *could* have honoured (a policy `Invalid`, a conflict)
        // is worth showing, and blocks the projection.
        const absent =
          !(error instanceof ApiError) ||
          error.unreachable ||
          error.status === 404 ||
          error.tag === "ObjectNotFound";
        if (!absent) {
          this.mergeNotice = describe(error);
          return;
        }
      }
    }
    store.merge(cr.id);
  }

  async #submitReview(decision: "approve" | "reject"): Promise<void> {
    this.acting = true;
    try {
      const sent = await store.reviewRemote(this.taskId, decision);
      if (!sent) this.mergeNotice = "the hub refused the review — is this key a member?";
    } finally {
      this.acting = false;
    }
  }

  async #thread(thread: string, action: "resolve" | "reopen" | "reply", body = ""): Promise<void> {
    this.acting = true;
    try {
      const sent =
        action === "reply"
          ? await store.replyRemote(this.taskId, thread, body)
          : await store.resolveRemote(this.taskId, thread, action === "resolve");
      if (!sent) this.mergeNotice = "the hub refused the thread update";
    } finally {
      this.acting = false;
    }
  }

  async #taskAction(action: "claim" | "release" | "complete" | "abandon"): Promise<void> {
    this.acting = true;
    try {
      const sent = await store.taskActionRemote(this.taskId, action);
      if (!sent) this.mergeNotice = "the hub refused the task update";
    } finally {
      this.acting = false;
    }
  }

  /**
   * Append a comment, split on provenance. A hub Change Request's comment is
   * a signed event, read back from the projection — a refusal shows *as* a
   * refusal, never as a tab-local comment pretending to be repository state.
   * A fixture's comment stays tab-local, authored as whoever `/whoami` said
   * is asking, which is the design's documented sample behaviour. A hub
   * *Task* never reaches here: its form is disabled with the reason
   * (`#commentForm`), because no canonical task-comment event exists yet and
   * the pull-request namespace is not a place to improvise one.
   */
  #comment = async (event: SubmitEvent): Promise<void> => {
    event.preventDefault();
    const form = event.currentTarget;
    if (!(form instanceof HTMLFormElement)) return;
    const field = form.elements.namedItem("text");
    if (!(field instanceof HTMLTextAreaElement)) return;
    const text = field.value.trim();
    if (text === "") return;
    const task = this.#task;
    if (task.hub === true) {
      const sent = await store.commentRemote(task.id, text);
      if (!sent) {
        this.mergeNotice = "the hub refused the comment — is this key a member?";
        return;
      }
    } else {
      const author = this.viewer ?? "anonymous";
      store.comment(task.id, { avatar: initials(author), author, when: "just now", text });
    }
    form.reset();
  };

  /**
   * The comment form — or, for a live hub Task, the honest absence of one.
   *
   * Task and pull-request ids share one shape, so a task comment written
   * into the pull-request namespace would *create* a ghost Change Request
   * ref; and a tab-local comment on live repository state would vanish on
   * reload while looking canonical. Until a task-comment event exists in
   * the protocol, saying so beats either lie.
   */
  #commentForm(task: Task): TemplateResult {
    if (task.hub === true && !isChangeRequest(task)) {
      return html`
        <p class="gp-notice">
          Live task discussion is not part of the hub protocol yet — there is no signed task-comment
          event for this browser to append, so commenting is off rather than written somewhere it
          does not belong.
        </p>
      `;
    }
    return html`
      <form class="gp-comment-form" @submit=${this.#comment}>
        <textarea
          class="gp-textarea"
          name="text"
          rows="3"
          required
          placeholder="Leave a comment…"
          aria-label="Leave a comment"
        ></textarea>
        <div class="gp-comment-actions">
          <button class="gp-btn-primary" type="submit">Comment</button>
        </div>
      </form>
    `;
  }

  #select(tab: Tab): void {
    this.tab = tab;
    // A route can replace the task while its previous diff is still resolving.
    // Reload a settled Diff selection: its visible state may belong to that
    // previous route, while `#loadDiff`'s generation rejects its late answer.
    if (tab === "diff" && this.diffState.tag !== "loading") void this.#loadDiff();
  }

  /** Ask the server what changed, then read both sides of each file. */
  async #loadDiff(): Promise<void> {
    const api = this.api;
    const task = this.#task;
    const taskId = task.id;
    const generation = ++this.#diffGeneration;
    if (api === null || !isChangeRequest(task)) {
      this.diffState = {
        tag: "fallback",
        taskId,
        reason: `${task.id} names refs that are not in this repository`,
      };
      return;
    }
    this.diffState = { tag: "loading", taskId };
    try {
      const files = await api.diff(task.targetRef, task.sourceRef);
      const loaded = await Promise.all(
        files
          .filter((file) => !file.binary)
          .map(async (file): Promise<LoadedDiff> => {
            // `added` has no old side and `removed` has no new one; asking for
            // the missing side would be a guaranteed 404. Existing sides and
            // separate files are independent, so they all load in parallel.
            const [oldContents, newContents] = await Promise.all([
              file.status === "added" ? Promise.resolve(null) : api.file(task.targetRef, file.path),
              file.status === "removed"
                ? Promise.resolve(null)
                : api.file(task.sourceRef, file.path),
            ]);
            return { path: file.path, status: file.status, oldContents, newContents };
          }),
      );
      if (generation === this.#diffGeneration && taskId === this.taskId) {
        this.diffState = { tag: "loaded", taskId, files: loaded };
      }
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#diffGeneration && taskId === this.taskId) {
        this.diffState = { tag: "fallback", taskId, reason: describe(error) };
      }
    }
  }

  protected override updated(): void {
    if (this.tab === "diff") void this.#renderDiffs();
  }

  async #renderDiffs(): Promise<void> {
    const state = this.diffState;
    if (state.tag !== "loaded" || state.files.length === 0) return;
    const files = state.files;
    const { FileDiff } = await diffs();
    if (this.diffState !== state || state.taskId !== this.taskId) return;
    for (const file of files) {
      const host = this.querySelector<HTMLElement>(`[data-diff-host="${CSS.escape(file.path)}"]`);
      if (host === null) continue;
      let renderer = this.#renderers.get(file.path);
      if (renderer === undefined) {
        renderer = new FileDiff({
          themeType: this.theme,
          diffStyle: "unified",
          disableFileHeader: true,
          overflow: "scroll",
        });
        this.#renderers.set(file.path, renderer);
      }
      renderer.setThemeType(this.theme);
      const oldFile =
        file.oldContents === null ? null : { name: file.path, contents: file.oldContents };
      const newFile =
        file.newContents === null ? null : { name: file.path, contents: file.newContents };
      // One of the two is always present: a diff entry with neither side would
      // not have been reported as a change.
      if (newFile === null && oldFile !== null) {
        renderer.render({ oldFile, newFile: null, containerWrapper: host });
      } else if (newFile !== null) {
        renderer.render({ oldFile, newFile, containerWrapper: host });
      }
    }
  }

  protected override render(): TemplateResult {
    const task = this.#task;
    const cr = isChangeRequest(task) ? task : null;
    return html`
      <div class="gp-screen gp-screen--flush">
        <div class="gp-detail">
          <div class="gp-detail-main">
            <button
              class="gp-back"
              type="button"
              @click=${() => navigate(this, { screen: "tasks" })}
            >
              ${icons.chevronLeft()} Tasks
            </button>

            <div class="gp-detail-eyebrow">
              ${kindChip(task)}
              <span class="gp-id">${task.id}</span>
              ${statusPill(task)}
            </div>
            <h1 class="gp-detail-title">${task.title}</h1>
            <p class="gp-detail-desc">${task.desc}</p>

            ${cr === null ? nothing : this.#changeRequest(cr)} ${this.#subtasks(task)}
            ${this.#lifecycle(task)}

            <h2 class="gp-section-label">Discussion</h2>
            ${
              task.threads !== undefined && task.threads.length > 0
                ? task.threads.map((thread) => this.#thread_(task, thread))
                : task.comments.map(
                    (comment) => html`
                      <div class="gp-comment">
                        <span class="gp-avatar" data-size="lg">${comment.avatar}</span>
                        <div>
                          <div class="gp-comment-head">
                            <span class="gp-comment-author">${comment.author}</span>
                            <span class="gp-comment-when">${comment.when}</span>
                          </div>
                          <div class="gp-comment-body">${comment.text}</div>
                        </div>
                      </div>
                    `,
                  )
            }
            ${this.#commentForm(task)}
          </div>

          ${this.#meta(task)}
        </div>
      </div>
    `;
  }

  #changeRequest(cr: ChangeRequest): TemplateResult {
    return html`
      <div class="gp-refs">
        <span class="gp-ref" data-source>${cr.sourceRef}</span>
        ${icons.arrowRight()}
        <span class="gp-ref">${cr.targetRef}</span>
      </div>

      <ui-tabs class="gp-tabs" value=${this.tab}>
        <ui-tab-list class="gp-tablist">
          ${this.#tab("conversation", html`Conversation`)}
          ${this.#tab("diff", html`Diff <span class="gp-tab-count">${cr.diffStat}</span>`)}
          ${this.#tab("commits", html`Commits <span class="gp-tab-count">${cr.commitCount}</span>`)}
          ${this.#tab("checks", html`Checks`)}
        </ui-tab-list>

        <div
          class="gp-tabpanel"
          data-tab-panel
          value="conversation"
          ?hidden=${this.tab !== "conversation"}
        >
          ${this.#review(cr)}
        </div>
        <div class="gp-tabpanel" data-tab-panel value="diff" ?hidden=${this.tab !== "diff"}>
          ${this.#diff(cr)}
        </div>
        <div class="gp-tabpanel" data-tab-panel value="commits" ?hidden=${this.tab !== "commits"}>
          <div class="gp-panel-card">
            ${cr.commits.map(
              (commit) => html`
                <div class="gp-list-row">
                  <span class="gp-sha">${commit.sha}</span>
                  <span>${commit.msg}</span>
                  <span class="gp-when">${commit.when}</span>
                </div>
              `,
            )}
          </div>
        </div>
        <div class="gp-tabpanel" data-tab-panel value="checks" ?hidden=${this.tab !== "checks"}>
          <div class="gp-panel-card">
            ${cr.checks.map(
              (check) => html`
                <div class="gp-list-row">
                  <span class="gp-check-dot" ?data-ok=${check.ok}></span>
                  <span>${check.name}</span>
                  <span class="gp-when">${check.detail}</span>
                </div>
              `,
            )}
          </div>
        </div>
      </ui-tabs>
    `;
  }

  #tab(id: Tab, label: TemplateResult): TemplateResult {
    return html`
      <button
        class="gp-tab"
        type="button"
        data-tab
        value=${id}
        ?data-selected=${this.tab === id}
        @click=${() => this.#select(id)}
      >
        ${label}
      </button>
    `;
  }

  #diff(cr: ChangeRequest): TemplateResult {
    const state = this.diffState;
    if (state.tag === "idle" || state.tag === "loading")
      return html`<div class="gp-panel-card"><div class="gp-empty">Loading diff…</div></div>`;

    if (state.tag === "loaded") {
      if (state.files.length === 0) {
        return html`<div class="gp-panel-card">
          <div class="gp-empty">No textual changes.</div>
        </div>`;
      }
      return html`${state.files.map(
        (file) => html`
          <div class="gp-panel-card">
            <div class="gp-diff-file-head">
              <span>${file.path}</span>
              <span
                class="gp-diff-state"
                style="--gp-status-color: var(--gp-${file.status === "removed" ? "red" : file.status === "added" ? "accent" : "amber"});"
                >${file.status}</span
              >
            </div>
            <div class="gp-diff-host" data-diff-host=${file.path}></div>
          </div>
        `,
      )}`;
    }

    // The fixture rendering: the design's own diff, line for line.
    return html`
      ${
        state.tag === "fallback"
          ? html`<p class="gp-notice">Showing the design's sample diff — ${state.reason}.</p>`
          : nothing
      }
      <div class="gp-panel-card">
        <div class="gp-diff-file-head"><span>${cr.diffFile}</span></div>
        <div class="gp-diff-static">
          ${cr.diff.map(
            // Only the text span preserves whitespace, and the row is a flex
            // container: this template's own newlines and indentation are
            // whitespace-only nodes there, so they are dropped rather than
            // rendered as leading spaces and blank lines.
            (line) =>
              html`<div class="gp-diff-line" data-kind=${line.kind}>
                <span class="gp-diff-num">${line.n}</span>
                <span class="gp-diff-text">${line.text}</span>
              </div>`,
          )}
        </div>
      </div>
    `;
  }

  #review(cr: ChangeRequest): TemplateResult {
    const state = cr.review.merged === true ? "merged" : cr.review.ok ? "ready" : "blocked";
    return html`
      <div class="gp-review">
        <span class="gp-review-dot" ?data-ok=${cr.review.ok}></span>
        <div>
          <div class="gp-review-headline">${cr.review.headline}</div>
          <div class="gp-review-detail">${cr.review.detail}</div>
        </div>
        ${
          cr.hub === true && cr.review.merged !== true
            ? html`
                <button
                  class="gp-btn-quiet"
                  type="button"
                  title="Approve the proposed revision, signed with this browser's key"
                  ?disabled=${this.acting}
                  @click=${() => void this.#submitReview("approve")}
                >
                  Approve
                </button>
                <button
                  class="gp-btn-quiet"
                  type="button"
                  title="Request changes to the proposed revision"
                  ?disabled=${this.acting}
                  @click=${() => void this.#submitReview("reject")}
                >
                  Request changes
                </button>
              `
            : nothing
        }
        <button
          class="gp-merge-btn"
          type="button"
          data-state=${state}
          ?disabled=${state !== "ready" || this.acting}
          @click=${() => void this.#merge(cr)}
        >
          ${cr.review.action}
        </button>
      </div>
      ${
        this.mergeNotice === null
          ? nothing
          : html`<p class="gp-notice" data-error>${this.mergeNotice}</p>`
      }
    `;
  }

  #subtasks(task: Task): TemplateResult | typeof nothing {
    const children = (task.children ?? [])
      .map((id) => store.get(id))
      .filter((child): child is Task => child !== undefined);
    if (children.length === 0) return nothing;
    return html`
      <h2 class="gp-section-label">Subtasks</h2>
      <div class="gp-panel-card">
        ${children.map(
          (child) => html`
            <button
              class="gp-subtask-row"
              type="button"
              @click=${() => navigate(this, { screen: "detail", id: child.id })}
            >
              ${kindChip(child)}
              <span class="gp-id">${child.id}</span>
              <span class="gp-subtask-title">${child.title}</span>
              ${statusPill(child)}
            </button>
          `,
        )}
      </div>
    `;
  }

  /**
   * One review thread: its conversation, its state, and the two verbs a
   * reader has — answer it, or settle it. Hub Change Requests only; the
   * fixtures keep the flat discussion they were designed with.
   */
  #thread_(task: Task, thread: NonNullable<Task["threads"]>[number]): TemplateResult {
    return html`
      <div class="gp-panel-card gp-thread" ?data-resolved=${thread.resolved}>
        <div class="gp-thread-head">
          <span class="gp-thread-path">${thread.path ?? "conversation"}</span>
          <span class="gp-thread-state">${thread.resolved ? "resolved" : "open"}</span>
          <button
            class="gp-btn-quiet"
            type="button"
            ?disabled=${this.acting}
            @click=${() => void this.#thread(thread.id, thread.resolved ? "reopen" : "resolve")}
          >
            ${thread.resolved ? "Reopen" : "Resolve"}
          </button>
        </div>
        ${thread.comments.map(
          (comment) => html`
            <div class="gp-comment">
              <span class="gp-avatar" data-size="lg">${comment.avatar}</span>
              <div>
                <div class="gp-comment-head">
                  <span class="gp-comment-author">${comment.author}</span>
                  <span class="gp-comment-when">${comment.when}</span>
                </div>
                <div class="gp-comment-body">${comment.text}</div>
              </div>
            </div>
          `,
        )}
        <form
          class="gp-thread-reply"
          @submit=${(event: SubmitEvent) => {
            event.preventDefault();
            const form = event.currentTarget;
            if (!(form instanceof HTMLFormElement)) return;
            const field = form.elements.namedItem("reply");
            if (!(field instanceof HTMLInputElement)) return;
            const body = field.value.trim();
            if (body === "") return;
            void this.#thread(thread.id, "reply", body).then(() => form.reset());
          }}
        >
          <input
            class="gp-input"
            name="reply"
            placeholder="Reply in this thread…"
            autocomplete="off"
          />
        </form>
      </div>
    `;
  }

  /**
   * A hub task's lease, and its end: claim it, let it go, close it. The
   * fixture tasks have no lease to speak of, so they show nothing here.
   */
  #lifecycle(task: Task): TemplateResult | typeof nothing {
    if (task.hub !== true || task.kind !== "Task") return nothing;
    const open = task.status !== "Done";
    if (!open) return nothing;
    const claimed = task.status === "In progress";
    return html`
      <div class="gp-panel-card gp-task-actions">
        <span class="gp-field-label">${claimed ? "Claimed" : "Available"}</span>
        <button
          class="gp-btn-quiet"
          type="button"
          ?disabled=${this.acting}
          @click=${() => void this.#taskAction(claimed ? "release" : "claim")}
        >
          ${claimed ? "Release" : "Claim"}
        </button>
        <button
          class="gp-btn-quiet"
          type="button"
          ?disabled=${this.acting}
          @click=${() => void this.#taskAction("complete")}
        >
          Complete
        </button>
        <button
          class="gp-btn-quiet"
          type="button"
          ?disabled=${this.acting}
          @click=${() => void this.#taskAction("abandon")}
        >
          Abandon
        </button>
      </div>
    `;
  }

  /**
   * Move this task under another, and re-render from the store's answer.
   *
   * The select carries task ids; the empty option detaches. Any member may
   * re-file work in the hub, so this is offered on every task rather than
   * only on the ones this browser opened.
   */
  #move = (event: Event): void => {
    const select = event.currentTarget;
    if (!(select instanceof HTMLSelectElement)) return;
    void store.move(this.taskId, select.value);
  };

  #meta(task: Task): TemplateResult {
    // One chain, outermost first. Rendering the release and the parent as two
    // rows said the same thing twice for anything filed straight under a
    // release, and claimed two relationships where the hub records one edge.
    const chain = store.ancestorsOf(task);
    // Nothing may be filed under its own descendants, and nothing under
    // itself — offering either would only earn a refusal from the hub.
    const under = new Set<string>([task.id, ...descendants(task)]);
    return html`
      <aside class="gp-meta">
        <div>
          <div class="gp-meta-label">Assignees</div>
          ${task.assignees.map(
            (person) => html`
              <div class="gp-assignee">
                <span class="gp-avatar" data-size="sm">${person.avatar}</span>
                <span>${person.name}</span>
              </div>
            `,
          )}
        </div>
        <div>
          <div class="gp-meta-label">Labels</div>
          <div class="gp-labels">
            ${task.labels.map(
              (label) => html`<span
                class="gp-label"
                style="--gp-label-color: var(--gp-${label.hue});"
                >${label.name}</span
              >`,
            )}
          </div>
        </div>
        <div>
          <div class="gp-meta-label">Belongs to</div>
          ${
            chain.length === 0
              ? html`<div class="gp-meta-value">—</div>`
              : html`<div class="gp-crumbs">
                  ${chain.map(
                    (ancestor, index) => html`
                      ${
                        index === 0
                          ? nothing
                          : html`<span class="gp-crumb-sep" aria-hidden="true">›</span>`
                      }
                      <button
                        class="gp-parent-link"
                        type="button"
                        title=${ancestor.id}
                        @click=${() => navigate(this, { screen: "detail", id: ancestor.id })}
                      >
                        ${ancestor.title}
                      </button>
                    `,
                  )}
                </div>`
          }
          <select class="gp-meta-select" aria-label="Move this task" @change=${this.#move}>
            <option value="" ?selected=${task.parent === undefined}>Belongs to nothing</option>
            ${store
              .list()
              .filter((candidate) => !under.has(candidate.id))
              .map(
                (candidate) => html`<option
                  value=${candidate.id}
                  ?selected=${candidate.id === task.parent}
                >
                  ${candidate.id} — ${candidate.title}
                </option>`,
              )}
          </select>
        </div>
      </aside>
    `;
  }
}

/**
 * Every task below this one, so the move control cannot offer a descendant.
 *
 * Cycle-guarded like `store.ancestorsOf`, and for the reason given there.
 */
const descendants = (task: Task, seen: Set<string> = new Set()): readonly string[] => {
  const out: string[] = [];
  for (const childId of task.children ?? []) {
    if (seen.has(childId)) continue;
    seen.add(childId);
    out.push(childId);
    const child = store.get(childId);
    if (child !== undefined) out.push(...descendants(child, seen));
  }
  return out;
};

/** Only reachable if the fixtures are emptied; keeps `#task` total. */
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

declare global {
  interface HTMLElementTagNameMap {
    "gp-detail": GpDetail;
  }
}
