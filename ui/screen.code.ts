/**
 * Code — the repository explorer and file view.
 *
 * This is the screen wired to the live server. `GET /:repo/files` answers every
 * blob at a ref as a flat list of tree paths, and `@pierre/trees` is path-first
 * — it takes `paths: string[]` and derives the folder structure itself — so the
 * response needs no reshaping. Selecting a row fetches that blob through
 * `GET /:repo/file` and renders it with `@pierre/diffs`, which brings Shiki
 * highlighting and a header that matches the design's file card.
 *
 * The pane edits as well as views. The pencil on the file card opens the blob
 * in a plain textarea, and the explorer's "+" opens the same editor over a new
 * path; committing either sends `POST /:repo/commit` with the file layered
 * onto the branch's current tree and `expected` pinned to the tip the editor
 * opened at — so a commit that lands mid-edit is a visible conflict, not a
 * silent overwrite. Deleting is the same request with `content: null`.
 *
 * When the API cannot be reached the screen falls back to the design's own
 * fixture tree and README, and says so in a quiet inline note. That keeps the
 * branch reviewable without a running worker, and keeps the fallback honest
 * rather than passing stale data off as live — and read-only, since there is
 * nothing to write to.
 */
import { html, nothing, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import { UIDialog } from "@chr33s/base-wc/src/dialog";
import type { MenuSelectDetail } from "@chr33s/base-wc/src/menu";
import { FileTree, type GitStatusEntry } from "@pierre/trees";
import type { File as DiffsFile } from "@pierre/diffs";

import {
  ApiError,
  type CommitDetail,
  type CommitFilesRequest,
  type CommitSummary,
  describe,
  type FileWrite,
  type GitApi,
  type Unavailable,
  type Whoami,
} from "./api.ts";
import { GitPlusElement } from "./base.ts";
import { diffs } from "./highlight.ts";
import * as icons from "./icons.ts";
import { current as currentTheme, type Theme } from "./theme.ts";
import { ago, initials } from "./time.ts";

/** The design's explorer, used when the server is unreachable. */
const FALLBACK_PATHS: readonly string[] = [
  ".github/workflows/ci.yml",
  ".husky/pre-commit",
  "docs/architecture.md",
  "node_modules/.package-lock.json",
  "src/components/Avatar.tsx",
  "src/components/Badge.tsx",
  "src/components/Button.test.tsx",
  "src/components/Button.tsx",
  "src/components/Card.test.tsx",
  "src/components/Card.tsx",
  "src/components/Dialog.tsx",
  "src/components/Input.tsx",
  "src/components/Spinner.tsx",
  "src/components/Tabs.tsx",
  "src/components/Tooltip.tsx",
  "src/hooks/useDebounce.test.ts",
  "src/hooks/useDebounce.ts",
  "src/hooks/useLocalStorage.ts",
  "src/hooks/useMediaQuery.ts",
  "src/hooks/useOnClickOutside.ts",
  "src/styles/globals.css",
  "src/styles/tokens.css",
  "src/styles/typography.css",
  "README.md",
];

/** The M and A badges the design shows against changed files. */
const FALLBACK_STATUS: readonly GitStatusEntry[] = [
  { path: "src/components/Button.tsx", status: "modified" },
  { path: "src/components/Tabs.tsx", status: "added" },
  { path: "src/components/Tooltip.tsx", status: "added" },
  { path: "src/hooks/useOnClickOutside.ts", status: "added" },
  { path: "src/styles/globals.css", status: "modified" },
  { path: "src/styles/tokens.css", status: "added" },
  { path: "src/styles/typography.css", status: "added" },
];

const FALLBACK_README = `# git+ core

The reference server implementation for git+ — a source platform where every
unit of work is a **Task**, and code changes are **Change Requests**: Tasks
with a diff attached.

\`\`\`
$ gitp clone git-plus/core
\`\`\`

See docs/architecture.md for the service layout and CONTRIBUTING.md before
opening a Change Request.
`;

/**
 * The tip commit, as the commit bar shows it.
 *
 * Author and age come from the raw commit header via `api.commitDetail` — no
 * JSON endpoint carries a timestamp, so that is the only route to one.
 */
interface HeadCommit {
  readonly sha: string;
  readonly message: string;
  readonly author: string;
  readonly avatar: string;
  readonly when: string;
}

/** One coherent repository view, committed only after every request succeeds. */
interface CodeSnapshot {
  readonly ref: string;
  readonly branches: readonly string[];
  readonly paths: readonly string[];
  readonly selected: string | null;
  readonly content: string | null;
  readonly head: HeadCommit | null;
  /** The tip's full oid — what `expected` pins when an edit commits. */
  readonly tip: string | null;
  readonly offline: boolean;
  readonly reason: string;
}

@customElement("gp-code")
export class GpCode extends GitPlusElement {
  /** Injected by the shell so every screen shares one client. */
  api: GitApi | null = null;

  @property({ type: String }) accessor theme: Theme = currentTheme();

  /** The `/whoami` answer, resolved once by the shell. */
  @property({ attribute: false }) accessor who: Whoami | null = null;

  /** A path navigation asked for — a search hit, or `#/code/<path>`. */
  @property({ type: String }) accessor wanted: string | null = null;

  @state() private accessor ref = "main";
  @state() private accessor branches: readonly string[] = ["main"];
  @state() private accessor selected: string | null = null;
  @state() private accessor content: string | null = null;
  @state() private accessor head: HeadCommit | null = null;
  @state() private accessor offline = false;
  /** Why the fallback is showing, in the reader\'s terms. */
  @state() private accessor reason = "";
  @state() private accessor loading = true;

  /** `edit` swaps the source pane for a textarea over the same blob. */
  @state() private accessor mode: "view" | "edit" = "view";

  /** Which side panel is open under the commit bar, if any. */
  @state() private accessor panel: "none" | "commits" | "filelog" = "none";
  /** Recent commits at the tip, for the commit-bar panel. */
  @state() private accessor commitLog: readonly CommitDetail[] | null = null;
  /** Commits touching the open file, for the file-history panel. */
  @state() private accessor fileLog: readonly CommitSummary[] | null = null;
  /** Set while viewing the open file at an older commit — read-only. */
  @state() private accessor at: string | null = null;
  @state() private accessor copied = false;
  /** Editing a path that does not exist yet, from the explorer's "+". */
  @state() private accessor editingNew = false;
  @state() private accessor saving = false;
  /** Why the last commit attempt failed, shown inside the editor. */
  @state() private accessor editError: string | null = null;

  /** The tip's full oid at last load — the `expected` for the next commit. */
  #tip: string | null = null;

  #tree: FileTree | null = null;
  #treeHost: HTMLElement | null = null;
  #viewer: DiffsFile | null = null;
  #paths: readonly string[] = [];
  #loadGeneration = 0;
  #fileGeneration = 0;

  override connectedCallback(): void {
    super.connectedCallback();
    void this.#load();
  }

  override willUpdate(changed: Map<string, unknown>): void {
    // A later navigation naming a path — another search hit — opens it in
    // place; the initial one is honoured by `#load` before the tree exists.
    if (changed.has("wanted") && this.wanted !== null && this.wanted !== this.selected) {
      void this.#open(this.wanted);
    }
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this.#loadGeneration += 1;
    this.#fileGeneration += 1;
    this.#tree?.cleanUp();
    this.#tree = null;
    this.#treeHost = null;
    this.#viewer = null;
  }

  /**
   * Fetch the ref list, the paths at the tip, and the tip commit.
   *
   * A failure at any step drops the whole screen to fixtures rather than
   * showing a half-live tree beside a fixture README, which would be the more
   * confusing outcome.
   */
  async #load(): Promise<void> {
    const generation = ++this.#loadGeneration;
    this.#fileGeneration += 1;
    const api = this.api;
    if (api === null) {
      this.#commit(this.#fallback());
      this.loading = false;
      return;
    }
    try {
      const refs = await api.refs();
      const branches = this.#branchNames(refs);
      const ref = branches.includes("main") ? "main" : (branches[0] ?? "main");
      const snapshot = await this.#snapshot(api, ref, refs, this.wanted ?? undefined);
      if (generation === this.#loadGeneration) this.#commit(snapshot);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#loadGeneration) this.#commit(this.#fallback(error));
    } finally {
      if (generation === this.#loadGeneration) {
        this.loading = false;
        this.#mountTree();
      }
    }
  }

  #branchNames(refs: readonly { readonly name: string }[]): readonly string[] {
    return refs
      .filter((ref) => ref.name.startsWith("refs/heads/"))
      .map((ref) => ref.name.slice("refs/heads/".length));
  }

  /**
   * Load one ref without consulting or mutating the component's current ref.
   *
   * `keep` names a path to stay on if it still exists — a reload after a
   * commit should show the file that was just written, not jump to the README.
   */
  async #snapshot(
    api: GitApi,
    ref: string,
    knownRefs?: Awaited<ReturnType<GitApi["refs"]>>,
    keep?: string,
  ): Promise<CodeSnapshot> {
    const [refs, files] = await Promise.all([
      knownRefs === undefined ? api.refs() : Promise.resolve(knownRefs),
      api.files(ref),
    ]);
    const paths = files.map((file) => file.path);
    const readme = paths.find((path) => /^readme(\.md|\.txt)?$/i.test(path)) ?? null;
    const selected = keep !== undefined && paths.includes(keep) ? keep : readme;
    const tip = refs.find((candidate) => candidate.name === `refs/heads/${ref}`);
    const [commit, content] = await Promise.all([
      tip === undefined ? Promise.resolve(null) : api.commitDetail(tip.oid),
      selected === null ? Promise.resolve(null) : api.file(ref, selected),
    ]);
    const head =
      commit === null
        ? null
        : {
            sha: commit.oid.slice(0, 7),
            message: commit.subject,
            author: commit.author,
            avatar: initials(commit.author),
            when: ago(commit.at),
          };
    return {
      ref,
      branches: this.#branchNames(refs),
      paths,
      selected,
      content,
      head,
      tip: tip?.oid ?? null,
      offline: false,
      reason: "",
    };
  }

  /**
   * Switch the ref the whole screen is showing.
   *
   * The explorer, the file view and the commit bar all read one ref, so this
   * refetches the lot rather than trying to patch each in place.
   */
  async #switchTo(ref: string): Promise<void> {
    const api = this.api;
    if (api === null) return;
    const generation = ++this.#loadGeneration;
    this.#fileGeneration += 1;
    this.loading = true;
    try {
      const snapshot = await this.#snapshot(api, ref);
      if (generation === this.#loadGeneration) this.#commit(snapshot);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#loadGeneration) this.#commit(this.#fallback(error));
    } finally {
      if (generation === this.#loadGeneration) {
        this.loading = false;
        this.#mountTree();
      }
    }
  }

  #fallback(error?: Unavailable): CodeSnapshot {
    return {
      ref: "main",
      branches: ["main"],
      paths: FALLBACK_PATHS,
      selected: "README.md",
      content: FALLBACK_README,
      head: {
        sha: "e4a91c2",
        message: "merge CR-18: update pipeline config",
        author: "rbaek",
        avatar: "RB",
        when: "2h ago",
      },
      tip: null,
      offline: true,
      reason: error === undefined ? "no API client was provided" : describe(error),
    };
  }

  #commit(snapshot: CodeSnapshot): void {
    this.ref = snapshot.ref;
    this.branches = snapshot.branches;
    this.#paths = snapshot.paths;
    this.selected = snapshot.selected;
    this.content = snapshot.content;
    this.head = snapshot.head;
    this.#tip = snapshot.tip;
    this.offline = snapshot.offline;
    this.reason = snapshot.reason;
    this.mode = "view";
    this.editingNew = false;
    this.editError = null;
    this.panel = "none";
    this.commitLog = null;
    this.fileLog = null;
    this.at = null;
    this.#viewer = null;
  }

  /**
   * Build the tree once, then feed it new paths on later loads.
   *
   * `FileTree` owns its own shadow root and rendering, so it is mounted
   * imperatively into a host element Lit leaves alone — Lit must not re-render
   * the subtree the tree manages.
   */
  #mountTree(): void {
    const host = this.querySelector<HTMLElement>(".gp-explorer-tree");
    if (host === null) return;

    if (this.#tree !== null && this.#treeHost === host) {
      this.#tree.resetPaths(this.#paths);
      if (this.offline) this.#tree.setGitStatus(FALLBACK_STATUS);
      return;
    }

    this.#tree?.cleanUp();
    this.#tree = new FileTree({
      paths: this.#paths,
      initialExpansion: "open",
      flattenEmptyDirectories: true,
      // The design's explorer header carries only "+" and "…"; repository-wide
      // search lives in the sidebar's ⌘K field, so the tree does not add a
      // second search box of its own.
      search: false,
      gitStatus: this.offline ? FALLBACK_STATUS : [],
      onSelectionChange: (paths) => {
        const path = paths[0];
        if (path !== undefined) void this.#open(path);
      },
    });
    this.#tree.render({ containerWrapper: host });
    this.#treeHost = host;
  }

  /** Selecting a row loads that blob and swaps the right-hand pane. */
  async #open(path: string): Promise<void> {
    // Directories arrive here too; they have no blob to read.
    if (!this.#paths.includes(path)) return;
    const generation = ++this.#fileGeneration;
    const ref = this.ref;
    this.selected = path;
    this.content = null;
    // Walking the tree abandons an open editor rather than carrying a draft
    // of one file over to another, and a history view belongs to the file it
    // was opened from.
    this.mode = "view";
    this.editingNew = false;
    this.editError = null;
    this.at = null;
    if (this.panel === "filelog") {
      this.panel = "none";
      this.fileLog = null;
    }
    const api = this.api;
    if (api === null || this.offline) {
      this.content = path === "README.md" ? FALLBACK_README : `// ${path}`;
      return;
    }
    try {
      const content = await api.file(ref, path);
      if (generation === this.#fileGeneration && ref === this.ref && path === this.selected) {
        this.content = content;
      }
    } catch (error) {
      if (!(error instanceof ApiError)) throw error;
      if (generation === this.#fileGeneration && ref === this.ref && path === this.selected) {
        this.content = `// ${path} — ${error.message}`;
      }
    }
  }

  /** Open the current blob in the editor. */
  #edit(): void {
    if (this.offline || this.loading || this.selected === null || this.content === null) return;
    this.editingNew = false;
    this.editError = null;
    this.mode = "edit";
  }

  /** The explorer's "+": the same editor, over a path that does not exist. */
  #newFile(): void {
    if (this.offline || this.loading || this.api === null) return;
    this.editingNew = true;
    this.editError = null;
    this.mode = "edit";
  }

  #cancelEdit(): void {
    this.mode = "view";
    this.editingNew = false;
    this.editError = null;
  }

  /** Re-read the current ref after a write, staying on `keep` if it survived. */
  async #reload(keep?: string): Promise<void> {
    const api = this.api;
    if (api === null) return;
    const generation = ++this.#loadGeneration;
    this.#fileGeneration += 1;
    try {
      const snapshot = await this.#snapshot(api, this.ref, undefined, keep);
      if (generation === this.#loadGeneration) this.#commit(snapshot);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      if (generation === this.#loadGeneration) this.#commit(this.#fallback(error));
    } finally {
      if (generation === this.#loadGeneration) this.#mountTree();
    }
  }

  /** Commit what the editor holds: the draft content at the chosen path. */
  async #save(): Promise<void> {
    if (this.saving) return;
    const editor = this.querySelector<HTMLTextAreaElement>(".gp-editor");
    if (editor === null) return;
    const path = this.editingNew
      ? (this.querySelector<HTMLInputElement>(".gp-editor-path")?.value ?? "").trim()
      : this.selected;
    if (path === null || path === "") {
      this.editError = "name the file to create";
      return;
    }
    if (this.editingNew && this.#paths.includes(path)) {
      this.editError = `${path} already exists — select it in the explorer and edit it instead`;
      return;
    }
    const message = (
      this.querySelector<HTMLInputElement>(".gp-editor-message")?.value ?? ""
    ).trim();
    const fallback = `${this.editingNew ? "add" : "update"} ${path}`;
    await this.#write({ path, content: editor.value }, message === "" ? fallback : message, path);
  }

  /** Remove the open file — the same commit request, with `content: null`. */
  async #delete(): Promise<void> {
    const path = this.selected;
    if (path === null || this.editingNew || this.saving) return;
    await this.#write({ path, content: null }, `delete ${path}`, undefined);
  }

  /**
   * One commit against the tip the editor opened at, then a reload.
   *
   * `expected` pins that tip, so a commit that landed mid-edit answers
   * `RefConflict` — surfaced as an error the writer can act on — instead of
   * being silently parented over.
   */
  async #write(file: FileWrite, message: string, keep: string | undefined): Promise<void> {
    const api = this.api;
    if (api === null) return;
    this.saving = true;
    this.editError = null;
    const options: CommitFilesRequest = {
      branch: this.ref,
      message,
      files: [file],
    };
    if (this.#tip !== null) options.expected = this.#tip;
    try {
      await api.commitFiles(options);
      await this.#reload(keep);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.editError =
        error instanceof ApiError && error.tag === "RefConflict"
          ? `someone else committed to ${this.ref} while you were editing — copy your draft, reload, and reapply it`
          : describe(error);
    } finally {
      this.saving = false;
    }
  }

  /** Show the open file as it was at `oid` — a read-only look back. */
  async #openAt(oid: string): Promise<void> {
    const api = this.api;
    const path = this.selected;
    if (api === null || path === null || this.offline) return;
    const generation = ++this.#fileGeneration;
    this.mode = "view";
    this.content = null;
    this.at = oid;
    try {
      const content = await api.file(oid, path);
      if (generation === this.#fileGeneration && path === this.selected) this.content = content;
    } catch (error) {
      if (!(error instanceof ApiError)) throw error;
      if (generation === this.#fileGeneration && path === this.selected) {
        this.content = `// ${path} — ${error.message}`;
      }
    }
  }

  /** The commit bar toggles the recent-history panel it summarises. */
  async #toggleCommits(): Promise<void> {
    if (this.panel === "commits") {
      this.panel = "none";
      return;
    }
    this.panel = "commits";
    const api = this.api;
    const tip = this.#tip;
    if (api === null || tip === null || this.commitLog !== null) return;
    try {
      const commits = await api.recentCommits(tip, 20);
      if (this.#tip === tip) this.commitLog = commits;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.commitLog = [];
    }
  }

  /** The clock on the file card: commits that touched the open file. */
  async #toggleFileLog(): Promise<void> {
    if (this.panel === "filelog") {
      this.panel = "none";
      return;
    }
    this.panel = "filelog";
    const api = this.api;
    const tip = this.#tip;
    const path = this.selected;
    if (api === null || tip === null || path === null) return;
    this.fileLog = null;
    try {
      const entries = await api.history(tip, path);
      if (this.#tip === tip && this.selected === path) this.fileLog = entries;
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.fileLog = [];
    }
  }

  /** The branch menu's "New branch…": create at the current tip, switch to it. */
  #createBranch = async (event: SubmitEvent): Promise<void> => {
    event.preventDefault();
    const api = this.api;
    const form = event.currentTarget;
    if (api === null || !(form instanceof HTMLFormElement)) return;
    const field = form.elements.namedItem("branch");
    if (!(field instanceof HTMLInputElement)) return;
    const name = field.value.trim();
    if (name === "") return;
    try {
      await api.branchCreate(name, this.ref);
    } catch (error) {
      if (!(error instanceof ApiError) && !(error instanceof TypeError)) throw error;
      this.editError = null;
      this.reason = "";
      // The dialog stays open with the field intact; the title carries why.
      field.setCustomValidity(error instanceof ApiError ? error.message : "not reachable");
      field.reportValidity();
      return;
    }
    form.reset();
    this.#branchDialog()?.hide();
    await this.#switchTo(name);
  };

  #branchDialog(): UIDialog | null {
    const dialog = this.querySelector(".gp-new-branch");
    return dialog instanceof UIDialog ? dialog : null;
  }

  /** The Clone dialog's copy: the same URL `git clone` would be handed. */
  #copyClone = (): void => {
    const url = this.api?.cloneUrl;
    if (url === undefined) return;
    void navigator.clipboard.writeText(url).then(() => {
      this.copied = true;
      setTimeout(() => {
        this.copied = false;
      }, 1500);
    });
  };

  protected override updated(): void {
    this.#mountTree();
    void this.#renderSource();
  }

  /**
   * Hand the file to `@pierre/diffs`, which owns its own shadow root.
   *
   * The module is fetched on first use — see `highlight.ts` — so the path and
   * content are re-read after the await in case the selection moved on while
   * the chunk was in flight.
   */
  async #renderSource(): Promise<void> {
    const host = this.querySelector<HTMLElement>(".gp-source-host");
    if (host === null || this.selected === null || this.content === null) return;
    const { File } = await diffs();
    const path = this.selected;
    const contents = this.content;
    if (path === null || contents === null) return;
    this.#viewer ??= new File({
      themeType: this.theme,
      disableFileHeader: true,
      overflow: "scroll",
      stickyHeader: false,
    });
    this.#viewer.setThemeType(this.theme);
    this.#viewer.render({ file: { name: path, contents }, containerWrapper: host });
  }

  protected override render(): TemplateResult {
    return html`
      <div class="gp-explorer">
        <div class="gp-explorer-head">
          <div class="gp-explorer-title">Explorer</div>
          <div class="gp-explorer-actions">
            <button
              class="gp-icon-btn"
              type="button"
              title=${this.offline ? "Read-only — the git+ API is not reachable" : "New file"}
              aria-label="New file"
              ?disabled=${this.offline || this.loading}
              @click=${() => this.#newFile()}
            >
              ${icons.plus()}
            </button>
            <ui-menu class="gp-explorer-menu" @menu-select=${this.#onExplorerAction}>
              <button
                class="gp-icon-btn"
                type="button"
                data-menu-trigger
                title="Explorer actions"
                aria-label="Explorer actions"
              >
                ${icons.ellipsis()}
              </button>
              <ui-menu-popup class="gp-menu-popup">
                <ui-menu-item class="gp-menu-item" value="refresh">Refresh</ui-menu-item>
              </ui-menu-popup>
            </ui-menu>
          </div>
        </div>
        <div class="gp-explorer-tree"></div>
      </div>

      <div class="gp-main">
        <header class="gp-repo-header">
          <div class="gp-breadcrumb">
            <span class="gp-breadcrumb-owner">git-plus</span>
            <span class="gp-breadcrumb-sep">/</span>
            <span class="gp-breadcrumb-name">${this.api?.repo ?? "core"}</span>
            <span class="gp-pill-outline" title=${this.who?.why ?? ""}
              >${this.who?.member === true ? "Member" : "Public"}</span
            >
          </div>
          <div class="gp-repo-actions">${this.#branchMenu()} ${this.#clone()}</div>
        </header>

        <div class="gp-screen gp-screen--code">
          ${
            this.offline
              ? html`<p class="gp-notice">
                  Showing the design's sample repository — ${this.reason}.
                </p>`
              : ""
          }
          ${this.head === null ? "" : this.#commitBar(this.head)} ${this.#panel()}

          <div class="gp-card gp-file-card">
            <div class="gp-card-head">
              ${icons.document_()}
              ${
                this.mode === "edit" && this.editingNew
                  ? html`<input
                      class="gp-input gp-editor-path"
                      placeholder="path/to/file.md"
                      aria-label="New file path"
                      autocomplete="off"
                      spellcheck="false"
                    />`
                  : html`${this.selected ?? "—"}`
              }
              ${
                this.mode === "view"
                  ? html`<button
                        class="gp-icon-btn"
                        type="button"
                        title="File history"
                        aria-label="File history"
                        ?data-active=${this.panel === "filelog"}
                        ?disabled=${this.offline || this.loading || this.selected === null}
                        @click=${() => void this.#toggleFileLog()}
                      >
                        ${icons.clock(14)}
                      </button>
                      <button
                        class="gp-icon-btn"
                        type="button"
                        data-tight
                        title=${
                          this.offline
                            ? "Read-only — the git+ API is not reachable"
                            : this.at !== null
                              ? "Read-only — viewing an old commit"
                              : "Edit file"
                        }
                        aria-label="Edit file"
                        ?disabled=${
                          this.offline || this.loading || this.selected === null || this.at !== null
                        }
                        @click=${() => this.#edit()}
                      >
                        ${icons.pencil(14)}
                      </button>`
                  : nothing
              }
            </div>
            ${this.#atBanner()}
            ${
              this.loading
                ? html`<div class="gp-empty">Loading…</div>`
                : this.mode === "edit"
                  ? this.#editor()
                  : html`<div class="gp-source-host gp-diff-host"></div>`
            }
          </div>
        </div>
      </div>
    `;
  }

  /**
   * The editor: the blob in a textarea, and a commit bar under it.
   *
   * The textarea is uncontrolled on purpose — Lit sets `.value` when the
   * template's value changes and leaves the user's typing alone otherwise —
   * and `#save` reads it back at commit time.
   */
  #editor(): TemplateResult {
    return html`
      <textarea
        class="gp-editor"
        spellcheck="false"
        aria-label="File contents"
        .value=${this.editingNew ? "" : (this.content ?? "")}
      ></textarea>
      ${
        this.editError === null
          ? nothing
          : html`<p class="gp-notice" data-error>${this.editError}</p>`
      }
      <div class="gp-editor-bar">
        <input
          class="gp-input gp-editor-message"
          aria-label="Commit message"
          placeholder=${
            this.editingNew ? "commit message" : `update ${this.selected ?? "this file"}`
          }
          autocomplete="off"
        />
        ${
          this.editingNew
            ? nothing
            : html`<button
                class="gp-btn-quiet"
                type="button"
                title="Remove this file in a new commit"
                ?disabled=${this.saving}
                @click=${() => void this.#delete()}
              >
                Delete file
              </button>`
        }
        <button
          class="gp-btn-quiet"
          type="button"
          ?disabled=${this.saving}
          @click=${() => this.#cancelEdit()}
        >
          Cancel
        </button>
        <button
          class="gp-btn-primary"
          type="button"
          ?disabled=${this.saving}
          @click=${() => void this.#save()}
        >
          ${this.saving ? "Committing…" : "Commit"}
        </button>
      </div>
    `;
  }

  /** base-wc types its own `menu-select` detail, so nothing here is asserted. */
  #onBranchSelect = (event: CustomEvent<MenuSelectDetail>): void => {
    if (event.detail.value === "__new-branch") {
      this.#branchDialog()?.show();
      return;
    }
    void this.#switchTo(event.detail.value);
  };

  #onExplorerAction = (event: CustomEvent<MenuSelectDetail>): void => {
    if (event.detail.value === "refresh") void this.#reload(this.selected ?? undefined);
  };

  /**
   * The branch picker, over the real ref list.
   *
   * `ui-menu` from base-wc owns the popover, keyboard handling and dismissal;
   * `menu-select` carries the chosen value. The last item creates a branch at
   * the current tip through `POST /branches/create`. Offline there is nothing
   * to pick or create, so the design's plain button renders disabled.
   */
  #branchMenu(): TemplateResult {
    const trigger = html`${icons.branch()} ${this.ref} ${icons.chevronDown()}`;
    if (this.offline || this.api === null) {
      return html`<button class="gp-branch-trigger" type="button" disabled>${trigger}</button>`;
    }
    return html`
      <ui-menu class="gp-branch-menu" @menu-select=${this.#onBranchSelect}>
        <button class="gp-branch-trigger" type="button" data-menu-trigger>${trigger}</button>
        <ui-menu-popup class="gp-menu-popup">
          ${this.branches.map(
            (branch) => html`
              <ui-menu-item
                class="gp-menu-item"
                value=${branch}
                ?data-current=${branch === this.ref}
              >
                ${icons.branch(12)} ${branch}
              </ui-menu-item>
            `,
          )}
          <ui-menu-item class="gp-menu-item" data-action value="__new-branch">
            ${icons.plus(12)} New branch…
          </ui-menu-item>
        </ui-menu-popup>
      </ui-menu>
      <ui-dialog class="gp-new-branch">
        <ui-dialog-popup class="gp-dialog">
          <h2 class="gp-dialog-title" data-dialog-title>New branch</h2>
          <p class="gp-dialog-hint" data-dialog-description>
            Created at the tip of ${this.ref} and switched to.
          </p>
          <form @submit=${this.#createBranch}>
            <label class="gp-field-label" for="gp-new-branch-name">Name</label>
            <input
              id="gp-new-branch-name"
              class="gp-input"
              name="branch"
              required
              autocomplete="off"
              spellcheck="false"
              placeholder="topic/branch-name"
              @input=${(event: Event) => {
                if (event.target instanceof HTMLInputElement) event.target.setCustomValidity("");
              }}
            />
            <div class="gp-dialog-actions">
              <button
                class="gp-btn-quiet"
                type="button"
                @click=${() => this.#branchDialog()?.hide()}
              >
                Cancel
              </button>
              <button class="gp-btn-primary" type="submit">Create branch</button>
            </div>
          </form>
        </ui-dialog-popup>
      </ui-dialog>
    `;
  }

  /** The Clone dialog: the smart-HTTP URL, and a copy that says it copied. */
  #clone(): TemplateResult {
    return html`
      <ui-dialog class="gp-clone">
        <button class="gp-btn-primary" data-dialog-trigger type="button">Clone</button>
        <ui-dialog-popup class="gp-dialog">
          <h2 class="gp-dialog-title" data-dialog-title>Clone</h2>
          <p class="gp-dialog-hint" data-dialog-description>
            Smart HTTP, served from the same place as this page.
          </p>
          <div class="gp-clone-row">
            <input
              class="gp-input gp-clone-url"
              readonly
              .value=${this.api?.cloneUrl ?? ""}
              aria-label="Clone URL"
              @focus=${(event: Event) => {
                if (event.target instanceof HTMLInputElement) event.target.select();
              }}
            />
            <button class="gp-btn-quiet" type="button" @click=${this.#copyClone}>
              ${this.copied ? "Copied" : "Copy"}
            </button>
          </div>
          <p class="gp-dialog-hint">git clone ${this.api?.cloneUrl ?? ""}</p>
        </ui-dialog-popup>
      </ui-dialog>
    `;
  }

  /** Whichever history panel is open: the branch's commits, or one file's. */
  #panel(): TemplateResult | typeof nothing {
    if (this.panel === "none") return nothing;
    if (this.panel === "commits") {
      const log = this.commitLog;
      return html`
        <div class="gp-panel-card gp-history-panel">
          ${
            log === null
              ? html`<div class="gp-empty">Loading history…</div>`
              : log.length === 0
                ? html`<div class="gp-empty">No history to show.</div>`
                : log.map(
                    (commit) => html`
                      <div class="gp-list-row">
                        <span class="gp-sha">${commit.oid.slice(0, 7)}</span>
                        <span>${commit.subject}</span>
                        <span class="gp-when">${initials(commit.author)} · ${ago(commit.at)}</span>
                      </div>
                    `,
                  )
          }
        </div>
      `;
    }
    const entries = this.fileLog;
    return html`
      <div class="gp-panel-card gp-history-panel">
        ${
          entries === null
            ? html`<div class="gp-empty">Loading file history…</div>`
            : entries.length === 0
              ? html`<div class="gp-empty">No commits touch this file.</div>`
              : entries.map(
                  (entry) => html`
                    <button
                      class="gp-list-row gp-filelog-row"
                      type="button"
                      ?data-current=${this.at === entry.oid}
                      @click=${() => void this.#openAt(entry.oid)}
                    >
                      <span class="gp-sha">${entry.oid.slice(0, 7)}</span>
                      <span>${(entry.message.split("\n", 1)[0] ?? "").trim()}</span>
                    </button>
                  `,
                )
        }
      </div>
    `;
  }

  /** The read-only banner while the pane shows an old revision. */
  #atBanner(): TemplateResult | typeof nothing {
    const path = this.selected;
    const at = this.at;
    if (path === null || at === null) return nothing;
    return html`
      <p class="gp-notice" data-history>
        Viewing ${path} at ${at.slice(0, 7)} — read-only.
        <button class="gp-link-btn" type="button" @click=${() => void this.#open(path)}>
          Back to tip
        </button>
      </p>
    `;
  }

  #commitBar(head: HeadCommit): TemplateResult {
    return html`
      <button
        class="gp-commit-bar"
        type="button"
        title="Recent commits"
        aria-expanded=${this.panel === "commits" ? "true" : "false"}
        ?disabled=${this.offline}
        @click=${() => void this.#toggleCommits()}
      >
        ${head.avatar === undefined ? "" : html`<span class="gp-avatar">${head.avatar}</span>`}
        ${
          head.author === undefined
            ? ""
            : html`<span class="gp-commit-author">${head.author}</span>`
        }
        <span class="gp-commit-subject">${head.message}</span>
        <span class="gp-commit-sha">${head.sha}</span>
        <span>${head.when}</span>
      </button>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-code": GpCode;
  }
}
