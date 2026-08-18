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
 * When the API cannot be reached the screen falls back to the design's own
 * fixture tree and README, and says so in a quiet inline note. That keeps the
 * branch reviewable without a running worker, and keeps the fallback honest
 * rather than passing stale data off as live.
 */
import { html, type TemplateResult } from "lit";
import { customElement, property, state } from "lit/decorators.js";

import type { MenuSelectDetail } from "@chr33s/base-wc/src/menu";
import { FileTree, type GitStatusEntry } from "@pierre/trees";
import type { File as DiffsFile } from "@pierre/diffs";

import { ApiError, describe, type GitApi, type Unavailable } from "./api.ts";
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
  readonly offline: boolean;
  readonly reason: string;
}

@customElement("gp-code")
export class GpCode extends GitPlusElement {
  /** Injected by the shell so every screen shares one client. */
  api: GitApi | null = null;

  @property({ type: String }) accessor theme: Theme = currentTheme();

  @state() private accessor ref = "main";
  @state() private accessor branches: readonly string[] = ["main"];
  @state() private accessor selected: string | null = null;
  @state() private accessor content: string | null = null;
  @state() private accessor head: HeadCommit | null = null;
  @state() private accessor offline = false;
  /** Why the fallback is showing, in the reader\'s terms. */
  @state() private accessor reason = "";
  @state() private accessor loading = true;

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
      const snapshot = await this.#snapshot(api, ref, refs);
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

  /** Load one ref without consulting or mutating the component's current ref. */
  async #snapshot(
    api: GitApi,
    ref: string,
    knownRefs?: Awaited<ReturnType<GitApi["refs"]>>,
  ): Promise<CodeSnapshot> {
    const [refs, files] = await Promise.all([
      knownRefs === undefined ? api.refs() : Promise.resolve(knownRefs),
      api.files(ref),
    ]);
    const paths = files.map((file) => file.path);
    const readme = paths.find((path) => /^readme(\.md|\.txt)?$/i.test(path)) ?? null;
    const tip = refs.find((candidate) => candidate.name === `refs/heads/${ref}`);
    const [commit, content] = await Promise.all([
      tip === undefined ? Promise.resolve(null) : api.commitDetail(tip.oid),
      readme === null ? Promise.resolve(null) : api.file(ref, readme),
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
      selected: readme,
      content,
      head,
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
    this.offline = snapshot.offline;
    this.reason = snapshot.reason;
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
          <div class="gp-explorer-actions">${icons.plus()} ${icons.ellipsis()}</div>
        </div>
        <div class="gp-explorer-tree"></div>
      </div>

      <div class="gp-main">
        <header class="gp-repo-header">
          <div class="gp-breadcrumb">
            <span class="gp-breadcrumb-owner">git-plus</span>
            <span class="gp-breadcrumb-sep">/</span>
            <span class="gp-breadcrumb-name">${this.api?.repo ?? "core"}</span>
            <span class="gp-pill-outline">Public</span>
          </div>
          <div class="gp-repo-actions">
            ${this.#branchMenu()}
            <button class="gp-btn-primary" type="button">Clone</button>
          </div>
        </header>

        <div class="gp-screen gp-screen--code">
          ${
            this.offline
              ? html`<p class="gp-notice">
                  Showing the design's sample repository — ${this.reason}.
                </p>`
              : ""
          }
          ${this.head === null ? "" : this.#commitBar(this.head)}

          <div class="gp-card gp-file-card">
            <div class="gp-card-head">
              ${icons.document_()} ${this.selected ?? "—"}
              <button
                class="gp-icon-btn"
                type="button"
                title="View source"
                aria-label="View source"
              >
                ${icons.code(14)}
              </button>
            </div>
            ${
              this.loading
                ? html`<div class="gp-empty">Loading…</div>`
                : html`<div class="gp-source-host gp-diff-host"></div>`
            }
          </div>
        </div>
      </div>
    `;
  }

  /** base-wc types its own `menu-select` detail, so nothing here is asserted. */
  #onBranchSelect = (event: CustomEvent<MenuSelectDetail>): void => {
    void this.#switchTo(event.detail.value);
  };

  /**
   * The branch picker, over the real ref list.
   *
   * `ui-menu` from base-wc owns the popover, keyboard handling and dismissal;
   * `menu-select` carries the chosen value. With one branch there is nothing to
   * pick, so it renders as the plain button the design draws.
   */
  #branchMenu(): TemplateResult {
    const trigger = html`${icons.branch()} ${this.ref} ${icons.chevronDown()}`;
    if (this.branches.length < 2) {
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
        </ui-menu-popup>
      </ui-menu>
    `;
  }

  #commitBar(head: HeadCommit): TemplateResult {
    return html`
      <div class="gp-commit-bar">
        ${head.avatar === undefined ? "" : html`<span class="gp-avatar">${head.avatar}</span>`}
        ${
          head.author === undefined
            ? ""
            : html`<span class="gp-commit-author">${head.author}</span>`
        }
        <span class="gp-commit-subject">${head.message}</span>
        <span class="gp-commit-sha">${head.sha}</span>
        <span>${head.when}</span>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "gp-code": GpCode;
  }
}
